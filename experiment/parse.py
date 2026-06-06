#!/usr/bin/env -S uv run

"""
Parse benchmark output files and produce a summary result.yaml per experiment.

Usage:
    uv run experiment/parse.py <experiment_name>
    uv run experiment/parse.py scale_clients
    uv run experiment/parse.py scale_clients --warmup 3
"""

import argparse
import math
import re
import statistics
from pathlib import Path

import yaml
from config import (
    config_path_for,
    load_experiment_spec,
    resolve_run_spec,
    result_dir,
)

ONLINE_COLUMNS = [
    "min_us",
    "p5_us",
    "median_us",
    "avg_us",
    "p95_us",
    "p99_us",
    "p999_us",
    "total_ops",
    "iter_ops",
    "tpt_mops",
]

AOF_BASE_COLUMNS = ["time_ms", "bytes", "bandwidth", "throughput"]
OFFLINE_COLUMNS = ["time_ms", "total_ops", "throughput"]

AOF_METRIC_NAME_MAP = {
    "Bandwidth": "bandwidth",
    "Total pages send": "pages",
    "Total records replayed": "records",
    "Total records enqueued": "records",
}

HEADER_RE = re.compile(r"min\s*\(us\)")
AOF_METRIC_RE = re.compile(r"^\[(?P<name>[^\]]+)\]:\s*(?P<value>.+)$")
AOF_LATENCY_LINE_RE = re.compile(r"^\[(?P<who>Reader|Writer) latency us\]\s+(?P<value>.+)$")
AOF_LATENCY_FIELD_RE = re.compile(r"(?P<key>p\d+(?:\.\d+)?|max|mean)=(?P<value>[-+]?\d[\d,]*(?:\.\d+)?)")
AOF_NUMBER_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")
OFFLINE_OPERATION_RE = re.compile(r"^Operation type:\s*(?P<value>.+)$")


def _is_data_row(parts: list[str]) -> bool:
    if len(parts) != len(ONLINE_COLUMNS):
        return False
    try:
        [float(p) for p in parts]
        return True
    except ValueError:
        return False


def _parse_number(text: str) -> float | None:
    match = AOF_NUMBER_RE.search(text)
    if match is None:
        return None
    return float(match.group(0).replace(",", ""))


def _snake_case(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("/", " per ")
    text = re.sub(r"[()]", "", text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _stats(values: list[float | None]) -> dict:
    values = [v for v in values if v is not None]
    if not values:
        return {"median": None, "mean": None, "std": None, "min": None, "max": None}
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n if n > 1 else 0.0
    return {
        "median": round(statistics.median(values), 4),
        "mean": round(mean, 4),
        "std": round(math.sqrt(variance), 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
    }


def _summarize_samples(samples: list[dict], columns: list[str]) -> dict:
    if not samples:
        return {col: _stats([]) for col in columns}
    return {col: _stats([sample.get(col) for sample in samples]) for col in columns}


def _column_median(samples: list[dict], col: str) -> float | None:
    values = [s.get(col) for s in samples if s.get(col) is not None]
    return statistics.median(values) if values else None


def _aggregate_repetitions(
    samples: list[dict], columns: list[str], repeat: int
) -> list[dict]:
    """Split samples evenly into `repeat` chunks; emit one median row per chunk."""
    if repeat <= 1 or len(samples) < repeat:
        return samples
    chunk_size = len(samples) // repeat
    aggregated: list[dict] = []
    for i in range(repeat):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < repeat - 1 else len(samples)
        chunk = samples[start:end]
        if not chunk:
            continue
        aggregated.append({col: _column_median(chunk, col) for col in columns})
    return aggregated


def _format_count(value: float | None) -> str:
    if value is None:
        return "None"
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{value:,.3f}"


def _format_table_number(
    value: float | None, decimals: int = 3, integer_threshold: float = 1_000_000
) -> str:
    if value is None:
        return "-"
    if float(value).is_integer() and abs(value) >= integer_threshold:
        return f"{int(value):,}"
    return f"{value:.{decimals}f}"


def _format_sweep_value(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _parse_online_output(
    path: Path, warmup_rows: int = 2
) -> tuple[list[dict], list[str]]:
    """Parse the tabular RespOnlineBench output format."""
    samples = []
    past_header = False
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not past_header:
                if HEADER_RE.search(line):
                    past_header = True
                continue
            parts = line.split()
            if _is_data_row(parts):
                row = {col: float(v) for col, v in zip(ONLINE_COLUMNS, parts)}
                row["tpt_mops"] /= 1000.0
                samples.append(row)

    return samples[warmup_rows:], ONLINE_COLUMNS


def _parse_aof_output(path: Path) -> tuple[list[dict], list[str]]:
    """Parse the labeled summary format emitted by AofBench and ReplicationBench."""
    samples = []
    columns: list[str] = [*AOF_BASE_COLUMNS]
    current = None

    def keep(sample: dict | None) -> bool:
        return bool(sample) and any(
            sample.get(key) is not None
            for key in ("throughput", "reader_throughput", "writer_throughput")
        )

    with open(path) as f:
        for raw_line in f:
            line = raw_line.strip()

            lat_match = AOF_LATENCY_LINE_RE.match(line)
            if lat_match and current is not None:
                prefix = lat_match.group("who").lower() + "_lat_"
                for field in AOF_LATENCY_FIELD_RE.finditer(lat_match.group("value")):
                    key = prefix + field.group("key").replace(".", "_")
                    current[key] = _parse_number(field.group("value"))
                for key in current:
                    if key not in columns:
                        columns.append(key)
                continue

            match = AOF_METRIC_RE.match(line)
            if not match:
                continue

            name = match.group("name")
            value = match.group("value")

            if name == "Total time":
                if keep(current):
                    samples.append(current)
                current = {}
                current["time_ms"] = _parse_number(value)

                bytes_match = re.search(
                    r"for\s+([-+]?\d[\d,]*(?:\.\d+)?)\s+AOF bytes", value
                )
                current["bytes"] = (
                    float(bytes_match.group(1).replace(",", ""))
                    if bytes_match is not None
                    else None
                )
            elif current is not None:
                metric_key = AOF_METRIC_NAME_MAP.get(name, _snake_case(name))
                metric_value = _parse_number(value)
                if metric_key in ("throughput", "reader_throughput", "writer_throughput"):
                    current[metric_key] = (
                        None if metric_value is None else metric_value / 1_000_000.0
                    )
                else:
                    current[metric_key] = metric_value

            if current is not None:
                for key in current:
                    if key not in columns:
                        columns.append(key)

    if keep(current):
        samples.append(current)
    return samples, columns


def _parse_offline_output(
    path: Path, expected_op: str | None = None
) -> tuple[list[dict], list[str]]:
    """Parse the labeled summary format emitted by Resp.benchmark offline mode."""
    samples = []
    current = None
    expected_op_normalized = expected_op.upper() if expected_op else None

    with open(path) as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            op_match = OFFLINE_OPERATION_RE.match(line)
            if op_match:
                if current and current.get("throughput") is not None:
                    samples.append(current)
                current = {"operation": op_match.group("value").strip()}
                continue

            metric_match = AOF_METRIC_RE.match(line)
            if metric_match is None or current is None:
                continue

            name = metric_match.group("name")
            value = metric_match.group("value")

            if name == "Total time":
                current["time_ms"] = _parse_number(value)
                ops_match = re.search(r"for\s+([-+]?\d[\d,]*(?:\.\d+)?)\s+ops", value)
                current["total_ops"] = (
                    float(ops_match.group(1).replace(",", ""))
                    if ops_match is not None
                    else None
                )
            elif name == "Throughput":
                parsed = _parse_number(value)
                current["throughput"] = None if parsed is None else parsed / 1_000_000.0

    if current and current.get("throughput") is not None:
        samples.append(current)

    if expected_op_normalized is not None:
        filtered_samples = [
            sample
            for sample in samples
            if str(sample.get("operation", "")).upper() == expected_op_normalized
        ]
        if filtered_samples:
            samples = filtered_samples

    normalized_samples = [
        {column: sample.get(column) for column in OFFLINE_COLUMNS} for sample in samples
    ]
    return normalized_samples, OFFLINE_COLUMNS


def parse_output(
    path: Path,
    benchmark: str,
    warmup_rows: int = 2,
    expected_op: str | None = None,
) -> tuple[list[dict], list[str]]:
    """Return parsed samples and the benchmark-specific metric columns."""
    if benchmark in ("aof", "replication"):
        return _parse_aof_output(path)
    if benchmark == "online":
        return _parse_online_output(path, warmup_rows=warmup_rows)
    if benchmark == "offline":
        return _parse_offline_output(path, expected_op=expected_op)
    raise ValueError(f"Unsupported benchmark: {benchmark}")


def _format_summary(
    benchmark: str, stats: dict, num_samples: int, run_name: str
) -> str:
    def fmt_mops(key: str) -> str:
        median = (stats.get(key) or {}).get("median")
        return "None" if median is None else f"{median:.3f} M"

    if benchmark == "replication":
        return ", ".join(
            [
                f"  Parsed {run_name}: {num_samples} samples",
                f"median writer={fmt_mops('writer_throughput')} ops/s",
                f"median reader={fmt_mops('reader_throughput')} ops/s",
            ]
        )

    thr_key = "tpt_mops" if benchmark == "online" else "throughput"
    thr = stats[thr_key]["median"]
    thr_str = "None" if thr is None else f"{thr:.3f} M"
    if benchmark == "aof":
        return ", ".join(
            [
                f"  Parsed {run_name}: {num_samples} samples",
                f"median throughput={thr_str} records/s",
                f"bandwidth={stats['bandwidth']['median']} GiB/s",
            ]
        )
    if benchmark == "offline":
        return ", ".join(
            [
                f"  Parsed {run_name}: {num_samples} samples",
                f"median throughput={thr_str} ops/s",
                f"total ops={_format_count(stats['total_ops']['median'])}",
            ]
        )

    return ", ".join(
        [
            f"  Parsed {run_name}: {num_samples} samples",
            f"median throughput={thr_str} ops/s",
            f"median lat={stats['median_us']['median']} us",
        ]
    )


def _build_summary_rows(runs: dict[str, dict]) -> dict[str, list[dict[str, str]]]:
    grouped_rows: dict[str, list[dict[str, str]]] = {}

    def fmt(key: str, decimals: int = 3) -> str:
        return _format_table_number((stats.get(key) or {}).get("median"), decimals)

    for run_name, entry in runs.items():
        benchmark = entry["benchmark"]
        config = entry.get("config", {})
        sweep_params = config.get("sweep_params", {})
        stats = entry.get("stats", {})

        row = {
            "run": run_name,
            "samples": str(entry.get("num_samples", 0)),
        }
        for key, value in sweep_params.items():
            row[key] = _format_sweep_value(value)

        if benchmark == "aof":
            row["throughput_mrec_s"] = fmt("throughput")
            row["bandwidth_gib_s"] = fmt("bandwidth")
            if (stats.get("reader_throughput") or {}).get("median") is not None:
                row["reader_throughput_mops_s"] = fmt("reader_throughput")
                for pct in ("p50", "p90", "p99", "p99_9"):
                    key = f"reader_lat_{pct}"
                    if (stats.get(key) or {}).get("median") is not None:
                        row[f"{key}_us"] = fmt(key)
            row["time_ms"] = fmt("time_ms")
            row["bytes"] = fmt("bytes", decimals=0)
        elif benchmark == "replication":
            row["writer_tpt_mops_s"] = fmt("writer_throughput")
            row["reader_tpt_mops_s"] = fmt("reader_throughput")
            for who in ("writer", "reader"):
                for pct in ("p50", "p99", "p99_9"):
                    key = f"{who}_lat_{pct}"
                    if (stats.get(key) or {}).get("median") is not None:
                        row[f"{key}_us"] = fmt(key)
            row["replication_lag_bytes"] = fmt("replication_lag_bytes", decimals=0)
            row["time_ms"] = fmt("time_ms")
        elif benchmark == "offline":
            row["throughput_mops_s"] = fmt("throughput")
            row["total_ops"] = fmt("total_ops", decimals=0)
            row["time_ms"] = fmt("time_ms")
        else:
            row["throughput_mops_s"] = fmt("tpt_mops")
            row["median_us"] = fmt("median_us")
            row["p95_us"] = fmt("p95_us")
            row["p99_us"] = fmt("p99_us")

        grouped_rows.setdefault(benchmark, []).append(row)

    return grouped_rows


def _render_text_table(rows: list[dict[str, str]], columns: list[str]) -> str:
    widths = {
        column: max(
            len(column),
            max((len(str(row.get(column, "-"))) for row in rows), default=0),
        )
        for column in columns
    }

    def _fmt_row(row: dict[str, str]) -> str:
        return " | ".join(
            str(row.get(column, "-")).rjust(widths[column]) for column in columns
        )

    separator = "-+-".join("-" * widths[column] for column in columns)
    header = " | ".join(column.rjust(widths[column]) for column in columns)
    body = [_fmt_row(row) for row in rows]
    return "\n".join([header, separator, *body])


def _git_summary_line(git_meta: dict | None) -> str:
    git_meta = git_meta or {}
    commit = git_meta.get("git_commit")
    if commit is None:
        return "Git: unknown"
    dirty = git_meta.get("git_dirty")
    state = "dirty" if dirty else "clean" if dirty is not None else "unknown"
    return f"Git: {git_meta.get('git_branch')} @ {commit[:12]} ({state})"


def _write_summary_file(
    exp_dir: Path,
    experiment_name: str,
    warmup: int,
    runs: dict,
    git_meta: dict | None = None,
) -> Path:
    grouped_rows = _build_summary_rows(runs)
    lines = [f"Experiment: {experiment_name}"]
    if git_meta:
        lines.append(_git_summary_line(git_meta))
    lines.append(f"Warmup rows discarded: {warmup}")

    benchmark_column_order = {
        "aof": [
            "run",
            "samples",
            "throughput_mrec_s",
            "bandwidth_gib_s",
            "time_ms",
            "bytes",
        ],
        "replication": [
            "run",
            "samples",
            "writer_tpt_mops_s",
            "reader_tpt_mops_s",
            "replication_lag_bytes",
            "time_ms",
        ],
        "offline": ["run", "samples", "throughput_mops_s", "total_ops", "time_ms"],
        "online": [
            "run",
            "samples",
            "throughput_mops_s",
            "median_us",
            "p95_us",
            "p99_us",
        ],
    }

    for benchmark in sorted(grouped_rows):
        rows = grouped_rows[benchmark]
        sweep_columns = sorted(
            {
                key
                for row in rows
                for key in row
                if key not in {"run", "samples"}
                and key not in set(benchmark_column_order.get(benchmark, []))
            }
        )
        ordered_columns = [
            "run",
            *sweep_columns,
            *benchmark_column_order.get(benchmark, ["samples"])[1:],
        ]
        lines.extend(
            [
                "",
                f"[{benchmark}]",
                _render_text_table(rows, ordered_columns),
            ]
        )

    summary_path = exp_dir / "summary.txt"
    with open(summary_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return summary_path


def _parse_run_dir(run_dir: Path, warmup: int) -> dict | None:
    """Parse a single run directory. Returns None if output.txt is missing."""
    # The client's output: benchmark/ for the classic single-client layout, client/ for
    # replication runs (each role keeps its own subdir), bare output.txt for legacy runs.
    output_path = run_dir / "benchmark" / "output.txt"
    config_path = run_dir / "config.yaml"
    if not output_path.exists():
        for candidate in (run_dir / "client" / "output.txt", run_dir / "output.txt"):
            if candidate.exists():
                output_path = candidate
                break

    if not output_path.exists():
        print(f"  [skip] {run_dir.name}: no output.txt")
        return None

    config = {}
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}

    benchmark = config.get("benchmark")
    if benchmark is None:
        raise ValueError(
            f"Config '{config_path}' is missing required field 'benchmark'"
        )
    samples, metric_columns = parse_output(
        output_path,
        benchmark=benchmark,
        warmup_rows=warmup,
        expected_op=config.get("client_params", {}).get("op"),
    )

    # Split-process AOF run: the Replica role's replay metrics land in _server.log (one
    # [Total time] block per pass) while output.txt carries the Client role's reader metrics.
    # Merge them pairwise by pass; the replica's time_ms/bytes describe the pass window, so
    # the client's are dropped.
    server_log = run_dir / "_server.log"
    if (
        benchmark == "aof"
        and server_log.exists()
        and "[Total time]" in server_log.read_text(errors="ignore")
    ):
        server_samples, server_columns = _parse_aof_output(server_log)
        if len(server_samples) != len(samples):
            raise ValueError(
                f"{run_dir.name}: replica passes ({len(server_samples)}) != "
                f"client passes ({len(samples)}) -- split-run outputs out of sync"
            )
        merged = []
        for srv, cli in zip(server_samples, samples):
            row = dict(srv)
            row.update({k: v for k, v in cli.items() if k not in ("time_ms", "bytes")})
            merged.append(row)
        samples = merged
        metric_columns = server_columns + [
            c for c in metric_columns if c not in server_columns and c not in ("time_ms", "bytes")
        ]

    repeat = int(config.get("repeat", 1) or 1)
    samples = _aggregate_repetitions(samples, metric_columns, repeat)
    stats = _summarize_samples(samples, metric_columns)
    print(_format_summary(benchmark, stats, len(samples), run_dir.name))
    return {
        "benchmark": benchmark,
        "config": config,
        "num_samples": len(samples),
        "samples": samples,
        "stats": stats,
    }


def _collect_runs(run_dirs: list, warmup: int) -> tuple[dict, dict[str, list]]:
    """Parse a list of run directories. Returns (runs, sweep_params)."""
    runs = {}
    sweep_params: dict[str, list] = {}
    for run_dir in run_dirs:
        entry = _parse_run_dir(run_dir, warmup)
        if entry is None:
            continue
        for key, value in (entry.get("config", {}).get("sweep_params", {})).items():
            values = sweep_params.setdefault(key, [])
            if value not in values:
                values.append(value)
        runs[run_dir.name] = entry
    return runs, sweep_params


def _read_meta(exp_dir: Path) -> dict:
    """Read run-time provenance (git) persisted by run.py. Empty dict if meta.yaml is
    absent -- e.g. results produced before this feature -- in which case callers skip
    the git fields entirely for backward compatibility."""
    meta_path = exp_dir / "meta.yaml"
    if not meta_path.exists():
        return {}
    with open(meta_path) as f:
        return yaml.safe_load(f) or {}


def _process_one(config: str, warmup: int) -> None:
    spec = load_experiment_spec(config_path_for(config), default_name=Path(config).stem)

    exp_dir = result_dir(spec.name)
    if not exp_dir.exists():
        raise FileNotFoundError(f"Experiment directory not found: {exp_dir}")

    run_dirs = [
        exp_dir / resolve_run_spec(spec, combo).run_name for combo in spec.combos
    ]
    if not run_dirs:
        raise ValueError(f"No run directories found in {exp_dir}")

    runs, sweep_params = _collect_runs(run_dirs, warmup)

    git_meta = _read_meta(exp_dir)

    result = {
        "experiment_name": spec.name,
        "sweep_params": sweep_params,
        "warmup_rows_discarded": warmup,
        "runs": runs,
    }
    if git_meta:
        result["git_commit"] = git_meta.get("git_commit")
        result["git_branch"] = git_meta.get("git_branch")
        result["git_dirty"] = git_meta.get("git_dirty")

    out_path = exp_dir / "result.yaml"
    with open(out_path, "w") as f:
        yaml.dump(result, f)
    summary_path = _write_summary_file(exp_dir, spec.name, warmup, runs, git_meta)
    print(f"\nResult written to: {out_path}")
    print(f"Summary written to: {summary_path}")


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Parse Garnet benchmark outputs into result.yaml"
    )
    parser.add_argument(
        "configs",
        nargs="+",
        help="One or more experiment config names (or paths). Each is processed sequentially.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=5,
        help="Number of initial samples to discard as warmup (default: 5)",
    )
    args = parser.parse_args(argv)
    for config in args.configs:
        _process_one(config, args.warmup)


if __name__ == "__main__":
    main()
