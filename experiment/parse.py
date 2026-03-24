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
from pathlib import Path

import yaml
from config import RESULT_ROOT, expected_run_dirs

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
    "tpt_kops",
]

AOF_BASE_COLUMNS = ["time_ms", "bytes", "bandwidth", "throughput"]

AOF_METRIC_NAME_MAP = {
    "Bandwidth": "bandwidth",
    "Total pages send": "pages",
    "Total records replayed": "records",
    "Total records enqueued": "records",
}

HEADER_RE = re.compile(r"min\s*\(us\)")
AOF_METRIC_RE = re.compile(r"^\[(?P<name>[^\]]+)\]:\s*(?P<value>.+)$")
AOF_NUMBER_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")


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
        return {"mean": None, "std": None, "min": None, "max": None}
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n if n > 1 else 0.0
    return {
        "mean": round(mean, 4),
        "std": round(math.sqrt(variance), 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
    }


def _summarize_samples(samples: list[dict], columns: list[str]) -> dict:
    if not samples:
        return {col: _stats([]) for col in columns}
    return {col: _stats([sample.get(col) for sample in samples]) for col in columns}


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
                samples.append(row)

    return samples[warmup_rows:], ONLINE_COLUMNS


def _parse_aof_output(path: Path) -> tuple[list[dict], list[str]]:
    """Parse the labeled summary format emitted by AofBench."""
    samples = []
    columns: list[str] = [*AOF_BASE_COLUMNS]
    current = None

    with open(path) as f:
        for raw_line in f:
            line = raw_line.strip()
            match = AOF_METRIC_RE.match(line)
            if not match:
                continue

            name = match.group("name")
            value = match.group("value")

            if name == "Total time":
                if current and current.get("throughput") is not None:
                    samples.append(current)
                current = {}
                current["time_ms"] = _parse_number(value)
                current["bytes"] = None

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
                if metric_key == "throughput":
                    current["throughput"] = (
                        None if metric_value is None else metric_value / 1000.0
                    )
                else:
                    current[metric_key] = metric_value

            if current is not None:
                for key in current:
                    if key not in columns:
                        columns.append(key)

    if current:
        if current.get("throughput") is not None:
            samples.append(current)
    return samples, columns


def parse_output(
    path: Path, benchmark: str, warmup_rows: int = 2
) -> tuple[list[dict], list[str]]:
    """Return parsed samples and the benchmark-specific metric columns."""
    if benchmark == "aof":
        return _parse_aof_output(path)
    if benchmark == "online":
        return _parse_online_output(path, warmup_rows=warmup_rows)
    raise ValueError(f"Unsupported benchmark: {benchmark}")


def _format_summary(
    benchmark: str, stats: dict, num_samples: int, run_name: str
) -> str:
    if benchmark == "aof":
        return ", ".join(
            [
                f"  Parsed {run_name}: {num_samples} samples",
                f"mean throughput={stats['throughput']['mean']} Krecords/s",
                f"bandwidth={stats['bandwidth']['mean']} GiB/s",
            ]
        )

    return ", ".join(
        [
            f"  Parsed {run_name}: {num_samples} samples",
            f"mean tpt={stats['tpt_kops']['mean']} Kops/s",
            f"median lat={stats['median_us']['mean']} us",
        ]
    )


def _parse_run_dir(run_dir: Path, warmup: int) -> dict | None:
    """Parse a single run directory. Returns None if output.txt is missing."""
    output_path = run_dir / "benchmark" / "output.txt"
    legacy_output_path = run_dir / "output.txt"
    config_path = run_dir / "config.yaml"

    if not output_path.exists() and legacy_output_path.exists():
        output_path = legacy_output_path

    if not output_path.exists():
        print(f"  [skip] {run_dir.name}: no output.txt")
        return None

    config = {}
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}

    benchmark = config.get("benchmark")
    if benchmark is None:
        raise ValueError(f"Config '{config_path}' is missing required field 'benchmark'")
    samples, metric_columns = parse_output(
        output_path, benchmark=benchmark, warmup_rows=warmup
    )
    stats = _summarize_samples(samples, metric_columns)
    print(_format_summary(benchmark, stats, len(samples), run_dir.name))
    return {
        "benchmark": benchmark,
        "config": config,
        "metric_columns": metric_columns,
        "sweep_params": config.get("sweep_params", {}),
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
        for key, value in entry["sweep_params"].items():
            values = sweep_params.setdefault(key, [])
            if value not in values:
                values.append(value)
        runs[run_dir.name] = entry
    return runs, sweep_params


def main():
    parser = argparse.ArgumentParser(
        description="Parse Garnet benchmark outputs into result.yaml"
    )
    parser.add_argument("experiment", help="Experiment name (subdirectory of result/)")
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Number of initial samples to discard as warmup (default: 2)",
    )
    args = parser.parse_args()

    exp_dir = RESULT_ROOT / args.experiment
    if not exp_dir.exists():
        raise FileNotFoundError(f"Experiment directory not found: {exp_dir}")

    run_dirs = expected_run_dirs(exp_dir)
    if not run_dirs:
        raise ValueError(f"No run directories found in {exp_dir}")

    runs, sweep_params = _collect_runs(run_dirs, args.warmup)

    result = {
        "experiment_name": args.experiment,
        "sweep_params": sweep_params,
        "warmup_rows_discarded": args.warmup,
        "runs": runs,
    }

    out_path = exp_dir / "result.yaml"
    with open(out_path, "w") as f:
        yaml.dump(result, f)
    print(f"\nResult written to: {out_path}")


if __name__ == "__main__":
    main()
