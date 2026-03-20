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
import yaml
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"

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

AOF_BASE_COLUMNS = [
    "time_ms",
    "bytes",
    "bandwidth",
    "throughput",
]

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


def _detect_benchmark(config: dict) -> str:
    params = config.get("params", {}) or {}
    if params.get("aof_bench"):
        return "aof_bench"
    return "online"


def _parse_online_output(path: Path, warmup_rows: int = 2) -> tuple[list[dict], list[str]]:
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
                    if bytes_match is not None else None
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


def parse_output(path: Path, benchmark: str, warmup_rows: int = 2) -> tuple[list[dict], list[str]]:
    """Return parsed samples and the benchmark-specific metric columns."""
    if benchmark == "aof_bench":
        return _parse_aof_output(path)
    if benchmark == "online":
        return _parse_online_output(path, warmup_rows=warmup_rows)
    raise ValueError(f"Unsupported benchmark type: {benchmark}")


def _format_summary(benchmark: str, stats: dict, num_samples: int, run_name: str) -> str:
    if benchmark == "aof_bench":
        return ", ".join([
            f"  Parsed {run_name}: {num_samples} samples",
            f"mean throughput={stats['throughput']['mean']} Krecords/s",
            f"bandwidth={stats['bandwidth']['mean']} GiB/s",
        ])

    return ", ".join([
        f"  Parsed {run_name}: {num_samples} samples",
        f"mean tpt={stats['tpt_kops']['mean']} Kops/s",
        f"median lat={stats['median_us']['mean']} us",
    ])


def _parse_run_dir(run_dir: Path, warmup: int) -> dict | None:
    """Parse a single run directory. Returns None if output.txt is missing."""
    output_path = run_dir / "output.txt"
    config_path = run_dir / "config.yaml"

    if not output_path.exists():
        print(f"  [skip] {run_dir.name}: no output.txt")
        return None

    config = {}
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}

    benchmark = _detect_benchmark(config)
    samples, metric_columns = parse_output(output_path, benchmark=benchmark, warmup_rows=warmup)
    stats = _summarize_samples(samples, metric_columns)
    print(_format_summary(benchmark, stats, len(samples), run_dir.name))
    return {
        "benchmark": benchmark,
        "config": config,
        "metric_columns": metric_columns,
        "sweep_value": config.get("sweep_value"),
        "num_samples": len(samples),
        "samples": samples,
        "stats": stats,
    }


def _collect_runs(run_dirs: list, warmup: int) -> tuple[dict, str | None, list]:
    """Parse a list of run directories. Returns (runs, sweep_param, sweep_values)."""
    runs = {}
    sweep_param = None
    sweep_values = []
    for run_dir in run_dirs:
        entry = _parse_run_dir(run_dir, warmup)
        if entry is None:
            continue
        if sweep_param is None and "sweep_param" in entry["config"]:
            sweep_param = entry["config"]["sweep_param"]
        if entry["sweep_value"] is not None:
            sweep_values.append(entry["sweep_value"])
        runs[run_dir.name] = entry
    return runs, sweep_param, sweep_values


def main():
    parser = argparse.ArgumentParser(
        description="Parse Garnet benchmark outputs into result.json")
    parser.add_argument("experiment", help="Experiment name (subdirectory of result/)")
    parser.add_argument("--warmup", type=int, default=2,
                        help="Number of initial samples to discard as warmup (default: 2)")
    args = parser.parse_args()

    exp_dir = RESULT_ROOT / args.experiment
    if not exp_dir.exists():
        print(f"Error: experiment directory not found: {exp_dir}")
        raise SystemExit(1)

    # Detect whether this experiment used setup dirs by reading the top-level config.
    exp_config = {}
    exp_config_path = exp_dir / "config.yaml"
    if exp_config_path.exists():
        with open(exp_config_path) as f:
            exp_config = yaml.safe_load(f) or {}

    has_setup = bool(exp_config.get("setup"))

    if has_setup:
        setup_cfg = exp_config["setup"]
        setup_param = setup_cfg["param"]
        setup_values = setup_cfg["values"]

        # Each setup is a subdirectory named <param>_<value>
        setup_dirs = sorted(
            d for d in exp_dir.iterdir()
            if d.is_dir() and d.name not in ("_load",)
            and not (d / "output.txt").exists()  # skip plain run dirs if any
        )
        if not setup_dirs:
            print(f"Error: no setup directories found in {exp_dir}")
            raise SystemExit(1)

        setups = {}
        sweep_param = None
        sweep_values_found: list = []

        for setup_dir in setup_dirs:
            print(f"\n[setup] {setup_dir.name}")
            run_dirs = sorted(
                d for d in setup_dir.iterdir()
                if d.is_dir() and d.name != "_load"
            )
            runs, sp, svs = _collect_runs(run_dirs, args.warmup)
            if sweep_param is None and sp is not None:
                sweep_param = sp
            for sv in svs:
                if sv not in sweep_values_found:
                    sweep_values_found.append(sv)

            # Recover setup_value from the first run's config, or infer from dir name
            setup_value = None
            for entry in runs.values():
                setup_value = entry["config"].get("setup_value")
                if setup_value is not None:
                    break

            setups[setup_dir.name] = {
                "setup_value": setup_value,
                "runs": runs,
            }

        result = {
            "experiment_name": args.experiment,
            "setup_param": setup_param,
            "setup_values": setup_values,
            "sweep_param": sweep_param,
            "sweep_values": sweep_values_found,
            "warmup_rows_discarded": args.warmup,
            "setups": setups,
        }
    else:
        run_dirs = sorted(d for d in exp_dir.iterdir() if d.is_dir() and d.name != "_load")
        if not run_dirs:
            print(f"Error: no run directories found in {exp_dir}")
            raise SystemExit(1)

        runs, sweep_param, sweep_values = _collect_runs(run_dirs, args.warmup)

        result = {
            "experiment_name": args.experiment,
            "sweep_param": sweep_param,
            "sweep_values": sweep_values,
            "warmup_rows_discarded": args.warmup,
            "runs": runs,
        }

    out_path = exp_dir / "result.yaml"
    with open(out_path, "w") as f:
        yaml.dump(result, f)
    print(f"\nResult written to: {out_path}")


if __name__ == "__main__":
    main()
