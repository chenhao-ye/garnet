#!/usr/bin/env -S uv run

"""
Parse benchmark output files and produce a summary result.json per experiment.

Usage:
    uv run experiment/parse.py <experiment_name>
    uv run experiment/parse.py scale_clients
    uv run experiment/parse.py scale_clients --warmup 3
"""

import argparse
import json
import math
import yaml
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"

# Columns emitted by RespOnlineBench in order
COLUMNS = [
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

# Regex that matches the stats header line
HEADER_RE = re.compile(r"min\s*\(us\)")


def _is_data_row(parts: list[str]) -> bool:
    if len(parts) != len(COLUMNS):
        return False
    try:
        [float(p) for p in parts]
        return True
    except ValueError:
        return False


def parse_output(path: Path, warmup_rows: int = 2) -> list[dict]:
    """Return list of sample dicts parsed from a benchmark output file."""
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
                row = {col: float(v) for col, v in zip(COLUMNS, parts)}
                samples.append(row)

    # Skip warmup rows at the front
    return samples[warmup_rows:]


def _stats(values: list[float]) -> dict:
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


def summarize_samples(samples: list[dict]) -> dict:
    """Compute per-column statistics across samples."""
    if not samples:
        return {col: _stats([]) for col in COLUMNS}
    return {col: _stats([s[col] for s in samples]) for col in COLUMNS}


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

    run_dirs = sorted(d for d in exp_dir.iterdir() if d.is_dir())
    if not run_dirs:
        print(f"Error: no run directories found in {exp_dir}")
        raise SystemExit(1)

    runs = {}
    sweep_param = None
    sweep_values = []

    for run_dir in run_dirs:
        output_path = run_dir / "output.txt"
        config_path = run_dir / "config.yaml"

        if not output_path.exists():
            print(f"  [skip] {run_dir.name}: no output.txt")
            continue

        config = {}
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f)

        samples = parse_output(output_path, warmup_rows=args.warmup)
        stats = summarize_samples(samples)

        # Extract sweep metadata from config if present
        if sweep_param is None and "sweep_param" in config:
            sweep_param = config["sweep_param"]
        sweep_value = config.get("sweep_value")
        if sweep_value is not None:
            sweep_values.append(sweep_value)

        runs[run_dir.name] = {
            "config": config,
            "sweep_value": sweep_value,
            "num_samples": len(samples),
            "samples": samples,
            "stats": stats,
        }
        print(f"  Parsed {run_dir.name}: {len(samples)} samples, "
              f"mean tpt={stats['tpt_kops']['mean']} Kops/s, "
              f"median lat={stats['median_us']['mean']} us")

    result = {
        "experiment_name": args.experiment,
        "sweep_param": sweep_param,
        "sweep_values": sweep_values,
        "warmup_rows_discarded": args.warmup,
        "runs": runs,
    }

    out_path = exp_dir / "result.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nResult written to: {out_path}")


if __name__ == "__main__":
    main()
