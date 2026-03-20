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
            config = yaml.safe_load(f)

    samples = parse_output(output_path, warmup_rows=warmup)
    stats = summarize_samples(samples)
    print(f"  Parsed {run_dir.name}: {len(samples)} samples, "
          f"mean tpt={stats['tpt_kops']['mean']} Kops/s, "
          f"median lat={stats['median_us']['mean']} us")
    return {
        "config": config,
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
