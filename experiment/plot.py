#!/usr/bin/env -S uv run

"""
Plot experiment results from result.json.

Usage:
    uv run experiment/plot.py <experiment_name>
    uv run experiment/plot.py scale_clients
    uv run experiment/plot.py scale_clients --output-dir /tmp/plots
"""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"


def load_result(experiment: str) -> dict:
    path = RESULT_ROOT / experiment / "result.json"
    if not path.exists():
        raise FileNotFoundError(f"result.json not found: {path}\n"
                                f"Run parse.py first.")
    with open(path) as f:
        return json.load(f)


def sorted_runs(result: dict):
    """Return runs sorted by sweep_value (numeric if possible, else lexicographic)."""
    runs = result["runs"]
    items = list(runs.items())
    try:
        items.sort(key=lambda kv: float(kv[1]["sweep_value"])
                   if kv[1]["sweep_value"] is not None else kv[0])
    except (TypeError, ValueError):
        items.sort(key=lambda kv: kv[0])
    return items


def _x_label(result: dict) -> str:
    param = result.get("sweep_param") or "run"
    return param.replace("_", " ")


def _use_log_scale(values: list) -> bool:
    try:
        nums = [float(v) for v in values if v is not None]
        return len(nums) >= 2 and max(nums) / min(nums) > 10
    except (TypeError, ZeroDivisionError):
        return False


def plot_throughput(result: dict, out_dir: Path) -> Path:
    items = sorted_runs(result)
    x_labels = [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0])
                for kv in items]
    x = np.arange(len(items))
    means = [kv[1]["stats"]["tpt_kops"]["mean"] or 0 for kv in items]
    stds  = [kv[1]["stats"]["tpt_kops"]["std"]  or 0 for kv in items]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x, means, yerr=stds, capsize=5, color="steelblue", alpha=0.85,
           error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_xlabel(_x_label(result))
    ax.set_ylabel("Throughput (Kops/sec)")
    ax.set_title(f"{result['experiment_name']} - Throughput")
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "throughput.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_latency(result: dict, out_dir: Path) -> Path:
    items = sorted_runs(result)
    x_labels = [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0])
                for kv in items]
    x = np.arange(len(items))

    percentiles = [
        ("median_us",  "Median",  "steelblue"),
        ("p95_us",     "p95",     "orange"),
        ("p99_us",     "p99",     "tomato"),
        ("p999_us",    "p99.9",   "purple"),
    ]

    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.18
    n = len(percentiles)
    offsets = np.linspace(-(n - 1) / 2 * width, (n - 1) / 2 * width, n)

    for (col, label, color), offset in zip(percentiles, offsets):
        means = [kv[1]["stats"][col]["mean"] or 0 for kv in items]
        stds  = [kv[1]["stats"][col]["std"]  or 0 for kv in items]
        ax.bar(x + offset, means, width=width, yerr=stds, capsize=3,
               label=label, color=color, alpha=0.85,
               error_kw={"elinewidth": 1.0, "ecolor": "black"})

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_xlabel(_x_label(result))
    ax.set_ylabel("Latency (us)")
    ax.set_title(f"{result['experiment_name']} - Latency Percentiles")
    ax.legend()
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "latency.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_latency_line(result: dict, out_dir: Path) -> Path:
    """Line chart of latency percentiles vs sweep param (good for many x values)."""
    items = sorted_runs(result)
    x_vals = [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else i
              for i, kv in enumerate(items)]
    x_labels = [str(v) for v in x_vals]

    percentiles = [
        ("median_us",  "Median",  "steelblue",   "o"),
        ("p95_us",     "p95",     "orange",       "s"),
        ("p99_us",     "p99",     "tomato",       "^"),
        ("p999_us",    "p99.9",   "purple",       "D"),
    ]

    use_log = _use_log_scale(x_vals)
    x = np.array([float(v) for v in x_vals]) if use_log else np.arange(len(items))

    fig, ax = plt.subplots(figsize=(8, 5))
    for col, label, color, marker in percentiles:
        means = np.array([kv[1]["stats"][col]["mean"] or 0 for kv in items])
        stds  = np.array([kv[1]["stats"][col]["std"]  or 0 for kv in items])
        ax.plot(x, means, marker=marker, label=label, color=color)
        ax.fill_between(x, means - stds, means + stds, alpha=0.15, color=color)

    if use_log:
        ax.set_xscale("log")
        ax.set_xlabel(_x_label(result))
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel(_x_label(result))

    ax.set_ylabel("Latency (us)")
    ax.set_title(f"{result['experiment_name']} - Latency Percentiles")
    ax.legend()
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "latency_line.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_throughput_line(result: dict, out_dir: Path) -> Path:
    """Line chart of throughput vs sweep param."""
    items = sorted_runs(result)
    x_vals = [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else i
              for i, kv in enumerate(items)]
    x_labels = [str(v) for v in x_vals]
    means = np.array([kv[1]["stats"]["tpt_kops"]["mean"] or 0 for kv in items])
    stds  = np.array([kv[1]["stats"]["tpt_kops"]["std"]  or 0 for kv in items])

    use_log = _use_log_scale(x_vals)
    x = np.array([float(v) for v in x_vals]) if use_log else np.arange(len(items))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(x, means, marker="o", color="steelblue", label="mean tpt")
    ax.fill_between(x, means - stds, means + stds, alpha=0.2, color="steelblue")

    if use_log:
        ax.set_xscale("log")
        ax.set_xlabel(_x_label(result))
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel(_x_label(result))

    ax.set_ylabel("Throughput (Kops/sec)")
    ax.set_title(f"{result['experiment_name']} - Throughput")
    ax.legend()
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "throughput_line.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Plot Garnet experiment results from result.json")
    parser.add_argument("experiment", help="Experiment name (subdirectory of result/)")
    parser.add_argument("--output-dir",
                        help="Directory to write plots (default: result/<exp>/plots)")
    args = parser.parse_args()

    result = load_result(args.experiment)

    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = RESULT_ROOT / args.experiment / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    n_runs = len(result["runs"])
    print(f"Plotting {args.experiment} ({n_runs} runs) -> {out_dir}")

    # Use bar charts for small sweep sizes, line charts for large ones
    if n_runs <= 8:
        plot_throughput(result, out_dir)
        plot_latency(result, out_dir)
    plot_throughput_line(result, out_dir)
    plot_latency_line(result, out_dir)

    print("Done.")


if __name__ == "__main__":
    main()
