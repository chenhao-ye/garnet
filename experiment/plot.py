#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "matplotlib",
#   "numpy",
#   "pyyaml",
# ]
# ///
"""
Plot experiment results from result.yaml.

Usage:
    uv run experiment/plot.py <experiment_name>
    uv run experiment/plot.py scale_clients
    uv run experiment/plot.py scale_clients --output-dir /tmp/plots
"""

import argparse
import yaml
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"

AOF_METRICS = [
    ("bandwidth", "Bandwidth", "GiB/s", "steelblue"),
    ("records", "Records", "count", "tomato"),
    ("pages", "Pages", "count", "seagreen"),
    ("bytes", "Bytes", "bytes", "darkorange"),
]


def load_result(experiment: str) -> dict:
    path = RESULT_ROOT / experiment / "result.yaml"
    if not path.exists():
        raise FileNotFoundError(f"result.yaml not found: {path}\n"
                                f"Run parse.py first.")
    with open(path) as f:
        return yaml.safe_load(f)


def load_exp_config(experiment: str) -> dict:
    path = RESULT_ROOT / experiment / "config.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def sorted_runs(result: dict, sweep_values: list | None = None):
    """Return runs ordered by sweep_values list, or numerically/lexicographically."""
    runs = result["runs"]
    items = list(runs.items())
    if sweep_values is not None:
        order = {str(v): i for i, v in enumerate(sweep_values)}
        items.sort(key=lambda kv: order.get(str(kv[1]["sweep_value"]), len(order)))
    else:
        try:
            items.sort(key=lambda kv: float(kv[1]["sweep_value"])
                       if kv[1]["sweep_value"] is not None else kv[0])
        except (TypeError, ValueError):
            items.sort(key=lambda kv: kv[0])
    return items


def _x_label(sweep: dict) -> str:
    param = sweep.get("param") or "run"
    return param.replace("_", " ")


def _use_log_scale(values: list) -> bool:
    try:
        nums = [float(v) for v in values if v is not None]
        return len(nums) >= 2 and max(nums) / min(nums) > 10
    except (TypeError, ZeroDivisionError):
        return False


def _first_run_entry(result: dict) -> dict | None:
    if "runs" in result and result["runs"]:
        return next(iter(result["runs"].values()))
    if "setups" in result:
        for setup in result["setups"].values():
            if setup.get("runs"):
                return next(iter(setup["runs"].values()))
    return None


def _benchmark_type(result: dict) -> str:
    entry = _first_run_entry(result)
    return entry.get("benchmark", "online") if entry else "online"


def _metric_stat(entry: dict, metric: str, field: str = "mean") -> float:
    return entry["stats"].get(metric, {}).get(field) or 0


def plot_throughput(result: dict, sweep: dict, out_dir: Path) -> Path:
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_labels = [str(v) for v in sweep_values] if sweep_values else \
               [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0]) for kv in items]
    x = np.arange(len(items))
    means = [kv[1]["stats"]["tpt_kops"]["mean"] or 0 for kv in items]
    stds  = [kv[1]["stats"]["tpt_kops"]["std"]  or 0 for kv in items]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x, means, yerr=stds, capsize=5, color="steelblue", alpha=0.85,
           error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_xlabel(_x_label(sweep))
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


def plot_aof_throughput(result: dict, sweep: dict, out_dir: Path) -> Path:
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_labels = [str(v) for v in sweep_values] if sweep_values else \
               [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0]) for kv in items]
    x = np.arange(len(items))
    means = [_metric_stat(kv[1], "throughput") for kv in items]
    stds = [_metric_stat(kv[1], "throughput", "std") for kv in items]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x, means, yerr=stds, capsize=5, color="steelblue", alpha=0.85,
           error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_xlabel(_x_label(sweep))
    ax.set_ylabel("Throughput (Krecords/s)")
    ax.set_title(f"{result['experiment_name']} - AOF Throughput")
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "throughput.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_latency(result: dict, sweep: dict, out_dir: Path) -> Path:
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_labels = [str(v) for v in sweep_values] if sweep_values else \
               [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0]) for kv in items]
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
    ax.set_xlabel(_x_label(sweep))
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


def plot_aof_metrics(result: dict, sweep: dict, out_dir: Path) -> Path:
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_labels = [str(v) for v in sweep_values] if sweep_values else \
               [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0]) for kv in items]
    x = np.arange(len(items))

    fig, axes = plt.subplots(1, len(AOF_METRICS), figsize=(5 * len(AOF_METRICS), 5))
    if len(AOF_METRICS) == 1:
        axes = [axes]

    for ax, (metric, title, unit, color) in zip(axes, AOF_METRICS):
        means = [_metric_stat(kv[1], metric) for kv in items]
        stds = [_metric_stat(kv[1], metric, "std") for kv in items]
        ax.bar(x, means, yerr=stds, capsize=4, color=color, alpha=0.85,
               error_kw={"elinewidth": 1.0, "ecolor": "black"})
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel(_x_label(sweep))
        ax.set_ylabel(unit)
        ax.set_title(title)
        ax.yaxis.grid(True, linestyle="--", alpha=0.6)
        ax.set_axisbelow(True)

    fig.suptitle(f"{result['experiment_name']} - AOF Metrics")
    fig.tight_layout()

    out_path = out_dir / "aof_metrics.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_latency_line(result: dict, sweep: dict, out_dir: Path) -> Path:
    """Line chart of latency percentiles vs sweep param (good for many x values)."""
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_vals = sweep_values if sweep_values else \
             [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else i
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
        ax.set_xlabel(_x_label(sweep))
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel(_x_label(sweep))

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


def plot_aof_throughput_line(result: dict, sweep: dict, out_dir: Path) -> Path:
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_vals = sweep_values if sweep_values else \
             [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else i
              for i, kv in enumerate(items)]
    x_labels = [str(v) for v in x_vals]
    means = np.array([_metric_stat(kv[1], "throughput") for kv in items])
    stds = np.array([_metric_stat(kv[1], "throughput", "std") for kv in items])

    use_log = _use_log_scale(x_vals)
    x = np.array([float(v) for v in x_vals]) if use_log else np.arange(len(items))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(x, means, marker="o", color="steelblue", label="throughput")
    ax.fill_between(x, means - stds, means + stds, alpha=0.2, color="steelblue")

    if use_log:
        ax.set_xscale("log")
        ax.set_xlabel(_x_label(sweep))
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel(_x_label(sweep))

    ax.set_ylabel("Throughput (Krecords/s)")
    ax.set_title(f"{result['experiment_name']} - AOF Throughput")
    ax.legend()
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "throughput_line.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_throughput_line(result: dict, sweep: dict, out_dir: Path) -> Path:
    """Line chart of throughput vs sweep param."""
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_vals = sweep_values if sweep_values else \
             [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else i
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
        ax.set_xlabel(_x_label(sweep))
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel(_x_label(sweep))

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


def plot_aof_metrics_line(result: dict, sweep: dict, out_dir: Path) -> Path:
    sweep_values = sweep.get("values")
    items = sorted_runs(result, sweep_values)
    x_vals = sweep_values if sweep_values else \
             [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else i
              for i, kv in enumerate(items)]
    x_labels = [str(v) for v in x_vals]
    use_log = _use_log_scale(x_vals)
    x = np.array([float(v) for v in x_vals]) if use_log else np.arange(len(items))

    fig, axes = plt.subplots(1, len(AOF_METRICS), figsize=(5 * len(AOF_METRICS), 5))
    if len(AOF_METRICS) == 1:
        axes = [axes]

    for ax, (metric, title, unit, color) in zip(axes, AOF_METRICS):
        means = np.array([_metric_stat(kv[1], metric) for kv in items])
        stds = np.array([_metric_stat(kv[1], metric, "std") for kv in items])
        ax.plot(x, means, marker="o", color=color)
        ax.fill_between(x, means - stds, means + stds, alpha=0.15, color=color)
        if use_log:
            ax.set_xscale("log")
            ax.set_xlabel(_x_label(sweep))
        else:
            ax.set_xticks(x)
            ax.set_xticklabels(x_labels)
            ax.set_xlabel(_x_label(sweep))
        ax.set_ylabel(unit)
        ax.set_title(title)
        ax.yaxis.grid(True, linestyle="--", alpha=0.6)
        ax.set_axisbelow(True)

    fig.suptitle(f"{result['experiment_name']} - AOF Metrics")
    fig.tight_layout()

    out_path = out_dir / "aof_metrics_line.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


SETUP_COLORS = [
    "steelblue", "tomato", "green", "orange", "purple", "brown", "deeppink", "teal"
]


def plot_throughput_setups(result: dict, sweep: dict, out_dir: Path) -> Path:
    """Line chart: one throughput curve per setup."""
    sweep_values = sweep.get("values")
    setups = result["setups"]

    use_log = False
    x = np.array([])
    x_vals: list = []

    fig, ax = plt.subplots(figsize=(8, 5))
    for i, (setup_name, setup_data) in enumerate(setups.items()):
        items = sorted_runs(setup_data["runs"], sweep_values)
        x_vals = sweep_values if sweep_values else \
                 [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else j
                  for j, kv in enumerate(items)]
        use_log = _use_log_scale(x_vals)
        x = np.array([float(v) for v in x_vals]) if use_log else np.arange(len(items))
        means = np.array([kv[1]["stats"]["tpt_kops"]["mean"] or 0 for kv in items])
        stds  = np.array([kv[1]["stats"]["tpt_kops"]["std"]  or 0 for kv in items])
        color = SETUP_COLORS[i % len(SETUP_COLORS)]
        label = str(setup_data.get("setup_value", setup_name))
        ax.plot(x, means, marker="o", color=color, label=label)
        ax.fill_between(x, means - stds, means + stds, alpha=0.15, color=color)

    if use_log:
        ax.set_xscale("log")
        ax.set_xlabel(_x_label(sweep))
    else:
        ax.set_xticks(x)
        ax.set_xticklabels([str(v) for v in x_vals])
        ax.set_xlabel(_x_label(sweep))

    setup_param = result.get("setup_param", "setup")
    ax.set_ylabel("Throughput (Kops/sec)")
    ax.set_title(f"{result['experiment_name']} - Throughput by {setup_param}")
    ax.legend(title=setup_param.replace("_", " "))
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "throughput_setups.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_latency_setups(result: dict, sweep: dict, out_dir: Path) -> Path:
    """Line chart: one subplot per percentile, one curve per setup."""
    sweep_values = sweep.get("values")
    setups = result["setups"]

    percentiles = [
        ("median_us", "Median"),
        ("p99_us",    "p99"),
        ("p999_us",   "p99.9"),
    ]

    fig, axes = plt.subplots(1, len(percentiles), figsize=(5 * len(percentiles), 5))
    if len(percentiles) == 1:
        axes = [axes]

    use_log = False
    x = np.array([])
    x_vals: list = []

    for i, (setup_name, setup_data) in enumerate(setups.items()):
        items = sorted_runs(setup_data["runs"], sweep_values)
        x_vals = sweep_values if sweep_values else \
                 [kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else j
                  for j, kv in enumerate(items)]
        use_log = _use_log_scale(x_vals)
        x = np.array([float(v) for v in x_vals]) if use_log else np.arange(len(items))
        color = SETUP_COLORS[i % len(SETUP_COLORS)]
        label = str(setup_data.get("setup_value", setup_name))

        for ax, (col, _) in zip(axes, percentiles):
            means = np.array([kv[1]["stats"][col]["mean"] or 0 for kv in items])
            stds  = np.array([kv[1]["stats"][col]["std"]  or 0 for kv in items])
            ax.plot(x, means, marker="o", color=color, label=label)
            ax.fill_between(x, means - stds, means + stds, alpha=0.15, color=color)

    for ax, (_, title) in zip(axes, percentiles):
        if use_log:
            ax.set_xscale("log")
            ax.set_xlabel(_x_label(sweep))
        else:
            ax.set_xticks(x)
            ax.set_xticklabels([str(v) for v in x_vals])
            ax.set_xlabel(_x_label(sweep))
        ax.set_ylabel("Latency (us)")
        ax.set_title(title)
        ax.yaxis.grid(True, linestyle="--", alpha=0.6)
        ax.set_axisbelow(True)

    setup_param = result.get("setup_param", "setup")
    axes[0].legend(title=setup_param.replace("_", " "))
    fig.suptitle(f"{result['experiment_name']} - Latency by {setup_param}")
    fig.tight_layout()

    out_path = out_dir / "latency_setups.pdf"
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def plot_throughput_bar_setups(result: dict, sweep: dict, out_dir: Path) -> Path:
    """Grouped bar chart: groups = sweep values, bars = setups."""
    sweep_values = sweep.get("values")
    setups = result["setups"]
    setup_items = list(setups.items())

    first_runs = sorted_runs(list(setups.values())[0]["runs"], sweep_values)
    x_labels = [str(v) for v in sweep_values] if sweep_values else \
               [str(kv[1]["sweep_value"] if kv[1]["sweep_value"] is not None else kv[0])
                for kv in first_runs]
    x = np.arange(len(x_labels))
    n = len(setup_items)
    width = 0.8 / n
    offsets = np.linspace(-(n - 1) / 2 * width, (n - 1) / 2 * width, n)

    fig, ax = plt.subplots(figsize=(max(8, len(x_labels) * 1.2), 5))
    for i, (setup_name, setup_data) in enumerate(setup_items):
        items = sorted_runs(setup_data["runs"], sweep_values)
        means = [kv[1]["stats"]["tpt_kops"]["mean"] or 0 for kv in items]
        stds  = [kv[1]["stats"]["tpt_kops"]["std"]  or 0 for kv in items]
        color = SETUP_COLORS[i % len(SETUP_COLORS)]
        label = str(setup_data.get("setup_value", setup_name))
        ax.bar(x + offsets[i], means, width=width, yerr=stds, capsize=4,
               color=color, alpha=0.85, label=label,
               error_kw={"elinewidth": 1.2, "ecolor": "black"})

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_xlabel(_x_label(sweep))
    ax.set_ylabel("Throughput (Kops/sec)")
    setup_param = result.get("setup_param", "setup")
    ax.set_title(f"{result['experiment_name']} - Throughput by {setup_param}")
    ax.legend(title=setup_param.replace("_", " "))
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()

    out_path = out_dir / "throughput_bar_setups.pdf"
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
    exp_cfg = load_exp_config(args.experiment)
    sweep = exp_cfg.get("sweep", {})

    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = RESULT_ROOT / args.experiment / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    if "setups" in result:
        n_setups = len(result["setups"])
        n_runs = max(len(s["runs"]) for s in result["setups"].values()) if n_setups else 0
        print(f"Plotting {args.experiment} ({n_setups} setups x {n_runs} runs each) -> {out_dir}")
        plot_throughput_setups(result, sweep, out_dir)
        plot_latency_setups(result, sweep, out_dir)
        if n_runs <= 8:
            plot_throughput_bar_setups(result, sweep, out_dir)
    else:
        n_runs = len(result["runs"])
        benchmark = _benchmark_type(result)
        print(f"Plotting {args.experiment} ({n_runs} runs) -> {out_dir}")
        if benchmark == "aof_bench":
            if n_runs <= 8:
                plot_aof_throughput(result, sweep, out_dir)
                plot_aof_metrics(result, sweep, out_dir)
            plot_aof_throughput_line(result, sweep, out_dir)
            plot_aof_metrics_line(result, sweep, out_dir)
        else:
            # Use bar charts for small sweep sizes, line charts for large ones
            if n_runs <= 8:
                plot_throughput(result, sweep, out_dir)
                plot_latency(result, sweep, out_dir)
            plot_throughput_line(result, sweep, out_dir)
            plot_latency_line(result, sweep, out_dir)

    print("Done.")


if __name__ == "__main__":
    main()
