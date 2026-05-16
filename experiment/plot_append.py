#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "matplotlib",
#   "numpy",
#   "pyyaml",
# ]
# ///
"""Append scalability figure (evaluation plan v3, sec:3.1, sec:6.2).

Throughput vs. primary worker threads, one curve per
aof_physical_sublog_count. Proves claim C1 (R1a).
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from plot_style import (
    APPEND_M_VALUES,
    LINEWIDTH,
    MARKER_SIZE,
    color_map,
    labels_map,
    linestyle_map,
    marker_map,
)
from plot_util import (
    RESULT_ROOT,
    build_fig_single_col,
    extract_series,
    load_plot_config,
    load_result,
    save_fig,
    to_mrecords,
)

DEFAULT_EXPERIMENT = "aof_enqueue_random"
X_PARAM = "client.threads"
FILTER_PARAM = "client.aof_physical_sublog_count"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=DEFAULT_EXPERIMENT,
        help=(
            "Config name (data directory under RESULT_ROOT)."
            f" Default: {DEFAULT_EXPERIMENT}"
        ),
    )
    args = parser.parse_args()
    experiment = args.config

    result = load_result(experiment)
    plot_cfg = load_plot_config(experiment)

    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75)

    all_threads: set[float] = set()
    all_y: list[float] = []
    for m in APPEND_M_VALUES:
        key = f"multilog_m{m}"
        xs, ys, _ = extract_series(
            result,
            x_param=X_PARAM,
            filter_params={FILTER_PARAM: m},
        )
        if not xs:
            print(f"WARN: no data for {FILTER_PARAM}={m}", file=sys.stderr)
            continue
        all_threads.update(xs)
        ys_mrec = to_mrecords(ys)
        all_y.extend(ys_mrec)
        ax.plot(
            xs,
            ys_mrec,
            color=color_map[key],
            linestyle=linestyle_map[key],
            marker=marker_map[key],
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            label=labels_map[key],
        )

    sorted_threads = sorted(all_threads)
    y_min = min(y for y in all_y if y > 0)
    y_max = max(all_y)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(sorted_threads)
    ax.set_xticklabels([str(int(t)) for t in sorted_threads])
    ax.set_xlim(sorted_threads[0] / 1.2, sorted_threads[-1] * 1.2)
    y_ticks = plot_cfg.get("y_ticks")
    if y_ticks:
        ax.set_yticks(y_ticks)
        ax.set_yticklabels([str(t) for t in y_ticks])
    y_lo = plot_cfg.get("y_min", y_min / 1.5)
    y_hi = plot_cfg.get("y_max", y_max * 1.5)
    ax.set_ylim(y_lo, y_hi)
    ax.set_xlabel("Primary worker threads")
    ax.set_ylabel("Append throughput (Mrec/s)")
    ax.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax.set_axisbelow(True)
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(0.0, 1.0),
        frameon=False,
        ncol=2,
        columnspacing=1.0,
        handlelength=1.8,
        handletextpad=0.5,
        labelspacing=0.3,
    )

    out_path = RESULT_ROOT / experiment / "plots" / "append_scaling.pdf"
    save_fig(fig, out_path)
    plt.close(fig)


if __name__ == "__main__":
    main()
