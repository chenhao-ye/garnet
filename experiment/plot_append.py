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
    apply_axis_cfg,
    build_fig_single_col,
    extract_series,
    load_plot_config,
    load_result,
    save_fig,
    save_legend,
)

DEFAULT_EXPERIMENT = "aof_enqueue_random"
X_PARAM = "client.threads"
FILTER_PARAM = "client.aof_physical_sublog_count"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "config",
        nargs="?",
        default=DEFAULT_EXPERIMENT,
        help=f"Config name (data directory under RESULT_ROOT). Default: {DEFAULT_EXPERIMENT}",
    )
    args = parser.parse_args()
    experiment = args.config

    result = load_result(experiment)
    plot_cfg = load_plot_config(experiment)

    scale = float(plot_cfg.get("scale", 1.0))
    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

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
        all_y.extend(ys)
        ax.plot(
            xs,
            ys,
            color=color_map[key],
            linestyle=linestyle_map[key],
            marker=marker_map[key],
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            label=labels_map[key],
        )

    sorted_threads = sorted(all_threads)
    y_log = plot_cfg.get("yscale") == "log"
    default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
    apply_axis_cfg(
        ax,
        plot_cfg,
        default_xlabel="#threads",
        default_ylabel="Throughput (Mop/s)",
        default_xticks=sorted_threads,
        default_ymax=default_ymax,
    )
    out_path = RESULT_ROOT / experiment / "append_scaling.pdf"
    legend_kwargs = dict(
        frameon=False,
        ncol=2,
        columnspacing=1.0,
        handlelength=1.8,
        handletextpad=0.5,
        labelspacing=0.3,
    )
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, **legend_kwargs)
    else:
        ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.0), **legend_kwargs)

    save_fig(fig, out_path)
    plt.close(fig)


if __name__ == "__main__":
    main()
