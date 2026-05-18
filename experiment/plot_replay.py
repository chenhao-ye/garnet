#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "matplotlib",
#   "numpy",
#   "pyyaml",
# ]
# ///
"""Replay scalability figure (evaluation plan v3, sec:3.2, sec:6.3).

Replay throughput vs. number of sublogs. Three curves:
  - Single Log: horizontal dashed reference (m=1 from the physical config).
  - MultiLog-virtual: m=1, n varies (virtual config sweep).
  - MultiLog-physical: n=1, m varies (physical config sweep).
Proves claim C2 (R1b). NoPrefix curve omitted: data not yet available.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from plot_style import (
    LEGEND_KWARGS,
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
    require_results_ready,
    resolve_dependencies,
    row_major_handles,
    save_fig,
    save_legend,
)

DEFAULT_PLOT_CONFIG = "replay_scaling"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "plot_config",
        nargs="?",
        default=DEFAULT_PLOT_CONFIG,
        help=f"Plot config name under experiment/plot_configs/. Default: {DEFAULT_PLOT_CONFIG}",
    )
    args = parser.parse_args()

    plot_cfg = load_plot_config(args.plot_config)
    deps = resolve_dependencies(plot_cfg)
    require_results_ready(deps)
    phys = load_result(deps["physical"])
    virt = load_result(deps["virtual"])

    xs_virt, ys_virt, _ = extract_series(virt, x_param="client.aof_replay_task_count")
    xs_phys, ys_phys, _ = extract_series(
        phys, x_param="client.aof_physical_sublog_count"
    )

    single_log_y_mrec = None
    for x, y in zip(xs_phys, ys_phys):
        if x == 1:
            single_log_y_mrec = y
            break
    if single_log_y_mrec is None:
        raise RuntimeError(
            f"Could not find aof_physical_sublog_count=1 datapoint in {deps['physical']}; "
            "needed for the Single Log reference line."
        )

    scale = float(plot_cfg.get("scale", 1.0))
    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

    all_x = sorted(set(xs_virt) | set(xs_phys))
    if not all_x:
        raise RuntimeError("Empty replay datasets; nothing to plot.")

    ax.axhline(
        single_log_y_mrec,
        color=color_map["single_log"],
        linestyle=linestyle_map["single_log"],
        linewidth=LINEWIDTH,
        label=labels_map["single_log"],
    )

    ax.plot(
        xs_virt,
        ys_virt,
        color=color_map["multilog_virtual"],
        linestyle=linestyle_map["multilog_virtual"],
        marker=marker_map["multilog_virtual"],
        markersize=MARKER_SIZE,
        linewidth=LINEWIDTH,
        label=labels_map["multilog_virtual"],
    )

    ax.plot(
        xs_phys,
        ys_phys,
        color=color_map["multilog_physical"],
        linestyle=linestyle_map["multilog_physical"],
        marker=marker_map["multilog_physical"],
        markersize=MARKER_SIZE,
        linewidth=LINEWIDTH,
        label=labels_map["multilog_physical"],
    )

    all_y = list(ys_virt) + list(ys_phys) + [single_log_y_mrec]
    y_log = plot_cfg.get("yscale") == "log"
    default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
    apply_axis_cfg(
        ax,
        plot_cfg,
        default_xlabel="#threads",
        default_ylabel="Throughput (Mop/s)",
        default_xticks=all_x,
        default_ymax=default_ymax,
    )
    out_path = RESULT_ROOT / args.plot_config / f"{args.plot_config}.pdf"
    legend_kwargs = dict(LEGEND_KWARGS, ncol=3)
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, **legend_kwargs)
    else:
        handles, labels = row_major_handles(ax, legend_kwargs["ncol"])
        ax.legend(
            handles,
            labels,
            loc="upper left",
            bbox_to_anchor=(0.0, 1.0),
            **legend_kwargs,
        )

    save_fig(fig, out_path)
    plt.close(fig)


if __name__ == "__main__":
    main()
