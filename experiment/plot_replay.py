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
  - Single Log: horizontal dashed reference (m=1 from aof_replay_physical).
  - MultiLog-virtual: m=1, n varies (aof_replay_virtual sweep).
  - MultiLog-physical: n=1, m varies (aof_replay_physical sweep).
Proves claim C2 (R1b). NoPrefix curve omitted: data not yet available.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from plot_style import (
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
    load_result,
    save_fig,
    to_mrecords,
)

VIRTUAL_EXP = "aof_replay_virtual"
PHYSICAL_EXP = "aof_replay_physical"


def main():
    virt = load_result(VIRTUAL_EXP)
    phys = load_result(PHYSICAL_EXP)

    xs_virt, ys_virt, _ = extract_series(virt, x_param="client.aof_replay_task_count")
    xs_phys, ys_phys, _ = extract_series(phys, x_param="client.aof_physical_sublog_count")

    single_log_y_mrec = None
    for x, y in zip(xs_phys, ys_phys):
        if x == 1:
            single_log_y_mrec = y / 1000.0
            break
    if single_log_y_mrec is None:
        raise RuntimeError(
            f"Could not find aof_physical_sublog_count=1 datapoint in {PHYSICAL_EXP}; "
            "needed for the Single Log reference line."
        )

    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75)

    all_x = sorted(set(xs_virt) | set(xs_phys))
    if not all_x:
        raise RuntimeError("Empty replay datasets; nothing to plot.")
    x_max = max(all_x)

    ax.axhline(
        single_log_y_mrec,
        color=color_map["single_log"],
        linestyle=linestyle_map["single_log"],
        linewidth=LINEWIDTH,
        label=labels_map["single_log"],
    )

    ax.plot(
        xs_virt,
        to_mrecords(ys_virt),
        color=color_map["multilog_virtual"],
        linestyle=linestyle_map["multilog_virtual"],
        marker=marker_map["multilog_virtual"],
        markersize=MARKER_SIZE,
        linewidth=LINEWIDTH,
        label=labels_map["multilog_virtual"],
    )

    ax.plot(
        xs_phys,
        to_mrecords(ys_phys),
        color=color_map["multilog_physical"],
        linestyle=linestyle_map["multilog_physical"],
        marker=marker_map["multilog_physical"],
        markersize=MARKER_SIZE,
        linewidth=LINEWIDTH,
        label=labels_map["multilog_physical"],
    )

    all_y = to_mrecords(ys_virt) + to_mrecords(ys_phys) + [single_log_y_mrec]
    y_min = min(y for y in all_y if y > 0)
    y_max = max(all_y)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(all_x)
    ax.set_xticklabels([str(int(x)) for x in all_x])
    ax.set_xlim(all_x[0] / 1.2, x_max * 1.2)
    ax.set_ylim(y_min / 1.5, y_max * 1.5)
    ax.set_xlabel("Number of sublogs")
    ax.set_ylabel("Replay throughput (Mrec/s)")
    ax.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax.set_axisbelow(True)
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(0.0, 1.0),
        frameon=False,
        ncol=1,
        handlelength=1.8,
        handletextpad=0.5,
        labelspacing=0.3,
    )

    out_path = RESULT_ROOT / "plots" / "replay_scaling.pdf"
    save_fig(fig, out_path)
    plt.close(fig)


if __name__ == "__main__":
    main()
