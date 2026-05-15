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
    load_result,
    save_fig,
    to_mrecords,
)

EXPERIMENT = "aof_enqueue_random"
X_PARAM = "client.threads"
FILTER_PARAM = "client.aof_physical_sublog_count"


def main():
    result = load_result(EXPERIMENT)

    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75)

    all_threads: set[float] = set()
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
        ax.plot(
            xs,
            to_mrecords(ys),
            color=color_map[key],
            linestyle=linestyle_map[key],
            marker=marker_map[key],
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            label=labels_map[key],
        )

    sorted_threads = sorted(all_threads)
    # Drop the tightly clustered low ticks (1, 2) — they overlap at single-column width.
    visible_ticks = [t for t in sorted_threads if t not in {1.0, 2.0}]
    ax.set_xticks(visible_ticks)
    ax.set_xticklabels([str(int(t)) for t in visible_ticks])
    ax.set_xlim(0, max(sorted_threads) * 1.02)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Primary worker threads")
    ax.set_ylabel("Append throughput (Mrec/s)")
    ax.yaxis.grid(True, linestyle=":", linewidth=0.5, alpha=0.6)
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

    out_path = RESULT_ROOT / EXPERIMENT / "plots" / "append_scaling.pdf"
    save_fig(fig, out_path)
    plt.close(fig)


if __name__ == "__main__":
    main()
