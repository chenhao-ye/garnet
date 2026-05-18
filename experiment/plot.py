#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "matplotlib",
#   "numpy",
#   "pyyaml",
# ]
# ///
"""Render figures defined by experiment/plot_configs/*.yaml.

Each plot config carries a `template` field that selects a renderer:
  - replay: replay-scaling figure (sec:6.3)
            dependencies: [<physical sweep>, <virtual sweep>]
  - append: append-scaling figure (sec:6.2)
            dependencies: [<single experiment>]
  - set:    online SET throughput figure (no AOF vs single-log)
            dependencies: [<no-AOF run>, <single-log AOF run>]

Usage:
    uv run experiment/plot.py replay_scaling
    uv run experiment/plot.py append_scaling set_scaling     # render multiple
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
    LEGEND_KWARGS,
    LINEWIDTH,
    MARKER_SIZE,
    color_map,
    labels_map,
    linestyle_map,
    marker_map,
    zorder_map,
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


def render_replay(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # dependencies[0] = physical sweep, dependencies[1] = virtual sweep.
    if len(deps) != 2:
        raise ValueError(
            f"replay template expects 2 dependencies [physical, virtual]; got {deps}"
        )
    physical_name, virtual_name = deps
    phys = load_result(physical_name)
    virt = load_result(virtual_name)

    xs_virt, ys_virt, _ = extract_series(virt, x_param="client.aof_replay_task_count")
    xs_phys, ys_phys, _ = extract_series(
        phys, x_param="client.aof_physical_sublog_count"
    )

    single_log_y = None
    for x, y in zip(xs_phys, ys_phys):
        if x == 1:
            single_log_y = y
            break
    if single_log_y is None:
        raise RuntimeError(
            f"Could not find aof_physical_sublog_count=1 datapoint in "
            f"{physical_name}; needed for the Single Log reference line."
        )

    scale = float(plot_cfg.get("scale", 1.0))
    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

    all_x = sorted(set(xs_virt) | set(xs_phys))
    if not all_x:
        raise RuntimeError("Empty replay datasets; nothing to plot.")

    ax.axhline(
        single_log_y,
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

    all_y = list(ys_virt) + list(ys_phys) + [single_log_y]
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


def render_append(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    if len(deps) != 1:
        raise ValueError(f"append template expects 1 dependency; got {deps}")
    result = load_result(deps[0])
    x_param = "client.threads"
    filter_param = "client.aof_physical_sublog_count"

    scale = float(plot_cfg.get("scale", 1.0))
    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

    all_threads: set[float] = set()
    all_y: list[float] = []
    for m in APPEND_M_VALUES:
        key = "single_log" if m == 1 else f"multilog_m{m}"
        xs, ys, _ = extract_series(
            result, x_param=x_param, filter_params={filter_param: m}
        )
        if not xs:
            print(f"WARN: no data for {filter_param}={m}", file=sys.stderr)
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
            zorder=zorder_map.get(key, 2),
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

    legend_kwargs = dict(LEGEND_KWARGS, ncol=4)
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


def render_set(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # dependencies are paired positionally with style keys: index 0 = no_aof
    # (AOF disabled), index 1 = aof_single (single physical sublog AOF).
    style_keys = ["no_aof", "aof_single"]
    if len(deps) != len(style_keys):
        raise ValueError(
            f"set template expects {len(style_keys)} dependencies "
            f"[{', '.join(style_keys)}]; got {deps}"
        )

    x_param = "client.threads"
    y_metric = "tpt_mops"

    scale = float(plot_cfg.get("scale", 1.0))
    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

    all_threads: set[float] = set()
    all_y: list[float] = []
    for key, experiment in zip(style_keys, deps):
        result = load_result(experiment)
        xs, ys, _ = extract_series(result, x_param=x_param, y_metric=y_metric)
        if not xs:
            print(f"WARN: no data for {experiment}", file=sys.stderr)
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

    # Set figure has only 2 entries; widen spacing/handles over the LEGEND_KWARGS
    # defaults tuned for the denser 3-4 entry replay/append legends.
    legend_kwargs = dict(
        LEGEND_KWARGS,
        ncol=2,
        columnspacing=1.0,
        handlelength=1.8,
        handletextpad=0.5,
    )
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, **legend_kwargs)
    else:
        ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.0), **legend_kwargs)

    save_fig(fig, out_path)


TEMPLATES = {
    "replay": render_replay,
    "append": render_append,
    "set": render_set,
}


def _render_one(name: str) -> None:
    plot_cfg = load_plot_config(name)
    deps = resolve_dependencies(plot_cfg)
    require_results_ready(deps)

    template = plot_cfg.get("template")
    if template not in TEMPLATES:
        raise ValueError(
            f"Plot config '{name}' has unknown template {template!r}; "
            f"expected one of: {', '.join(sorted(TEMPLATES))}"
        )

    out_path = RESULT_ROOT / name / f"{name}.pdf"
    print(f"=== Rendering {name} (template={template}) ===")
    TEMPLATES[template](plot_cfg, deps, out_path)
    plt.close("all")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "plot_configs",
        nargs="+",
        help="One or more plot config names under experiment/plot_configs/.",
    )
    args = parser.parse_args()
    for name in args.plot_configs:
        _render_one(name)


if __name__ == "__main__":
    main()
