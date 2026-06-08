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
  - replay:          replay-scaling figure (sec:6.3)
                     dependencies: [<physical sweep>, <virtual sweep>]
  - replay_spectrum: bar plot of replay throughput across (k, m) combos
                     dependencies: [<sweep_combo experiment>]
  - replay_reader:   four figures of reader tput / p50 / p99 / p99.9 latency
                     vs. replay throughput, one datapoint per sublog count
                     pinned by the config's `filter` map
                     dependencies: [<reader sweep>]
  - append:          append-scaling figure (sec:6.2)
                     dependencies: [<single experiment>]
  - set:             online SET throughput figure (no AOF vs single-log)
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
    resolve_legend_geom,
    row_major_handles,
    save_fig,
    save_legend,
)


def render_replay(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # dependencies[0] = physical sweep, dependencies[1] = virtual sweep,
    # dependencies[2] (optional) = physical NoPrefix sweep, used to draw the
    # NoPrefix upper-bound curve along the physical axis only.
    if len(deps) not in (2, 3):
        raise ValueError(
            f"replay template expects 2 or 3 dependencies "
            f"[physical, virtual, (physical_noprefix)]; got {deps}"
        )
    physical_name, virtual_name = deps[0], deps[1]
    noprefix_name = deps[2] if len(deps) == 3 else None
    phys = load_result(physical_name)
    virt = load_result(virtual_name)

    xs_virt, ys_virt, _ = extract_series(virt, x_param="client.aof_replay_task_count")
    xs_phys, ys_phys, _ = extract_series(
        phys, x_param="client.aof_physical_sublog_count"
    )

    xs_nop: list[float] = []
    ys_nop: list[float] = []
    if noprefix_name is not None:
        nop = load_result(noprefix_name)
        xs_nop, ys_nop, _ = extract_series(
            nop, x_param="client.aof_physical_sublog_count"
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

    all_x = sorted(set(xs_virt) | set(xs_phys) | set(xs_nop))
    if not all_x:
        raise RuntimeError("Empty replay datasets; nothing to plot.")

    ax.axhline(
        single_log_y,
        color=color_map["single_log"],
        linestyle=linestyle_map["single_log"],
        linewidth=LINEWIDTH,
        label=labels_map["single_log"],
        zorder=zorder_map.get("single_log", 11),
    )
    # NoPrefix sits between Single Log and the MultiLog curves so the legend
    # reads Single Log -> NoPrefix -> MultiLog(...) and the upper-bound curve
    # renders on top where it crosses Single Log.
    if xs_nop:
        ax.plot(
            xs_nop,
            ys_nop,
            color=color_map["noprefix_physical"],
            linestyle=linestyle_map["noprefix_physical"],
            marker=marker_map["noprefix_physical"],
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            label=labels_map["noprefix_physical"],
            zorder=zorder_map.get("noprefix_physical", 12),
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

    all_y = list(ys_virt) + list(ys_phys) + list(ys_nop) + [single_log_y]
    y_log = plot_cfg.get("yscale") == "log"
    default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
    apply_axis_cfg(
        ax,
        plot_cfg,
        default_xlabel="Number of threads",
        default_ylabel="Throughput (Mop/s)",
        default_xticks=all_x,
        default_ymax=default_ymax,
    )

    ncol, legend_width = resolve_legend_geom(plot_cfg, 4 if xs_nop else 3)
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, width=legend_width, **legend_kwargs)
    else:
        handles, labels = row_major_handles(ax, ncol)
        ax.legend(
            handles,
            labels,
            loc="upper left",
            bbox_to_anchor=(0.0, 1.0),
            **legend_kwargs,
        )

    save_fig(fig, out_path)


def render_replay_spectrum(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # Bar plot: one bar per (aof_physical_sublog_count, aof_replay_task_count)
    # combo along a fixed k*m diagonal (see configs/aof_replay_spectrum_*.yaml).
    # Bars are sorted by k ascending so the leftmost bar is the all-virtual
    # extreme (k=1) and the rightmost is the all-physical extreme (m=1).
    if len(deps) != 1:
        raise ValueError(
            f"replay_spectrum template expects 1 dependency; got {deps}"
        )
    result = load_result(deps[0])

    rows: list[tuple[int, int, float, float]] = []
    for entry in result["runs"].values():
        sweep = entry.get("config", {}).get("sweep_params") or {}
        k = sweep.get("client.aof_physical_sublog_count")
        m = sweep.get("client.aof_replay_task_count")
        if k is None or m is None:
            continue
        stats = entry.get("stats", {}).get("throughput", {})
        y = stats.get("mean")
        if y is None:
            continue
        std = stats.get("std")
        rows.append((int(k), int(m), float(y), float(std) if std is not None else 0.0))

    if not rows:
        raise RuntimeError(
            f"No (k, m, throughput) data found in {deps[0]}; "
            "expected sweep_combo over aof_physical_sublog_count x aof_replay_task_count."
        )

    rows.sort(key=lambda r: r[0])

    scale = float(plot_cfg.get("scale", 1.0))
    fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

    n = len(rows)
    xs = list(range(n))
    ys = [r[2] for r in rows]
    # m=1 is the all-direct extreme; label it with just `k` to flag it as the
    # pure MultiLog-direct configuration (no virtual sublogs per physical).
    # All other combos keep the `(k, m)` tuple form.
    labels = [f"({k},{m})" if m > 1 else f"{k}" for k, m, _, _ in rows]

    # Draw bars in two labeled groups so the legend gets one entry per mode.
    # m=1 is the all-direct extreme (multilog_direct color); m>1 are hybrid.
    hybrid_idx = [i for i, (_, m, _, _) in enumerate(rows) if m > 1]
    direct_idx = [i for i, (_, m, _, _) in enumerate(rows) if m == 1]
    if hybrid_idx:
        ax.bar(
            [xs[i] for i in hybrid_idx],
            [ys[i] for i in hybrid_idx],
            color=color_map["multilog_hybrid"],
            edgecolor="black",
            linewidth=0.5,
            label=labels_map["multilog_hybrid"],
        )
    if direct_idx:
        ax.bar(
            [xs[i] for i in direct_idx],
            [ys[i] for i in direct_idx],
            color=color_map["multilog_direct"],
            edgecolor="black",
            linewidth=0.5,
            label=labels_map["multilog_direct"],
        )

    y_log = plot_cfg.get("yscale") == "log"
    default_ymax = max(ys) * (1.5 if y_log else 1.1) if ys else None
    apply_axis_cfg(
        ax,
        plot_cfg,
        default_xlabel="MultiLog configuration",
        default_ylabel="Throughput (Mop/s)",
        default_xticks=[],  # x-axis ticks/limits set manually below
        default_ymax=default_ymax,
    )

    # Override anything apply_axis_cfg may have set on the x-axis: bars sit at
    # integer positions, and the labels are the (k, m) tuples, not numbers.
    # Slight rotation lets us keep a near-base font size while fitting 6-7
    # tuples along a half-column-wide axis.
    ax.set_xticks(xs)
    ax.set_xticklabels(
        labels,
        fontsize=plt.rcParams["font.size"] * 0.85,
        rotation=30,
        ha="right",
        rotation_mode="anchor",
    )
    ax.set_xlim(-0.6, n - 0.4)

    # The rotated x tick labels shorten the axes box, so the y-label centered
    # on it pokes past the figure's top edge and gets cropped; anchor it a
    # little lower along the axis.
    ax.yaxis.label.set_y(0.42)

    ncol, legend_width = resolve_legend_geom(plot_cfg, 2)
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, width=legend_width, **legend_kwargs)
    else:
        handles, labels_legend = row_major_handles(ax, ncol)
        ax.legend(
            handles,
            labels_legend,
            loc="upper left",
            bbox_to_anchor=(0.0, 1.0),
            **legend_kwargs,
        )

    save_fig(fig, out_path)


_LATENCY_UNIT_SCALE = {"ms": 1e-3, "us": 1.0}


def render_replay_reader(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # dependencies[0] = reader experiment. Each `series` produces four figures
    # sharing X = replay throughput, with one datapoint per sublog count.
    #
    # Config keys (all optional):
    #   sublog_param  dotted param holding the sublog count and the source of
    #                 k_values (default server.aof_physical_sublog_count; use
    #                 client.aof_physical_sublog_count for in-proc experiments).
    #   point_param   dotted param swept along each curve (default = sublog_param,
    #                 i.e. one point per curve). Set it to trace a curve over a
    #                 third sweep dimension, e.g. client.aof_replay_drift_threshold.
    #   point_exclude list of point_param values to drop, e.g. [-1].
    #   filter        base filter (dotted param keys) applied to every series,
    #                 e.g. client.aof_replay_reader: 1, client.itp: 128.
    #   latency_unit  "us" (default) or "ms"; scales the three latency figures.
    #   series        list of {suffix, filter} -- one set of four figures each,
    #                 with `filter` merged over the base filter. Defaults to a
    #                 single unsuffixed set (no extra filter). Use it to split a
    #                 second sweep dimension into separate figures, e.g. one set
    #                 per aof_replay_dist value.
    if len(deps) != 1:
        raise ValueError(
            f"replay_reader template expects 1 dependency [reader sweep]; got {deps}"
        )
    result = load_result(deps[0])
    sublog_param = plot_cfg.get("sublog_param", "server.aof_physical_sublog_count")
    k_values = sorted(result["sweep_params"][sublog_param])
    base_filter = dict(plot_cfg.get("filter") or {})
    point_param = plot_cfg.get("point_param", sublog_param)
    point_exclude = set(plot_cfg.get("point_exclude") or [])

    unit = str(plot_cfg.get("latency_unit", "us")).lower()
    if unit not in _LATENCY_UNIT_SCALE:
        raise ValueError(
            f"latency_unit must be one of {sorted(_LATENCY_UNIT_SCALE)}; got {unit!r}"
        )
    lat_scale = _LATENCY_UNIT_SCALE[unit]

    # (figure file suffix, y metric in result.yaml, default y-axis label,
    # y unit scale). parse.py records reader latencies in us.
    metric_figures = [
        ("tput", "reader_throughput", "Reader\nthroughput (Mop/s)", 1.0),
        ("p50", "reader_lat_p50", f"Reader\np50 latency ({unit})", lat_scale),
        ("p99", "reader_lat_p99", f"Reader\np99 latency ({unit})", lat_scale),
        ("p999", "reader_lat_p99_9", f"Reader\np99.9 latency ({unit})", lat_scale),
    ]

    series = plot_cfg.get("series") or [{}]

    scale = float(plot_cfg.get("scale", 1.0))
    ncol, legend_width = resolve_legend_geom(plot_cfg, 3)
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)

    legend_saved = False
    for s in series:
        s_suffix = s.get("suffix", "")
        s_filter = {**base_filter, **(s.get("filter") or {})}
        for suffix, y_metric, default_ylabel, y_scale in metric_figures:
            # Per-figure axis overrides use figure-suffixed keys, applied least
            # to most specific so the most specific wins. For any axis-styling
            # key (e.g. yticks, ymax, yscale):
            #   <key>_<metric>              -> that metric across every series
            #                                  (e.g. yticks_p99)
            #   <key>_<series>_<metric>     -> one figure only
            #                                  (e.g. yticks_zipf_p99)
            fig_cfg = dict(plot_cfg)
            override_tags = [f"_{suffix}"]
            if s_suffix:
                override_tags.append(f"_{s_suffix}_{suffix}")
            for tag in override_tags:
                for cfg_key, value in plot_cfg.items():
                    if cfg_key.endswith(tag):
                        fig_cfg[cfg_key[: -len(tag)]] = value
            fig, ax = build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)

            all_x: list[float] = []
            all_y: list[float] = []
            for k in k_values:
                key = "single_log" if k == 1 else f"multilog_m{k}"
                filt = {**s_filter, sublog_param: k}
                # Each curve's points sweep point_param (one point per value;
                # the default point_param == sublog_param yields a single point
                # per curve). Both extract_series calls share the filter and
                # x_param, so they sort identically and pair index-for-index as
                # (point value, replay tput, reader metric).
                pts, xs_replay, _ = extract_series(
                    result,
                    x_param=point_param,
                    y_metric="throughput",
                    y_field="median",
                    filter_params=filt,
                )
                _, ys, _ = extract_series(
                    result,
                    x_param=point_param,
                    y_metric=y_metric,
                    y_field="median",
                    filter_params=filt,
                )
                # Drop excluded point values (e.g. -1, the unbounded baseline
                # that starves the reader and explodes its latency).
                kept = [
                    (rep, met)
                    for p, rep, met in zip(pts, xs_replay, ys)
                    if p not in point_exclude
                ]
                xs_replay = [rep for rep, _ in kept]
                ys = [met * y_scale for _, met in kept]
                if not xs_replay:
                    print(
                        f"WARN: no paired data for {sublog_param}={k} "
                        f"under filter {filt}",
                        file=sys.stderr,
                    )
                    continue
                all_x.extend(xs_replay)
                all_y.extend(ys)
                ax.plot(
                    xs_replay,
                    ys,
                    color=color_map[key],
                    linestyle=linestyle_map[key],
                    marker=marker_map[key],
                    markersize=MARKER_SIZE,
                    linewidth=LINEWIDTH,
                    label=labels_map[key],
                    zorder=zorder_map.get(key, 2),
                )

            y_log = fig_cfg.get("yscale") == "log"
            default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
            default_xmax = max(all_x) * 1.05 if all_x else None
            apply_axis_cfg(
                ax,
                fig_cfg,
                default_xlabel="Replay throughput (Mop/s)",
                default_ylabel=default_ylabel,
                default_xmax=default_xmax,
                default_ymax=default_ymax,
            )

            # The long x-label would clip at the figure's right edge; anchor it
            # slightly left of the axes center.
            ax.xaxis.label.set_x(0.48)

            if plot_cfg.get("legend_separate"):
                # Every figure carries the same curves; save the shared legend
                # once as <name>_legend.pdf.
                if not legend_saved:
                    save_legend(ax, out_path, width=legend_width, **legend_kwargs)
                    legend_saved = True
            else:
                handles, labels = row_major_handles(ax, ncol)
                ax.legend(
                    handles,
                    labels,
                    loc="upper left",
                    bbox_to_anchor=(0.0, 1.0),
                    **legend_kwargs,
                )

            # Filename: <stem>[_<series suffix>]_<metric>.<ext>
            tags = [out_path.stem] + ([s_suffix] if s_suffix else []) + [suffix]
            save_fig(fig, out_path.with_name("_".join(tags) + out_path.suffix))


def render_append(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # dependencies[0] = MultiLog 2-D sweep (threads x aof_physical_sublog_count);
    # dependencies[1] (optional) = matching NoPrefix sweep, used to draw a
    # single NoPrefix upper-bound curve at the highest sublog count in
    # APPEND_M_VALUES (overridable via plot_cfg `noprefix_m`).
    if len(deps) not in (1, 2):
        raise ValueError(
            f"append template expects 1 or 2 dependencies "
            f"[main, (noprefix)]; got {deps}"
        )
    result = load_result(deps[0])
    noprefix_result = load_result(deps[1]) if len(deps) == 2 else None
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
        if xs:
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
        else:
            print(f"WARN: no data for {filter_param}={m}", file=sys.stderr)

        # Plot NoPrefix right after Single Log so the legend reads
        # Single Log -> NoPrefix -> MultiLog(...).
        if m == 1 and noprefix_result is not None:
            noprefix_m = int(plot_cfg.get("noprefix_m", max(APPEND_M_VALUES)))
            xs_nop, ys_nop, _ = extract_series(
                noprefix_result,
                x_param=x_param,
                filter_params={filter_param: noprefix_m},
            )
            if xs_nop:
                all_threads.update(xs_nop)
                all_y.extend(ys_nop)
                ax.plot(
                    xs_nop,
                    ys_nop,
                    color=color_map["noprefix_m64"],
                    linestyle=linestyle_map["noprefix_m64"],
                    marker=marker_map["noprefix_m64"],
                    markersize=MARKER_SIZE,
                    linewidth=LINEWIDTH,
                    label=labels_map["noprefix_m64"],
                    zorder=zorder_map.get("noprefix_m64", 12),
                )
            else:
                print(
                    f"WARN: no NoPrefix data for {filter_param}={noprefix_m}",
                    file=sys.stderr,
                )

    sorted_threads = sorted(all_threads)
    y_log = plot_cfg.get("yscale") == "log"
    default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
    apply_axis_cfg(
        ax,
        plot_cfg,
        default_xlabel="Number of threads",
        default_ylabel="Throughput (Mop/s)",
        default_xticks=sorted_threads,
        default_ymax=default_ymax,
    )

    ncol, legend_width = resolve_legend_geom(plot_cfg, 4)
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, width=legend_width, **legend_kwargs)
    else:
        handles, labels = row_major_handles(ax, ncol)
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
        default_xlabel="Number of threads",
        default_ylabel="Throughput (Mop/s)",
        default_xticks=sorted_threads,
        default_ymax=default_ymax,
    )

    # Set figure has only 2 entries; widen spacing/handles over the LEGEND_KWARGS
    # defaults tuned for the denser 3-4 entry replay/append legends.
    ncol, legend_width = resolve_legend_geom(plot_cfg, 2)
    legend_kwargs = dict(
        LEGEND_KWARGS,
        ncol=ncol,
        columnspacing=1.0,
        handlelength=1.8,
        handletextpad=0.5,
    )
    if plot_cfg.get("legend_separate"):
        save_legend(ax, out_path, width=legend_width, **legend_kwargs)
    else:
        ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.0), **legend_kwargs)

    save_fig(fig, out_path)


TEMPLATES = {
    "replay": render_replay,
    "replay_spectrum": render_replay_spectrum,
    "replay_reader": render_replay_reader,
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
