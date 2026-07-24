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
  - replay_reader_threshold:
                     per sublog count, five figures (replay tput, reader tput,
                     p50/p99/p99.9 latency) vs. a threshold knob (optionally
                     normalized by `threshold_norm`), one curve per replay
                     distribution
                     dependencies: [<threshold sweep>]
  - replay_reader_bar:
                     per series, five bar figures (replay tput, reader tput,
                     p50/p99/p99.9 latency) with one bar per sublog count
                     dependencies: [<reader sweep>]
  - replay_reader_scaling:
                     curve version of replay_reader_bar: five figures per
                     series vs. sublog count (log2 x), SingleLog dot + MultiLog
                     curve
                     dependencies: [<reader sweep(s)>]
  - replay_reader_sketch:
                     per dbsize, five figures (replay tput, reader tput,
                     p50/p99/p99.9 latency) vs. sketch size (log x, k/m tick
                     labels), one curve per sublog count
                     dependencies: [<sketch sweep>]
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
    #
    # Optionally add extra "C5" curves (a second virtual replay-task sweep,
    # e.g. split by snapshot frequency): set `c5_dependency` and `c5_x_param`
    # (default client.aof_replay_task_count), with `c5_curves` a list of
    # {style, filter} -- `style` names a plot_style key.
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

    # Optional C5 curves (each a (style, xs, ys) tuple).
    c5_dependency = plot_cfg.get("c5_dependency")
    c5_x_param = plot_cfg.get("c5_x_param", "client.aof_replay_task_count")
    c5_curves = plot_cfg.get("c5_curves") or []
    c5_series: list[tuple[str, list[float], list[float]]] = []
    if c5_dependency and c5_curves:
        c5_result = load_result(c5_dependency)
        for cc in c5_curves:
            cxs, cys, _ = extract_series(
                c5_result, x_param=c5_x_param, filter_params=cc.get("filter") or {}
            )
            if cxs:
                c5_series.append((cc["style"], cxs, cys))

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

    all_x = sorted(
        set(xs_virt)
        | set(xs_phys)
        | set(xs_nop)
        | {x for _, cxs, _ in c5_series for x in cxs}
    )
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
    # C5 curves are drawn before MultiLog so MultiLog renders on top, and the
    # legend reads SingleLog/NoPrefix, then C5..., then MultiLog... (MultiLog
    # in the last row).
    for style, cxs, cys in c5_series:
        ax.plot(
            cxs,
            cys,
            color=color_map[style],
            linestyle=linestyle_map[style],
            marker=marker_map[style],
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            label=labels_map[style],
            zorder=zorder_map.get(style, 2),
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

    all_y = (
        list(ys_virt)
        + list(ys_phys)
        + list(ys_nop)
        + [single_log_y]
        + [y for _, _, cys in c5_series for y in cys]
    )
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

    base_ncol = (4 if xs_nop else 3) + len(c5_series)
    ncol, legend_width = resolve_legend_geom(plot_cfg, base_ncol)
    # `legend_ncol` overrides the column count independently of the host width
    # (useful when many long labels need several rows at full column width).
    ncol = int(plot_cfg.get("legend_ncol", ncol))
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


def _fmt_pow2_size(value) -> str:
    """Format a byte/slot count as a binary-prefixed label: 1024 -> "1k",
    1048576 -> "1m", 4194304 -> "4m"."""
    v = int(value)
    if v >= 1024 * 1024:
        n = v / (1024 * 1024)
        return f"{int(n) if float(n).is_integer() else n}m"
    if v >= 1024:
        n = v / 1024
        return f"{int(n) if float(n).is_integer() else n}k"
    return str(v)


def _fmt_si_count(value) -> str:
    """Format a decimal count as an SI-suffixed label: 10000000 -> "10m"."""
    v = int(value)
    for suffix, base in (("b", 1_000_000_000), ("m", 1_000_000), ("k", 1_000)):
        if v >= base:
            n = v / base
            return f"{int(n) if float(n).is_integer() else n}{suffix}"
    return str(v)


def _build_metric_fig(plot_cfg: dict):
    """Build a 1x1 figure for the metric-grid templates.

    With only `scale` set, the figure keeps the default 4:3 width:height
    (hw_ratio 0.75). If `width_scale` and/or `height_scale` are given, each
    axis is sized directly (so equal values give a square); a missing one
    falls back to `scale`.
    """
    scale = float(plot_cfg.get("scale", 1.0))
    w = plot_cfg.get("width_scale")
    h = plot_cfg.get("height_scale")
    if w is None and h is None:
        return build_fig_single_col(1, 1, hw_ratio=0.75, width_scale=scale)
    return build_fig_single_col(
        1,
        1,
        width_scale=float(w) if w is not None else scale,
        height_scale=float(h) if h is not None else scale,
    )


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
        ("tput", "reader_throughput", "Read\nthroughput (Mop/s)", 1.0),
        ("p50", "reader_lat_p50", f"Read\np50 latency ({unit})", lat_scale),
        ("p99", "reader_lat_p99", f"Read\np99 latency ({unit})", lat_scale),
        ("p999", "reader_lat_p99_9", f"Read\np99.9 latency ({unit})", lat_scale),
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


def render_replay_reader_threshold(
    plot_cfg: dict, deps: list[str], out_path: Path
) -> None:
    # dependencies[0] = threshold-sweep reader experiment. For each sublog
    # count (m) this emits one figure per metric, with X = the threshold knob
    # and one colored curve per replay distribution.
    #
    # Config keys (all optional unless noted):
    #   sublog_param  param identifying each figure-set / m (default
    #                 client.aof_physical_sublog_count).
    #   x_param       param on the x-axis (default
    #                 client.aof_replay_drift_threshold).
    #   x_include     if set, keep only these x_param values (an allow-list);
    #                 e.g. [10000, ..., 60000] to drop -1 and higher thresholds.
    #   threshold_norm  divide x values by this for display, so ticks read as
    #                 multiples of a reference threshold (default 1).
    #   width_scale, height_scale  size each axis directly when set (equal =>
    #                 square). With only `scale`, the figure keeps the default
    #                 4:3 width:height.
    #   curves        list of {style, filter, [label]} -- one colored curve
    #                 each; `style` names entries in plot_style's maps. Defaults
    #                 to the three replay distributions.
    #   latency_unit  "us" (default) or "ms"; scales the three latency figures.
    #   Figure-suffixed axis overrides: <key>_<metric> (all m) and
    #   <key>_m{m}_<metric> (one figure), e.g. yticks_p99, yticks_m32_p99.
    if len(deps) != 1:
        raise ValueError(
            "replay_reader_threshold template expects 1 dependency "
            f"[threshold sweep]; got {deps}"
        )
    result = load_result(deps[0])
    sublog_param = plot_cfg.get("sublog_param", "client.aof_physical_sublog_count")
    x_param = plot_cfg.get("x_param", "client.aof_replay_drift_threshold")
    x_include = plot_cfg.get("x_include")
    x_include = None if x_include is None else set(x_include)
    threshold_norm = float(plot_cfg.get("threshold_norm", 1))
    m_values = sorted(result["sweep_params"][sublog_param])

    unit = str(plot_cfg.get("latency_unit", "us")).lower()
    if unit not in _LATENCY_UNIT_SCALE:
        raise ValueError(
            f"latency_unit must be one of {sorted(_LATENCY_UNIT_SCALE)}; got {unit!r}"
        )
    lat_scale = _LATENCY_UNIT_SCALE[unit]

    # (figure file suffix, y metric in result.yaml, default y-axis label,
    # y unit scale). parse.py records reader latencies in us.
    # Single-line labels (short forms) so they fit the narrow figure height.
    metric_figures = [
        ("replay", "throughput", "Replay tput (Mop/s)", 1.0),
        ("reader", "reader_throughput", "Read tput (Mop/s)", 1.0),
        ("p50", "reader_lat_p50", f"Read p50 ({unit})", lat_scale),
        ("p99", "reader_lat_p99", f"Read p99 ({unit})", lat_scale),
        ("p999", "reader_lat_p99_9", f"Read p99.9 ({unit})", lat_scale),
    ]

    curves = plot_cfg.get("curves") or [
        {"style": "dist_uniform", "filter": {"client.aof_replay_dist": "Uniform"}},
        {"style": "dist_zipfrev", "filter": {"client.aof_replay_dist": "ZipfRev"}},
        {"style": "dist_zipf", "filter": {"client.aof_replay_dist": "Zipf"}},
    ]

    ncol, legend_width = resolve_legend_geom(plot_cfg, len(curves))
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)

    legend_saved = False
    for m in m_values:
        for suffix, y_metric, default_ylabel, y_scale in metric_figures:
            # Figure-suffixed axis overrides, least to most specific so the
            # most specific wins: <key>_<metric> for that metric across all m,
            # <key>_m{m}_<metric> for one figure.
            fig_cfg = dict(plot_cfg)
            for tag in (f"_{suffix}", f"_m{m}_{suffix}"):
                for cfg_key, value in plot_cfg.items():
                    if cfg_key.endswith(tag):
                        fig_cfg[cfg_key[: -len(tag)]] = value
            fig, ax = _build_metric_fig(plot_cfg)

            all_x: list[float] = []
            all_y: list[float] = []
            for curve in curves:
                style = curve["style"]
                filt = {**(curve.get("filter") or {}), sublog_param: m}
                xs, ys, _ = extract_series(
                    result,
                    x_param=x_param,
                    y_metric=y_metric,
                    y_field="median",
                    filter_params=filt,
                )
                # Keep only included x values (if an allow-list is set), then
                # rescale x for display and y for its unit.
                kept = [
                    (x, y)
                    for x, y in zip(xs, ys)
                    if x_include is None or x in x_include
                ]
                xs_plot = [x / threshold_norm for x, _ in kept]
                ys_plot = [y * y_scale for _, y in kept]
                if not xs_plot:
                    print(
                        f"WARN: no data for {sublog_param}={m} under filter {filt}",
                        file=sys.stderr,
                    )
                    continue
                all_x.extend(xs_plot)
                all_y.extend(ys_plot)
                ax.plot(
                    xs_plot,
                    ys_plot,
                    color=color_map[style],
                    linestyle=linestyle_map[style],
                    marker=marker_map[style],
                    markersize=MARKER_SIZE,
                    linewidth=LINEWIDTH,
                    label=curve.get("label", labels_map[style]),
                    zorder=zorder_map.get(style, 2),
                )

            y_log = fig_cfg.get("yscale") == "log"
            default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
            default_xmax = max(all_x) * 1.05 if all_x else None
            apply_axis_cfg(
                ax,
                fig_cfg,
                default_xlabel="Replay imbalance threshold",
                default_ylabel=default_ylabel,
                default_xmax=default_xmax,
                default_ymax=default_ymax,
            )
            # The long x-label would clip at the narrow figure's right edge;
            # anchor it left of the axes center so it stays whole.
            # ax.xaxis.label.set_x(0.43)
            # Nudge the y-label down a little from the axes center.
            ax.yaxis.label.set_y(0.42)

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

            # Filename: <stem>_m{m}_<metric>.<ext>
            tags = [out_path.stem, f"m{m}", suffix]
            save_fig(fig, out_path.with_name("_".join(tags) + out_path.suffix))


def render_replay_reader_bar(plot_cfg: dict, deps: list[str], out_path: Path) -> None:
    # dependencies[0] = reader experiment. Each `series` produces five bar
    # figures (replay tput, reader tput, p50/p99/p99.9 latency); each figure has
    # one bar per sublog count (SingleLog, MultiLog(2)...), colored by config.
    #
    # Config keys (all optional):
    #   sublog_param  param holding the sublog count / bar categories (default
    #                 client.aof_physical_sublog_count).
    #   filter        base filter applied to every series.
    #   latency_unit  "us" (default) or "ms"; scales the three latency figures.
    #   width_scale, height_scale  size each axis directly when set (equal =>
    #                 square). With only `scale`, the figure keeps 4:3.
    #   series        list of {suffix, filter, dependency} -- one set of five
    #                 figures each; defaults to a single unsuffixed set.
    #                 `dependency` names which experiment to read (defaults to
    #                 the first dependency), so distributions split into
    #                 separate experiments each get their own series.
    #   Figure-suffixed axis overrides: <key>_<metric> (all series) and
    #   <key>_<series>_<metric> (one figure), e.g. yticks_p99, yticks_zipf_p99.
    if not deps:
        raise ValueError("replay_reader_bar template expects >= 1 dependency")
    sublog_param = plot_cfg.get("sublog_param", "client.aof_physical_sublog_count")
    base_filter = dict(plot_cfg.get("filter") or {})

    unit = str(plot_cfg.get("latency_unit", "us")).lower()
    if unit not in _LATENCY_UNIT_SCALE:
        raise ValueError(
            f"latency_unit must be one of {sorted(_LATENCY_UNIT_SCALE)}; got {unit!r}"
        )
    lat_scale = _LATENCY_UNIT_SCALE[unit]

    # Single-line labels (short forms) so they fit the narrow figure height.
    metric_figures = [
        ("replay", "throughput", "Replay tput (Mop/s)", 1.0),
        ("reader", "reader_throughput", "Read tput (Mop/s)", 1.0),
        ("p50", "reader_lat_p50", f"Read p50 ({unit})", lat_scale),
        ("p99", "reader_lat_p99", f"Read p99 ({unit})", lat_scale),
        ("p999", "reader_lat_p99_9", f"Read p99.9 ({unit})", lat_scale),
    ]

    series = plot_cfg.get("series") or [{}]

    ncol, legend_width = resolve_legend_geom(plot_cfg, 3)
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)

    legend_saved = False
    for s in series:
        s_suffix = s.get("suffix", "")
        s_filter = {**base_filter, **(s.get("filter") or {})}
        # Each series may read its own experiment (default: first dependency).
        result = load_result(s.get("dependency", deps[0]))
        k_values = sorted(result["sweep_params"][sublog_param])
        for suffix, y_metric, default_ylabel, y_scale in metric_figures:
            # Figure-suffixed axis overrides, least to most specific.
            fig_cfg = dict(plot_cfg)
            override_tags = [f"_{suffix}"]
            if s_suffix:
                override_tags.append(f"_{s_suffix}_{suffix}")
            for tag in override_tags:
                for cfg_key, value in plot_cfg.items():
                    if cfg_key.endswith(tag):
                        fig_cfg[cfg_key[: -len(tag)]] = value
            fig, ax = _build_metric_fig(plot_cfg)

            heights: list[float] = []
            for i, k in enumerate(k_values):
                key = "single_log" if k == 1 else f"multilog_m{k}"
                filt = {**s_filter, sublog_param: k}
                _, ys, _ = extract_series(
                    result,
                    x_param=sublog_param,
                    y_metric=y_metric,
                    y_field="median",
                    filter_params=filt,
                )
                if not ys:
                    print(
                        f"WARN: no data for {sublog_param}={k} under filter {filt}",
                        file=sys.stderr,
                    )
                    continue
                height = ys[0] * y_scale
                heights.append(height)
                # One labeled bar per config so the shared legend names them
                # (SingleLog, MultiLog(2)...); the x-axis itself stays clean.
                ax.bar(
                    i,
                    height,
                    color=color_map[key],
                    edgecolor="black",
                    linewidth=0.5,
                    label=labels_map[key],
                    zorder=2,
                )

            y_log = fig_cfg.get("yscale") == "log"
            default_ymax = max(heights) * (1.5 if y_log else 1.1) if heights else None
            apply_axis_cfg(
                ax,
                fig_cfg,
                default_xlabel="",
                default_ylabel=default_ylabel,
                default_xticks=[],  # bars are named by the legend, not x ticks
                default_ymax=default_ymax,
            )

            # Clean x-axis: the config of each bar is read from the legend.
            ax.set_xticks([])
            ax.set_xlim(-0.6, len(k_values) - 0.4)

            if plot_cfg.get("legend_separate"):
                # All figures share the same config bars; save the legend once.
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


def render_replay_reader_scaling(
    plot_cfg: dict, deps: list[str], out_path: Path
) -> None:
    # Curve version of replay_reader (cf. render_replay). Per `series` (each a
    # replay distribution, typically its own experiment) five figures (replay
    # tput, reader tput, p50/p99/p99.9) plot the metric vs. sublog count on a
    # log2 x-axis: SingleLog is a lone point at x=1, MultiLog a curve over x>=2.
    # Colors follow replay_scaling (SingleLog slate blue, MultiLog red).
    #
    # Config keys mirror render_replay_reader_bar, with `x_param` (the sublog
    # count on the x-axis, default client.aof_physical_sublog_count) replacing
    # `sublog_param`. Set a positive `xmin` (a log axis cannot start at 0).
    #
    # Optionally add extra "C5" curves (the virtual replay-task sweep) from a
    # separate experiment: set `c5_dependency` (the experiment name) and
    # `c5_x_param` (default client.aof_replay_task_count). Each series carries a
    # `c5_filter` to select its distribution. `c5_curves` is a list of
    # {style, filter} -- one curve each; `style` names a plot_style key (color,
    # marker, label) and `filter` is merged over the series c5_filter (e.g. to
    # select a snapshot frequency). No C5 curves are drawn when omitted.
    if not deps:
        raise ValueError("replay_reader_scaling template expects >= 1 dependency")
    x_param = plot_cfg.get("x_param", "client.aof_physical_sublog_count")
    base_filter = dict(plot_cfg.get("filter") or {})
    c5_dependency = plot_cfg.get("c5_dependency")
    c5_x_param = plot_cfg.get("c5_x_param", "client.aof_replay_task_count")
    c5_curves = plot_cfg.get("c5_curves") or []
    c5_result = load_result(c5_dependency) if (c5_dependency and c5_curves) else None

    unit = str(plot_cfg.get("latency_unit", "us")).lower()
    if unit not in _LATENCY_UNIT_SCALE:
        raise ValueError(
            f"latency_unit must be one of {sorted(_LATENCY_UNIT_SCALE)}; got {unit!r}"
        )
    lat_scale = _LATENCY_UNIT_SCALE[unit]

    metric_figures = [
        ("replay", "throughput", "Replay tput (Mop/s)", 1.0),
        ("reader", "reader_throughput", "Read tput (Mop/s)", 1.0),
        ("p50", "reader_lat_p50", f"Read p50 ({unit})", lat_scale),
        ("p99", "reader_lat_p99", f"Read p99 ({unit})", lat_scale),
        ("p999", "reader_lat_p99_9", f"Read p99.9 ({unit})", lat_scale),
    ]

    series = plot_cfg.get("series") or [{}]
    ncol, legend_width = resolve_legend_geom(plot_cfg, 2 + len(c5_curves))
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)

    legend_saved = False
    for s in series:
        s_suffix = s.get("suffix", "")
        s_filter = {**base_filter, **(s.get("filter") or {})}
        result = load_result(s.get("dependency", deps[0]))
        for suffix, y_metric, default_ylabel, y_scale in metric_figures:
            fig_cfg = dict(plot_cfg)
            override_tags = [f"_{suffix}"]
            if s_suffix:
                override_tags.append(f"_{s_suffix}_{suffix}")
            for tag in override_tags:
                for cfg_key, value in plot_cfg.items():
                    if cfg_key.endswith(tag):
                        fig_cfg[cfg_key[: -len(tag)]] = value
            fig, ax = _build_metric_fig(plot_cfg)

            xs, ys, _ = extract_series(
                result,
                x_param=x_param,
                y_metric=y_metric,
                y_field="median",
                filter_params=s_filter,
            )
            ys = [y * y_scale for y in ys]
            single = [(x, y) for x, y in zip(xs, ys) if x == 1]
            multi = [(x, y) for x, y in zip(xs, ys) if x >= 2]
            if not xs:
                print(f"WARN: no data under filter {s_filter}", file=sys.stderr)

            # SingleLog first so the legend reads SingleLog -> MultiLog.
            if single:
                ax.plot(
                    [x for x, _ in single],
                    [y for _, y in single],
                    color=color_map["single_log"],
                    linestyle="none",
                    marker="o",
                    markersize=MARKER_SIZE,
                    label="SingleLog",
                    zorder=zorder_map.get("single_log", 11),
                )
            if multi:
                ax.plot(
                    [x for x, _ in multi],
                    [y for _, y in multi],
                    color=color_map["multilog_physical"],
                    linestyle=linestyle_map["multilog_physical"],
                    marker=marker_map["multilog_physical"],
                    markersize=MARKER_SIZE,
                    linewidth=LINEWIDTH,
                    label="MultiLog",
                    zorder=zorder_map.get("multilog_physical", 2),
                )

            # C5 curves: the virtual replay-task sweep, from a separate
            # experiment, plotted over its own x_param (aof_replay_task_count).
            # Each c5_curve merges its filter over the series distribution
            # filter (e.g. to pick a snapshot frequency).
            c5_all_y: list[float] = []
            if c5_result is not None:
                for cc in c5_curves:
                    style = cc["style"]
                    cfilt = {
                        **base_filter,
                        **(s.get("c5_filter") or {}),
                        **(cc.get("filter") or {}),
                    }
                    cxs, cys, _ = extract_series(
                        c5_result,
                        x_param=c5_x_param,
                        y_metric=y_metric,
                        y_field="median",
                        filter_params=cfilt,
                    )
                    # Start C5 at x=2, matching MultiLog (x=1 is the base).
                    pts = [(x, y * y_scale) for x, y in zip(cxs, cys) if x >= 2]
                    if not pts:
                        continue
                    c5_all_y.extend(y for _, y in pts)
                    ax.plot(
                        [x for x, _ in pts],
                        [y for _, y in pts],
                        color=color_map[style],
                        linestyle=linestyle_map[style],
                        marker=marker_map[style],
                        markersize=MARKER_SIZE,
                        linewidth=LINEWIDTH,
                        label=labels_map[style],
                        zorder=zorder_map.get(style, 2),
                    )

            y_log = fig_cfg.get("yscale") == "log"
            all_y = [y for _, y in single] + [y for _, y in multi] + c5_all_y
            default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
            apply_axis_cfg(
                ax,
                fig_cfg,
                default_xlabel="Number of threads",
                default_ylabel=default_ylabel,
                default_ymax=default_ymax,
            )
            # Nudge the x-label left so it is not cut off at the right edge,
            # and the y-label down so it is not cut off at the top.
            ax.xaxis.label.set_x(0.42)
            ax.yaxis.label.set_y(0.42)

            if plot_cfg.get("legend_separate"):
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


def render_replay_reader_sketch(
    plot_cfg: dict, deps: list[str], out_path: Path
) -> None:
    # dependencies[0] = sketch-size sweep reader experiment. For each value of
    # `set_param` (e.g. dbsize) this emits one figure per metric (replay tput,
    # reader tput, p50/p99/p99.9), with X = sketch size on a log2 axis and one
    # curve per sublog count.
    #
    # Config keys (all optional):
    #   set_param     param identifying each figure-set (default client.dbsize).
    #   curve_param   param drawn as one curve each (default
    #                 client.aof_physical_sublog_count).
    #   x_param       param on the (log) x-axis (default client.aof_sketch_size).
    #   filter        base filter applied to every figure.
    #   latency_unit  "us" (default) or "ms"; scales the three latency figures.
    #   width_scale, height_scale  size each axis directly when set (equal =>
    #                 square). With only `scale`, the figure keeps 4:3.
    #   xticks        sketch-size positions; labels auto-format as 1k/4m/...
    #   Figure-suffixed axis overrides: <key>_<metric> (all sets) and
    #   <key>_<set>_<metric> (one figure), where <set> is the SI-formatted set
    #   value (e.g. yticks_p99, yticks_100m_p99).
    if len(deps) != 1:
        raise ValueError(
            "replay_reader_sketch template expects 1 dependency "
            f"[sketch sweep]; got {deps}"
        )
    result = load_result(deps[0])
    set_param = plot_cfg.get("set_param", "client.dbsize")
    curve_param = plot_cfg.get("curve_param", "client.aof_physical_sublog_count")
    x_param = plot_cfg.get("x_param", "client.aof_sketch_size")
    base_filter = dict(plot_cfg.get("filter") or {})
    set_values = sorted(result["sweep_params"][set_param])
    curve_values = sorted(result["sweep_params"][curve_param])

    unit = str(plot_cfg.get("latency_unit", "us")).lower()
    if unit not in _LATENCY_UNIT_SCALE:
        raise ValueError(
            f"latency_unit must be one of {sorted(_LATENCY_UNIT_SCALE)}; got {unit!r}"
        )
    lat_scale = _LATENCY_UNIT_SCALE[unit]

    # Single-line labels (short forms) so they fit the narrow figure height.
    metric_figures = [
        ("replay", "throughput", "Replay tput (Mop/s)", 1.0),
        ("reader", "reader_throughput", "Read tput (Mop/s)", 1.0),
        ("p50", "reader_lat_p50", f"Read p50 ({unit})", lat_scale),
        ("p99", "reader_lat_p99", f"Read p99 ({unit})", lat_scale),
        ("p999", "reader_lat_p99_9", f"Read p99.9 ({unit})", lat_scale),
    ]

    ncol, legend_width = resolve_legend_geom(plot_cfg, 3)
    legend_kwargs = dict(LEGEND_KWARGS, ncol=ncol)

    legend_saved = False
    for sv in set_values:
        set_suffix = _fmt_si_count(sv)
        for suffix, y_metric, default_ylabel, y_scale in metric_figures:
            # Figure-suffixed axis overrides, least to most specific:
            # <key>_<metric> across all sets, <key>_<set>_<metric> for one.
            fig_cfg = dict(plot_cfg)
            for tag in (f"_{suffix}", f"_{set_suffix}_{suffix}"):
                for cfg_key, value in plot_cfg.items():
                    if cfg_key.endswith(tag):
                        fig_cfg[cfg_key[: -len(tag)]] = value
            fig, ax = _build_metric_fig(plot_cfg)

            all_y: list[float] = []
            for k in curve_values:
                key = "single_log" if k == 1 else f"multilog_m{k}"
                filt = {**base_filter, set_param: sv, curve_param: k}
                xs, ys, _ = extract_series(
                    result,
                    x_param=x_param,
                    y_metric=y_metric,
                    y_field="median",
                    filter_params=filt,
                )
                ys = [y * y_scale for y in ys]
                if not xs:
                    print(
                        f"WARN: no data for {set_param}={sv} {curve_param}={k}",
                        file=sys.stderr,
                    )
                    continue
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

            # Frame the log x-axis to its ticks: xmax at the last tick, xmin at
            # half the first tick (a log axis cannot start at 0). Explicit
            # xmin/xmax in the config still win.
            xticks = fig_cfg.get("xticks")
            if xticks and fig_cfg.get("xscale") == "log":
                fig_cfg.setdefault("xmin", min(xticks) / 2)
                fig_cfg.setdefault("xmax", max(xticks))

            y_log = fig_cfg.get("yscale") == "log"
            default_ymax = max(all_y) * (1.5 if y_log else 1.1) if all_y else None
            apply_axis_cfg(
                ax,
                fig_cfg,
                default_xlabel="Sketch size",
                default_ylabel=default_ylabel,
                default_ymax=default_ymax,
            )
            # Relabel the x ticks with binary-prefixed sizes (1k, 4m, ...).
            if xticks:
                ax.set_xticklabels([_fmt_pow2_size(t) for t in xticks])

            if plot_cfg.get("legend_separate"):
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

            # Filename: <stem>_<set>_<metric>.<ext>
            tags = [out_path.stem, set_suffix, suffix]
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
    "replay_reader_threshold": render_replay_reader_threshold,
    "replay_reader_bar": render_replay_reader_bar,
    "replay_reader_scaling": render_replay_reader_scaling,
    "replay_reader_sketch": render_replay_reader_sketch,
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
