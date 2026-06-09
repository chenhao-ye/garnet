"""Shared plotting utilities for MultiLog paper figures.

result.yaml loaders, figure factory, save helpers. Mirrors the
hopperkv/scripts/plot_util.py API.
"""

from collections.abc import Iterable
from pathlib import Path

import matplotlib
import matplotlib.layout_engine
import matplotlib.pyplot as plt
import yaml
from matplotlib.transforms import Bbox
from plot_style import DOUBLE_COLUMN_WIDTH, SINGLE_COLUMN_WIDTH

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"
FIGURES_DIR = RESULT_ROOT / "figures"
CONFIG_ROOT = Path(__file__).resolve().parent / "configs"
PLOT_CONFIG_ROOT = Path(__file__).resolve().parent / "plot_configs"


def _link_to_figures(file_path: Path) -> None:
    """Mirror an output figure under FIGURES_DIR as <experiment>-<filename>.
    The link is relative so the result tree remains portable."""
    file_path = Path(file_path)
    if not file_path.exists():
        return
    try:
        rel = file_path.relative_to(RESULT_ROOT)
    except ValueError:
        return
    if rel.parts[0] == FIGURES_DIR.name:
        return
    experiment = rel.parts[0]
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    link_path = FIGURES_DIR / f"{experiment}-{file_path.name}"
    target = Path("..") / file_path.relative_to(RESULT_ROOT)
    if link_path.is_symlink() or link_path.exists():
        link_path.unlink()
    link_path.symlink_to(target)

PLOT_CONFIG_KEYS = {
    "scale",
    "xticks",
    "yticks",
    "xticks_minor",
    "yticks_minor",
    "xmin",
    "xmax",
    "ymin",
    "ymax",
    "xscale",
    "yscale",
    "xlabel",
    "ylabel",
    "xgrid",
    "ygrid",
    "legend_separate",
    "legend_scale",
}


def resolve_legend_geom(plot_cfg: dict, base_ncol: int) -> tuple[int, float]:
    """Scale a base column count and host width by plot_cfg's `legend_scale`.

    `legend_scale` is the legend counterpart of `scale`: it multiplies the
    standalone-legend host width (default SINGLE_COLUMN_WIDTH) and rescales
    the column count proportionally so a wider host actually uses the room
    (and a narrower host collapses entries into fewer columns). Defaults
    to 1.0; ncol is clamped to at least 1.

    For inline legends, only the scaled ncol is meaningful -- the axis
    extent is controlled by `scale`.
    """
    legend_scale = float(plot_cfg.get("legend_scale", 1.0))
    ncol = max(1, round(base_ncol * legend_scale))
    width = SINGLE_COLUMN_WIDTH * legend_scale
    return ncol, width


def load_result(experiment: str) -> dict:
    path = RESULT_ROOT / experiment / "result.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"result.yaml not found at {path}.\n"
            f"Run `uv run experiment/parse.py {experiment}` to generate it."
        )
    with open(path) as f:
        return yaml.safe_load(f)


def load_plot_config(plot_name: str) -> dict:
    """Load a plot config from plot_configs/<plot_name>.yaml.

    The returned dict contains the figure's dependencies and all axis-styling
    keys (the former `plot:` section). The caller is responsible for resolving
    `dependencies` via `resolve_dependencies` and checking data readiness via
    `require_results_ready`.
    """
    path = PLOT_CONFIG_ROOT / f"{plot_name}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"Plot config not found at {path}.\n"
            f"Create it under {PLOT_CONFIG_ROOT.relative_to(REPO_ROOT)}/."
        )
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    return cfg


def resolve_dependencies(plot_cfg: dict) -> list[str]:
    """Return the `dependencies` block of a plot config as a list of experiments.

    Order is preserved; each template documents what its slots mean.
    """
    raw = plot_cfg.get("dependencies")
    if raw is None:
        raise ValueError("Plot config is missing required 'dependencies' field")
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        raise ValueError(
            f"'dependencies' must be a list of experiment names, got {type(raw).__name__}"
        )
    return list(raw)


def require_results_ready(deps: list[str]) -> None:
    """Verify that result.yaml exists for every dependency. Aborts loudly if not.

    Lists every missing experiment at once so the user can rerun parse.py in a
    single batch instead of discovering them one at a time.
    """
    missing = [exp for exp in deps if not (RESULT_ROOT / exp / "result.yaml").exists()]
    if missing:
        raise RuntimeError(
            "Missing result.yaml for dependencies: " + ", ".join(missing) + ".\n"
            f"Run: uv run experiment/parse.py {' '.join(missing)}"
        )


def _run_sweep_params(run_entry: dict) -> dict:
    sweep = run_entry.get("sweep_params")
    if sweep is None:
        sweep = run_entry.get("config", {}).get("sweep_params") or {}
    return sweep


def _run_param(run_entry: dict, key: str):
    """Resolve a dotted `scope.param` key (e.g. "client.itp") for a run:
    sweep_params first, then the run's resolved client/server params, so
    filters can also pin params that were not swept."""
    sweep = _run_sweep_params(run_entry)
    if key in sweep:
        return sweep[key]
    scope, _, param = key.partition(".")
    return run_entry.get("config", {}).get(f"{scope}_params", {}).get(param)


def extract_series(
    result: dict,
    x_param: str,
    y_metric: str = "throughput",
    y_field: str = "mean",
    filter_params: dict | None = None,
) -> tuple[list[float], list[float], list[float]]:
    """Pull (xs, ys, stds) from result.yaml. Filters by exact match on filter_params."""
    filter_params = filter_params or {}
    rows: list[tuple[float, float, float]] = []
    for _, entry in result["runs"].items():
        sweep = _run_sweep_params(entry)
        if x_param not in sweep:
            continue
        if not all(_run_param(entry, k) == v for k, v in filter_params.items()):
            continue
        x = float(sweep[x_param])
        stats = entry.get("stats", {}).get(y_metric, {})
        y = stats.get(y_field)
        std = stats.get("std")
        if y is None:
            continue
        rows.append((x, float(y), float(std) if std is not None else 0.0))
    rows.sort(key=lambda r: r[0])
    xs = [r[0] for r in rows]
    ys = [r[1] for r in rows]
    stds = [r[2] for r in rows]
    return xs, ys, stds


def _hide_top_right_spines(axes):
    if isinstance(axes, Iterable):
        for ax in axes:
            _hide_top_right_spines(ax)
    else:
        axes.spines[["right", "top"]].set_visible(False)


def build_fig(
    nrows: int, ncols: int, total_width: float, total_height: float, **kwargs
):
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, **kwargs)
    fig.set_size_inches(total_width, total_height)
    _hide_top_right_spines(axes)
    return fig, axes


def build_fig_single_col(
    nrows: int,
    ncols: int,
    hw_ratio: float = 1.0,
    width_scale: float = 1.0,
    height_scale: float | None = None,
    **kwargs,
):
    # height tracks width by default (the figure scales uniformly); pass
    # height_scale to size height independently, e.g. a narrower width_scale
    # while keeping the taller height of a larger scale.
    total_width = SINGLE_COLUMN_WIDTH * width_scale
    h_scale = width_scale if height_scale is None else height_scale
    subplot_height = (SINGLE_COLUMN_WIDTH * h_scale / ncols) * hw_ratio
    return build_fig(nrows, ncols, total_width, subplot_height * nrows, **kwargs)


def build_fig_double_col(
    nrows: int, ncols: int, hw_ratio: float = 1.0, width_scale: float = 1.0, **kwargs
):
    total_width = DOUBLE_COLUMN_WIDTH * width_scale
    subplot_width = total_width / ncols
    subplot_height = subplot_width * hw_ratio
    return build_fig(nrows, ncols, total_width, subplot_height * nrows, **kwargs)


def _tick_label(value) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def _resolve_max(plot_cfg, ticks, max_key, default_max):
    explicit_max = plot_cfg.get(max_key)
    if explicit_max is not None:
        return explicit_max
    if ticks:
        return max(ticks)
    return default_max


def apply_axis_cfg(
    ax,
    plot_cfg: dict,
    *,
    default_xlabel: str = "",
    default_ylabel: str = "",
    default_xticks: list | None = None,
    default_yticks: list | None = None,
    default_xmax: float | None = None,
    default_ymax: float | None = None,
) -> None:
    """Apply axis settings from a config's `plot:` section.

    Recognized keys: scale (handled by the caller before figure creation),
    xscale, yscale (string, e.g. "log" / "linear"; default linear -- for x,
    "log" uses base 2 to match power-of-two sweeps), xticks, yticks (list),
    xmin/xmax/ymin/ymax (number), xlabel/ylabel (string).

    Limit resolution: xmin/ymin default to 0 regardless of ticks; the user
    must set them explicitly to override. xmax/ymax fall back to the
    ticks' last value, then to the caller-supplied upper bound.
    """
    xscale = plot_cfg.get("xscale")
    if xscale == "log":
        ax.set_xscale("log", base=2)
    elif xscale:
        ax.set_xscale(xscale)
    yscale = plot_cfg.get("yscale")
    if yscale:
        ax.set_yscale(yscale)

    xticks = plot_cfg.get("xticks", default_xticks)
    if xticks:
        ax.set_xticks(xticks)
        ax.set_xticklabels([_tick_label(t) for t in xticks])
    xticks_minor = plot_cfg.get("xticks_minor")
    if xticks_minor:
        ax.set_xticks(xticks_minor, minor=True)

    yticks = plot_cfg.get("yticks", default_yticks)
    if yticks:
        ax.set_yticks(yticks)
        ax.set_yticklabels([_tick_label(t) for t in yticks])
    yticks_minor = plot_cfg.get("yticks_minor")
    if yticks_minor:
        ax.set_yticks(yticks_minor, minor=True)

    xmin = plot_cfg.get("xmin", 0)
    xmax = _resolve_max(plot_cfg, xticks, "xmax", default_xmax)
    if xmax is not None:
        ax.set_xlim(xmin, xmax)
    ymin = plot_cfg.get("ymin", 0)
    ymax = _resolve_max(plot_cfg, yticks, "ymax", default_ymax)
    if ymax is not None:
        ax.set_ylim(ymin, ymax)

    ax.set_xlabel(plot_cfg.get("xlabel", default_xlabel))
    ax.set_ylabel(plot_cfg.get("ylabel", default_ylabel))

    if plot_cfg.get("xgrid", True):
        ax.grid(True, axis="x", which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    if plot_cfg.get("ygrid", True):
        ax.grid(True, axis="y", which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax.set_axisbelow(True)

    for line in ax.get_lines():
        line.set_clip_on(False)


def row_major_handles(ax, ncol: int):
    """Return (handles, labels) reordered so a matplotlib legend with `ncol`
    columns displays them row-major. matplotlib fills legends column-major by
    default; this reshuffle compensates so callers can write entries in
    natural reading order."""
    handles, labels = ax.get_legend_handles_labels()
    if not handles or ncol <= 1:
        return handles, labels
    n = len(handles)
    nrows = -(-n // ncol)
    order = [r * ncol + c for c in range(ncol) for r in range(nrows) if r * ncol + c < n]
    return [handles[i] for i in order], [labels[i] for i in order]


def save_legend(
    ax, fig_path: Path, *, width: float = SINGLE_COLUMN_WIDTH, **legend_kwargs
) -> Path | None:
    """Save the axes' legend as a standalone PDF of fixed `width` inches.

    The legend is centered within a host figure of width `width` (default
    SINGLE_COLUMN_WIDTH); narrower legends get whitespace on both sides,
    so the saved file is always exactly `width` wide. When fig_path ends
    in .pdf, also writes a 300 DPI PNG sibling (mirrors save_fig).
    Returns the legend PDF path, or None if the axes has no labeled
    artists.
    """
    ncol = legend_kwargs.get("ncol", 1)
    handles, labels = row_major_handles(ax, ncol)
    if not handles:
        return None
    fig_leg = plt.figure(figsize=(width, 1))
    legend = fig_leg.legend(
        handles,
        labels,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        **dict(legend_kwargs, borderpad=0),
    )
    fig_leg.canvas.draw()
    legend_bbox = legend.get_window_extent().transformed(
        fig_leg.dpi_scale_trans.inverted()
    )
    # Width is fixed at `width` (centered legend; whitespace padding on
    # both sides when the legend is narrower). Vertical crop tightens to
    # the rendered legend extent with a hairline pad for anti-aliasing;
    # the text's own line-box slack keeps ascenders/descenders intact.
    pad_y = 0.005
    bbox = Bbox.from_extents(0, legend_bbox.y0 - pad_y, width, legend_bbox.y1 + pad_y)
    legend_path = fig_path.with_name(f"{fig_path.stem}_legend{fig_path.suffix}")
    legend_path.parent.mkdir(parents=True, exist_ok=True)
    fig_leg.savefig(legend_path, bbox_inches=bbox, pad_inches=0)
    print(f"Saved {legend_path}")
    _link_to_figures(legend_path)
    if str(legend_path).endswith(".pdf"):
        png_path = Path(str(legend_path).replace(".pdf", ".png"))
        fig_leg.savefig(png_path, bbox_inches=bbox, pad_inches=0, dpi=300)
        print(f"Saved {png_path}")
        _link_to_figures(png_path)
    plt.close(fig_leg)
    return legend_path


def save_fig(fig, fig_path: Path, tight_pad: float | None = 0.1):
    fig_path = Path(fig_path)
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    if tight_pad is not None:
        fig.set_layout_engine(matplotlib.layout_engine.TightLayoutEngine(pad=tight_pad))
    fig.savefig(fig_path)
    print(f"Saved {fig_path}")
    _link_to_figures(fig_path)
    if str(fig_path).endswith(".pdf"):
        png_path = Path(str(fig_path).replace(".pdf", ".png"))
        fig.savefig(png_path, dpi=300)
        print(f"Saved {png_path}")
        _link_to_figures(png_path)
    # Release the figure now that it is written; templates that emit many
    # figures in a loop would otherwise accumulate them (matplotlib warns past
    # 20 open figures).
    plt.close(fig)
