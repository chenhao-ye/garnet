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
from plot_style import DOUBLE_COLUMN_WIDTH, SINGLE_COLUMN_WIDTH

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"
CONFIG_ROOT = Path(__file__).resolve().parent / "configs"


def load_result(experiment: str) -> dict:
    path = RESULT_ROOT / experiment / "result.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"result.yaml not found at {path}.\n"
            f"Run `uv run experiment/parse.py {experiment}` to generate it."
        )
    with open(path) as f:
        return yaml.safe_load(f)


def load_plot_config(experiment: str) -> dict:
    """Load the `plot:` section from configs/<experiment>.yaml.

    Returns {} if the config file is missing or has no `plot:` section."""
    path = CONFIG_ROOT / f"{experiment}.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("plot") or {}


def _run_sweep_params(run_entry: dict) -> dict:
    sweep = run_entry.get("sweep_params")
    if sweep is None:
        sweep = run_entry.get("config", {}).get("sweep_params") or {}
    return sweep


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
    for _name, entry in result["runs"].items():
        sweep = _run_sweep_params(entry)
        if x_param not in sweep:
            continue
        if not all(sweep.get(k) == v for k, v in filter_params.items()):
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
    nrows: int, ncols: int, hw_ratio: float = 1.0, width_scale: float = 1.0, **kwargs
):
    total_width = SINGLE_COLUMN_WIDTH * width_scale
    subplot_width = total_width / ncols
    subplot_height = subplot_width * hw_ratio
    return build_fig(nrows, ncols, total_width, subplot_height * nrows, **kwargs)


def build_fig_double_col(
    nrows: int, ncols: int, hw_ratio: float = 1.0, width_scale: float = 1.0, **kwargs
):
    total_width = DOUBLE_COLUMN_WIDTH * width_scale
    subplot_width = total_width / ncols
    subplot_height = subplot_width * hw_ratio
    return build_fig(nrows, ncols, total_width, subplot_height * nrows, **kwargs)


def save_fig(fig, fig_path: Path, tight_pad: float | None = 0.1):
    fig_path = Path(fig_path)
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    if tight_pad is not None:
        fig.set_layout_engine(matplotlib.layout_engine.TightLayoutEngine(pad=tight_pad))
    fig.savefig(fig_path)
    print(f"Saved {fig_path}")
    if str(fig_path).endswith(".pdf"):
        png_path = Path(str(fig_path).replace(".pdf", ".png"))
        fig.savefig(png_path, dpi=300)
        print(f"Saved {png_path}")
