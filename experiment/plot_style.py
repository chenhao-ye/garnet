"""Shared plotting style for MultiLog paper figures.

Color/linestyle/marker maps keyed by curve identity, plus rcParams and
column-width constants. Imported by plot_append.py, plot_replay.py, etc.
"""

import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
matplotlib.rcParams["font.size"] = 7
matplotlib.rcParams["hatch.linewidth"] = 0.3

plt.rcParams["xtick.major.pad"] = "2"
plt.rcParams["ytick.major.pad"] = "2"
plt.rcParams["xtick.major.size"] = "3"
plt.rcParams["ytick.major.size"] = "3"
plt.rcParams["xtick.minor.size"] = "1.5"
plt.rcParams["ytick.minor.size"] = "1.5"
plt.rcParams["axes.labelpad"] = "1"
plt.rcParams.update({"mathtext.default": "regular"})

# ACM/USENIX template, unit: inch
DOUBLE_COLUMN_WIDTH = 7
COLUMN_SEP = 0.33
SINGLE_COLUMN_WIDTH = (DOUBLE_COLUMN_WIDTH - COLUMN_SEP) / 2

MARKER_SIZE = 2.5
LEGEND_MARKER_SIZE = 5
LINEWIDTH = 1

# Shared legend defaults. Render functions pass `dict(LEGEND_KWARGS, ncol=N,
# ...)` to twist ncol and any other knob per figure.
LEGEND_KWARGS = dict(
    frameon=False,
    columnspacing=0.8,
    handlelength=1.2,
    handletextpad=0.4,
    labelspacing=0.3,
    borderpad=0.1,
)

APPEND_M_VALUES = [1, 2, 4, 8, 16, 32, 64]


def _sample_cmap(cmap_name: str, t: float):
    """Return one color from a matplotlib colormap at position t in [0, 1]."""
    return plt.get_cmap(cmap_name)(t)


# autumn goes red (t=0) -> yellow (t=1). Tune individual t per key as needed.
MULTILOG_CMAP = "autumn"

color_map = {
    "single_log": "#2c7bb6",
    "multilog_virtual": "#FF8C8E",
    "multilog_physical": _sample_cmap(MULTILOG_CMAP, 0.00),
    "multilog_hybrid": "#FF8C8E",
    "multilog_direct": _sample_cmap(MULTILOG_CMAP, 0.00),
    "noprefix": "#9467bd",
    "noprefix_m64": "#9467bd",
    "noprefix_physical": "#9467bd",
    "no_aof": "#abd9e9",
    "aof_single": "#2c7bb6",
    "aof_multilog": "red",
    "multilog_m2": _sample_cmap(MULTILOG_CMAP, 0.90),
    "multilog_m4": _sample_cmap(MULTILOG_CMAP, 0.75),
    "multilog_m8": _sample_cmap(MULTILOG_CMAP, 0.6),
    "multilog_m16": _sample_cmap(MULTILOG_CMAP, 0.45),
    "multilog_m32": _sample_cmap(MULTILOG_CMAP, 0.3),
    "multilog_m64": _sample_cmap(MULTILOG_CMAP, 0.00),
}

linestyle_map = {
    "single_log": "--",
    "multilog_virtual": "-",
    "multilog_physical": "-",
    "multilog_hybrid": "-",
    "multilog_direct": "-",
    "noprefix": ":",
    "noprefix_m64": ":",
    "noprefix_physical": ":",
    "no_aof": "-",
    "aof_single": "-",
    "aof_multilog": "-",
    # higher m -> less broken; m=64 fully solid.
    # "multilog_m2": (0, (1, 3)),
    # "multilog_m4": (0, (2, 2)),
    # "multilog_m8": (0, (4, 2)),
    # "multilog_m16": (0, (6, 2)),
    # "multilog_m32": (0, (10, 2)),
    "multilog_m2": "-",
    "multilog_m4": "-",
    "multilog_m8": "-",
    "multilog_m16": "-",
    "multilog_m32": "-",
    "multilog_m64": "-",
}

marker_map = {
    "single_log": None,
    "multilog_virtual": "v",
    "multilog_physical": "o",
    "multilog_hybrid": "v",
    "multilog_direct": "o",
    "noprefix": ".",
    "noprefix_m64": ".",
    "noprefix_physical": ".",
    "no_aof": "d",
    "aof_single": ".",
    "aof_multilog": "o",
    "multilog_m2": "^",
    "multilog_m4": "v",
    "multilog_m8": "D",
    "multilog_m16": "s",
    "multilog_m32": "x",
    "multilog_m64": "o",
}

labels_map = {
    "single_log": "SingleLog",
    "multilog_virtual": "MultiLog(1,x)",
    "multilog_physical": "MultiLog(x)",
    "multilog_hybrid": "Hybrid mode (m,n)",
    "multilog_direct": "Direct mode (m)",
    "noprefix": "NoPrefix",
    "noprefix_m64": "NoPrefix(64)",
    "noprefix_physical": "NoPrefix(x)",
    "no_aof": "Garnet w/o Log",
    "aof_single": "Garnet w/ Log",
    "aof_multilog": "Garnet w/ MultiLog(64)",
}
labels_map.update(
    {f"multilog_m{m}": f"MultiLog({m})" for m in APPEND_M_VALUES if m != 1}
)

# Smaller m sits in front so the lighter (yellow) curves aren't buried under
# the deeper-red high-m curves where they overlap. m=1 is Single Log baseline.
_multilog_m_family = [m for m in APPEND_M_VALUES if m != 1]
zorder_map = {
    f"multilog_m{m}": 10 - i for i, m in enumerate(_multilog_m_family)
}
# Single Log baseline (slate blue, dashed) is the reference -- keep it on top.
zorder_map["single_log"] = 11
zorder_map["multilog_m1"] = 11
# NoPrefix is the upper-bound reference; render it above Single Log so the
# gap to MultiLog stays visible where the two cross.
zorder_map["noprefix"] = 12
zorder_map["noprefix_m64"] = 12
zorder_map["noprefix_physical"] = 12
