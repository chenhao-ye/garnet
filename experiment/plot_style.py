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
LINEWIDTH = 1.2

APPEND_M_VALUES = [1, 2, 4, 8, 16, 32, 64]


def _viridis_samples(n: int):
    cmap = plt.get_cmap("viridis")
    return [cmap(i / (n - 1) * 0.9) for i in range(n)]


_append_palette = _viridis_samples(len(APPEND_M_VALUES))

color_map = {
    "single_log": "#d62728",
    "multilog_virtual": "#1f77b4",
    "multilog_physical": "#2ca02c",
    "noprefix": "#9467bd",
    "no_aof": "#1f77b4",
    "aof_single": "#d62728",
    "aof_multilog": "#2ca02c",
}
color_map.update(
    {f"multilog_m{m}": _append_palette[i] for i, m in enumerate(APPEND_M_VALUES)}
)
color_map["multilog_m1"] = color_map["single_log"]

linestyle_map = {
    "single_log": "--",
    "multilog_virtual": "-",
    "multilog_physical": "-",
    "noprefix": ":",
    "no_aof": "-",
    "aof_single": "-",
    "aof_multilog": "-",
}
linestyle_map.update({f"multilog_m{m}": "-" for m in APPEND_M_VALUES})
linestyle_map["multilog_m1"] = "--"

_marker_cycle = ["o", "s", "^", "D", "v", "P", "X"]
marker_map = {
    "single_log": None,
    "multilog_virtual": "o",
    "multilog_physical": "s",
    "noprefix": "^",
    "no_aof": "o",
    "aof_single": "s",
    "aof_multilog": "^",
}
marker_map.update(
    {
        f"multilog_m{m}": _marker_cycle[i % len(_marker_cycle)]
        for i, m in enumerate(APPEND_M_VALUES)
    }
)
marker_map["multilog_m1"] = None

labels_map = {
    "single_log": "Single Log",
    "multilog_virtual": "MultiLog-virtual",
    "multilog_physical": "MultiLog-physical",
    "noprefix": "NoPrefix",
    "no_aof": "No AOF",
    "aof_single": "Single Log AOF",
    "aof_multilog": "MultiLog AOF (k=64)",
}
labels_map.update({f"multilog_m{m}": f"MultiLog({m})" for m in APPEND_M_VALUES})
labels_map["multilog_m1"] = "Single Log"
