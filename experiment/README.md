# Garnet Experiment Harness

A config-driven toolkit for benchmarking Garnet. One YAML file under
`configs/` describes an experiment; the harness expands it into a parameter
sweep against `Resp.benchmark`, launches the server and client for each run,
parses the raw output into aggregated stats, and renders paper-ready figures.

```
configs/<name>.yaml ──check.py──> (validate)
                    ──run.py────> result/<name>/<run>/...  (raw output)
                    ──parse.py──> result/<name>/result.yaml + summary.txt
plot_configs/<name>.yaml ──plot.py──> result/<name>/<name>.pdf + .png
```

## Prerequisites

- [uv](https://docs.astral.sh/uv/) -- runs the scripts and manages the Python
  deps (`matplotlib`, `numpy`, `pyyaml`) declared in `code/pyproject.toml`.
  Each script has a `#!/usr/bin/env -S uv run` shebang.
- .NET SDK (net10.0) -- to build and run `Resp.benchmark` and `GarnetServer`.
- `numactl` -- only if a config uses the `affinity` block (NUMA pinning).

All commands below are run from the repo root (`code/`).

## Quick Start

```bash
# 1. Validate config semantics before spending machine time
uv run experiment/check.py online_set

# 2. Run the sweep (server is launched and shut down automatically;
#    parse.py runs at the end so result.yaml/summary.txt are produced)
uv run experiment/run.py online_set

# 3. (Optional) re-parse with a different warmup
uv run experiment/parse.py online_set --warmup 3

# 4. Render a figure from a plot config that depends on this experiment
uv run experiment/plot.py set_scaling
```

Each script accepts a bare config name (resolved to
`experiment/configs/<name>.yaml`) or an explicit path, and takes multiple
names to process sequentially:

```bash
uv run experiment/run.py online_set online_set_aof online_set_multilog
```

## Scripts

| Script | Role |
|--------|------|
| `check.py` | Static validation of a config against `Resp.benchmark`'s `Options.cs` -- flags unknown params and params that are silently ignored/overridden for the chosen mode. Exits non-zero on any ERROR. |
| `run.py` | Expands the sweep, launches server + client per run, captures raw output, then auto-invokes `parse.py`. |
| `parse.py` | Parses each run's `benchmark/output.txt` into per-metric stats (median/mean/std/min/max), writes `result.yaml` and a human-readable `summary.txt`. |
| `plot.py` | Renders a figure from a `plot_configs/<name>.yaml`, pulling series out of one or more experiments' `result.yaml`. |
| `config.py` | Shared library (not a CLI): config loading, sweep expansion, run-name encoding, path resolution. |
| `plot_util.py` / `plot_style.py` | Shared plotting helpers: `result.yaml` loaders, figure factory, save/legend helpers, and the color/marker/label style maps. |

## Experiment Config Schema

Configs live in `experiment/configs/`. Minimal example (`online_set.yaml`):

```yaml
name: online_set                  # must match the filename stem (check.py enforces)
description: "..."                 # free text
benchmark: online                 # online | offline | aof  (selects the parser)

affinity:                         # optional NUMA pinning (numactl); omit to skip
  server: { numa_node: 0 }
  client: { numa_node: 1 }        # prepare inherits client's affinity unless set

base:                             # parameters common to every run
  client_params:                  # passed to Resp.benchmark as CLI flags
    client: GarnetClientSession
    online: true
    op_workload: [SET]
    op_percent: [100]
    dbsize: 10000000
    runtime: 60
    disable_console_logger: true
  server_params:                  # passed to GarnetServer (omit for no_server)
    aof_memory: 256m

sweep:                            # Cartesian product across listed dimensions
  client_params:
    threads: [1, 2, 4, 8, 16, 32, 64]
```

### Top-level keys

| Key | Default | Meaning |
|-----|---------|---------|
| `name` | filename stem | Experiment name; also the `result/<name>/` directory. |
| `description` | -- | Free text, echoed into the config snapshot. |
| `benchmark` | **required** | `online`, `offline`, or `aof`; selects the output parser. |
| `benchmark_project` | `benchmark/Resp.benchmark/Resp.benchmark.csproj` | Client project to `dotnet run`. |
| `server_project` | `main/GarnetServer/GarnetServer.csproj` | Server project to `dotnet run`. |
| `no_server` | `false` | If true, the server is not launched (e.g. the embedded `InProc` AofBench creates its own). `server_params` are then ignored (check.py warns). |
| `repeat` | `1` | Run each combo `repeat` times; `parse.py` splits the samples into `repeat` chunks and emits one median row per chunk. |
| `affinity` | none | NUMA pinning per role (see below). |
| `prepare` | none | An extra benchmark invocation run *before* the main benchmark of every run (e.g. to preload keys). |
| `base` | **required** | `client_params` (required) and `server_params` (optional) shared by all runs. |
| `sweep` *or* `sweep_combo` | none | The parameter sweep (mutually exclusive). |

### `sweep` vs `sweep_combo`

- **`sweep`** takes the Cartesian product of every listed dimension. The
  example above yields 7 runs. Adding a second dimension multiplies them
  (`aof_enqueue_sharded.yaml` sweeps `aof_physical_sublog_count` x `threads` =
  49 runs).
- **`sweep_combo`** is an explicit list of parameter tuples -- use it to walk a
  diagonal instead of a grid. `aof_replay_spectrum_64.yaml` walks the
  `m * n = 64` diagonal: `(1,64), (2,32), ... (64,1)`.

```yaml
sweep_combo:
  - client_params: { aof_physical_sublog_count: 1, aof_replay_task_count: 64 }
  - client_params: { aof_physical_sublog_count: 2, aof_replay_task_count: 32 }
  # ...
```

Both scopes (`client_params`, `server_params`) may appear in a sweep. Each
swept value becomes part of the run directory name, e.g.
`c.threads.16` or `c.aof_physical_sublog_count.8-c.aof_replay_task_count.4`
(prefix `c` = client, `s` = server). A run with no swept params is named
`default`.

### Parameter -> CLI flag translation (`run.py`)

- Snake_case keys become dashed flags: `op_workload` -> `--op-workload`.
- **List params** (`op_workload`, `op_percent`, `batchsize`, `threads`) are
  joined with commas: `op_workload: [GET, SET]` -> `--op-workload GET,SET`.
- **Bool params** are emitted as bare flags only when truthy: `online: true`
  -> `--online`; `online: false` emits nothing.
- Everything else is `--flag value`.

The full server command is
`dotnet run -c Release --framework net10.0 --project <project> -- <flags>`,
optionally prefixed with `numactl` when affinity is set.

### Affinity

```yaml
affinity:
  server:  { numa_node: 0 }              # --cpunodebind=0 --membind=0
  client:  { numa_node: 1, cpus: "8-15" }  # --physcpubind=8-15 --membind=1
  # prepare: defaults to the client spec if omitted
```

`cpus` (a `numactl --physcpubind` string) takes precedence over `numa_node`
for CPU binding; memory is always bound to `numa_node`.

## `run.py` Lifecycle

Per invocation, for each config:

1. **Kill leftovers** -- `pkill -f` the server and benchmark project stems.
2. **Wipe results** -- `rm -rf result/<name>/` then recreate it (no stale data).
3. **Snapshot** the experiment config to `result/<name>/config.yaml`.
4. **Expand** the sweep into runs.
5. For each run:
   a. Launch the server (skipped if `no_server`), wait until its
      host:port accepts a TCP connection (60s timeout).
   b. Run the `prepare` step if configured.
   c. Run the benchmark step.
   d. Shut the server down (terminate, then kill after 10s).
6. **Auto-parse** by calling `parse.py` on the config.

`--dry-run` prints every command without executing or touching the filesystem.

If the server process exits unexpectedly mid-run, the client is killed and the
run aborts with an error.

## Benchmark Modes & Parsers (`parse.py`)

The `benchmark` field selects the parser. `--warmup N` (default 5) discards the
first N samples (online mode only); other modes emit one sample per labeled
block.

| Mode | Output format parsed | Metric columns (in `result.yaml`) |
|------|----------------------|-----------------------------------|
| `online` | Tabular `RespOnlineBench` rows after the `min (us)` header | latency percentiles (`min_us`..`p999_us`), `total_ops`, `iter_ops`, `tpt_mops` (throughput in Mops/s) |
| `offline` | `Operation type:` / `Total time` / `Throughput` blocks | `time_ms`, `total_ops`, `throughput` (Mops/s) |
| `aof` | Labeled `[name]: value` blocks per `Total time` sample | `time_ms`, `bytes`, `bandwidth` (GiB/s), `throughput` (M records/s), plus reader latency percentiles when present |

For each metric column, `parse.py` records `median`, `mean`, `std`, `min`,
`max` across the kept samples.

### Outputs

- **`result.yaml`** -- machine-readable; consumed by `plot.py`. Shape:
  ```yaml
  experiment_name: online_set
  sweep_params: { client.threads: [1, 2, 4, ...] }
  warmup_rows_discarded: 5
  runs:
    c.threads.16:
      benchmark: online
      config: { ... }          # the run's resolved config.yaml
      num_samples: 55
      samples: [ {...}, ... ]
      stats: { tpt_mops: {median: ..., mean: ..., std: ...}, ... }
  ```
- **`summary.txt`** -- human-readable text table, grouped by benchmark mode,
  one row per run with the swept params and headline metrics.

## `result/` Layout

```
result/
  <experiment>/
    config.yaml             # snapshot of the experiment config (by run.py)
    result.yaml             # aggregated stats (by parse.py)
    summary.txt             # text table (by parse.py)
    <run_name>/             # e.g. c.threads.16
      _server.log           # GarnetServer stdout/stderr for this run
      config.yaml           # resolved params + exact commands for this run
      prepare/output.txt    # raw prepare-step stdout (if a prepare step ran)
      benchmark/output.txt  # raw benchmark stdout (parsed by parse.py)
  <plot_name>/
    <plot_name>.pdf / .png  # rendered figure (by plot.py)
    <plot_name>_legend.*    # standalone legend (if legend_separate: true)
  figures/                  # flat mirror: <experiment>-<filename> symlinks to
                            # every rendered figure for easy collection
```

## Plotting (`plot.py` + `plot_configs/`)

Figures are described by `plot_configs/<name>.yaml`. A plot config picks a
`template` (renderer) and lists the experiments it draws from via
`dependencies`; `plot.py` reads those experiments' `result.yaml` files.

```yaml
name: set_scaling
template: set                     # replay | append | set
dependencies: [online_set, online_set_aof]
scale: 0.5                        # figure width as a fraction of column width
xticks: [1, 8, 16, 32, 64]
yticks: [0, 20, 40, 60, 80, 100]
legend_separate: true             # write the legend to its own file
```

### Templates

| Template | Dependencies (ordered) | Figure |
|----------|------------------------|--------|
| `set` | `[no_aof, aof_single]` | Online SET throughput vs. threads; one curve per dependency. |
| `append` | `[<single experiment>]` | Append throughput vs. threads; one curve per `aof_physical_sublog_count` value (the `m` family). |
| `replay` | `[<physical sweep>, <virtual sweep>]` | Replay throughput vs. sublog count; physical and virtual curves plus a Single-Log reference line taken from the `m=1` point of the physical sweep. |

### Axis styling keys

Recognized in any plot config: `scale`, `xscale`/`yscale` (`log` on x uses
base 2), `xticks`/`yticks` (+ `_minor` variants), `xmin`/`xmax`/`ymin`/`ymax`
(min defaults to 0), `xlabel`/`ylabel`, `xgrid`/`ygrid` (default on),
`legend_separate`.

Curve identity (color, marker, linestyle, label) is fixed centrally in
`plot_style.py` -- e.g. `single_log` is slate blue, the `multilog_m{2,4,...}`
family walks the `autumn` colormap. Column widths follow an ACM/USENIX
two-column template.

Outputs land in `result/<plot_name>/<plot_name>.pdf` (plus a 300-DPI `.png`),
and a symlink is mirrored into `result/figures/` for easy collection.
`plot.py` aborts with a clear message listing any dependency whose
`result.yaml` is missing -- run `parse.py` for those first.

## Adding an Experiment

1. Copy an existing config in `configs/` whose `benchmark` mode matches, set a
   unique `name` (== filename stem), and edit `base`/`sweep`.
2. `uv run experiment/check.py <name>` until it reports `OK` (or only expected
   warnings).
3. `uv run experiment/run.py <name>` -- this also parses.
4. To plot, add a `plot_configs/<plot>.yaml` listing your experiment(s) under
   `dependencies` with the right `template`, then
   `uv run experiment/plot.py <plot>`.
