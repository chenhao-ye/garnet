# Garnet Experiment Framework

Python harness for running YCSB-style parameter-sweep benchmarks against the
Garnet online benchmark, parsing the output, and plotting the results.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (handles Python and package dependencies automatically)
- .NET SDK (to build and run the benchmark)

## Quick Start

```bash
# Run an experiment (server is launched and shut down automatically)
uv run experiment/run.py experiment/configs/scale_clients.yaml

# Parse results
uv run experiment/parse.py scale_clients

# Plot
uv run experiment/plot.py scale_clients
```

Outputs land in `result/scale_clients/`.

## File Layout

```
experiment/
  run.py          # run benchmark sweeps
  parse.py        # parse output.txt -> result.json
  plot.py         # plot from result.json
  configs/
    scale_clients.yaml    # example: sweep threads 1-32
    scale_batchsize.yaml  # example: sweep batchsize 1-4096
    readonly.yaml         # example: pre-load then 100% GET sweep

result/
  <experiment_name>/
    _server.log         # Garnet server stdout/stderr
    _load/              # output of the load step (if configured)
    <param>_<value>/
      config.json       # resolved parameters for this run
      output.txt        # raw benchmark stdout
    result.json         # aggregated stats (written by parse.py)
    plots/              # PDF files (written by plot.py)
```

## Experiment Config

Each experiment is described by a YAML file:

```yaml
name: scale_clients
description: "Vary number of client threads (1-32) with 70/30 GET/SET workload"
benchmark_project: benchmark/Resp.benchmark/Resp.benchmark.csproj

base_params:
  host: 127.0.0.1
  port: 6379
  online: true
  op_workload: [GET, SET]
  op_percent: [70, 30]
  dbsize: 100000
  batchsize: 128
  runtime: 30
  disable_console_logger: true

sweep:
  param: threads
  values: [1, 2, 4, 8, 16, 32]
```

- **`base_params`**: parameters shared by every run. Boolean flags (e.g. `online`,
  `zipf`) are emitted as bare flags when `true` and omitted when `false`. List
  values (e.g. `op_workload`, `op_percent`) are joined with commas.
- **`sweep`**: one parameter to vary. Each value produces one run named
  `<param>_<value>`. Omit `sweep` entirely for a single run.
- **`benchmark_project`**: path to the `.csproj` relative to the repo root.
  Defaults to `benchmark/Resp.benchmark/Resp.benchmark.csproj`.
- **`load`** *(optional)*: one-time server pre-load step run before the sweep
  (see [Pre-loading the server](#pre-loading-the-server) below).
- **`server_project`** *(optional)*: path to the GarnetServer `.csproj` relative
  to the repo root. Defaults to `main/GarnetServer/GarnetServer.csproj`.
- **`server_params`** *(optional)*: flags forwarded to the Garnet server process.
  Supported keys: `port`, `host`, `index`, `aof`, `aof_commit_freq`,
  `aof_memory_size`, `aof_page_size`, `aof_physical_sublog_count`,
  `cluster`, `tls`, `auth`. Defaults to whatever the server project uses
  when no flags are provided.

### Supported Parameters

| YAML key | CLI flag | Notes |
|---|---|---|
| `host` | `--host` | |
| `port` | `--port` | |
| `online` | `--online` | bool |
| `op_workload` | `--op-workload` | list, e.g. `[GET, SET, DEL]` |
| `op_percent` | `--op-percent` | list, must sum to 100 |
| `dbsize` | `--dbsize` | number of keys |
| `batchsize` | `--batchsize` | pipeline depth |
| `threads` | `--threads` | client thread count |
| `runtime` | `--runtime` | seconds per run |
| `disable_console_logger` | `--disable-console-logger` | bool; required for clean stdout parsing |
| `skipload` | `--skipload` | bool |
| `zipf` | `--zipf` | bool; Zipf(0.99) key distribution |
| `client` | `--client` | `LightClient`, `SERedis`, `GarnetClientSession`, etc. |
| `keylength` | `--keylength` | |
| `valuelength` | `--valuelength` | |
| `ttl` | `--ttl` | |
| `itp` | `--itp` | intra-thread parallelism |
| `aof` | `--aof` | bool |
| `aof_commit_freq` | `--aof-commit-freq` | |
| `aof_physical_sublog_count` | `--aof-physical-sublog-count` | |
| `aof_memory_size` | `--aof-memory-size` | |
| `aof_page_size` | `--aof-page-size` | |
| `cluster` | `--cluster` | bool |
| `totalops` | `--totalops` | |
| `client_hist` | `--client-hist` | bool |

## Pre-loading the server

The `--online` benchmark mode does not support `--skipload` — it has no
built-in load phase. For mixed GET/SET workloads this is fine (SET operations
create keys on the fly). For read-heavy or pure-GET workloads the keys must
exist before measuring, so a separate load step is required.

Add a `load` section to the config. Its fields override `base_params` for the
load run only. `run.py` automatically strips `online`, `skipload`, and
`disable_console_logger` from the load params (they are not meaningful for a
plain write run). The load step runs once, before any sweep iteration, and its
output is saved to `result/<name>/_load/`.

```yaml
base_params:
  host: 127.0.0.1
  port: 6379
  online: true
  op_workload: [GET]
  op_percent: [100]
  dbsize: 100000
  keylength: 8
  valuelength: 64
  batchsize: 128
  runtime: 30
  disable_console_logger: true

load:
  op: MSET
  threads: 8
  batchsize: 4096
  runtime: -1
  totalops: 100000   # should cover dbsize

sweep:
  param: threads
  values: [1, 2, 4, 8, 16, 32]
```

See `configs/readonly.yaml` for a complete example.

## run.py

```
uv run experiment/run.py <config.yaml> [--dry-run] [--no-server]
```

Full lifecycle per invocation:

1. **Kill leftovers** — `pkill -f GarnetServer` and `pkill -f Resp.benchmark`
   to eliminate processes left over from any prior run.
2. **Clean result dir** — deletes and recreates `result/<name>/` so no stale
   output files remain.
3. **Launch server** — starts `dotnet run -c Release --project
   main/GarnetServer/GarnetServer.csproj` in the background and polls the TCP
   port (default 60 s timeout) until the server accepts connections. Server
   stdout/stderr is captured to `result/<name>/_server.log`.
4. **Load step** *(if `load` section present)* — runs once before the sweep.
5. **Sweep** — runs each configuration in order, streaming output to the
   terminal and saving it to `result/<name>/<run>/output.txt`.
6. **Shutdown server** — terminates the server process (always, even on error).

Flags:
- `--dry-run` — print all commands without executing anything.
- `--no-server` — skip server launch/shutdown (use an already-running server).

## parse.py

```
uv run experiment/parse.py <experiment_name> [--warmup N]
```

Reads every `output.txt` under `result/<experiment_name>/`, locates the stats
header line (`min (us); 5th (us); ...`), and parses the 10-column data rows
printed every 2 seconds by the benchmark. The first `N` rows (default 2) are
discarded as warmup. Per-run statistics (mean, std, min, max) are computed for
each column and written to `result/<experiment_name>/result.json`.

### result.json schema

```json
{
  "experiment_name": "scale_clients",
  "sweep_param": "threads",
  "sweep_values": [1, 2, 4, 8, 16, 32],
  "warmup_rows_discarded": 2,
  "runs": {
    "threads_8": {
      "config": { ... },
      "sweep_value": 8,
      "num_samples": 13,
      "samples": [ { "tpt_kops": 512.3, "median_us": 45.1, ... }, ... ],
      "stats": {
        "tpt_kops":  { "mean": 510.2, "std": 4.1, "min": 502.0, "max": 518.5 },
        "median_us": { "mean": 45.8,  "std": 1.2, "min": 44.0,  "max": 48.3 },
        ...
      }
    }
  }
}
```

Columns: `min_us`, `p5_us`, `median_us`, `avg_us`, `p95_us`, `p99_us`,
`p999_us`, `total_ops`, `iter_ops`, `tpt_kops`.

## plot.py

```
uv run experiment/plot.py <experiment_name> [--output-dir DIR]
```

Reads `result/<experiment_name>/result.json` and writes PNG files to
`result/<experiment_name>/plots/` (or `--output-dir`).

- **throughput.pdf** / **throughput_line.pdf** -- throughput (Kops/sec) vs
  sweep parameter, with +/-1 std shading / error bars.
- **latency.pdf** / **latency_line.pdf** -- latency percentiles (median, p95,
  p99, p99.9) vs sweep parameter.

Bar charts are produced when there are 8 or fewer sweep values; line charts are
always produced. The x-axis switches to log scale automatically when the sweep
range spans more than 10x.

## Included Configs

| Config | Sweep | Workload | Load step |
|---|---|---|---|
| `configs/scale_clients.yaml` | `threads`: 1, 2, 4, 8, 16, 32 | 70% GET / 30% SET | no |
| `configs/scale_batchsize.yaml` | `batchsize`: 1, 4, 16, 64, 256, 1024, 4096 | 70% GET / 30% SET, 8 threads | no |
| `configs/readonly.yaml` | `threads`: 1, 2, 4, 8, 16, 32 | 100% GET | yes (MSET 100k keys) |
