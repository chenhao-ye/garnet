# Garnet Experiment Framework

Python harness for running YCSB-style parameter-sweep benchmarks against the
Garnet online benchmark, parsing the output, and plotting the results.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (handles Python and package dependencies automatically)
- .NET SDK (to build and run the benchmark)
- A running Garnet server (see below)

## Quick Start

```bash
# 1. Start a Garnet server
dotnet run -c Release --project main/GarnetServer/ -- --port 6379

# 2. Run an experiment
uv run experiment/run.py experiment/configs/scale_clients.yaml

# 3. Parse results
uv run experiment/parse.py scale_clients

# 4. Plot
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

result/
  <experiment_name>/
    <param>_<value>/
      config.json   # resolved parameters for this run
      output.txt    # raw benchmark stdout
    result.json     # aggregated stats (written by parse.py)
    plots/          # PNG files (written by plot.py)
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

## run.py

```
uv run experiment/run.py <config.yaml> [--dry-run]
```

Iterates over sweep values, builds the `dotnet run` command for each, streams
output to the terminal and saves it to `result/<name>/<run>/output.txt`.
`--dry-run` prints the commands without executing them.

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

| Config | Sweep | Workload |
|---|---|---|
| `configs/scale_clients.yaml` | `threads`: 1, 2, 4, 8, 16, 32 | 70% GET / 30% SET |
| `configs/scale_batchsize.yaml` | `batchsize`: 1, 4, 16, 64, 256, 1024, 4096 | 70% GET / 30% SET, 8 threads |
