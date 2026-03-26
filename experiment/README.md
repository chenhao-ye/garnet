# Garnet Experiment Framework

Python harness for running parameter-sweep benchmarks against Resp.benchmark
in online, offline, and AOF modes, then parsing the output and plotting the
results.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (handles Python and package dependencies automatically)
- .NET SDK (to build and run the benchmark)

## Quick Start

```bash
# Validate config semantics before running
uv run experiment/check.py scale_clients

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
  check.py        # validate config semantics against Resp.benchmark
  run.py          # run benchmark sweeps
  parse.py        # parse benchmark output -> result.yaml
  plot.py         # plot from result.yaml
  configs/
    scale_clients.yaml    # example: sweep threads 1-32
    scale_batchsize.yaml  # example: sweep batchsize 1-4096
    readonly.yaml         # example: pre-load then 100% GET sweep
    aof_bench_clients.yaml  # example: vary sublog setup, sweep threads
    aof_replay_matrix.yaml  # example: sweep physical sublogs x replay tasks

result/
  <experiment_name>/
    config.yaml         # original experiment config snapshot
    <run_name>/
      _server.log       # Garnet server stdout/stderr for this run
      config.yaml       # resolved parameters for this run
      prepare/
        output.txt      # raw prepare-step stdout (if configured)
      benchmark/
        output.txt      # raw benchmark stdout
    result.yaml         # aggregated stats (written by parse.py)
    plots/              # PDF files (written by plot.py)
```

## Experiment Config

Each experiment is described by a YAML file:

```yaml
name: scale_clients
description: "Vary number of client threads (1-32) with 70/30 GET/SET workload"
benchmark: online
benchmark_project: benchmark/Resp.benchmark/Resp.benchmark.csproj

base:
  client_params:
    client: GarnetClientSession
    host: 127.0.0.1
    port: 6379
    online: true
    op_workload: [GET, SET]
    op_percent: [70, 30]
    dbsize: 100000
    batchsize: 128
    runtime: 30
    disable_console_logger: true
  server_params:
    bind: 127.0.0.1
    port: 6379

sweep:
  client_params:
    threads: [1, 2, 4, 8, 16, 32]
```

- **`prepare`** *(optional)*: one benchmark-client-only step run after server
  startup and before the benchmark step for every run. It supports only
  `client_params` and is resolved independently from `base.client_params`.
- **`base`**: shared defaults for each run. It may contain `client_params` and
  `server_params`.
- **`sweep`** *(optional)*: parameter lists to vary. It may contain
  `client_params` and `server_params`, and the runner executes the Cartesian
  product across all listed values. Omit `sweep` or leave both subsections
  empty to execute a single run.
- **`benchmark_project`**: path to the `.csproj` relative to the repo root.
  Defaults to `benchmark/Resp.benchmark/Resp.benchmark.csproj`.
- **`server_project`** *(optional)*: path to the GarnetServer `.csproj` relative
  to the repo root. Defaults to `main/GarnetServer/GarnetServer.csproj`.
- **`benchmark`**: one of `online`, `offline`, or `aof`.
- **`no_server`** *(optional)*: skip external server launch/shutdown for embedded
  benchmark modes such as AOF `InProc`.

Boolean flags (e.g. `online`, `zipf`) are emitted as bare flags when `true` and
omitted when `false`. List-valued client parameters such as `op_workload` and
`op_percent` are joined with commas before invoking the benchmark client.

The runner no longer relies on Resp.benchmark’s own sweep behavior to create
multiple runs. Each experiment run is resolved explicitly in Python from the
`sweep` Cartesian product.

## Prepare Step

The `--online` benchmark mode does not support `--skipload`, so read-heavy
workloads still need a write phase before measurement. Use `prepare` for that.

`prepare.client_params` is used as-is, then `run.py` automatically strips
`online`, `skipload`, and
`disable_console_logger` because they are not meaningful for the prepare write
step. If the prepare phase needs non-default client settings such as `client`,
`dbsize`, `keylength`, or `valuelength`, specify them explicitly. The prepare
step runs once per run, and its output is saved to
`result/<name>/<run_name>/prepare/output.txt`.

```yaml
prepare:
  client_params:
    client: GarnetClientSession
    op: MSET
    threads: 8
    runtime: -1
    totalops: 100000
    dbsize: 100000
    keylength: 8
    valuelength: 64

base:
  client_params:
    online: true
    op_workload: [GET]
    op_percent: [100]
    dbsize: 100000
    keylength: 8
    valuelength: 64
    batchsize: 128
    runtime: 30
    disable_console_logger: true
  server_params:
    port: 6379

sweep:
  client_params:
    threads: [1, 2, 4, 8, 16, 32]
```

See `configs/readonly.yaml` for a complete example.

## run.py

```
uv run experiment/run.py <config.yaml> [--dry-run]
```

Full lifecycle per invocation:

1. **Kill leftovers** — `pkill -f GarnetServer` and `pkill -f Resp.benchmark`
   to eliminate processes left over from any prior run.
2. **Clean result dir** — deletes and recreates `result/<name>/` so no stale
   output files remain.
3. **Expand sweep** — compute the Cartesian product across all
   `sweep.client_params.*` and `sweep.server_params.*` value lists.
4. **Per run**:
   launch the server, wait for readiness, run `prepare` if configured, run the
   benchmark, then shut the server down.
5. **Store outputs** — each run gets its own resolved `config.yaml`,
   `prepare/output.txt`, `benchmark/output.txt`, and `_server.log`.

Flags:
- `--dry-run` — print all commands without executing anything.

## check.py

```
uv run experiment/check.py <experiment_name> [--config PATH]
```

Validates that an experiment config maps cleanly onto `Resp.benchmark`:

- catches unknown client parameters that do not map to any benchmark option
- catches benchmark-label mismatches such as `benchmark: online` without
  `online: true` in `client_params`
- warns when a parameter is accepted by `Resp.benchmark` but ignored or
  overridden in the selected mode, such as `batchsize != 1` for online mode or
  `threads` in AOF replay mode

Exit code is `1` if any errors are found, otherwise `0`.

## parse.py

```
uv run experiment/parse.py <experiment_name> [--warmup N]
```

Reads every `benchmark/output.txt` under `result/<experiment_name>/`, locates
the stats header line (`min (us); 5th (us); ...`), and parses the benchmark
rows. The first `N` rows (default 2) are discarded as warmup. Per-run
statistics (mean, std, min, max) are computed for each column and written to
`result/<experiment_name>/result.yaml`. A human-friendly tabular summary is also
written to `result/<experiment_name>/summary.txt`.

### result.yaml schema

```yaml
experiment_name: scale_clients
sweep_params:
  client.threads: [1, 2, 4, 8, 16, 32]
warmup_rows_discarded: 2
runs:
  threads_8:
    config: {...}
    num_samples: 13
    samples:
      - {tpt_kops: 512.3, median_us: 45.1, ...}
    stats:
      tpt_kops: {mean: 510.2, std: 4.1, min: 502.0, max: 518.5}
      median_us: {mean: 45.8, std: 1.2, min: 44.0, max: 48.3}
      ...
```

Columns: `min_us`, `p5_us`, `median_us`, `avg_us`, `p95_us`, `p99_us`,
`p999_us`, `total_ops`, `iter_ops`, `tpt_kops`.

## plot.py

```
uv run experiment/plot.py <experiment_name> [--output-dir DIR]
```

Reads `result/<experiment_name>/result.yaml` and writes PNG files to
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
| `configs/scale_clients.yaml` | `client_params.threads`: 1, 2, 4, 8, 16, 32 | 70% GET / 30% SET | no |
| `configs/setonly_lightclient.yaml` | `client_params.threads`: 1, 2, 4, 8, 16, 32 | offline 100% SET with `LightClient` | no |
| `configs/scale_batchsize.yaml` | `client_params.batchsize`: 1, 4, 16, 64, 256, 1024, 4096 | 70% GET / 30% SET, 8 threads | no |
| `configs/readonly.yaml` | `client_params.threads`: 1, 2, 4, 8, 16, 32 | 100% GET | yes, per-run `prepare` with MSET |
| `configs/aof_bench_clients.yaml` | `server_params.aof_physical_sublog_count`: 1, 8 and `client_params.threads`: 1, 2, 4, 8, 16, 32 | online SET workload, null-device AOF | launches server |
| `configs/aof_bench_sublog.yaml` | `client_params.aof_replay_task_count`: 1, 2, 4, 8 | AofBench Replay, InProc, null device | no server |
| `configs/aof_replay_matrix.yaml` | `client_params.aof_physical_sublog_count`: 1, 2, 4, 8, 16, 32 and `client_params.aof_replay_task_count`: 1, 2, 4, 8, 16, 32 | AofBench Replay, InProc, null device | no server |
