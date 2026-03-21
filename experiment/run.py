#!/usr/bin/env -S uv run

"""
Run a YCSB-style experiment against the Garnet online benchmark.

Lifecycle per invocation:
  1. Kill leftover server / benchmark processes from any previous run.
  2. Delete this experiment's result directory to avoid stale data.
  3. Build and launch the Garnet server; wait until it accepts connections.
  4. Optionally run a one-time load step (see `load` in config).
  5. Run each sweep configuration; stream + save output.
  6. Shut down the server (always, even on error).

Usage:
    uv run experiment/run.py scale_clients
    uv run experiment/run.py scale_clients --dry-run
    uv run experiment/run.py scale_clients --no-server
    uv run experiment/run.py scale_clients --config path/to/custom.yaml
"""

import argparse
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"

DEFAULT_SERVER_PROJECT = "main/GarnetServer/GarnetServer.csproj"
SERVER_READY_TIMEOUT = 60  # seconds to wait for server TCP port
SERVER_READY_INTERVAL = 0.5


# ---------------------------------------------------------------------------
# Config -> CLI flag mapping
# ---------------------------------------------------------------------------

LIST_PARAMS = {"op_workload", "op_percent", "batchsize", "threads"}

BOOL_PARAMS = {
    "online",
    "disable_console_logger",
    "skipload",
    "burst",
    "zipf",
    "lset",
    "pool",
    "tls",
    "aof",
    "cluster",
    "aof_null_device",
    "client_hist",
    "aof_bench",
}

PARAM_TO_FLAG = {
    "host": "--host",
    "port": "--port",
    "online": "--online",
    "op_workload": "--op-workload",
    "op_percent": "--op-percent",
    "dbsize": "--dbsize",
    "batchsize": "--batchsize",
    "threads": "--threads",
    "runtime": "--runtime",
    "disable_console_logger": "--disable-console-logger",
    "skipload": "--skipload",
    "burst": "--burst",
    "zipf": "--zipf",
    "lset": "--lset",
    "client": "--client",
    "pool": "--pool",
    "tls": "--tls",
    "auth": "--auth",
    "keylength": "--keylength",
    "valuelength": "--valuelength",
    "ttl": "--ttl",
    "itp": "--itp",
    "aof": "--aof",
    "aof_commit_freq": "--aof-commit-freq",
    "aof_physical_sublog_count": "--aof-physical-sublog-count",
    "aof_replay_task_count": "--aof-replay-task-count",
    "aof_memory_size": "--aof-memory-size",
    "aof_page_size": "--aof-page-size",
    "aof_null_device": "--aof-null-device",
    "cluster": "--cluster",
    "totalops": "--totalops",
    "client_hist": "--client-hist",
    "aof_bench": "--aof-bench",
    "aof_bench_type": "--aof-bench-type",
    "index": "--index",
}

# Server-specific flag mapping (GarnetServer CLI)
SERVER_PARAM_TO_FLAG = {
    "port": "--port",
    "host": "--bind",
    "index": "--index",
    "aof": "--aof",
    "aof_commit_freq": "--aof-commit-freq",
    "aof_memory_size": "--aof-memory",
    "aof_page_size": "--aof-page-size",
    "aof_physical_sublog_count": "--aof-physical-sublog-count",
    "aof_null_device": "--aof-null-device",
    "cluster": "--cluster",
    "tls": "--tls",
    "auth": "--auth",
}

SERVER_BOOL_FLAGS = {"aof", "aof_null_device", "cluster", "tls"}


def params_to_args(params: dict) -> list[str]:
    args = []
    for key, value in params.items():
        flag = PARAM_TO_FLAG.get(key)
        if flag is None:
            print(f"  [warn] unknown param '{key}', skipping", file=sys.stderr)
            continue
        if key in BOOL_PARAMS:
            if value:
                args.append(flag)
        elif key in LIST_PARAMS:
            if isinstance(value, list):
                args += [flag, ",".join(str(v) for v in value)]
            else:
                args += [flag, str(value)]
        else:
            args += [flag, str(value)]
    return args


def server_params_to_args(params: dict) -> list[str]:
    args = []
    for key, value in params.items():
        flag = SERVER_PARAM_TO_FLAG.get(key)
        if flag is None:
            continue
        if key in SERVER_BOOL_FLAGS:
            if value:
                args.append(flag)
        else:
            args += [flag, str(value)]
    return args


def build_command(benchmark_project: str, params: dict) -> list[str]:
    project_path = REPO_ROOT / benchmark_project
    cmd = [
        "dotnet",
        "run",
        "-c",
        "Release",
        "--framework",
        "net10.0",
        "--project",
        str(project_path),
        "--",
    ]
    cmd += params_to_args(params)
    return cmd


# ---------------------------------------------------------------------------
# Process / directory cleanup
# ---------------------------------------------------------------------------


def killall_leftover(
    server_project: str, benchmark_project: str, dry_run: bool
) -> None:
    """Kill any leftover server or benchmark dotnet processes from a prior run."""
    patterns = [
        Path(server_project).stem,
        Path(benchmark_project).stem,
    ]
    for pat in patterns:
        cmd = ["pkill", "-f", pat]
        print(f"  [cleanup] pkill -f {pat}")
        if not dry_run:
            subprocess.run(cmd, check=False)
    if not dry_run:
        time.sleep(1)  # give OS time to reap processes


def cleanup_result_dir(exp_dir: Path, dry_run: bool) -> None:
    """Remove the experiment result directory so no stale data remains."""
    if exp_dir.exists():
        print(f"  [cleanup] removing {exp_dir}")
        if not dry_run:
            shutil.rmtree(exp_dir)
    if not dry_run:
        exp_dir.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------


def launch_server(
    server_project: str, server_params: dict, log_path: Path, dry_run: bool
):
    """Start the Garnet server in the background. Returns the Popen handle."""
    project_path = REPO_ROOT / server_project
    cmd = [
        "dotnet",
        "run",
        "-c",
        "Release",
        "--framework",
        "net10.0",
        "--project",
        str(project_path),
        "--",
    ]
    cmd += server_params_to_args(server_params)

    print(f"\n  [server] launching: {' '.join(cmd)}")
    if dry_run:
        print("  [server] [dry-run] skipping launch")
        return None

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(log_path, "w")
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(log_path.parent),
    )
    return proc


def wait_for_server(host: str, port: int, dry_run: bool, proc=None) -> None:
    """Poll until the server accepts TCP connections or timeout expires."""
    if dry_run:
        return
    deadline = time.time() + SERVER_READY_TIMEOUT
    print(f"  [server] waiting for {host}:{port} ...", end="", flush=True)
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            print()
            raise RuntimeError(
                f"Server process exited unexpectedly (code {proc.returncode}) "
                f"before becoming ready on {host}:{port}"
            )
        try:
            with socket.create_connection((host, port), timeout=1):
                print(" ready.")
                return
        except OSError:
            print(".", end="", flush=True)
            time.sleep(SERVER_READY_INTERVAL)
    print()
    raise TimeoutError(
        f"Server did not become ready on {host}:{port} within {SERVER_READY_TIMEOUT}s"
    )


def shutdown_server(proc, dry_run: bool) -> None:
    """Terminate the server process and wait for it to exit."""
    if dry_run or proc is None:
        return
    print("\n  [server] shutting down...")
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    print("  [server] stopped.")


# ---------------------------------------------------------------------------
# Benchmark runs
# ---------------------------------------------------------------------------


def run_single(
    run_name: str,
    run_dir: Path,
    cmd: list[str],
    config: dict,
    dry_run: bool,
    server_proc=None,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "config.yaml", "w") as f:
        yaml.dump(config, f)

    print(f"\n{'=' * 60}")
    print(f"  Run: {run_name}")
    print(f"  Dir: {run_dir}")
    print(f"  Cmd: {' '.join(cmd)}")
    print(f"{'=' * 60}")

    if dry_run:
        print("  [dry-run] skipping execution")
        return

    start = time.time()
    with open(run_dir / "output.txt", "w") as out_f:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(REPO_ROOT),
        )
        for line in proc.stdout:
            sys.stdout.write(line)
            out_f.write(line)
            if server_proc is not None and server_proc.poll() is not None:
                proc.kill()
                proc.wait()
                raise RuntimeError(
                    f"Server exited unexpectedly (code {server_proc.returncode}) "
                    f"during run '{run_name}'"
                )
        proc.wait()

    elapsed = time.time() - start
    rc = proc.returncode
    print(f"\n  Finished in {elapsed:.1f}s (exit code {rc})")
    if rc != 0:
        raise RuntimeError(f"Run '{run_name}' failed with exit code {rc}")


def run_load(
    benchmark_project: str,
    base_params: dict,
    load_cfg: dict,
    exp_dir: Path,
    dry_run: bool,
    server_proc=None,
) -> None:
    """Run a one-time server load step before the sweep (optional)."""
    load_params = dict(base_params)
    load_params.update(load_cfg)
    for key in ("online", "skipload", "disable_console_logger"):
        load_params.pop(key, None)

    load_dir = exp_dir / "_load"
    config_record = {"run_name": "_load", "params": load_params}
    cmd = build_command(benchmark_project, load_params)
    run_single("_load", load_dir, cmd, config_record, dry_run, server_proc=server_proc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _resolve_setup_target(setup_param: str, explicit_target: str | None) -> str:
    """Return 'server_params' or 'base_params' for this setup parameter."""
    if explicit_target:
        return explicit_target
    return "server_params" if setup_param in SERVER_PARAM_TO_FLAG else "base_params"


def run_sweep(
    exp_name: str,
    benchmark_project: str,
    base_params: dict,
    sweep: dict,
    run_dir_root: Path,
    dry_run: bool,
    server_proc=None,
    extra_config: dict | None = None,
) -> None:
    """Run the sweep (or single default run) under run_dir_root."""
    extra = extra_config or {}
    if sweep:
        sweep_param = sweep["param"]
        sweep_values = sweep["values"]
        print(f"\n[{exp_name}] Sweep {sweep_param} over {sweep_values}")
        for value in sweep_values:
            run_params = dict(base_params)
            run_params[sweep_param] = value
            run_name = f"{sweep_param}_{value}"
            config_record = {
                "experiment": exp_name,
                "run_name": run_name,
                "sweep_param": sweep_param,
                "sweep_value": value,
                "params": run_params,
                **extra,
            }
            cmd = build_command(benchmark_project, run_params)
            run_single(
                run_name,
                run_dir_root / run_name,
                cmd,
                config_record,
                dry_run,
                server_proc=server_proc,
            )
    else:
        run_name = "default"
        config_record = {
            "experiment": exp_name,
            "run_name": run_name,
            "params": base_params,
            **extra,
        }
        cmd = build_command(benchmark_project, base_params)
        run_single(
            run_name,
            run_dir_root / run_name,
            cmd,
            config_record,
            dry_run,
            server_proc=server_proc,
        )


def main():
    parser = argparse.ArgumentParser(description="Run Garnet YCSB experiments")
    parser.add_argument("experiment", help="Experiment name (looks up experiment/configs/<name>.yaml)")
    parser.add_argument("--config", help="Override config path (default: experiment/configs/<name>.yaml)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without executing"
    )
    parser.add_argument(
        "--no-server",
        action="store_true",
        help="Skip server launch/shutdown (use an already-running server)",
    )
    args = parser.parse_args()

    config_path = args.config or str(REPO_ROOT / "experiment" / "configs" / f"{args.experiment}.yaml")
    cfg = load_config(config_path)
    exp_name = cfg.get("name", args.experiment)
    benchmark_project = cfg.get(
        "benchmark_project", "benchmark/Resp.benchmark/Resp.benchmark.csproj"
    )
    server_project = cfg.get("server_project", DEFAULT_SERVER_PROJECT)
    base_params = dict(cfg.get("base_params", {}))
    server_params = dict(cfg.get("server_params", {}))
    sweep = cfg.get("sweep", {})
    setup = cfg.get("setup", {})
    no_server = args.no_server or cfg.get("no_server", False)

    exp_dir = RESULT_ROOT / exp_name

    # ------------------------------------------------------------------
    # Step 1: kill leftover processes from any previous run
    # ------------------------------------------------------------------
    print(f"\n[{exp_name}] Killing leftover processes...")
    killall_leftover(server_project, benchmark_project, args.dry_run)

    # ------------------------------------------------------------------
    # Step 2: clean up this experiment's result directory
    # ------------------------------------------------------------------
    print(f"[{exp_name}] Cleaning result directory...")
    cleanup_result_dir(exp_dir, args.dry_run)
    if not args.dry_run:
        with open(exp_dir / "config.yaml", "w") as f:
            yaml.dump(cfg, f)

    # ------------------------------------------------------------------
    # Step 3: launch server, run everything, shut server down
    # ------------------------------------------------------------------
    if setup:
        setup_param = setup["param"]
        setup_values = setup["values"]
        setup_target = _resolve_setup_target(setup_param, setup.get("target"))

        # Derive host/port for readiness check (setup may override these)
        def _host_port(sp: dict, bp: dict):
            h = sp.get("host") or bp.get("host", "127.0.0.1")
            p = int(sp.get("port") or bp.get("port", 6379))
            return h, p

        print(f"\n[{exp_name}] Setup {setup_param} over {setup_values} (target: {setup_target})")
        for setup_value in setup_values:
            setup_name = f"{setup_param}_{setup_value}"
            setup_dir = exp_dir / setup_name

            # Build per-setup params
            cur_server_params = dict(server_params)
            cur_base_params = dict(base_params)
            if setup_target == "server_params":
                cur_server_params[setup_param] = setup_value
            else:
                cur_base_params[setup_param] = setup_value

            host, port = _host_port(cur_server_params, cur_base_params)

            print(f"\n[{exp_name}] === Setup: {setup_name} ===")
            server_proc = None
            try:
                if not no_server:
                    server_log = setup_dir / "_server.log"
                    server_proc = launch_server(
                        server_project, cur_server_params, server_log, args.dry_run
                    )
                    wait_for_server(host, port, args.dry_run, server_proc)

                load_cfg = cfg.get("load")
                if load_cfg is not None:
                    print(f"\n[{exp_name}] Running load step for {setup_name}...")
                    run_load(
                        benchmark_project,
                        cur_base_params,
                        load_cfg,
                        setup_dir,
                        args.dry_run,
                        server_proc=server_proc,
                    )

                run_sweep(
                    exp_name,
                    benchmark_project,
                    cur_base_params,
                    sweep,
                    setup_dir,
                    args.dry_run,
                    server_proc=server_proc,
                    extra_config={
                        "setup_param": setup_param,
                        "setup_value": setup_value,
                    },
                )
            finally:
                if not no_server:
                    shutdown_server(server_proc, args.dry_run)
    else:
        # No setup: original single-server flow
        host = server_params.get("host") or base_params.get("host", "127.0.0.1")
        port = int(server_params.get("port") or base_params.get("port", 6379))

        server_proc = None
        try:
            if not no_server:
                server_log = exp_dir / "_server.log"
                server_proc = launch_server(
                    server_project, server_params, server_log, args.dry_run
                )
                wait_for_server(host, port, args.dry_run, server_proc)

            load_cfg = cfg.get("load")
            if load_cfg is not None:
                print(f"\n[{exp_name}] Running load step...")
                run_load(
                    benchmark_project,
                    base_params,
                    load_cfg,
                    exp_dir,
                    args.dry_run,
                    server_proc=server_proc,
                )

            run_sweep(
                exp_name,
                benchmark_project,
                base_params,
                sweep,
                exp_dir,
                args.dry_run,
                server_proc=server_proc,
            )
        finally:
            if not no_server:
                shutdown_server(server_proc, args.dry_run)

    print(f"\nAll runs complete. Results in: {exp_dir}")


if __name__ == "__main__":
    main()
