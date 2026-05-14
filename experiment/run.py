#!/usr/bin/env -S uv run

"""
Run Garnet experiments from YAML configs with explicit prepare/base/sweep phases.

Lifecycle per invocation:
  1. Kill leftover server / benchmark processes from any previous run.
  2. Delete this experiment's result directory to avoid stale data.
  3. Expand the Cartesian product of sweep client/server parameters.
  4. For each run:
     a. Launch the Garnet server (unless `no_server: true` in the config).
     b. Optionally execute the prepare client step.
     c. Execute the benchmark client step.
     d. Shut down the server.

Usage:
    uv run experiment/run.py scale_clients
    uv run experiment/run.py scale_clients --dry-run
    uv run experiment/run.py scale_clients --config path/to/custom.yaml
"""

import argparse
import logging
import os
import shlex
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

import yaml
from config import REPO_ROOT, load_experiment_spec, resolve_run_spec, result_dir
from parse import main as parse_main

SERVER_READY_TIMEOUT = 60
SERVER_READY_INTERVAL = 0.5

CLIENT_LIST_PARAMS = {"op_workload", "op_percent", "batchsize", "threads"}
CLIENT_BOOL_PARAMS = {
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
    "repl_bench",
}
SERVER_BOOL_PARAMS = {"aof", "aof_null_device", "cluster", "tls"}

logger = logging.getLogger(__name__)
dry_run = False


def flag_for_param(key: str) -> str:
    return f"--{key.replace('_', '-')}"


def check_client_params(params: dict) -> None:
    expected_client = "InProc" if params.get("aof_bench") else "GarnetClientSession"
    actual_client = params.get("client")
    if actual_client != expected_client:
        logger.warning(f"expected client={expected_client!r}, got {actual_client!r}")


def params_to_args(
    params: dict, *, bool_params: set[str], list_params: set[str]
) -> list[str]:
    args: list[str] = []
    for key, value in params.items():
        flag = flag_for_param(key)
        if key in bool_params:
            if value:
                args.append(flag)
        elif key in list_params:
            if isinstance(value, list):
                args += [flag, ",".join(str(v) for v in value)]
            else:
                args += [flag, str(value)]
        else:
            args += [flag, str(value)]
    return args


def build_command(project: str, params: dict, is_server: bool = False) -> list[str]:
    project_path = REPO_ROOT / project
    cmd = [
        "dotnet",
        "run",
        "--no-build",
        "-c",
        "Release",
        "--framework",
        "net10.0",
        "--project",
        str(project_path),
        "--",
    ]
    if is_server:
        cmd += params_to_args(params, bool_params=SERVER_BOOL_PARAMS, list_params=set())
    else:
        # check_client_params(params)
        cmd += params_to_args(
            params, bool_params=CLIENT_BOOL_PARAMS, list_params=CLIENT_LIST_PARAMS
        )
    return cmd


def ensure_built(project_relpath: str) -> None:
    """Pre-build a project so `dotnet run --no-build` can launch it.

    `dotnet run` without `--no-build` invokes an MSBuild check on every invocation,
    and that path empirically loses captured stdout when the parent's stdout is a
    Popen-piped file (the bench output ends up empty). `--no-build` avoids that path
    but requires the binary to already exist; this helper makes sure it does. Cost
    is paid once per session — dotnet build is a no-op when nothing has changed.
    """
    project_path = REPO_ROOT / project_relpath
    logger.info(f"Build: {project_path}")
    if dry_run:
        return
    subprocess.run(
        ["dotnet", "build", "-c", "Release", "-f", "net10.0", str(project_path)],
        check=True,
    )


def killall_leftover(server_project: str, benchmark_project: str) -> None:
    patterns = [Path(server_project).stem, Path(benchmark_project).stem]
    for pat in patterns:
        cmd = ["pkill", "-f", pat]
        logger.debug(f"[cleanup] {' '.join(cmd)}")
        if not dry_run:
            subprocess.run(cmd, check=False)
    if not dry_run:
        time.sleep(1)


def cleanup_result_dir(exp_dir: Path) -> None:
    if exp_dir.exists():
        logger.debug(f"[cleanup] removing {exp_dir}")
        if not dry_run:
            shutil.rmtree(exp_dir)
    if not dry_run:
        exp_dir.mkdir(parents=True, exist_ok=True)


def resolve_server_endpoint(
    server_params: dict, client_params: dict
) -> tuple[str, int]:
    host = server_params.get("bind") or client_params.get("host") or "127.0.0.1"
    port = int(server_params.get("port") or client_params.get("port") or 6379)
    return host, port


def dump_config(path: Path, payload: dict) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        # set width to avoid break a simple string into multiple lines
        yaml.dump(payload, f, sort_keys=False, width=10_000)


def launch_server(
    server_project: str, server_params: dict, log_path: Path
) -> subprocess.Popen | None:
    cmd = build_command(server_project, server_params, is_server=True)

    logger.info(f"Launch server: {shlex.join(cmd)}")
    if dry_run:
        logger.info("[dry-run] skipping launch")
        return

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(log_path, "w")
    # start_new_session=True puts the server (and any children it spawns, e.g. the
    # GarnetServer apphost forked by `dotnet run`) into a fresh process group so
    # shutdown_server() can signal the whole group, not just the wrapper.
    return subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(log_path.parent),
        start_new_session=True,
    )


def wait_for_server(host: str, port: int, proc: subprocess.Popen | None = None) -> None:
    if dry_run:
        return
    deadline = time.time() + SERVER_READY_TIMEOUT
    logger.debug(f"Waiting for server {host}:{port} ...")
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(
                f"Server process exited unexpectedly (code {proc.returncode}) "
                f"before becoming ready on {host}:{port}"
            )
        try:
            with socket.create_connection((host, port), timeout=1):
                logger.debug(f"Server ready on {host}:{port}")
                return
        except OSError:
            time.sleep(SERVER_READY_INTERVAL)
    raise TimeoutError(
        f"Server did not become ready on {host}:{port} within {SERVER_READY_TIMEOUT}s"
    )


def shutdown_server(proc: subprocess.Popen | None) -> None:
    if dry_run or proc is None:
        return
    logger.info("Shutting down server...")
    # The wrapper `dotnet run` forks a separate GarnetServer apphost; signalling only
    # the wrapper leaves the apphost holding the port. Signal the whole process group
    # we created in launch_server() instead.
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        pgid = None
    try:
        if pgid is not None:
            os.killpg(pgid, signal.SIGTERM)
        else:
            proc.terminate()
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                proc.kill()
        except ProcessLookupError:
            pass
        proc.wait()
    logger.info("Server stopped")


def run_command(
    run_dir: Path, cmd: list[str], server_proc: subprocess.Popen | None = None
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Cmd: {shlex.join(cmd)} @ {run_dir}")

    if dry_run:
        logger.info("[dry-run] skipping execution")
        return

    start = time.time()
    with open(run_dir / "output.txt", "w") as out_f:
        proc: subprocess.Popen = subprocess.Popen(
            cmd,
            stdout=out_f,
            stderr=subprocess.STDOUT,
            cwd=str(run_dir),
        )
        while proc.poll() is None:
            if server_proc is not None and server_proc.poll() is not None:
                proc.kill()
                proc.wait()
                raise RuntimeError(
                    f"Server exited unexpectedly (code {server_proc.returncode}) "
                )
            time.sleep(0.1)

    elapsed = time.time() - start
    rc = proc.returncode
    logger.info(f"Finished in {elapsed:.1f}s (exit code {rc})")
    if rc != 0:
        raise RuntimeError(f"Server failed with exit code {rc}")


def execute_run(
    exp_name: str,
    benchmark: str,
    benchmark_project: str,
    server_project: str,
    run_dir: Path,
    run_name: str,
    client_params: dict,
    server_params: dict,
    sweep_combo: dict,
    sweep_params: dict,
    prepare_params: dict,
    no_server: bool,
    primary_params: dict | None = None,
    replica_params: dict | None = None,
) -> None:
    if benchmark == "replication":
        execute_replication_run(
            exp_name=exp_name,
            benchmark=benchmark,
            benchmark_project=benchmark_project,
            server_project=server_project,
            run_dir=run_dir,
            run_name=run_name,
            client_params=client_params,
            server_params=server_params,
            primary_params=primary_params or {},
            replica_params=replica_params or {},
            sweep_combo=sweep_combo,
            sweep_params=sweep_params,
            prepare_params=prepare_params,
        )
        return

    server_cmd = build_command(server_project, server_params, is_server=True)
    prepare_cmd = (
        build_command(benchmark_project, prepare_params) if prepare_params else None
    )
    benchmark_cmd = build_command(benchmark_project, client_params)

    config_record = {
        "experiment": exp_name,
        "benchmark": benchmark,
        "run_name": run_name,
        "client_params": client_params,
        "server_params": server_params,
        "sweep": sweep_combo,
        "sweep_params": sweep_params,
        "server_cmd": shlex.join(server_cmd),
        "prepare_cmd": shlex.join(prepare_cmd) if prepare_cmd is not None else "",
        "client_cmd": shlex.join(benchmark_cmd),
    }
    dump_config(run_dir / "config.yaml", config_record)

    host, port = resolve_server_endpoint(server_params, client_params)
    server_proc: subprocess.Popen | None = None
    try:
        if not no_server:
            server_log = run_dir / "_server.log"
            server_proc = launch_server(server_project, server_params, server_log)
            wait_for_server(host, port, server_proc)

        if prepare_params:
            run_command(run_dir / "prepare", prepare_cmd, server_proc=server_proc)

        run_command(run_dir / "benchmark", benchmark_cmd, server_proc=server_proc)
    finally:
        if not no_server:
            shutdown_server(server_proc)


def execute_replication_run(
    *,
    exp_name: str,
    benchmark: str,
    benchmark_project: str,
    server_project: str,
    run_dir: Path,
    run_name: str,
    client_params: dict,
    server_params: dict,
    primary_params: dict,
    replica_params: dict,
    sweep_combo: dict,
    sweep_params: dict,
    prepare_params: dict,
) -> None:
    """One primary + one replica Garnet subprocess; bench wires them and runs.

    Server config layering: ``server_params`` is the shared base; ``primary_params``
    and ``replica_params`` are per-node overlays that override the base. Ports are
    assigned last: primary uses the merged ``port`` (default 7000); replica uses
    its merged ``port`` if set, else the client's ``repl_replica_port``, else
    primary + 1.
    """

    # Merge overlays on top of the shared base. Primary/replica overlays win
    # over server_params, but the port handling below has final say.
    primary_params = {**server_params, **primary_params}
    replica_params = {**server_params, **replica_params}

    primary_port = int(primary_params.get("port", 7000))
    primary_params["port"] = primary_port

    replica_port = int(
        replica_params.get("port")
        or client_params.get("repl_replica_port")
        or (primary_port + 1)
    )
    replica_params["port"] = replica_port

    # The bench is client-only and connects to the two endpoints below. We auto-populate
    # client_params so the YAML does not have to repeat ports already on the server side.
    client_params = dict(client_params)
    client_params.setdefault("repl_bench", True)
    client_params.setdefault("repl_primary_port", primary_port)
    client_params.setdefault("repl_replica_port", replica_port)

    primary_cmd = build_command(server_project, primary_params, is_server=True)
    replica_cmd = build_command(server_project, replica_params, is_server=True)
    benchmark_cmd = build_command(benchmark_project, client_params)

    config_record = {
        "experiment": exp_name,
        "benchmark": benchmark,
        "run_name": run_name,
        "client_params": client_params,
        "primary_server_params": primary_params,
        "replica_server_params": replica_params,
        "sweep": sweep_combo,
        "sweep_params": sweep_params,
        "primary_cmd": shlex.join(primary_cmd),
        "replica_cmd": shlex.join(replica_cmd),
        "client_cmd": shlex.join(benchmark_cmd),
    }
    dump_config(run_dir / "config.yaml", config_record)

    primary_log = run_dir / "primary" / "_server.log"
    replica_log = run_dir / "replica" / "_server.log"

    primary_proc: subprocess.Popen | None = None
    replica_proc: subprocess.Popen | None = None
    try:
        primary_proc = launch_server(server_project, primary_params, primary_log)
        replica_proc = launch_server(server_project, replica_params, replica_log)
        wait_for_server("127.0.0.1", primary_port, primary_proc)
        wait_for_server("127.0.0.1", replica_port, replica_proc)

        # The bench performs cluster wiring (CLUSTER ADDSLOTSRANGE / MEET / REPLICATE)
        # itself; no prepare step is needed for that. Custom user prepare steps
        # (e.g. data load) still run against the primary.
        if prepare_params:
            prepare_cmd = build_command(benchmark_project, prepare_params)
            run_command(run_dir / "prepare", prepare_cmd, server_proc=primary_proc)

        run_command(run_dir / "benchmark", benchmark_cmd, server_proc=primary_proc)
    finally:
        if replica_proc is not None:
            shutdown_server(replica_proc)
        if primary_proc is not None:
            shutdown_server(primary_proc)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Run Garnet experiments")
    parser.add_argument(
        "experiment", help="Experiment name (looks up experiment/configs/<name>.yaml)"
    )
    parser.add_argument(
        "--config",
        help="Override config path (default: experiment/configs/<name>.yaml)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without executing"
    )
    args = parser.parse_args()

    global dry_run
    dry_run = args.dry_run

    spec = load_experiment_spec(
        args.config
        or str(REPO_ROOT / "experiment" / "configs" / f"{args.experiment}.yaml"),
        default_name=args.experiment,
    )
    if not spec.prepare_params:
        logger.warning("empty prepare.client_params")
    if not spec.base_server_params:
        logger.warning("empty base.server_params")

    exp_dir = result_dir(spec.name)

    if not spec.no_server:
        ensure_built(spec.server_project)
    ensure_built(spec.benchmark_project)

    logger.debug("Killing leftover processes...")
    killall_leftover(spec.server_project, spec.benchmark_project)

    logger.debug("Cleaning result directory...")
    cleanup_result_dir(exp_dir)
    dump_config(exp_dir / "config.yaml", spec.config)

    logger.info(f"Expanded {len(spec.combos)} runs")

    for combo in spec.combos:
        run_spec = resolve_run_spec(spec, combo)

        logger.info(
            f"==================== Run: [{spec.name}] @{run_spec.run_name} ===================="
        )
        execute_run(
            exp_name=spec.name,
            benchmark=spec.benchmark,
            benchmark_project=spec.benchmark_project,
            server_project=spec.server_project,
            run_dir=exp_dir / run_spec.run_name,
            run_name=run_spec.run_name,
            client_params=run_spec.client_params,
            server_params=run_spec.server_params,
            primary_params=spec.base_primary_params,
            replica_params=spec.base_replica_params,
            sweep_combo=run_spec.combo,
            sweep_params=run_spec.sweep_params,
            prepare_params=spec.prepare_params,
            no_server=spec.no_server,
        )

    logger.info(f"All runs complete. Results in: {exp_dir}")

    parse_argv = [args.experiment]
    if args.config:
        parse_argv += ["--config", args.config]
    parse_main(parse_argv)


if __name__ == "__main__":
    main()
