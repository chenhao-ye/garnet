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
    uv run experiment/run.py scale_clients other_experiment   # run both sequentially
"""

import argparse
import logging
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path

import yaml
from config import (
    REPO_ROOT,
    Affinity,
    AffinitySpec,
    config_path_for,
    load_experiment_spec,
    resolve_run_spec,
    result_dir,
)
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
    "aof_reader_skip",
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


def numactl_prefix(affinity: AffinitySpec | None) -> list[str]:
    if affinity is None:
        return []
    args = ["numactl"]
    if affinity.cpus is not None:
        args.append(f"--physcpubind={affinity.cpus}")
    else:
        args.append(f"--cpunodebind={affinity.numa_node}")
    args += [f"--membind={affinity.numa_node}", "--"]
    return args


def build_command(
    project: str,
    params: dict,
    is_server: bool = False,
    affinity: AffinitySpec | None = None,
) -> list[str]:
    project_path = REPO_ROOT / project
    cmd = numactl_prefix(affinity)
    cmd += [
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
    if is_server:
        cmd += params_to_args(params, bool_params=SERVER_BOOL_PARAMS, list_params=set())
    else:
        # check_client_params(params)
        cmd += params_to_args(
            params, bool_params=CLIENT_BOOL_PARAMS, list_params=CLIENT_LIST_PARAMS
        )
    return cmd


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


def capture_git_info() -> dict:
    """Git state of the repository (REPO_ROOT) -- the source that builds the binary.

    Returns {git_commit, git_branch, git_dirty}; any field is None if git is unavailable
    or REPO_ROOT is not a repo, so provenance capture never breaks a run. git_dirty is
    True when tracked files differ from HEAD; untracked files are intentionally ignored
    (here they are experiment configs/docs/result/, which do not build into the binary).
    """

    def git(*args: str) -> str | None:
        try:
            out = subprocess.run(
                ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True
            )
            return out.stdout.strip() if out.returncode == 0 else None
        except OSError:
            return None

    commit = git("rev-parse", "HEAD")
    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    dirty: bool | None = None
    if commit is not None:
        try:
            rc = subprocess.run(
                ["git", "diff", "--quiet", "HEAD"], cwd=REPO_ROOT, capture_output=True
            ).returncode
            dirty = rc != 0
        except OSError:
            dirty = None
    return {"git_commit": commit, "git_branch": branch, "git_dirty": dirty}


def launch_server(
    server_project: str,
    server_params: dict,
    log_path: Path,
    affinity: AffinitySpec | None = None,
) -> subprocess.Popen | None:
    cmd = build_command(
        server_project, server_params, is_server=True, affinity=affinity
    )

    logger.info(f"Launch server: {shlex.join(cmd)}")
    if dry_run:
        logger.info("[dry-run] skipping launch")
        return

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(log_path, "w")
    return subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(log_path.parent),
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
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
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
    repeat: int,
    affinity: Affinity,
) -> None:
    client_params = dict(client_params)
    if repeat > 1:
        client_params["repeat"] = repeat

    server_cmd = build_command(
        server_project, server_params, is_server=True, affinity=affinity.server
    )
    prepare_cmd = (
        build_command(benchmark_project, prepare_params, affinity=affinity.prepare)
        if prepare_params
        else None
    )
    benchmark_cmd = build_command(
        benchmark_project, client_params, affinity=affinity.client
    )

    config_record = {
        "experiment": exp_name,
        "benchmark": benchmark,
        "run_name": run_name,
        "client_params": client_params,
        "server_params": server_params,
        "sweep": sweep_combo,
        "sweep_params": sweep_params,
        "repeat": repeat,
        "affinity": affinity.to_dict(),
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
            server_proc = launch_server(
                server_project, server_params, server_log, affinity=affinity.server
            )
            wait_for_server(host, port, server_proc)

        if prepare_params:
            run_command(run_dir / "prepare", prepare_cmd, server_proc=server_proc)

        run_command(run_dir / "benchmark", benchmark_cmd, server_proc=server_proc)
    finally:
        if not no_server:
            shutdown_server(server_proc)


def _run_one(config: str) -> None:
    spec = load_experiment_spec(config_path_for(config), default_name=Path(config).stem)
    if not spec.prepare_params:
        logger.warning("empty prepare.client_params")
    if not spec.base_server_params:
        logger.warning("empty base.server_params")

    if spec.affinity.any_set() and shutil.which("numactl") is None:
        raise RuntimeError(
            "affinity configured in YAML but 'numactl' is not on PATH"
        )

    exp_dir = result_dir(spec.name)

    logger.debug("Killing leftover processes...")
    killall_leftover(spec.server_project, spec.benchmark_project)

    logger.debug("Cleaning result directory...")
    cleanup_result_dir(exp_dir)
    dump_config(exp_dir / "config.yaml", spec.config)
    dump_config(exp_dir / "meta.yaml", capture_git_info())

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
            sweep_combo=run_spec.combo,
            sweep_params=run_spec.sweep_params,
            prepare_params=spec.prepare_params,
            no_server=spec.no_server,
            repeat=spec.repeat,
            affinity=spec.affinity,
        )

    logger.info(f"All runs complete. Results in: {exp_dir}")
    parse_main([config])


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Run Garnet experiments")
    parser.add_argument(
        "configs",
        nargs="+",
        help="One or more experiment config names (or paths). Each is run sequentially.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without executing"
    )
    args = parser.parse_args()

    global dry_run
    dry_run = args.dry_run

    for config in args.configs:
        _run_one(config)


if __name__ == "__main__":
    main()
