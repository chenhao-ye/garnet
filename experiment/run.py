#!/usr/bin/env -S uv run

"""
Run Garnet experiments from YAML configs with explicit prepare/base/sweep phases.

Lifecycle per invocation:
  1. Kill leftover server / benchmark processes from any previous run.
  2. Delete this experiment's result directory to avoid stale data.
  3. Expand the Cartesian product of sweep client/server parameters.
  4. For each run:
     a. Launch the Garnet server (unless `no_server: true` in the config).
     b. Launch the replication primary and replica GarnetServers (when the config defines
        primary_params/replica_params) and bootstrap the cluster over plain RESP (epochs,
        slots, MEET, REPLICAOF blocking until the initial sync) -- the cluster is fully
        formed before the client runs.
     c. Optionally execute the prepare client step.
     d. Execute the benchmark client step.
     e. Shut down the servers (replica, then primary, then the generic server).

Usage:
    uv run experiment/run.py scale_clients
    uv run experiment/run.py scale_clients --dry-run
    uv run experiment/run.py scale_clients other_experiment   # run both sequentially
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

import remote_util
import yaml
from check import main as check_main
from config import (
    REPO_ROOT,
    Affinity,
    AffinitySpec,
    RemoteEntry,
    config_path_for,
    load_experiment_spec,
    remote_mode,
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
    "replication_bench",
}
SERVER_BOOL_PARAMS = {
    "aof",
    "aof_null_device",
    "cluster",
    "tls",
    "fast_aof_truncate",
    "repl_diskless_sync",
}

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


def is_benchmark_project(project: str) -> bool:
    """True when the project is Resp.benchmark, e.g. a bench process in the server slot
    (the Replica role of a split-process AOF run)."""
    return project.endswith("Resp.benchmark.csproj")


def build_command(
    project: str,
    params: dict,
    is_server: bool = False,
    affinity: AffinitySpec | None = None,
    remote: bool = False,
) -> list[str]:
    # Remote commands run after `cd <repo>` on the remote machine, so the project path
    # stays home-relative (machines only share the repo path relative to $HOME).
    project_path = Path(project) if remote else REPO_ROOT / project
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
    if is_server and not is_benchmark_project(project):
        cmd += params_to_args(params, bool_params=SERVER_BOOL_PARAMS, list_params=set())
    else:
        # A bench in the server slot takes Resp.benchmark flags, not GarnetServer ones.
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
    # New session => the process is a group leader; `dotnet run` and the app host it spawns
    # share one group, so teardown can signal the whole tree at once (see shutdown_server).
    return subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(log_path.parent),
        start_new_session=True,
    )


def resolve_replication_server_endpoint(params: dict, what: str) -> tuple[str, int]:
    """Endpoint of a GarnetServer in a replication slot (primary/replica): it binds the
    bind/port given by its own params (run.py injects bind in remote mode)."""
    port = params.get("port")
    if port is None:
        raise ValueError(f"{what}_params must set 'port'")
    return str(params.get("bind", "127.0.0.1")), int(port)


def resp_command(host: str, port: int, *args, timeout: float = 60.0) -> str | None:
    """Send one RESP command and return the reply (simple string / integer / bulk as str,
    None for a null bulk). Raises RuntimeError on -ERR replies. Minimal client so the
    replication cluster bootstrap needs no external Redis dependency."""
    payload = f"*{len(args)}\r\n".encode()
    for arg in args:
        encoded = str(arg).encode()
        payload += b"$%d\r\n%s\r\n" % (len(encoded), encoded)

    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(payload)
        buf = b""
        while b"\r\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                raise RuntimeError(f"{host}:{port} closed the connection mid-reply")
            buf += chunk
        line, buf = buf.split(b"\r\n", 1)
        kind, rest = line[:1], line[1:].decode()
        if kind in (b"+", b":"):
            return rest
        if kind == b"-":
            raise RuntimeError(rest)
        if kind == b"$":
            length = int(rest)
            if length < 0:
                return None
            while len(buf) < length + 2:
                chunk = sock.recv(65536)
                if not chunk:
                    raise RuntimeError(f"{host}:{port} closed the connection mid-reply")
                buf += chunk
            return buf[:length].decode()
        raise RuntimeError(f"unexpected RESP reply from {host}:{port}: {line!r}")


def bootstrap_replication_cluster(
    primary_host: str, primary_port: int, replica_host: str, replica_port: int
) -> None:
    """Form the primary/replica pair with plain RESP commands: config epochs and full slot
    ownership on the primary, then MEET + REPLICAOF on the replica. REPLICAOF replies only
    after the initial sync completes, so returning from here means the cluster is ready
    for clients. REPLICAOF resolves the primary's node id from its address, which the
    replica learns only once the MEET gossip lands; retried until then."""
    if dry_run:
        logger.info("[dry-run] skipping replication cluster bootstrap")
        return
    logger.info(
        f"Bootstrapping replication cluster: primary {primary_host}:{primary_port}, "
        f"replica {replica_host}:{replica_port}"
    )
    resp_command(primary_host, primary_port, "CLUSTER", "SET-CONFIG-EPOCH", "1")
    resp_command(primary_host, primary_port, "CLUSTER", "ADDSLOTSRANGE", "0", "16383")
    resp_command(replica_host, replica_port, "CLUSTER", "SET-CONFIG-EPOCH", "2")
    resp_command(replica_host, replica_port, "CLUSTER", "MEET", primary_host, primary_port)

    deadline = time.time() + SERVER_READY_TIMEOUT
    while True:
        try:
            resp_command(replica_host, replica_port, "REPLICAOF", primary_host, primary_port)
            break
        except (RuntimeError, OSError) as e:
            if time.time() >= deadline:
                raise
            logger.debug(f"REPLICAOF retry: {e}")
            time.sleep(0.25)

    info = resp_command(replica_host, replica_port, "INFO", "replication") or ""
    fields = dict(
        line.split(":", 1) for line in info.replace("\r", "").split("\n") if ":" in line
    )
    if fields.get("role") != "slave" or fields.get("master_sync_in_progress") != "False":
        raise RuntimeError(
            f"replica {replica_host}:{replica_port} did not reach synced-replica state "
            f"(role={fields.get('role')}, sync_in_progress={fields.get('master_sync_in_progress')})"
        )
    logger.info("Replication cluster ready (replica attached and synced)")


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


def _signal_process_group(proc: subprocess.Popen, sig: int) -> None:
    """Send `sig` to the process's whole group. Servers are launched with
    start_new_session=True, so this reaches `dotnet run` and the app host it spawns
    together -- signalling only the wrapper would orphan the running server."""
    try:
        os.killpg(os.getpgid(proc.pid), sig)
    except (ProcessLookupError, PermissionError):
        pass  # already gone


def shutdown_server(proc: subprocess.Popen | None) -> None:
    if dry_run or proc is None:
        return
    logger.info("Shutting down server...")
    _signal_process_group(proc, signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _signal_process_group(proc, signal.SIGKILL)
        proc.wait()
    logger.info("Server stopped")


def run_command(
    run_dir: Path,
    cmd: list[str],
    server_procs: list[subprocess.Popen] | None = None,
    output_name: str = "output.txt",
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Cmd: {shlex.join(cmd)} @ {run_dir}")

    if dry_run:
        logger.info("[dry-run] skipping execution")
        return

    start = time.time()
    with open(run_dir / output_name, "w") as out_f:
        proc: subprocess.Popen = subprocess.Popen(
            cmd,
            stdout=out_f,
            stderr=subprocess.STDOUT,
            cwd=str(run_dir),
            start_new_session=True,
        )
        while proc.poll() is None:
            for server_proc in server_procs or []:
                if server_proc.poll() is not None:
                    _signal_process_group(proc, signal.SIGKILL)
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
    primary_params: dict,
    replica_params: dict,
    sweep_combo: dict,
    sweep_params: dict,
    prepare_params: dict,
    no_server: bool,
    repeat: int,
    affinity: Affinity,
    remote: dict[str, RemoteEntry],
    loopback: dict[str, bool],
) -> None:
    client_params = dict(client_params)
    if repeat > 1:
        client_params["repeat"] = repeat

    # In a split-process AOF run the Replica role (the bench in the server slot) drives the
    # passes, so it carries the repeat count; the client learns the count implicitly from the
    # control channel's DONE message. The replication primary/replica slots ignore repeat
    # (the client drives the passes), so nothing is injected there.
    server_params = dict(server_params)
    if repeat > 1 and is_benchmark_project(server_project):
        server_params["repeat"] = repeat

    def role_ssh(role: str) -> str | None:
        entry = remote.get(role)
        return entry.ssh if entry is not None else None

    # Remote mode: every process must use cross-machine-reachable addresses; inject the
    # resolved data-plane IPs (bind for the GarnetServer roles, connect targets for the
    # client and the bootstrap).
    primary_params = dict(primary_params)
    replica_params = dict(replica_params)
    if remote_mode(remote):
        primary_ip = remote_util.resolve_role_ip("primary", remote)
        replica_ip = remote_util.resolve_role_ip("replica", remote)
        primary_params["bind"] = primary_ip
        replica_params["bind"] = replica_ip
        client_params["primary_host"] = primary_ip
        client_params["replica_host"] = replica_ip

    # Hermetic per-role server state (checkpoints + persisted cluster config): point each
    # GarnetServer at a directory inside its machine's result tree for this run, so a rerun
    # never sees stale cluster state. Local roles run with cwd = their role dir; remote
    # roles run with cwd = the repo root.
    for role, params in (("primary", primary_params), ("replica", replica_params)):
        if params and "checkpointdir" not in params:
            params["checkpointdir"] = (
                f"result/{exp_name}/{run_name}/{role}/garnet-data"
                if role_ssh(role)
                else str(run_dir / role / "garnet-data")
            )

    def server_role_cmd(role: str, params: dict, aff) -> list[str] | None:
        if not params:
            return None
        if role_ssh(role):
            return remote_util.ssh_role_command(
                role_ssh(role),
                build_command(
                    server_project, params, is_server=True, affinity=aff, remote=True
                ),
                remote_util.remote_role_output(exp_name, run_name, role),
            )
        return build_command(server_project, params, is_server=True, affinity=aff)

    server_cmd = build_command(
        server_project, server_params, is_server=True, affinity=affinity.server
    )
    primary_cmd = server_role_cmd("primary", primary_params, affinity.primary)
    replica_cmd = server_role_cmd("replica", replica_params, affinity.replica)
    prepare_cmd = (
        build_command(benchmark_project, prepare_params, affinity=affinity.prepare)
        if prepare_params
        else None
    )
    benchmark_cmd = (
        remote_util.ssh_role_command(
            role_ssh("client"),
            build_command(
                benchmark_project, client_params, affinity=affinity.client, remote=True
            ),
            remote_util.remote_role_output(exp_name, run_name, "client"),
        )
        if role_ssh("client")
        else build_command(benchmark_project, client_params, affinity=affinity.client)
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
    if primary_params:
        config_record["primary_params"] = primary_params
        config_record["primary_cmd"] = shlex.join(primary_cmd)
    if replica_params:
        config_record["replica_params"] = replica_params
        config_record["replica_cmd"] = shlex.join(replica_cmd)
    if remote:
        config_record["remote"] = {
            role: entry.to_dict() for role, entry in remote.items()
        }
    dump_config(run_dir / "config.yaml", config_record)

    host, port = resolve_server_endpoint(server_params, client_params)
    server_proc: subprocess.Popen | None = None
    primary_proc: subprocess.Popen | None = None
    replica_proc: subprocess.Popen | None = None

    def launch_replication_server(role: str, params: dict, full_cmd: list[str], aff):
        """Launch a GarnetServer replication slot (locally or over ssh) and block until its
        port accepts connections."""
        role_dir = run_dir / role
        role_host, role_port = resolve_replication_server_endpoint(params, role)
        ssh_host = role_ssh(role)
        if ssh_host:
            proc = remote_util.launch_ssh_role(full_cmd, role_dir / "_ssh.log", role, ssh_host)
        else:
            proc = launch_server(
                server_project, params, role_dir / "output.txt", affinity=aff
            )
        wait_for_server(role_host, role_port, proc)
        return proc

    try:
        if not no_server:
            server_log = run_dir / "_server.log"
            server_proc = launch_server(
                server_project, server_params, server_log, affinity=affinity.server
            )
            wait_for_server(host, port, server_proc)

        if primary_params:
            primary_proc = launch_replication_server(
                "primary", primary_params, primary_cmd, affinity.primary
            )
        if replica_params:
            replica_proc = launch_replication_server(
                "replica", replica_params, replica_cmd, affinity.replica
            )

        # Form the cluster before any client runs: slots + epochs on the primary, then
        # MEET + REPLICAOF (blocking until the initial sync completes) on the replica.
        if primary_params and replica_params:
            p_host, p_port = resolve_replication_server_endpoint(primary_params, "primary")
            r_host, r_port = resolve_replication_server_endpoint(replica_params, "replica")
            bootstrap_replication_cluster(p_host, p_port, r_host, r_port)

        server_procs = [p for p in (server_proc, primary_proc, replica_proc) if p is not None]
        if prepare_params:
            run_command(run_dir / "prepare", prepare_cmd, server_procs=server_procs)

        # Replication runs keep each role's output in its own subdir; for an ssh client the
        # local file holds ssh diagnostics only (the real output is copied back below).
        client_dir = run_dir / ("client" if primary_params else "benchmark")
        run_command(
            client_dir,
            benchmark_cmd,
            server_procs=server_procs,
            output_name="_ssh.log" if role_ssh("client") else "output.txt",
        )
    finally:
        server_stem = Path(server_project).stem
        shutdown_server(replica_proc)
        if replica_params:
            remote_util.kill_remote_role(
                role_ssh("replica"), f"{server_stem}.*--port {replica_params.get('port')}"
            )
        shutdown_server(primary_proc)
        if primary_params:
            remote_util.kill_remote_role(
                role_ssh("primary"), f"{server_stem}.*--port {primary_params.get('port')}"
            )
        remote_util.kill_remote_role(role_ssh("client"), "--replication-bench")
        if not no_server:
            shutdown_server(server_proc)

        # Pull remote role outputs into the local result tree; loopback hosts wrote into
        # the local result dir directly.
        for role in ("primary", "replica", "client"):
            ssh_host = role_ssh(role)
            if ssh_host and not loopback.get(ssh_host, False):
                remote_util.rsync_back(
                    ssh_host,
                    f"{remote_util.remote_repo_path()}/result/{exp_name}/{run_name}/{role}",
                    run_dir / role,
                )


def _run_one(config: str) -> None:
    spec = load_experiment_spec(config_path_for(config), default_name=Path(config).stem)
    # Validate the config (and machine/affinity core requirements) before any work
    if check_main([config]) != 0:
        raise SystemExit(f"check reported errors for '{config}'; aborting run")
    if not spec.prepare_params:
        logger.warning("empty prepare.client_params")
    if not spec.base_server_params and not spec.base_primary_params:
        logger.warning("empty base.server_params")

    if spec.affinity.any_set() and shutil.which("numactl") is None:
        raise RuntimeError("affinity configured in YAML but 'numactl' is not on PATH")

    exp_dir = result_dir(spec.name)

    logger.debug("Killing leftover processes...")
    killall_leftover(spec.server_project, spec.benchmark_project)

    logger.debug("Cleaning result directory...")
    cleanup_result_dir(exp_dir)
    dump_config(exp_dir / "config.yaml", spec.config)
    dump_config(exp_dir / "meta.yaml", capture_git_info())

    loopback: dict[str, bool] = {}
    if remote_mode(spec.remote):
        loopback = remote_util.remote_preflight(spec, exp_dir)

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
            primary_params=run_spec.primary_params,
            replica_params=run_spec.replica_params,
            sweep_combo=run_spec.combo,
            sweep_params=run_spec.sweep_params,
            prepare_params=spec.prepare_params,
            no_server=spec.no_server,
            repeat=spec.repeat,
            affinity=spec.affinity,
            remote=spec.remote,
            loopback=loopback,
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
    remote_util.dry_run = args.dry_run

    for config in args.configs:
        _run_one(config)


if __name__ == "__main__":
    main()
