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
import itertools
import logging
import re
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"

DEFAULT_SERVER_PROJECT = "main/GarnetServer/GarnetServer.csproj"
SERVER_READY_TIMEOUT = 60
SERVER_READY_INTERVAL = 0.5
SUPPORTED_BENCHMARKS = {"online", "aof"}

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


def client_params_to_args(params: dict) -> list[str]:
    check_client_params(params)
    args: list[str] = []
    for key, value in params.items():
        flag = flag_for_param(key)
        if key in CLIENT_BOOL_PARAMS:
            if value:
                args.append(flag)
        elif key in CLIENT_LIST_PARAMS:
            if isinstance(value, list):
                args += [flag, ",".join(str(v) for v in value)]
            else:
                args += [flag, str(value)]
        else:
            args += [flag, str(value)]
    return args


def server_params_to_args(params: dict) -> list[str]:
    args: list[str] = []
    for key, value in params.items():
        flag = flag_for_param(key)
        if key in SERVER_BOOL_PARAMS:
            if value:
                args.append(flag)
        else:
            args += [flag, str(value)]
    return args


def build_command(project: str, params: dict, is_server: bool = False) -> list[str]:
    project_path = REPO_ROOT / project
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
    cmd += server_params_to_args(params) if is_server else client_params_to_args(params)
    return cmd


def format_command(cmd: list[str]) -> str:
    return shlex.join(cmd)


def killall_leftover(server_project: str, benchmark_project: str) -> None:
    patterns = [Path(server_project).stem, Path(benchmark_project).stem]
    for pat in patterns:
        cmd = ["pkill", "-f", pat]
        logger.info(f"[cleanup] {' '.join(cmd)}")
        if not dry_run:
            subprocess.run(cmd, check=False)
    if not dry_run:
        time.sleep(1)


def cleanup_result_dir(exp_dir: Path) -> None:
    if exp_dir.exists():
        logger.info(f"[cleanup] removing {exp_dir}")
        if not dry_run:
            shutil.rmtree(exp_dir)
    if not dry_run:
        exp_dir.mkdir(parents=True, exist_ok=True)


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_benchmark(cfg: dict, config_path: str) -> str:
    benchmark = cfg.get("benchmark")
    if not benchmark:
        raise ValueError(
            f"Config '{config_path}' is missing required field 'benchmark'"
        )
    if benchmark not in SUPPORTED_BENCHMARKS:
        supported = ", ".join(sorted(SUPPORTED_BENCHMARKS))
        raise ValueError(
            f"Unsupported benchmark '{benchmark}' in '{config_path}'. "
            f"Expected one of: {supported}"
        )
    return benchmark


def sweep_dimensions(sweep: dict) -> list[tuple[str, str, list]]:
    dims: list[tuple[str, str, list]] = []
    for scope in sorted(sweep):
        param_map = sweep[scope]
        for key in sorted(param_map):
            values = param_map[key]
            assert isinstance(values, list), (
                f"sweep.{scope}.{key} must be a list of values, got {type(values).__name__}"
            )
            assert values, f"sweep.{scope}.{key} must not be empty"
            dims.append((scope, key, values))
    return dims


def expand_sweep(sweep: dict) -> list[dict]:
    dims = sweep_dimensions(sweep)
    if not dims:
        logger.warning("No sweep detected!")
        return [{"client_params": {}, "server_params": {}}]

    combos: list[dict] = []
    value_lists = [values for _, _, values in dims]
    for picked_values in itertools.product(*value_lists):
        combo = {"client_params": {}, "server_params": {}}
        for (scope, key, _), value in zip(dims, picked_values):
            combo[scope][key] = value
        combos.append(combo)
    return combos


def sanitize_name_part(value) -> str:
    text = str(value)
    text = text.replace("/", "-")
    return re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-") or "_"


def run_name_for_combo(combo: dict) -> str:
    parts: list[str] = []
    for scope, prefix in (("client_params", "client"), ("server_params", "server")):
        for key in sorted(combo[scope]):
            value = combo[scope][key]
            parts.append(f"{prefix}_{key}_{sanitize_name_part(value)}")
    return "__".join(parts) if parts else "default"


def describe_sweep(combo: dict) -> tuple[str | None, object | None]:
    entries: list[tuple[str, object]] = []
    for scope, prefix in (("client_params", "client"), ("server_params", "server")):
        for key in sorted(combo[scope]):
            entries.append((f"{prefix}.{key}", combo[scope][key]))

    if not entries:
        return None, None
    if len(entries) == 1:
        return entries[0]
    return "run", {key: value for key, value in entries}


def merged_prepare_params(client_params: dict, prepare_params: dict) -> dict:
    params = dict(client_params)
    params.update(prepare_params)
    # sanitize the keys for prepare
    for key in ("online", "skipload", "disable_console_logger"):
        params.pop(key, None)
    return params


def resolve_server_endpoint(
    server_params: dict, client_params: dict
) -> tuple[str, int]:
    host = server_params.get("bind") or client_params.get("host") or "127.0.0.1"
    port = int(server_params.get("port") or client_params.get("port")) or 6379
    return host, port


def dump_config(path: Path, payload: dict) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(payload, f, sort_keys=False)


def launch_server(
    server_project: str, server_params: dict, log_path: Path
) -> subprocess.Popen | None:
    cmd = build_command(server_project, server_params, is_server=True)

    logger.info(f"[server] launching: {format_command(cmd)}")
    if dry_run:
        logger.info("[server] [dry-run] skipping launch")
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
    logger.info(f"[server] waiting for {host}:{port} ...")
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(
                f"Server process exited unexpectedly (code {proc.returncode}) "
                f"before becoming ready on {host}:{port}"
            )
        try:
            with socket.create_connection((host, port), timeout=1):
                logger.info(f"[server] ready on {host}:{port}")
                return
        except OSError:
            time.sleep(SERVER_READY_INTERVAL)
    raise TimeoutError(
        f"Server did not become ready on {host}:{port} within {SERVER_READY_TIMEOUT}s"
    )


def shutdown_server(proc: subprocess.Popen | None) -> None:
    if dry_run or proc is None:
        return
    logger.info("[server] shutting down...")
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    logger.info("[server] stopped.")


def run_command(
    label: str,
    run_name: str,
    run_dir: Path,
    cmd: list[str],
    server_proc: subprocess.Popen | None = None,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info(f"Run: {run_name}")
    logger.info(f"Step: {label}")
    logger.info(f"Dir: {run_dir}")
    logger.info(f"Cmd: {format_command(cmd)}")
    logger.info("=" * 60)

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
                    f"during {label} for run '{run_name}'"
                )
            time.sleep(0.1)

    elapsed = time.time() - start
    rc = proc.returncode
    logger.info(f"Finished in {elapsed:.1f}s (exit code {rc})")
    if rc != 0:
        raise RuntimeError(f"{label} for run '{run_name}' failed with exit code {rc}")


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
    prepare_params: dict,
    no_server: bool,
) -> None:
    sweep_param, sweep_value = describe_sweep(sweep_combo)
    server_cmd = build_command(server_project, server_params, is_server=True)
    prepare_cmd = None
    if prepare_params:
        prepared_client_params = merged_prepare_params(client_params, prepare_params)
        prepare_cmd = build_command(benchmark_project, prepared_client_params)
    else:
        prepared_client_params = None
    benchmark_cmd = build_command(benchmark_project, client_params)

    config_record = {
        "experiment": exp_name,
        "benchmark": benchmark,
        "run_name": run_name,
        "client_params": client_params,
        "server_params": server_params,
        "sweep": sweep_combo,
        "sweep_param": sweep_param,
        "sweep_value": sweep_value,
        "server_cmd": format_command(server_cmd),
        "prepare_cmd": format_command(prepare_cmd) if prepare_cmd is not None else "",
        "client_cmd": format_command(benchmark_cmd),
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
            run_command(
                "prepare",
                run_name,
                run_dir / "prepare",
                prepare_cmd,
                server_proc=server_proc,
            )

        run_command(
            "benchmark",
            run_name,
            run_dir / "benchmark",
            benchmark_cmd,
            server_proc=server_proc,
        )
    finally:
        if not no_server:
            shutdown_server(server_proc)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s: [%(levelname)s] %(message)s",
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

    config_path = args.config or str(
        REPO_ROOT / "experiment" / "configs" / f"{args.experiment}.yaml"
    )
    cfg = load_config(config_path)
    exp_name = cfg.get("name", args.experiment)
    benchmark = get_benchmark(cfg, config_path)
    benchmark_project = cfg.get(
        "benchmark_project", "benchmark/Resp.benchmark/Resp.benchmark.csproj"
    )
    server_project = cfg.get("server_project", DEFAULT_SERVER_PROJECT)
    prepare_params = cfg.get("prepare", {}).get("client_params", {})
    if not prepare_params:
        logger.warning("empty prepare.client_params")
    base_section = cfg["base"]
    base_client_params = base_section["client_params"]
    base_server_params = base_section.get("server_params", {})
    if not base_server_params:
        logger.warning("empty base.server_params")
    sweep = cfg["sweep"]
    no_server = cfg.get("no_server", False)

    exp_dir = RESULT_ROOT / exp_name

    logger.info(f"[{exp_name}] Killing leftover processes...")
    killall_leftover(server_project, benchmark_project)

    logger.info(f"[{exp_name}] Cleaning result directory...")
    cleanup_result_dir(exp_dir)
    dump_config(exp_dir / "config.yaml", cfg)

    combos = expand_sweep(sweep)
    logger.info(f"[{exp_name}] Expanded {len(combos)} run(s)")

    for combo in combos:
        run_name = run_name_for_combo(combo)
        run_dir = exp_dir / run_name
        client_params = dict(base_client_params)
        client_params.update(combo["client_params"])
        server_params = dict(base_server_params)
        server_params.update(combo["server_params"])

        logger.info(f"[{exp_name}] === Run: {run_name} ===")
        execute_run(
            exp_name=exp_name,
            benchmark=benchmark,
            benchmark_project=benchmark_project,
            server_project=server_project,
            run_dir=run_dir,
            run_name=run_name,
            client_params=client_params,
            server_params=server_params,
            sweep_combo=combo,
            prepare_params=prepare_params,
            no_server=no_server,
        )

    logger.info(f"All runs complete. Results in: {exp_dir}")


if __name__ == "__main__":
    main()
