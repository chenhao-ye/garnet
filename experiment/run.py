#!/usr/bin/env -S uv run
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pyyaml",
# ]
# ///
"""
Run a YCSB-style experiment against the Garnet online benchmark.

Usage:
    uv run experiment/run.py experiment/configs/scale_clients.yaml
    uv run experiment/run.py experiment/configs/scale_clients.yaml --dry-run
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import yaml

# Root of the repository (two levels up from this file).
REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"


# ---------------------------------------------------------------------------
# Config -> CLI flag mapping
# ---------------------------------------------------------------------------

# Flags whose value is a comma-joined list (e.g. op_workload: [GET, SET])
LIST_PARAMS = {"op_workload", "op_percent", "batchsize", "threads"}

# Bool flags (no value, just presence)
BOOL_PARAMS = {"online", "disable_console_logger", "skipload", "burst", "zipf",
               "lset", "pool", "tls", "aof", "cluster", "aof_null_device", "client_hist"}

# YAML key -> dotnet CLI flag name
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
    "aof_memory_size": "--aof-memory-size",
    "aof_page_size": "--aof-page-size",
    "aof_null_device": "--aof-null-device",
    "cluster": "--cluster",
    "totalops": "--totalops",
    "client_hist": "--client-hist",
}


def params_to_args(params: dict) -> list[str]:
    """Convert a params dict to a flat list of CLI arguments."""
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


def build_command(benchmark_project: str, params: dict) -> list[str]:
    project_path = REPO_ROOT / benchmark_project
    cmd = [
        "dotnet", "run", "-c", "Release",
        "--project", str(project_path),
        "--",
    ]
    cmd += params_to_args(params)
    return cmd


# ---------------------------------------------------------------------------
# Run a single benchmark
# ---------------------------------------------------------------------------

def run_single(run_name: str, run_dir: Path, cmd: list[str], config: dict,
               dry_run: bool) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    config_path = run_dir / "config.json"
    output_path = run_dir / "output.txt"

    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    print(f"\n{'='*60}")
    print(f"  Run: {run_name}")
    print(f"  Dir: {run_dir}")
    print(f"  Cmd: {' '.join(cmd)}")
    print(f"{'='*60}")

    if dry_run:
        print("  [dry-run] skipping execution")
        return

    start = time.time()
    with open(output_path, "w") as out_f:
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
        proc.wait()

    elapsed = time.time() - start
    rc = proc.returncode
    print(f"\n  Finished in {elapsed:.1f}s (exit code {rc})")
    if rc != 0:
        print(f"  [warn] non-zero exit code; results may be incomplete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Run Garnet YCSB experiments")
    parser.add_argument("config", help="Path to experiment YAML config")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    args = parser.parse_args()

    cfg = load_config(args.config)
    exp_name = cfg["name"]
    benchmark_project = cfg.get("benchmark_project",
                                "benchmark/Resp.benchmark/Resp.benchmark.csproj")
    base_params = dict(cfg.get("base_params", {}))
    sweep = cfg.get("sweep", {})

    exp_dir = RESULT_ROOT / exp_name

    if sweep:
        sweep_param = sweep["param"]
        sweep_values = sweep["values"]
        print(f"Experiment: {exp_name}  sweep {sweep_param} over {sweep_values}")
        for value in sweep_values:
            run_params = dict(base_params)
            run_params[sweep_param] = value
            run_name = f"{sweep_param}_{value}"
            run_dir = exp_dir / run_name
            config_record = {
                "experiment": exp_name,
                "run_name": run_name,
                "sweep_param": sweep_param,
                "sweep_value": value,
                "params": run_params,
            }
            cmd = build_command(benchmark_project, run_params)
            run_single(run_name, run_dir, cmd, config_record, args.dry_run)
    else:
        # Single run with base_params only
        run_name = "default"
        run_dir = exp_dir / run_name
        config_record = {
            "experiment": exp_name,
            "run_name": run_name,
            "params": base_params,
        }
        cmd = build_command(benchmark_project, base_params)
        run_single(run_name, run_dir, cmd, config_record, args.dry_run)

    print(f"\nAll runs complete. Results in: {exp_dir}")


if __name__ == "__main__":
    main()
