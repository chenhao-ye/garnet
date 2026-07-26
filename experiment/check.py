#!/usr/bin/env -S uv run

"""
Validate Garnet experiment YAML configs against Resp.benchmark option handling.

The checker focuses on two failure modes:
  1. parameters that do not map to any Resp.benchmark option
  2. parameters that parse successfully but are ignored or overridden for the
     selected benchmark mode

Usage:
    uv run experiment/check.py readonly
    uv run experiment/check.py experiment/configs/readonly.yaml
"""

import argparse
import itertools
import os
import re
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import (
    REPO_ROOT,
    config_path_for,
    load_experiment_spec,
    remote_mode,
    resolve_run_spec,
)

OPTIONS_CS_PATH = REPO_ROOT / "benchmark/Resp.benchmark/Options.cs"
GARNET_OPTIONS_CS_PATH = REPO_ROOT / "libs/host/Configuration/Options.cs"
SUPPORTED_SERVER_PARAMS = {
    "bind",
    "port",
    "aof",
    "aof_null_device",
    "cluster",
    "tls",
    "index",
    "aof_commit_freq",
    "aof_physical_sublog_count",
    "aof_replay_task_count",
    "aof_memory",
    "aof_memory_size",
    "aof_page_size",
}


@dataclass(frozen=True)
class Issue:
    level: str
    scope: str
    message: str


def flag_for_param(key: str) -> str:
    return f"--{key.replace('_', '-')}"


def normalize_option_name(key: str) -> str:
    return flag_for_param(key)[2:]


SUPPORTED_SERVER_OPTION_NAMES = {
    normalize_option_name(key) for key in SUPPORTED_SERVER_PARAMS
}
CLIENT_DEFAULTS = {
    "online": False,
    "aof_bench": False,
    "replication_bench": False,
    "skipload": False,
    "op": "GET",
    "batchsize": [4096],
    "client": "LightClient",
    "pool": False,
    "client_hist": False,
    "aof_bench_type": "Replay",
    "cluster": False,
}


def option_names_from_options_cs(path: Path) -> set[str]:
    text = path.read_text()
    return set(re.findall(r'\[Option\((?:\'[^\']+\',\s*)?"([^"]+)"', text))


def add_issue(issues: list[Issue], level: str, scope: str, message: str) -> None:
    issues.append(Issue(level=level, scope=scope, message=message))


def validate_param_keys(
    issues: list[Issue],
    params: dict[str, Any],
    *,
    scope: str,
    supported: set[str],
    kind: str,
) -> None:
    for key in sorted(params):
        if normalize_option_name(key) not in supported:
            add_issue(
                issues,
                "ERROR",
                scope,
                f"{kind} parameter '{key}' does not map to a known command-line option",
            )


def value_as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return [value]


def format_values(values: list[Any]) -> str:
    return ", ".join(str(value) for value in values)


def infer_main_mode(client_params: dict[str, Any]) -> str:
    if client_params.get("online"):
        return "online"
    if client_params.get("aof_bench"):
        return "aof"
    if client_params.get("replication_bench"):
        return "replication"
    return "throughput"


def param_values(
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    key: str,
) -> list[Any]:
    if key in sweep_client_params:
        return list(sweep_client_params[key])
    if key in base_params:
        return [base_params[key]]
    if key in CLIENT_DEFAULTS:
        return [CLIENT_DEFAULTS[key]]
    return [None]


def iter_param_contexts(
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    keys: list[str],
):
    values = [param_values(base_params, sweep_client_params, key) for key in keys]
    for picked in itertools.product(*values):
        yield dict(zip(keys, picked))


def validate_prepare_phase(issues: list[Issue], prepare_params: dict[str, Any]) -> None:
    if not prepare_params:
        return

    prepare_scope = "prepare.client_params"
    if prepare_params.get("online"):
        add_issue(
            issues,
            "ERROR",
            prepare_scope,
            "prepare phase sets 'online: true', which runs the online benchmark instead of a preload step",
        )
    if prepare_params.get("aof_bench"):
        add_issue(
            issues,
            "ERROR",
            prepare_scope,
            "prepare phase sets 'aof_bench: true', which runs AOF bench instead of preloading data",
        )
    if prepare_params.get("skipload"):
        add_issue(
            issues,
            "ERROR",
            prepare_scope,
            "prepare phase sets 'skipload: true', which skips the load work the prepare phase is meant to do",
        )


def validate_online_mode(
    issues: list[Issue],
    scope: str,
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    specified_keys: set[str],
) -> None:
    online_contexts = list(
        iter_param_contexts(
            base_params,
            sweep_client_params,
            [
                "aof_bench",
                "skipload",
                "op",
                "batchsize",
                "client",
                "pool",
                "client_hist",
            ],
        )
    )

    if any(context["aof_bench"] for context in online_contexts):
        add_issue(
            issues,
            "ERROR",
            scope,
            "'online: true' and 'aof_bench: true' can both be enabled; Resp.benchmark takes the online path first",
        )

    if any(context["skipload"] for context in online_contexts):
        add_issue(
            issues,
            "ERROR",
            scope,
            "'skipload: true' is not supported with online mode",
        )

    op_values = param_values(base_params, sweep_client_params, "op")
    non_get_ops = [value for value in op_values if value != "GET"]
    if "op" in specified_keys and non_get_ops:
        add_issue(
            issues,
            "ERROR",
            scope,
            f"'op' uses unsupported online-mode value(s): {format_values(non_get_ops)}; use op_workload/op_percent instead",
        )

    if "op" in specified_keys and not non_get_ops:
        add_issue(
            issues,
            "WARNING",
            scope,
            "'op' is ignored in online mode; Resp.benchmark uses op_workload/op_percent instead",
        )

    if "batchsize" in specified_keys:
        batch_values = param_values(base_params, sweep_client_params, "batchsize")
        overridden_batch_values = [
            value for value in batch_values if value_as_list(value) != [1]
        ]
        if overridden_batch_values:
            add_issue(
                issues,
                "WARNING",
                scope,
                f"'batchsize' value(s) {format_values(overridden_batch_values)} are overridden to 1 in online mode",
            )

    if any(
        context["client"] == "LightClient" and context["pool"]
        for context in online_contexts
    ):
        add_issue(
            issues,
            "ERROR",
            scope,
            "pooling is not supported with client=LightClient in online mode",
        )

    if any(context["pool"] and context["client_hist"] for context in online_contexts):
        add_issue(
            issues,
            "ERROR",
            scope,
            "'pool' and 'client_hist' cannot be enabled together in online mode",
        )


def validate_aof_mode(
    issues: list[Issue],
    scope: str,
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    specified_keys: set[str],
) -> None:
    aof_contexts = list(
        iter_param_contexts(
            base_params,
            sweep_client_params,
            ["online", "aof_bench_type", "client", "cluster"],
        )
    )

    if any(context["online"] for context in aof_contexts):
        add_issue(
            issues,
            "ERROR",
            scope,
            "'aof_bench: true' and 'online: true' can both be enabled; Resp.benchmark takes the online path first",
        )

    if any(
        context["client"] == "InProc"
        and str(context["aof_bench_type"]) == "Replay"
        and not context["cluster"]
        for context in aof_contexts
    ):
        add_issue(
            issues,
            "ERROR",
            scope,
            "InProc AOF replay requires 'cluster: true'",
        )

    common_ignored_keys = {
        "op",
        "totalops",
        "batchsize",
        "skipload",
        "pool",
        "sync",
        "op_workload",
        "op_percent",
        "sscardinality",
        "ttl",
        "client_hist",
        "txn",
    }
    common_ignored = sorted(common_ignored_keys & specified_keys)
    if common_ignored:
        add_issue(
            issues,
            "WARNING",
            scope,
            f"AOF bench ignores parameter(s): {format_values(common_ignored)}",
        )

    replay_types = {
        str(value)
        for value in param_values(base_params, sweep_client_params, "aof_bench_type")
    }
    if "Replay" in replay_types and "threads" in specified_keys:
        add_issue(
            issues,
            "WARNING",
            scope,
            "'threads' is ignored for AOF replay; replay uses aof_physical_sublog_count threads",
        )

    client_values = {
        str(value) for value in param_values(base_params, sweep_client_params, "client")
    }
    if "InProc" in client_values:
        ignored_connection_keys = {
            "host",
            "port",
            "clientaddr",
            "auth",
            "tls",
            "tlshost",
            "cert_file_name",
            "cert_password",
        }
        ignored = sorted(ignored_connection_keys & specified_keys)
        if ignored:
            add_issue(
                issues,
                "WARNING",
                scope,
                f"embedded InProc AOF bench ignores connection parameter(s): {format_values(ignored)}",
            )

    # 'itp' pipelines GETs only on the GarnetClientSession reader path (AofBench sizes the
    # client send buffer by it and runs the parallel reader loop when itp > 1); it has no
    # effect for InProc readers or when no reader threads run.
    if "itp" in specified_keys:
        reader_counts = param_values(base_params, sweep_client_params, "aof_replay_reader")
        gcs_reader_possible = "GarnetClientSession" in client_values and any(
            (count or 0) > 0 for count in reader_counts
        )
        if not gcs_reader_possible:
            add_issue(
                issues,
                "WARNING",
                scope,
                "'itp' only affects GarnetClientSession reader threads (aof_replay_reader > 0); it is ignored for this config",
            )


def validate_offline_mode(
    issues: list[Issue],
    scope: str,
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    specified_keys: set[str],
) -> None:
    offline_contexts = list(
        iter_param_contexts(
            base_params,
            sweep_client_params,
            ["client", "op"],
        )
    )

    common_ignored_keys = {
        "pool",
        "itp",
        "sync",
        "op_workload",
        "op_percent",
        "sscardinality",
        "client_hist",
        "aof_bench_type",
    }
    common_ignored = sorted(common_ignored_keys & specified_keys)
    if common_ignored:
        add_issue(
            issues,
            "WARNING",
            scope,
            f"offline benchmark ignores parameter(s): {format_values(common_ignored)}",
        )

    external_client_ignored_keys = {
        "aof",
        "cluster",
        "aof_null_device",
        "aof_commit_freq",
        "aof_physical_sublog_count",
        "aof_replay_task_count",
        "aof_memory",
        "aof_memory_size",
        "aof_page_size",
        "index",
    }
    if any(context["client"] != "InProc" for context in offline_contexts):
        ignored = sorted(external_client_ignored_keys & specified_keys)
        if ignored:
            add_issue(
                issues,
                "WARNING",
                scope,
                f"offline benchmark with a non-InProc client ignores embedded-server parameter(s): {format_values(ignored)}",
            )

    if any(
        context["client"] in {"GarnetClientSession", "SERedis"}
        and context["op"] != "MSET"
        for context in offline_contexts
    ):
        add_issue(
            issues,
            "ERROR",
            scope,
            "offline benchmark supports client=GarnetClientSession and client=SERedis only with 'op: MSET'",
        )


def validate_replication_mode(
    issues: list[Issue],
    scope: str,
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    specified_keys: set[str],
) -> None:
    common_ignored_keys = {
        "op",
        "totalops",
        "batchsize",
        "skipload",
        "pool",
        "sync",
        "op_workload",
        "op_percent",
        "sscardinality",
        "ttl",
        "client_hist",
        "txn",
        "threads",
        "aof_bench_type",
        "aof_gen_pages",
        "aof_replay_reader",
        "aof_reader_skip",
    }
    common_ignored = sorted(common_ignored_keys & specified_keys)
    if common_ignored:
        add_issue(
            issues,
            "WARNING",
            scope,
            f"replication bench ignores parameter(s): {format_values(common_ignored)}",
        )


def validate_main_config(
    issues: list[Issue],
    *,
    benchmark_label: str,
    base_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
    specified_keys: set[str],
) -> None:
    scope = "benchmark.client_params"
    mode_contexts = list(
        iter_param_contexts(
            base_params, sweep_client_params, ["online", "aof_bench", "replication_bench"]
        )
    )
    possible_modes = {infer_main_mode(context) for context in mode_contexts}

    if benchmark_label == "online" and "online" not in possible_modes:
        add_issue(
            issues,
            "ERROR",
            scope,
            "experiment benchmark is 'online' but client_params does not enable online mode",
        )
    if benchmark_label == "offline" and "throughput" not in possible_modes:
        add_issue(
            issues,
            "ERROR",
            scope,
            "experiment benchmark is 'offline' but client_params enables a different mode",
        )
    if benchmark_label == "aof" and "aof" not in possible_modes:
        add_issue(
            issues,
            "ERROR",
            scope,
            "experiment benchmark is 'aof' but client_params does not enable aof_bench mode",
        )
    if benchmark_label == "replication" and "replication" not in possible_modes:
        add_issue(
            issues,
            "ERROR",
            scope,
            "experiment benchmark is 'replication' but client_params does not enable replication_bench mode",
        )

    if len(possible_modes) > 1:
        add_issue(
            issues,
            "WARNING",
            scope,
            f"sweep changes the effective Resp.benchmark mode across runs: {format_values(sorted(possible_modes))}",
        )

    if "online" in possible_modes:
        validate_online_mode(
            issues, scope, base_params, sweep_client_params, specified_keys
        )
    if "throughput" in possible_modes:
        validate_offline_mode(
            issues, scope, base_params, sweep_client_params, specified_keys
        )
    if "aof" in possible_modes:
        validate_aof_mode(
            issues, scope, base_params, sweep_client_params, specified_keys
        )
    if "replication" in possible_modes:
        validate_replication_mode(
            issues, scope, base_params, sweep_client_params, specified_keys
        )


def validate_config_name_matches_filename(
    issues: list[Issue], *, config: dict[str, Any], config_path: Path
) -> None:
    configured_name = config.get("name")
    if configured_name is None:
        return

    expected_name = config_path.stem
    if str(configured_name) != expected_name:
        add_issue(
            issues,
            "ERROR",
            "config.name",
            f"config name '{configured_name}' does not match filename '{expected_name}'",
        )


def print_issues(issues: list[Issue], config_path: Path) -> None:
    if not issues:
        print(f"OK: {config_path} is consistent with Resp.benchmark option handling")
        return

    for issue in issues:
        print(f"{issue.level}: {issue.scope}: {issue.message}")

    errors = sum(issue.level == "ERROR" for issue in issues)
    warnings = sum(issue.level == "WARNING" for issue in issues)
    print(f"Summary: {errors} error(s), {warnings} warning(s) in {config_path}")


def _expand_cpu_list(spec: Any) -> list[int]:
    """Expand a numactl / sysfs list like '0,2,4-6' into [0, 2, 4, 5, 6]."""
    result: list[int] = []
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            result.extend(range(int(lo), int(hi) + 1))
        else:
            result.append(int(part))
    return result


def _cores_in_numa_nodes(node_spec: Any) -> int | None:
    """Total logical CPUs across the given NUMA node(s); None if topology unreadable."""
    total = 0
    for node in _expand_cpu_list(node_spec):
        cpulist = Path(f"/sys/devices/system/node/node{node}/cpulist")
        if not cpulist.exists():
            return None
        total += len(_expand_cpu_list(cpulist.read_text().strip()))
    return total


def validate_split_roles(
    issues: list[Issue],
    *,
    spec: Any,
    sweep_server_params: dict[str, Any],
    sweep_client_params: dict[str, Any],
) -> None:
    """Cross-checks for split-process AOF runs (Replica-role bench in the server slot,
    Client-role bench in the client slot). There is no runtime parameter handshake -- this
    static consistency check is the sole guard."""

    def scope_values(base: dict[str, Any], sweep_map: dict[str, Any], key: str) -> set[str]:
        if key in sweep_map:
            return {str(v) for v in sweep_map[key]}
        if key in base:
            return {str(base[key])}
        return set()

    s_roles = {
        v.lower()
        for v in scope_values(spec.base_server_params, sweep_server_params, "aof_bench_role")
    }
    c_roles = {
        v.lower()
        for v in scope_values(spec.base_client_params, sweep_client_params, "aof_bench_role")
    }

    if "primary" in (s_roles | c_roles):
        add_issue(
            issues,
            "ERROR",
            "aof_bench_role",
            "'Primary' is reserved for a future streaming-AOF bench and is not implemented",
        )

    if "client" not in c_roles and "replica" not in s_roles:
        return  # not a split run

    if "client" in c_roles and "replica" not in s_roles:
        add_issue(
            issues,
            "ERROR",
            "aof_bench_role",
            "client_params run the Client role but server_params do not run the Replica role",
        )
    if "replica" in s_roles and "client" not in c_roles:
        add_issue(
            issues,
            "ERROR",
            "aof_bench_role",
            "server_params run the Replica role but client_params do not run the Client role; "
            "the replica would wait on the control channel forever",
        )
    if "replica" in s_roles and spec.no_server:
        add_issue(
            issues,
            "ERROR",
            "aof_bench_role",
            "a Replica-role server requires no_server: false (the harness must launch it)",
        )

    if "client" in c_roles:
        client_types = scope_values(spec.base_client_params, sweep_client_params, "client")
        if client_types and client_types != {"GarnetClientSession"}:
            add_issue(
                issues,
                "ERROR",
                "aof_bench_role",
                "the Client role requires client: GarnetClientSession",
            )

    # The Client role regenerates the keyset locally; both processes must agree on the
    # parameters that determine it.
    for key in ("dbsize", "keylength"):
        s_vals = scope_values(spec.base_server_params, sweep_server_params, key)
        c_vals = scope_values(spec.base_client_params, sweep_client_params, key)
        if s_vals != c_vals:
            add_issue(
                issues,
                "ERROR",
                "aof_bench_role",
                f"'{key}' differs between server_params ({sorted(s_vals) or 'unset'}) and "
                f"client_params ({sorted(c_vals) or 'unset'}); the Client role would "
                "regenerate a different keyset",
            )


def validate_replication_roles(issues: list[Issue], *, spec: Any) -> None:
    """Cross-checks for replication-bench runs: GarnetServer primary/replica slots plus
    the bench client, with consistent endpoints and sublog counts. There is no runtime
    parameter handshake -- this static consistency check is the sole guard."""
    has_primary = bool(spec.base_primary_params) or any(
        combo.get("primary_params") for combo in spec.combos
    )
    has_replica = bool(spec.base_replica_params) or any(
        combo.get("replica_params") for combo in spec.combos
    )
    if not has_primary and not has_replica:
        return

    scope = "replication"
    if has_primary != has_replica:
        add_issue(
            issues,
            "ERROR",
            scope,
            "primary_params and replica_params must be configured together",
        )
        return
    if not spec.no_server:
        add_issue(
            issues,
            "ERROR",
            scope,
            "primary_params/replica_params require no_server: true "
            "(the generic server slot stays off; the harness launches the primary and "
            "replica GarnetServers itself)",
        )
    if spec.benchmark != "replication":
        add_issue(
            issues,
            "ERROR",
            scope,
            f"primary_params/replica_params require benchmark: replication, got '{spec.benchmark}'",
        )

    # Validate every expanded run so swept values stay paired; identical messages across
    # combos are reported once.
    seen: set[tuple[str, str]] = set()

    def once(issue_scope: str, message: str) -> None:
        if (issue_scope, message) not in seen:
            seen.add((issue_scope, message))
            add_issue(issues, "ERROR", issue_scope, message)

    for combo in spec.combos:
        run = resolve_run_spec(spec, combo)
        p, r, c = run.primary_params, run.replica_params, run.client_params

        if not c.get("replication_bench"):
            once("client_params", "replication_bench: true is required in client_params")
        if str(c.get("client")) != "GarnetClientSession":
            once(
                "client_params",
                "the replication client requires client: GarnetClientSession",
            )

        # The harness bootstraps a cluster-replicated pair; both servers must run with the
        # cluster layer and the AOF enabled.
        for scope_name, params in (("primary_params", p), ("replica_params", r)):
            for key in ("cluster", "aof"):
                if not params.get(key):
                    once(scope_name, f"'{key}: true' is required for the replication pair")

        # The AOF stream is sharded per physical sublog; primary and replica must agree
        # on the sublog count.
        p_m = p.get("aof_physical_sublog_count", 1)
        r_m = r.get("aof_physical_sublog_count", 1)
        if p_m != r_m:
            once(
                scope,
                f"aof_physical_sublog_count differs between primary_params ({p_m}) and "
                f"replica_params ({r_m}); pair the values via sweep_combo or set it once "
                f"under server_params (applied to both)",
            )

        # Endpoint cross-references: the client writes to the primary's port and reads
        # from the replica's port (the replica's attach target comes from the harness
        # bootstrap, not from params).
        if c.get("primary_port") != p.get("port"):
            once(
                scope,
                f"client_params.primary_port ({c.get('primary_port')}) must equal the "
                f"primary's port ({p.get('port')})",
            )
        if c.get("replica_port") != r.get("port"):
            once(
                scope,
                f"client_params.replica_port ({c.get('replica_port')}) must equal the "
                f"replica's port ({r.get('port')})",
            )


def validate_remote(issues: list[Issue], *, spec: Any) -> None:
    """Cross-checks for the 'remote' section (replication roles mapped to ssh hosts).
    Schema shape is validated while loading the config; this validates the semantics."""
    if not spec.remote:
        return
    if spec.benchmark != "replication":
        add_issue(
            issues,
            "ERROR",
            "remote",
            f"the remote section applies to benchmark: replication only, got '{spec.benchmark}'",
        )
        return
    if not remote_mode(spec.remote):
        return  # ip-only entries: nothing launches over ssh

    # run.py injects the resolved data-plane IPs; mirroring its resolution here fails fast
    # before any machine time is spent.
    for role in ("primary", "replica"):
        entry = spec.remote.get(role)
        if entry is not None and entry.ip is not None:
            continue
        name = (
            entry.ssh
            if entry is not None and entry.ssh is not None
            else socket.gethostname().split(".")[0]
        )
        try:
            socket.gethostbyname(name)
        except OSError:
            add_issue(
                issues,
                "ERROR",
                "remote",
                f"cannot resolve an IP for the {role} role from '{name}'; "
                f"set remote.{role}.ip explicitly",
            )

    # Manually set addresses would be silently overridden by the injection.
    sweep = spec.config.get("sweep", {}) or {}
    injected = (
        ("primary_params", spec.base_primary_params, ("bind",)),
        ("replica_params", spec.base_replica_params, ("bind",)),
        ("client_params", spec.base_client_params, ("primary_host", "replica_host")),
    )
    for scope, base, keys in injected:
        present = (set(base) | set(sweep.get(scope, {}) or {})) & set(keys)
        for key in sorted(present):
            add_issue(
                issues,
                "ERROR",
                scope,
                f"'{key}' is overridden by the remote-mode address injection; remove it "
                f"(use remote.<role>.ip to control the address)",
            )

    client_entry = spec.remote.get("client")
    if client_entry is not None and client_entry.ssh and spec.check.get("num_cores"):
        add_issue(
            issues,
            "WARNING",
            "check",
            "num_cores is validated against the local machine, but the client launches remotely",
        )


def _available_cores(client: Any) -> tuple[int | None, str]:
    """Logical CPUs the experiment may use, with a human description. Uses the client
    affinity binding when set (the role that runs the workload), else the whole machine."""
    if client is None:
        return os.cpu_count(), "whole machine"
    if client.cpus is not None:
        return len(_expand_cpu_list(client.cpus)), f"cpus={client.cpus}"
    return _cores_in_numa_nodes(client.numa_node), f"numa_node={client.numa_node}"


def validate_check_section(
    issues: list[Issue], *, check_cfg: dict[str, Any], affinity: Any
) -> None:
    """Validate the optional 'check' section: pre-run machine requirements."""
    if not check_cfg:
        return
    unknown = sorted(set(check_cfg) - {"num_cores"})
    if unknown:
        add_issue(
            issues, "ERROR", "check", f"unknown key(s) in 'check': {format_values(unknown)}"
        )

    num_cores = check_cfg.get("num_cores")
    if num_cores is None:
        return
    if isinstance(num_cores, bool) or not isinstance(num_cores, int) or num_cores <= 0:
        add_issue(
            issues,
            "ERROR",
            "check",
            f"'num_cores' must be a positive integer, got {num_cores!r}",
        )
        return

    available, where = _available_cores(affinity.client)
    if available is None:
        add_issue(
            issues,
            "WARNING",
            "check",
            f"cannot determine available cores ({where}); skipping num_cores check",
        )
        return
    if num_cores > available:
        add_issue(
            issues,
            "ERROR",
            "check",
            f"num_cores={num_cores} exceeds available logical CPUs ({available}, {where})",
        )


def _check_one(config: str, supported_client_params: set[str]) -> bool:
    """Validate one config. Returns True if any ERROR-level issues were found."""
    spec = load_experiment_spec(config_path_for(config), default_name=Path(config).stem)
    issues: list[Issue] = []

    validate_config_name_matches_filename(
        issues, config=spec.config, config_path=spec.config_path
    )

    validate_param_keys(
        issues,
        spec.prepare_params,
        scope="prepare.client_params",
        supported=supported_client_params,
        kind="prepare client",
    )
    validate_prepare_phase(issues, spec.prepare_params)

    validate_param_keys(
        issues,
        spec.base_client_params,
        scope="base.client_params",
        supported=supported_client_params,
        kind="client",
    )
    # The replication primary/replica slots run real GarnetServers; their params are
    # GarnetServer flags, validated against the host option set.
    supported_garnet_params = option_names_from_options_cs(GARNET_OPTIONS_CS_PATH)
    # server_params is a shared server layer folded into primary and replica. For a replication
    # pair those are real GarnetServers, so validate server_params against the full GarnetServer
    # option set; otherwise it configures the generic single server (narrower supported set), or a
    # bench in the server slot (split-process Replica role) which takes Resp.benchmark flags.
    bench_as_server = spec.server_project.endswith("Resp.benchmark.csproj")
    replication_roles = bool(spec.base_primary_params) or bool(spec.base_replica_params) or any(
        combo.get("primary_params") or combo.get("replica_params") for combo in spec.combos
    )
    if bench_as_server:
        server_supported, server_kind = supported_client_params, "client"
    elif replication_roles:
        server_supported, server_kind = supported_garnet_params, "server"
    else:
        server_supported, server_kind = SUPPORTED_SERVER_OPTION_NAMES, "server"
    validate_param_keys(
        issues,
        spec.base_server_params,
        scope="base.server_params",
        supported=server_supported,
        kind=server_kind,
    )
    for scope_name, params in (
        ("base.primary_params", spec.base_primary_params),
        ("base.replica_params", spec.base_replica_params),
    ):
        validate_param_keys(
            issues,
            params,
            scope=scope_name,
            supported=supported_garnet_params,
            kind="server",
        )
    for scope_name, params in (
        ("base.primary_params", spec.base_primary_params),
        ("base.replica_params", spec.base_replica_params),
    ):
        validate_param_keys(
            issues,
            params,
            scope=scope_name,
            supported=supported_garnet_params,
            kind="server",
        )

    sweep = spec.config.get("sweep", {}) or {}
    sweep_client_params = dict((sweep.get("client_params", {}) or {}))
    for scope, param_map in sweep.items():
        param_map = param_map or {}
        if scope == "client_params":
            supported, kind = supported_client_params, "client"
        elif scope in ("primary_params", "replica_params"):
            supported, kind = supported_garnet_params, "server"
        else:
            supported, kind = server_supported, server_kind
        validate_param_keys(
            issues,
            param_map,
            scope=f"sweep.{scope}",
            supported=supported,
            kind=kind,
        )

    # server_params is a shared layer applied to primary and replica, so it is meaningful for
    # a replication pair even under no_server=true; it is only ignored for a plain no_server
    # (InProc) bench that launches no server at all.
    has_replication_roles = (
        bool(spec.base_primary_params)
        or bool(spec.base_replica_params)
        or any(
            combo.get("primary_params") or combo.get("replica_params")
            for combo in spec.combos
        )
    )
    if spec.no_server and not has_replication_roles:
        ignored_server_keys = sorted(spec.base_server_params)
        for key in ignored_server_keys:
            add_issue(
                issues,
                "WARNING",
                "base.server_params",
                f"'{key}' is ignored because no_server=true skips server launch",
            )
        for key in sorted((sweep.get("server_params", {}) or {}).keys()):
            add_issue(
                issues,
                "WARNING",
                "sweep.server_params",
                f"'{key}' is ignored because no_server=true skips server launch",
            )

    specified_keys = set(spec.base_client_params) | set(sweep_client_params)
    validate_main_config(
        issues,
        benchmark_label=spec.benchmark,
        base_params=spec.base_client_params,
        sweep_client_params=sweep_client_params,
        specified_keys=specified_keys,
    )

    validate_check_section(issues, check_cfg=spec.check, affinity=spec.affinity)

    validate_split_roles(
        issues,
        spec=spec,
        sweep_server_params=dict(sweep.get("server_params", {}) or {}),
        sweep_client_params=sweep_client_params,
    )
    validate_replication_roles(issues, spec=spec)
    validate_remote(issues, spec=spec)

    print_issues(issues, spec.config_path)
    return any(issue.level == "ERROR" for issue in issues)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate experiment configs against Resp.benchmark behavior"
    )
    parser.add_argument(
        "configs",
        nargs="+",
        help="One or more experiment config names (or paths).",
    )
    args = parser.parse_args(argv)

    supported_client_params = option_names_from_options_cs(OPTIONS_CS_PATH)
    any_errors = False
    for config in args.configs:
        if _check_one(config, supported_client_params):
            any_errors = True
    return 1 if any_errors else 0


if __name__ == "__main__":
    sys.exit(main())
