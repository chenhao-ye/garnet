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
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import REPO_ROOT, load_experiment_spec

OPTIONS_CS_PATH = REPO_ROOT / "benchmark/Resp.benchmark/Options.cs"
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


def config_path_for_arg(experiment: str, override: str | None) -> Path:
    if override:
        return Path(override)

    candidate = Path(experiment)
    if candidate.exists():
        return candidate

    return REPO_ROOT / "experiment" / "configs" / f"{experiment}.yaml"


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
            ["aof_bench", "skipload", "op", "batchsize", "client", "pool", "client_hist"],
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
        "itp",
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
        iter_param_contexts(base_params, sweep_client_params, ["online", "aof_bench"])
    )
    possible_modes = {
        infer_main_mode(context)
        for context in mode_contexts
    }

    if benchmark_label == "online" and "online" not in possible_modes:
        add_issue(
            issues,
            "ERROR",
            scope,
            "experiment benchmark is 'online' but client_params does not enable online mode",
        )
    if benchmark_label == "aof" and "aof" not in possible_modes:
        add_issue(
            issues,
            "ERROR",
            scope,
            "experiment benchmark is 'aof' but client_params does not enable aof_bench mode",
        )

    if len(possible_modes) > 1:
        add_issue(
            issues,
            "WARNING",
            scope,
            f"sweep changes the effective Resp.benchmark mode across runs: {format_values(sorted(possible_modes))}",
        )

    if "online" in possible_modes:
        validate_online_mode(issues, scope, base_params, sweep_client_params, specified_keys)
    if "aof" in possible_modes:
        validate_aof_mode(issues, scope, base_params, sweep_client_params, specified_keys)


def print_issues(issues: list[Issue], config_path: Path) -> None:
    if not issues:
        print(f"OK: {config_path} is consistent with Resp.benchmark option handling")
        return

    for issue in issues:
        print(f"{issue.level}: {issue.scope}: {issue.message}")

    errors = sum(issue.level == "ERROR" for issue in issues)
    warnings = sum(issue.level == "WARNING" for issue in issues)
    print(
        f"Summary: {errors} error(s), {warnings} warning(s) in {config_path}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate experiment configs against Resp.benchmark behavior"
    )
    parser.add_argument(
        "experiment",
        help="Experiment name or path to YAML config",
    )
    parser.add_argument(
        "--config",
        help="Override config path (default: experiment/configs/<name>.yaml)",
    )
    args = parser.parse_args()

    config_path = config_path_for_arg(args.experiment, args.config)
    spec = load_experiment_spec(config_path, default_name=Path(args.experiment).stem)
    supported_client_params = option_names_from_options_cs(OPTIONS_CS_PATH)
    issues: list[Issue] = []

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
    validate_param_keys(
        issues,
        spec.base_server_params,
        scope="base.server_params",
        supported=SUPPORTED_SERVER_OPTION_NAMES,
        kind="server",
    )

    sweep = spec.config.get("sweep", {}) or {}
    sweep_client_params = dict((sweep.get("client_params", {}) or {}))
    for scope, param_map in sweep.items():
        param_map = param_map or {}
        supported = (
            supported_client_params
            if scope == "client_params"
            else SUPPORTED_SERVER_OPTION_NAMES
        )
        kind = "client" if scope == "client_params" else "server"
        validate_param_keys(
            issues,
            param_map,
            scope=f"sweep.{scope}",
            supported=supported,
            kind=kind,
        )

    if spec.no_server:
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

    print_issues(issues, spec.config_path)
    sys.exit(1 if any(issue.level == "ERROR" for issue in issues) else 0)


if __name__ == "__main__":
    main()
