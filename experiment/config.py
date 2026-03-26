import itertools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_ROOT = REPO_ROOT / "result"

DEFAULT_BENCHMARK_PROJECT = "benchmark/Resp.benchmark/Resp.benchmark.csproj"
DEFAULT_SERVER_PROJECT = "main/GarnetServer/GarnetServer.csproj"
SUPPORTED_BENCHMARKS = {"online", "offline", "aof"}
SWEEP_SCOPES = ("client_params", "server_params")
SCOPE_PREFIXES = {"client_params": "c", "server_params": "s"}
SWEEP_PARAM_PREFIXES = {"client_params": "client", "server_params": "server"}


@dataclass(frozen=True)
class ExperimentSpec:
    name: str
    benchmark: str
    benchmark_project: str
    server_project: str
    prepare_params: dict[str, Any]
    base_client_params: dict[str, Any]
    base_server_params: dict[str, Any]
    no_server: bool
    combos: list[dict[str, dict[str, Any]]]
    config: dict[str, Any]
    config_path: Path


@dataclass(frozen=True)
class ResolvedRunSpec:
    combo: dict[str, dict[str, Any]]
    run_name: str
    client_params: dict[str, Any]
    server_params: dict[str, Any]
    sweep_params: dict[str, Any]


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_experiment_spec(
    config_path: str | Path,
    *,
    default_name: str | None = None,
    default_benchmark_project: str = DEFAULT_BENCHMARK_PROJECT,
    default_server_project: str = DEFAULT_SERVER_PROJECT,
) -> ExperimentSpec:
    path = Path(config_path)
    config = load_yaml_config(path)
    benchmark = config.get("benchmark")
    if benchmark is None:
        raise ValueError(f"Config '{path}' is missing required field 'benchmark'")
    if benchmark not in SUPPORTED_BENCHMARKS:
        raise ValueError(
            f"Unsupported benchmark '{benchmark}' in '{path}'. "
            f"Expected one of: {', '.join(sorted(SUPPORTED_BENCHMARKS))}"
        )

    return ExperimentSpec(
        name=config.get("name", default_name or path.stem),
        benchmark=benchmark,
        benchmark_project=config.get(
            "benchmark_project", default_benchmark_project
        ),
        server_project=config.get("server_project", default_server_project),
        prepare_params=dict(config.get("prepare", {}).get("client_params", {})),
        base_client_params=dict(config["base"]["client_params"]),
        base_server_params=dict(config["base"].get("server_params", {})),
        no_server=config.get("no_server", False),
        combos=expand_sweep(config.get("sweep", {})),
        config=config,
        config_path=path,
    )


def _sweep_dimensions(
    sweep: dict[str, dict[str, list[Any]]],
) -> list[tuple[str, str, list[Any]]]:
    dims: list[tuple[str, str, list[Any]]] = []
    for scope in SWEEP_SCOPES:
        param_map = sweep.get(scope, {}) or {}
        for key, values in param_map.items():
            assert isinstance(values, list), (
                f"sweep.{scope}.{key} must be a list of values, "
                f"got {type(values).__name__}"
            )
            assert values, f"sweep.{scope}.{key} must not be empty"
            dims.append((scope, key, values))
    return dims


def expand_sweep(
    sweep: dict[str, dict[str, list[Any]]],
) -> list[dict[str, dict[str, Any]]]:
    dims = _sweep_dimensions(sweep)
    if not dims:
        return [{"client_params": {}, "server_params": {}}]

    combos: list[dict[str, dict[str, Any]]] = []
    value_lists = [values for _, _, values in dims]
    for picked_values in itertools.product(*value_lists):
        combo = {scope: {} for scope in SWEEP_SCOPES}
        for (scope, key, _), value in zip(dims, picked_values):
            combo[scope][key] = value
        combos.append(combo)
    return combos


def sanitize_name_part(value: Any) -> str:
    text = str(value)
    text = text.replace("/", "-")
    return re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-") or "_"


def run_name_for_combo(combo: dict[str, dict[str, Any]]) -> str:
    parts: list[str] = []
    for scope in SWEEP_SCOPES:
        for key, value in combo.get(scope, {}).items():
            parts.append(f"{SCOPE_PREFIXES[scope]}.{key}.{sanitize_name_part(value)}")
    return "-".join(parts) if parts else "default"


def flatten_sweep_params(combo: dict[str, dict[str, Any]]) -> dict[str, Any]:
    entries: dict[str, Any] = {}
    for scope in SWEEP_SCOPES:
        for key, value in combo.get(scope, {}).items():
            prefix = SWEEP_PARAM_PREFIXES[scope]
            entries[f"{prefix}.{key}"] = value
    return entries


def resolve_run_spec(
    spec: ExperimentSpec, combo: dict[str, dict[str, Any]]
) -> ResolvedRunSpec:
    client_params = dict(spec.base_client_params)
    client_params.update(combo.get("client_params", {}))

    server_params = dict(spec.base_server_params)
    server_params.update(combo.get("server_params", {}))

    return ResolvedRunSpec(
        combo=combo,
        run_name=run_name_for_combo(combo),
        client_params=client_params,
        server_params=server_params,
        sweep_params=flatten_sweep_params(combo),
    )


def expected_run_dirs(exp_dir: Path) -> list[Path]:
    experiment_config_path = exp_dir / "config.yaml"
    if not experiment_config_path.exists():
        raise FileNotFoundError(
            f"Experiment config not found: {experiment_config_path}"
        )

    spec = load_experiment_spec(experiment_config_path)
    return [exp_dir / resolve_run_spec(spec, combo).run_name for combo in spec.combos]
