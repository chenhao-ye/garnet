import itertools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
_RESULT_ROOT = REPO_ROOT / "result"


def result_dir(name: str) -> Path:
    return _RESULT_ROOT / name


def config_path_for(name_or_path: str) -> Path:
    """Resolve a CLI arg to a config path.

    Accepts either a bare config name (looks up
    experiment/configs/<name>.yaml) or a filesystem path.
    """
    candidate = Path(name_or_path)
    if candidate.exists():
        return candidate
    return REPO_ROOT / "experiment" / "configs" / f"{name_or_path}.yaml"


DEFAULT_BENCHMARK_PROJECT = "benchmark/Resp.benchmark/Resp.benchmark.csproj"
DEFAULT_SERVER_PROJECT = "main/GarnetServer/GarnetServer.csproj"
SUPPORTED_BENCHMARKS = {"online", "offline", "aof"}
SWEEP_SCOPES = ("client_params", "server_params")
SCOPE_PREFIXES = {"client_params": "c", "server_params": "s"}
SWEEP_PARAM_PREFIXES = {"client_params": "client", "server_params": "server"}


@dataclass(frozen=True)
class AffinitySpec:
    numa_node: int
    cpus: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"numa_node": self.numa_node}
        if self.cpus is not None:
            d["cpus"] = self.cpus
        return d


@dataclass(frozen=True)
class Affinity:
    server: AffinitySpec | None = None
    client: AffinitySpec | None = None
    prepare: AffinitySpec | None = None

    def any_set(self) -> bool:
        return any(p is not None for p in (self.server, self.client, self.prepare))

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.server is not None:
            out["server"] = self.server.to_dict()
        if self.client is not None:
            out["client"] = self.client.to_dict()
        if self.prepare is not None:
            out["prepare"] = self.prepare.to_dict()
        return out


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
    repeat: int
    combos: list[dict[str, dict[str, Any]]]
    affinity: Affinity
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


def _parse_affinity_block(role: str, block: Any) -> AffinitySpec | None:
    if block is None:
        return None
    if not isinstance(block, dict):
        raise ValueError(f"affinity.{role} must be a mapping, got {type(block).__name__}")
    if "numa_node" not in block:
        raise ValueError(f"affinity.{role} requires 'numa_node'")
    numa_node = block["numa_node"]
    if not isinstance(numa_node, int) or isinstance(numa_node, bool):
        raise ValueError(
            f"affinity.{role}.numa_node must be an int, got {type(numa_node).__name__}"
        )
    cpus = block.get("cpus")
    if cpus is not None and not isinstance(cpus, str):
        raise ValueError(
            f"affinity.{role}.cpus must be a string (numactl --physcpubind syntax), "
            f"got {type(cpus).__name__}"
        )
    unknown = set(block) - {"numa_node", "cpus"}
    if unknown:
        raise ValueError(f"affinity.{role} has unknown keys: {sorted(unknown)}")
    return AffinitySpec(numa_node=numa_node, cpus=cpus)


def _parse_affinity(config: dict[str, Any]) -> Affinity:
    raw = config.get("affinity")
    if raw is None:
        return Affinity()
    if not isinstance(raw, dict):
        raise ValueError(f"'affinity' must be a mapping, got {type(raw).__name__}")
    unknown = set(raw) - {"server", "client", "prepare"}
    if unknown:
        raise ValueError(f"affinity has unknown roles: {sorted(unknown)}")
    server = _parse_affinity_block("server", raw.get("server"))
    client = _parse_affinity_block("client", raw.get("client"))
    prepare = _parse_affinity_block("prepare", raw.get("prepare"))
    if prepare is None:
        prepare = client
    return Affinity(server=server, client=client, prepare=prepare)


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
        benchmark_project=config.get("benchmark_project", default_benchmark_project),
        server_project=config.get("server_project", default_server_project),
        prepare_params=dict(config.get("prepare", {}).get("client_params", {})),
        base_client_params=dict(config["base"]["client_params"]),
        base_server_params=dict(config["base"].get("server_params", {})),
        no_server=config.get("no_server", False),
        repeat=int(config.get("repeat", 1)),
        combos=expand_sweep(config.get("sweep", {})),
        affinity=_parse_affinity(config),
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
