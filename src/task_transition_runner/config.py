"""Public runner configuration contract.

The loader accepts one YAML document, validates the exact nested mapping
schema, and materializes canonical roots plus a full ``refs/heads/...`` ref.
It does not search parent trees, invent host-path defaults, or start adapters.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


_SCHEMA_VERSION = "1.0"
_ADAPTER_KIND = "cursor"
_BRANCH_NAME = re.compile(
    r"^(?!HEAD$)(?!refs/)(?!.*//)(?!.*\.\.)(?!.*(?:^|/)\.)(?!.*\.lock(?:/|$))"
    r"[A-Za-z0-9](?:[A-Za-z0-9._/-]*[A-Za-z0-9])?$"
)
_TOP_LEVEL_REQUIRED = frozenset(
    {
        "schema_version",
        "target",
        "runtime_root",
        "task_source",
        "baseline_validation",
        "adapter",
        "prompts",
    }
)
_TARGET_REQUIRED = frozenset({"repo_root", "main_branch"})
_BASELINE_REQUIRED = frozenset({"command", "timeout_seconds"})
_ADAPTER_REQUIRED = frozenset({"kind", "executable", "timeout_seconds"})
_ADAPTER_OPTIONAL = frozenset({"model"})
_PROMPT_ROLES = ("developer", "orchestrator", "validator")
_PROMPT_REQUIRED = frozenset(_PROMPT_ROLES)


class ConfigError(Exception):
    """Fail-closed violation of the public configuration contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class _DuplicateKeyError(yaml.YAMLError):
    """A mapping node declared the same key more than once."""


class _StrictLoader(yaml.SafeLoader):
    """Safe loader that rejects duplicate mapping keys at every nesting level."""


def _construct_mapping_rejecting_duplicates(
    loader: yaml.Loader,
    node: yaml.nodes.MappingNode,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise _DuplicateKeyError("duplicate mapping key")
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_rejecting_duplicates,
)


@dataclass(frozen=True)
class TargetConfig:
    repo_root: Path
    main_branch: str


@dataclass(frozen=True)
class BaselineValidationConfig:
    command: str
    timeout_seconds: int


@dataclass(frozen=True)
class AdapterConfig:
    kind: str
    executable: str
    model: str | None
    timeout_seconds: int


@dataclass(frozen=True)
class PromptsConfig:
    developer: Path
    orchestrator: Path
    validator: Path
    declared_developer: str
    declared_orchestrator: str
    declared_validator: str


@dataclass(frozen=True)
class RunnerConfig:
    schema_version: str
    config_root: Path
    task_source: Path
    task_source_root: Path
    target: TargetConfig
    target_worktree: Path
    main_ref: str
    runtime_root: Path
    runner_source: Path
    baseline_validation: BaselineValidationConfig
    adapter: AdapterConfig
    prompts: PromptsConfig


def load_config(config_path: Path) -> RunnerConfig:
    path = Path(config_path)
    document_text = _read_utf8_file(path)
    config_root = path.resolve().parent
    document = _parse_mapping_document(document_text)
    _reject_unknown_and_missing(document, _TOP_LEVEL_REQUIRED)

    schema_version = _require_str(document["schema_version"])
    if schema_version != _SCHEMA_VERSION:
        raise ConfigError("CONFIG_VALUE_CONFLICT")

    target_raw = _require_mapping(document["target"])
    _reject_unknown_and_missing(target_raw, _TARGET_REQUIRED)
    repo_root_raw = _require_str(target_raw["repo_root"])
    main_branch = _require_str(target_raw["main_branch"])
    if _BRANCH_NAME.fullmatch(main_branch) is None:
        raise ConfigError("CONFIG_VALUE_CONFLICT")

    runtime_root_raw = _require_str(document["runtime_root"])
    task_source_raw = _require_str(document["task_source"])

    baseline_raw = _require_mapping(document["baseline_validation"])
    _reject_unknown_and_missing(baseline_raw, _BASELINE_REQUIRED)
    baseline_command = _require_str(baseline_raw["command"])
    baseline_timeout = _require_positive_int(baseline_raw["timeout_seconds"])

    adapter_raw = _require_mapping(document["adapter"])
    _reject_unknown_and_missing(
        adapter_raw,
        _ADAPTER_REQUIRED,
        optional=_ADAPTER_OPTIONAL,
    )
    adapter_kind = _require_str(adapter_raw["kind"])
    if adapter_kind != _ADAPTER_KIND:
        raise ConfigError("CONFIG_VALUE_CONFLICT")
    adapter_executable = _validated_executable(adapter_raw["executable"])
    adapter_model = _validated_optional_model(adapter_raw)
    adapter_timeout = _require_positive_int(adapter_raw["timeout_seconds"])

    prompts_raw = _require_mapping(document["prompts"])
    _reject_unknown_and_missing(prompts_raw, _PROMPT_REQUIRED)

    target_worktree = _resolve_declared_path(config_root, repo_root_raw)
    runtime_root = _resolve_declared_path(config_root, runtime_root_raw)
    task_source = _resolve_declared_path(config_root, task_source_raw)
    _read_utf8_file(task_source)

    prompt_paths: dict[str, Path] = {}
    declared_prompts: dict[str, str] = {}
    for role in _PROMPT_ROLES:
        declared = _require_str(prompts_raw[role])
        prompt_path = _resolve_declared_path(config_root, declared)
        if not prompt_path.is_relative_to(config_root):
            raise ConfigError("CONFIG_PATH_ESCAPE")
        _read_utf8_file(prompt_path)
        prompt_paths[role] = prompt_path
        declared_prompts[role] = declared

    runner_source = Path(__file__).resolve().parent
    if target_worktree.is_relative_to(runtime_root) or runtime_root.is_relative_to(
        target_worktree
    ):
        raise ConfigError("CONFIG_ROOT_OVERLAP")
    if target_worktree.is_relative_to(runner_source) or runner_source.is_relative_to(
        target_worktree
    ):
        raise ConfigError("CONFIG_ROOT_OVERLAP")
    if runtime_root.is_relative_to(runner_source) or runner_source.is_relative_to(
        runtime_root
    ):
        raise ConfigError("CONFIG_ROOT_OVERLAP")

    return RunnerConfig(
        schema_version=schema_version,
        config_root=config_root,
        task_source=task_source,
        task_source_root=task_source.parent,
        target=TargetConfig(repo_root=target_worktree, main_branch=main_branch),
        target_worktree=target_worktree,
        main_ref=f"refs/heads/{main_branch}",
        runtime_root=runtime_root,
        runner_source=runner_source,
        baseline_validation=BaselineValidationConfig(
            command=baseline_command,
            timeout_seconds=baseline_timeout,
        ),
        adapter=AdapterConfig(
            kind=adapter_kind,
            executable=adapter_executable,
            model=adapter_model,
            timeout_seconds=adapter_timeout,
        ),
        prompts=PromptsConfig(
            developer=prompt_paths["developer"],
            orchestrator=prompt_paths["orchestrator"],
            validator=prompt_paths["validator"],
            declared_developer=declared_prompts["developer"],
            declared_orchestrator=declared_prompts["orchestrator"],
            declared_validator=declared_prompts["validator"],
        ),
    )


def _read_utf8_file(path: Path) -> str:
    if not path.is_file():
        raise ConfigError("CONFIG_SOURCE_MISSING")
    try:
        return path.read_text(encoding="utf-8")
    except PermissionError as exc:
        raise ConfigError("CONFIG_SOURCE_UNREADABLE") from exc


def _parse_mapping_document(document_text: str) -> dict[Any, Any]:
    try:
        loaded = yaml.load(document_text, Loader=_StrictLoader)
    except _DuplicateKeyError:
        raise ConfigError("CONFIG_VALUE_CONFLICT") from None
    except yaml.YAMLError:
        raise ConfigError("CONFIG_FIELD_TYPE") from None
    if not isinstance(loaded, dict):
        raise ConfigError("CONFIG_FIELD_TYPE")
    return loaded


def _reject_unknown_and_missing(
    mapping: Mapping[Any, Any],
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> None:
    allowed = required | optional
    if set(mapping) - allowed:
        raise ConfigError("CONFIG_FIELD_UNKNOWN")
    if required - set(mapping):
        raise ConfigError("CONFIG_FIELD_MISSING")


def _require_mapping(value: object) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise ConfigError("CONFIG_FIELD_TYPE")
    return value


def _require_str(value: object) -> str:
    if not isinstance(value, str):
        raise ConfigError("CONFIG_FIELD_TYPE")
    return value


def _require_positive_int(value: object) -> int:
    if type(value) is not int:
        raise ConfigError("CONFIG_FIELD_TYPE")
    if value <= 0:
        raise ConfigError("CONFIG_VALUE_CONFLICT")
    return value


def _validated_optional_model(adapter_raw: Mapping[Any, Any]) -> str | None:
    if "model" not in adapter_raw:
        return None
    model_value = adapter_raw["model"]
    if model_value is None or isinstance(model_value, str):
        return model_value
    raise ConfigError("CONFIG_FIELD_TYPE")


def _validated_executable(value: object) -> str:
    executable = _require_str(value)
    if Path(executable).is_absolute():
        return executable
    if not executable or "/" in executable:
        raise ConfigError("CONFIG_VALUE_CONFLICT")
    return executable


def _resolve_declared_path(config_root: Path, raw: str) -> Path:
    if raw == "":
        raise ConfigError("CONFIG_VALUE_CONFLICT")
    declared = Path(raw)
    if ".." in declared.parts:
        raise ConfigError("CONFIG_PATH_TRAVERSAL")
    if declared.is_absolute():
        return declared.resolve()
    return (config_root / declared).resolve()
