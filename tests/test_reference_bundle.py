"""Public reference config, task sources, task specs, and history-free seed contracts.

Asset tests read only ``examples/reference/runner.yaml``,
``examples/reference/tasks.yaml``,
``examples/reference/task_sources/task_b.yaml``,
``examples/reference/task_sources/terminal_rest.yaml``,
``examples/reference/task_specs/task_a.md``,
``examples/reference/task_specs/task_b.md``,
``examples/reference/bundle_manifest.json``,
``prompts/developer.md``,
``prompts/orchestrator.md``,
``prompts/validator.md``, and
``examples/fixture/``.
While a bound asset is absent, its own witness fails on that absence alone.
Schema, relative-path, outcome, baseline separation, synthetic spec,
synthetic seed, source-to-destination, digest, regeneration, root-separation,
and authority checks do not create reference assets.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.config import ConfigError, load_config
from task_transition_runner.task_contract import (
    TaskContractError,
    canonicalize_persisted_task_snapshot,
)

_MISSING_ASSET_SIGNATURE = "EXPECTED_RED_REFERENCE_CONFIG_AND_TASK_A_SOURCE_MISSING"
_MISSING_TASK_B_SOURCE_SIGNATURE = "EXPECTED_RED_TASK_B_SOURCE_MISSING"
_MISSING_TERMINAL_SOURCE_SIGNATURE = "EXPECTED_RED_TERMINAL_SOURCE_MISSING"
_MISSING_TASK_A_SPEC_SIGNATURE = "EXPECTED_RED_TASK_A_SPEC_MISSING"
_MISSING_TASK_B_SPEC_SIGNATURE = "EXPECTED_RED_TASK_B_SPEC_MISSING"
_TASK_B_MODULE_DEPENDENCY_RED = (
    "EXPECTED_RED_REFERENCE_TASK_B_MODULE_DEPENDENCY_REQUIRED"
)
_SPEC_HIDDEN_SOLUTION = "REFERENCE_SPEC_HIDDEN_SOLUTION"
_MISSING_SEED_SIGNATURE = "EXPECTED_RED_HISTORY_FREE_REFERENCE_SEED_MISSING"
_MISSING_BUNDLE_MANIFEST_SIGNATURE = "EXPECTED_RED_BUNDLE_MANIFEST_MISSING"
_REFERENCE_ADAPTER_TIMEOUT_RED = (
    "EXPECTED_RED_REFERENCE_ADAPTER_TIMEOUT_1800_REQUIRED"
)
_BUNDLE_DIGEST_DRIFT = "REFERENCE_BUNDLE_DIGEST_DRIFT"
_ASSEMBLY_SOURCE_DRIFT = "REFERENCE_ASSEMBLY_SOURCE_DRIFT"
_BUNDLE_AUTHORITY_INVALID = "REFERENCE_BUNDLE_AUTHORITY_INVALID"
_ASSEMBLY_ROOT_OVERLAP = "REFERENCE_ASSEMBLY_ROOT_OVERLAP"
_SEED_HISTORY = "REFERENCE_SEED_HISTORY_PRESENT"
_SEED_SOLUTION = "REFERENCE_SEED_HIDDEN_SOLUTION"
_CONFIG_PATH_INVALID = "CONFIG_PATH_INVALID"
_TASK_CONTRACT_INVALID = "TASK_CONTRACT_INVALID"
_REFERENCE_CONFIG_RELATIVE = "examples/reference/runner.yaml"
_INITIAL_TASK_SOURCE_RELATIVE = "examples/reference/tasks.yaml"
_TASK_B_SOURCE_RELATIVE = "examples/reference/task_sources/task_b.yaml"
_TERMINAL_SOURCE_RELATIVE = "examples/reference/task_sources/terminal_rest.yaml"
_TASK_A_SPEC_RELATIVE = "examples/reference/task_specs/task_a.md"
_TASK_B_SPEC_RELATIVE = "examples/reference/task_specs/task_b.md"
_BUNDLE_MANIFEST_RELATIVE = "examples/reference/bundle_manifest.json"
_BUNDLE_MANIFEST_KIND = "REFERENCE_BUNDLE_MANIFEST"
_SEED_RELATIVE = "examples/fixture"
_TASK_19_RUNNER_YAML_SHA256 = (
    "f64132db3a4ea577cee1ec38dec65e3e08adb0e5b7e7bd5237f3a170d21aff5c"
)
_TASK_19_TASKS_YAML_SHA256 = (
    "3f6ba538a175a3e89f0c3c35ad537c6589a8ba0e15c22facbd542d82408f4842"
)
_SCHEMA_VERSION = "1.0"
_TASK_SOURCE_DECLARATION = "tasks.yaml"
_TASK_A_ID = "reference-task-a"
_TASK_A_SPEC_REF = "task_specs/task_a.md"
_TASK_A_ALLOWED_PATH = "src/reference_app/labels.py"
_TASK_A_COMMAND_ID = "task_a_contract"
_TASK_B_ID = "reference-task-b"
_TASK_B_SPEC_REF = "task_specs/task_b.md"
_TASK_B_ALLOWED_PATH = "src/reference_app/summary.py"
_TASK_A_COMMAND = "python -m pytest -q -p no:cacheprovider tests/test_labels.py"
_TASK_B_COMMAND_ID = "task_b_contract"
_TASK_B_COMMAND = "python -m pytest -q -p no:cacheprovider tests/test_summary.py"
_TASK_COMMAND_TIMEOUT = 30
_REFERENCE_ADAPTER_TIMEOUT_SECONDS = 1800
_PROMPT_DECLARATIONS = {
    "developer": "prompts/developer.md",
    "orchestrator": "prompts/orchestrator.md",
    "validator": "prompts/validator.md",
}
_CONFIG_TOP_LEVEL = frozenset(
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
_TASK_TOP_LEVEL = frozenset({"schema_version", "active_task"})
_ACTIVE_TASK_KEYS = frozenset(
    {
        "task_id",
        "spec_ref",
        "allowed_paths",
        "validation",
        "expected_outcome",
        "adapter",
        "metadata",
    }
)
_TASK_CONTRACT_MARKERS = (
    "src/reference_app/labels.py",
    "src/reference_app/summary.py",
    "task_specs/task_a.md",
    "task_specs/task_b.md",
    "TASK_A_CONTRACT",
    "TASK_B_CONTRACT",
)
_TASK_B_MARKERS = (
    "src/reference_app/summary.py",
    "task_specs/task_b.md",
    "TASK_B_CONTRACT",
)
_PRIVATE_MARKERS = (
    "/home/",
    "aethelgard",
    "chronicle",
    "execution_ledger.md",
    "gaia",
    "gubel",
    "hostname",
    "hydra",
    "myfistchat",
    "orp",
    "persona",
    "private_prompt",
    "runenschmied",
    "session_id",
    "settled_predecessor",
    "username",
)
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")


class ReferenceBundleContractError(Exception):
    """Reference-bundle rule that is stricter than the public loader."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _candidate_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _asset_path(relative: str) -> Path:
    return _candidate_root() / relative


def _encode(document: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(document), sort_keys=False).encode("utf-8")


def _minimal_config() -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "target": {
            "repo_root": "target",
            "main_branch": "main",
        },
        "runtime_root": "runtime",
        "task_source": _TASK_SOURCE_DECLARATION,
        "baseline_validation": {
            "command": "true",
            "timeout_seconds": 30,
        },
        "adapter": {
            "kind": "cursor",
            "executable": "cursor-agent",
            "timeout_seconds": _REFERENCE_ADAPTER_TIMEOUT_SECONDS,
        },
        "prompts": dict(_PROMPT_DECLARATIONS),
    }


def _minimal_task() -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "active_task": {
            "task_id": _TASK_A_ID,
            "spec_ref": _TASK_A_SPEC_REF,
            "allowed_paths": [_TASK_A_ALLOWED_PATH],
            "validation": {
                "commands": [
                    {
                        "id": _TASK_A_COMMAND_ID,
                        "command": _TASK_A_COMMAND,
                        "timeout_seconds": 30,
                    }
                ]
            },
            "expected_outcome": {"kind": "GREEN"},
            "adapter": {"kind": "cursor"},
            "metadata": {},
        },
    }


def _task_a_contract_command() -> str:
    command = _minimal_task()["active_task"]["validation"]["commands"][0]["command"]
    if not isinstance(command, str):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    return command


def _minimal_task_b() -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "active_task": {
            "task_id": _TASK_B_ID,
            "spec_ref": _TASK_B_SPEC_REF,
            "allowed_paths": [_TASK_B_ALLOWED_PATH],
            "validation": {
                "commands": [
                    {
                        "id": _TASK_A_COMMAND_ID,
                        "command": _task_a_contract_command(),
                        "timeout_seconds": _TASK_COMMAND_TIMEOUT,
                    },
                    {
                        "id": _TASK_B_COMMAND_ID,
                        "command": _TASK_B_COMMAND,
                        "timeout_seconds": _TASK_COMMAND_TIMEOUT,
                    },
                ]
            },
            "expected_outcome": {"kind": "GREEN"},
            "adapter": {"kind": "cursor"},
            "metadata": {},
        },
    }


def _minimal_terminal_source() -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "active_task": None,
    }


def _closed_relative(value: object) -> bool:
    if not isinstance(value, str) or value == "":
        return False
    declared = Path(value)
    if declared.is_absolute() or declared.as_posix() != value:
        return False
    return all(part not in {".", ".."} for part in declared.parts)


def _require_mapping(value: object, reason_code: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReferenceBundleContractError(reason_code)
    return value


def _require_exact_keys(
    mapping: Mapping[str, Any],
    required: frozenset[str],
    reason_code: str,
) -> None:
    if set(mapping) != required:
        raise ReferenceBundleContractError(reason_code)


def _positive_int(value: object, reason_code: str) -> int:
    if type(value) is not int or value <= 0:
        raise ReferenceBundleContractError(reason_code)
    return value


def _paths_overlap(left: str, right: str) -> bool:
    left_path = Path(left)
    right_path = Path(right)
    return (
        left_path == right_path
        or left_path.is_relative_to(right_path)
        or right_path.is_relative_to(left_path)
    )


def _assert_public_privacy(payload: bytes) -> None:
    text = payload.decode("utf-8").casefold()
    for marker in _PRIVATE_MARKERS:
        if marker.casefold() in text:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def _mapping_from_bytes(payload: bytes, reason_code: str) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReferenceBundleContractError(reason_code) from exc
    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ReferenceBundleContractError(reason_code) from exc
    return _require_mapping(loaded, reason_code)


def _write_relative_file(root: Path, relative: object, payload: bytes) -> None:
    if not isinstance(relative, str) or not _closed_relative(relative):
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    destination = (root / relative).resolve()
    if not destination.is_relative_to(root.resolve()):
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)


def _stage_reference_config(
    tmp_path: Path,
    payload: bytes,
    document: Mapping[str, Any],
) -> Any:
    root = tmp_path / "reference-input"
    root.mkdir()
    _write_relative_file(root, document["task_source"], b"synthetic: true\n")
    prompts = _require_mapping(document["prompts"], _CONFIG_PATH_INVALID)
    for role in _PROMPT_DECLARATIONS:
        _write_relative_file(
            root,
            prompts[role],
            f"synthetic {role}\n".encode("utf-8"),
        )
    config_path = root / "runner.yaml"
    config_path.write_bytes(payload)
    return load_config(config_path)


def _set_dotted(document: dict[str, Any], dotted: str, value: Any) -> None:
    cursor: Any = document
    keys = dotted.split(".")
    for key in keys[:-1]:
        cursor = cursor[key]
    cursor[keys[-1]] = value


def assert_reference_config_geometry(document: Mapping[str, Any]) -> str:
    """Return the baseline command of one minimal relative reference config."""

    _require_exact_keys(document, _CONFIG_TOP_LEVEL, _CONFIG_PATH_INVALID)
    if document["schema_version"] != _SCHEMA_VERSION:
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    target = _require_mapping(document["target"], _CONFIG_PATH_INVALID)
    _require_exact_keys(
        target,
        frozenset({"repo_root", "main_branch"}),
        _CONFIG_PATH_INVALID,
    )
    repo_root = target["repo_root"]
    runtime_root = document["runtime_root"]
    task_source = document["task_source"]
    if target["main_branch"] != "main":
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    if not all(_closed_relative(item) for item in (repo_root, runtime_root)):
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    if _paths_overlap(str(repo_root), str(runtime_root)):
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    if task_source != _TASK_SOURCE_DECLARATION:
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)

    baseline = _require_mapping(document["baseline_validation"], _TASK_CONTRACT_INVALID)
    _require_exact_keys(
        baseline,
        frozenset({"command", "timeout_seconds"}),
        _TASK_CONTRACT_INVALID,
    )
    command = baseline["command"]
    if not isinstance(command, str) or command.strip() == "":
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if any(marker in command for marker in _TASK_CONTRACT_MARKERS):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    _positive_int(baseline["timeout_seconds"], _TASK_CONTRACT_INVALID)

    adapter = _require_mapping(document["adapter"], _CONFIG_PATH_INVALID)
    allowed_adapter = frozenset({"kind", "executable", "timeout_seconds", "model"})
    required_adapter = frozenset({"kind", "executable", "timeout_seconds"})
    if not required_adapter <= set(adapter) <= allowed_adapter:
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    if adapter["kind"] != "cursor":
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    executable = adapter["executable"]
    if not isinstance(executable, str) or executable == "" or "/" in executable:
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    _positive_int(adapter["timeout_seconds"], _CONFIG_PATH_INVALID)
    if "model" in adapter and not isinstance(adapter["model"], str | None):
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)

    prompts = _require_mapping(document["prompts"], _CONFIG_PATH_INVALID)
    if set(prompts) != set(_PROMPT_DECLARATIONS):
        raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    for role, declared in _PROMPT_DECLARATIONS.items():
        if prompts[role] != declared:
            raise ReferenceBundleContractError(_CONFIG_PATH_INVALID)
    return command


def assert_initial_task_source_geometry(document: Mapping[str, Any]) -> str:
    """Return the single Task A contract command."""

    _require_exact_keys(document, _TASK_TOP_LEVEL, _TASK_CONTRACT_INVALID)
    if document["schema_version"] != _SCHEMA_VERSION:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    active = document["active_task"]
    if not isinstance(active, dict):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    _require_exact_keys(active, _ACTIVE_TASK_KEYS, _TASK_CONTRACT_INVALID)
    if active["task_id"] != _TASK_A_ID or active["spec_ref"] != _TASK_A_SPEC_REF:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["allowed_paths"] != [_TASK_A_ALLOWED_PATH]:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["expected_outcome"] != {"kind": "GREEN"}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["adapter"] != {"kind": "cursor"}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["metadata"] != {}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)

    validation = _require_mapping(active["validation"], _TASK_CONTRACT_INVALID)
    _require_exact_keys(validation, frozenset({"commands"}), _TASK_CONTRACT_INVALID)
    commands = validation["commands"]
    if not isinstance(commands, list) or len(commands) != 1:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    command_raw = _require_mapping(commands[0], _TASK_CONTRACT_INVALID)
    _require_exact_keys(
        command_raw,
        frozenset({"id", "command", "timeout_seconds"}),
        _TASK_CONTRACT_INVALID,
    )
    if command_raw["id"] != _TASK_A_COMMAND_ID:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    command = command_raw["command"]
    if not isinstance(command, str) or command.strip() == "":
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if any(marker in command for marker in _TASK_B_MARKERS):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    _positive_int(command_raw["timeout_seconds"], _TASK_CONTRACT_INVALID)
    return command


def assert_task_b_source_path_boundary(relative: str) -> None:
    """Require the single public Task B source path."""

    if relative != _TASK_B_SOURCE_RELATIVE or not _closed_relative(relative):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative == _INITIAL_TASK_SOURCE_RELATIVE:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative.startswith(f"{_SEED_RELATIVE}/"):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def assert_task_b_source_geometry(document: Mapping[str, Any]) -> tuple[str, str]:
    """Return the Task A command and the Task B command, in that order."""

    _require_exact_keys(document, _TASK_TOP_LEVEL, _TASK_CONTRACT_INVALID)
    if document["schema_version"] != _SCHEMA_VERSION:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    active = document["active_task"]
    if not isinstance(active, dict):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    _require_exact_keys(active, _ACTIVE_TASK_KEYS, _TASK_CONTRACT_INVALID)
    if active["task_id"] != _TASK_B_ID or active["spec_ref"] != _TASK_B_SPEC_REF:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if not _closed_relative(active["spec_ref"]):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["allowed_paths"] != [_TASK_B_ALLOWED_PATH]:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if not _closed_relative(_TASK_B_ALLOWED_PATH):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["expected_outcome"] != {"kind": "GREEN"}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["adapter"] != {"kind": "cursor"}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if active["metadata"] != {}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)

    validation = _require_mapping(active["validation"], _TASK_CONTRACT_INVALID)
    _require_exact_keys(validation, frozenset({"commands"}), _TASK_CONTRACT_INVALID)
    commands = validation["commands"]
    if not isinstance(commands, list) or len(commands) != 2:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    expected = (
        (_TASK_A_COMMAND_ID, _task_a_contract_command()),
        (_TASK_B_COMMAND_ID, _TASK_B_COMMAND),
    )
    resolved: list[str] = []
    for entry, (expected_id, expected_command) in zip(commands, expected, strict=True):
        command_raw = _require_mapping(entry, _TASK_CONTRACT_INVALID)
        _require_exact_keys(
            command_raw,
            frozenset({"id", "command", "timeout_seconds"}),
            _TASK_CONTRACT_INVALID,
        )
        if command_raw["id"] != expected_id or command_raw["command"] != expected_command:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
        if command_raw["timeout_seconds"] != _TASK_COMMAND_TIMEOUT:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
        resolved.append(expected_command)
    if resolved[0] == resolved[1]:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    return (resolved[0], resolved[1])


def assert_terminal_source_path_boundary(relative: str) -> None:
    """Require the single public terminal-rest source path."""

    if relative != _TERMINAL_SOURCE_RELATIVE or not _closed_relative(relative):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative in {_INITIAL_TASK_SOURCE_RELATIVE, _TASK_B_SOURCE_RELATIVE}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative.startswith(f"{_SEED_RELATIVE}/"):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def assert_terminal_source_geometry(document: Mapping[str, Any]) -> None:
    """Require schema_version and active_task null, without task authority."""

    _require_exact_keys(document, _TASK_TOP_LEVEL, _TASK_CONTRACT_INVALID)
    if document["schema_version"] != _SCHEMA_VERSION:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if document["active_task"] is not None:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def assert_task_b_source_real_task_a_import(payload: bytes) -> None:
    """Require Task B to call the real Task A public behavior."""

    tree = _parsed_module(payload)
    imported = _module_imports(tree)
    if imported != {"reference_app.labels", "reference_app.summary"}:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if not _has_test_function(tree) or not _calls_public_summary_on_public_labels(tree):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def _attribute_call(node: ast.AST, owner: str, name: str) -> ast.Call | None:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    value = node.func.value
    if (
        node.func.attr == name
        and isinstance(value, ast.Attribute)
        and isinstance(value.value, ast.Name)
        and value.value.id == "reference_app"
        and value.attr == owner
    ):
        return node
    return None


def _calls_public_summary_on_public_labels(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        summary_call = _attribute_call(node, "summary", "public_summary")
        if summary_call is None:
            continue
        for arg in summary_call.args:
            if _attribute_call(arg, "labels", "public_labels") is not None:
                return True
    return False


def assert_baseline_separated(baseline_command: str, task_command: str) -> None:
    if baseline_command.strip() == "" or task_command.strip() == "":
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if baseline_command.strip() == task_command.strip():
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if any(marker in baseline_command for marker in _TASK_CONTRACT_MARKERS):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if any(marker in task_command for marker in _TASK_B_MARKERS):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def _fail_missing(relative_paths: tuple[str, ...]) -> None:
    missing = [
        relative
        for relative in relative_paths
        if not _asset_path(relative).is_file()
    ]
    if missing:
        pytest.fail(
            f"{_MISSING_ASSET_SIGNATURE}: missing {', '.join(missing)}"
        )


def _write_config_variant(tmp_path: Path, dotted: str, value: Any) -> Path:
    document = _minimal_config()
    _set_dotted(document, dotted, value)
    root = tmp_path / "parser-input"
    root.mkdir()
    if dotted != "task_source":
        (root / "tasks.yaml").write_text("synthetic: true\n", encoding="utf-8")
    if not dotted.startswith("prompts."):
        prompt_root = root / "prompts"
        prompt_root.mkdir()
        for role in _PROMPT_DECLARATIONS:
            (prompt_root / f"{role}.md").write_text(
                f"synthetic {role}\n",
                encoding="utf-8",
            )
    config_path = root / "runner.yaml"
    config_path.write_bytes(_encode(document))
    return config_path


def test_reference_config_candidate_bytes_bind_minimal_public_schema(
    tmp_path: Path,
) -> None:
    _fail_missing((_REFERENCE_CONFIG_RELATIVE,))
    path = _asset_path(_REFERENCE_CONFIG_RELATIVE)
    payload = path.read_bytes()
    _assert_public_privacy(payload)
    document = _mapping_from_bytes(payload, _CONFIG_PATH_INVALID)
    assert_reference_config_geometry(document)
    if document["adapter"]["timeout_seconds"] != _REFERENCE_ADAPTER_TIMEOUT_SECONDS:
        pytest.fail(_REFERENCE_ADAPTER_TIMEOUT_RED)
    resolved_task_source = (path.parent / _TASK_SOURCE_DECLARATION).resolve()
    assert resolved_task_source == _asset_path(_INITIAL_TASK_SOURCE_RELATIVE).resolve()
    assert resolved_task_source.is_relative_to(_candidate_root())
    loaded = _stage_reference_config(tmp_path, payload, document)
    assert loaded.schema_version == _SCHEMA_VERSION
    assert loaded.prompts.declared_developer == _PROMPT_DECLARATIONS["developer"]
    assert loaded.prompts.declared_orchestrator == _PROMPT_DECLARATIONS["orchestrator"]
    assert loaded.prompts.declared_validator == _PROMPT_DECLARATIONS["validator"]
    assert loaded.target.main_branch == "main"
    assert loaded.main_ref == "refs/heads/main"


def test_initial_task_source_candidate_bytes_bind_task_a_green_geometry() -> None:
    _fail_missing((_INITIAL_TASK_SOURCE_RELATIVE,))
    payload = _asset_path(_INITIAL_TASK_SOURCE_RELATIVE).read_bytes()
    _assert_public_privacy(payload)
    identity = canonicalize_persisted_task_snapshot(payload)
    document = _mapping_from_bytes(payload, _TASK_CONTRACT_INVALID)
    command = assert_initial_task_source_geometry(document)
    assert identity.task_id == _TASK_A_ID
    assert _FINGERPRINT.fullmatch(identity.fingerprint)
    assert _FINGERPRINT.fullmatch(identity.snapshot_sha256)
    assert command == document["active_task"]["validation"]["commands"][0]["command"]
    assert document["active_task"]["expected_outcome"] == {"kind": "GREEN"}


def test_reference_config_baseline_stays_apart_from_initial_task_source_contract() -> None:
    _fail_missing((_REFERENCE_CONFIG_RELATIVE, _INITIAL_TASK_SOURCE_RELATIVE))
    config = _mapping_from_bytes(
        _asset_path(_REFERENCE_CONFIG_RELATIVE).read_bytes(),
        _CONFIG_PATH_INVALID,
    )
    task = _mapping_from_bytes(
        _asset_path(_INITIAL_TASK_SOURCE_RELATIVE).read_bytes(),
        _TASK_CONTRACT_INVALID,
    )
    assert_baseline_separated(
        assert_reference_config_geometry(config),
        assert_initial_task_source_geometry(task),
    )


def test_reference_config_minimal_relative_geometry_is_parser_accepted(
    tmp_path: Path,
) -> None:
    document = _minimal_config()
    payload = _encode(document)
    baseline = assert_reference_config_geometry(document)
    loaded = _stage_reference_config(tmp_path, payload, document)

    assert baseline == "true"
    assert loaded.baseline_validation.command == baseline
    assert loaded.task_source == (tmp_path / "reference-input" / "tasks.yaml").resolve()
    assert loaded.prompts.developer == (
        tmp_path / "reference-input" / "prompts" / "developer.md"
    ).resolve()
    assert not loaded.runtime_root.is_relative_to(loaded.target_worktree)
    assert not loaded.target_worktree.is_relative_to(loaded.runtime_root)


def test_initial_task_source_task_a_green_geometry_is_parser_accepted() -> None:
    document = _minimal_task()
    payload = _encode(document)
    command = assert_initial_task_source_geometry(document)
    identity = canonicalize_persisted_task_snapshot(payload)

    assert identity.task_id == _TASK_A_ID
    assert command == _TASK_A_COMMAND
    assert _FINGERPRINT.fullmatch(identity.fingerprint)
    assert_baseline_separated("true", command)


def test_reference_config_schema_negative_missing_task_source_is_rejected(
    tmp_path: Path,
) -> None:
    document = _minimal_config()
    del document["task_source"]
    config_path = tmp_path / "runner.yaml"
    config_path.write_bytes(_encode(document))

    with pytest.raises(ConfigError, match="CONFIG_FIELD_MISSING") as exc_info:
        load_config(config_path)

    assert exc_info.value.reason_code == "CONFIG_FIELD_MISSING"


def test_reference_config_schema_negative_unknown_field_is_rejected(
    tmp_path: Path,
) -> None:
    document = _minimal_config()
    document["notes"] = "synthetic"
    config_path = tmp_path / "runner.yaml"
    config_path.write_bytes(_encode(document))

    with pytest.raises(ConfigError, match="CONFIG_FIELD_UNKNOWN") as exc_info:
        load_config(config_path)

    assert exc_info.value.reason_code == "CONFIG_FIELD_UNKNOWN"


def test_initial_task_source_schema_negative_missing_allowed_paths_is_rejected() -> None:
    document = _minimal_task()
    del document["active_task"]["allowed_paths"]

    with pytest.raises(TaskContractError, match="TASK_FIELD_MISSING") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_MISSING"


def test_initial_task_source_schema_negative_unknown_field_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["notes"] = "synthetic"

    with pytest.raises(TaskContractError, match="TASK_FIELD_UNKNOWN") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_UNKNOWN"


@pytest.mark.parametrize(
    ("dotted", "value"),
    (
        ("task_source", "../tasks.yaml"),
        ("runtime_root", "../runtime"),
        ("target.repo_root", "../target"),
        ("prompts.developer", "../developer.md"),
    ),
)
def test_reference_config_relative_path_negative_parent_traversal_is_rejected(
    tmp_path: Path,
    dotted: str,
    value: str,
) -> None:
    config_path = _write_config_variant(tmp_path, dotted, value)

    with pytest.raises(ConfigError, match="CONFIG_PATH_TRAVERSAL") as exc_info:
        load_config(config_path)

    assert exc_info.value.reason_code == "CONFIG_PATH_TRAVERSAL"


@pytest.mark.parametrize(
    ("dotted", "value"),
    (
        ("task_source", "/synthetic/tasks.yaml"),
        ("runtime_root", "/synthetic/runtime"),
        ("target.repo_root", "/synthetic/target"),
        ("prompts.developer", "/synthetic/developer.md"),
    ),
)
def test_reference_config_relative_path_negative_absolute_declaration_is_rejected(
    dotted: str,
    value: str,
) -> None:
    document = _minimal_config()
    _set_dotted(document, dotted, value)

    with pytest.raises(ReferenceBundleContractError, match=_CONFIG_PATH_INVALID) as exc_info:
        assert_reference_config_geometry(document)

    assert exc_info.value.reason_code == _CONFIG_PATH_INVALID


def test_initial_task_source_relative_path_negative_absolute_allowed_path_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["allowed_paths"] = ["/synthetic/labels.py"]

    with pytest.raises(TaskContractError, match="TASK_VALUE_CONFLICT") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_VALUE_CONFLICT"


def test_initial_task_source_relative_path_negative_parent_spec_ref_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["spec_ref"] = "../task_a.md"

    with pytest.raises(TaskContractError, match="TASK_PATH_TRAVERSAL") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_PATH_TRAVERSAL"


def test_initial_task_source_outcome_negative_expected_red_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["expected_outcome"] = {
        "kind": "EXPECTED_RED",
        "command_id": _TASK_A_COMMAND_ID,
        "failure_signature": "synthetic failure",
    }
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_A_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_initial_task_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_initial_task_source_outcome_negative_settled_kind_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["expected_outcome"] = {"kind": "SETTLED"}

    with pytest.raises(TaskContractError, match="TASK_VALUE_CONFLICT") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_VALUE_CONFLICT"


def test_initial_task_source_outcome_negative_settled_field_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["expected_outcome"]["settled"] = True

    with pytest.raises(TaskContractError, match="TASK_FIELD_UNKNOWN") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_UNKNOWN"


def test_initial_task_source_outcome_negative_null_task_is_rejected() -> None:
    document = {"schema_version": _SCHEMA_VERSION, "active_task": None}
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id is None
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_initial_task_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_initial_task_source_outcome_negative_extra_task_b_path_is_rejected() -> None:
    document = _minimal_task()
    document["active_task"]["allowed_paths"] = [
        _TASK_A_ALLOWED_PATH,
        "src/reference_app/summary.py",
    ]
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_A_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_initial_task_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_reference_config_baseline_negative_task_contract_is_rejected(
    tmp_path: Path,
) -> None:
    document = _minimal_config()
    document["baseline_validation"]["command"] = (
        "python -m pytest src/reference_app/labels.py"
    )
    loaded = _stage_reference_config(tmp_path, _encode(document), document)

    assert "src/reference_app/labels.py" in loaded.baseline_validation.command
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_reference_config_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_initial_task_source_baseline_negative_same_command_is_rejected() -> None:
    shared = _TASK_A_COMMAND
    config = _minimal_config()
    task = _minimal_task()
    config["baseline_validation"]["command"] = shared
    task["active_task"]["validation"]["commands"][0]["command"] = shared

    assert_reference_config_geometry(config)
    assert_initial_task_source_geometry(task)
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_baseline_separated(shared, shared)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


_SEED_FILES = (
    "pyproject.toml",
    "src/reference_app/__init__.py",
    "tests/test_environment.py",
    "tests/test_labels.py",
    "tests/test_summary.py",
)
_SEED_DIRECTORIES = frozenset({"src", "src/reference_app", "tests"})
_SEED_BASELINE_RELATIVE = "tests/test_environment.py"
_SEED_BASELINE_COMMAND = "python -m pytest tests/test_environment.py"
_SEED_TASK_A_RELATIVE = "tests/test_labels.py"
_SEED_TASK_B_RELATIVE = "tests/test_summary.py"
_PROTOCOL_MARKERS = ("Runner-", "TICK", "TOCK", "Promotion-")
_RESULT_MARKERS = ("Result commit", "RESULT_COMMIT", "result_commit")
_SOLUTION_BYTE_MARKERS = (
    "diff --git",
    "+++ b/src/reference_app/labels.py",
    "+++ b/src/reference_app/summary.py",
    "HIDDEN_SOLUTION",
)
_RUNTIME_FILENAMES = frozenset(
    {
        "main_state.yaml",
        "promotion_journal.yaml",
        "drain_outcome.yaml",
        "promotion_consumed.yaml",
        "pending_promotion.yaml",
        "settled_predecessor.yaml",
        "commit_editmsg",
    }
)
_BASELINE_FORBIDDEN_MARKERS = (
    "reference_app.labels",
    "reference_app.summary",
    "labels.py",
    "summary.py",
    "TASK_A_CONTRACT",
    "TASK_B_CONTRACT",
    "task_a_contract",
    "task_b_contract",
    "task_specs/",
    "tasks.yaml",
)
_SEED_PYPROJECT = {
    "project": {
        "name": "reference-app",
        "version": "0.1.0",
        "requires-python": ">=3.12",
    },
    "build-system": {
        "requires": ["setuptools>=69"],
        "build-backend": "setuptools.build_meta",
    },
    "tool": {
        "setuptools": {
            "packages": {
                "find": {
                    "where": ["src"],
                },
            },
        },
        "pytest": {
            "ini_options": {
                "pythonpath": ["src"],
            },
        },
    },
}
_SEED_PYPROJECT_TEXT = """\
[project]
name = "reference-app"
version = "0.1.0"
requires-python = ">=3.12"

[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
pythonpath = ["src"]
"""
_SEED_ENVIRONMENT_TEXT = """\
import sys


def test_baseline_environment() -> None:
    assert sys.version_info >= (3, 12)
"""
_SEED_TASK_A_TEXT = """\
import reference_app.labels


def test_task_a_contract() -> None:
    reference_app.labels.public_labels()
"""
_SEED_TASK_B_TEXT = """\
import reference_app.labels
import reference_app.summary


def test_task_b_contract() -> None:
    reference_app.summary.public_summary(reference_app.labels.public_labels())
"""


def _minimal_seed_payloads() -> dict[str, bytes]:
    return {
        "pyproject.toml": _SEED_PYPROJECT_TEXT.encode("utf-8"),
        "src/reference_app/__init__.py": b"",
        "tests/test_environment.py": _SEED_ENVIRONMENT_TEXT.encode("utf-8"),
        "tests/test_labels.py": _SEED_TASK_A_TEXT.encode("utf-8"),
        "tests/test_summary.py": _SEED_TASK_B_TEXT.encode("utf-8"),
    }


def _write_minimal_seed(root: Path) -> Path:
    for relative, payload in _minimal_seed_payloads().items():
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    return root


def _module_imports(tree: ast.AST) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def _parsed_module(payload: bytes) -> ast.AST:
    try:
        return ast.parse(payload.decode("utf-8"))
    except (UnicodeDecodeError, SyntaxError) as exc:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID) from exc


def _has_test_function(tree: ast.AST) -> bool:
    return any(
        isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
        for node in getattr(tree, "body", ())
    )


def _scan_history_free_seed(root: Path) -> dict[str, bytes]:
    if not root.is_dir():
        raise ReferenceBundleContractError(_SEED_SOLUTION)
    found: dict[str, bytes] = {}
    for path in sorted(root.rglob("*")):
        relative_path = path.relative_to(root)
        relative = relative_path.as_posix()
        parts = relative_path.parts
        if any(part.casefold() == ".git" for part in parts):
            raise ReferenceBundleContractError(_SEED_HISTORY)
        if any(part.casefold() in {"runtime", "runs"} for part in parts):
            raise ReferenceBundleContractError(_SEED_HISTORY)
        if path.is_symlink() or path.name.casefold() in _RUNTIME_FILENAMES:
            reason = (
                _SEED_HISTORY
                if path.name.casefold() in _RUNTIME_FILENAMES
                else _SEED_SOLUTION
            )
            raise ReferenceBundleContractError(reason)
        if path.is_dir():
            if relative not in _SEED_DIRECTORIES:
                raise ReferenceBundleContractError(_SEED_SOLUTION)
            continue
        if not path.is_file():
            raise ReferenceBundleContractError(_SEED_SOLUTION)
        filename = path.name.casefold()
        if (
            filename in {"labels.py", "summary.py"}
            or filename.endswith((".patch", ".diff"))
            or "solution" in filename
        ):
            raise ReferenceBundleContractError(_SEED_SOLUTION)
        payload = path.read_bytes()
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ReferenceBundleContractError(_SEED_SOLUTION) from exc
        _assert_public_privacy(payload)
        if any(marker in text for marker in (*_PROTOCOL_MARKERS, *_RESULT_MARKERS)):
            raise ReferenceBundleContractError(_SEED_HISTORY)
        if any(marker in text for marker in _SOLUTION_BYTE_MARKERS):
            raise ReferenceBundleContractError(_SEED_SOLUTION)
        found[relative] = payload
    if set(found) != set(_SEED_FILES):
        raise ReferenceBundleContractError(_SEED_SOLUTION)
    return found


def _assert_package_init(payload: bytes) -> None:
    if payload.strip() != b"":
        raise ReferenceBundleContractError(_SEED_SOLUTION)


def _assert_pyproject(payload: bytes) -> None:
    try:
        document = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ReferenceBundleContractError(_SEED_SOLUTION) from exc
    if document != _SEED_PYPROJECT:
        raise ReferenceBundleContractError(_SEED_SOLUTION)


def _assert_environment_test(payload: bytes) -> None:
    text = payload.decode("utf-8")
    if any(marker in text for marker in _BASELINE_FORBIDDEN_MARKERS):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    tree = _parsed_module(payload)
    imported = _module_imports(tree)
    if any(name == "reference_app" or name.startswith("reference_app.") for name in imported):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if not _has_test_function(tree):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def _assert_contract_test(
    payload: bytes,
    required: set[str],
    forbidden: set[str],
) -> None:
    tree = _parsed_module(payload)
    imported = _module_imports(tree)
    if not required <= imported or imported & forbidden or not _has_test_function(tree):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def _run_pytest(root: Path, relative: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTEST_ADDOPTS"] = ""
    env["PYTHONPATH"] = str(root / "src")
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--noconftest",
            "-p",
            "no:cacheprovider",
            f"--rootdir={root}",
            relative,
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "-c", "core.hooksPath=/dev/null", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise AssertionError(detail)
    return completed.stdout.strip()


def assert_history_free_seed_geometry(root: Path) -> str:
    """Return the baseline command of one history-free reference seed."""

    payloads = _scan_history_free_seed(root)
    _assert_package_init(payloads["src/reference_app/__init__.py"])
    _assert_pyproject(payloads["pyproject.toml"])
    _assert_environment_test(payloads["tests/test_environment.py"])
    _assert_contract_test(
        payloads["tests/test_labels.py"],
        required={"reference_app.labels"},
        forbidden={"reference_app.summary"},
    )
    _assert_contract_test(
        payloads["tests/test_summary.py"],
        required={"reference_app.labels", "reference_app.summary"},
        forbidden=set(),
    )
    task_command = _minimal_task()["active_task"]["validation"]["commands"][0]["command"]
    assert_baseline_separated(_SEED_BASELINE_COMMAND, task_command)
    baseline = _run_pytest(root, _SEED_BASELINE_RELATIVE)
    if baseline.returncode != 0:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if set(_scan_history_free_seed(root)) != set(_SEED_FILES):
        raise ReferenceBundleContractError(_SEED_SOLUTION)
    if (root / "src/reference_app/labels.py").exists():
        raise ReferenceBundleContractError(_SEED_SOLUTION)
    if (root / "src/reference_app/summary.py").exists():
        raise ReferenceBundleContractError(_SEED_SOLUTION)
    if (root / ".git").exists():
        raise ReferenceBundleContractError(_SEED_HISTORY)
    return _SEED_BASELINE_COMMAND


def _assert_committed_reference_assets() -> None:
    pairs = (
        (_REFERENCE_CONFIG_RELATIVE, _TASK_19_RUNNER_YAML_SHA256),
        (_INITIAL_TASK_SOURCE_RELATIVE, _TASK_19_TASKS_YAML_SHA256),
    )
    for relative, digest in pairs:
        payload = _asset_path(relative).read_bytes()
        _assert_public_privacy(payload)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise AssertionError(relative)


def test_history_free_seed_candidate_bytes_bind_neutral_baseline() -> None:
    _assert_committed_reference_assets()
    root = _asset_path(_SEED_RELATIVE)
    missing = [
        f"{_SEED_RELATIVE}/{relative}"
        for relative in _SEED_FILES
        if not (root / relative).is_file()
    ]
    if missing:
        pytest.fail(f"{_MISSING_SEED_SIGNATURE}: missing {', '.join(missing)}")
    assert assert_history_free_seed_geometry(root) == _SEED_BASELINE_COMMAND
    task_a = _run_pytest(root, _SEED_TASK_A_RELATIVE)
    task_b = _run_pytest(root, _SEED_TASK_B_RELATIVE)
    assert task_a.returncode != 0
    assert "reference_app.labels" in task_a.stdout + task_a.stderr
    assert task_b.returncode != 0
    assert "reference_app" in task_b.stdout + task_b.stderr


def test_history_free_seed_minimal_geometry_separates_baseline_from_task_contracts(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    baseline = assert_history_free_seed_geometry(root)
    task_command = _minimal_task()["active_task"]["validation"]["commands"][0]["command"]

    assert baseline == _SEED_BASELINE_COMMAND
    assert baseline != task_command
    assert not (root / ".git").exists()
    assert not (root / "src/reference_app/labels.py").exists()
    assert not (root / "src/reference_app/summary.py").exists()
    assert not (root / "tasks.yaml").exists()


def test_history_free_seed_fresh_repository_has_one_neutral_baseline_commit(
    tmp_path: Path,
) -> None:
    seed = _write_minimal_seed(tmp_path / "seed")
    assert_history_free_seed_geometry(seed)
    target = tmp_path / "target"
    for relative in _SEED_FILES:
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((seed / relative).read_bytes())

    _git(target, "init", "-b", "main")
    for relative in _SEED_FILES:
        _git(target, "add", "--", relative)
    _git(
        target,
        "-c",
        "user.name=Baseline",
        "-c",
        "user.email=baseline@example.com",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-m",
        "neutral baseline",
    )

    assert _git(target, "rev-list", "--count", "HEAD") == "1"
    assert _git(target, "log", "-1", "--format=%s") == "neutral baseline"
    body = _git(target, "log", "--format=%B")
    for marker in (*_PROTOCOL_MARKERS, *_RESULT_MARKERS):
        assert marker not in body
    assert _git(target, "ls-files").splitlines() == list(_SEED_FILES)
    assert not (seed / ".git").exists()
    for relative in _SEED_FILES:
        assert (target / relative).read_bytes() == (seed / relative).read_bytes()


def test_history_free_seed_task_contracts_remain_unexecuted_without_implementations(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    assert assert_history_free_seed_geometry(root) == _SEED_BASELINE_COMMAND

    task_a = _run_pytest(root, _SEED_TASK_A_RELATIVE)
    task_b = _run_pytest(root, _SEED_TASK_B_RELATIVE)
    task_a_output = task_a.stdout + task_a.stderr
    task_b_output = task_b.stdout + task_b.stderr

    assert task_a.returncode != 0
    assert "reference_app.labels" in task_a_output
    assert task_b.returncode != 0
    assert "reference_app" in task_b_output
    assert not (root / "src/reference_app/labels.py").exists()
    assert not (root / "src/reference_app/summary.py").exists()


@pytest.mark.parametrize(
    ("relative", "payload", "reason_code"),
    (
        (".git/HEAD", b"ref: refs/heads/main\n", _SEED_HISTORY),
        ("runtime/main_state.yaml", b"state: present\n", _SEED_HISTORY),
        ("promotion_journal.yaml", b"kind: FINALIZED\n", _SEED_HISTORY),
        ("protocol.txt", b"TOCK\n", _SEED_HISTORY),
        ("RESULT", b"Result commit\n", _SEED_HISTORY),
        ("src/reference_app/labels.py", b"VALUE = 1\n", _SEED_SOLUTION),
        ("src/reference_app/summary.py", b"VALUE = 1\n", _SEED_SOLUTION),
        ("hidden_solution.patch", b"patch\n", _SEED_SOLUTION),
        ("tasks.yaml", b"schema_version: '1.0'\n", _SEED_SOLUTION),
        ("task_specs/task_a.md", b"task a\n", _SEED_SOLUTION),
        ("prompts/developer.md", b"prompt\n", _SEED_SOLUTION),
    ),
)
def test_history_free_seed_negative_forbidden_payload_is_rejected(
    tmp_path: Path,
    relative: str,
    payload: bytes,
    reason_code: str,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)

    with pytest.raises(ReferenceBundleContractError, match=reason_code) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == reason_code


def test_history_free_seed_negative_protocol_trailer_in_package_is_rejected(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    (root / "src/reference_app/__init__.py").write_text("TOCK\n", encoding="utf-8")

    with pytest.raises(ReferenceBundleContractError, match=_SEED_HISTORY) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _SEED_HISTORY


def test_history_free_seed_negative_result_commit_text_is_rejected(tmp_path: Path) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    path = root / "tests/test_environment.py"
    path.write_text(
        path.read_text(encoding="utf-8") + "\n# Result commit\n",
        encoding="utf-8",
    )

    with pytest.raises(ReferenceBundleContractError, match=_SEED_HISTORY) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _SEED_HISTORY


def test_history_free_seed_negative_package_implementation_is_rejected(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    (root / "src/reference_app/__init__.py").write_text(
        "def public_labels() -> tuple[object, ...]:\n    return ()\n",
        encoding="utf-8",
    )

    with pytest.raises(ReferenceBundleContractError, match=_SEED_SOLUTION) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _SEED_SOLUTION


def test_history_free_seed_negative_package_dependency_is_rejected(tmp_path: Path) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    path = root / "pyproject.toml"
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'version = "0.1.0"\n',
            'version = "0.1.0"\ndependencies = ["pyyaml"]\n',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReferenceBundleContractError, match=_SEED_SOLUTION) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _SEED_SOLUTION


def test_history_free_seed_negative_baseline_task_contract_is_rejected(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    path = root / "tests/test_environment.py"
    path.write_text(
        path.read_text(encoding="utf-8") + "\nTASK_A_CONTRACT = 'labels.py'\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_history_free_seed_negative_task_contract_without_product_import_is_rejected(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    (root / "tests/test_labels.py").write_text(
        "def test_task_a_contract() -> None:\n    assert True\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_history_free_seed_negative_task_b_without_task_a_import_is_rejected(
    tmp_path: Path,
) -> None:
    root = _write_minimal_seed(tmp_path / "seed")
    (root / "tests/test_summary.py").write_text(
        "import reference_app.summary\n\n"
        "def test_task_b_contract() -> None:\n"
        "    reference_app.summary.public_summary()\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_history_free_seed_geometry(root)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def _seed_task_b_contract_bytes() -> bytes:
    return _asset_path(f"{_SEED_RELATIVE}/{_SEED_TASK_B_RELATIVE}").read_bytes()


def test_task_b_source_candidate_bytes_bind_green_geometry() -> None:
    path = _asset_path(_TASK_B_SOURCE_RELATIVE)
    if not path.is_file():
        pytest.fail(
            f"{_MISSING_TASK_B_SOURCE_SIGNATURE}: missing {_TASK_B_SOURCE_RELATIVE}"
        )
    payload = path.read_bytes()
    _assert_public_privacy(payload)
    identity = canonicalize_persisted_task_snapshot(payload)
    document = _mapping_from_bytes(payload, _TASK_CONTRACT_INVALID)
    task_a_command, task_b_command = assert_task_b_source_geometry(document)
    assert_task_b_source_path_boundary(_TASK_B_SOURCE_RELATIVE)
    assert_task_b_source_real_task_a_import(_seed_task_b_contract_bytes())

    assert identity.task_id == _TASK_B_ID
    assert _FINGERPRINT.fullmatch(identity.fingerprint)
    assert _FINGERPRINT.fullmatch(identity.snapshot_sha256)
    assert task_a_command == _task_a_contract_command()
    assert task_b_command == _TASK_B_COMMAND
    assert document["active_task"]["expected_outcome"] == {"kind": "GREEN"}
    assert document["active_task"]["allowed_paths"] == [_TASK_B_ALLOWED_PATH]


def test_task_b_source_green_geometry_is_parser_accepted() -> None:
    document = _minimal_task_b()
    payload = _encode(document)
    _assert_public_privacy(payload)
    task_a_command, task_b_command = assert_task_b_source_geometry(document)
    identity = canonicalize_persisted_task_snapshot(payload)
    assert_task_b_source_path_boundary(_TASK_B_SOURCE_RELATIVE)

    assert identity.task_id == _TASK_B_ID
    assert task_a_command == _TASK_A_COMMAND
    assert task_b_command == _TASK_B_COMMAND
    assert _FINGERPRINT.fullmatch(identity.fingerprint)
    assert document["active_task"]["spec_ref"] == _TASK_B_SPEC_REF
    assert document["active_task"]["expected_outcome"] == {"kind": "GREEN"}
    assert document["active_task"]["validation"]["commands"][0]["id"] == _TASK_A_COMMAND_ID
    assert document["active_task"]["validation"]["commands"][1]["id"] == _TASK_B_COMMAND_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_initial_task_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_binds_real_task_a_import_and_combined_validation() -> None:
    document = _minimal_task_b()
    task_a_command, task_b_command = assert_task_b_source_geometry(document)
    committed = _mapping_from_bytes(
        _asset_path(_INITIAL_TASK_SOURCE_RELATIVE).read_bytes(),
        _TASK_CONTRACT_INVALID,
    )
    committed_command = assert_initial_task_source_geometry(committed)
    assert_task_b_source_real_task_a_import(_seed_task_b_contract_bytes())
    assert_task_b_source_real_task_a_import(_SEED_TASK_B_TEXT.encode("utf-8"))

    assert task_a_command == committed_command
    assert task_b_command != task_a_command
    assert [item["id"] for item in document["active_task"]["validation"]["commands"]] == [
        _TASK_A_COMMAND_ID,
        _TASK_B_COMMAND_ID,
    ]
    assert_baseline_separated("true", task_a_command)
    assert_baseline_separated("true", task_b_command)


def test_task_b_source_path_boundary_stays_outside_seed_and_task_a() -> None:
    assert_task_b_source_path_boundary(_TASK_B_SOURCE_RELATIVE)
    declared = Path(_TASK_B_SOURCE_RELATIVE)
    assert not declared.is_absolute()
    assert not _TASK_B_SOURCE_RELATIVE.startswith(f"{_SEED_RELATIVE}/")
    assert _TASK_B_SOURCE_RELATIVE != _INITIAL_TASK_SOURCE_RELATIVE
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_path_boundary(f"{_SEED_RELATIVE}/tasks.yaml")

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_schema_negative_missing_allowed_paths_is_rejected() -> None:
    document = _minimal_task_b()
    del document["active_task"]["allowed_paths"]

    with pytest.raises(TaskContractError, match="TASK_FIELD_MISSING") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_MISSING"


def test_task_b_source_schema_negative_unknown_field_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["notes"] = "synthetic"

    with pytest.raises(TaskContractError, match="TASK_FIELD_UNKNOWN") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_UNKNOWN"


def test_task_b_source_relative_path_negative_absolute_allowed_path_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["allowed_paths"] = ["/synthetic/summary.py"]

    with pytest.raises(TaskContractError, match="TASK_VALUE_CONFLICT") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_VALUE_CONFLICT"


def test_task_b_source_relative_path_negative_parent_spec_ref_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["spec_ref"] = "../task_b.md"

    with pytest.raises(TaskContractError, match="TASK_PATH_TRAVERSAL") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_PATH_TRAVERSAL"


def test_task_b_source_outcome_negative_expected_red_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["expected_outcome"] = {
        "kind": "EXPECTED_RED",
        "command_id": _TASK_B_COMMAND_ID,
        "failure_signature": "synthetic failure",
    }
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_B_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_outcome_negative_settled_kind_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["expected_outcome"] = {"kind": "SETTLED"}

    with pytest.raises(TaskContractError, match="TASK_VALUE_CONFLICT") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_VALUE_CONFLICT"


def test_task_b_source_outcome_negative_null_task_is_rejected() -> None:
    document = {"schema_version": _SCHEMA_VERSION, "active_task": None}
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id is None
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_validation_negative_task_a_only_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["validation"]["commands"] = [
        document["active_task"]["validation"]["commands"][0]
    ]
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_B_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_validation_negative_fake_task_a_command_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["validation"]["commands"][0]["command"] = (
        "python -m reference_task_a_stub"
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_validation_negative_reversed_commands_are_rejected() -> None:
    document = _minimal_task_b()
    commands = document["active_task"]["validation"]["commands"]
    document["active_task"]["validation"]["commands"] = [commands[1], commands[0]]

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_allowed_path_negative_labels_only_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["allowed_paths"] = [_TASK_A_ALLOWED_PATH]
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_B_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_allowed_path_negative_extra_labels_path_is_rejected() -> None:
    document = _minimal_task_b()
    document["active_task"]["allowed_paths"] = [
        _TASK_B_ALLOWED_PATH,
        _TASK_A_ALLOWED_PATH,
    ]
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_B_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_identity_negative_task_a_document_is_rejected() -> None:
    document = _minimal_task()
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_A_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_source_import_negative_summary_without_task_a_use_is_rejected() -> None:
    payload = (
        "import reference_app.labels\n"
        "import reference_app.summary\n\n"
        "def test_task_b_contract() -> None:\n"
        "    reference_app.summary.public_summary()\n"
    ).encode("utf-8")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_source_real_task_a_import(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_candidate_bytes_bind_null_active_task() -> None:
    path = _asset_path(_TERMINAL_SOURCE_RELATIVE)
    if not path.is_file():
        pytest.fail(
            f"{_MISSING_TERMINAL_SOURCE_SIGNATURE}: missing {_TERMINAL_SOURCE_RELATIVE}"
        )
    payload = path.read_bytes()
    _assert_public_privacy(payload)
    identity = canonicalize_persisted_task_snapshot(payload)
    document = _mapping_from_bytes(payload, _TASK_CONTRACT_INVALID)
    assert_terminal_source_geometry(document)
    assert_terminal_source_path_boundary(_TERMINAL_SOURCE_RELATIVE)

    assert identity.task_id is None
    assert document["schema_version"] == _SCHEMA_VERSION
    assert document["active_task"] is None
    assert set(document) == set(_TASK_TOP_LEVEL)
    assert _FINGERPRINT.fullmatch(identity.fingerprint)
    assert _FINGERPRINT.fullmatch(identity.snapshot_sha256)
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as task_a_boundary:
        assert_initial_task_source_geometry(document)
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as task_b_boundary:
        assert_task_b_source_geometry(document)

    assert task_a_boundary.value.reason_code == _TASK_CONTRACT_INVALID
    assert task_b_boundary.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_null_geometry_is_parser_accepted() -> None:
    document = _minimal_terminal_source()
    payload = _encode(document)
    _assert_public_privacy(payload)
    identity = canonicalize_persisted_task_snapshot(payload)
    assert_terminal_source_geometry(document)
    assert_terminal_source_path_boundary(_TERMINAL_SOURCE_RELATIVE)

    assert identity.task_id is None
    assert document["active_task"] is None
    assert set(document) == {"schema_version", "active_task"}
    assert _FINGERPRINT.fullmatch(identity.fingerprint)
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as task_a_boundary:
        assert_initial_task_source_geometry(document)
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as task_b_boundary:
        assert_task_b_source_geometry(document)

    assert task_a_boundary.value.reason_code == _TASK_CONTRACT_INVALID
    assert task_b_boundary.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_path_boundary_stays_outside_seed_task_a_and_task_b() -> None:
    assert_terminal_source_path_boundary(_TERMINAL_SOURCE_RELATIVE)
    declared = Path(_TERMINAL_SOURCE_RELATIVE)
    assert not declared.is_absolute()
    assert _TERMINAL_SOURCE_RELATIVE != _INITIAL_TASK_SOURCE_RELATIVE
    assert _TERMINAL_SOURCE_RELATIVE != _TASK_B_SOURCE_RELATIVE
    assert not _TERMINAL_SOURCE_RELATIVE.startswith(f"{_SEED_RELATIVE}/")
    for foreign in (
        _INITIAL_TASK_SOURCE_RELATIVE,
        _TASK_B_SOURCE_RELATIVE,
        f"{_SEED_RELATIVE}/tasks.yaml",
    ):
        with pytest.raises(
            ReferenceBundleContractError,
            match=_TASK_CONTRACT_INVALID,
        ) as exc_info:
            assert_terminal_source_path_boundary(foreign)

        assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_schema_negative_missing_active_task_is_rejected() -> None:
    document = _minimal_terminal_source()
    del document["active_task"]

    with pytest.raises(TaskContractError, match="TASK_FIELD_MISSING") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_MISSING"
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as local:
        assert_terminal_source_geometry(document)

    assert local.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_schema_negative_unknown_field_is_rejected() -> None:
    for key, value in (
        ("history", "synthetic"),
        ("reopen", True),
        ("queue", ["synthetic"]),
        ("kind", "TERMINAL_REST"),
        ("settled", True),
    ):
        document = _minimal_terminal_source()
        document[key] = value
        with pytest.raises(TaskContractError, match="TASK_FIELD_UNKNOWN") as exc_info:
            canonicalize_persisted_task_snapshot(_encode(document))

        assert exc_info.value.reason_code == "TASK_FIELD_UNKNOWN"
        with pytest.raises(
            ReferenceBundleContractError,
            match=_TASK_CONTRACT_INVALID,
        ) as local:
            assert_terminal_source_geometry(document)

        assert local.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_schema_negative_wrong_schema_version_is_rejected() -> None:
    document = _minimal_terminal_source()
    document["schema_version"] = "9.9"

    with pytest.raises(TaskContractError, match="TASK_VALUE_CONFLICT") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_VALUE_CONFLICT"
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as local:
        assert_terminal_source_geometry(document)

    assert local.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_authority_negative_task_a_document_is_rejected() -> None:
    document = _minimal_task()
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_A_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_terminal_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_authority_negative_task_b_document_is_rejected() -> None:
    document = _minimal_task_b()
    identity = canonicalize_persisted_task_snapshot(_encode(document))

    assert identity.task_id == _TASK_B_ID
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_terminal_source_geometry(document)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_authority_negative_empty_mapping_is_rejected() -> None:
    document = _minimal_terminal_source()
    document["active_task"] = {}

    with pytest.raises(TaskContractError, match="TASK_FIELD_MISSING") as exc_info:
        canonicalize_persisted_task_snapshot(_encode(document))

    assert exc_info.value.reason_code == "TASK_FIELD_MISSING"
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as local:
        assert_terminal_source_geometry(document)

    assert local.value.reason_code == _TASK_CONTRACT_INVALID


def test_terminal_source_authority_negative_non_null_scalar_is_rejected() -> None:
    for value in ("null", False, 0, []):
        document = _minimal_terminal_source()
        document["active_task"] = value
        with pytest.raises(TaskContractError, match="TASK_FIELD_TYPE") as exc_info:
            canonicalize_persisted_task_snapshot(_encode(document))

        assert exc_info.value.reason_code == "TASK_FIELD_TYPE"
        with pytest.raises(
            ReferenceBundleContractError,
            match=_TASK_CONTRACT_INVALID,
        ) as local:
            assert_terminal_source_geometry(document)

        assert local.value.reason_code == _TASK_CONTRACT_INVALID


_TASK_A_SPEC_LINES = (
    "allowed_path: src/reference_app/labels.py",
    "behavior: reference_app.labels.public_labels()",
    "contract_test: tests/test_labels.py",
    f"validation_command: {_TASK_A_COMMAND}",
)
_TASK_A_SPEC_FORBIDDEN = (
    "src/reference_app/summary.py",
    "tests/test_summary.py",
    "reference_task_a_contract",
    "reference_task_b_contract",
    "python -m reference_task_b_contract",
    "task_specs/task_b.md",
    "prompts/developer.md",
    "prompts/orchestrator.md",
    "prompts/validator.md",
    "schema_version:",
    "active_task:",
    "expected_outcome:",
    "spec_ref:",
    "SETTLED",
    "DRAINED_AWAITING_TASK",
    "TERMINAL_REST",
    "PUBLIC-P-",
)
_TASK_A_SPEC_SOLUTION_MARKERS = (
    "def ",
    "return ",
    "class ",
    "lambda ",
    "import ",
    "```",
    *_SOLUTION_BYTE_MARKERS,
)


def _task_a_spec_bytes(
    *extra: str,
    omit: frozenset[str] = frozenset(),
) -> bytes:
    lines = [line for line in _TASK_A_SPEC_LINES if line not in omit]
    lines.extend(extra)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _task_a_spec_text(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID) from exc


def assert_task_a_spec_path_boundary(relative: str) -> None:
    """Require the single public Task A spec path."""

    if relative != _TASK_A_SPEC_RELATIVE or not _closed_relative(relative):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative in {
        _INITIAL_TASK_SOURCE_RELATIVE,
        _TASK_B_SOURCE_RELATIVE,
        _TERMINAL_SOURCE_RELATIVE,
        "examples/reference/task_specs/task_b.md",
    }:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative.startswith(f"{_SEED_RELATIVE}/"):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def assert_task_a_spec_contract(payload: bytes) -> None:
    """Require labels behavior, allowed path, and validation without solution bytes."""

    text = _task_a_spec_text(payload)
    if text.strip() == "":
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    _assert_public_privacy(payload)
    lines = [line.strip() for line in text.splitlines() if line.strip() != ""]
    for prefix, expected in (
        ("allowed_path:", _TASK_A_SPEC_LINES[0]),
        ("behavior:", _TASK_A_SPEC_LINES[1]),
        ("contract_test:", _TASK_A_SPEC_LINES[2]),
        ("validation_command:", _TASK_A_SPEC_LINES[3]),
    ):
        matched = [line for line in lines if line.startswith(prefix)]
        if matched != [expected]:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    for forbidden in _TASK_A_SPEC_FORBIDDEN:
        if forbidden in text:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    for marker in _TASK_A_SPEC_SOLUTION_MARKERS:
        if marker in text:
            raise ReferenceBundleContractError(_SPEC_HIDDEN_SOLUTION)


def test_task_a_spec_candidate_bytes_bind_labels_contract() -> None:
    path = _asset_path(_TASK_A_SPEC_RELATIVE)
    if not path.is_file():
        pytest.fail(
            f"{_MISSING_TASK_A_SPEC_SIGNATURE}: missing {_TASK_A_SPEC_RELATIVE}"
        )
    payload = path.read_bytes()
    assert_task_a_spec_path_boundary(_TASK_A_SPEC_RELATIVE)
    assert_task_a_spec_contract(payload)
    assert _FINGERPRINT.fullmatch(hashlib.sha256(payload).hexdigest())


def test_task_a_spec_positive_bytes_bind_labels_function_path_and_validation() -> None:
    payload = _task_a_spec_bytes()
    assert_task_a_spec_path_boundary(_TASK_A_SPEC_RELATIVE)
    assert_task_a_spec_contract(payload)
    text = payload.decode("utf-8")

    assert _TASK_A_ALLOWED_PATH in text
    assert "reference_app.labels.public_labels()" in text
    assert "tests/test_labels.py" in text
    assert _TASK_A_COMMAND in text
    assert _TASK_B_ALLOWED_PATH not in text
    assert _FINGERPRINT.fullmatch(hashlib.sha256(payload).hexdigest())


def test_task_a_spec_positive_bytes_allow_boundary_prose() -> None:
    payload = _task_a_spec_bytes("No other product path is authorized.")
    assert_task_a_spec_contract(payload)


def test_task_a_spec_path_boundary_stays_outside_seed_and_sources() -> None:
    assert_task_a_spec_path_boundary(_TASK_A_SPEC_RELATIVE)
    declared = Path(_TASK_A_SPEC_RELATIVE)
    assert not declared.is_absolute()
    assert _TASK_A_SPEC_RELATIVE != _TASK_A_SPEC_REF
    assert not _TASK_A_SPEC_RELATIVE.startswith(f"{_SEED_RELATIVE}/")
    for foreign in (
        _INITIAL_TASK_SOURCE_RELATIVE,
        _TASK_B_SOURCE_RELATIVE,
        _TERMINAL_SOURCE_RELATIVE,
        f"{_SEED_RELATIVE}/task_specs/task_a.md",
        "examples/reference/task_specs/task_b.md",
        _TASK_A_SPEC_REF,
        "../task_a.md",
        "/tmp/task_a.md",
    ):
        with pytest.raises(
            ReferenceBundleContractError,
            match=_TASK_CONTRACT_INVALID,
        ) as exc_info:
            assert_task_a_spec_path_boundary(foreign)

        assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


@pytest.mark.parametrize("omitted", _TASK_A_SPEC_LINES)
def test_task_a_spec_contract_negative_missing_obligation_is_rejected(
    omitted: str,
) -> None:
    payload = _task_a_spec_bytes(omit=frozenset({omitted}))

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_allowed_path_negative_summary_is_rejected() -> None:
    payload = _task_a_spec_bytes(f"allowed_path: {_TASK_B_ALLOWED_PATH}")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_allowed_path_negative_absolute_is_rejected() -> None:
    payload = _task_a_spec_bytes(
        f"allowed_path: /{_TASK_A_ALLOWED_PATH}",
        omit=frozenset({_TASK_A_SPEC_LINES[0]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_allowed_path_negative_parent_is_rejected() -> None:
    payload = _task_a_spec_bytes(
        f"allowed_path: ../{_TASK_A_ALLOWED_PATH}",
        omit=frozenset({_TASK_A_SPEC_LINES[0]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_validation_negative_task_b_command_is_rejected() -> None:
    payload = _task_a_spec_bytes(f"validation_command: {_TASK_B_COMMAND}")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_validation_negative_baseline_command_is_rejected() -> None:
    payload = _task_a_spec_bytes(
        "validation_command: true",
        omit=frozenset({_TASK_A_SPEC_LINES[3]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_authority_negative_task_source_is_rejected() -> None:
    payload = _encode(_minimal_task())

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_authority_negative_task_b_source_is_rejected() -> None:
    payload = _encode(_minimal_task_b())

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_authority_negative_task_source_shape_is_rejected() -> None:
    payload = _task_a_spec_bytes("schema_version: '1.0'")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_outcome_negative_settled_claim_is_rejected() -> None:
    payload = _task_a_spec_bytes("SETTLED")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_hidden_solution_negative_function_is_rejected() -> None:
    payload = _task_a_spec_bytes() + b"\ndef public_labels():\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_SPEC_HIDDEN_SOLUTION,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _SPEC_HIDDEN_SOLUTION


def test_task_a_spec_hidden_solution_negative_patch_is_rejected() -> None:
    payload = _task_a_spec_bytes() + b"\ndiff --git a/labels b/labels\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_SPEC_HIDDEN_SOLUTION,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _SPEC_HIDDEN_SOLUTION


def test_task_a_spec_hidden_solution_negative_code_fence_is_rejected() -> None:
    payload = _task_a_spec_bytes() + b"\n```\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_SPEC_HIDDEN_SOLUTION,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _SPEC_HIDDEN_SOLUTION


def test_task_a_spec_private_coupling_negative_marker_is_rejected() -> None:
    payload = _task_a_spec_bytes() + b"\npersona\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_contract_negative_empty_is_rejected() -> None:
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(b"")

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_a_spec_contract_negative_non_utf8_is_rejected() -> None:
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_a_spec_contract(b"\xff")

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


_TASK_B_BEHAVIOR = (
    "reference_app.summary.public_summary(reference_app.labels.public_labels())"
)
_TASK_B_SPEC_LINES = (
    f"allowed_path: {_TASK_B_ALLOWED_PATH}",
    f"behavior: {_TASK_B_BEHAVIOR}",
    "contract_test: tests/test_labels.py",
    "contract_test: tests/test_summary.py",
    f"validation_command: {_task_a_contract_command()}",
    f"validation_command: {_TASK_B_COMMAND}",
)
_TASK_B_SPEC_GROUPS = (
    ("allowed_path:", (_TASK_B_SPEC_LINES[0],)),
    ("behavior:", (_TASK_B_SPEC_LINES[1],)),
    ("contract_test:", (_TASK_B_SPEC_LINES[2], _TASK_B_SPEC_LINES[3])),
    ("validation_command:", (_TASK_B_SPEC_LINES[4], _TASK_B_SPEC_LINES[5])),
)
_TASK_B_SPEC_FORBIDDEN = (
    "reference_task_a_contract",
    "reference_task_b_contract",
    "src/reference_app/labels.py",
    "task_specs/task_a.md",
    "task_specs/task_b.md",
    "prompts/developer.md",
    "prompts/orchestrator.md",
    "prompts/validator.md",
    "schema_version:",
    "active_task:",
    "expected_outcome:",
    "spec_ref:",
    "SETTLED",
    "DRAINED_AWAITING_TASK",
    "TERMINAL_REST",
    "PUBLIC-P-",
)
_TASK_B_SPEC_SOLUTION_MARKERS = (
    "def ",
    "return ",
    "class ",
    "lambda ",
    "import ",
    "```",
    *_SOLUTION_BYTE_MARKERS,
)
_TASK_B_PRODUCT_MODULE_DEPENDENCY_LINE = "dependency_module: reference_app.labels"
_OBSERVED_TASK_B_PASS_THROUGH_SHA256 = (
    "52c1e408c23050e610180d45b8126abfd932fc8f901dde5ffebb8e40a4b46f60"
)
_OBSERVED_TASK_B_PASS_THROUGH_BYTES = b"def public_summary(labels):\n    return labels\n"
_TASK_B_PRODUCT_IMPORTED_AND_USED_BYTES = (
    b"import reference_app.labels\n"
    b"\n"
    b"\n"
    b"def public_summary(labels):\n"
    b"    return labels\n"
)
_TASK_B_PRODUCT_IMPORT_WITHOUT_LOAD_BYTES = (
    b"import reference_app.labels\n"
    b"\n"
    b"\n"
    b"def public_summary(labels):\n"
    b"    return None\n"
)


def _task_b_spec_bytes(
    *extra: str,
    omit: frozenset[str] = frozenset(),
) -> bytes:
    lines = [line for line in _TASK_B_SPEC_LINES if line not in omit]
    lines.extend(extra)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _task_b_spec_text(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID) from exc


def assert_task_b_spec_path_boundary(relative: str) -> None:
    """Require the single public Task B spec path."""

    if relative != _TASK_B_SPEC_RELATIVE or not _closed_relative(relative):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative in {
        _INITIAL_TASK_SOURCE_RELATIVE,
        _TASK_B_SOURCE_RELATIVE,
        _TERMINAL_SOURCE_RELATIVE,
        _TASK_A_SPEC_RELATIVE,
    }:
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    if relative.startswith(f"{_SEED_RELATIVE}/"):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def assert_task_b_spec_contract(payload: bytes) -> None:
    """Require the composed summary call, summary path, and A-then-B validation."""

    text = _task_b_spec_text(payload)
    if text.strip() == "":
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    _assert_public_privacy(payload)
    lines = [line.strip() for line in text.splitlines() if line.strip() != ""]
    for prefix, expected in _TASK_B_SPEC_GROUPS:
        matched = tuple(line for line in lines if line.startswith(prefix))
        if matched != expected:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    for forbidden in _TASK_B_SPEC_FORBIDDEN:
        if forbidden in text:
            raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)
    for marker in _TASK_B_SPEC_SOLUTION_MARKERS:
        if marker in text:
            raise ReferenceBundleContractError(_SPEC_HIDDEN_SOLUTION)


def test_task_b_spec_candidate_bytes_bind_summary_contract() -> None:
    path = _asset_path(_TASK_B_SPEC_RELATIVE)
    if not path.is_file():
        pytest.fail(
            f"{_MISSING_TASK_B_SPEC_SIGNATURE}: missing {_TASK_B_SPEC_RELATIVE}"
        )
    payload = path.read_bytes()
    assert_task_b_spec_path_boundary(_TASK_B_SPEC_RELATIVE)
    assert_task_b_spec_contract(payload)
    assert _FINGERPRINT.fullmatch(hashlib.sha256(payload).hexdigest())


def test_task_b_spec_positive_bytes_bind_summary_call_path_and_validation() -> None:
    payload = _task_b_spec_bytes()
    assert_task_b_spec_path_boundary(_TASK_B_SPEC_RELATIVE)
    assert_task_b_spec_contract(payload)
    assert_task_b_source_real_task_a_import(_SEED_TASK_B_TEXT.encode("utf-8"))
    text = payload.decode("utf-8")

    assert _TASK_B_BEHAVIOR in text
    assert _TASK_B_BEHAVIOR in _SEED_TASK_B_TEXT
    assert _TASK_B_ALLOWED_PATH in text
    assert "tests/test_labels.py" in text
    assert "tests/test_summary.py" in text
    assert text.index(f"validation_command: {_task_a_contract_command()}") < text.index(
        f"validation_command: {_TASK_B_COMMAND}"
    )
    assert _TASK_A_ALLOWED_PATH not in text
    assert "SETTLED" not in text
    assert "PUBLIC-P-" not in text
    assert _FINGERPRINT.fullmatch(hashlib.sha256(payload).hexdigest())


def test_task_b_spec_positive_bytes_allow_boundary_prose() -> None:
    payload = _task_b_spec_bytes(
        "The summary call must use the labels result.",
        "No other product path is authorized.",
    )
    assert_task_b_spec_contract(payload)


def test_task_b_spec_path_boundary_stays_outside_seed_sources_and_task_a() -> None:
    assert_task_b_spec_path_boundary(_TASK_B_SPEC_RELATIVE)
    declared = Path(_TASK_B_SPEC_RELATIVE)
    assert not declared.is_absolute()
    assert _TASK_B_SPEC_RELATIVE != _TASK_B_SPEC_REF
    assert _TASK_B_SPEC_RELATIVE != _TASK_A_SPEC_RELATIVE
    assert not _TASK_B_SPEC_RELATIVE.startswith(f"{_SEED_RELATIVE}/")
    for foreign in (
        _INITIAL_TASK_SOURCE_RELATIVE,
        _TASK_B_SOURCE_RELATIVE,
        _TERMINAL_SOURCE_RELATIVE,
        _TASK_A_SPEC_RELATIVE,
        f"{_SEED_RELATIVE}/task_specs/task_b.md",
        _TASK_B_SPEC_REF,
        "../task_b.md",
        "/tmp/task_b.md",
    ):
        with pytest.raises(
            ReferenceBundleContractError,
            match=_TASK_CONTRACT_INVALID,
        ) as exc_info:
            assert_task_b_spec_path_boundary(foreign)

        assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


@pytest.mark.parametrize("omitted", _TASK_B_SPEC_LINES)
def test_task_b_spec_contract_negative_missing_obligation_is_rejected(
    omitted: str,
) -> None:
    payload = _task_b_spec_bytes(omit=frozenset({omitted}))

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_allowed_path_negative_labels_only_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        f"allowed_path: {_TASK_A_ALLOWED_PATH}",
        omit=frozenset({_TASK_B_SPEC_LINES[0]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_allowed_path_negative_extra_labels_path_is_rejected() -> None:
    payload = _task_b_spec_bytes(f"allowed_path: {_TASK_A_ALLOWED_PATH}")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_allowed_path_negative_absolute_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        f"allowed_path: /{_TASK_B_ALLOWED_PATH}",
        omit=frozenset({_TASK_B_SPEC_LINES[0]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_allowed_path_negative_parent_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        f"allowed_path: ../{_TASK_B_ALLOWED_PATH}",
        omit=frozenset({_TASK_B_SPEC_LINES[0]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_behavior_negative_summary_without_labels_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        "behavior: reference_app.summary.public_summary()",
        omit=frozenset({_TASK_B_SPEC_LINES[1]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_behavior_negative_labels_only_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        "behavior: reference_app.labels.public_labels()",
        omit=frozenset({_TASK_B_SPEC_LINES[1]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_behavior_negative_unqualified_call_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        "behavior: public_summary(public_labels())",
        omit=frozenset({_TASK_B_SPEC_LINES[1]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_validation_negative_task_b_only_is_rejected() -> None:
    payload = _task_b_spec_bytes(omit=frozenset({_TASK_B_SPEC_LINES[4]}))

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_validation_negative_reversed_commands_are_rejected() -> None:
    payload = _task_b_spec_bytes(
        f"validation_command: {_TASK_B_COMMAND}",
        f"validation_command: {_task_a_contract_command()}",
        omit=frozenset({_TASK_B_SPEC_LINES[4], _TASK_B_SPEC_LINES[5]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_validation_negative_fake_task_a_command_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        "validation_command: python -m reference_task_a_stub",
        f"validation_command: {_TASK_B_COMMAND}",
        omit=frozenset({_TASK_B_SPEC_LINES[4], _TASK_B_SPEC_LINES[5]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_validation_negative_baseline_command_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        "validation_command: true",
        f"validation_command: {_TASK_B_COMMAND}",
        omit=frozenset({_TASK_B_SPEC_LINES[4], _TASK_B_SPEC_LINES[5]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_contract_test_negative_summary_only_is_rejected() -> None:
    payload = _task_b_spec_bytes(omit=frozenset({_TASK_B_SPEC_LINES[2]}))

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_contract_test_negative_reversed_order_is_rejected() -> None:
    payload = _task_b_spec_bytes(
        "contract_test: tests/test_summary.py",
        "contract_test: tests/test_labels.py",
        omit=frozenset({_TASK_B_SPEC_LINES[2], _TASK_B_SPEC_LINES[3]}),
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_authority_negative_task_a_spec_is_rejected() -> None:
    payload = _asset_path(_TASK_A_SPEC_RELATIVE).read_bytes()

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_authority_negative_task_b_source_is_rejected() -> None:
    payload = _encode(_minimal_task_b())

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_authority_negative_task_source_shape_is_rejected() -> None:
    payload = _task_b_spec_bytes("schema_version: '1.0'")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_outcome_negative_settled_claim_is_rejected() -> None:
    payload = _task_b_spec_bytes("SETTLED")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_acceptance_negative_public_claim_is_rejected() -> None:
    payload = _task_b_spec_bytes("PUBLIC-P-02")

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_hidden_solution_negative_function_is_rejected() -> None:
    payload = _task_b_spec_bytes() + b"\ndef public_summary():\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_SPEC_HIDDEN_SOLUTION,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _SPEC_HIDDEN_SOLUTION


def test_task_b_spec_hidden_solution_negative_patch_is_rejected() -> None:
    payload = _task_b_spec_bytes() + b"\ndiff --git a/summary b/summary\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_SPEC_HIDDEN_SOLUTION,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _SPEC_HIDDEN_SOLUTION


def test_task_b_spec_hidden_solution_negative_code_fence_is_rejected() -> None:
    payload = _task_b_spec_bytes() + b"\n```\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_SPEC_HIDDEN_SOLUTION,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _SPEC_HIDDEN_SOLUTION


def test_task_b_spec_private_coupling_negative_marker_is_rejected() -> None:
    payload = _task_b_spec_bytes() + b"\npersona\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_contract_negative_empty_is_rejected() -> None:
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(b"")

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_spec_contract_negative_non_utf8_is_rejected() -> None:
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_spec_contract(b"\xff")

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def _task_b_spec_structured_lines(payload: bytes) -> tuple[str, ...]:
    text = _task_b_spec_text(payload)
    return tuple(line.strip() for line in text.splitlines() if line.strip() != "")


def _task_b_spec_has_product_module_dependency(payload: bytes) -> bool:
    lines = _task_b_spec_structured_lines(payload)
    return lines.count(_TASK_B_PRODUCT_MODULE_DEPENDENCY_LINE) == 1


def _module_level_public_summary(tree: ast.AST) -> ast.FunctionDef | None:
    found = [
        node
        for node in getattr(tree, "body", ())
        if isinstance(node, ast.FunctionDef) and node.name == "public_summary"
    ]
    if len(found) != 1:
        return None
    return found[0]


def _public_summary_loads_its_single_parameter(function: ast.FunctionDef) -> bool:
    arguments = function.args
    if (
        arguments.posonlyargs
        or arguments.kwonlyargs
        or arguments.vararg is not None
        or arguments.kwarg is not None
        or arguments.defaults
        or len(arguments.args) != 1
    ):
        return False
    parameter_name = arguments.args[0].arg
    return any(
        isinstance(node, ast.Name)
        and node.id == parameter_name
        and isinstance(node.ctx, ast.Load)
        for node in ast.walk(function)
    )


def assert_task_b_product_module_dependency(payload: bytes) -> None:
    """Require a labels module import and a loaded single summary parameter."""

    tree = _parsed_module(payload)
    function = _module_level_public_summary(tree)
    imported = "reference_app.labels" in _module_imports(tree)
    used = function is not None and _public_summary_loads_its_single_parameter(function)
    if not (imported and used):
        raise ReferenceBundleContractError(_TASK_CONTRACT_INVALID)


def test_task_b_spec_candidate_bytes_bind_product_module_dependency() -> None:
    path = _asset_path(_TASK_B_SPEC_RELATIVE)
    if not path.is_file():
        pytest.fail(
            f"{_MISSING_TASK_B_SPEC_SIGNATURE}: missing {_TASK_B_SPEC_RELATIVE}"
        )
    payload = path.read_bytes()
    if not _task_b_spec_has_product_module_dependency(payload):
        pytest.fail(_TASK_B_MODULE_DEPENDENCY_RED)


def test_task_b_product_module_dependency_positive_import_and_loaded_parameter_is_accepted() -> None:
    assert_task_b_product_module_dependency(_TASK_B_PRODUCT_IMPORTED_AND_USED_BYTES)


def test_task_b_product_module_dependency_negative_observed_pass_through_is_rejected() -> None:
    payload = _OBSERVED_TASK_B_PASS_THROUGH_BYTES
    assert hashlib.sha256(payload).hexdigest() == _OBSERVED_TASK_B_PASS_THROUGH_SHA256
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_product_module_dependency(payload)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


def test_task_b_product_module_dependency_negative_import_without_loaded_parameter_is_rejected() -> None:
    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_task_b_product_module_dependency(_TASK_B_PRODUCT_IMPORT_WITHOUT_LOAD_BYTES)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


_BUNDLE_TOP_LEVEL = frozenset({"schema_version", "kind", "entries"})
_BUNDLE_ENTRY_KEYS = frozenset(
    {"source", "destination_root", "destination", "sha256", "byte_length"}
)
_BUNDLE_DESTINATION_ROOTS = frozenset({"input", "seed"})
_BUNDLE_ASSEMBLY_ROOTS = ("candidate", "input", "seed", "runtime", "evidence")
_BUNDLE_INPUT_DESTINATIONS = (
    (_REFERENCE_CONFIG_RELATIVE, "runner.yaml"),
    (_INITIAL_TASK_SOURCE_RELATIVE, "tasks.yaml"),
    (_TASK_B_SOURCE_RELATIVE, "task_sources/task_b.yaml"),
    (_TERMINAL_SOURCE_RELATIVE, "task_sources/terminal_rest.yaml"),
    (_TASK_A_SPEC_RELATIVE, "task_specs/task_a.md"),
    (_TASK_B_SPEC_RELATIVE, "task_specs/task_b.md"),
    ("prompts/developer.md", "prompts/developer.md"),
    ("prompts/orchestrator.md", "prompts/orchestrator.md"),
    ("prompts/validator.md", "prompts/validator.md"),
)
_BUNDLE_MANIFEST_PAIRS = tuple(
    ("input", source, destination) for source, destination in _BUNDLE_INPUT_DESTINATIONS
) + tuple(
    ("seed", f"{_SEED_RELATIVE}/{relative}", relative) for relative in _SEED_FILES
)
_BUNDLE_FORBIDDEN_IMPLEMENTATIONS = frozenset(
    {
        "src/reference_app/labels.py",
        "src/reference_app/summary.py",
        "labels.py",
        "summary.py",
        ".git",
        "settled_predecessor.yaml",
    }
)


def _forbidden_bundle_relative(relative: str) -> bool:
    if relative in _BUNDLE_FORBIDDEN_IMPLEMENTATIONS or ".git" in Path(relative).parts:
        return True
    return relative.endswith("/labels.py") or relative.endswith("/summary.py")


def _encode_bundle_manifest(document: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(document, indent=2, ensure_ascii=True, sort_keys=False) + "\n"
    ).encode("utf-8")


def _read_closed_bundle_sources(source_root: Path) -> dict[str, bytes]:
    payloads: dict[str, bytes] = {}
    source_resolved = source_root.resolve()
    for _destination_root, source, _destination in _BUNDLE_MANIFEST_PAIRS:
        path = source_root / source
        if not path.is_file():
            raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
        resolved = path.resolve()
        if not resolved.is_relative_to(source_resolved) or resolved == source_resolved:
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        payloads[source] = path.read_bytes()
    return payloads


def _bundle_manifest_document(payloads: Mapping[str, bytes]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for destination_root, source, destination in _BUNDLE_MANIFEST_PAIRS:
        payload = payloads.get(source)
        if payload is None:
            raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
        entries.append(
            {
                "source": source,
                "destination_root": destination_root,
                "destination": destination,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "byte_length": len(payload),
            }
        )
    return {
        "schema_version": _SCHEMA_VERSION,
        "kind": _BUNDLE_MANIFEST_KIND,
        "entries": entries,
    }


def regenerate_bundle_manifest_bytes(source_root: Path) -> bytes:
    """Return the canonical manifest for one closed candidate source root."""

    if source_root.resolve() != _candidate_root().resolve():
        raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
    encoded = _encode_bundle_manifest(
        _bundle_manifest_document(_read_closed_bundle_sources(source_root))
    )
    _assert_public_privacy(encoded)
    return encoded


def assert_bundle_manifest_document(
    document: Mapping[str, Any],
    payloads: Mapping[str, bytes],
) -> None:
    """Require the closed source map and exact SHA-256 bindings."""

    _require_exact_keys(document, _BUNDLE_TOP_LEVEL, _BUNDLE_AUTHORITY_INVALID)
    if document["schema_version"] != _SCHEMA_VERSION:
        raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
    if document["kind"] != _BUNDLE_MANIFEST_KIND:
        raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
    entries = document["entries"]
    if not isinstance(entries, list):
        raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
    seen_sources: set[str] = set()
    seen_destinations: set[tuple[str, str]] = set()
    observed: list[tuple[str, str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        _require_exact_keys(entry, _BUNDLE_ENTRY_KEYS, _BUNDLE_AUTHORITY_INVALID)
        source = entry["source"]
        destination_root = entry["destination_root"]
        destination = entry["destination"]
        digest = entry["sha256"]
        byte_length = entry["byte_length"]
        if (
            not isinstance(source, str)
            or not isinstance(destination_root, str)
            or not isinstance(destination, str)
            or not isinstance(digest, str)
            or type(byte_length) is not int
        ):
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        for text in (source, destination_root, destination):
            _assert_public_privacy(text.encode("utf-8"))
        if destination_root not in _BUNDLE_DESTINATION_ROOTS:
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        if not _closed_relative(source) or not _closed_relative(destination):
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        if _forbidden_bundle_relative(source) or _forbidden_bundle_relative(destination):
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        if source in seen_sources or (destination_root, destination) in seen_destinations:
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        seen_sources.add(source)
        seen_destinations.add((destination_root, destination))
        observed.append((destination_root, source, destination))
    if tuple(observed) != _BUNDLE_MANIFEST_PAIRS:
        raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
    if _BUNDLE_MANIFEST_RELATIVE in seen_sources:
        raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
    for entry in entries:
        payload = payloads.get(entry["source"])
        if payload is None:
            raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
        _assert_public_privacy(payload)
        digest = hashlib.sha256(payload).hexdigest()
        if entry["sha256"] != digest or entry["byte_length"] != len(payload):
            raise ReferenceBundleContractError(_BUNDLE_DIGEST_DRIFT)
        if _FINGERPRINT.fullmatch(entry["sha256"]) is None:
            raise ReferenceBundleContractError(_BUNDLE_DIGEST_DRIFT)


def _assert_assembly_roots(roots: Mapping[str, Path]) -> None:
    if tuple(roots) != _BUNDLE_ASSEMBLY_ROOTS:
        raise ReferenceBundleContractError(_ASSEMBLY_ROOT_OVERLAP)
    resolved = {name: path.resolve() for name, path in roots.items()}
    names = _BUNDLE_ASSEMBLY_ROOTS
    for index, left_name in enumerate(names):
        for right_name in names[index + 1 :]:
            if _paths_overlap(str(resolved[left_name]), str(resolved[right_name])):
                raise ReferenceBundleContractError(_ASSEMBLY_ROOT_OVERLAP)


def _relative_file_bytes(root: Path) -> dict[str, bytes]:
    if not root.is_dir():
        raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
    payloads: dict[str, bytes] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if not _closed_relative(relative):
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        payloads[relative] = path.read_bytes()
    return payloads


def _materialize_bundle(
    document: Mapping[str, Any],
    payloads: Mapping[str, bytes],
    input_root: Path,
    seed_root: Path,
) -> None:
    assert_bundle_manifest_document(document, payloads)
    roots = {"input": input_root, "seed": seed_root}
    for entry in document["entries"]:
        root = roots[str(entry["destination_root"])]
        destination = (root / str(entry["destination"])).resolve()
        if not destination.is_relative_to(root.resolve()):
            raise ReferenceBundleContractError(_BUNDLE_AUTHORITY_INVALID)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payloads[str(entry["source"])])


def _assert_materialized_bundle(
    input_root: Path,
    seed_root: Path,
    payloads: Mapping[str, bytes],
) -> None:
    document = _bundle_manifest_document(payloads)
    expected: dict[str, dict[str, bytes]] = {"input": {}, "seed": {}}
    for entry in document["entries"]:
        expected[str(entry["destination_root"])][str(entry["destination"])] = payloads[
            str(entry["source"])
        ]
    for root_name, root in (("input", input_root), ("seed", seed_root)):
        actual = _relative_file_bytes(root)
        if set(actual) != set(expected[root_name]):
            raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
        for relative, payload in expected[root_name].items():
            if actual[relative] != payload:
                raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)


def _prepare_assembly_roots(parent: Path) -> dict[str, Path]:
    parent.mkdir(parents=True, exist_ok=True)
    roots = {
        "input_root": parent / "input",
        "seed_root": parent / "seed",
        "runtime_root": parent / "runtime",
        "evidence_root": parent / "evidence",
    }
    for path in roots.values():
        path.mkdir()
    return roots


def _source_digests(source_root: Path) -> dict[str, str]:
    payloads = _read_closed_bundle_sources(source_root)
    return {
        relative: hashlib.sha256(payload).hexdigest()
        for relative, payload in payloads.items()
    }


def assemble_reference_bundle(
    *,
    source_root: Path,
    input_root: Path,
    seed_root: Path,
    runtime_root: Path,
    evidence_root: Path,
) -> None:
    """Copy closed candidate bytes into disjoint input and seed roots."""

    candidate = _candidate_root()
    _assert_assembly_roots(
        {
            "candidate": candidate,
            "input": input_root,
            "seed": seed_root,
            "runtime": runtime_root,
            "evidence": evidence_root,
        }
    )
    if source_root.resolve() != candidate.resolve():
        raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
    before = _source_digests(source_root)
    payloads = _read_closed_bundle_sources(source_root)
    _materialize_bundle(
        _bundle_manifest_document(payloads),
        payloads,
        input_root,
        seed_root,
    )
    if _source_digests(source_root) != before:
        raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
    if _relative_file_bytes(runtime_root) or _relative_file_bytes(evidence_root):
        raise ReferenceBundleContractError(_ASSEMBLY_SOURCE_DRIFT)
    _assert_materialized_bundle(input_root, seed_root, payloads)


def _bound_bundle_document() -> tuple[dict[str, Any], dict[str, bytes]]:
    payloads = _read_closed_bundle_sources(_candidate_root())
    return _bundle_manifest_document(payloads), payloads


def test_bundle_manifest_candidate_bytes_bind_regenerated_map() -> None:
    path = _asset_path(_BUNDLE_MANIFEST_RELATIVE)
    if not path.is_file():
        pytest.fail(
            f"{_MISSING_BUNDLE_MANIFEST_SIGNATURE}: missing {_BUNDLE_MANIFEST_RELATIVE}"
        )
    payload = path.read_bytes()
    expected = regenerate_bundle_manifest_bytes(_candidate_root())
    if payload != expected:
        pytest.fail(f"{_BUNDLE_DIGEST_DRIFT}: {_BUNDLE_MANIFEST_RELATIVE}")
    _assert_public_privacy(payload)
    document = json.loads(payload.decode("utf-8"))
    assert_bundle_manifest_document(
        document,
        _read_closed_bundle_sources(_candidate_root()),
    )


def test_bundle_manifest_source_destination_map_is_closed() -> None:
    document, payloads = _bound_bundle_document()
    assert_bundle_manifest_document(document, payloads)
    observed = tuple(
        (entry["destination_root"], entry["source"], entry["destination"])
        for entry in document["entries"]
    )

    assert observed == _BUNDLE_MANIFEST_PAIRS
    assert _BUNDLE_MANIFEST_RELATIVE not in {entry["source"] for entry in document["entries"]}
    assert [item[1] for item in observed if item[0] == "input"] == [
        source for source, _destination in _BUNDLE_INPUT_DESTINATIONS
    ]
    assert [item[2] for item in observed if item[0] == "input"] == [
        destination for _source, destination in _BUNDLE_INPUT_DESTINATIONS
    ]
    assert [item[2] for item in observed if item[0] == "seed"] == list(_SEED_FILES)


def test_bundle_manifest_sha256_binds_exact_candidate_bytes() -> None:
    document, payloads = _bound_bundle_document()

    for entry in document["entries"]:
        payload = payloads[entry["source"]]
        digest = hashlib.sha256(payload).hexdigest()
        assert entry["sha256"] == digest
        assert entry["byte_length"] == len(payload)
        assert _FINGERPRINT.fullmatch(entry["sha256"])
        assert entry["sha256"] == hashlib.sha256(
            (_candidate_root() / entry["source"]).read_bytes()
        ).hexdigest()


def test_bundle_manifest_regeneration_is_byte_identical() -> None:
    root = _candidate_root()
    first = regenerate_bundle_manifest_bytes(root)
    second = regenerate_bundle_manifest_bytes(root)
    parsed = json.loads(first.decode("utf-8"))

    assert first == second
    assert first.endswith(b"\n")
    assert _encode_bundle_manifest(parsed) == first
    _assert_public_privacy(first)


def test_bundle_assembly_copies_exact_bytes_into_separated_roots(tmp_path: Path) -> None:
    candidate = _candidate_root()
    before = _source_digests(candidate)
    first = _prepare_assembly_roots(tmp_path / "first")
    second = _prepare_assembly_roots(tmp_path / "second")
    assemble_reference_bundle(source_root=candidate, **first)
    assemble_reference_bundle(source_root=candidate, **second)
    payloads = _read_closed_bundle_sources(candidate)

    assert _source_digests(candidate) == before
    assert _relative_file_bytes(first["input_root"]) == _relative_file_bytes(
        second["input_root"]
    )
    assert _relative_file_bytes(first["seed_root"]) == _relative_file_bytes(
        second["seed_root"]
    )
    _assert_materialized_bundle(first["input_root"], first["seed_root"], payloads)
    assert _relative_file_bytes(first["runtime_root"]) == {}
    assert _relative_file_bytes(first["evidence_root"]) == {}
    assert not (first["input_root"] / "bundle_manifest.json").exists()
    assert not (first["input_root"] / "target").exists()
    assert not (first["input_root"] / "runtime").exists()
    assert not (first["seed_root"] / ".git").exists()
    assert not (first["seed_root"] / "src" / "reference_app" / "labels.py").exists()
    assert not (first["seed_root"] / "src" / "reference_app" / "summary.py").exists()
    _assert_assembly_roots(
        {
            "candidate": candidate,
            "input": first["input_root"],
            "seed": first["seed_root"],
            "runtime": first["runtime_root"],
            "evidence": first["evidence_root"],
        }
    )


def test_bundle_assembly_validates_schema_after_exact_copy(tmp_path: Path) -> None:
    roots = _prepare_assembly_roots(tmp_path / "schema")
    assemble_reference_bundle(source_root=_candidate_root(), **roots)
    input_root = roots["input_root"]
    config_path = input_root / "runner.yaml"
    config_document = _mapping_from_bytes(config_path.read_bytes(), _CONFIG_PATH_INVALID)
    assert_reference_config_geometry(config_document)
    loaded = load_config(config_path)
    task_a = _mapping_from_bytes(
        (input_root / "tasks.yaml").read_bytes(),
        _TASK_CONTRACT_INVALID,
    )
    task_b = _mapping_from_bytes(
        (input_root / "task_sources" / "task_b.yaml").read_bytes(),
        _TASK_CONTRACT_INVALID,
    )
    terminal = _mapping_from_bytes(
        (input_root / "task_sources" / "terminal_rest.yaml").read_bytes(),
        _TASK_CONTRACT_INVALID,
    )

    assert loaded.task_source == (input_root / "tasks.yaml").resolve()
    assert loaded.prompts.developer == (
        input_root / "prompts" / "developer.md"
    ).resolve()
    assert loaded.prompts.orchestrator == (
        input_root / "prompts" / "orchestrator.md"
    ).resolve()
    assert loaded.prompts.validator == (
        input_root / "prompts" / "validator.md"
    ).resolve()
    assert not loaded.runtime_root.is_relative_to(loaded.target_worktree)
    assert_initial_task_source_geometry(task_a)
    assert_task_b_source_geometry(task_b)
    assert_terminal_source_geometry(terminal)
    canonicalize_persisted_task_snapshot((input_root / "tasks.yaml").read_bytes())
    canonicalize_persisted_task_snapshot(
        (input_root / "task_sources" / "task_b.yaml").read_bytes()
    )
    canonicalize_persisted_task_snapshot(
        (input_root / "task_sources" / "terminal_rest.yaml").read_bytes()
    )
    assert_task_a_spec_contract((input_root / "task_specs" / "task_a.md").read_bytes())
    assert_task_b_spec_contract((input_root / "task_specs" / "task_b.md").read_bytes())
    assert assert_history_free_seed_geometry(roots["seed_root"]) == _SEED_BASELINE_COMMAND
    assert _relative_file_bytes(roots["runtime_root"]) == {}
    assert _relative_file_bytes(roots["evidence_root"]) == {}


@pytest.mark.parametrize(
    "overlap",
    (
        "input_inside_seed",
        "seed_inside_input",
        "runtime_inside_input",
        "evidence_equals_input",
        "input_equals_seed",
        "input_equals_candidate",
        "seed_inside_candidate",
    ),
)
def test_bundle_assembly_root_separation_rejects_overlap(
    tmp_path: Path,
    overlap: str,
) -> None:
    candidate = _candidate_root()
    input_root = tmp_path / "input"
    seed_root = tmp_path / "seed"
    runtime_root = tmp_path / "runtime"
    evidence_root = tmp_path / "evidence"
    if overlap == "input_inside_seed":
        input_root = seed_root / "input"
    elif overlap == "seed_inside_input":
        seed_root = input_root / "seed"
    elif overlap == "runtime_inside_input":
        runtime_root = input_root / "runtime"
    elif overlap == "evidence_equals_input":
        evidence_root = input_root
    elif overlap == "input_equals_seed":
        seed_root = input_root
    elif overlap == "input_equals_candidate":
        input_root = candidate
    elif overlap == "seed_inside_candidate":
        seed_root = candidate / _SEED_RELATIVE

    with pytest.raises(
        ReferenceBundleContractError,
        match=_ASSEMBLY_ROOT_OVERLAP,
    ) as exc_info:
        assemble_reference_bundle(
            source_root=candidate,
            input_root=input_root,
            seed_root=seed_root,
            runtime_root=runtime_root,
            evidence_root=evidence_root,
        )

    assert exc_info.value.reason_code == _ASSEMBLY_ROOT_OVERLAP


@pytest.mark.parametrize("mutation", ("sha256", "byte_length"))
def test_bundle_manifest_authority_negative_digest_drift_is_rejected(
    mutation: str,
) -> None:
    document, payloads = _bound_bundle_document()
    if mutation == "sha256":
        document["entries"][0]["sha256"] = "0" * 64
    else:
        document["entries"][0]["byte_length"] += 1

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_DIGEST_DRIFT,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _BUNDLE_DIGEST_DRIFT


def test_bundle_assembly_authority_negative_invented_bytes_are_rejected(
    tmp_path: Path,
) -> None:
    roots = _prepare_assembly_roots(tmp_path / "invented")
    assemble_reference_bundle(source_root=_candidate_root(), **roots)
    destination = roots["input_root"] / "runner.yaml"
    destination.write_bytes(destination.read_bytes() + b"invented\n")
    payloads = _read_closed_bundle_sources(_candidate_root())

    with pytest.raises(
        ReferenceBundleContractError,
        match=_ASSEMBLY_SOURCE_DRIFT,
    ) as exc_info:
        _assert_materialized_bundle(roots["input_root"], roots["seed_root"], payloads)

    assert exc_info.value.reason_code == _ASSEMBLY_SOURCE_DRIFT


def test_bundle_assembly_authority_negative_normalized_bytes_are_rejected(
    tmp_path: Path,
) -> None:
    roots = _prepare_assembly_roots(tmp_path / "normalized")
    assemble_reference_bundle(source_root=_candidate_root(), **roots)
    destination = roots["input_root"] / "tasks.yaml"
    destination.write_bytes(destination.read_bytes().replace(b"\n", b"\r\n"))
    payloads = _read_closed_bundle_sources(_candidate_root())

    with pytest.raises(
        ReferenceBundleContractError,
        match=_ASSEMBLY_SOURCE_DRIFT,
    ) as exc_info:
        _assert_materialized_bundle(roots["input_root"], roots["seed_root"], payloads)

    assert exc_info.value.reason_code == _ASSEMBLY_SOURCE_DRIFT


def test_bundle_assembly_authority_negative_silent_repair_is_rejected(
    tmp_path: Path,
) -> None:
    document, payloads = _bound_bundle_document()
    document["entries"][0]["sha256"] = "0" * 64
    input_root = tmp_path / "input"
    seed_root = tmp_path / "seed"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_DIGEST_DRIFT,
    ) as exc_info:
        _materialize_bundle(document, payloads, input_root, seed_root)

    assert exc_info.value.reason_code == _BUNDLE_DIGEST_DRIFT
    assert not input_root.exists()
    assert not seed_root.exists()


def test_bundle_manifest_authority_negative_private_marker_is_rejected() -> None:
    document, payloads = _bound_bundle_document()
    document["entries"][0]["source"] = "prompts/persona.md"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_TASK_CONTRACT_INVALID,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _TASK_CONTRACT_INVALID


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("source", "/tmp/runner.yaml"),
        ("source", "../runner.yaml"),
        ("destination", "/tmp/runner.yaml"),
        ("destination", "../runner.yaml"),
        ("destination_root", "runtime"),
        ("destination_root", "evidence"),
    ),
)
def test_bundle_manifest_authority_negative_path_escape_is_rejected(
    field: str,
    value: str,
) -> None:
    document, payloads = _bound_bundle_document()
    document["entries"][0][field] = value

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_AUTHORITY_INVALID,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _BUNDLE_AUTHORITY_INVALID


def test_bundle_manifest_authority_negative_missing_entry_is_rejected() -> None:
    document, payloads = _bound_bundle_document()
    document["entries"].pop()

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_AUTHORITY_INVALID,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _BUNDLE_AUTHORITY_INVALID


def test_bundle_manifest_authority_negative_extra_entry_is_rejected() -> None:
    document, payloads = _bound_bundle_document()
    document["entries"].append(
        {
            "source": _BUNDLE_MANIFEST_RELATIVE,
            "destination_root": "input",
            "destination": "bundle_manifest.json",
            "sha256": "0" * 64,
            "byte_length": 1,
        }
    )

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_AUTHORITY_INVALID,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _BUNDLE_AUTHORITY_INVALID


def test_bundle_manifest_authority_negative_duplicate_destination_is_rejected() -> None:
    document, payloads = _bound_bundle_document()
    document["entries"][1]["destination_root"] = document["entries"][0]["destination_root"]
    document["entries"][1]["destination"] = document["entries"][0]["destination"]

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_AUTHORITY_INVALID,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _BUNDLE_AUTHORITY_INVALID


@pytest.mark.parametrize(
    "field",
    ("promotion", "decision", "transition", "repair", "settled", "provenance"),
)
def test_bundle_manifest_authority_negative_transition_key_is_rejected(field: str) -> None:
    document, payloads = _bound_bundle_document()
    document[field] = "synthetic"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_AUTHORITY_INVALID,
    ) as exc_info:
        assert_bundle_manifest_document(document, payloads)

    assert exc_info.value.reason_code == _BUNDLE_AUTHORITY_INVALID


def test_bundle_assembly_authority_negative_source_mutation_is_rejected() -> None:
    document, payloads = _bound_bundle_document()
    mutated = dict(payloads)
    mutated[_REFERENCE_CONFIG_RELATIVE] = payloads[_REFERENCE_CONFIG_RELATIVE] + b"# mutated\n"

    with pytest.raises(
        ReferenceBundleContractError,
        match=_BUNDLE_DIGEST_DRIFT,
    ) as exc_info:
        assert_bundle_manifest_document(document, mutated)

    assert exc_info.value.reason_code == _BUNDLE_DIGEST_DRIFT
    assert _source_digests(_candidate_root())[_REFERENCE_CONFIG_RELATIVE] == hashlib.sha256(
        payloads[_REFERENCE_CONFIG_RELATIVE]
    ).hexdigest()


def test_bundle_assembly_authority_negative_evidence_reused_as_input_is_rejected(
    tmp_path: Path,
) -> None:
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()

    with pytest.raises(
        ReferenceBundleContractError,
        match=_ASSEMBLY_SOURCE_DRIFT,
    ) as exc_info:
        assemble_reference_bundle(
            source_root=evidence_root,
            input_root=tmp_path / "input",
            seed_root=tmp_path / "seed",
            runtime_root=tmp_path / "runtime",
            evidence_root=evidence_root,
        )

    assert exc_info.value.reason_code == _ASSEMBLY_SOURCE_DRIFT


def test_candidate_pytest_collection_is_scoped_away_from_reference_seed() -> None:
    seed_contract_tests = (
        f"{_SEED_RELATIVE}/{_SEED_BASELINE_RELATIVE}",
        f"{_SEED_RELATIVE}/{_SEED_TASK_A_RELATIVE}",
        f"{_SEED_RELATIVE}/{_SEED_TASK_B_RELATIVE}",
    )
    for relative in seed_contract_tests:
        assert _asset_path(relative).is_file()
    with _asset_path("pyproject.toml").open("rb") as pyproject:
        document = tomllib.load(pyproject)
    tool = document.get("tool")
    pytest_table = tool.get("pytest") if isinstance(tool, Mapping) else None
    ini_options = (
        pytest_table.get("ini_options") if isinstance(pytest_table, Mapping) else None
    )
    testpaths = ini_options.get("testpaths") if isinstance(ini_options, Mapping) else None
    assert testpaths == ["tests"]


def test_public_validation_commands_are_self_contained_without_harness_modules() -> None:
    """Public task bytes run pytest directly and never name harness wrappers."""

    public_paths = (
        _INITIAL_TASK_SOURCE_RELATIVE,
        _TASK_B_SOURCE_RELATIVE,
        _TASK_A_SPEC_RELATIVE,
        _TASK_B_SPEC_RELATIVE,
    )
    public = b"".join(_asset_path(relative).read_bytes() for relative in public_paths)
    for wrapper in ("reference_task_a_contract", "reference_task_b_contract"):
        assert wrapper.encode("ascii") not in public

    tasks_text = _asset_path(_INITIAL_TASK_SOURCE_RELATIVE).read_text(encoding="utf-8")
    task_b_text = _asset_path(_TASK_B_SOURCE_RELATIVE).read_text(encoding="utf-8")
    assert tasks_text.count(_TASK_A_COMMAND) == 1
    assert task_b_text.count(_TASK_A_COMMAND) == 1
    assert task_b_text.count(_TASK_B_COMMAND) == 1
    assert task_b_text.index(_TASK_A_COMMAND) < task_b_text.index(_TASK_B_COMMAND)

    tasks = _mapping_from_bytes(
        _asset_path(_INITIAL_TASK_SOURCE_RELATIVE).read_bytes(),
        _TASK_CONTRACT_INVALID,
    )
    task_b = _mapping_from_bytes(
        _asset_path(_TASK_B_SOURCE_RELATIVE).read_bytes(),
        _TASK_CONTRACT_INVALID,
    )
    assert assert_initial_task_source_geometry(tasks) == _TASK_A_COMMAND
    assert assert_task_b_source_geometry(task_b) == (_TASK_A_COMMAND, _TASK_B_COMMAND)

    pyproject = tomllib.loads(
        _asset_path(f"{_SEED_RELATIVE}/pyproject.toml").read_text(encoding="utf-8")
    )
    assert pyproject["tool"]["pytest"]["ini_options"]["pythonpath"] == ["src"]

    manifest = json.loads(_asset_path(_BUNDLE_MANIFEST_RELATIVE).read_bytes())
    entries = manifest["entries"]
    assert len(entries) == 14
    sources = [entry["source"] for entry in entries]
    assert len(sources) == len(set(sources))
    for source in sources:
        assert "reference_task_a_contract" not in source
        assert "reference_task_b_contract" not in source
        data = _asset_path(source).read_bytes()
        assert entry_matches(entries, source, data)

    integration = _asset_path("tests/integration/test_direct_green.py").read_text(
        encoding="utf-8"
    )
    assert "def _install_contract_modules(" not in integration
    assert "reference_task_a_contract" not in integration
    assert "reference_task_b_contract" not in integration


def entry_matches(entries: list[dict[str, Any]], source: str, data: bytes) -> bool:
    for entry in entries:
        if entry["source"] != source:
            continue
        return (
            entry["byte_length"] == len(data)
            and entry["sha256"] == hashlib.sha256(data).hexdigest()
        )
    return False
