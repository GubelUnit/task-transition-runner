"""Public null-or-one task contract.

The loader accepts one YAML document and materializes either ``active_task:
null`` or exactly one fully determined task object. It does not select,
queue, execute, sandbox, or promote work. ``allowed_paths`` are declared
exact relative files for a later promotion gate, not a host isolation
boundary. Identity is a pure fingerprint of the public semantic contract.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml


_SCHEMA_VERSION = "1.0"
_ADAPTER_KIND = "cursor"
_TOP_LEVEL_REQUIRED = frozenset({"schema_version", "active_task"})
_ACTIVE_TASK_REQUIRED = frozenset(
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
_VALIDATION_REQUIRED = frozenset({"commands"})
_COMMAND_REQUIRED = frozenset({"id", "command", "timeout_seconds"})
_ADAPTER_REQUIRED = frozenset({"kind"})
_GREEN_OUTCOME_REQUIRED = frozenset({"kind"})
_RED_OUTCOME_REQUIRED = frozenset({"kind", "command_id", "failure_signature"})
_GLOB_MARKERS = ("*", "?", "[", "]")


class TaskContractError(Exception):
    """Fail-closed violation of the public task contract."""

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
class ValidationCommand:
    id: str
    command: str
    timeout_seconds: int


@dataclass(frozen=True)
class TaskValidation:
    commands: tuple[ValidationCommand, ...]


@dataclass(frozen=True)
class DirectGreenOutcome:
    kind: str


@dataclass(frozen=True)
class ExpectedRedOutcome:
    kind: str
    command_id: str
    failure_signature: str


@dataclass(frozen=True)
class TaskAdapter:
    kind: str


@dataclass(frozen=True)
class ActiveTask:
    task_id: str
    spec_ref: Path
    declared_spec_ref: str
    allowed_paths: tuple[str, ...]
    validation: TaskValidation
    expected_outcome: DirectGreenOutcome | ExpectedRedOutcome
    adapter: TaskAdapter
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class TaskContract:
    schema_version: str
    active_task: ActiveTask | None
    source_root: Path
    fingerprint: str
    source_bytes: bytes


@dataclass(frozen=True)
class PersistedTaskSnapshotIdentity:
    """Identity reproduced solely from exact persisted task-snapshot bytes."""

    task_id: str | None
    fingerprint: str
    snapshot_sha256: str


@dataclass(frozen=True)
class _CanonicalActiveTask:
    task_id: str
    declared_spec_ref: str
    spec_ref: Path | None
    allowed_paths: tuple[str, ...]
    commands: tuple[ValidationCommand, ...]
    expected_outcome: DirectGreenOutcome | ExpectedRedOutcome
    adapter: TaskAdapter
    metadata: dict[Any, Any]
    document: dict[str, Any]


def load_task_contract(
    source_path: Path,
    *,
    target_worktree: Path,
    previous_snapshot: TaskContract | None = None,
) -> TaskContract:
    source_bytes, document_text, source_root = _read_source_document(source_path)
    document = _parse_mapping_document(document_text)
    _reject_unknown_and_missing(document, _TOP_LEVEL_REQUIRED)

    schema_version = _require_str(document["schema_version"])
    if schema_version != _SCHEMA_VERSION:
        raise TaskContractError("TASK_VALUE_CONFLICT")

    target_root = Path(target_worktree).resolve()
    active_raw = document["active_task"]
    if active_raw is None:
        active_task: ActiveTask | None = None
        canonical_active: dict[str, Any] | None = None
    else:
        active_task, canonical_active = _load_active_task(
            active_raw,
            source_root=source_root,
            target_root=target_root,
        )

    contract = TaskContract(
        schema_version=schema_version,
        active_task=active_task,
        source_root=source_root,
        fingerprint=_fingerprint(
            {
                "schema_version": schema_version,
                "active_task": canonical_active,
            }
        ),
        source_bytes=source_bytes,
    )
    _reject_same_id_drift(contract, previous_snapshot)
    return contract


def canonicalize_persisted_task_snapshot(
    snapshot_bytes: bytes,
) -> PersistedTaskSnapshotIdentity:
    """Recompute task identity without consulting mutable source files."""

    if not isinstance(snapshot_bytes, bytes):
        raise TaskContractError("TASK_FIELD_TYPE")
    document = _parse_mapping_document(_decode_source_bytes(snapshot_bytes))
    _reject_unknown_and_missing(document, _TOP_LEVEL_REQUIRED)
    schema_version = _require_str(document["schema_version"])
    if schema_version != _SCHEMA_VERSION:
        raise TaskContractError("TASK_VALUE_CONFLICT")

    active_raw = document["active_task"]
    if active_raw is None:
        task_id = None
        canonical_active: dict[str, Any] | None = None
    else:
        canonical = _canonicalize_active_task(active_raw)
        task_id = canonical.task_id
        canonical_active = canonical.document
    fingerprint = _fingerprint(
        {
            "schema_version": schema_version,
            "active_task": canonical_active,
        }
    )
    return PersistedTaskSnapshotIdentity(
        task_id=task_id,
        fingerprint=fingerprint,
        snapshot_sha256=hashlib.sha256(snapshot_bytes).hexdigest(),
    )


def _read_source_document(source_path: Path) -> tuple[bytes, str, Path]:
    resolved = Path(source_path).resolve()
    if not resolved.is_file():
        raise TaskContractError("TASK_SOURCE_MISSING")
    try:
        source_bytes = resolved.read_bytes()
    except PermissionError:
        raise TaskContractError("TASK_SOURCE_UNREADABLE") from None
    return source_bytes, _decode_source_bytes(source_bytes), resolved.parent


def _decode_source_bytes(source_bytes: bytes) -> str:
    try:
        return source_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raise TaskContractError("TASK_FIELD_TYPE") from None


def _parse_mapping_document(document_text: str) -> dict[Any, Any]:
    try:
        loaded = yaml.load(document_text, Loader=_StrictLoader)
    except _DuplicateKeyError:
        raise TaskContractError("TASK_VALUE_CONFLICT") from None
    except yaml.YAMLError:
        raise TaskContractError("TASK_FIELD_TYPE") from None
    if not isinstance(loaded, dict):
        raise TaskContractError("TASK_FIELD_TYPE")
    return loaded


def _load_active_task(
    active_raw: object,
    *,
    source_root: Path,
    target_root: Path,
) -> tuple[ActiveTask, dict[str, Any]]:
    canonical = _canonicalize_active_task(
        active_raw,
        target_root=target_root,
        source_root=source_root,
    )
    assert canonical.spec_ref is not None
    return (
        ActiveTask(
            task_id=canonical.task_id,
            spec_ref=canonical.spec_ref,
            declared_spec_ref=canonical.declared_spec_ref,
            allowed_paths=canonical.allowed_paths,
            validation=TaskValidation(commands=canonical.commands),
            expected_outcome=canonical.expected_outcome,
            adapter=canonical.adapter,
            metadata=_freeze_json_value(canonical.metadata),
        ),
        canonical.document,
    )


def _canonicalize_active_task(
    active_raw: object,
    *,
    target_root: Path | None = None,
    source_root: Path | None = None,
) -> _CanonicalActiveTask:
    active = _require_mapping(active_raw)
    _reject_unknown_and_missing(active, _ACTIVE_TASK_REQUIRED)

    task_id = _require_non_blank_str(active["task_id"])
    declared_spec_ref = _require_non_empty_str(active["spec_ref"])
    if source_root is None:
        _validated_spec_ref(declared_spec_ref)
        spec_ref = None
    else:
        spec_ref = _resolve_spec_ref(source_root, declared_spec_ref)
    allowed_paths = _load_allowed_paths(active["allowed_paths"], target_root=target_root)
    commands = _load_validation_commands(active["validation"])
    expected_outcome, canonical_outcome = _load_expected_outcome(
        active["expected_outcome"],
        command_ids=frozenset(command.id for command in commands),
    )
    adapter = _load_adapter(active["adapter"])
    metadata_canonical = _canonical_json_mapping(active["metadata"])

    return _CanonicalActiveTask(
        task_id=task_id,
        declared_spec_ref=declared_spec_ref,
        spec_ref=spec_ref,
        allowed_paths=allowed_paths,
        commands=commands,
        expected_outcome=expected_outcome,
        adapter=adapter,
        metadata=metadata_canonical,
        document={
            "adapter": {"kind": adapter.kind},
            "allowed_paths": list(allowed_paths),
            "expected_outcome": canonical_outcome,
            "metadata": metadata_canonical,
            "spec_ref": declared_spec_ref,
            "task_id": task_id,
            "validation": {
                "commands": [
                    {
                        "command": command.command,
                        "id": command.id,
                        "timeout_seconds": command.timeout_seconds,
                    }
                    for command in commands
                ]
            },
        },
    )


def _load_validation_commands(validation_raw: object) -> tuple[ValidationCommand, ...]:
    validation = _require_mapping(validation_raw)
    _reject_unknown_and_missing(validation, _VALIDATION_REQUIRED)
    commands_raw = validation["commands"]
    if not isinstance(commands_raw, list):
        raise TaskContractError("TASK_FIELD_TYPE")
    if commands_raw == []:
        raise TaskContractError("TASK_VALUE_CONFLICT")

    commands: list[ValidationCommand] = []
    seen_ids: set[str] = set()
    for entry in commands_raw:
        command = _load_validation_command(entry)
        if command.id in seen_ids:
            raise TaskContractError("TASK_VALUE_CONFLICT")
        seen_ids.add(command.id)
        commands.append(command)
    return tuple(commands)


def _load_validation_command(entry: object) -> ValidationCommand:
    command_raw = _require_mapping(entry)
    _reject_unknown_and_missing(command_raw, _COMMAND_REQUIRED)
    command_id = _require_non_blank_str(command_raw["id"])
    command_text = _require_non_blank_str(command_raw["command"])
    timeout_seconds = _require_positive_int(command_raw["timeout_seconds"])
    return ValidationCommand(
        id=command_id,
        command=command_text,
        timeout_seconds=timeout_seconds,
    )


def _load_expected_outcome(
    outcome_raw: object,
    *,
    command_ids: frozenset[str],
) -> tuple[DirectGreenOutcome | ExpectedRedOutcome, dict[str, Any]]:
    outcome = _require_mapping(outcome_raw)
    if "kind" not in outcome:
        _reject_unknown_and_missing(outcome, _GREEN_OUTCOME_REQUIRED)
    kind = _require_str(outcome["kind"])
    if kind == "GREEN":
        _reject_unknown_and_missing(outcome, _GREEN_OUTCOME_REQUIRED)
        return DirectGreenOutcome(kind=kind), {"kind": kind}
    if kind == "EXPECTED_RED":
        _reject_unknown_and_missing(outcome, _RED_OUTCOME_REQUIRED)
        command_id = _require_non_blank_str(outcome["command_id"])
        failure_signature = _require_str(outcome["failure_signature"])
        if command_id not in command_ids:
            raise TaskContractError("TASK_VALUE_CONFLICT")
        if failure_signature.strip() == "":
            raise TaskContractError("TASK_VALUE_CONFLICT")
        return (
            ExpectedRedOutcome(
                kind=kind,
                command_id=command_id,
                failure_signature=failure_signature,
            ),
            {
                "command_id": command_id,
                "failure_signature": failure_signature,
                "kind": kind,
            },
        )
    raise TaskContractError("TASK_VALUE_CONFLICT")


def _load_adapter(adapter_raw: object) -> TaskAdapter:
    adapter = _require_mapping(adapter_raw)
    _reject_unknown_and_missing(adapter, _ADAPTER_REQUIRED)
    kind = _require_str(adapter["kind"])
    if kind != _ADAPTER_KIND:
        raise TaskContractError("TASK_VALUE_CONFLICT")
    return TaskAdapter(kind=kind)


def _canonical_json_mapping(metadata_raw: object) -> dict[Any, Any]:
    if not isinstance(metadata_raw, dict):
        raise TaskContractError("TASK_FIELD_TYPE")
    canonical = copy.deepcopy(metadata_raw)
    _reject_non_string_metadata_keys(canonical)
    try:
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        raise TaskContractError("TASK_FIELD_TYPE") from None
    return canonical


def _reject_non_string_metadata_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TaskContractError("TASK_FIELD_TYPE")
            _reject_non_string_metadata_keys(item)
        return
    if isinstance(value, list):
        for item in value:
            _reject_non_string_metadata_keys(item)


def _freeze_json_value(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json_value(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json_value(item) for item in value)
    return value


def _load_allowed_paths(
    paths_raw: object,
    *,
    target_root: Path | None,
) -> tuple[str, ...]:
    if not isinstance(paths_raw, list):
        raise TaskContractError("TASK_FIELD_TYPE")
    if paths_raw == []:
        raise TaskContractError("TASK_VALUE_CONFLICT")

    allowed: list[str] = []
    seen: set[str] = set()
    for entry in paths_raw:
        relative = _validated_allowed_path(entry, target_root=target_root)
        if relative in seen:
            raise TaskContractError("TASK_VALUE_CONFLICT")
        seen.add(relative)
        allowed.append(relative)
    return tuple(allowed)


def _validated_allowed_path(raw: object, *, target_root: Path | None) -> str:
    if not isinstance(raw, str):
        raise TaskContractError("TASK_FIELD_TYPE")
    if (
        raw == ""
        or raw in {".", "./"}
        or raw.endswith("/")
        or any(marker in raw for marker in _GLOB_MARKERS)
        or Path(raw).is_absolute()
    ):
        raise TaskContractError("TASK_VALUE_CONFLICT")
    if _has_relative_normalization_drift(raw):
        raise TaskContractError("TASK_PATH_TRAVERSAL")
    if target_root is None:
        return raw

    candidate = (target_root / raw).resolve()
    if not candidate.is_relative_to(target_root):
        raise TaskContractError("TASK_PATH_ESCAPE")
    if candidate.is_dir():
        raise TaskContractError("TASK_VALUE_CONFLICT")
    return raw


def _resolve_spec_ref(source_root: Path, raw: str) -> Path:
    _validated_spec_ref(raw)
    spec_path = (source_root / raw).resolve()
    if not spec_path.is_file():
        raise TaskContractError("TASK_SOURCE_MISSING")
    if not spec_path.is_relative_to(source_root):
        raise TaskContractError("TASK_PATH_ESCAPE")
    return spec_path


def _validated_spec_ref(raw: str) -> None:
    if Path(raw).is_absolute():
        raise TaskContractError("TASK_VALUE_CONFLICT")
    declared = raw[2:] if raw.startswith("./") else raw
    if _has_relative_normalization_drift(declared):
        raise TaskContractError("TASK_PATH_TRAVERSAL")


def _has_relative_normalization_drift(raw: str) -> bool:
    declared = Path(raw)
    return declared.as_posix() != raw or any(part in {".", ".."} for part in declared.parts)


def _reject_same_id_drift(
    current: TaskContract,
    previous: TaskContract | None,
) -> None:
    if previous is None:
        return
    previous_task = previous.active_task
    current_task = current.active_task
    if previous_task is None or current_task is None:
        return
    if previous_task.task_id != current_task.task_id:
        return
    if previous.fingerprint != current.fingerprint:
        raise TaskContractError("TASK_VALUE_CONFLICT")


def _fingerprint(canonical: Mapping[str, Any]) -> str:
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _reject_unknown_and_missing(
    mapping: Mapping[Any, Any],
    required: frozenset[str],
) -> None:
    if set(mapping) - required:
        raise TaskContractError("TASK_FIELD_UNKNOWN")
    if required - set(mapping):
        raise TaskContractError("TASK_FIELD_MISSING")


def _require_mapping(value: object) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise TaskContractError("TASK_FIELD_TYPE")
    return value


def _require_str(value: object) -> str:
    if not isinstance(value, str):
        raise TaskContractError("TASK_FIELD_TYPE")
    return value


def _require_non_empty_str(value: object) -> str:
    text = _require_str(value)
    if text == "":
        raise TaskContractError("TASK_VALUE_CONFLICT")
    return text


def _require_non_blank_str(value: object) -> str:
    text = _require_str(value)
    if text.strip() == "":
        raise TaskContractError("TASK_VALUE_CONFLICT")
    return text


def _require_positive_int(value: object) -> int:
    if type(value) is not int:
        raise TaskContractError("TASK_FIELD_TYPE")
    if value <= 0:
        raise TaskContractError("TASK_VALUE_CONFLICT")
    return value
