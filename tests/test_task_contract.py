"""Synthetic public null-or-one task contract.

The public surface is intentionally small and explicit:

- ``load_task_contract`` reads one YAML file and fail-closed validates it.
- ``TaskContract`` is the loaded canonical value object.
- ``TaskContractError.reason_code`` is the stable ``TASK_*`` failure token.
- ``active_task`` is exactly ``None`` or one fully determined task object.
- ``allowed_paths`` are declared exact relative files for a later promotion
  gate. The loader does not sandbox workers, commands, or the host.
- Identity is a pure fingerprint. YAML key order and formatting must not
  change it. Same-``task_id`` content drift against a caller-provided
  snapshot is blocked. Unsettled-epoch detection is not this contract.
"""

from __future__ import annotations

import copy
import hashlib
import inspect
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.task_contract import (
    ActiveTask,
    PersistedTaskSnapshotIdentity,
    TaskContract,
    TaskContractError,
    canonicalize_persisted_task_snapshot,
    load_task_contract,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_SPEC_REF = "./tasks/synthetic-task-001.md"
_REFERENCE_ALLOWED_PATHS = ("src/example.py", "tests/test_example.py")
_REFERENCE_COMMAND_ID = "tests"
_REFERENCE_COMMAND = "python -m pytest -q"
_REFERENCE_TIMEOUT_SECONDS = 300
_REFERENCE_ADAPTER_KIND = "cursor"
_REFERENCE_FAILURE_SIGNATURE = "synthetic failure signature"


def _null_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": None,
    }


def _active_task_body() -> dict[str, Any]:
    return {
        "task_id": _REFERENCE_TASK_ID,
        "spec_ref": _REFERENCE_SPEC_REF,
        "allowed_paths": list(_REFERENCE_ALLOWED_PATHS),
        "validation": {
            "commands": [
                {
                    "id": _REFERENCE_COMMAND_ID,
                    "command": _REFERENCE_COMMAND,
                    "timeout_seconds": _REFERENCE_TIMEOUT_SECONDS,
                }
            ]
        },
        "expected_outcome": {"kind": "GREEN"},
        "adapter": {"kind": _REFERENCE_ADAPTER_KIND},
        "metadata": {},
    }


def _active_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": _active_task_body(),
    }


def _expected_red_payload() -> dict[str, Any]:
    payload = _active_payload()
    payload["active_task"]["expected_outcome"] = {
        "kind": "EXPECTED_RED",
        "command_id": _REFERENCE_COMMAND_ID,
        "failure_signature": _REFERENCE_FAILURE_SIGNATURE,
    }
    return payload


def _is_safe_relative_spec(raw: object) -> bool:
    if not isinstance(raw, str) or raw == "":
        return False
    declared = Path(raw)
    if declared.is_absolute() or ".." in declared.parts:
        return False
    if raw.endswith("/") or any(marker in raw for marker in ("*", "?", "[", "]")):
        return False
    return True


def _write_spec(source_root: Path, relative: str = _REFERENCE_SPEC_REF) -> Path:
    declared = Path(relative)
    spec_path = (source_root / declared).resolve()
    if not spec_path.is_relative_to(source_root.resolve()):
        raise AssertionError("synthetic spec would escape the fixture root")
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text("synthetic spec\n", encoding="utf-8")
    return spec_path


def _write_task_document(
    root: Path,
    payload: dict[str, Any],
    *,
    filename: str = "tasks.yaml",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    source_path = root / filename
    source_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return source_path


def _write_null_tree(root: Path) -> tuple[Path, Path]:
    target = root / "fixture-repo"
    target.mkdir(parents=True, exist_ok=True)
    source_path = _write_task_document(root, _null_payload())
    return source_path, target


def _write_active_tree(
    root: Path,
    payload: dict[str, Any] | None = None,
) -> tuple[Path, Path, Path]:
    document = _active_payload() if payload is None else copy.deepcopy(payload)
    target = root / "fixture-repo"
    target.mkdir(parents=True, exist_ok=True)
    spec_ref = document.get("active_task", {}).get("spec_ref") if isinstance(document.get("active_task"), dict) else None
    spec_path = (
        _write_spec(root, spec_ref)
        if _is_safe_relative_spec(spec_ref)
        else _write_spec(root, _REFERENCE_SPEC_REF)
    )
    source_path = _write_task_document(root, document)
    return source_path, target, spec_path


def _overwrite_source(source_path: Path, text: str) -> Path:
    source_path.write_text(text, encoding="utf-8")
    return source_path


def _drop_field(payload: dict[str, Any], dotted: str) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    keys = dotted.split(".")
    cursor: Any = clone
    for key in keys[:-1]:
        cursor = cursor[key]
    del cursor[keys[-1]]
    return clone


def _set_field(payload: dict[str, Any], dotted: str, value: Any) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    keys = dotted.split(".")
    cursor: Any = clone
    for key in keys[:-1]:
        cursor = cursor[key]
    cursor[keys[-1]] = value
    return clone


def _load(
    source_path: Path,
    target_worktree: Path,
    previous_snapshot: TaskContract | None = None,
) -> TaskContract:
    if previous_snapshot is None:
        return load_task_contract(source_path, target_worktree=target_worktree)
    return load_task_contract(
        source_path,
        target_worktree=target_worktree,
        previous_snapshot=previous_snapshot,
    )


def _expect_reason(
    source_path: Path,
    target_worktree: Path,
    reason_code: str,
    previous_snapshot: TaskContract | None = None,
) -> TaskContractError:
    with pytest.raises(TaskContractError, match=reason_code) as exc_info:
        _load(source_path, target_worktree, previous_snapshot)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _composed_absolute(*segments: str) -> str:
    return "/" + "/".join(segments)


def test_null_active_task_is_the_terminal_rest_form(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    loaded = _load(source_path, target)

    assert isinstance(loaded, TaskContract)
    assert loaded.schema_version == _REFERENCE_SCHEMA_VERSION
    assert loaded.active_task is None
    assert loaded.source_root == source_path.resolve().parent
    assert isinstance(loaded.fingerprint, str)
    assert loaded.fingerprint != ""


def test_null_form_does_not_invent_a_default_task(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    loaded = _load(source_path, target)

    assert loaded.active_task is None
    assert "task_id" not in yaml.safe_load(source_path.read_text(encoding="utf-8"))


def test_single_active_task_exposes_the_ratified_public_fields(
    tmp_path: Path,
) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    loaded = _load(source_path, target)
    task = loaded.active_task

    assert isinstance(loaded, TaskContract)
    assert isinstance(task, ActiveTask)
    assert loaded.schema_version == _REFERENCE_SCHEMA_VERSION
    assert task.task_id == _REFERENCE_TASK_ID
    assert task.spec_ref == spec_path
    assert task.spec_ref.is_relative_to(loaded.source_root)
    assert task.allowed_paths == _REFERENCE_ALLOWED_PATHS
    assert task.validation.commands[0].id == _REFERENCE_COMMAND_ID
    assert task.validation.commands[0].command == _REFERENCE_COMMAND
    assert task.validation.commands[0].timeout_seconds == _REFERENCE_TIMEOUT_SECONDS
    assert task.expected_outcome.kind == "GREEN"
    assert getattr(task.expected_outcome, "command_id", None) is None
    assert getattr(task.expected_outcome, "failure_signature", None) is None
    assert task.adapter.kind == _REFERENCE_ADAPTER_KIND
    assert task.metadata == {}
    assert loaded.source_root == tmp_path.resolve()


def test_declared_spec_ref_is_the_unnormalized_yaml_scalar(tmp_path: Path) -> None:
    payload = _set_field(
        _active_payload(),
        "active_task.spec_ref",
        "tasks/synthetic-task-001.md",
    )
    source_path, target, spec_path = _write_active_tree(tmp_path, payload)
    loaded = _load(source_path, target)
    task = loaded.active_task

    assert task is not None
    assert task.declared_spec_ref == "tasks/synthetic-task-001.md"
    assert isinstance(task.declared_spec_ref, str)
    assert task.spec_ref == spec_path
    assert task.declared_spec_ref != str(task.spec_ref)


def test_declared_spec_ref_stays_distinct_from_symlink_resolved_path(
    tmp_path: Path,
) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    canonical = tmp_path / "tasks" / "canonical-spec.md"
    canonical.write_text("canonical spec\n", encoding="utf-8")
    spec_path.unlink()
    spec_path.symlink_to(canonical)
    loaded = _load(source_path, target)
    task = loaded.active_task

    assert task is not None
    assert task.declared_spec_ref == _REFERENCE_SPEC_REF
    assert task.spec_ref == canonical.resolve()
    assert task.declared_spec_ref != str(task.spec_ref)


def test_task_contract_preserves_the_once_read_source_bytes(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(tmp_path)
    original = source_path.read_bytes()
    loaded = _load(source_path, target)
    source_path.write_bytes(b"mutated-after-load\n")

    assert loaded.source_bytes == original
    assert isinstance(loaded.source_bytes, bytes)
    with pytest.raises(AttributeError):
        loaded.source_bytes = b"replaced"  # type: ignore[misc]


def test_persisted_task_snapshot_canonicalization_is_pure_and_matches_loader(
    tmp_path: Path,
) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    snapshot_bytes = source_path.read_bytes()
    loaded = _load(source_path, target)
    source_path.unlink()
    spec_path.unlink()
    target.rmdir()

    identity = canonicalize_persisted_task_snapshot(snapshot_bytes)

    assert isinstance(identity, PersistedTaskSnapshotIdentity)
    assert identity.task_id == _REFERENCE_TASK_ID
    assert identity.fingerprint == loaded.fingerprint
    assert identity.snapshot_sha256 == hashlib.sha256(snapshot_bytes).hexdigest()


def test_a5_current_successor_bytes_cannot_replace_predecessor_snapshot_identity(
    tmp_path: Path,
) -> None:
    source_path, target, _spec_path = _write_active_tree(tmp_path)
    predecessor_bytes = source_path.read_bytes()
    predecessor = _load(source_path, target)
    successor_payload = _set_field(
        _active_payload(),
        "active_task.task_id",
        "synthetic-task-002",
    )
    source_path.write_text(
        yaml.safe_dump(successor_payload, sort_keys=False),
        encoding="utf-8",
    )

    reproved_predecessor = canonicalize_persisted_task_snapshot(predecessor_bytes)
    current_successor = canonicalize_persisted_task_snapshot(source_path.read_bytes())

    assert reproved_predecessor.task_id == _REFERENCE_TASK_ID
    assert reproved_predecessor.fingerprint == predecessor.fingerprint
    assert reproved_predecessor.snapshot_sha256 == hashlib.sha256(
        predecessor_bytes
    ).hexdigest()
    assert current_successor.task_id == "synthetic-task-002"
    assert current_successor.fingerprint != reproved_predecessor.fingerprint
    assert current_successor.snapshot_sha256 != reproved_predecessor.snapshot_sha256


def test_persisted_task_snapshot_keeps_semantic_identity_but_hashes_exact_bytes(
    tmp_path: Path,
) -> None:
    first_source, _target, _spec = _write_active_tree(tmp_path)
    first_bytes = first_source.read_bytes()
    payload = yaml.safe_load(first_bytes)
    second_bytes = (yaml.safe_dump(payload, sort_keys=True) + "\n\n").encode("utf-8")

    first = canonicalize_persisted_task_snapshot(first_bytes)
    second = canonicalize_persisted_task_snapshot(second_bytes)

    assert first.task_id == second.task_id == _REFERENCE_TASK_ID
    assert first.fingerprint == second.fingerprint
    assert first.snapshot_sha256 != second.snapshot_sha256


def test_null_persisted_task_snapshot_has_no_task_identity() -> None:
    snapshot_bytes = b'schema_version: "1.0"\nactive_task: null\n'

    identity = canonicalize_persisted_task_snapshot(snapshot_bytes)

    assert identity.task_id is None
    assert identity.snapshot_sha256 == hashlib.sha256(snapshot_bytes).hexdigest()


@pytest.mark.parametrize(
    "snapshot_bytes",
    (
        b"\xff",
        b"schema_version: [unterminated\n",
        b'schema_version: "2.0"\nactive_task: null\n',
        b'schema_version: "1.0"\nactive_task:\n  task_id: only\n',
        (
            b'schema_version: "1.0"\n'
            b"active_task:\n"
            b"  task_id: synthetic-task-001\n"
            b"  spec_ref: ../outside.md\n"
            b"  allowed_paths: [src/example.py]\n"
            b"  validation:\n"
            b"    commands:\n"
            b"      - id: tests\n"
            b"        command: python -m pytest -q\n"
            b"        timeout_seconds: 300\n"
            b"  expected_outcome: {kind: GREEN}\n"
            b"  adapter: {kind: cursor}\n"
            b"  metadata: {}\n"
        ),
    ),
)
def test_malformed_persisted_task_snapshot_fails_closed(
    snapshot_bytes: bytes,
) -> None:
    with pytest.raises(TaskContractError):
        canonicalize_persisted_task_snapshot(snapshot_bytes)


def test_persisted_task_snapshot_requires_exact_bytes_type() -> None:
    with pytest.raises(TaskContractError, match="TASK_FIELD_TYPE"):
        canonicalize_persisted_task_snapshot(bytearray(b"snapshot"))  # type: ignore[arg-type]


def test_null_contract_preserves_the_once_read_source_bytes(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    original = source_path.read_bytes()
    loaded = _load(source_path, target)
    source_path.write_bytes(b"mutated-after-load\n")

    assert loaded.active_task is None
    assert loaded.source_bytes == original


def test_direct_green_and_expected_red_are_distinct_outcome_variants(
    tmp_path: Path,
) -> None:
    green_source, green_target, _ = _write_active_tree(tmp_path / "green")
    red_source, red_target, _ = _write_active_tree(
        tmp_path / "red",
        _expected_red_payload(),
    )

    green = _load(green_source, green_target)
    red = _load(red_source, red_target)

    assert green.active_task is not None
    assert red.active_task is not None
    assert green.active_task.expected_outcome.kind == "GREEN"
    assert red.active_task.expected_outcome.kind == "EXPECTED_RED"
    assert red.active_task.expected_outcome.command_id == _REFERENCE_COMMAND_ID
    assert red.active_task.expected_outcome.failure_signature == (
        _REFERENCE_FAILURE_SIGNATURE
    )
    assert green.fingerprint != red.fingerprint


def test_expected_red_command_id_must_name_one_declared_command(
    tmp_path: Path,
) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["validation"]["commands"].append(
        {
            "id": "lint",
            "command": "python -m compileall src",
            "timeout_seconds": 60,
        }
    )
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    loaded = _load(source_path, target)

    assert loaded.active_task is not None
    command_ids = [item.id for item in loaded.active_task.validation.commands]
    assert loaded.active_task.expected_outcome.command_id in command_ids
    assert loaded.active_task.expected_outcome.command_id == _REFERENCE_COMMAND_ID


def test_metadata_is_preserved_and_not_interpreted(tmp_path: Path) -> None:
    payload = _set_field(
        _active_payload(),
        "active_task.metadata",
        {"label": "synthetic-note", "nested": {"flag": True, "count": 2}},
    )
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    loaded = _load(source_path, target)

    assert loaded.active_task is not None
    assert loaded.active_task.metadata == {
        "label": "synthetic-note",
        "nested": {"flag": True, "count": 2},
    }
    assert not hasattr(loaded.active_task, "planner")
    assert not hasattr(loaded.active_task, "persona")
    assert not hasattr(loaded.active_task, "ack")
    assert not hasattr(loaded, "run_id")


def test_metadata_is_recursively_immutable(tmp_path: Path) -> None:
    payload = _set_field(
        _active_payload(),
        "active_task.metadata",
        {
            "label": "synthetic-note",
            "nested": {"flag": True, "count": 2},
            "tags": ["alpha", {"inner": "value"}],
        },
    )
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    loaded = _load(source_path, target)

    assert loaded.active_task is not None
    metadata = loaded.active_task.metadata
    assert metadata["tags"] == ("alpha", {"inner": "value"})
    with pytest.raises(TypeError):
        metadata["label"] = "changed"
    with pytest.raises(TypeError):
        metadata["nested"]["flag"] = False
    with pytest.raises(TypeError):
        metadata["tags"][0] = "beta"


def test_non_json_canonical_metadata_is_blocked(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.metadata", {"blob": b"hello"}),
    )
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


@pytest.mark.parametrize(
    "metadata",
    (
        {1: "value"},
        {"nested": {1: "value"}},
        {"items": [{1: "value"}]},
    ),
)
def test_non_string_metadata_keys_are_blocked(
    tmp_path: Path,
    metadata: dict[str | int, object],
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.metadata", metadata),
    )
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


@pytest.mark.parametrize("value", (float("nan"), float("inf"), float("-inf")))
def test_non_finite_metadata_floats_are_blocked(tmp_path: Path, value: float) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.metadata", {"n": value}),
    )
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


def test_allowed_paths_need_not_exist_because_they_are_not_a_sandbox(
    tmp_path: Path,
) -> None:
    source_path, target, _ = _write_active_tree(tmp_path)
    loaded = _load(source_path, target)

    assert loaded.active_task is not None
    for relative in loaded.active_task.allowed_paths:
        assert not (target / relative).exists()
    assert not hasattr(loaded, "sandbox")
    assert not hasattr(loaded.active_task, "sandbox")
    assert not hasattr(load_task_contract, "sandbox")


def test_public_loader_has_no_queue_planner_or_universal_run_id() -> None:
    parameters = inspect.signature(load_task_contract).parameters

    assert "target_worktree" in parameters
    assert "previous_snapshot" in parameters
    assert "run_id" not in parameters
    assert "queue" not in parameters
    assert "planner" not in parameters
    assert "priority" not in parameters
    assert "sandbox" not in parameters


def test_source_root_follows_symlink_resolved_task_file(tmp_path: Path) -> None:
    real_root = tmp_path / "real-source"
    source_path, target, spec_path = _write_active_tree(real_root)
    link_dir = tmp_path / "link-dir"
    link_dir.mkdir()
    linked_source = link_dir / "tasks.yaml"
    linked_source.symlink_to(source_path)

    loaded = _load(linked_source, target)

    assert loaded.source_root == real_root.resolve()
    assert loaded.active_task is not None
    assert loaded.active_task.spec_ref == spec_path
    assert loaded.active_task.spec_ref.is_relative_to(loaded.source_root)


def test_fingerprint_ignores_yaml_key_order_and_formatting(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_source, first_target, _ = _write_active_tree(first_root)
    payload = yaml.safe_load(first_source.read_text(encoding="utf-8"))
    second_source, second_target, _ = _write_active_tree(second_root, payload)
    _overwrite_source(
        second_source,
        yaml.safe_dump(payload, sort_keys=True) + "\n\n",
    )

    first = _load(first_source, first_target)
    second = _load(second_source, second_target)

    assert first.fingerprint == second.fingerprint
    assert first.active_task is not None
    assert second.active_task is not None
    assert first.active_task.task_id == second.active_task.task_id


def test_null_fingerprint_is_stable_across_formatting(tmp_path: Path) -> None:
    first_source, first_target = _write_null_tree(tmp_path / "first")
    second_source, second_target = _write_null_tree(tmp_path / "second")
    _overwrite_source(second_source, 'active_task: null\nschema_version: "1.0"\n')

    first = _load(first_source, first_target)
    second = _load(second_source, second_target)

    assert first.fingerprint == second.fingerprint
    assert first.active_task is None
    assert second.active_task is None


def test_same_task_id_content_drift_against_snapshot_is_blocked(
    tmp_path: Path,
) -> None:
    original_source, original_target, _ = _write_active_tree(tmp_path / "original")
    previous = _load(original_source, original_target)

    drifted = _set_field(
        _active_payload(),
        "active_task.spec_ref",
        "./tasks/drifted-spec.md",
    )
    drifted_source, drifted_target, _ = _write_active_tree(tmp_path / "drifted", drifted)
    _expect_reason(
        drifted_source,
        drifted_target,
        "TASK_VALUE_CONFLICT",
        previous_snapshot=previous,
    )


def test_identical_snapshot_of_the_same_task_id_is_accepted(tmp_path: Path) -> None:
    first_source, first_target, _ = _write_active_tree(tmp_path / "first")
    previous = _load(first_source, first_target)
    second_source, second_target, _ = _write_active_tree(tmp_path / "second")

    loaded = _load(second_source, second_target, previous_snapshot=previous)

    assert loaded.fingerprint == previous.fingerprint
    assert loaded.active_task is not None
    assert loaded.active_task.task_id == previous.active_task.task_id


def test_metadata_drift_of_the_same_task_id_is_blocked(tmp_path: Path) -> None:
    original_source, original_target, _ = _write_active_tree(tmp_path / "original")
    previous = _load(original_source, original_target)
    drifted = _set_field(
        _active_payload(),
        "active_task.metadata",
        {"label": "changed"},
    )
    drifted_source, drifted_target, _ = _write_active_tree(tmp_path / "drifted", drifted)
    _expect_reason(
        drifted_source,
        drifted_target,
        "TASK_VALUE_CONFLICT",
        previous_snapshot=previous,
    )


def test_different_task_id_with_changed_content_is_not_snapshot_drift(
    tmp_path: Path,
) -> None:
    original_source, original_target, _ = _write_active_tree(tmp_path / "original")
    previous = _load(original_source, original_target)
    successor = _set_field(_active_payload(), "active_task.task_id", "synthetic-task-002")
    successor = _set_field(successor, "active_task.spec_ref", "./tasks/successor.md")
    successor_source, successor_target, _ = _write_active_tree(
        tmp_path / "successor",
        successor,
    )

    loaded = _load(successor_source, successor_target, previous_snapshot=previous)

    assert previous.active_task is not None
    assert loaded.active_task is not None
    assert loaded.active_task.task_id == "synthetic-task-002"
    assert loaded.active_task.task_id != previous.active_task.task_id
    assert loaded.fingerprint != previous.fingerprint


def test_null_to_active_against_snapshot_is_not_same_id_drift(tmp_path: Path) -> None:
    null_source, null_target = _write_null_tree(tmp_path / "rest")
    previous = _load(null_source, null_target)
    active_source, active_target, _ = _write_active_tree(tmp_path / "active")

    loaded = _load(active_source, active_target, previous_snapshot=previous)

    assert previous.active_task is None
    assert loaded.active_task is not None
    assert loaded.fingerprint != previous.fingerprint


def test_active_to_null_against_snapshot_is_not_same_id_drift(tmp_path: Path) -> None:
    active_source, active_target, _ = _write_active_tree(tmp_path / "active")
    previous = _load(active_source, active_target)
    null_source, null_target = _write_null_tree(tmp_path / "rest")

    loaded = _load(null_source, null_target, previous_snapshot=previous)

    assert previous.active_task is not None
    assert loaded.active_task is None
    assert loaded.fingerprint != previous.fingerprint


@pytest.mark.parametrize(
    "dotted_field",
    (
        "schema_version",
        "active_task",
        "active_task.task_id",
        "active_task.spec_ref",
        "active_task.allowed_paths",
        "active_task.validation",
        "active_task.validation.commands",
        "active_task.expected_outcome",
        "active_task.expected_outcome.kind",
        "active_task.adapter",
        "active_task.adapter.kind",
        "active_task.metadata",
    ),
)
def test_missing_required_field_is_blocked(tmp_path: Path, dotted_field: str) -> None:
    if dotted_field in {"schema_version", "active_task"}:
        source_path = _write_task_document(
            tmp_path,
            _drop_field(_null_payload(), dotted_field),
        )
        target = tmp_path / "fixture-repo"
        target.mkdir(parents=True, exist_ok=True)
    else:
        source_path, target, _ = _write_active_tree(
            tmp_path,
            _drop_field(_active_payload(), dotted_field),
        )
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


def test_missing_expected_red_failure_fields_are_blocked(tmp_path: Path) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["expected_outcome"] = {"kind": "EXPECTED_RED"}
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


@pytest.mark.parametrize(
    "dotted_field",
    (
        "active_task.expected_outcome.command_id",
        "active_task.expected_outcome.failure_signature",
    ),
)
def test_missing_individual_expected_red_field_is_blocked(
    tmp_path: Path,
    dotted_field: str,
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _drop_field(_expected_red_payload(), dotted_field),
    )
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


def test_empty_mapping_document_is_blocked(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    _overwrite_source(source_path, "{}\n")
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


def test_missing_validation_command_id_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    del payload["active_task"]["validation"]["commands"][0]["id"]
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


def test_missing_validation_command_text_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    del payload["active_task"]["validation"]["commands"][0]["command"]
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


def test_missing_validation_command_timeout_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    del payload["active_task"]["validation"]["commands"][0]["timeout_seconds"]
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_MISSING")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("unexpected", True),
        ("queue", []),
        ("planner", {}),
        ("run_id", "synthetic-run"),
        ("active_task.priority", 1),
        ("active_task.queue", []),
        ("active_task.planner", {}),
        ("active_task.run_id", "synthetic-run"),
        ("active_task.status", "ready"),
        ("active_task.persona", "developer"),
        ("active_task.ack", True),
        ("active_task.extra", "ignored"),
        ("active_task.validation.cwd", "."),
        ("active_task.adapter.executable", "cursor-agent"),
        ("active_task.adapter.model", None),
        ("active_task.expected_outcome.command_id", _REFERENCE_COMMAND_ID),
        ("active_task.expected_outcome.failure_signature", "leftover"),
    ),
)
def test_unknown_field_is_blocked(
    tmp_path: Path,
    dotted_field: str,
    value: Any,
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), dotted_field, value),
    )
    _expect_reason(source_path, target, "TASK_FIELD_UNKNOWN")


def test_unknown_validation_command_field_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["shell"] = "bash"
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_UNKNOWN")


def test_unknown_expected_red_field_is_blocked(tmp_path: Path) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["expected_outcome"]["retry"] = True
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_UNKNOWN")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("schema_version", 1.0),
        ("schema_version", 1),
        ("active_task", "synthetic-task-001"),
        ("active_task", []),
        ("active_task", [_active_task_body()]),
        ("active_task.task_id", 1),
        ("active_task.spec_ref", ["./tasks/synthetic-task-001.md"]),
        ("active_task.allowed_paths", "src/example.py"),
        ("active_task.allowed_paths", {"src/example.py": True}),
        ("active_task.validation", ["./validate.sh"]),
        ("active_task.validation.commands", {"id": "tests"}),
        ("active_task.expected_outcome", "GREEN"),
        ("active_task.expected_outcome.kind", True),
        ("active_task.adapter", "cursor"),
        ("active_task.adapter.kind", ["cursor"]),
        ("active_task.metadata", []),
        ("active_task.metadata", "opaque"),
    ),
)
def test_wrong_field_type_is_blocked(
    tmp_path: Path,
    dotted_field: str,
    value: Any,
) -> None:
    payload = _null_payload() if dotted_field == "schema_version" else _active_payload()
    if dotted_field == "schema_version":
        source_path, target = _write_null_tree(tmp_path)
        source_path = _write_task_document(tmp_path, _set_field(payload, dotted_field, value))
    else:
        source_path, target, _ = _write_active_tree(
            tmp_path,
            _set_field(payload, dotted_field, value),
        )
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


def test_list_of_two_tasks_is_not_a_third_legal_form(tmp_path: Path) -> None:
    second = _active_task_body()
    second["task_id"] = "synthetic-task-002"
    payload = _set_field(_active_payload(), "active_task", [_active_task_body(), second])
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


def test_empty_command_list_is_blocked(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.validation.commands", []),
    )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_validation_command_wrong_types_are_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["timeout_seconds"] = "300"
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


def test_boolean_timeout_is_not_accepted_as_an_integer(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["timeout_seconds"] = True
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


@pytest.mark.parametrize(
    "command_entry",
    (
        "python -m pytest -q",
        ["python", "-m", "pytest", "-q"],
        1,
    ),
)
def test_validation_command_entry_must_be_a_mapping(
    tmp_path: Path,
    command_entry: object,
) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"] = [command_entry]
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("id", 1),
        ("id", ["tests"]),
        ("command", 1),
        ("command", ["python", "-m", "pytest", "-q"]),
    ),
)
def test_validation_command_id_and_text_wrong_types_are_blocked(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0][field] = value
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("active_task.expected_outcome.command_id", 1),
        ("active_task.expected_outcome.command_id", ["tests"]),
        ("active_task.expected_outcome.failure_signature", 1),
        ("active_task.expected_outcome.failure_signature", ["synthetic failure signature"]),
    ),
)
def test_expected_red_command_id_and_signature_wrong_types_are_blocked(
    tmp_path: Path,
    dotted_field: str,
    value: object,
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_expected_red_payload(), dotted_field, value),
    )
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


@pytest.mark.parametrize(
    "document",
    (
        "",
        "\n",
        "- just a list\n",
        "not a mapping\n",
        "schema_version: [unterminated\n",
    ),
)
def test_empty_non_mapping_and_syntax_errors_are_blocked(
    tmp_path: Path,
    document: str,
) -> None:
    source_path, target = _write_null_tree(tmp_path)
    _overwrite_source(source_path, document)
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


def test_duplicate_yaml_keys_are_blocked(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    text = source_path.read_text(encoding="utf-8")
    _overwrite_source(source_path, 'schema_version: "1.0"\n' + text)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_duplicate_active_task_key_is_blocked(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(tmp_path)
    text = source_path.read_text(encoding="utf-8")
    _overwrite_source(source_path, text + "active_task: null\n")
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("schema_version", "2.0"),
        ("schema_version", "1.0.0"),
        ("active_task.task_id", ""),
        ("active_task.spec_ref", ""),
        ("active_task.adapter.kind", "other"),
        ("active_task.adapter.kind", ""),
        ("active_task.expected_outcome.kind", "RED"),
        ("active_task.expected_outcome.kind", "PASS"),
        ("active_task.allowed_paths", []),
        ("active_task.allowed_paths", ["src/example.py", "src/example.py"]),
    ),
)
def test_conflicting_values_are_blocked(
    tmp_path: Path,
    dotted_field: str,
    value: Any,
) -> None:
    payload = _null_payload() if dotted_field.startswith("schema_version") else _active_payload()
    if dotted_field.startswith("schema_version"):
        source_path = _write_task_document(tmp_path, _set_field(payload, dotted_field, value))
        target = tmp_path / "fixture-repo"
        target.mkdir(parents=True, exist_ok=True)
    else:
        source_path, target, _ = _write_active_tree(
            tmp_path,
            _set_field(payload, dotted_field, value),
        )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_empty_validation_command_id_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["id"] = ""
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_blank_task_id_is_blocked(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.task_id", "   "),
    )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_blank_validation_command_id_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["id"] = "   "
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_empty_validation_command_text_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["command"] = ""
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_blank_validation_command_text_is_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["command"] = "   "
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


@pytest.mark.parametrize("timeout", (0, -1))
def test_non_positive_timeout_is_blocked(tmp_path: Path, timeout: int) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"][0]["timeout_seconds"] = timeout
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_duplicate_validation_command_ids_are_blocked(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["active_task"]["validation"]["commands"].append(
        {
            "id": _REFERENCE_COMMAND_ID,
            "command": "python -m compileall src",
            "timeout_seconds": 60,
        }
    )
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_expected_red_unknown_command_id_is_blocked(tmp_path: Path) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["expected_outcome"]["command_id"] = "missing"
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_expected_red_empty_failure_signature_is_blocked(tmp_path: Path) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["expected_outcome"]["failure_signature"] = ""
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_expected_red_blank_failure_signature_is_blocked(tmp_path: Path) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["expected_outcome"]["failure_signature"] = "   "
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_expected_red_blank_command_id_is_blocked(tmp_path: Path) -> None:
    payload = _expected_red_payload()
    payload["active_task"]["expected_outcome"]["command_id"] = "   "
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


@pytest.mark.parametrize(
    "relative",
    (
        "../outside.py",
        "src/foo/../example.py",
        "src/./example.py",
        "./src/example.py",
        "src//example.py",
        "src/example.py/..",
    ),
)
def test_allowed_path_traversal_and_normalization_drift_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.allowed_paths", [relative]),
    )
    _expect_reason(source_path, target, "TASK_PATH_TRAVERSAL")


@pytest.mark.parametrize(
    "relative",
    (
        "../outside.md",
        "./tasks/../tasks/synthetic-task-001.md",
        "tasks//synthetic-task-001.md",
        "tasks/./synthetic-task-001.md",
    ),
)
def test_spec_ref_traversal_and_normalization_drift_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    payload = _set_field(_active_payload(), "active_task.spec_ref", relative)
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_PATH_TRAVERSAL")


@pytest.mark.parametrize(
    "relative",
    (
        ".",
        "./",
        "src/",
        "src/*",
        "src/*.py",
        "src/**",
        "src/**/*.py",
        "src/example?.py",
        "src/example[ab].py",
        "*",
        "",
    ),
)
def test_root_glob_directory_and_empty_allowed_paths_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    payload = _set_field(_active_payload(), "active_task.allowed_paths", [relative])
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_existing_target_directory_named_src_is_not_an_allowed_path(
    tmp_path: Path,
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.allowed_paths", ["src"]),
    )
    (target / "src").mkdir()
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_absolute_allowed_path_is_blocked(tmp_path: Path) -> None:
    absolute = str((tmp_path / "fixture-repo" / "src" / "example.py").resolve())
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.allowed_paths", [absolute]),
    )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_composed_home_class_absolute_allowed_path_is_blocked(tmp_path: Path) -> None:
    absolute = _composed_absolute("home", "synthetic-operator", "escaped.py")
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.allowed_paths", [absolute]),
    )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_composed_users_class_absolute_allowed_path_is_blocked(tmp_path: Path) -> None:
    absolute = _composed_absolute("Users", "synthetic-operator", "escaped.py")
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.allowed_paths", [absolute]),
    )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_absolute_spec_ref_is_blocked(tmp_path: Path) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    payload = _set_field(_active_payload(), "active_task.spec_ref", str(spec_path))
    source_path, target, _ = _write_active_tree(tmp_path, payload)
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_allowed_path_symlink_escape_from_target_is_blocked(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(tmp_path)
    leaked = tmp_path / "outside-target" / "example.py"
    leaked.parent.mkdir()
    leaked.write_text("escaped\n", encoding="utf-8")
    nested = target / "src"
    nested.symlink_to(leaked.parent)
    _expect_reason(source_path, target, "TASK_PATH_ESCAPE")


def test_spec_ref_symlink_escape_from_source_root_is_blocked(tmp_path: Path) -> None:
    enclosed = tmp_path / "enclosed"
    enclosed.mkdir()
    leaked = tmp_path / "outside-spec" / "leaked.md"
    leaked.parent.mkdir()
    leaked.write_text("escaped spec\n", encoding="utf-8")
    tasks_dir = enclosed / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "synthetic-task-001.md").symlink_to(leaked)
    payload = _active_payload()
    source_path = _write_task_document(enclosed, payload)
    target = enclosed / "fixture-repo"
    target.mkdir()
    _expect_reason(source_path, target, "TASK_PATH_ESCAPE")


def test_duplicate_allowed_paths_after_identical_entries_are_blocked(
    tmp_path: Path,
) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(
            _active_payload(),
            "active_task.allowed_paths",
            ["src/example.py", "tests/test_example.py", "src/example.py"],
        ),
    )
    _expect_reason(source_path, target, "TASK_VALUE_CONFLICT")


def test_non_string_allowed_path_entry_is_blocked(tmp_path: Path) -> None:
    source_path, target, _ = _write_active_tree(
        tmp_path,
        _set_field(_active_payload(), "active_task.allowed_paths", [1]),
    )
    _expect_reason(source_path, target, "TASK_FIELD_TYPE")


def test_missing_spec_source_is_blocked(tmp_path: Path) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    spec_path.unlink()
    _expect_reason(source_path, target, "TASK_SOURCE_MISSING")


def test_spec_source_directory_is_blocked(tmp_path: Path) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    spec_path.unlink()
    spec_path.mkdir()
    _expect_reason(source_path, target, "TASK_SOURCE_MISSING")


def test_broken_spec_symlink_is_blocked(tmp_path: Path) -> None:
    source_path, target, spec_path = _write_active_tree(tmp_path)
    spec_path.unlink()
    spec_path.symlink_to(tmp_path / "missing-spec.md")
    _expect_reason(source_path, target, "TASK_SOURCE_MISSING")


def test_missing_task_document_is_blocked(tmp_path: Path) -> None:
    target = tmp_path / "fixture-repo"
    target.mkdir()
    _expect_reason(tmp_path / "tasks.yaml", target, "TASK_SOURCE_MISSING")


def test_task_document_directory_is_blocked(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    source_path.unlink()
    source_path.mkdir()
    _expect_reason(source_path, target, "TASK_SOURCE_MISSING")


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip("process retains read access after clearing read mode bits")


def test_unreadable_task_document_is_blocked(tmp_path: Path) -> None:
    source_path, target = _write_null_tree(tmp_path)
    original_mode = source_path.stat().st_mode
    try:
        _make_unreadable(source_path)
        _expect_reason(source_path, target, "TASK_SOURCE_UNREADABLE")
    finally:
        if source_path.is_file():
            source_path.chmod(original_mode)
