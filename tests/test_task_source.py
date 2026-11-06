"""Synthetic public task-source and instruction-snapshot contract.

The public surface is intentionally small and explicit:

- ``load_task_source`` consumes a loaded ``RunnerConfig`` and the existing
  task contract. It does not parse private task lists or invent work.
- ``active_task: null`` is terminal rest: no spec, prompt, or successor
  artifacts are created.
- Exactly one active task is snapshotted as-is. There is no queue,
  planner, selection, or decomposition.
- Each used spec and prompt source is read exactly once during snapshot
  materialization, persisted under an explicit run root, and bound by
  logical reference, relative snapshot path, SHA-256, and byte length.
  Config loading is a separate earlier read and is not counted here.
- Snapshot bytes are the used bytes, including newlines and spacing.
  Later live edits do not replace them. Digest and length prove
  provenance only; they are not decision, promotion, trust, or secret
  authority.
- Raw instruction bytes stay in the run-local snapshot. Exportable facts
  carry metadata and digests only.
- ``previous_facts`` is caller-supplied. This module does not search
  history, detect an unsettled epoch, or choose a repeat policy.
"""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import inspect
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.task_source as task_source_module
from task_transition_runner.config import RunnerConfig, load_config
from task_transition_runner.task_contract import TaskContractError
from task_transition_runner.task_source import (
    InstructionSourceBinding,
    ReprovedInstructionSource,
    RunLocalSnapshotReproof,
    TaskSourceError,
    TaskSourceFacts,
    load_task_source,
    read_instruction_bytes,
    reprove_run_local_snapshot,
)

_PROMPT_ROLES = ("developer", "orchestrator", "validator")
_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_SPEC_REF = "./tasks/synthetic-task-001.md"
_REFERENCE_ALLOWED_PATHS = ("src/example.py", "tests/test_example.py")
_SPEC_LOGICAL_REF = _REFERENCE_SPEC_REF
_PROMPT_LOGICAL_REFS = {
    "developer": "./prompts/developer.md",
    "orchestrator": "./prompts/orchestrator.md",
    "validator": "./prompts/validator.md",
}
_SPEC_SNAPSHOT_PATH = "instructions/task_spec"
_PROMPT_SNAPSHOT_PATHS = {
    "developer": "instructions/prompts/developer",
    "orchestrator": "instructions/prompts/orchestrator",
    "validator": "instructions/prompts/validator",
}
_SPEC_BODY = "synthetic spec body with   trailing spaces  \r\nCRLF and a tab\tstay\n"
_PROMPT_BODIES = {
    "developer": "synthetic developer prompt\r\n  keep spacing  \n",
    "orchestrator": "synthetic orchestrator prompt\nno rewrite\n",
    "validator": "synthetic validator prompt\nline two\n",
}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _null_task_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": None,
    }


def _active_task_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": {
            "task_id": _REFERENCE_TASK_ID,
            "spec_ref": _REFERENCE_SPEC_REF,
            "allowed_paths": list(_REFERENCE_ALLOWED_PATHS),
            "validation": {
                "commands": [
                    {
                        "id": "tests",
                        "command": "python -m pytest -q",
                        "timeout_seconds": 300,
                    }
                ]
            },
            "expected_outcome": {"kind": "GREEN"},
            "adapter": {"kind": "cursor"},
            "metadata": {},
        },
    }


def _config_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "target": {
            "repo_root": "./fixture-repo",
            "main_branch": _REFERENCE_MAIN_BRANCH,
        },
        "runtime_root": "./runner-runtime",
        "task_source": "./tasks.yaml",
        "baseline_validation": {
            "command": "python -m pytest -q",
            "timeout_seconds": 300,
        },
        "adapter": {
            "kind": "cursor",
            "executable": "cursor-agent",
            "model": None,
            "timeout_seconds": 1800,
        },
        "prompts": {
            "developer": "./prompts/developer.md",
            "orchestrator": "./prompts/orchestrator.md",
            "validator": "./prompts/validator.md",
        },
    }


def _write_utf8(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _write_bytes(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _dump_yaml(path: Path, payload: dict[str, Any]) -> Path:
    return _write_utf8(path, yaml.safe_dump(payload, sort_keys=False))


def _write_source_tree(
    root: Path,
    *,
    task_payload: dict[str, Any] | None = None,
    write_spec: bool = True,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "fixture-repo").mkdir(parents=True, exist_ok=True)
    (root / "runner-runtime").mkdir(parents=True, exist_ok=True)
    document = _active_task_payload() if task_payload is None else copy.deepcopy(task_payload)
    if write_spec and isinstance(document.get("active_task"), dict):
        _write_utf8(root / "tasks" / "synthetic-task-001.md", _SPEC_BODY)
    _dump_yaml(root / "tasks.yaml", document)
    for role in _PROMPT_ROLES:
        _write_utf8(root / "prompts" / f"{role}.md", _PROMPT_BODIES[role])
    return _dump_yaml(root / "runner.yaml", _config_payload())


def _run_root(root: Path) -> Path:
    return root / "synthetic-run"


def _load_tree(
    root: Path,
    *,
    task_payload: dict[str, Any] | None = None,
    write_spec: bool = True,
) -> tuple[RunnerConfig, Path]:
    config_path = _write_source_tree(
        root,
        task_payload=task_payload,
        write_spec=write_spec,
    )
    return load_config(config_path), _run_root(root)


def _materialize(
    config: RunnerConfig,
    run_root: Path,
    previous_facts: TaskSourceFacts | None = None,
) -> TaskSourceFacts:
    if previous_facts is None:
        return load_task_source(config, run_root=run_root)
    return load_task_source(
        config,
        run_root=run_root,
        previous_facts=previous_facts,
    )


def _expect_source_reason(
    config: RunnerConfig,
    run_root: Path,
    reason_code: str,
    previous_facts: TaskSourceFacts | None = None,
) -> TaskSourceError:
    with pytest.raises(TaskSourceError, match=reason_code) as exc_info:
        _materialize(config, run_root, previous_facts)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _expect_contract_reason(
    config: RunnerConfig,
    run_root: Path,
    reason_code: str,
) -> TaskContractError:
    with pytest.raises(TaskContractError, match=reason_code) as exc_info:
        _materialize(config, run_root)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _binding_map(facts: TaskSourceFacts) -> dict[str, InstructionSourceBinding]:
    return {item.logical_ref: item for item in facts.sources}


def _live_instruction_paths(config: RunnerConfig) -> dict[str, Path]:
    paths = {
        "spec": (
            config.task_source_root / "tasks" / "synthetic-task-001.md"
        ).resolve(),
        "developer": config.prompts.developer.resolve(),
        "orchestrator": config.prompts.orchestrator.resolve(),
        "validator": config.prompts.validator.resolve(),
    }
    return paths


def _record_opens(watched: set[Path]) -> list[Path]:
    wanted = {path.resolve() for path in watched}
    recorded: list[Path] = []

    def _hook(event: str, args: tuple[object, ...]) -> None:
        if event != "open" or not args:
            return
        raw = args[0]
        if not isinstance(raw, (str, bytes, os.PathLike)):
            return
        opened = Path(os.fsdecode(raw)).resolve()
        if opened in wanted:
            recorded.append(opened)

    sys.addaudithook(_hook)
    return recorded


def _composed_absolute(*segments: str) -> str:
    return "/" + "/".join(segments)


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip(
            "process retains read access after clearing read mode bits"
        )


def _exportable_view(facts: TaskSourceFacts) -> dict[str, Any]:
    return {
        "contract_fingerprint": facts.contract.fingerprint,
        "sources": [
            {
                "logical_ref": item.logical_ref,
                "snapshot_path": item.snapshot_path,
                "sha256": item.sha256,
                "byte_length": item.byte_length,
            }
            for item in facts.sources
        ],
    }


def _reproof_anchors(facts: TaskSourceFacts) -> tuple[str, str, str]:
    task_snapshot = facts.run_root / "task_snapshot.yaml"
    canonical_sources = [
        {
            "byte_length": item.byte_length,
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "snapshot_path": item.snapshot_path,
        }
        for item in facts.sources
    ]
    instruction_signature = _sha256(
        json.dumps(
            canonical_sources,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    return (
        facts.contract.fingerprint,
        _sha256(task_snapshot.read_bytes()),
        instruction_signature,
    )


def _reprove(facts: TaskSourceFacts) -> RunLocalSnapshotReproof:
    fingerprint, task_digest, instruction_signature = _reproof_anchors(facts)
    return reprove_run_local_snapshot(
        run_root=facts.run_root,
        expected_task_fingerprint=fingerprint,
        expected_task_snapshot_sha256=task_digest,
        expected_instruction_signature_sha256=instruction_signature,
    )


def test_null_active_task_is_terminal_rest_without_instruction_artifacts(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path, task_payload=_null_task_payload(), write_spec=False)
    facts = _materialize(config, run_root)

    assert isinstance(facts, TaskSourceFacts)
    assert facts.contract.active_task is None
    assert facts.sources == ()
    assert facts.run_root == run_root.resolve()
    assert (run_root / "task_snapshot.yaml").is_file()
    assert not (run_root / "instructions").exists()
    document = yaml.safe_load((run_root / "task_snapshot.yaml").read_text(encoding="utf-8"))
    assert document["active_task"] is None
    assert "task_id" not in document
    runtime_files = [path for path in config.runtime_root.rglob("*") if path.is_file()]
    assert runtime_files == []
    assert not (config.runtime_root / "instructions").exists()
    assert not (config.runtime_root / "task_snapshot.yaml").exists()
    assert not (config.config_root / "instructions").exists()


def test_null_form_does_not_invent_a_task_or_snapshot_spec(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path, task_payload=_null_task_payload(), write_spec=False)
    facts = _materialize(config, run_root)

    assert facts.contract.active_task is None
    assert not (config.task_source_root / "tasks").exists()
    assert not hasattr(facts, "queue")
    assert not hasattr(facts, "planner")
    assert not hasattr(facts, "origin_run_id")
    with pytest.raises(TaskSourceError) as exc_info:
        read_instruction_bytes(facts, _SPEC_LOGICAL_REF)
    assert exc_info.value.reason_code == "SOURCE_VALUE_CONFLICT"


def test_single_active_task_binds_spec_and_configured_prompts(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    bindings = _binding_map(facts)

    assert facts.contract.active_task is not None
    assert facts.contract.active_task.task_id == _REFERENCE_TASK_ID
    assert tuple(item.logical_ref for item in facts.sources) == (
        _SPEC_LOGICAL_REF,
        _PROMPT_LOGICAL_REFS["developer"],
        _PROMPT_LOGICAL_REFS["orchestrator"],
        _PROMPT_LOGICAL_REFS["validator"],
    )
    assert bindings[_SPEC_LOGICAL_REF].snapshot_path == _SPEC_SNAPSHOT_PATH
    for role in _PROMPT_ROLES:
        assert bindings[_PROMPT_LOGICAL_REFS[role]].snapshot_path == (
            _PROMPT_SNAPSHOT_PATHS[role]
        )
    task_snapshot = run_root / "task_snapshot.yaml"
    snapshot_text = task_snapshot.read_text(encoding="utf-8")
    snapshot = yaml.safe_load(snapshot_text)
    assert snapshot["active_task"]["task_id"] == _REFERENCE_TASK_ID
    assert _SPEC_BODY not in snapshot_text


def test_snapshot_bytes_match_used_bytes_including_newlines(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    bindings = _binding_map(facts)
    spec_bytes = _SPEC_BODY.encode("utf-8")

    persisted_spec = (run_root / _SPEC_SNAPSHOT_PATH).read_bytes()
    assert persisted_spec == spec_bytes
    assert bindings[_SPEC_LOGICAL_REF].byte_length == len(spec_bytes)
    assert bindings[_SPEC_LOGICAL_REF].sha256 == _sha256(spec_bytes)
    assert read_instruction_bytes(facts, _SPEC_LOGICAL_REF) == spec_bytes

    for role in _PROMPT_ROLES:
        expected = _PROMPT_BODIES[role].encode("utf-8")
        logical = _PROMPT_LOGICAL_REFS[role]
        persisted = (run_root / _PROMPT_SNAPSHOT_PATHS[role]).read_bytes()
        assert persisted == expected
        assert bindings[logical].byte_length == len(expected)
        assert bindings[logical].sha256 == _sha256(expected)
        assert read_instruction_bytes(facts, logical) == expected


def test_spec_bytes_are_not_reencoded_or_normalized(tmp_path: Path) -> None:
    config_path = _write_source_tree(tmp_path)
    spec_bytes = b"\xef\xbb\xbf" + "cafe\u0301\n".encode("utf-8")
    (tmp_path / "tasks" / "synthetic-task-001.md").write_bytes(spec_bytes)
    config = load_config(config_path)
    facts = _materialize(config, _run_root(tmp_path))

    assert read_instruction_bytes(facts, _SPEC_LOGICAL_REF) == spec_bytes
    assert (facts.run_root / _SPEC_SNAPSHOT_PATH).read_bytes() == spec_bytes
    nfc = b"\xef\xbb\xbf" + "caf\u00e9\n".encode("utf-8")
    assert spec_bytes != nfc


def test_instruction_snapshot_metadata_is_persisted_without_raw_bodies(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    meta_path = run_root / "instructions" / "instruction_snapshot.yaml"
    raw_meta = meta_path.read_text(encoding="utf-8")
    loaded_meta = yaml.safe_load(raw_meta)

    assert meta_path.is_file()
    assert _SPEC_BODY not in raw_meta
    for body in _PROMPT_BODIES.values():
        assert body not in raw_meta
    listed = loaded_meta["sources"]
    assert [item["logical_ref"] for item in listed] == [
        binding.logical_ref for binding in facts.sources
    ]
    for item, binding in zip(listed, facts.sources, strict=True):
        assert item["snapshot_path"] == binding.snapshot_path
        assert item["sha256"] == binding.sha256
        assert item["byte_length"] == binding.byte_length
        assert "content" not in item
        assert "bytes" not in item
        assert "body" not in item
        assert "text" not in item


def test_exportable_facts_carry_digests_not_instruction_bodies(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    exported = yaml.safe_dump(_exportable_view(facts), sort_keys=True)

    assert not hasattr(facts, "spec_bytes")
    assert not hasattr(facts, "prompt_bytes")
    assert not hasattr(facts, "content")
    assert _SPEC_BODY not in exported
    for body in _PROMPT_BODIES.values():
        assert body not in exported
    for binding in facts.sources:
        assert binding.sha256 in exported
        assert "decision" not in exported
        assert "promotion" not in exported
        assert "secret" not in exported
        assert "trust" not in exported
    assert not (run_root / "decisions").exists()
    assert not (run_root / "evidence").exists()
    assert not hasattr(facts, "decision")
    assert not hasattr(facts, "promotion")
    assert not hasattr(facts, "secret")
    assert not hasattr(facts, "trust")


def test_each_used_live_source_is_opened_exactly_once_during_snapshot_materialization(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    live_paths = _live_instruction_paths(config)
    recorded = _record_opens(set(live_paths.values()))

    _materialize(config, run_root)

    for path in live_paths.values():
        assert recorded.count(path.resolve()) == 1


def test_task_source_document_is_opened_once_during_snapshot_materialization(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    recorded = _record_opens({config.task_source})

    _materialize(config, run_root)

    assert recorded.count(config.task_source.resolve()) == 1


def test_live_task_document_edit_after_snapshot_does_not_replace_captured_source(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    captured = facts.contract.source_bytes
    fingerprint = facts.contract.fingerprint
    snapshot_path = run_root / "task_snapshot.yaml"
    assert snapshot_path.read_bytes() == captured
    config.task_source.write_text(
        'schema_version: "1.0"\nactive_task: null\n',
        encoding="utf-8",
    )

    assert snapshot_path.read_bytes() == captured
    assert facts.contract.source_bytes == captured
    assert facts.contract.fingerprint == fingerprint
    snapshot = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["active_task"]["task_id"] == _REFERENCE_TASK_ID


def test_task_snapshot_bytes_match_captured_source_including_crlf_and_spacing(
    tmp_path: Path,
) -> None:
    config_path = _write_source_tree(tmp_path)
    task_path = tmp_path / "tasks.yaml"
    spaced = task_path.read_text(encoding="utf-8").replace("\n", "  \r\n")
    original_bytes = spaced.encode("utf-8")
    task_path.write_bytes(original_bytes)
    config = load_config(config_path)
    facts = _materialize(config, _run_root(tmp_path))
    snapshot_path = facts.run_root / "task_snapshot.yaml"

    assert b"\r\n" in original_bytes
    assert b"  \r\n" in original_bytes
    assert facts.contract.source_bytes == original_bytes
    assert snapshot_path.read_bytes() == facts.contract.source_bytes


def test_foreign_yaml_and_suffixless_config_keep_snapshot_prompt_logical_refs(
    tmp_path: Path,
) -> None:
    yaml_config = _write_source_tree(tmp_path)
    (tmp_path / "notes.yaml").write_text(
        "prompts:\n"
        "  developer: ./prompts/validator.md\n"
        "  orchestrator: ./prompts/developer.md\n"
        "  validator: ./prompts/orchestrator.md\n",
        encoding="utf-8",
    )
    (tmp_path / "extra.yml").write_text(
        "prompts:\n  developer: ./prompts/orchestrator.md\n",
        encoding="utf-8",
    )
    unsuffixed = tmp_path / "runner"
    unsuffixed.write_bytes(yaml_config.read_bytes())
    config = load_config(unsuffixed)
    facts = _materialize(config, _run_root(tmp_path))
    task = facts.contract.active_task

    assert task is not None
    assert tuple(item.logical_ref for item in facts.sources) == (
        task.declared_spec_ref,
        config.prompts.declared_developer,
        config.prompts.declared_orchestrator,
        config.prompts.declared_validator,
    )
    assert task.declared_spec_ref == _SPEC_LOGICAL_REF
    assert config.prompts.declared_developer == _PROMPT_LOGICAL_REFS["developer"]
    assert config.prompts.declared_orchestrator == _PROMPT_LOGICAL_REFS["orchestrator"]
    assert config.prompts.declared_validator == _PROMPT_LOGICAL_REFS["validator"]


def test_live_edits_after_snapshot_do_not_replace_bound_bytes(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    live = _live_instruction_paths(config)
    live["spec"].write_text("mutated spec\n", encoding="utf-8")
    live["developer"].write_text("mutated developer\n", encoding="utf-8")

    assert read_instruction_bytes(facts, _SPEC_LOGICAL_REF) == _SPEC_BODY.encode("utf-8")
    assert read_instruction_bytes(
        facts,
        _PROMPT_LOGICAL_REFS["developer"],
    ) == _PROMPT_BODIES["developer"].encode("utf-8")
    assert (run_root / _SPEC_SNAPSHOT_PATH).read_bytes() == _SPEC_BODY.encode("utf-8")


def test_read_after_snapshot_does_not_open_live_sources(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    live_paths = _live_instruction_paths(config)
    recorded = _record_opens(set(live_paths.values()))

    read_instruction_bytes(facts, _SPEC_LOGICAL_REF)
    read_instruction_bytes(facts, _PROMPT_LOGICAL_REFS["developer"])
    read_instruction_bytes(facts, _PROMPT_LOGICAL_REFS["orchestrator"])
    read_instruction_bytes(facts, _PROMPT_LOGICAL_REFS["validator"])

    assert recorded == []


def test_deleted_or_unreadable_live_sources_do_not_affect_snapshot(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    live = _live_instruction_paths(config)
    original_mode = live["developer"].stat().st_mode
    live["spec"].unlink()
    try:
        _make_unreadable(live["developer"])
        assert read_instruction_bytes(facts, _SPEC_LOGICAL_REF) == _SPEC_BODY.encode(
            "utf-8"
        )
        assert read_instruction_bytes(
            facts,
            _PROMPT_LOGICAL_REFS["developer"],
        ) == _PROMPT_BODIES["developer"].encode("utf-8")
    finally:
        if live["developer"].is_file():
            live["developer"].chmod(original_mode)


def test_contract_fingerprint_ignores_yaml_key_order_and_formatting(
    tmp_path: Path,
) -> None:
    first_config, first_run = _load_tree(tmp_path / "first")
    first = _materialize(first_config, first_run)

    second_root = tmp_path / "second"
    second_config, second_run = _load_tree(second_root)
    payload = yaml.safe_load((second_root / "tasks.yaml").read_text(encoding="utf-8"))
    _write_utf8(
        second_root / "tasks.yaml",
        yaml.safe_dump(payload, sort_keys=True) + "\n\n",
    )
    second_config = load_config(second_root / "runner.yaml")
    second = _materialize(second_config, second_run, previous_facts=first)

    assert first.contract.fingerprint == second.contract.fingerprint
    assert first.contract.active_task is not None
    assert second.contract.active_task is not None
    assert first.contract.active_task.task_id == second.contract.active_task.task_id


def test_same_task_id_spec_byte_drift_against_previous_facts_is_blocked(
    tmp_path: Path,
) -> None:
    original_config, original_run = _load_tree(tmp_path / "original")
    previous = _materialize(original_config, original_run)

    drifted_root = tmp_path / "drifted"
    drifted_config, drifted_run = _load_tree(drifted_root)
    _write_utf8(drifted_root / "tasks" / "synthetic-task-001.md", "drifted spec\n")
    drifted_config = load_config(drifted_root / "runner.yaml")
    _expect_source_reason(
        drifted_config,
        drifted_run,
        "SOURCE_VALUE_CONFLICT",
        previous_facts=previous,
    )


def test_same_task_id_prompt_byte_drift_against_previous_facts_is_blocked(
    tmp_path: Path,
) -> None:
    original_config, original_run = _load_tree(tmp_path / "original")
    previous = _materialize(original_config, original_run)

    drifted_root = tmp_path / "drifted"
    drifted_config, drifted_run = _load_tree(drifted_root)
    _write_utf8(drifted_root / "prompts" / "validator.md", "drifted validator\n")
    drifted_config = load_config(drifted_root / "runner.yaml")
    _expect_source_reason(
        drifted_config,
        drifted_run,
        "SOURCE_VALUE_CONFLICT",
        previous_facts=previous,
    )


def test_different_task_id_is_not_instruction_drift(
    tmp_path: Path,
) -> None:
    original_config, original_run = _load_tree(tmp_path / "original")
    previous = _materialize(original_config, original_run)

    successor = _active_task_payload()
    successor["active_task"]["task_id"] = "synthetic-task-002"
    successor["active_task"]["spec_ref"] = "./tasks/synthetic-task-002.md"
    successor_root = tmp_path / "successor"
    successor_config, successor_run = _load_tree(
        successor_root,
        task_payload=successor,
        write_spec=False,
    )
    _write_utf8(successor_root / "tasks" / "synthetic-task-002.md", "successor spec\n")
    successor_config = load_config(successor_root / "runner.yaml")
    loaded = _materialize(successor_config, successor_run, previous_facts=previous)

    assert loaded.contract.active_task is not None
    assert loaded.contract.active_task.task_id == "synthetic-task-002"
    assert loaded.contract.fingerprint != previous.contract.fingerprint


def test_empty_run_root_is_accepted_for_first_materialization(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    run_root.mkdir()
    assert list(run_root.iterdir()) == []

    facts = _materialize(config, run_root)

    assert facts.contract.active_task is not None
    assert (run_root / _SPEC_SNAPSHOT_PATH).is_file()


def test_null_against_previous_active_facts_is_terminal_rest(tmp_path: Path) -> None:
    active_config, active_run = _load_tree(tmp_path / "active")
    previous = _materialize(active_config, active_run)
    null_config, null_run = _load_tree(
        tmp_path / "rest",
        task_payload=_null_task_payload(),
        write_spec=False,
    )

    facts = _materialize(null_config, null_run, previous_facts=previous)

    assert previous.contract.active_task is not None
    assert facts.contract.active_task is None
    assert facts.sources == ()
    assert not (null_run / "instructions").exists()


def test_null_previous_facts_against_later_active_task_is_accepted(
    tmp_path: Path,
) -> None:
    null_config, null_run = _load_tree(
        tmp_path / "rest",
        task_payload=_null_task_payload(),
        write_spec=False,
    )
    previous = _materialize(null_config, null_run)
    active_config, active_run = _load_tree(tmp_path / "active")

    facts = _materialize(active_config, active_run, previous_facts=previous)

    assert previous.contract.active_task is None
    assert facts.contract.active_task is not None
    assert facts.contract.active_task.task_id == _REFERENCE_TASK_ID
    assert facts.sources != ()


def test_a5_snapshot_byte_tamper_fails_closed(tmp_path: Path) -> None:
    marker = (
        "OSS7_A5_RED::ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID::"
        "test_a5_snapshot_byte_tamper_fails_closed"
    )
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    task_snapshot = run_root / "task_snapshot.yaml"
    expected_task_snapshot_sha256 = _sha256(task_snapshot.read_bytes())
    canonical_sources = [
        {
            "byte_length": item.byte_length,
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "snapshot_path": item.snapshot_path,
        }
        for item in facts.sources
    ]
    expected_instruction_signature_sha256 = _sha256(
        json.dumps(
            canonical_sources,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    task_snapshot.write_bytes(task_snapshot.read_bytes() + b"# byte tamper\n")

    reprove = getattr(task_source_module, "reprove_run_local_snapshot", None)
    if reprove is None:
        raise AssertionError(marker)
    try:
        reprove(
            run_root=run_root,
            target_worktree=config.target_worktree,
            expected_task_fingerprint=facts.contract.fingerprint,
            expected_task_snapshot_sha256=expected_task_snapshot_sha256,
            expected_instruction_signature_sha256=(
                expected_instruction_signature_sha256
            ),
        )
    except TaskSourceError as exc:
        if exc.reason_code == "ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID":
            return
    raise AssertionError(marker)


def test_run_local_snapshot_reproof_returns_exact_ordered_persisted_identity(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    task_snapshot_bytes = (run_root / "task_snapshot.yaml").read_bytes()
    config.task_source.write_bytes(b'schema_version: "1.0"\nactive_task: null\n')
    for path in _live_instruction_paths(config).values():
        path.unlink()

    reproved = _reprove(facts)

    assert isinstance(reproved, RunLocalSnapshotReproof)
    assert reproved.task_id == _REFERENCE_TASK_ID
    assert reproved.task_fingerprint == facts.contract.fingerprint
    assert reproved.task_snapshot_sha256 == _sha256(task_snapshot_bytes)
    assert reproved.instruction_signature_sha256 == _reproof_anchors(facts)[2]
    assert all(
        isinstance(item, ReprovedInstructionSource)
        for item in reproved.instruction_sources
    )
    assert tuple(item.logical_ref for item in reproved.instruction_sources) == (
        _SPEC_LOGICAL_REF,
        _PROMPT_LOGICAL_REFS["developer"],
        _PROMPT_LOGICAL_REFS["orchestrator"],
        _PROMPT_LOGICAL_REFS["validator"],
    )
    assert tuple(
        (item.logical_ref, item.sha256, item.byte_length)
        for item in reproved.instruction_sources
    ) == tuple(
        (item.logical_ref, item.sha256, item.byte_length) for item in facts.sources
    )
    assert all(
        not hasattr(item, "snapshot_path") for item in reproved.instruction_sources
    )


@pytest.mark.parametrize(
    "anchor",
    (
        "task_fingerprint",
        "task_snapshot_sha256",
        "instruction_signature_sha256",
    ),
)
def test_run_local_snapshot_reproof_rejects_each_anchor_disagreement(
    tmp_path: Path,
    anchor: str,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    fingerprint, task_digest, instruction_signature = _reproof_anchors(facts)
    arguments = {
        "expected_task_fingerprint": fingerprint,
        "expected_task_snapshot_sha256": task_digest,
        "expected_instruction_signature_sha256": instruction_signature,
    }
    arguments[f"expected_{anchor}"] = "f" * 64
    expected_reason = (
        "ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID"
        if anchor == "task_snapshot_sha256"
        else "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"
    )

    with pytest.raises(TaskSourceError, match=expected_reason) as exc_info:
        reprove_run_local_snapshot(run_root=run_root, **arguments)

    assert exc_info.value.reason_code == expected_reason


def test_a5_cross_run_snapshot_family_cannot_claim_predecessor_identity(
    tmp_path: Path,
) -> None:
    predecessor_config, predecessor_run = _load_tree(tmp_path / "predecessor")
    predecessor = _materialize(predecessor_config, predecessor_run)
    foreign_payload = _active_task_payload()
    foreign_payload["active_task"]["task_id"] = "synthetic-task-002"
    foreign_config, foreign_run = _load_tree(
        tmp_path / "foreign",
        task_payload=foreign_payload,
    )
    foreign = _materialize(foreign_config, foreign_run)
    foreign_anchors = _reproof_anchors(foreign)
    predecessor_before = {
        path.relative_to(predecessor_run): path.read_bytes()
        for path in predecessor_run.rglob("*")
        if path.is_file()
    }
    foreign_before = {
        path.relative_to(foreign_run): path.read_bytes()
        for path in foreign_run.rglob("*")
        if path.is_file()
    }

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH",
    ) as exc_info:
        reprove_run_local_snapshot(
            run_root=foreign_run,
            expected_task_fingerprint=predecessor.contract.fingerprint,
            expected_task_snapshot_sha256=foreign_anchors[1],
            expected_instruction_signature_sha256=foreign_anchors[2],
        )

    assert exc_info.value.reason_code == "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"
    assert predecessor_before == {
        path.relative_to(predecessor_run): path.read_bytes()
        for path in predecessor_run.rglob("*")
        if path.is_file()
    }
    assert foreign_before == {
        path.relative_to(foreign_run): path.read_bytes()
        for path in foreign_run.rglob("*")
        if path.is_file()
    }


def test_run_local_snapshot_reproof_rejects_malformed_task_bytes(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    task_snapshot = run_root / "task_snapshot.yaml"
    task_snapshot.write_bytes(b"\xff")
    _, _, instruction_signature = _reproof_anchors(facts)

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ) as exc_info:
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=facts.contract.fingerprint,
            expected_task_snapshot_sha256=_sha256(task_snapshot.read_bytes()),
            expected_instruction_signature_sha256=instruction_signature,
        )

    assert exc_info.value.reason_code == "ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID"


def test_run_local_snapshot_reproof_rejects_null_task_snapshot(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    task_snapshot = run_root / "task_snapshot.yaml"
    task_snapshot.write_bytes(b'schema_version: "1.0"\nactive_task: null\n')
    _, _, instruction_signature = _reproof_anchors(facts)

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=facts.contract.fingerprint,
            expected_task_snapshot_sha256=_sha256(task_snapshot.read_bytes()),
            expected_instruction_signature_sha256=instruction_signature,
        )


@pytest.mark.parametrize(
    "document",
    (
        {},
        {"sources": "not-a-list"},
        {"sources": []},
        {"sources": [{"logical_ref": "only"}]},
        {"sources": [], "extra": True},
        b"\xff",
        b"sources: [unterminated\n",
    ),
)
def test_run_local_snapshot_reproof_rejects_malformed_instruction_metadata(
    tmp_path: Path,
    document: object,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    meta_path = run_root / "instructions" / "instruction_snapshot.yaml"
    if isinstance(document, bytes):
        meta_path.write_bytes(document)
    else:
        meta_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    fingerprint, task_digest, instruction_signature = _reproof_anchors(facts)

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ) as exc_info:
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=fingerprint,
            expected_task_snapshot_sha256=task_digest,
            expected_instruction_signature_sha256=instruction_signature,
        )

    assert exc_info.value.reason_code == "ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID"


@pytest.mark.parametrize("duplicate_level", ("root", "binding"))
def test_run_local_snapshot_reproof_rejects_duplicate_metadata_keys(
    tmp_path: Path,
    duplicate_level: str,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    anchors = _reproof_anchors(facts)
    meta_path = run_root / "instructions" / "instruction_snapshot.yaml"
    text = meta_path.read_text(encoding="utf-8")
    if duplicate_level == "root":
        text = text + text
    else:
        logical_line = f"- logical_ref: {_SPEC_LOGICAL_REF}\n"
        duplicate = f"  logical_ref: {_SPEC_LOGICAL_REF}\n"
        text = text.replace(logical_line, logical_line + duplicate, 1)
    meta_path.write_text(text, encoding="utf-8")

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=anchors[0],
            expected_task_snapshot_sha256=anchors[1],
            expected_instruction_signature_sha256=anchors[2],
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("logical_ref", 1),
        ("logical_ref", ""),
        ("snapshot_path", 1),
        ("snapshot_path", ""),
        ("snapshot_path", "/absolute"),
        ("snapshot_path", "instructions//task_spec"),
        ("snapshot_path", "../outside"),
        ("sha256", 1),
        ("sha256", "A" * 64),
        ("byte_length", True),
        ("byte_length", -1),
    ),
)
def test_run_local_snapshot_reproof_rejects_invalid_instruction_binding_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    fingerprint, task_digest, instruction_signature = _reproof_anchors(facts)
    meta_path = run_root / "instructions" / "instruction_snapshot.yaml"
    document = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    document["sources"][0][field] = value
    meta_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=fingerprint,
            expected_task_snapshot_sha256=task_digest,
            expected_instruction_signature_sha256=instruction_signature,
        )


@pytest.mark.parametrize("tamper_kind", ("length", "same-length-digest"))
def test_run_local_snapshot_reproof_rejects_instruction_payload_drift(
    tmp_path: Path,
    tamper_kind: str,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    fingerprint, task_digest, instruction_signature = _reproof_anchors(facts)
    payload_path = run_root / facts.sources[0].snapshot_path
    payload = payload_path.read_bytes()
    if tamper_kind == "length":
        payload_path.write_bytes(payload + b"x")
    else:
        payload_path.write_bytes(bytes([payload[0] ^ 0xFF]) + payload[1:])

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=fingerprint,
            expected_task_snapshot_sha256=task_digest,
            expected_instruction_signature_sha256=instruction_signature,
        )


@pytest.mark.parametrize("artifact", ("task", "metadata", "payload"))
def test_run_local_snapshot_reproof_rejects_missing_artifacts(
    tmp_path: Path,
    artifact: str,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    anchors = _reproof_anchors(facts)
    paths = {
        "task": run_root / "task_snapshot.yaml",
        "metadata": run_root / "instructions" / "instruction_snapshot.yaml",
        "payload": run_root / facts.sources[0].snapshot_path,
    }
    paths[artifact].unlink()

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=anchors[0],
            expected_task_snapshot_sha256=anchors[1],
            expected_instruction_signature_sha256=anchors[2],
        )


def test_run_local_snapshot_reproof_rejects_payload_symlink_escape(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path / "enclosed")
    facts = _materialize(config, run_root)
    anchors = _reproof_anchors(facts)
    payload_path = run_root / facts.sources[0].snapshot_path
    payload = payload_path.read_bytes()
    escaped = _write_bytes(tmp_path / "outside" / "payload", payload)
    payload_path.unlink()
    payload_path.symlink_to(escaped)

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=anchors[0],
            expected_task_snapshot_sha256=anchors[1],
            expected_instruction_signature_sha256=anchors[2],
        )


def test_run_local_snapshot_reproof_maps_read_failure_to_snapshot_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    anchors = _reproof_anchors(facts)
    task_snapshot = (run_root / "task_snapshot.yaml").resolve()
    original = Path.read_bytes

    def fail_selected(path: Path) -> bytes:
        if path.resolve() == task_snapshot:
            raise PermissionError("synthetic denial")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", fail_selected)
    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID",
    ):
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=anchors[0],
            expected_task_snapshot_sha256=anchors[1],
            expected_instruction_signature_sha256=anchors[2],
        )


def test_run_local_snapshot_reproof_preserves_order_in_signature(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    fingerprint, task_digest, instruction_signature = _reproof_anchors(facts)
    meta_path = run_root / "instructions" / "instruction_snapshot.yaml"
    document = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    document["sources"].reverse()
    meta_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(
        TaskSourceError,
        match="ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH",
    ) as exc_info:
        reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=fingerprint,
            expected_task_snapshot_sha256=task_digest,
            expected_instruction_signature_sha256=instruction_signature,
        )

    assert exc_info.value.reason_code == "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"


def test_snapshot_digest_mismatch_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    spec_path = run_root / _SPEC_SNAPSHOT_PATH
    spec_path.write_bytes(spec_path.read_bytes() + b"tamper")

    with pytest.raises(TaskSourceError, match="SOURCE_VALUE_CONFLICT") as exc_info:
        read_instruction_bytes(facts, _SPEC_LOGICAL_REF)
    assert exc_info.value.reason_code == "SOURCE_VALUE_CONFLICT"


def test_snapshot_byte_length_mismatch_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    prompt_path = run_root / _PROMPT_SNAPSHOT_PATHS["orchestrator"]
    original = prompt_path.read_bytes()
    prompt_path.write_bytes(original[: max(0, len(original) - 1)])

    with pytest.raises(TaskSourceError, match="SOURCE_VALUE_CONFLICT") as exc_info:
        read_instruction_bytes(facts, _PROMPT_LOGICAL_REFS["orchestrator"])
    assert exc_info.value.reason_code == "SOURCE_VALUE_CONFLICT"


def test_same_length_snapshot_tamper_is_blocked_by_digest(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    spec_path = run_root / _SPEC_SNAPSHOT_PATH
    original = spec_path.read_bytes()
    tampered = bytes([original[0] ^ 0xFF]) + original[1:]
    assert len(tampered) == len(original)
    assert _sha256(tampered) != _sha256(original)
    spec_path.write_bytes(tampered)

    with pytest.raises(TaskSourceError, match="SOURCE_VALUE_CONFLICT") as exc_info:
        read_instruction_bytes(facts, _SPEC_LOGICAL_REF)
    assert exc_info.value.reason_code == "SOURCE_VALUE_CONFLICT"


def test_recorded_byte_length_mismatch_is_blocked_when_digest_still_matches(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    binding = facts.sources[0]
    persisted = (run_root / binding.snapshot_path).read_bytes()
    assert _sha256(persisted) == binding.sha256
    assert len(persisted) == binding.byte_length
    drifted = InstructionSourceBinding(
        logical_ref=binding.logical_ref,
        snapshot_path=binding.snapshot_path,
        sha256=binding.sha256,
        byte_length=binding.byte_length + 1,
    )
    mutated_facts = TaskSourceFacts(
        contract=facts.contract,
        sources=(drifted,) + facts.sources[1:],
        run_root=facts.run_root,
    )

    with pytest.raises(TaskSourceError, match="SOURCE_VALUE_CONFLICT") as exc_info:
        read_instruction_bytes(mutated_facts, binding.logical_ref)
    assert exc_info.value.reason_code == "SOURCE_VALUE_CONFLICT"


def test_unknown_logical_ref_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)

    with pytest.raises(TaskSourceError, match="SOURCE_VALUE_CONFLICT") as exc_info:
        read_instruction_bytes(facts, "prompts.unknown")
    assert exc_info.value.reason_code == "SOURCE_VALUE_CONFLICT"


def test_missing_snapshot_file_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    (run_root / _SPEC_SNAPSHOT_PATH).unlink()

    with pytest.raises(TaskSourceError, match="SOURCE_MISSING") as exc_info:
        read_instruction_bytes(facts, _SPEC_LOGICAL_REF)
    assert exc_info.value.reason_code == "SOURCE_MISSING"


def test_unreadable_snapshot_file_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    snapshot = run_root / _PROMPT_SNAPSHOT_PATHS["developer"]
    original_mode = snapshot.stat().st_mode
    try:
        _make_unreadable(snapshot)
        with pytest.raises(TaskSourceError, match="SOURCE_UNREADABLE") as exc_info:
            read_instruction_bytes(facts, _PROMPT_LOGICAL_REFS["developer"])
        assert exc_info.value.reason_code == "SOURCE_UNREADABLE"
    finally:
        if snapshot.is_file():
            snapshot.chmod(original_mode)


def test_snapshot_symlink_escape_is_blocked_even_with_matching_digest(
    tmp_path: Path,
) -> None:
    enclosed = tmp_path / "enclosed"
    config, run_root = _load_tree(enclosed)
    facts = _materialize(config, run_root)
    snapshot = run_root / _SPEC_SNAPSHOT_PATH
    original = snapshot.read_bytes()
    leaked = tmp_path / "outside-snapshot" / "task_spec"
    _write_bytes(leaked, original)
    snapshot.unlink()
    snapshot.symlink_to(leaked)

    assert _sha256(leaked.read_bytes()) == _binding_map(facts)[_SPEC_LOGICAL_REF].sha256
    assert leaked.read_bytes() == original
    with pytest.raises(TaskSourceError, match="SOURCE_PATH_ESCAPE") as exc_info:
        read_instruction_bytes(facts, _SPEC_LOGICAL_REF)
    assert exc_info.value.reason_code == "SOURCE_PATH_ESCAPE"


def test_spec_parent_traversal_is_blocked_by_reused_task_contract(
    tmp_path: Path,
) -> None:
    payload = _active_task_payload()
    payload["active_task"]["spec_ref"] = "../outside.md"
    config, run_root = _load_tree(tmp_path, task_payload=payload, write_spec=False)
    _write_utf8(tmp_path / "outside.md", "escaped spec\n")
    _expect_contract_reason(config, run_root, "TASK_PATH_TRAVERSAL")


def test_prompt_symlink_escape_after_config_load_is_blocked(
    tmp_path: Path,
) -> None:
    enclosed = tmp_path / "enclosed"
    config, run_root = _load_tree(enclosed)
    leaked = tmp_path / "outside-prompts" / "leaked.md"
    _write_utf8(leaked, "escaped prompt\n")
    prompt = enclosed / "prompts" / "developer.md"
    prompt.unlink()
    prompt.symlink_to(leaked)
    _expect_source_reason(config, run_root, "SOURCE_PATH_ESCAPE")


def test_prompt_relative_parent_symlink_escape_is_blocked(tmp_path: Path) -> None:
    enclosed = tmp_path / "enclosed"
    config, run_root = _load_tree(enclosed)
    leaked = tmp_path / "outside-prompt.md"
    _write_utf8(leaked, "escaped prompt\n")
    prompt = enclosed / "prompts" / "orchestrator.md"
    prompt.unlink()
    prompt.symlink_to(Path("..") / "outside-prompt.md")
    _expect_source_reason(config, run_root, "SOURCE_PATH_ESCAPE")


def test_spec_symlink_escape_is_blocked(tmp_path: Path) -> None:
    enclosed = tmp_path / "enclosed"
    config, run_root = _load_tree(enclosed)
    leaked = tmp_path / "outside-spec" / "leaked.md"
    _write_utf8(leaked, "escaped spec\n")
    spec = enclosed / "tasks" / "synthetic-task-001.md"
    spec.unlink()
    spec.symlink_to(leaked)
    _expect_contract_reason(config, run_root, "TASK_PATH_ESCAPE")


def test_composed_home_class_prompt_symlink_escape_is_blocked(
    tmp_path: Path,
) -> None:
    enclosed = tmp_path / "enclosed"
    config, run_root = _load_tree(enclosed)
    prompt = enclosed / "prompts" / "validator.md"
    prompt.unlink()
    prompt.symlink_to(
        _composed_absolute("home", "synthetic-operator", "leaked.md")
    )
    _expect_source_reason(config, run_root, "SOURCE_PATH_ESCAPE")


def test_composed_users_class_prompt_symlink_escape_is_blocked(
    tmp_path: Path,
) -> None:
    enclosed = tmp_path / "enclosed"
    config, run_root = _load_tree(enclosed)
    prompt = enclosed / "prompts" / "developer.md"
    prompt.unlink()
    prompt.symlink_to(
        _composed_absolute("Users", "synthetic-operator", "leaked.md")
    )
    _expect_source_reason(config, run_root, "SOURCE_PATH_ESCAPE")


def test_missing_spec_source_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    (config.task_source_root / "tasks" / "synthetic-task-001.md").unlink()
    _expect_contract_reason(config, run_root, "TASK_SOURCE_MISSING")


def test_missing_prompt_after_config_load_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    config.prompts.developer.unlink()
    _expect_source_reason(config, run_root, "SOURCE_MISSING")


def test_broken_prompt_symlink_after_config_load_is_blocked(
    tmp_path: Path,
) -> None:
    config, run_root = _load_tree(tmp_path)
    prompt = config.prompts.orchestrator
    prompt.unlink()
    prompt.symlink_to(tmp_path / "missing-prompt.md")
    _expect_source_reason(config, run_root, "SOURCE_MISSING")


def test_broken_spec_symlink_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    spec = config.task_source_root / "tasks" / "synthetic-task-001.md"
    spec.unlink()
    spec.symlink_to(tmp_path / "missing-spec.md")
    _expect_contract_reason(config, run_root, "TASK_SOURCE_MISSING")


def test_unreadable_spec_source_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    spec = config.task_source_root / "tasks" / "synthetic-task-001.md"
    original_mode = spec.stat().st_mode
    try:
        _make_unreadable(spec)
        _expect_source_reason(config, run_root, "SOURCE_UNREADABLE")
    finally:
        if spec.is_file():
            spec.chmod(original_mode)


def test_unreadable_prompt_after_config_load_is_blocked(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    prompt = config.prompts.validator
    original_mode = prompt.stat().st_mode
    try:
        _make_unreadable(prompt)
        _expect_source_reason(config, run_root, "SOURCE_UNREADABLE")
    finally:
        if prompt.is_file():
            prompt.chmod(original_mode)


def test_prompt_symlink_inside_config_root_is_accepted(tmp_path: Path) -> None:
    config_path = _write_source_tree(tmp_path)
    canonical = tmp_path / "prompts" / "canonical-developer.md"
    _write_utf8(canonical, _PROMPT_BODIES["developer"])
    developer = tmp_path / "prompts" / "developer.md"
    developer.unlink()
    developer.symlink_to(canonical)
    config = load_config(config_path)
    facts = _materialize(config, _run_root(tmp_path))

    assert read_instruction_bytes(
        facts,
        _PROMPT_LOGICAL_REFS["developer"],
    ) == _PROMPT_BODIES["developer"].encode("utf-8")


def test_spec_symlink_inside_task_source_root_is_accepted(tmp_path: Path) -> None:
    config_path = _write_source_tree(tmp_path)
    canonical = tmp_path / "tasks" / "canonical-spec.md"
    _write_utf8(canonical, _SPEC_BODY)
    spec = tmp_path / "tasks" / "synthetic-task-001.md"
    spec.unlink()
    spec.symlink_to(canonical)
    config = load_config(config_path)
    facts = _materialize(config, _run_root(tmp_path))

    assert read_instruction_bytes(facts, _SPEC_LOGICAL_REF) == _SPEC_BODY.encode("utf-8")


def test_instruction_bindings_are_immutable(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)

    with pytest.raises(AttributeError):
        facts.sources = ()  # type: ignore[misc]
    binding = facts.sources[0]
    assert isinstance(binding, InstructionSourceBinding)
    with pytest.raises(AttributeError):
        binding.sha256 = "0" * 64  # type: ignore[misc]


def test_public_loader_has_no_queue_planner_epoch_or_authority_hooks() -> None:
    parameters = inspect.signature(load_task_source).parameters

    assert "config" in parameters
    assert "run_root" in parameters
    assert "previous_facts" in parameters
    assert "queue" not in parameters
    assert "planner" not in parameters
    assert "priority" not in parameters
    assert "sandbox" not in parameters
    assert "origin_run_id" not in parameters
    assert "drain_run_id" not in parameters
    assert "promotion_attempt_id" not in parameters
    assert "decision" not in parameters
    assert "epoch" not in parameters
    assert "adapter" not in parameters
    reader = inspect.signature(read_instruction_bytes).parameters
    assert "facts" in reader
    assert "logical_ref" in reader


def test_snapshot_module_is_not_part_of_the_public_surface() -> None:
    assert (
        importlib.util.find_spec("task_transition_runner.instruction_snapshot") is None
    )
    assert importlib.util.find_spec("task_transition_runner.snapshot") is None
    module_path = Path(inspect.getfile(load_task_source)).name
    assert module_path == "task_source.py"


def test_persistence_stays_under_the_explicit_run_root(tmp_path: Path) -> None:
    config, run_root = _load_tree(tmp_path)
    facts = _materialize(config, run_root)

    assert facts.run_root == run_root.resolve()
    assert (run_root / _SPEC_SNAPSHOT_PATH).is_file()
    assert not (config.runtime_root / "instructions").exists()
    assert not (config.target_worktree / "instructions").exists()
    assert not (config.task_source_root / "instructions").exists()
    for path in run_root.rglob("*"):
        if path.is_file():
            assert path.resolve().is_relative_to(run_root.resolve())
