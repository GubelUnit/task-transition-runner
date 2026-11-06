"""Synthetic public fresh-activation and terminal-rest contract.

The public surface is intentionally small and explicit:

- ``evaluate_task_activation`` consumes already loaded public task-source
  contract ``TaskSourceFacts``. It does not parse a queue, invent a task, or
  select among tasks.
- ``active_task: null`` after a settled predecessor is terminal rest: no
  Git mutation, no Start-TICK, no task branch, and no continuation
  consumption.
- Exactly one external ``active_task`` may become exactly one fresh
  origin/attempt identity bound to the immutable task fingerprint and
  instruction-snapshot sources.
- An identical already-settled task is a no-op only when ``task_id``,
  contract fingerprint, and the immutable public task-source contract
  ``instruction_sources`` signature all match. That signature is ``logical_ref``, ``sha256``,
  and ``byte_length``. ``snapshot_path`` is run-local persistence, not
  identity. Spec- or prompt-SHA drift is identity drift. A non-empty
  list of the same mappings is a valid sequence, like a tuple.
- Settled identity with a ``task_id`` is closed: fingerprint and source
  signature must be present and well-typed. ``facts`` must be
  ``TaskSourceFacts``; predecessor keys and origin/attempt are closed.
- Repeated evaluate of the same unpersisted fresh task is not an
  identity-stability contract here. Start-TICK remains a later task.
- Start-TICK, task-branch, continuation lifecycle, recovery, and
  private staging are not this module.
- Crash-stable activation identity is a separate closed tagged union.
  ``FRESH_INIT_BASELINE`` binds the canonical initialized main state and
  forbids a predecessor. ``SETTLED_PROMOTION`` binds the settled
  predecessor identity. Origin and attempt are domain-separated SHA-256
  digests of that payload, stable across processes, and never UUIDs.
"""

from __future__ import annotations

import copy
import hashlib
import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.config import RunnerConfig, load_config
from task_transition_runner.task_source import (
    InstructionSourceBinding,
    TaskSourceFacts,
    load_task_source,
)
from task_transition_runner.task_activation import (
    ActivationDecision,
    ActivationIdentity,
    ActivationStatus,
    TaskActivationError,
    evaluate_task_activation,
)

_PROMPT_ROLES = ("developer", "orchestrator", "validator")
_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_TASK_ID = "synthetic-task-001"
_SUCCESSOR_TASK_ID = "synthetic-task-002"
_REFERENCE_SPEC_REF = "./tasks/synthetic-task-001.md"
_SUCCESSOR_SPEC_REF = "./tasks/synthetic-task-002.md"
_REFERENCE_ALLOWED_PATHS = ("src/example.py", "tests/test_example.py")
_SPEC_BODY = "synthetic spec body bound once\n"
_SUCCESSOR_SPEC_BODY = "synthetic successor spec body bound once\n"
_PROMPT_BODIES = {
    "developer": "synthetic developer prompt\n",
    "orchestrator": "synthetic orchestrator prompt\n",
    "validator": "synthetic validator prompt\n",
}
_SETTLED_ORIGIN_RUN_ID = "origin-run-settled-001"
_SETTLED_PROMOTION_ATTEMPT_ID = "promotion-attempt-settled-001"
_SETTLED_HEAD_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_FOREIGN_SOURCE_SHA = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
_SPEC_LOGICAL_REF = _REFERENCE_SPEC_REF
_PROMPT_LOGICAL_REFS = (
    "./prompts/developer.md",
    "./prompts/orchestrator.md",
    "./prompts/validator.md",
)
_INSTRUCTION_LOGICAL_REFS = (_SPEC_LOGICAL_REF, *_PROMPT_LOGICAL_REFS)
_UNSET = object()
_MAIN_STATE_NAME = "main_state.yaml"
_CONTINUATION_MARKER_NAME = "continuation_contract.yaml"
_REQUIRED_EXPORTS = (
    "ActivationDecision",
    "ActivationIdentity",
    "ActivationStatus",
    "TaskActivationError",
    "evaluate_task_activation",
)
_FORBIDDEN_EXPORTS = (
    "consume_continuation",
    "create_task_branch",
    "invent_task",
    "plan_task",
    "select_next_task",
    "start_tick",
    "transition_main_state_for_start_tick",
)
_FORBIDDEN_STATUS_VALUES = {
    "CONSUMED",
    "RECOVERED",
    "REOPENED",
    "START_TICK",
    "TASK_BRANCH",
}
_FORBIDDEN_PARAMETER_NAMES = {
    "default_task",
    "planner",
    "queue",
    "successor_intent",
    "tick_sha",
}
_FORBIDDEN_TREE_NAMES = {
    "start_tick.yaml",
    "task_branch",
}
_EVALUATE_REQUIRED_PARAMETERS = ("facts", "predecessor")
_STAGING_TEXT = "synthetic continuation marker must remain unread\n"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _null_task_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": None,
    }


def _active_task_body(
    *,
    task_id: str = _REFERENCE_TASK_ID,
    spec_ref: str = _REFERENCE_SPEC_REF,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "spec_ref": spec_ref,
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
    }


def _active_task_payload(
    *,
    task_id: str = _REFERENCE_TASK_ID,
    spec_ref: str = _REFERENCE_SPEC_REF,
) -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": _active_task_body(task_id=task_id, spec_ref=spec_ref),
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


def _dump_yaml(path: Path, payload: dict[str, Any]) -> Path:
    return _write_utf8(path, yaml.safe_dump(payload, sort_keys=False))


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _init_target_repo(repo: Path) -> str:
    repo.mkdir(parents=True, exist_ok=True)
    _write_utf8(repo / "src" / "example.py", "print('synthetic')\n")
    _write_utf8(repo / "tests" / "test_example.py", "def test_synthetic() -> None:\n    assert True\n")
    subprocess.run(["git", "init", "-b", _REFERENCE_MAIN_BRANCH], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=synthetic",
            "-c",
            "user.email=synthetic@example.test",
            "add",
            "src/example.py",
            "tests/test_example.py",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=synthetic",
            "-c",
            "user.email=synthetic@example.test",
            "commit",
            "-m",
            "synthetic baseline",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    return _git(repo, "rev-parse", "HEAD")


def _write_source_tree(
    root: Path,
    *,
    task_payload: dict[str, Any] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    repo = root / "fixture-repo"
    _init_target_repo(repo)
    runtime = root / "runner-runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    _dump_yaml(
        runtime / _MAIN_STATE_NAME,
        {
            "schema_version": _REFERENCE_SCHEMA_VERSION,
            "state": "GREEN_VALIDATED",
            "main_branch": _REFERENCE_MAIN_BRANCH,
            "validated_result_sha": _SETTLED_HEAD_SHA,
            "last_tock_sha": _SETTLED_HEAD_SHA,
            "observed_main_head_sha": _SETTLED_HEAD_SHA,
            "reason_code": "GREEN_VALIDATED",
            "updated_at_utc": "2026-01-01T00:00:00Z",
        },
    )
    _write_utf8(runtime / _CONTINUATION_MARKER_NAME, _STAGING_TEXT)
    document = _active_task_payload() if task_payload is None else copy.deepcopy(task_payload)
    active = document.get("active_task")
    if isinstance(active, dict):
        spec_ref = str(active["spec_ref"])
        spec_text = _SUCCESSOR_SPEC_BODY if task_id_of(document) == _SUCCESSOR_TASK_ID else _SPEC_BODY
        _write_utf8(root / Path(spec_ref.removeprefix("./")), spec_text)
    _dump_yaml(root / "tasks.yaml", document)
    for role in _PROMPT_ROLES:
        _write_utf8(root / "prompts" / f"{role}.md", _PROMPT_BODIES[role])
    return _dump_yaml(root / "runner.yaml", _config_payload())


def task_id_of(document: dict[str, Any]) -> str | None:
    active = document.get("active_task")
    if not isinstance(active, dict):
        return None
    task_id = active.get("task_id")
    return task_id if isinstance(task_id, str) else None


def _load_tree(
    root: Path,
    *,
    task_payload: dict[str, Any] | None = None,
) -> tuple[RunnerConfig, Path, Path]:
    config_path = _write_source_tree(root, task_payload=task_payload)
    config = load_config(config_path)
    run_root = root / "synthetic-run"
    return config, run_root, root / "fixture-repo"


def _materialize(
    config: RunnerConfig,
    run_root: Path,
) -> TaskSourceFacts:
    return load_task_source(config, run_root=run_root)


def _instruction_signature(
    sources: tuple[InstructionSourceBinding, ...],
) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "byte_length": item.byte_length,
        }
        for item in sources
    )


def _with_source_sha(
    sources: tuple[dict[str, Any], ...],
    logical_ref: str,
    sha256: str,
) -> tuple[dict[str, Any], ...]:
    updated: list[dict[str, Any]] = []
    found = False
    for item in sources:
        clone = dict(item)
        if clone["logical_ref"] == logical_ref:
            clone["sha256"] = sha256
            found = True
        updated.append(clone)
    if not found:
        raise AssertionError(f"missing instruction source {logical_ref}")
    return tuple(updated)


def _settled_predecessor(
    *,
    task_id: str | None = None,
    task_fingerprint: object = _UNSET,
    instruction_sources: object = _UNSET,
    origin_run_id: object = _UNSET,
    promotion_attempt_id: object = _UNSET,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "settled": True,
    }
    if origin_run_id is _UNSET:
        payload["origin_run_id"] = _SETTLED_ORIGIN_RUN_ID
    else:
        payload["origin_run_id"] = origin_run_id
    if promotion_attempt_id is _UNSET:
        payload["promotion_attempt_id"] = _SETTLED_PROMOTION_ATTEMPT_ID
    else:
        payload["promotion_attempt_id"] = promotion_attempt_id
    if task_id is not None:
        payload["task_id"] = task_id
    if task_fingerprint is not _UNSET:
        payload["task_fingerprint"] = task_fingerprint
    if instruction_sources is not _UNSET:
        payload["instruction_sources"] = instruction_sources
    return payload


def _unresolved_predecessor() -> dict[str, Any]:
    return {
        "settled": False,
        "origin_run_id": _SETTLED_ORIGIN_RUN_ID,
        "promotion_attempt_id": _SETTLED_PROMOTION_ATTEMPT_ID,
    }


def _tree_digest(root: Path) -> dict[str, str]:
    digest: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if ".git" in path.parts:
            continue
        digest[path.relative_to(root).as_posix()] = _sha256(path.read_bytes())
    return digest


def _git_observation(repo: Path) -> dict[str, str]:
    return {
        "head": _git(repo, "rev-parse", "HEAD"),
        "branch": _git(repo, "symbolic-ref", "HEAD"),
        "porcelain": _git(repo, "status", "--porcelain"),
        "refs": _git(repo, "for-each-ref", "--format=%(refname) %(objectname)"),
    }


def _evaluate(
    facts: TaskSourceFacts,
    predecessor: dict[str, Any],
) -> ActivationDecision:
    return evaluate_task_activation(facts, predecessor=predecessor)


def _expect_reason(
    facts: object,
    predecessor: object,
    reason_code: str,
) -> TaskActivationError:
    with pytest.raises(TaskActivationError, match=reason_code) as exc_info:
        evaluate_task_activation(facts, predecessor=predecessor)  # type: ignore[arg-type]
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _expect_reason_without_effects(
    tmp_path: Path,
    repo: Path,
    facts: object,
    predecessor: object,
    reason_code: str,
) -> TaskActivationError:
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)
    error = _expect_reason(facts, predecessor, reason_code)
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git
    return error


def test_public_exports_are_the_named_activation_surface() -> None:
    module = inspect.getmodule(evaluate_task_activation)
    assert module is not None
    exported = {name for name in dir(module) if not name.startswith("_")}

    for name in _REQUIRED_EXPORTS:
        assert name in exported
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported
    assert "Queue" not in exported
    assert "Planner" not in exported


def test_activation_statuses_are_rest_activated_or_blocked() -> None:
    values = {status.value for status in ActivationStatus}

    assert ActivationStatus.TERMINAL_REST.value == "TERMINAL_REST"
    assert ActivationStatus.ACTIVATED.value == "ACTIVATED"
    assert ActivationStatus.BLOCKED.value == "BLOCKED"
    assert values == {"TERMINAL_REST", "ACTIVATED", "BLOCKED"}
    for forbidden in _FORBIDDEN_STATUS_VALUES:
        assert forbidden not in values


def test_evaluate_accepts_only_facts_and_predecessor() -> None:
    parameters = inspect.signature(evaluate_task_activation).parameters

    assert tuple(parameters) == _EVALUATE_REQUIRED_PARAMETERS
    assert parameters["facts"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["predecessor"].kind is inspect.Parameter.KEYWORD_ONLY
    for name in _FORBIDDEN_PARAMETER_NAMES:
        assert name not in parameters


def test_null_active_task_is_mutation_free_terminal_rest(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(facts, _settled_predecessor())

    assert facts.contract.active_task is None
    assert facts.sources == ()
    assert decision.status is ActivationStatus.TERMINAL_REST
    assert decision.reason_code == "ACTIVE_TASK_NULL"
    assert decision.identity is None
    assert decision.facts is facts
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git
    assert not (config.runtime_root / "start_tick.yaml").exists()
    for name in _FORBIDDEN_TREE_NAMES:
        assert not (tmp_path / name).exists()
        assert not (run_root / name).exists()
        assert not (config.runtime_root / name).exists()


def test_null_active_task_does_not_invent_work(tmp_path: Path) -> None:
    config, run_root, _repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)

    decision = _evaluate(facts, _settled_predecessor())

    assert decision.identity is None
    assert facts.contract.active_task is None
    assert not hasattr(decision, "task_id") or getattr(decision, "task_id", None) in {None, ""}


def test_fresh_external_task_binds_immutable_snapshots_and_fresh_identity(
    tmp_path: Path,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    live_spec = (config.task_source_root / "tasks" / "synthetic-task-001.md").resolve()
    original_spec = live_spec.read_bytes()
    live_spec.write_text("live spec mutation after snapshot\n", encoding="utf-8")
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(facts, _settled_predecessor())

    assert facts.contract.active_task is not None
    identity = decision.identity
    assert decision.status is ActivationStatus.ACTIVATED
    assert decision.reason_code == "FRESH_TASK_ACTIVATED"
    assert decision.facts is facts
    assert identity is not None
    assert isinstance(identity, ActivationIdentity)
    assert identity.task_id == _REFERENCE_TASK_ID
    assert identity.task_fingerprint == facts.contract.fingerprint
    assert identity.instruction_sources == facts.sources
    assert identity.origin_run_id != _SETTLED_ORIGIN_RUN_ID
    assert identity.promotion_attempt_id != _SETTLED_PROMOTION_ATTEMPT_ID
    assert identity.origin_run_id != identity.task_id
    assert identity.promotion_attempt_id != identity.origin_run_id
    assert identity.origin_run_id.strip() != ""
    assert identity.promotion_attempt_id.strip() != ""
    assert all(isinstance(item, InstructionSourceBinding) for item in identity.instruction_sources)
    assert tuple(item.sha256 for item in identity.instruction_sources) == tuple(
        item.sha256 for item in facts.sources
    )
    snapshot_spec = run_root / "instructions" / "task_spec"
    assert snapshot_spec.read_bytes() == original_spec
    assert snapshot_spec.read_bytes() != live_spec.read_bytes()
    after_tree = _tree_digest(tmp_path)
    assert after_tree[live_spec.relative_to(tmp_path.resolve()).as_posix()] == _sha256(
        live_spec.read_bytes()
    )
    assert after_tree[snapshot_spec.relative_to(tmp_path).as_posix()] == _sha256(original_spec)
    assert after_tree == before_tree
    assert _git_observation(repo) == before_git
    assert before_tree[f"runner-runtime/{_MAIN_STATE_NAME}"] == after_tree[
        f"runner-runtime/{_MAIN_STATE_NAME}"
    ]
    assert before_tree[f"runner-runtime/{_CONTINUATION_MARKER_NAME}"] == after_tree[
        f"runner-runtime/{_CONTINUATION_MARKER_NAME}"
    ]
    for name in _FORBIDDEN_TREE_NAMES:
        assert name not in after_tree
        assert not any(path == name or path.startswith(f"{name}/") for path in after_tree)


def test_fresh_activation_never_exposes_two_active_identities(tmp_path: Path) -> None:
    config, run_root, _repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)

    decision = _evaluate(facts, _settled_predecessor())

    identity = decision.identity
    assert identity is not None
    assert identity.task_id == facts.contract.active_task.task_id
    assert not hasattr(identity, "task_ids")
    assert not hasattr(decision, "identities")
    assert not hasattr(decision, "queue")


def test_successor_task_mints_identity_distinct_from_settled_predecessor(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_config, first_run, _first_repo = _load_tree(first_root)
    first_facts = _materialize(first_config, first_run)
    first = _evaluate(first_facts, _settled_predecessor())
    assert first.identity is not None

    second_config, second_run, _second_repo = _load_tree(
        second_root,
        task_payload=_active_task_payload(
            task_id=_SUCCESSOR_TASK_ID,
            spec_ref=_SUCCESSOR_SPEC_REF,
        ),
    )
    second_facts = _materialize(second_config, second_run)
    second = _evaluate(
        second_facts,
        _settled_predecessor(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint=first_facts.contract.fingerprint,
            instruction_sources=_instruction_signature(first_facts.sources),
        ),
    )

    assert second.status is ActivationStatus.ACTIVATED
    assert second.identity is not None
    assert second.identity.task_id == _SUCCESSOR_TASK_ID
    assert second.identity.task_id != first.identity.task_id
    assert second.identity.origin_run_id != first.identity.origin_run_id
    assert second.identity.promotion_attempt_id != first.identity.promotion_attempt_id
    assert second.identity.task_fingerprint == second_facts.contract.fingerprint
    assert second.identity.task_fingerprint != first.identity.task_fingerprint


def test_identical_settled_task_is_noop_terminal_rest(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(
        facts,
        _settled_predecessor(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint=facts.contract.fingerprint,
            instruction_sources=_instruction_signature(facts.sources),
        ),
    )

    assert decision.status is ActivationStatus.TERMINAL_REST
    assert decision.reason_code == "IDENTICAL_SETTLED_TASK"
    assert decision.identity is None
    assert decision.facts is facts
    assert facts.contract.active_task is not None
    assert facts.contract.active_task.task_id == _REFERENCE_TASK_ID
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def test_identical_settled_task_list_sequence_is_noop_terminal_rest(
    tmp_path: Path,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(
        facts,
        _settled_predecessor(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint=facts.contract.fingerprint,
            instruction_sources=list(_instruction_signature(facts.sources)),
        ),
    )

    assert decision.status is ActivationStatus.TERMINAL_REST
    assert decision.reason_code == "IDENTICAL_SETTLED_TASK"
    assert decision.identity is None
    assert decision.facts is facts
    assert facts.contract.active_task is not None
    assert facts.contract.active_task.task_id == _REFERENCE_TASK_ID
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def test_same_settled_task_id_with_fingerprint_drift_is_blocked(
    tmp_path: Path,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    drifted = "0" * 64
    assert drifted != facts.contract.fingerprint
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(
        facts,
        _settled_predecessor(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint=drifted,
            instruction_sources=_instruction_signature(facts.sources),
        ),
    )

    assert decision.status is ActivationStatus.BLOCKED
    assert decision.reason_code == "SETTLED_TASK_IDENTITY_DRIFT"
    assert decision.identity is None
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


@pytest.mark.parametrize("logical_ref", _INSTRUCTION_LOGICAL_REFS)
def test_same_settled_task_with_instruction_source_sha_drift_is_blocked(
    tmp_path: Path,
    logical_ref: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    sources = _instruction_signature(facts.sources)
    drifted = _with_source_sha(sources, logical_ref, _FOREIGN_SOURCE_SHA)
    assert drifted != sources
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(
        facts,
        _settled_predecessor(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint=facts.contract.fingerprint,
            instruction_sources=drifted,
        ),
    )

    assert decision.status is ActivationStatus.BLOCKED
    assert decision.reason_code == "SETTLED_TASK_IDENTITY_DRIFT"
    assert decision.identity is None
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


@pytest.mark.parametrize("omitted", ("task_fingerprint", "instruction_sources"))
def test_settled_task_identity_missing_fingerprint_or_sources_is_typed_missing(
    tmp_path: Path,
    omitted: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    predecessor = _settled_predecessor(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=_instruction_signature(facts.sources),
    )
    del predecessor[omitted]
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_MISSING",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_MISSING"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("task_fingerprint", None),
        ("task_fingerprint", ""),
        ("instruction_sources", None),
        ("instruction_sources", {}),
        ("instruction_sources", ()),
        ("instruction_sources", "sources"),
        (
            "instruction_sources",
            (
                {
                    "logical_ref": _SPEC_LOGICAL_REF,
                    "byte_length": 1,
                },
            ),
        ),
        (
            "instruction_sources",
            (
                {
                    "logical_ref": _SPEC_LOGICAL_REF,
                    "sha256": "",
                    "byte_length": 1,
                },
            ),
        ),
        (
            "instruction_sources",
            (
                {
                    "logical_ref": _SPEC_LOGICAL_REF,
                    "sha256": None,
                    "byte_length": 1,
                },
            ),
        ),
        ("instruction_sources", ("not-a-mapping",)),
    ),
)
def test_settled_task_identity_malformed_fingerprint_or_sources_is_typed(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    predecessor = _settled_predecessor(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=_instruction_signature(facts.sources),
    )
    predecessor[field] = value
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_TYPE",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_TYPE"


@pytest.mark.parametrize("task_id", (None, "", 1))
def test_settled_present_malformed_task_id_is_typed_without_effects(
    tmp_path: Path,
    task_id: object,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    predecessor = _settled_predecessor(
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=_instruction_signature(facts.sources),
    )
    predecessor["task_id"] = task_id
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_TYPE",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_TYPE"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("logical_ref", 1),
        ("byte_length", True),
        ("byte_length", "32"),
    ),
)
def test_settled_source_identity_field_types_are_typed_without_effects(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    sources = [dict(item) for item in _instruction_signature(facts.sources)]
    sources[0][field] = value
    predecessor = _settled_predecessor(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=tuple(sources),
    )
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_TYPE",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_TYPE"


def test_named_settled_identity_with_null_task_remains_terminal_rest(
    tmp_path: Path,
) -> None:
    named_root = tmp_path / "named"
    null_root = tmp_path / "null"
    named_config, named_run, named_repo = _load_tree(named_root)
    named_facts = _materialize(named_config, named_run)
    null_config, null_run, null_repo = _load_tree(
        null_root,
        task_payload=_null_task_payload(),
    )
    facts = _materialize(null_config, null_run)
    before_tree = _tree_digest(tmp_path)
    before_named_git = _git_observation(named_repo)
    before_null_git = _git_observation(null_repo)

    decision = _evaluate(
        facts,
        _settled_predecessor(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint=named_facts.contract.fingerprint,
            instruction_sources=_instruction_signature(named_facts.sources),
        ),
    )

    assert facts.contract.active_task is None
    assert named_facts.contract.active_task is not None
    assert named_facts.sources != ()
    assert decision.status is ActivationStatus.TERMINAL_REST
    assert decision.reason_code == "ACTIVE_TASK_NULL"
    assert decision.identity is None
    assert decision.facts is facts
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(named_repo) == before_named_git
    assert _git_observation(null_repo) == before_null_git


def test_unresolved_predecessor_blocks_new_external_task(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(facts, _unresolved_predecessor())

    assert facts.contract.active_task is not None
    assert decision.status is ActivationStatus.BLOCKED
    assert decision.reason_code == "UNRESOLVED_PREDECESSOR"
    assert decision.identity is None
    assert decision.facts is facts
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def test_unresolved_predecessor_does_not_reinterpret_null_as_rest(
    tmp_path: Path,
) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(facts, _unresolved_predecessor())

    assert decision.status is ActivationStatus.BLOCKED
    assert decision.reason_code == "UNRESOLVED_PREDECESSOR"
    assert decision.identity is None
    assert facts.contract.active_task is None
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def test_predecessor_must_be_a_mapping_with_settled_bool(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)

    _expect_reason_without_effects(tmp_path, repo, facts, None, "PREDECESSOR_FIELD_TYPE")
    _expect_reason_without_effects(tmp_path, repo, facts, [], "PREDECESSOR_FIELD_TYPE")
    _expect_reason_without_effects(tmp_path, repo, facts, {}, "PREDECESSOR_FIELD_MISSING")
    _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        {"settled": "true"},
        "PREDECESSOR_FIELD_TYPE",
    )


@pytest.mark.parametrize("facts_value", (None, {}, "facts", 1))
def test_non_task_source_facts_are_typed_without_effects(
    tmp_path: Path,
    facts_value: object,
) -> None:
    _config, _run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts_value,
        _settled_predecessor(),
        "FACTS_FIELD_TYPE",
    )
    assert error.reason_code == "FACTS_FIELD_TYPE"


@pytest.mark.parametrize("unknown_field", ("queue", "default_task", "tick_sha"))
def test_unknown_predecessor_fields_are_rejected_without_effects(
    tmp_path: Path,
    unknown_field: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)
    predecessor = _settled_predecessor()
    predecessor[unknown_field] = "synthetic-unknown"
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_UNKNOWN",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_UNKNOWN"


@pytest.mark.parametrize("omitted", ("origin_run_id", "promotion_attempt_id"))
def test_missing_origin_or_attempt_is_typed_missing_without_effects(
    tmp_path: Path,
    omitted: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)
    predecessor = _settled_predecessor()
    del predecessor[omitted]
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_MISSING",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_MISSING"


@pytest.mark.parametrize("field", ("origin_run_id", "promotion_attempt_id"))
@pytest.mark.parametrize("value", (None, ""))
def test_empty_origin_or_attempt_is_typed_without_effects(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)
    predecessor = _settled_predecessor()
    predecessor[field] = value
    error = _expect_reason_without_effects(
        tmp_path,
        repo,
        facts,
        predecessor,
        "PREDECESSOR_FIELD_TYPE",
    )
    assert error.reason_code == "PREDECESSOR_FIELD_TYPE"


def test_error_reason_code_is_the_stable_public_token() -> None:
    error = TaskActivationError("UNRESOLVED_PREDECESSOR")

    assert error.reason_code == "UNRESOLVED_PREDECESSOR"
    assert "UNRESOLVED_PREDECESSOR" in str(error)
    assert not hasattr(error, "private_code")


_ORIGIN_DOMAIN = "task-transition-runner/activation-origin/v1"
_ATTEMPT_DOMAIN = "task-transition-runner/promotion-attempt/v1"
_BOOTSTRAP_UPDATED_AT = "2026-09-10T20:00:00Z"
_DERIVED_PREDECESSOR_ORIGIN = "origin-" + ("ab" * 32)
_DERIVED_PREDECESSOR_ATTEMPT = "attempt-" + ("cd" * 32)


def _canonical_json(payload: dict[str, Any]) -> bytes:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    assert not encoded.endswith(b"\n")
    return encoded


def _domain_digest(domain: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(domain.encode("utf-8") + b"\x00" + _canonical_json(payload)).hexdigest()
    assert digest == digest.lower()
    assert len(digest) == 64
    return digest


def _facts_instruction_payload(facts: TaskSourceFacts) -> list[dict[str, Any]]:
    return [
        {
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "byte_length": item.byte_length,
        }
        for item in facts.sources
    ]


def _derivation_payload(facts: TaskSourceFacts, basis: dict[str, Any]) -> dict[str, Any]:
    active = facts.contract.active_task
    assert active is not None
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "task": {
            "task_id": active.task_id,
            "task_fingerprint": facts.contract.fingerprint,
            "instruction_sources": _facts_instruction_payload(facts),
        },
        "activation_basis": basis,
    }


def _derived_ids(facts: TaskSourceFacts, basis: dict[str, Any]) -> tuple[str, str]:
    payload = _derivation_payload(facts, basis)
    origin = "origin-" + _domain_digest(_ORIGIN_DOMAIN, payload)
    attempt = "attempt-" + _domain_digest(_ATTEMPT_DOMAIN, payload)
    assert origin != attempt
    return origin, attempt


def _fresh_main_state(repo: Path, *, updated_at_utc: str = _BOOTSTRAP_UPDATED_AT) -> dict[str, str]:
    head = _git(repo, "rev-parse", "HEAD")
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": _REFERENCE_MAIN_BRANCH,
        "validated_result_sha": head,
        "last_tock_sha": head,
        "observed_main_head_sha": head,
        "reason_code": "BOOTSTRAP_OK",
        "updated_at_utc": updated_at_utc,
    }


def _fresh_init_basis(repo: Path, *, updated_at_utc: str = _BOOTSTRAP_UPDATED_AT) -> dict[str, Any]:
    return {
        "kind": "FRESH_INIT_BASELINE",
        "main_state": _fresh_main_state(repo, updated_at_utc=updated_at_utc),
    }


def _settled_promotion_basis(
    *,
    task_id: str,
    task_fingerprint: str,
    instruction_sources: tuple[dict[str, Any], ...] | list[dict[str, Any]],
    origin_run_id: str = _DERIVED_PREDECESSOR_ORIGIN,
    promotion_attempt_id: str = _DERIVED_PREDECESSOR_ATTEMPT,
) -> dict[str, Any]:
    return {
        "kind": "SETTLED_PROMOTION",
        "predecessor": {
            "settled": True,
            "task_id": task_id,
            "origin_run_id": origin_run_id,
            "promotion_attempt_id": promotion_attempt_id,
            "task_fingerprint": task_fingerprint,
            "instruction_sources": list(instruction_sources),
        },
    }


def _basis_in_spawned_process(root: Path, run_name: str, basis: dict[str, Any]) -> dict[str, Any]:
    script = """
import json
import sys
from pathlib import Path

from task_transition_runner.config import load_config
from task_transition_runner.task_activation import (
    TaskActivationError,
    evaluate_task_activation,
)
from task_transition_runner.task_source import load_task_source

root = Path(sys.argv[1])
run_root = Path(sys.argv[2])
basis = json.loads(sys.argv[3])
facts = load_task_source(load_config(root / "runner.yaml"), run_root=run_root)
try:
    decision = evaluate_task_activation(facts, predecessor=basis)
except TaskActivationError as exc:
    print(json.dumps({"ok": False, "reason_code": exc.reason_code}))
else:
    identity = decision.identity
    if identity is None:
        print(json.dumps({"ok": False, "reason_code": decision.reason_code, "status": decision.status.value}))
    else:
        print(json.dumps({
            "ok": True,
            "origin": identity.origin_run_id,
            "attempt": identity.promotion_attempt_id,
            "reason_code": decision.reason_code,
        }))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(root), str(root / run_name), json.dumps(basis)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    assert isinstance(payload, dict)
    return payload


def test_fresh_init_basis_rejects_invented_predecessor_ids(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    basis = _fresh_init_basis(repo)
    basis["predecessor"] = {
        "origin_run_id": _DERIVED_PREDECESSOR_ORIGIN,
        "promotion_attempt_id": _DERIVED_PREDECESSOR_ATTEMPT,
    }
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    error = _expect_reason(facts, basis, "ACTIVATION_BASIS_INVALID")

    assert error.reason_code == "ACTIVATION_BASIS_INVALID"
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


@pytest.mark.parametrize(
    "mutate",
    (
        "extra_basis_key",
        "missing_main_state",
        "extra_main_state_key",
        "reason_not_bootstrap",
        "sha_inequality",
        "state_not_green",
        "schema_not_1",
        "settled_with_main_state",
        "settled_not_true",
        "unknown_kind",
        "source_extra_key",
        "source_missing_sha",
    ),
)
def test_closed_activation_basis_rejects_malformed_union(tmp_path: Path, mutate: str) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    if mutate == "settled_with_main_state":
        basis: dict[str, Any] = _settled_promotion_basis(
            task_id=_SUCCESSOR_TASK_ID,
            task_fingerprint="1" * 64,
            instruction_sources=_facts_instruction_payload(facts),
        )
        basis["main_state"] = _fresh_main_state(repo)
    elif mutate == "settled_not_true":
        basis = _settled_promotion_basis(
            task_id=_SUCCESSOR_TASK_ID,
            task_fingerprint="1" * 64,
            instruction_sources=_facts_instruction_payload(facts),
        )
        basis["predecessor"]["settled"] = False
    elif mutate == "unknown_kind":
        basis = _fresh_init_basis(repo)
        basis["kind"] = "REOPEN_BASELINE"
    elif mutate == "source_extra_key":
        sources = _facts_instruction_payload(facts)
        sources[0]["snapshot_path"] = "instructions/task_spec"
        basis = _settled_promotion_basis(
            task_id=_SUCCESSOR_TASK_ID,
            task_fingerprint="1" * 64,
            instruction_sources=sources,
        )
    elif mutate == "source_missing_sha":
        sources = _facts_instruction_payload(facts)
        del sources[0]["sha256"]
        basis = _settled_promotion_basis(
            task_id=_SUCCESSOR_TASK_ID,
            task_fingerprint="1" * 64,
            instruction_sources=sources,
        )
    else:
        basis = _fresh_init_basis(repo)
        if mutate == "extra_basis_key":
            basis["continuation"] = None
        elif mutate == "missing_main_state":
            del basis["main_state"]
        elif mutate == "extra_main_state_key":
            basis["main_state"]["predecessor"] = _DERIVED_PREDECESSOR_ORIGIN
        elif mutate == "reason_not_bootstrap":
            basis["main_state"]["reason_code"] = "GREEN_VALIDATED"
        elif mutate == "sha_inequality":
            basis["main_state"]["observed_main_head_sha"] = "b" * 40
        elif mutate == "state_not_green":
            basis["main_state"]["state"] = "BLOCKED"
        elif mutate == "schema_not_1":
            basis["main_state"]["schema_version"] = "2.0"
    _expect_reason_without_effects(tmp_path, repo, facts, basis, "ACTIVATION_BASIS_INVALID")


@pytest.mark.parametrize("mutate", ("missing_key", "extra_key"))
def test_settled_promotion_predecessor_keyset_is_closed(
    tmp_path: Path,
    mutate: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    basis = _settled_promotion_basis(
        task_id=_SUCCESSOR_TASK_ID,
        task_fingerprint="1" * 64,
        instruction_sources=_facts_instruction_payload(facts),
    )
    if mutate == "missing_key":
        del basis["predecessor"]["origin_run_id"]
    else:
        basis["predecessor"]["queue"] = "synthetic-extra"
    _expect_reason_without_effects(tmp_path, repo, facts, basis, "ACTIVATION_BASIS_INVALID")


def test_empty_settled_instruction_sources_are_not_a_complete_identity(
    tmp_path: Path,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    basis = _settled_promotion_basis(
        task_id=_SUCCESSOR_TASK_ID,
        task_fingerprint="1" * 64,
        instruction_sources=(),
    )
    _expect_reason_without_effects(tmp_path, repo, facts, basis, "ACTIVATION_BASIS_INVALID")


@pytest.mark.parametrize("kind", ("FRESH_INIT_BASELINE", "SETTLED_PROMOTION"))
def test_null_active_task_on_closed_basis_stays_rest_without_identity(
    tmp_path: Path,
    kind: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path, task_payload=_null_task_payload())
    facts = _materialize(config, run_root)
    if kind == "FRESH_INIT_BASELINE":
        basis: dict[str, Any] = _fresh_init_basis(repo)
    else:
        basis = _settled_promotion_basis(
            task_id=_REFERENCE_TASK_ID,
            task_fingerprint="1" * 64,
            instruction_sources=(
                {
                    "logical_ref": "tasks/synthetic.md",
                    "sha256": "a" * 64,
                    "byte_length": 1,
                },
            ),
        )
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    decision = _evaluate(facts, basis)

    assert facts.contract.active_task is None
    assert decision.status is ActivationStatus.TERMINAL_REST
    assert decision.reason_code == "ACTIVE_TASK_NULL"
    assert decision.identity is None
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def test_fresh_and_settled_bases_derive_domain_separated_ids_without_uuid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _forbid_uuid() -> str:
        raise AssertionError("uuid4 drift")

    monkeypatch.setattr("task_transition_runner.task_activation._uuid4", _forbid_uuid)
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    fresh = _fresh_init_basis(repo)
    expected_origin, expected_attempt = _derived_ids(facts, fresh)
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    first = _evaluate(facts, fresh)
    second = _evaluate(facts, fresh)

    assert first.status is ActivationStatus.ACTIVATED
    assert first.reason_code == "FRESH_TASK_ACTIVATED"
    assert first.identity is not None
    assert second.identity is not None
    assert first.identity.origin_run_id == second.identity.origin_run_id == expected_origin
    assert first.identity.promotion_attempt_id == second.identity.promotion_attempt_id == expected_attempt
    assert first.identity.instruction_sources == facts.sources
    settled = _settled_promotion_basis(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=_facts_instruction_payload(facts),
    )
    settled_origin, settled_attempt = _derived_ids(facts, settled)
    assert (settled_origin, settled_attempt) != (expected_origin, expected_attempt)
    drifted = _fresh_init_basis(repo, updated_at_utc="2026-09-10T21:00:00Z")
    drifted_origin, drifted_attempt = _derived_ids(facts, drifted)
    assert (drifted_origin, drifted_attempt) != (expected_origin, expected_attempt)
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def test_cross_process_basis_evaluation_is_byte_identical(tmp_path: Path) -> None:
    config, _run_root, repo = _load_tree(tmp_path)
    _materialize(config, tmp_path / "parent-run")
    basis = _fresh_init_basis(repo)
    facts = _materialize(config, tmp_path / "oracle-run")
    expected_origin, expected_attempt = _derived_ids(facts, basis)

    left = _basis_in_spawned_process(tmp_path, "process-a", basis)
    right = _basis_in_spawned_process(tmp_path, "process-b", basis)

    assert left == right == {
        "ok": True,
        "origin": expected_origin,
        "attempt": expected_attempt,
        "reason_code": "FRESH_TASK_ACTIVATED",
    }


def test_instruction_order_and_unicode_change_the_derived_identity(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    sources = _facts_instruction_payload(facts)
    ordered = _settled_promotion_basis(
        task_id="synthetic-task-predecessor",
        task_fingerprint="2" * 64,
        instruction_sources=sources,
    )
    ordered["predecessor"]["instruction_sources"][0]["logical_ref"] = "tasks/größe.md"
    reversed_sources = list(reversed(ordered["predecessor"]["instruction_sources"]))
    reversed_basis = _settled_promotion_basis(
        task_id="synthetic-task-predecessor",
        task_fingerprint="2" * 64,
        instruction_sources=reversed_sources,
    )
    expected_origin, expected_attempt = _derived_ids(facts, ordered)
    other_origin, other_attempt = _derived_ids(facts, reversed_basis)
    assert (expected_origin, expected_attempt) != (other_origin, other_attempt)

    decision = _evaluate(facts, ordered)

    assert decision.status is ActivationStatus.ACTIVATED
    assert decision.identity is not None
    assert decision.identity.origin_run_id == expected_origin
    assert decision.identity.promotion_attempt_id == expected_attempt


def test_identical_settled_basis_rests_and_fingerprint_drift_blocks(tmp_path: Path) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    sources = _facts_instruction_payload(facts)
    identical = _settled_promotion_basis(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=sources,
    )
    before_tree = _tree_digest(tmp_path)
    before_git = _git_observation(repo)

    rested = _evaluate(facts, identical)

    assert rested.status is ActivationStatus.TERMINAL_REST
    assert rested.reason_code == "IDENTICAL_SETTLED_TASK"
    assert rested.identity is None
    drifted = _settled_promotion_basis(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint="0" * 64,
        instruction_sources=sources,
    )
    blocked = _evaluate(facts, drifted)
    assert blocked.status is ActivationStatus.BLOCKED
    assert blocked.reason_code == "SETTLED_TASK_IDENTITY_DRIFT"
    assert blocked.identity is None
    assert _tree_digest(tmp_path) == before_tree
    assert _git_observation(repo) == before_git


def _cli_basis_selector() -> Any:
    import task_transition_runner.task_activation as activation

    selector = getattr(activation, "select_cli_activation_basis", None)
    assert callable(selector), "select_cli_activation_basis is not bound"
    return selector


def test_cli_basis_selector_uses_fresh_init_baseline_without_predecessor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.task_activation as activation

    _config, _run_root, repo = _load_tree(tmp_path)
    main_state = _fresh_main_state(repo)
    before = copy.deepcopy(main_state)
    monkeypatch.setattr(
        activation,
        "_uuid4",
        lambda: pytest.fail("fresh-init basis must not invent a uuid"),
    )

    selected = _cli_basis_selector()(main_state=main_state, settled_predecessor=None)

    assert selected["kind"] == "FRESH_INIT_BASELINE"
    assert set(selected) == {"kind", "main_state"}
    assert selected["main_state"] == before
    assert "origin_run_id" not in selected["main_state"]
    assert "promotion_attempt_id" not in selected["main_state"]
    assert main_state == before


def test_cli_basis_selector_uses_settled_promotion_without_main_state(
    tmp_path: Path,
) -> None:
    config, run_root, _repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    predecessor = _settled_promotion_basis(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=_facts_instruction_payload(facts),
    )["predecessor"]
    before = copy.deepcopy(predecessor)

    selected = _cli_basis_selector()(main_state=None, settled_predecessor=predecessor)

    assert selected["kind"] == "SETTLED_PROMOTION"
    assert set(selected) == {"kind", "predecessor"}
    assert selected["predecessor"] == before
    assert predecessor == before


@pytest.mark.parametrize(
    "case",
    ("mixed", "missing", "invented_origin", "invented_attempt"),
)
def test_cli_basis_selector_blocks_mixed_missing_and_invented_identity(
    tmp_path: Path,
    case: str,
) -> None:
    config, run_root, repo = _load_tree(tmp_path)
    facts = _materialize(config, run_root)
    main_state = _fresh_main_state(repo)
    predecessor = _settled_promotion_basis(
        task_id=_REFERENCE_TASK_ID,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=_facts_instruction_payload(facts),
    )["predecessor"]
    if case == "invented_origin":
        main_state = dict(main_state)
        main_state["origin_run_id"] = "origin-invented"
        predecessor = None
    elif case == "invented_attempt":
        main_state = dict(main_state)
        main_state["promotion_attempt_id"] = "attempt-invented"
        predecessor = None
    elif case == "missing":
        main_state = None
        predecessor = None
    before_main = copy.deepcopy(main_state)
    before_predecessor = copy.deepcopy(predecessor)

    with pytest.raises(TaskActivationError) as raised:
        _cli_basis_selector()(main_state=main_state, settled_predecessor=predecessor)

    assert raised.value.reason_code == "ACTIVATION_BASIS_INVALID"
    assert main_state == before_main
    assert predecessor == before_predecessor
