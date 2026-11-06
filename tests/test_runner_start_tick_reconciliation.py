"""Synthetic public activation-bound Start-TICK and task-branch contract.

Keep:
- one Git-grounded Start-TICK and one task branch bound to the exact
  ``ActivationIdentity`` from fresh activation and the observed Main head
- observed-head CAS through persisted ``main_state.yaml``
- local lock, Main branch, and promotion-state reproof before any effect
- identical retry of the same identity at the same observed head without a
  second TICK, branch rewrite, or canonical state rewrite

Drop:
- post-TOCK reconciliation, reopen, recovery, and continuation consumption
- private staging files, persona/spec trees, and a full public ``run``
- task invention over ``active_task: null`` or an unresolved predecessor
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import yaml

import test_task_activation as activation_tests
from task_transition_runner.runner_git_context import (
    acquire_runner_git_lock,
    build_runner_git_lock_payload,
    resolve_runner_git_context,
)
from task_transition_runner.runner_promotion_state import write_main_state
from task_transition_runner.task_activation import (
    ActivationDecision,
    ActivationIdentity,
    ActivationStatus,
    evaluate_task_activation,
)
from task_transition_runner.task_source import InstructionSourceBinding
from task_transition_runner.runner_start_tick_reconciliation import (
    StartTickDecision,
    StartTickError,
    StartTickStatus,
    reconcile_start_tick,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_UPDATED_AT = "2026-09-10T20:00:00Z"
_REFERENCE_LOCK_STARTED_AT = "2026-09-10T20:00:00Z"
_REFERENCE_LOCK_NOW = "2026-09-10T21:00:00Z"
_STALE_AFTER_SECONDS = 3600
_GIT_AUTHOR_NAME = "Start Tick Contract"
_GIT_AUTHOR_EMAIL = "start-tick@task-transition-runner.invalid"
_FOREIGN_TASK_ID = "synthetic-task-foreign"
_FOREIGN_FINGERPRINT = "foreign-task-fingerprint"
_FOREIGN_LOCK_RUN_ID = "lock-owner-synthetic-foreign"
_FOREIGN_LOCK_PID = 5252
_FOREIGN_MAIN_BRANCH = "trunk"
_ATTEMPT_ONLY_ID = "attempt-independent-001"
_MAIN_STATE_NAME = "main_state.yaml"
_REQUIRED_EXPORTS = (
    "StartTickDecision",
    "StartTickError",
    "StartTickStatus",
    "reconcile_start_tick",
)
_FORBIDDEN_EXPORTS = (
    "consume_continuation",
    "invent_task",
    "plan_task",
    "recover_start_tick",
    "reopen_start_tick",
    "select_next_task",
)
_RECONCILE_REQUIRED_PARAMETERS = (
    "decision",
    "target_worktree",
    "main_state_path",
    "lock_path",
    "ownership",
)
_RECONCILE_OPTIONAL_PARAMETERS = ("main_branch",)
_FORBIDDEN_PARAMETER_NAMES = {
    "allowed_paths",
    "default_task",
    "planner",
    "queue",
    "runner_git_context",
    "successor_intent",
    "task_id",
    "verify_lock_ownership",
}
_FORBIDDEN_STATUS_VALUES = {
    "CONSUMED",
    "RECOVERED",
    "REOPENED",
}


def _git(repo: Path, *args: str) -> str:
    return activation_tests._git(repo, *args)


def _commit_count(repo: Path) -> int:
    return int(_git(repo, "rev-list", "--count", "HEAD"))


def _head_refs(repo: Path) -> str:
    return _git(repo, "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads")


def _commit_message(repo: Path, sha: str) -> str:
    return _git(repo, "log", "-1", "--format=%B", sha)


def _task_branch_name(identity: ActivationIdentity) -> str:
    return f"runner/task/{identity.task_id}/{identity.origin_run_id}"


def _activated(world: dict[str, Any], identity: ActivationIdentity) -> ActivationDecision:
    return ActivationDecision(
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        facts=world["decision"].facts,
        identity=identity,
    )


def _foreign_sources(identity: ActivationIdentity) -> tuple[InstructionSourceBinding, ...]:
    first = identity.instruction_sources[0]
    return (
        InstructionSourceBinding(
            logical_ref=first.logical_ref,
            snapshot_path=first.snapshot_path,
            sha256=activation_tests._FOREIGN_SOURCE_SHA,
            byte_length=first.byte_length,
        ),
        *identity.instruction_sources[1:],
    )


def _bind_live_main_state(repo: Path, state_path: Path, *, state: str = "GREEN_VALIDATED") -> dict[str, str]:
    head = _git(repo, "rev-parse", "HEAD")
    payload = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": state,
        "main_branch": _REFERENCE_MAIN_BRANCH,
        "validated_result_sha": head,
        "last_tock_sha": head,
        "observed_main_head_sha": head,
        "reason_code": state,
        "updated_at_utc": _REFERENCE_UPDATED_AT,
    }
    return write_main_state(main_state_path=state_path, payload=payload)


def _acquire_lock(*, lock_path: Path, run_id: str) -> dict[str, Any]:
    acquired = acquire_runner_git_lock(
        lock_path=lock_path,
        run_id=run_id,
        pid=os.getpid(),
        started_at_utc=_REFERENCE_LOCK_STARTED_AT,
        now_utc=_REFERENCE_LOCK_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda owner_pid: owner_pid == os.getpid(),
    )
    assert acquired.get("ok") is True
    assert acquired.get("acquired") is True
    ownership = acquired["ownership"]
    assert isinstance(ownership, dict)
    return ownership


def _prepare_world(
    tmp_path: Path,
    *,
    task_payload: dict[str, Any] | None = None,
    predecessor: dict[str, Any] | None = None,
    acquire_lock: bool = True,
    write_state: bool = True,
    main_state: str = "GREEN_VALIDATED",
) -> dict[str, Any]:
    config, run_root, repo = activation_tests._load_tree(
        tmp_path,
        task_payload=task_payload,
    )
    _git(repo, "config", "user.name", _GIT_AUTHOR_NAME)
    _git(repo, "config", "user.email", _GIT_AUTHOR_EMAIL)
    facts = activation_tests._materialize(config, run_root)
    bound_predecessor = (
        activation_tests._settled_predecessor()
        if predecessor is None
        else predecessor
    )
    decision = evaluate_task_activation(facts, predecessor=bound_predecessor)
    context = resolve_runner_git_context(
        target_worktree=repo,
        runtime_root=config.runtime_root,
    )
    state_path = config.runtime_root / _MAIN_STATE_NAME
    if write_state:
        _bind_live_main_state(repo, state_path, state=main_state)
    elif state_path.exists():
        state_path.unlink()
    run_id = (
        decision.identity.origin_run_id
        if decision.identity is not None
        else "lock-owner-synthetic-rest"
    )
    ownership = (
        _acquire_lock(lock_path=Path(context["lock_path"]), run_id=run_id)
        if acquire_lock
        else build_runner_git_lock_payload(
            run_id=run_id,
            pid=os.getpid(),
            started_at_utc=_REFERENCE_LOCK_STARTED_AT,
        )
    )
    return {
        "config": config,
        "run_root": run_root,
        "repo": repo,
        "decision": decision,
        "state_path": state_path,
        "lock_path": Path(context["lock_path"]),
        "ownership": ownership,
        "observed_head": _git(repo, "rev-parse", "HEAD"),
    }


def _start(
    world: dict[str, Any],
    *,
    decision: ActivationDecision | None = None,
    historical_task_ref: str | None = None,
) -> StartTickDecision:
    bound = world["decision"] if decision is None else decision
    result = reconcile_start_tick(
        decision=bound,
        target_worktree=world["repo"],
        main_state_path=world["state_path"],
        lock_path=world["lock_path"],
        ownership=world["ownership"],
        main_branch=_REFERENCE_MAIN_BRANCH,
        historical_task_ref=historical_task_ref,
    )
    assert isinstance(result, StartTickDecision)
    return result


def _observation(world: dict[str, Any]) -> dict[str, Any]:
    repo = world["repo"]
    state_path: Path = world["state_path"]
    return {
        "git": activation_tests._git_observation(repo),
        "head": _git(repo, "rev-parse", "HEAD"),
        "branch": _git(repo, "rev-parse", "--abbrev-ref", "HEAD"),
        "commit_count": _commit_count(repo),
        "refs": _head_refs(repo),
        "state_bytes": state_path.read_bytes() if state_path.exists() else None,
        "lock_exists": world["lock_path"].exists(),
    }


def _workspace_digest(world: dict[str, Any]) -> dict[str, str]:
    return activation_tests._tree_digest(world["config"].runtime_root.parent)


def _runtime_digest(world: dict[str, Any]) -> dict[str, str]:
    return activation_tests._tree_digest(world["config"].runtime_root)


def _head_names(repo: Path) -> set[str]:
    raw = _git(repo, "for-each-ref", "--format=%(refname)", "refs/heads")
    return set(raw.splitlines()) if raw else set()


def test_public_exports_are_the_named_start_tick_surface() -> None:
    module = inspect.getmodule(reconcile_start_tick)
    assert module is not None
    exported = {name for name in dir(module) if not name.startswith("_")}

    for name in _REQUIRED_EXPORTS:
        assert name in exported
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported
    assert "Queue" not in exported
    assert "Planner" not in exported


def test_start_tick_statuses_are_materialized_reused_or_blocked() -> None:
    values = {status.value for status in StartTickStatus}

    assert StartTickStatus.MATERIALIZED.value == "MATERIALIZED"
    assert StartTickStatus.REUSED.value == "REUSED"
    assert StartTickStatus.BLOCKED.value == "BLOCKED"
    assert values == {"MATERIALIZED", "REUSED", "BLOCKED"}
    for forbidden in _FORBIDDEN_STATUS_VALUES:
        assert forbidden not in values


def test_reconcile_start_tick_takes_activation_identity_and_explicit_paths() -> None:
    parameters = inspect.signature(reconcile_start_tick).parameters

    assert tuple(name for name in parameters if name in _RECONCILE_REQUIRED_PARAMETERS) == (
        _RECONCILE_REQUIRED_PARAMETERS
    )
    for name in _RECONCILE_REQUIRED_PARAMETERS:
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[name].default is inspect.Parameter.empty
    for name in _RECONCILE_OPTIONAL_PARAMETERS:
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[name].default == _REFERENCE_MAIN_BRANCH
    for name in _FORBIDDEN_PARAMETER_NAMES:
        assert name not in parameters


def test_fresh_activation_identity_materializes_one_start_tick_and_task_branch(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    decision = world["decision"]
    identity = decision.identity
    repo = world["repo"]
    assert decision.status is ActivationStatus.ACTIVATED
    assert identity is not None
    before_count = _commit_count(repo)
    before_head = world["observed_head"]
    before_refs = _head_refs(repo)
    before_state = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    before_lock = world["lock_path"].read_bytes()
    before_runtime = _runtime_digest(world)
    state_rel = world["state_path"].relative_to(world["config"].runtime_root).as_posix()

    result = _start(world)

    tick_sha = result.tick_sha
    assert result.status is StartTickStatus.MATERIALIZED
    assert result.reason_code == "START_TICK_MATERIALIZED"
    assert result.identity is identity
    assert tick_sha is not None
    assert tick_sha != before_head
    assert result.observed_head_sha == tick_sha
    assert result.task_branch == _task_branch_name(identity)
    assert _git(repo, "rev-parse", "HEAD") == tick_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == _REFERENCE_MAIN_BRANCH
    assert _git(repo, "rev-parse", f"{tick_sha}^") == before_head
    assert _commit_count(repo) == before_count + 1
    assert _git(repo, "rev-parse", f"refs/heads/{result.task_branch}") == tick_sha
    assert _git(repo, "rev-parse", "refs/heads/main") == tick_sha
    after_refs = _head_refs(repo)
    assert before_refs != after_refs
    assert after_refs.count("\n") == 1
    message = _commit_message(repo, tick_sha)
    assert identity.task_id in message
    assert identity.origin_run_id in message
    assert identity.promotion_attempt_id in message
    assert before_head in message
    persisted = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    assert persisted["observed_main_head_sha"] == tick_sha
    assert persisted["last_tock_sha"] == before_state["last_tock_sha"]
    assert persisted["validated_result_sha"] == before_state["validated_result_sha"]
    assert persisted["state"] == "GREEN_VALIDATED"
    assert not (world["config"].runtime_root / "start_tick.yaml").exists()
    assert world["lock_path"].read_bytes() == before_lock
    after_runtime = _runtime_digest(world)
    added_runtime = set(after_runtime) - set(before_runtime)
    changed_runtime = {
        path for path, digest in after_runtime.items() if before_runtime.get(path) != digest
    }
    assert added_runtime == set()
    assert changed_runtime == {state_rel}


def test_identical_retry_at_the_same_observed_head_does_not_rewrite(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    first = _start(world)
    after_first = {
        "head": _git(world["repo"], "rev-parse", "HEAD"),
        "refs": _head_refs(world["repo"]),
        "state_bytes": world["state_path"].read_bytes(),
        "message": _commit_message(world["repo"], first.tick_sha or ""),
        "count": _commit_count(world["repo"]),
        "branch": _git(world["repo"], "rev-parse", f"refs/heads/{first.task_branch}"),
    }

    second = _start(world)

    assert first.status is StartTickStatus.MATERIALIZED
    assert second.status is StartTickStatus.REUSED
    assert second.reason_code == "START_TICK_REUSED"
    assert second.identity is world["decision"].identity
    assert second.tick_sha == first.tick_sha
    assert second.task_branch == first.task_branch
    assert second.observed_head_sha == first.tick_sha
    assert _git(world["repo"], "rev-parse", "HEAD") == after_first["head"]
    assert _head_refs(world["repo"]) == after_first["refs"]
    assert world["state_path"].read_bytes() == after_first["state_bytes"]
    assert _commit_count(world["repo"]) == after_first["count"]
    assert _commit_message(world["repo"], second.tick_sha or "") == after_first["message"]
    assert _git(world["repo"], "rev-parse", f"refs/heads/{first.task_branch}") == after_first["branch"]


def test_foreign_identity_does_not_create_a_second_start_tick(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path)
    first = _start(world)
    identity = world["decision"].identity
    assert identity is not None
    foreign = ActivationIdentity(
        task_id=_FOREIGN_TASK_ID,
        task_fingerprint=identity.task_fingerprint,
        instruction_sources=identity.instruction_sources,
        origin_run_id="origin-foreign-start",
        promotion_attempt_id="attempt-foreign-start",
    )
    drifted = ActivationDecision(
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        facts=world["decision"].facts,
        identity=foreign,
    )
    before_refs = _head_refs(world["repo"])
    before_state = world["state_path"].read_bytes()

    blocked = _start(world, decision=drifted)

    assert first.status is StartTickStatus.MATERIALIZED
    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert _git(world["repo"], "rev-parse", "HEAD") == first.tick_sha
    assert _head_refs(world["repo"]) == before_refs
    assert world["state_path"].read_bytes() == before_state
    assert _git(world["repo"], "rev-parse", f"refs/heads/{first.task_branch}") == first.tick_sha
    heads = _git(world["repo"], "for-each-ref", "--format=%(refname)", "refs/heads")
    assert f"refs/heads/{_task_branch_name(foreign)}" not in heads.splitlines()


def test_observed_head_drift_is_fail_closed_without_git_or_state_effect(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    repo = world["repo"]
    (repo / "src" / "example.py").write_text("print('drift')\n", encoding="utf-8")
    _git(repo, "add", "--", "src/example.py")
    _git(
        repo,
        "-c",
        f"user.name={_GIT_AUTHOR_NAME}",
        "-c",
        f"user.email={_GIT_AUTHOR_EMAIL}",
        "commit",
        "-m",
        "foreign observed-head drift",
    )
    before = _observation(world)
    before_tree = _workspace_digest(world)

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "MAIN_STATE_DRIFT"
    assert blocked.tick_sha is None
    assert blocked.task_branch is None
    after = _observation(world)
    assert after["head"] == before["head"]
    assert after["refs"] == before["refs"]
    assert after["state_bytes"] == before["state_bytes"]
    assert after["commit_count"] == before["commit_count"]
    assert _workspace_digest(world) == before_tree
    assert yaml.safe_load(before["state_bytes"] or b"")["observed_main_head_sha"] == world["observed_head"]


def test_wrong_branch_is_fail_closed_without_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path)
    _git(world["repo"], "checkout", "-b", "foreign/branch")
    before = _observation(world)

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "GIT_WRONG_BRANCH"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "--abbrev-ref", "HEAD") == "foreign/branch"
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]


def test_missing_lock_is_fail_closed_without_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path, acquire_lock=False)
    assert world["lock_path"].exists() is False
    before = _observation(world)

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "LOCK_OWNERSHIP_LOST"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == world["observed_head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert world["lock_path"].exists() is False


def test_missing_state_is_fail_closed_without_git_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path, write_state=False)
    before_git = activation_tests._git_observation(world["repo"])

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "MAIN_STATE_INVALID"
    assert blocked.tick_sha is None
    assert world["state_path"].exists() is False
    assert activation_tests._git_observation(world["repo"]) == before_git


def test_null_active_task_is_fail_closed_without_tick_or_branch(tmp_path: Path) -> None:
    world = _prepare_world(
        tmp_path,
        task_payload=activation_tests._null_task_payload(),
    )
    assert world["decision"].status is ActivationStatus.TERMINAL_REST
    assert world["decision"].identity is None
    before = _observation(world)

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "ACTIVE_TASK_NULL"
    assert blocked.identity is None
    assert blocked.tick_sha is None
    assert blocked.task_branch is None
    assert _observation(world)["head"] == before["head"]
    assert _observation(world)["refs"] == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert not (world["config"].runtime_root / "start_tick.yaml").exists()


def test_unresolved_predecessor_is_fail_closed_without_effect(tmp_path: Path) -> None:
    world = _prepare_world(
        tmp_path,
        predecessor=activation_tests._unresolved_predecessor(),
    )
    assert world["decision"].status is ActivationStatus.BLOCKED
    assert world["decision"].reason_code == "UNRESOLVED_PREDECESSOR"
    before = _observation(world)

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "UNRESOLVED_PREDECESSOR"
    assert blocked.tick_sha is None
    assert blocked.task_branch is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]


def test_activation_identity_drift_before_effect_is_fail_closed(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path)
    identity = world["decision"].identity
    assert identity is not None
    drifted_identity = ActivationIdentity(
        task_id=_FOREIGN_TASK_ID,
        task_fingerprint=identity.task_fingerprint,
        instruction_sources=identity.instruction_sources,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
    )
    drifted = ActivationDecision(
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        facts=world["decision"].facts,
        identity=drifted_identity,
    )
    before = _observation(world)

    blocked = _start(world, decision=drifted)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]


def test_blocked_main_state_is_fail_closed_without_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path, main_state="BLOCKED")
    before = _observation(world)

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "MAIN_STATE_BLOCKED"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]


def test_same_task_fresh_reevaluation_does_not_create_a_second_tick(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    first = _start(world)
    identity = world["decision"].identity
    assert identity is not None
    reevaluated = evaluate_task_activation(
        world["decision"].facts,
        predecessor=activation_tests._settled_predecessor(),
    )
    reevaluated_identity = reevaluated.identity
    assert reevaluated.status is ActivationStatus.ACTIVATED
    assert reevaluated_identity is not None
    assert reevaluated_identity.task_id == identity.task_id
    assert reevaluated_identity.task_fingerprint == identity.task_fingerprint
    assert reevaluated_identity.instruction_sources == identity.instruction_sources
    assert reevaluated_identity.origin_run_id != identity.origin_run_id
    assert reevaluated_identity.promotion_attempt_id != identity.promotion_attempt_id
    before = _observation(world)

    blocked = _start(world, decision=reevaluated)

    assert first.status is StartTickStatus.MATERIALIZED
    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert _git(world["repo"], "rev-parse", "HEAD") == first.tick_sha
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _head_names(world["repo"]) == {
        "refs/heads/main",
        f"refs/heads/{first.task_branch}",
    }
    assert f"refs/heads/{_task_branch_name(reevaluated_identity)}" not in _head_names(
        world["repo"]
    )

    attempt_only = _activated(
        world,
        replace(identity, promotion_attempt_id=_ATTEMPT_ONLY_ID),
    )
    attempt_blocked = _start(world, decision=attempt_only)

    assert attempt_only.identity is not None
    assert attempt_only.identity.origin_run_id == identity.origin_run_id
    assert attempt_only.identity.promotion_attempt_id != identity.promotion_attempt_id
    assert attempt_blocked.status is StartTickStatus.BLOCKED
    assert attempt_blocked.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]


def test_foreign_fingerprint_facts_mismatch_blocks_before_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path)
    identity = world["decision"].identity
    assert identity is not None
    drifted = _activated(
        world,
        replace(identity, task_fingerprint=_FOREIGN_FINGERPRINT),
    )
    before = _observation(world)

    blocked = _start(world, decision=drifted)

    facts = world["decision"].facts
    assert facts.contract.active_task is not None
    assert identity.task_id == facts.contract.active_task.task_id
    assert drifted.identity is not None
    assert drifted.identity.task_id == identity.task_id
    assert drifted.identity.task_fingerprint != facts.contract.fingerprint
    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]


def test_foreign_instruction_sources_facts_mismatch_blocks_before_effect(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    identity = world["decision"].identity
    assert identity is not None
    drifted = _activated(world, replace(identity, instruction_sources=_foreign_sources(identity)))
    before = _observation(world)

    blocked = _start(world, decision=drifted)

    assert drifted.identity is not None
    assert drifted.identity.task_id == identity.task_id
    assert drifted.identity.task_fingerprint == identity.task_fingerprint
    assert drifted.identity.instruction_sources != identity.instruction_sources
    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]


def test_existing_foreign_lock_ownership_is_fail_closed_without_effect(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path, acquire_lock=False)
    foreign = build_runner_git_lock_payload(
        run_id=_FOREIGN_LOCK_RUN_ID,
        pid=_FOREIGN_LOCK_PID,
        started_at_utc=_REFERENCE_LOCK_STARTED_AT,
    )
    world["lock_path"].write_text(json.dumps(foreign) + "\n", encoding="utf-8")
    assert world["ownership"]["run_id"] != foreign["run_id"]
    before = _observation(world)
    before_lock = world["lock_path"].read_bytes()

    blocked = _start(world)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "LOCK_OWNERSHIP_LOST"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert world["lock_path"].read_bytes() == before_lock
    assert json.loads(world["lock_path"].read_text(encoding="utf-8")) == foreign


def test_dirty_main_is_fail_closed_without_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path)
    (world["repo"] / "src" / "example.py").write_text("print('dirty main')\n", encoding="utf-8")
    before = _observation(world)

    blocked = _start(world)

    assert _git(world["repo"], "rev-parse", "--abbrev-ref", "HEAD") == _REFERENCE_MAIN_BRANCH
    assert before["git"]["porcelain"] != ""
    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "GIT_DIRTY"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]


def test_main_state_branch_drift_is_fail_closed_without_effect(tmp_path: Path) -> None:
    world = _prepare_world(tmp_path)
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["main_branch"] = _FOREIGN_MAIN_BRANCH
    write_main_state(main_state_path=world["state_path"], payload=payload)
    before = _observation(world)

    blocked = _start(world)

    assert _git(world["repo"], "rev-parse", "--abbrev-ref", "HEAD") == _REFERENCE_MAIN_BRANCH
    assert yaml.safe_load(before["state_bytes"] or b"")["main_branch"] == _FOREIGN_MAIN_BRANCH
    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "MAIN_STATE_DRIFT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]


def test_stale_observed_head_completes_local_cas_without_second_tick(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    parent = world["observed_head"]
    first = _start(world)
    tick_sha = first.tick_sha
    assert tick_sha is not None
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["observed_main_head_sha"] = parent
    write_main_state(main_state_path=world["state_path"], payload=payload)
    after_rewind = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    assert after_rewind["observed_main_head_sha"] == parent
    assert _git(world["repo"], "rev-parse", "HEAD") == tick_sha
    before_refs = _head_refs(world["repo"])
    before_count = _commit_count(world["repo"])
    before_message = _commit_message(world["repo"], tick_sha)

    reused = _start(world)

    assert first.status is StartTickStatus.MATERIALIZED
    assert reused.status is StartTickStatus.REUSED
    assert reused.reason_code == "START_TICK_REUSED"
    assert reused.tick_sha == tick_sha
    assert reused.task_branch == first.task_branch
    assert reused.observed_head_sha == tick_sha
    assert reused.identity is world["decision"].identity
    assert _git(world["repo"], "rev-parse", "HEAD") == tick_sha
    assert _head_refs(world["repo"]) == before_refs
    assert _commit_count(world["repo"]) == before_count
    assert _commit_message(world["repo"], tick_sha) == before_message
    persisted = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    assert persisted["observed_main_head_sha"] == tick_sha
    assert persisted["last_tock_sha"] == after_rewind["last_tock_sha"]


def test_start_tick_error_is_a_fail_closed_reason_carrier() -> None:
    error = StartTickError("START_TICK_IDENTITY_MISMATCH")
    assert isinstance(error, Exception)
    assert error.reason_code == "START_TICK_IDENTITY_MISMATCH"
    assert "START_TICK_IDENTITY_MISMATCH" in str(error)


def _exact_task_ref(identity: ActivationIdentity) -> str:
    return f"refs/heads/runner/task/{identity.task_id}/{identity.origin_run_id}"


def _derived_decision(world: dict[str, Any]) -> tuple[ActivationDecision, ActivationIdentity]:
    facts = world["decision"].facts
    basis = activation_tests._fresh_init_basis(world["repo"])
    origin_run_id, promotion_attempt_id = activation_tests._derived_ids(facts, basis)
    identity = ActivationIdentity(
        task_id=facts.contract.active_task.task_id,
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=facts.sources,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
    )
    decision = ActivationDecision(
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        facts=facts,
        identity=identity,
    )
    return decision, identity


def _start_object(repo: Path, parent_sha: str, identity: ActivationIdentity) -> str:
    tree = _git(repo, "rev-parse", f"{parent_sha}^{{tree}}")
    message = (
        f"START {identity.task_id} {identity.origin_run_id} "
        f"{identity.promotion_attempt_id} {parent_sha}"
    )
    return _git(repo, "commit-tree", tree, "-p", parent_sha, "-m", message)


def _record_git(monkeypatch: Any, exact_ref: str) -> list[tuple[str, ...]]:
    import task_transition_runner.runner_start_tick_reconciliation as start_tick

    seen: list[tuple[str, ...]] = []
    original = start_tick._git

    def _wrapped(repo: Path, *args: str) -> Any:
        seen.append(args)
        return original(repo, *args)

    monkeypatch.setattr(start_tick, "_git", _wrapped)
    assert exact_ref.startswith("refs/heads/runner/task/")
    return seen


def _assert_exact_ref_only(seen: list[tuple[str, ...]], exact_ref: str) -> None:
    for args in seen:
        assert "for-each-ref" not in args
        assert "show-ref" not in args
        assert "--all" not in args
        for arg in args:
            if arg == "refs/heads/runner/task":
                raise AssertionError(args)
            if arg.startswith("refs/heads/runner/task/") and arg != exact_ref:
                raise AssertionError(args)


def test_pre_main_adoption_interlock_failure_reuses_the_same_canonical_start(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    repo = world["repo"]
    exact_ref = _exact_task_ref(identity)
    predecessor_tock = world["observed_head"]
    predecessor_state = world["state_path"].read_bytes()
    before_count = _commit_count(repo)
    interlock_ticks: list[str] = []

    def _observe_pre_adoption_window() -> str:
        tick_sha = _git(repo, "rev-parse", exact_ref)
        interlock_ticks.append(tick_sha)
        assert _git(repo, "rev-parse", "refs/heads/main") == predecessor_tock
        assert world["state_path"].read_bytes() == predecessor_state
        return tick_sha

    def _fail_interlock() -> None:
        _observe_pre_adoption_window()
        raise RuntimeError("expected pre-main-adoption interlock failure")

    with pytest.raises(
        RuntimeError,
        match="expected pre-main-adoption interlock failure",
    ):
        reconcile_start_tick(
            decision=decision,
            target_worktree=repo,
            main_state_path=world["state_path"],
            lock_path=world["lock_path"],
            ownership=world["ownership"],
            main_branch=_REFERENCE_MAIN_BRANCH,
            before_main_adoption=_fail_interlock,
        )

    materialized_start = _git(repo, "rev-parse", exact_ref)
    assert interlock_ticks == [materialized_start]
    assert _git(repo, "rev-parse", "refs/heads/main") == predecessor_tock
    assert world["state_path"].read_bytes() == predecessor_state
    assert _commit_count(repo) == before_count

    def _complete_interlock() -> None:
        assert _observe_pre_adoption_window() == materialized_start

    completed = reconcile_start_tick(
        decision=decision,
        target_worktree=repo,
        main_state_path=world["state_path"],
        lock_path=world["lock_path"],
        ownership=world["ownership"],
        main_branch=_REFERENCE_MAIN_BRANCH,
        before_main_adoption=_complete_interlock,
    )

    assert completed.status is StartTickStatus.REUSED
    assert completed.reason_code == "START_TICK_REUSED"
    assert completed.tick_sha == materialized_start
    assert interlock_ticks == [materialized_start, materialized_start]
    assert _git(repo, "rev-parse", exact_ref) == materialized_start
    assert _git(repo, "rev-parse", "refs/heads/main") == materialized_start
    assert _commit_count(repo) == before_count + 1
    persisted = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    assert persisted["observed_main_head_sha"] == materialized_start
    assert persisted["last_tock_sha"] == yaml.safe_load(predecessor_state)["last_tock_sha"]


def test_unreferenced_start_object_has_no_canonical_effect(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    dangling = _start_object(world["repo"], world["observed_head"], identity)
    exact_ref = _exact_task_ref(identity)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    blocked = _start(world, decision=decision)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "START_TICK_NO_CANONICAL_EFFECT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    assert dangling != before["head"]
    _assert_exact_ref_only(seen, exact_ref)


def test_exact_task_ref_before_main_completes_main_and_state_once(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    tick = _start_object(world["repo"], parent, identity)
    exact_ref = _exact_task_ref(identity)
    _git(world["repo"], "update-ref", exact_ref, tick)
    seen = _record_git(monkeypatch, exact_ref)
    before_state = world["state_path"].read_bytes()
    before_exact_count = int(_git(world["repo"], "rev-list", "--count", exact_ref))

    completed = _start(world, decision=decision)

    assert completed.status is StartTickStatus.REUSED
    assert completed.reason_code == "START_TICK_REUSED"
    assert completed.tick_sha == tick
    assert completed.task_branch == _task_branch_name(identity)
    assert completed.identity is identity
    assert _git(world["repo"], "rev-parse", "HEAD") == tick
    assert _git(world["repo"], "rev-parse", exact_ref) == tick
    assert int(_git(world["repo"], "rev-list", "--count", exact_ref)) == before_exact_count
    reachable = set(_git(world["repo"], "rev-list", "HEAD").splitlines())
    parent_reachable = set(_git(world["repo"], "rev-list", parent).splitlines())
    assert reachable - parent_reachable == {tick}
    persisted = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    assert persisted["observed_main_head_sha"] == tick
    assert persisted["last_tock_sha"] == yaml.safe_load(before_state)["last_tock_sha"]
    _assert_exact_ref_only(seen, exact_ref)


def test_main_before_state_cas_updates_only_state(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    tick = _start_object(world["repo"], parent, identity)
    exact_ref = _exact_task_ref(identity)
    _git(world["repo"], "update-ref", exact_ref, tick)
    _git(world["repo"], "update-ref", "refs/heads/main", tick)
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["observed_main_head_sha"] = parent
    write_main_state(main_state_path=world["state_path"], payload=payload)
    seen = _record_git(monkeypatch, exact_ref)
    before_refs = _head_refs(world["repo"])
    before_count = _commit_count(world["repo"])

    completed = _start(world, decision=decision)

    assert completed.status is StartTickStatus.REUSED
    assert completed.reason_code == "START_TICK_REUSED"
    assert completed.tick_sha == tick
    assert _head_refs(world["repo"]) == before_refs
    assert _commit_count(world["repo"]) == before_count
    assert yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))["observed_main_head_sha"] == tick
    _assert_exact_ref_only(seen, exact_ref)


def test_complete_start_truth_is_reused_without_rewrite(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    tick = _start_object(world["repo"], parent, identity)
    exact_ref = _exact_task_ref(identity)
    _git(world["repo"], "update-ref", exact_ref, tick)
    _git(world["repo"], "update-ref", "refs/heads/main", tick)
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["observed_main_head_sha"] = tick
    write_main_state(main_state_path=world["state_path"], payload=payload)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    reused = _start(world, decision=decision)

    assert reused.status is StartTickStatus.REUSED
    assert reused.reason_code == "START_TICK_REUSED"
    assert reused.tick_sha == tick
    assert reused.task_branch == _task_branch_name(identity)
    assert _observation(world)["refs"] == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    _assert_exact_ref_only(seen, exact_ref)


def test_foreign_task_ref_blocks_without_mutation(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    foreign_ref = "refs/heads/runner/task/synthetic-task-foreign/origin-" + ("ef" * 32)
    foreign_tick = _start_object(world["repo"], parent, identity)
    _git(world["repo"], "update-ref", foreign_ref, foreign_tick)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    foreign = _start(world, decision=decision)

    assert foreign.status is StartTickStatus.BLOCKED
    assert foreign.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    _assert_exact_ref_only(seen, exact_ref)


def test_exact_task_ref_and_nonhistorical_foreign_ref_conflict_without_mutation(
    tmp_path: Path,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    exact_tick = _start_object(world["repo"], parent, identity)
    historical_ref = "refs/heads/runner/task/000-historical-task/historical-origin"
    foreign_ref = "refs/heads/runner/task/zzz-nonhistorical-task/nonhistorical-origin"
    _git(world["repo"], "update-ref", exact_ref, exact_tick)
    _git(world["repo"], "update-ref", historical_ref, parent)
    _git(world["repo"], "update-ref", foreign_ref, parent)
    before = _observation(world)

    blocked = _start(
        world,
        decision=decision,
        historical_task_ref=historical_ref,
    )

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", exact_ref) == exact_tick
    assert _git(world["repo"], "rev-parse", historical_ref) == parent
    assert _git(world["repo"], "rev-parse", foreign_ref) == parent
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]


def test_malformed_exact_ref_blocks_without_mutation(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    malformed = _git(
        world["repo"],
        "commit-tree",
        _git(world["repo"], "rev-parse", f"{parent}^{{tree}}"),
        "-p",
        parent,
        "-m",
        "MALFORMED",
    )
    _git(world["repo"], "update-ref", exact_ref, malformed)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    malformed_result = _start(world, decision=decision)

    assert malformed_result.status is StartTickStatus.BLOCKED
    assert malformed_result.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    _assert_exact_ref_only(seen, exact_ref)


def test_unreachable_non_start_commit_is_not_start_authority(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    dangling = _git(
        world["repo"],
        "commit-tree",
        _git(world["repo"], "rev-parse", f"{parent}^{{tree}}"),
        "-p",
        parent,
        "-m",
        "NOT-A-START",
    )
    seen = _record_git(monkeypatch, exact_ref)

    materialized = _start(world, decision=decision)

    assert materialized.reason_code != "START_TICK_NO_CANONICAL_EFFECT"
    assert materialized.status is StartTickStatus.MATERIALIZED
    assert materialized.tick_sha is not None
    assert materialized.tick_sha != dangling
    assert _git(world["repo"], "rev-parse", exact_ref) == materialized.tick_sha
    assert _git(world["repo"], "rev-parse", "HEAD") == materialized.tick_sha
    _assert_exact_ref_only(seen, exact_ref)


def test_malformed_exact_ref_conflicts_when_main_points_at_object(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    malformed = _git(
        world["repo"],
        "commit-tree",
        _git(world["repo"], "rev-parse", f"{parent}^{{tree}}"),
        "-p",
        parent,
        "-m",
        "MALFORMED",
    )
    _git(world["repo"], "update-ref", exact_ref, malformed)
    _git(world["repo"], "update-ref", "refs/heads/main", malformed)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    blocked = _start(world, decision=decision)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == malformed
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    _assert_exact_ref_only(seen, exact_ref)


def test_matching_start_on_main_without_exact_ref_conflicts_without_second_start(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    tick = _start_object(world["repo"], parent, identity)
    _git(world["repo"], "update-ref", "refs/heads/main", tick)
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["observed_main_head_sha"] = tick
    write_main_state(main_state_path=world["state_path"], payload=payload)
    seen = _record_git(monkeypatch, exact_ref)
    before_refs = _head_refs(world["repo"])
    before_count = _commit_count(world["repo"])
    before_state = world["state_path"].read_bytes()

    blocked = _start(world, decision=decision)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == tick
    assert _head_refs(world["repo"]) == before_refs
    assert world["state_path"].read_bytes() == before_state
    assert _commit_count(world["repo"]) == before_count
    missing_exact_ref = subprocess.run(
        ["git", "-C", str(world["repo"]), "rev-parse", "--verify", "--quiet", exact_ref],
        check=False,
        capture_output=True,
        text=True,
    )
    assert missing_exact_ref.returncode != 0
    _assert_exact_ref_only(seen, exact_ref)


def _parseable_mismatched_start(
    repo: Path,
    parent_sha: str,
    identity: ActivationIdentity,
    mismatch: str,
) -> str:
    tree = _git(repo, "rev-parse", f"{parent_sha}^{{tree}}")
    bound_parent = parent_sha
    attempt = identity.promotion_attempt_id
    if mismatch == "attempt":
        attempt = "attempt-" + ("ff" * 32)
    elif mismatch == "parent":
        bound_parent = _git(repo, "commit-tree", tree, "-p", parent_sha, "-m", "unrelated parent")
    elif mismatch == "tree":
        blob_path = repo.parent / "tick-tree-drift.txt"
        blob_path.write_text("tick tree drift\n", encoding="utf-8")
        blob = _git(repo, "hash-object", "-w", str(blob_path))
        built = subprocess.run(
            ["git", "-C", str(repo), "mktree"],
            input=f"100644 blob {blob}\ttick-tree-drift.txt\n",
            text=True,
            check=True,
            capture_output=True,
        )
        tree = built.stdout.strip()
    else:
        raise AssertionError(mismatch)
    message = (
        f"START {identity.task_id} {identity.origin_run_id} "
        f"{attempt} {bound_parent}"
    )
    return _git(repo, "commit-tree", tree, "-p", bound_parent, "-m", message)


@pytest.mark.parametrize("mismatch", ("attempt", "parent", "tree"))
def test_parseable_exact_ref_mismatch_blocks_without_mutation(
    tmp_path: Path,
    monkeypatch: Any,
    mismatch: str,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    mismatched = _parseable_mismatched_start(world["repo"], parent, identity, mismatch)
    _git(world["repo"], "update-ref", exact_ref, mismatched)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    blocked = _start(world, decision=decision)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    _assert_exact_ref_only(seen, exact_ref)


def test_main_drift_blocks_without_mutation(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    exact_ref = _exact_task_ref(identity)
    valid = _start_object(world["repo"], parent, identity)
    _git(world["repo"], "update-ref", exact_ref, valid)
    _git(world["repo"], "commit", "--allow-empty", "-m", "main drift")
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    drifted = _start(world, decision=decision)

    assert drifted.status is StartTickStatus.BLOCKED
    assert drifted.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert _git(world["repo"], "rev-parse", "HEAD") == before["head"]
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    _assert_exact_ref_only(seen, exact_ref)


def test_state_drift_after_main_blocks_without_rewrite(tmp_path: Path, monkeypatch: Any) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    tick = _start_object(world["repo"], parent, identity)
    exact_ref = _exact_task_ref(identity)
    _git(world["repo"], "update-ref", exact_ref, tick)
    _git(world["repo"], "update-ref", "refs/heads/main", tick)
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["observed_main_head_sha"] = "c" * 40
    write_main_state(main_state_path=world["state_path"], payload=payload)
    seen = _record_git(monkeypatch, exact_ref)
    before_refs = _head_refs(world["repo"])
    before_state = world["state_path"].read_bytes()
    before_count = _commit_count(world["repo"])

    blocked = _start(world, decision=decision)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "RECONCILIATION_MAIN_STATE_DRIFT"
    assert _head_refs(world["repo"]) == before_refs
    assert world["state_path"].read_bytes() == before_state
    assert _commit_count(world["repo"]) == before_count
    _assert_exact_ref_only(seen, exact_ref)


def test_state_ahead_of_main_conflicts_without_second_start(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    world = _prepare_world(tmp_path)
    decision, identity = _derived_decision(world)
    parent = world["observed_head"]
    tick = _start_object(world["repo"], parent, identity)
    exact_ref = _exact_task_ref(identity)
    _git(world["repo"], "update-ref", exact_ref, tick)
    payload = yaml.safe_load(world["state_path"].read_text(encoding="utf-8"))
    payload["observed_main_head_sha"] = tick
    write_main_state(main_state_path=world["state_path"], payload=payload)
    seen = _record_git(monkeypatch, exact_ref)
    before = _observation(world)

    blocked = _start(world, decision=decision)

    assert blocked.status is StartTickStatus.BLOCKED
    assert blocked.reason_code == "RECONCILIATION_START_TICK_CONFLICT"
    assert blocked.tick_sha is None
    assert _git(world["repo"], "rev-parse", "HEAD") == parent
    assert _git(world["repo"], "rev-parse", exact_ref) == tick
    assert _head_refs(world["repo"]) == before["refs"]
    assert world["state_path"].read_bytes() == before["state_bytes"]
    assert _commit_count(world["repo"]) == before["commit_count"]
    assert not any(arg == "commit-tree" for args in seen for arg in args)
    assert not any(arg == "update-ref" for args in seen for arg in args)
    _assert_exact_ref_only(seen, exact_ref)
