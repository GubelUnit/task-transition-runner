"""Activation-bound Start-TICK and task-branch materialization.

``reconcile_start_tick`` materializes exactly one Git-grounded Start-TICK
and one identity-bound task branch, or returns a typed no-effect block.
Identical retry of the same physical start is reused without rewriting
Git refs or canonical state bytes. A locally stale observed head is
completed through the existing Start-TICK CAS. This module does not
recover, reopen, consume a continuation, or invent a task.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from task_transition_runner.runner_git_context import (
    evaluate_branch_preflight,
    verify_runner_git_lock_ownership,
)
from task_transition_runner.runner_promotion_state import (
    RunnerPromotionStateError,
    load_and_validate_main_state,
    transition_main_state_for_start_tick,
)
from task_transition_runner.task_activation import (
    ActivationIdentity,
    ActivationStatus,
)

_TASK_BRANCH_PREFIX = "refs/heads/runner/task/"
_START_SUBJECT = "START"


class StartTickError(Exception):
    """Fail-closed violation of the public Start-TICK contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class StartTickStatus(Enum):
    """The only public Start-TICK outcomes."""

    MATERIALIZED = "MATERIALIZED"
    REUSED = "REUSED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class StartTickDecision:
    status: StartTickStatus
    reason_code: str
    identity: ActivationIdentity | None
    tick_sha: str | None
    task_branch: str | None
    observed_head_sha: str | None


def reconcile_start_tick(
    *,
    decision: object,
    target_worktree: Path | str,
    main_state_path: Path | str,
    lock_path: Path | str,
    ownership: dict[str, Any],
    main_branch: str = "main",
    before_main_adoption: Callable[[], None] | None = None,
    historical_task_ref: str | None = None,
) -> StartTickDecision:
    if decision.status is not ActivationStatus.ACTIVATED:
        return _blocked(
            reason_code=decision.reason_code,
            identity=decision.identity,
        )
    identity = decision.identity
    facts = decision.facts
    active = facts.contract.active_task
    if (
        identity.task_id != active.task_id
        or identity.task_fingerprint != facts.contract.fingerprint
        or identity.instruction_sources != facts.sources
    ):
        return _blocked(
            reason_code="START_TICK_IDENTITY_MISMATCH",
            identity=identity,
        )
    lock_gate = verify_runner_git_lock_ownership(
        lock_path=lock_path,
        ownership=ownership,
    )
    if lock_gate.get("ok") is not True:
        return _blocked(
            reason_code=lock_gate["reason_code"],
            identity=identity,
        )
    repo = Path(target_worktree)
    state_path = Path(main_state_path)
    try:
        payload = load_and_validate_main_state(main_state_path=state_path)
        if payload["state"] == "BLOCKED":
            return _blocked(
                reason_code="MAIN_STATE_BLOCKED",
                identity=identity,
            )
        if payload["main_branch"] != main_branch:
            return _blocked(
                reason_code="MAIN_STATE_DRIFT",
                identity=identity,
            )
        preflight = evaluate_branch_preflight(
            target_worktree=repo,
            main_branch=main_branch,
        )
        if preflight.get("ok") is not True:
            return _blocked(
                reason_code=preflight["reason_code"],
                identity=identity,
            )
        live_head = preflight["head_sha"]
        observed = payload["observed_main_head_sha"]
        return _reconcile_exact_ref(
            repo=repo,
            identity=identity,
            live_head=live_head,
            observed=observed,
            main_state_path=state_path,
            before_main_adoption=before_main_adoption,
            historical_task_ref=historical_task_ref,
        )
    except RunnerPromotionStateError as exc:
        return _blocked(
            reason_code=exc.reason_code,
            identity=identity,
        )


def _materialize_start_tick(
    *,
    repo: Path,
    identity: ActivationIdentity,
    parent_sha: str,
    main_state_path: Path,
    before_main_adoption: Callable[[], None] | None,
) -> StartTickDecision:
    tree = _git_stdout(repo, "rev-parse", f"{parent_sha}^{{tree}}")
    message = (
        f"{_START_SUBJECT} {identity.task_id} {identity.origin_run_id} "
        f"{identity.promotion_attempt_id} {parent_sha}"
    )
    tick_sha = _git_stdout(
        repo,
        "commit-tree",
        tree,
        "-p",
        parent_sha,
        "-m",
        message,
    )
    task_branch = _task_branch_name(identity)
    _git(repo, "update-ref", f"refs/heads/{task_branch}", tick_sha)
    if before_main_adoption is not None:
        before_main_adoption()
    _git(repo, "update-ref", "refs/heads/main", tick_sha, parent_sha)
    transition_main_state_for_start_tick(
        main_state_path=main_state_path,
        expected_base_sha=parent_sha,
        tick_sha=tick_sha,
    )
    return _decision(
        status=StartTickStatus.MATERIALIZED,
        reason_code="START_TICK_MATERIALIZED",
        identity=identity,
        tick_sha=tick_sha,
        task_branch=task_branch,
        observed_head_sha=tick_sha,
    )


def _reconcile_exact_ref(
    *,
    repo: Path,
    identity: ActivationIdentity,
    live_head: str,
    observed: str,
    main_state_path: Path,
    before_main_adoption: Callable[[], None] | None,
    historical_task_ref: str | None,
) -> StartTickDecision:
    exact_ref = f"{_TASK_BRANCH_PREFIX}{identity.task_id}/{identity.origin_run_id}"
    exact_sha = _git_optional(repo, "rev-parse", "--verify", "--quiet", exact_ref)
    if exact_sha is None:
        return _missing_exact_ref(
            repo=repo,
            identity=identity,
            live_head=live_head,
            observed=observed,
            exact_ref=exact_ref,
            main_state_path=main_state_path,
            before_main_adoption=before_main_adoption,
            historical_task_ref=historical_task_ref,
        )
    if _foreign_task_ref_exists(
        repo,
        exact_ref,
        live_head,
        historical_task_ref=historical_task_ref,
    ):
        return _blocked(
            reason_code="RECONCILIATION_START_TICK_CONFLICT",
            identity=identity,
        )
    recorded = _verified_start(repo, exact_sha, identity)
    if recorded is None:
        if live_head == exact_sha and _attempt_drift_on_exact_head(repo, exact_sha, identity):
            return _blocked(
                reason_code="START_TICK_IDENTITY_MISMATCH",
                identity=identity,
            )
        return _blocked(
            reason_code="RECONCILIATION_START_TICK_CONFLICT",
            identity=identity,
        )
    return _complete_bound_window(
        repo=repo,
        identity=identity,
        recorded=recorded,
        live_head=live_head,
        observed=observed,
        main_state_path=main_state_path,
        before_main_adoption=before_main_adoption,
    )


def _missing_exact_ref(
    *,
    repo: Path,
    identity: ActivationIdentity,
    live_head: str,
    observed: str,
    exact_ref: str,
    main_state_path: Path,
    before_main_adoption: Callable[[], None] | None,
    historical_task_ref: str | None,
) -> StartTickDecision:
    head_start = _verified_start(repo, live_head, identity)
    if head_start is not None:
        return _blocked(
            reason_code="RECONCILIATION_START_TICK_CONFLICT",
            identity=identity,
        )
    if _head_message_is_start(repo, live_head):
        return _blocked(
            reason_code="START_TICK_IDENTITY_MISMATCH",
            identity=identity,
        )
    if _foreign_task_ref_exists(
        repo,
        exact_ref,
        live_head,
        historical_task_ref=historical_task_ref,
    ):
        return _blocked(
            reason_code="RECONCILIATION_START_TICK_CONFLICT",
            identity=identity,
        )
    if _unreferenced_start_commit_exists(repo):
        return _blocked(
            reason_code="START_TICK_NO_CANONICAL_EFFECT",
            identity=identity,
        )
    if live_head != observed:
        return _blocked(
            reason_code="MAIN_STATE_DRIFT",
            identity=identity,
        )
    return _materialize_start_tick(
        repo=repo,
        identity=identity,
        parent_sha=live_head,
        main_state_path=main_state_path,
        before_main_adoption=before_main_adoption,
    )


def _complete_bound_window(
    *,
    repo: Path,
    identity: ActivationIdentity,
    recorded: dict[str, str],
    live_head: str,
    observed: str,
    main_state_path: Path,
    before_main_adoption: Callable[[], None] | None,
) -> StartTickDecision:
    tick_sha = recorded["tick_sha"]
    parent_sha = recorded["parent_sha"]
    task_branch = recorded["task_branch"]
    if live_head == parent_sha and observed == parent_sha:
        if before_main_adoption is not None:
            before_main_adoption()
        _git(repo, "update-ref", "refs/heads/main", tick_sha, parent_sha)
        transition_main_state_for_start_tick(
            main_state_path=main_state_path,
            expected_base_sha=parent_sha,
            tick_sha=tick_sha,
        )
        return _reused(identity, tick_sha, task_branch)
    if live_head == tick_sha and observed == parent_sha:
        transition_main_state_for_start_tick(
            main_state_path=main_state_path,
            expected_base_sha=parent_sha,
            tick_sha=tick_sha,
        )
        return _reused(identity, tick_sha, task_branch)
    if live_head == tick_sha and observed == tick_sha:
        return _reused(identity, tick_sha, task_branch)
    if live_head == tick_sha:
        return _blocked(
            reason_code="RECONCILIATION_MAIN_STATE_DRIFT",
            identity=identity,
        )
    return _blocked(
        reason_code="RECONCILIATION_START_TICK_CONFLICT",
        identity=identity,
    )


def _verified_start(
    repo: Path,
    sha: str,
    identity: ActivationIdentity,
) -> dict[str, str] | None:
    parent = _git_optional(repo, "rev-parse", "--verify", "--quiet", f"{sha}^")
    if parent is None:
        return None
    message = _git_stdout(repo, "log", "-1", "--format=%B", sha)
    expected = (
        f"{_START_SUBJECT} {identity.task_id} {identity.origin_run_id} "
        f"{identity.promotion_attempt_id} {parent}"
    )
    if message.splitlines()[0] != expected:
        return None
    tree = _git_stdout(repo, "rev-parse", f"{sha}^{{tree}}")
    parent_tree = _git_stdout(repo, "rev-parse", f"{parent}^{{tree}}")
    if tree != parent_tree:
        return None
    return {
        "parent_sha": parent,
        "tick_sha": sha,
        "task_branch": _task_branch_name(identity),
    }


def _head_message_is_start(repo: Path, sha: str) -> bool:
    message = _git_stdout(repo, "log", "-1", "--format=%B", sha)
    return message.startswith(f"{_START_SUBJECT} ")


def _foreign_task_ref_exists(
    repo: Path,
    exact_ref: str,
    live_head: str,
    *,
    historical_task_ref: str | None,
) -> bool:
    raw = _git_stdout(repo, "branch", "--format=%(refname) %(objectname)")
    prefix = _TASK_BRANCH_PREFIX
    for line in raw.splitlines():
        ref, sha = line.split()
        if not ref.startswith(prefix) or ref == exact_ref:
            continue
        if (
            ref == historical_task_ref
            and _git_optional(
                repo,
                "merge-base",
                "--is-ancestor",
                sha,
                live_head,
            )
            is not None
        ):
            continue
        return True
    return False


def _attempt_drift_on_exact_head(
    repo: Path,
    sha: str,
    identity: ActivationIdentity,
) -> bool:
    parent = _git_stdout(repo, "rev-parse", f"{sha}^")
    message = _git_stdout(repo, "log", "-1", "--format=%B", sha)
    prefix = f"{_START_SUBJECT} {identity.task_id} {identity.origin_run_id} "
    line = message.splitlines()[0]
    if not line.startswith(prefix) or not line.endswith(f" {parent}"):
        return False
    attempt = line[len(prefix) : -len(f" {parent}")]
    return attempt != identity.promotion_attempt_id


def _unreferenced_start_commit_exists(repo: Path) -> bool:
    completed = subprocess.run(
        ["git", "fsck", "--unreachable", "--no-reflogs"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    for line in (completed.stdout + completed.stderr).splitlines():
        sha = line.removeprefix("unreachable commit ").split()[0]
        message = _git_stdout(repo, "log", "-1", "--format=%B", sha)
        if message.startswith(f"{_START_SUBJECT} "):
            return True
    return False


def _reused(
    identity: ActivationIdentity,
    tick_sha: str,
    task_branch: str,
) -> StartTickDecision:
    return _decision(
        status=StartTickStatus.REUSED,
        reason_code="START_TICK_REUSED",
        identity=identity,
        tick_sha=tick_sha,
        task_branch=task_branch,
        observed_head_sha=tick_sha,
    )


def _git_optional(repo: Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _task_branch_name(identity: ActivationIdentity) -> str:
    return f"runner/task/{identity.task_id}/{identity.origin_run_id}"


def _blocked(
    *,
    reason_code: str,
    identity: ActivationIdentity | None,
) -> StartTickDecision:
    return _decision(
        status=StartTickStatus.BLOCKED,
        reason_code=reason_code,
        identity=identity,
        tick_sha=None,
        task_branch=None,
        observed_head_sha=None,
    )


def _decision(
    *,
    status: StartTickStatus,
    reason_code: str,
    identity: ActivationIdentity | None,
    tick_sha: str | None,
    task_branch: str | None,
    observed_head_sha: str | None,
) -> StartTickDecision:
    return StartTickDecision(
        status=status,
        reason_code=reason_code,
        identity=identity,
        tick_sha=tick_sha,
        task_branch=task_branch,
        observed_head_sha=observed_head_sha,
    )


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )


def _git_stdout(repo: Path, *args: str) -> str:
    return _git(repo, *args).stdout.strip()
