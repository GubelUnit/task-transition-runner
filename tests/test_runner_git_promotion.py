"""Synthetic public pending-intent and effect-admission contract.

Keep:
- persist a qualified ``NON_AUTHORIZING_PROMOTION_INTENT`` as
  ``pending_promotion.yaml``
- bind task, result, attempt, base, main, snapshot, decision, evidence,
  and expected_outcome facts
- store ``result_sha`` as later candidate-validation input, not as a
  validation proof
- identical persist is byte-stable
- conflicting persist fail-closes and does not overwrite
- ``admit_pending_promotion_effect`` is the only intent-to-effect entry
- normal journal creation copies the three already admitted pending anchors
  before ``commit-tree`` and keeps those exact values through FINALIZED
- a missing admitted anchor is not healed from a later journal, and a
  cross-attempt anchor drift blocks before canonical git progress
- all fifteen mechanical admission conditions are jointly reproofed immediately
  before the first canonical effect
- no-commit merge-candidate with MERGE_HEAD bound to result_sha while
  validation runs against the candidate index and working tree
- persisted runner validation binds command, cwd, time, timeout, exit,
  output, and the SHA-256 of those output bytes
- EXPECTED_RED success requires a non-zero exit and the failure signature
  inside the persisted output bytes
- native candidate-tree OID is identical before and after validation
- TOCK tree OID equals the frozen validated candidate-tree OID
- ``parse_canonical_tock`` is the only public parser for subject ``TOCK``,
  the six identity trailers each exactly once, parents ``[base, result]``,
  and the frozen validated tree
- a compact subject, one parent, extra identity metadata, a duplicate or
  missing trailer, and a foreign tree are rejected by that same parser
- unstaged, staged, index, hook, and identity drift block with no journal
  start, consumption, TOCK, or Main mutation, and restore Main cleanly
- live ``refs/heads/<task_branch>`` drift after pending, with kwargs still on
  the bound ``result_sha``, is a stable result/branch block with no effects
- no-commit merge conflict does not leak ``CalledProcessError``; Main, index,
  working tree, and ``MERGE_HEAD`` are restored with no journal, consumption,
  or TOCK
- validation timeout, missing executable, and non-UTF-8 EXPECTED_RED output
  persist a bound validation digest and block the outcome; they do not leak
  ``TimeoutExpired``, ``FileNotFoundError``, or ``UnicodeDecodeError``
- live ``runner_git.lock`` is reproofed through the existing ownership
  contract, not a PID subset: non-mapping JSON, non-UTF-8 bytes, same-PID
  foreign ``run_id``, wrong ``schema_version``, and unknown extra fields
  block as ``PROMOTION_LOCK_MISMATCH`` without raw errors
- live porcelain stays dirty for every in-repo untracked path, including
  ``validation_command[0]``; that preexisting file is never deleted on
  restore or an unrelated early block
- unexecutable PermissionError fixtures live outside the target repository
  and prove only the invocation mapping
- committed ``base_sha -> result_sha`` objects, exact ``allowed_paths``, and
  the closed sensitive/delete/protected-scope policy are the risk authority;
  a clean worktree and caller all-clear facts are not

Drop:
- journal start, consumption write, TOCK, and canonical Main mutation from
  pending persist alone
- private role fixtures, parser envelopes, and approval paths
- new receipt, authorization, digest, or hook-policy platform
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import inspect
import json
import os
import stat
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.runner_git_promotion as promotion_module
from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    EvidenceReference,
    OrchestratorDecision,
    ValidatorDecision,
)
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    load_pending_promotion,
    persist_pending_promotion,
)
from task_transition_runner.runner_promotion_contracts import (
    QualifiedPromotionIntent,
    qualify_pre_candidate_intent,
)
from task_transition_runner.runner_promotion_journal import (
    PromotionJournalPhase,
    create_promotion_journal,
    load_promotion_journal,
    persist_promotion_journal,
    transition_promotion_journal,
)
from task_transition_runner.runner_promotion_state import RunnerPromotionStateError
from task_transition_runner.task_contract import (
    DirectGreenOutcome,
    ExpectedRedOutcome,
)
from task_transition_runner.worker_result import (
    ValidationResult,
    WorkerReport,
    WorkerResult,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_REFERENCE_TASK_BRANCH = "runner/task/synthetic-task-001/attempt-001"
_REFERENCE_BASE_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_FOREIGN_TASK_ID = "synthetic-task-002"
_FOREIGN_ORIGIN_RUN_ID = "origin-run-002"
_FOREIGN_PROMOTION_ATTEMPT_ID = "promotion-attempt-002"
_FOREIGN_RESULT_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_FOREIGN_BASE_SHA = "dddddddddddddddddddddddddddddddddddddddd"
_PRIOR_TASK_ID = "synthetic-task-000"
_PRIOR_ORIGIN_RUN_ID = "origin-run-000"
_PRIOR_PROMOTION_ATTEMPT_ID = "promotion-attempt-000"
_PRIOR_RESULT_SHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
_REFERENCE_ALLOWED_PATHS = ("src/example.py", "tests/test_example.py")
_REFERENCE_COMMAND_ID = "tests"
_REFERENCE_OUTPUT_REF = "runs/origin-run-001/validation/tests.log"
_VALIDATION_LOG_BODY = b"synthetic validation log\n"
_REFERENCE_OUTPUT_SHA256 = hashlib.sha256(_VALIDATION_LOG_BODY).hexdigest()
_REFERENCE_FAILURE_SIGNATURE = "synthetic expected failing assertion text"
_REFERENCE_FINGERPRINT = "1" * 64
_REFERENCE_TASK_DIGEST = "2" * 64
_REFERENCE_INSTRUCTION_DIGEST = "3" * 64
_SPEC_BINDING_SHA256 = "4" * 64
_REFERENCE_UPDATED_AT = "2026-09-05T16:00:00Z"
_REFERENCE_LAST_TOCK_SHA = "ffffffffffffffffffffffffffffffffffffffff"
_REFERENCE_OBSERVED_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_CANONICAL_PENDING_NAME = "pending_promotion.yaml"
_GIT_AUTHOR_NAME = "Pending Intent Contract"
_GIT_AUTHOR_EMAIL = "pending-intent@task-transition-runner.invalid"
_REQUIRED_EXPORTS = (
    "RunnerGitPromotionError",
    "persist_pending_promotion",
    "load_pending_promotion",
)
_FORBIDDEN_EXPORTS = (
    "admit_promotion",
    "consume_pending_promotion",
    "create_promotion_journal",
    "materialize_merge_candidate",
    "persist_promotion_journal",
    "promote",
    "record_promotion_consumed",
    "start_journal",
    "tock_sha",
    "tree_oid",
    "validate_merge_candidate",
    "write_main_state",
)
_PERSIST_REQUIRED_PARAMETERS = (
    "runtime_root",
    "target_worktree",
    "intent",
    "task_snapshot",
    "instruction_snapshot",
    "worker_result",
    "expected_outcome",
    "orchestrator_decision",
    "validator_decision",
    "base_sha",
)
_LOAD_REQUIRED_PARAMETERS = ("runtime_root",)
_FORBIDDEN_PARAMETERS = (
    "journal",
    "merge_candidate",
    "pending_promotion",
    "tock_sha",
    "tree_oid",
    "consume",
)
_BOUND_DOCUMENT_FIELDS = (
    "schema_version",
    "artifact_class",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
    "expected_outcome",
    "expected_main_state_after_promotion",
    "base_sha",
    "task_branch",
    "main_state_before_promotion",
    "task_snapshot",
    "instruction_snapshot",
    "orchestrator_decision",
    "validator_decision",
    "allowed_paths",
    "red_lock_closure",
)
_FORBIDDEN_DOCUMENT_FIELDS = (
    "candidate_tree_oid",
    "candidate_validation",
    "consumed_at_utc",
    "journal",
    "journal_phase",
    "merge_candidate",
    "tock_sha",
    "tree_oid",
    "validation_proven",
    "worker_report",
)
_SUCCESS_REASON_CODES = (
    "PROMOTION_INTENT_PERSISTED",
    "PROMOTION_INTENT_ALREADY_PERSISTED",
)
_BLOCKED_REASON_CODES = (
    "PROMOTION_PENDING_AMBIGUOUS",
    "PROMOTION_PENDING_CONFLICT",
    "PROMOTION_PENDING_IDENTITY_MISMATCH",
    "PROMOTION_PENDING_INVALID",
    "PROMOTION_PENDING_MISSING",
    "PROMOTION_TARGET_MISSING",
)
_EFFECT_PATH_NAMES = (
    "promotion_journal.yaml",
    "promotion_consumed.yaml",
    "drain_outcome.yaml",
)
_EFFECT_ENTRY_NAME = "admit_pending_promotion_effect"
_EFFECT_REQUIRED_PARAMETERS = (
    "runtime_root",
    "target_worktree",
    "validation_command",
    "validation_timeout_seconds",
    "command_id",
    "task_snapshot",
    "instruction_snapshot",
    "worker_result",
    "expected_outcome",
    "orchestrator_decision",
    "validator_decision",
    "base_sha",
    "task_branch_head_sha",
    "allowed_paths",
    "risk_facts",
    "lock_ownership",
    "git_preflight",
    "consumed_payload",
    "red_lock",
    "unresolved_foreign_epoch",
)
_MECHANICAL_EFFECT_ADMISSION_CONDITIONS = (
    "task_and_instruction_snapshots_digest_valid",
    "result_commit_exists",
    "result_sha_matches_task_branch_head",
    "changed_paths_within_allowed_paths",
    "secret_delete_scope_risks_clear",
    "deterministic_validation_matches_expected_outcome",
    "orchestrator_decision_ready_identity_bound",
    "validator_decision_ready_identity_bound",
    "decisions_do_not_contradict",
    "main_state_matches_bound_pre_promotion_state",
    "promotion_attempt_not_contradictory_consumed",
    "local_lock_owned_by_current_process",
    "no_unresolved_foreign_epoch",
    "red_lock_contract_complete",
    "git_preflight_clean_and_allowed",
)
_TREE_DRIFT_REASON_CODES = (
    "PROMOTION_CANDIDATE_TREE_DRIFT",
    "PROMOTION_CANDIDATE_INDEX_DRIFT",
    "PROMOTION_HOOK_TREE_MUTATION",
)
_IDENTITY_DRIFT_REASON_CODES = (
    "PROMOTION_ATTEMPT_MISMATCH",
    "PROMOTION_BASE_SHA_MISMATCH",
    "PROMOTION_CONSUMPTION_CONFLICT",
    "PROMOTION_DECISION_CONTRADICTION",
    "PROMOTION_DECISION_IDENTITY_MISMATCH",
    "PROMOTION_DECISION_NOT_READY",
    "PROMOTION_EVIDENCE_MISSING",
    "PROMOTION_FOREIGN_RED",
    "PROMOTION_GIT_PREFLIGHT_BLOCKED",
    "PROMOTION_LOCK_MISMATCH",
    "PROMOTION_MAIN_STATE_MISMATCH",
    "PROMOTION_RED_LOCK_UNKNOWN",
    "PROMOTION_RESULT_SHA_MISMATCH",
    "PROMOTION_TASK_SNAPSHOT_MISMATCH",
    "PROMOTION_UNRESOLVED_FOREIGN_EPOCH",
    "PROMOTION_VALIDATION_OUTCOME_MISMATCH",
)
_VALIDATION_RECORD_FIELDS = (
    "command",
    "cwd",
    "started_at_utc",
    "timeout_seconds",
    "exit_code",
    "output_ref",
    "output_sha256",
)


def _yaml_canon(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_yaml_canon(item) for item in value]
    if isinstance(value, list):
        return [_yaml_canon(item) for item in value]
    if isinstance(value, dict):
        return {key: _yaml_canon(item) for key, item in value.items()}
    return value


def _validation_evidence() -> EvidenceReference:
    return EvidenceReference(
        kind="validation_log",
        ref=_REFERENCE_OUTPUT_REF,
        sha256=_REFERENCE_OUTPUT_SHA256,
    )


def _git_result_evidence(*, result_sha: str = _REFERENCE_RESULT_SHA) -> EvidenceReference:
    return EvidenceReference(kind="git_result", ref=result_sha, sha256=None)


def _task_view(
    *,
    task_id: str = _REFERENCE_TASK_ID,
    fingerprint: str = _REFERENCE_FINGERPRINT,
    digest: str = _REFERENCE_TASK_DIGEST,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "fingerprint": fingerprint,
        "digest": digest,
    }


def _reference_task_snapshot(
    *,
    current: dict[str, Any] | None = None,
    bound: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _task_view()
    return {
        "current": payload if current is None else current,
        "bound": copy.deepcopy(payload) if bound is None else bound,
    }


def _instruction_view(
    *,
    digest: str = _REFERENCE_INSTRUCTION_DIGEST,
    sha256: str = _SPEC_BINDING_SHA256,
    byte_length: int = 24,
) -> dict[str, Any]:
    return {
        "digest": digest,
        "bindings": (
            {
                "logical_ref": "./tasks/synthetic-task-001.md",
                "snapshot_path": "instructions/task_spec",
                "sha256": sha256,
                "byte_length": byte_length,
            },
        ),
    }


def _reference_instruction_snapshot(
    *,
    current: dict[str, Any] | None = None,
    bound: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _instruction_view()
    return {
        "current": payload if current is None else current,
        "bound": copy.deepcopy(payload) if bound is None else bound,
    }


def _reference_worker_result(**overrides: Any) -> WorkerResult:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "result_type": "worker_result",
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "task_branch": _REFERENCE_TASK_BRANCH,
        "result_sha": _REFERENCE_RESULT_SHA,
        "changed_paths": _REFERENCE_ALLOWED_PATHS,
        "validation_results": (
            ValidationResult(
                command_id=_REFERENCE_COMMAND_ID,
                exit_code=0,
                output_ref=_REFERENCE_OUTPUT_REF,
                output_sha256=_REFERENCE_OUTPUT_SHA256,
            ),
        ),
        "worker_report": WorkerReport(
            reported_status="success",
            summary="synthetic worker summary",
        ),
    }
    payload.update(overrides)
    return WorkerResult(**payload)


def _reference_orchestrator(
    *,
    ready: bool = True,
    promotion_class: str = "GREEN_PROMOTION",
    **identity: str,
) -> OrchestratorDecision:
    return OrchestratorDecision(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        decision_type="orchestrator",
        task_id=identity.get("task_id", _REFERENCE_TASK_ID),
        origin_run_id=identity.get("origin_run_id", _REFERENCE_ORIGIN_RUN_ID),
        promotion_attempt_id=identity.get(
            "promotion_attempt_id",
            _REFERENCE_PROMOTION_ATTEMPT_ID,
        ),
        result_sha=identity.get("result_sha", _REFERENCE_RESULT_SHA),
        ready=ready,
        promotion_class=promotion_class,
        evidence=(_validation_evidence(),),
        rationale=None,
    )


def _reference_validator(
    *,
    ready: bool = True,
    verdict: str = "GO",
    **identity: str,
) -> ValidatorDecision:
    result_sha = identity.get("result_sha", _REFERENCE_RESULT_SHA)
    return ValidatorDecision(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        decision_type="validator",
        task_id=identity.get("task_id", _REFERENCE_TASK_ID),
        origin_run_id=identity.get("origin_run_id", _REFERENCE_ORIGIN_RUN_ID),
        promotion_attempt_id=identity.get(
            "promotion_attempt_id",
            _REFERENCE_PROMOTION_ATTEMPT_ID,
        ),
        result_sha=result_sha,
        ready=ready,
        verdict=verdict,
        evidence=(_git_result_evidence(result_sha=result_sha), _validation_evidence()),
        rationale=None,
    )


def _reference_main_state(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": _REFERENCE_RESULT_SHA,
        "last_tock_sha": _REFERENCE_LAST_TOCK_SHA,
        "observed_main_head_sha": _REFERENCE_OBSERVED_HEAD_SHA,
        "reason_code": "STATE_READY_SYNTHETIC",
        "updated_at_utc": _REFERENCE_UPDATED_AT,
    }
    payload.update(overrides)
    return payload


def _empty_consumed() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "consumed_promotions": [],
    }


def _owned_lock() -> dict[str, Any]:
    ownership = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_name": "runner_git.lock",
        "run_id": _REFERENCE_ORIGIN_RUN_ID,
        "pid": 4242,
        "started_at_utc": "2026-09-05T15:00:00Z",
    }
    return {
        "ok": True,
        "status": "owned",
        "reason_code": "LOCK_OK",
        "owned": True,
        "ownership": ownership,
    }


def _clean_preflight() -> dict[str, Any]:
    return {"ok": True, "reason_code": "GIT_PREFLIGHT_OK"}


def _clear_risk_facts() -> dict[str, Any]:
    return {
        "secret_risk": False,
        "delete_risk": False,
        "scope_risk": False,
    }


def _opening_red_lock() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_type": "red_lock",
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "expected_main_state_after_promotion": "EXPECTED_RED_OPEN",
        "closure_gate": "GREEN_PROMOTION_REQUIRED",
        "expected_failure_signature": _REFERENCE_FAILURE_SIGNATURE,
        "command_id": _REFERENCE_COMMAND_ID,
    }


def _predecessor_red_lock() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_type": "red_lock",
        "task_id": _PRIOR_TASK_ID,
        "origin_run_id": _PRIOR_ORIGIN_RUN_ID,
        "promotion_attempt_id": _PRIOR_PROMOTION_ATTEMPT_ID,
        "result_sha": _PRIOR_RESULT_SHA,
        "expected_main_state_after_promotion": "EXPECTED_RED_OPEN",
        "closure_gate": "GREEN_PROMOTION_REQUIRED",
        "expected_failure_signature": _REFERENCE_FAILURE_SIGNATURE,
        "command_id": _REFERENCE_COMMAND_ID,
    }


def _direct_green_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "task_snapshot": _reference_task_snapshot(),
        "instruction_snapshot": _reference_instruction_snapshot(),
        "worker_result": _reference_worker_result(),
        "task_branch_head_sha": _REFERENCE_RESULT_SHA,
        "expected_outcome": DirectGreenOutcome(kind="GREEN"),
        "allowed_paths": _REFERENCE_ALLOWED_PATHS,
        "risk_facts": _clear_risk_facts(),
        "orchestrator_state": DecisionArtifactState.READY,
        "orchestrator_decision": _reference_orchestrator(),
        "validator_state": DecisionArtifactState.READY,
        "validator_decision": _reference_validator(),
        "main_state": _reference_main_state(),
        "consumed_payload": _empty_consumed(),
        "lock_ownership": _owned_lock(),
        "git_preflight": _clean_preflight(),
        "red_lock": None,
        "unresolved_foreign_epoch": False,
    }
    kwargs.update(overrides)
    return kwargs


def _expected_red_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs = _direct_green_kwargs(
        expected_outcome=ExpectedRedOutcome(
            kind="EXPECTED_RED",
            command_id=_REFERENCE_COMMAND_ID,
            failure_signature=_REFERENCE_FAILURE_SIGNATURE,
        ),
        orchestrator_decision=_reference_orchestrator(
            promotion_class="RED_TDD_PROMOTION",
        ),
        red_lock=_opening_red_lock(),
        worker_result=_reference_worker_result(
            validation_results=(
                ValidationResult(
                    command_id=_REFERENCE_COMMAND_ID,
                    exit_code=1,
                    output_ref=_REFERENCE_OUTPUT_REF,
                    output_sha256=_REFERENCE_OUTPUT_SHA256,
                ),
            ),
        ),
    )
    kwargs.update(overrides)
    return kwargs


def _green_closure_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs = _direct_green_kwargs(
        main_state=_reference_main_state(
            state="EXPECTED_RED_OPEN",
            validated_result_sha=_PRIOR_RESULT_SHA,
            reason_code="STATE_EXPECTED_RED_OPEN",
        ),
        red_lock=_predecessor_red_lock(),
    )
    kwargs.update(overrides)
    return kwargs


def _qualify_green(**overrides: Any) -> QualifiedPromotionIntent:
    result = qualify_pre_candidate_intent(**_direct_green_kwargs(**overrides))
    intent = result["intent"]
    assert isinstance(intent, QualifiedPromotionIntent)
    return intent


def _qualify_expected_red(**overrides: Any) -> QualifiedPromotionIntent:
    result = qualify_pre_candidate_intent(**_expected_red_kwargs(**overrides))
    intent = result["intent"]
    assert isinstance(intent, QualifiedPromotionIntent)
    return intent


def _qualify_green_closure(**overrides: Any) -> QualifiedPromotionIntent:
    result = qualify_pre_candidate_intent(**_green_closure_kwargs(**overrides))
    intent = result["intent"]
    assert isinstance(intent, QualifiedPromotionIntent)
    return intent


def _runtime_root(tmp_path: Path) -> Path:
    return tmp_path / "runner-runtime"


def _pending_path(runtime: Path) -> Path:
    return runtime / _CANONICAL_PENDING_NAME


def _write_yaml(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def _run_git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _init_target_repo(tmp_path: Path, *, name: str = "target-repo") -> Path:
    repo_root = tmp_path / name
    repo_root.mkdir(parents=True)
    subprocess.run(  # noqa: S603
        ["git", "-c", "init.defaultBranch=main", "init"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    _run_git(repo_root, "config", "user.name", _GIT_AUTHOR_NAME)
    _run_git(repo_root, "config", "user.email", _GIT_AUTHOR_EMAIL)
    (repo_root / "tracked.txt").write_text("baseline\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "tracked.txt")
    _run_git(repo_root, "commit", "-m", "baseline")
    return repo_root


def _git_dir(repo_root: Path) -> Path:
    return Path(_run_git(repo_root, "rev-parse", "--absolute-git-dir"))


def _git_snapshot(repo_root: Path) -> dict[str, str]:
    merge_head = _git_dir(repo_root) / "MERGE_HEAD"
    return {
        "head": _run_git(repo_root, "rev-parse", "HEAD"),
        "branch": _run_git(repo_root, "rev-parse", "--abbrev-ref", "HEAD"),
        "main": _run_git(repo_root, "rev-parse", "refs/heads/main"),
        "refs": _run_git(repo_root, "show-ref", "--heads"),
        "status": _run_git(repo_root, "status", "--porcelain"),
        "log": _run_git(repo_root, "log", "-1", "--format=%H%n%s"),
        "merge_head": merge_head.read_text(encoding="utf-8") if merge_head.is_file() else "",
    }


def _seed_main_state(runtime: Path, payload: Mapping[str, Any] | None = None) -> Path:
    path = runtime / "main_state.yaml"
    _write_yaml(path, dict(payload or _reference_main_state()))
    return path


def _persist_kwargs(
    tmp_path: Path,
    *,
    intent: QualifiedPromotionIntent | None = None,
    qualify: str = "green",
    target_worktree: Path | None = None,
    seed_main_state: bool = True,
    **overrides: Any,
) -> dict[str, Any]:
    if qualify == "expected_red":
        qualify_kwargs = _expected_red_kwargs()
        bound_intent = intent if intent is not None else _qualify_expected_red()
    elif qualify == "green_closure":
        qualify_kwargs = _green_closure_kwargs()
        bound_intent = intent if intent is not None else _qualify_green_closure()
    else:
        qualify_kwargs = _direct_green_kwargs()
        bound_intent = intent if intent is not None else _qualify_green()
    runtime = _runtime_root(tmp_path)
    target = target_worktree if target_worktree is not None else tmp_path / "target-root"
    target.mkdir(parents=True, exist_ok=True)
    if seed_main_state:
        _seed_main_state(runtime, bound_intent.main_state_before_promotion)
    kwargs: dict[str, Any] = {
        "runtime_root": runtime,
        "target_worktree": target,
        "intent": bound_intent,
        "task_snapshot": qualify_kwargs["task_snapshot"],
        "instruction_snapshot": qualify_kwargs["instruction_snapshot"],
        "worker_result": qualify_kwargs["worker_result"],
        "expected_outcome": qualify_kwargs["expected_outcome"],
        "orchestrator_decision": qualify_kwargs["orchestrator_decision"],
        "validator_decision": qualify_kwargs["validator_decision"],
        "base_sha": _REFERENCE_BASE_SHA,
    }
    kwargs.update(overrides)
    return kwargs


def _assert_blocked(result: Mapping[str, Any], reason_code: str) -> None:
    assert result["ok"] is False
    assert result["status"] == "blocked"
    assert result["reason_code"] == reason_code
    assert result.get("intent") is None
    for forbidden in ("tock_sha", "tree_oid", "journal", "consumed_at_utc"):
        assert forbidden not in result


_RELATION_BLOCK_CODES = (
    "PROMOTION_PENDING_INVALID",
    "PROMOTION_PENDING_IDENTITY_MISMATCH",
)


def _assert_blocked_one_of(
    result: Mapping[str, Any],
    reason_codes: tuple[str, ...],
) -> None:
    assert result["reason_code"] in reason_codes
    _assert_blocked(result, result["reason_code"])


def _assert_protocol_idle(
    runtime: Path,
    *,
    repo: Path | None = None,
    git_before: Mapping[str, str] | None = None,
    main_state_path: Path | None = None,
    main_before: bytes | None = None,
) -> None:
    for name in _EFFECT_PATH_NAMES:
        assert (runtime / name).exists() is False
    leftover = [
        path.name
        for path in runtime.rglob("*")
        if path.is_file()
        and (
            path.name.endswith(".tmp")
            or path.name.endswith(".temp")
            or path.name.startswith(".")
        )
    ]
    assert leftover == []
    if repo is not None and git_before is not None:
        _assert_git_unchanged(repo, git_before)
    if main_state_path is not None:
        assert main_before is not None
        assert _read_bytes(main_state_path) == main_before


def _assert_no_effect_files(runtime: Path, *, allow_main_state: bool = True) -> None:
    for name in _EFFECT_PATH_NAMES:
        assert (runtime / name).exists() is False
    assert (runtime / "pending_promotion.yml").exists() is False
    assert (runtime / "pending_promotion.json").exists() is False
    leftover = [
        path.name
        for path in runtime.rglob("*")
        if path.is_file()
        and (
            path.name.endswith(".tmp")
            or path.name.endswith(".temp")
            or path.name.startswith(".")
        )
    ]
    assert leftover == []
    if allow_main_state:
        return
    assert (runtime / "main_state.yaml").exists() is False


def _assert_git_unchanged(repo_root: Path, before: Mapping[str, str]) -> None:
    assert _git_snapshot(repo_root) == dict(before)


def _assert_non_authorizing_document(
    document: Mapping[str, Any],
    *,
    intent: QualifiedPromotionIntent,
    expected_outcome: DirectGreenOutcome | ExpectedRedOutcome,
    orchestrator: OrchestratorDecision,
    validator: ValidatorDecision,
    task_snapshot: Mapping[str, Any],
    instruction_snapshot: Mapping[str, Any],
    worker_result: WorkerResult,
    base_sha: str = _REFERENCE_BASE_SHA,
) -> None:
    assert document["schema_version"] == _REFERENCE_SCHEMA_VERSION
    assert document["artifact_class"] == _ARTIFACT_CLASS
    assert set(document) == set(_BOUND_DOCUMENT_FIELDS)
    assert document["task_id"] == intent.task_id == worker_result.task_id
    assert document["origin_run_id"] == intent.origin_run_id == worker_result.origin_run_id
    assert (
        document["promotion_attempt_id"]
        == intent.promotion_attempt_id
        == worker_result.promotion_attempt_id
    )
    assert document["result_sha"] == intent.result_sha == worker_result.result_sha
    assert document["promotion_class"] == intent.promotion_class
    assert (
        document["expected_main_state_after_promotion"]
        == intent.expected_main_state_after_promotion
    )
    assert document["base_sha"] == base_sha
    assert document["task_branch"] == worker_result.task_branch
    assert document["main_state_before_promotion"] == intent.main_state_before_promotion
    assert _yaml_canon(document["task_snapshot"]) == _yaml_canon(task_snapshot)
    assert _yaml_canon(document["instruction_snapshot"]) == _yaml_canon(
        instruction_snapshot
    )
    assert _yaml_canon(document["orchestrator_decision"]) == _yaml_canon(
        dataclasses.asdict(orchestrator)
    )
    assert _yaml_canon(document["validator_decision"]) == _yaml_canon(
        dataclasses.asdict(validator)
    )
    assert tuple(document["allowed_paths"]) == intent.allowed_paths
    assert document["red_lock_closure"] == intent.red_lock_closure
    assert document["expected_outcome"] == dataclasses.asdict(expected_outcome)
    evidence = document["orchestrator_decision"]["evidence"] + document[
        "validator_decision"
    ]["evidence"]
    assert evidence
    assert all(item["kind"] in {"validation_log", "git_result"} for item in evidence)
    for forbidden in _FORBIDDEN_DOCUMENT_FIELDS:
        assert forbidden not in document
    assert document["artifact_class"] != "PROMOTION_ADMISSION"
    assert document.get("authorizes_journal") is not True
    assert document.get("authorizes_consumption") is not True
    assert document.get("authorizes_tock") is not True
    assert document.get("authorizes_main_mutation") is not True
    assert document.get("candidate_validation_proven") is not True


def _assert_persisted(
    result: Mapping[str, Any],
    *,
    runtime: Path,
    reason_code: str = "PROMOTION_INTENT_PERSISTED",
    status: str = "persisted",
) -> dict[str, Any]:
    assert result["ok"] is True
    assert result["status"] == status
    assert result["reason_code"] == reason_code
    assert result["artifact_class"] == _ARTIFACT_CLASS
    pending = _pending_path(runtime)
    assert result["pending_path"] == str(pending)
    assert pending.is_file()
    loaded = load_pending_promotion(runtime_root=runtime)
    assert loaded == result["intent"]
    assert loaded["artifact_class"] == _ARTIFACT_CLASS
    raw = yaml.safe_load(pending.read_text(encoding="utf-8"))
    assert set(raw) == set(_BOUND_DOCUMENT_FIELDS)
    assert set(loaded) == set(_BOUND_DOCUMENT_FIELDS)
    _assert_no_effect_files(runtime)
    return loaded


def test_public_exports_are_the_pending_intent_persistence_surface() -> None:
    for name in _REQUIRED_EXPORTS:
        assert hasattr(promotion_module, name)
    for name in _FORBIDDEN_EXPORTS:
        assert not hasattr(promotion_module, name)
    persist_signature = inspect.signature(persist_pending_promotion)
    assert list(persist_signature.parameters) == list(_PERSIST_REQUIRED_PARAMETERS)
    for parameter in persist_signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
        for forbidden in _FORBIDDEN_PARAMETERS:
            assert forbidden not in parameter.name
    load_signature = inspect.signature(load_pending_promotion)
    assert list(load_signature.parameters) == list(_LOAD_REQUIRED_PARAMETERS)
    for parameter in load_signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
        for forbidden in _FORBIDDEN_PARAMETERS:
            assert forbidden not in parameter.name
    assert issubclass(RunnerGitPromotionError, Exception)


def test_direct_green_persists_bound_non_authorizing_intent(tmp_path: Path) -> None:
    intent = _qualify_green()
    kwargs = _persist_kwargs(tmp_path, intent=intent)
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    _assert_non_authorizing_document(
        loaded,
        intent=intent,
        expected_outcome=kwargs["expected_outcome"],
        orchestrator=kwargs["orchestrator_decision"],
        validator=kwargs["validator_decision"],
        task_snapshot=kwargs["task_snapshot"],
        instruction_snapshot=kwargs["instruction_snapshot"],
        worker_result=kwargs["worker_result"],
    )
    assert loaded["result_sha"] == _REFERENCE_RESULT_SHA
    assert loaded["expected_outcome"]["kind"] == "GREEN"
    main_state_path = kwargs["runtime_root"] / "main_state.yaml"
    assert yaml.safe_load(main_state_path.read_text(encoding="utf-8")) == (
        intent.main_state_before_promotion
    )


def test_expected_red_persists_bound_lock_and_outcome(tmp_path: Path) -> None:
    intent = _qualify_expected_red()
    kwargs = _persist_kwargs(tmp_path, intent=intent, qualify="expected_red")
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    _assert_non_authorizing_document(
        loaded,
        intent=intent,
        expected_outcome=kwargs["expected_outcome"],
        orchestrator=kwargs["orchestrator_decision"],
        validator=kwargs["validator_decision"],
        task_snapshot=kwargs["task_snapshot"],
        instruction_snapshot=kwargs["instruction_snapshot"],
        worker_result=kwargs["worker_result"],
    )
    assert loaded["promotion_class"] == "RED_TDD_PROMOTION"
    assert loaded["expected_main_state_after_promotion"] == "EXPECTED_RED_OPEN"
    assert loaded["expected_outcome"]["kind"] == "EXPECTED_RED"
    assert loaded["expected_outcome"]["failure_signature"] == _REFERENCE_FAILURE_SIGNATURE
    assert intent.red_lock_closure is None
    assert loaded["red_lock_closure"] is None


def test_green_closure_persists_predecessor_red_lock_without_effect(
    tmp_path: Path,
) -> None:
    intent = _qualify_green_closure()
    kwargs = _persist_kwargs(tmp_path, intent=intent, qualify="green_closure")
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    _assert_non_authorizing_document(
        loaded,
        intent=intent,
        expected_outcome=kwargs["expected_outcome"],
        orchestrator=kwargs["orchestrator_decision"],
        validator=kwargs["validator_decision"],
        task_snapshot=kwargs["task_snapshot"],
        instruction_snapshot=kwargs["instruction_snapshot"],
        worker_result=kwargs["worker_result"],
    )
    assert loaded["promotion_class"] == "GREEN_PROMOTION"
    assert loaded["red_lock_closure"] is not None
    assert loaded["main_state_before_promotion"]["state"] == "EXPECTED_RED_OPEN"


def test_result_sha_is_later_validation_input_not_candidate_proof(
    tmp_path: Path,
) -> None:
    kwargs = _persist_kwargs(tmp_path)
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    assert loaded["result_sha"] == kwargs["intent"].result_sha
    assert loaded["result_sha"] == kwargs["worker_result"].result_sha
    assert "candidate_tree_oid" not in loaded
    assert "merge_candidate" not in loaded
    assert loaded.get("result_sha_validated_against_merge_candidate") is not True
    assert loaded.get("candidate_validation_proven") is not True


def test_identical_persist_is_byte_stable_and_does_not_rewrite(tmp_path: Path) -> None:
    kwargs = _persist_kwargs(tmp_path)
    first = persist_pending_promotion(**kwargs)
    pending = _pending_path(kwargs["runtime_root"])
    first_bytes = _read_bytes(pending)
    loaded_first = _assert_persisted(first, runtime=kwargs["runtime_root"])
    second = persist_pending_promotion(**kwargs)
    _assert_persisted(
        second,
        runtime=kwargs["runtime_root"],
        reason_code="PROMOTION_INTENT_ALREADY_PERSISTED",
        status="already_persisted",
    )
    assert _read_bytes(pending) == first_bytes
    assert load_pending_promotion(runtime_root=kwargs["runtime_root"]) == loaded_first
    _assert_no_effect_files(kwargs["runtime_root"])


def _foreign_result_conflict(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "intent": dataclasses.replace(
            kwargs["intent"],
            result_sha=_FOREIGN_RESULT_SHA,
        ),
        "worker_result": dataclasses.replace(
            kwargs["worker_result"],
            result_sha=_FOREIGN_RESULT_SHA,
        ),
        "orchestrator_decision": _reference_orchestrator(
            result_sha=_FOREIGN_RESULT_SHA,
        ),
        "validator_decision": _reference_validator(
            result_sha=_FOREIGN_RESULT_SHA,
        ),
    }


def _expected_red_against_green(kwargs: dict[str, Any]) -> dict[str, Any]:
    red_facts = _expected_red_kwargs()
    return {
        **kwargs,
        "intent": _qualify_expected_red(),
        "task_snapshot": red_facts["task_snapshot"],
        "instruction_snapshot": red_facts["instruction_snapshot"],
        "worker_result": red_facts["worker_result"],
        "expected_outcome": red_facts["expected_outcome"],
        "orchestrator_decision": red_facts["orchestrator_decision"],
        "validator_decision": red_facts["validator_decision"],
    }


@pytest.mark.parametrize(
    "conflict_builder",
    (
        lambda kwargs: {**kwargs, "base_sha": _FOREIGN_BASE_SHA},
        _expected_red_against_green,
        _foreign_result_conflict,
    ),
    ids=("base_sha", "expected_red_class", "result_sha"),
)
def test_conflicting_persist_blocks_and_preserves_original_bytes(
    tmp_path: Path,
    conflict_builder: Any,
) -> None:
    kwargs = _persist_kwargs(tmp_path)
    first = persist_pending_promotion(**kwargs)
    pending = _pending_path(kwargs["runtime_root"])
    original = _read_bytes(pending)
    _assert_persisted(first, runtime=kwargs["runtime_root"])
    blocked = persist_pending_promotion(**conflict_builder(kwargs))
    _assert_blocked(blocked, "PROMOTION_PENDING_CONFLICT")
    assert _read_bytes(pending) == original
    assert load_pending_promotion(runtime_root=kwargs["runtime_root"])["result_sha"] == (
        _REFERENCE_RESULT_SHA
    )
    _assert_no_effect_files(kwargs["runtime_root"])


def test_conflicting_attempt_and_origin_do_not_overwrite(tmp_path: Path) -> None:
    kwargs = _persist_kwargs(tmp_path)
    persist_pending_promotion(**kwargs)
    pending = _pending_path(kwargs["runtime_root"])
    original = _read_bytes(pending)
    foreign_intent = dataclasses.replace(
        kwargs["intent"],
        origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
        promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
    )
    foreign_worker = dataclasses.replace(
        kwargs["worker_result"],
        origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
        promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
    )
    blocked = persist_pending_promotion(
        **{
            **kwargs,
            "intent": foreign_intent,
            "worker_result": foreign_worker,
            "orchestrator_decision": _reference_orchestrator(
                origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
                promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
            ),
            "validator_decision": _reference_validator(
                origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
                promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
            ),
        }
    )
    _assert_blocked(blocked, "PROMOTION_PENDING_CONFLICT")
    assert _read_bytes(pending) == original


@pytest.mark.parametrize(
    "mutator",
    (
        lambda kwargs: {
            **kwargs,
            "worker_result": dataclasses.replace(
                kwargs["worker_result"],
                result_sha=_FOREIGN_RESULT_SHA,
            ),
        },
        lambda kwargs: {
            **kwargs,
            "task_snapshot": _reference_task_snapshot(
                current=_task_view(task_id=_FOREIGN_TASK_ID),
                bound=_task_view(task_id=_FOREIGN_TASK_ID),
            ),
        },
    ),
    ids=("result_sha", "task_snapshot"),
)
def test_identity_mismatch_between_intent_and_facts_writes_nothing(
    tmp_path: Path,
    mutator: Any,
) -> None:
    kwargs = mutator(_persist_kwargs(tmp_path))
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_IDENTITY_MISMATCH")
    runtime = kwargs["runtime_root"]
    assert _pending_path(runtime).exists() is False
    _assert_no_effect_files(runtime)
    with pytest.raises(RunnerGitPromotionError) as excinfo:
        load_pending_promotion(runtime_root=runtime)
    assert excinfo.value.args[0] == "PROMOTION_PENDING_MISSING" or (
        getattr(excinfo.value, "reason_code", None) == "PROMOTION_PENDING_MISSING"
    )


def test_ambiguous_pending_files_fail_closed_without_overwrite(tmp_path: Path) -> None:
    kwargs = _persist_kwargs(tmp_path)
    persist_pending_promotion(**kwargs)
    runtime = kwargs["runtime_root"]
    pending = _pending_path(runtime)
    original = _read_bytes(pending)
    extra = runtime / "pending_promotion.json"
    extra.write_text("{\"artifact_class\": \"foreign\"}\n", encoding="utf-8")
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_AMBIGUOUS")
    assert _read_bytes(pending) == original
    with pytest.raises(RunnerGitPromotionError) as excinfo:
        load_pending_promotion(runtime_root=runtime)
    assert "PROMOTION_PENDING_AMBIGUOUS" in str(excinfo.value) or (
        getattr(excinfo.value, "reason_code", None) == "PROMOTION_PENDING_AMBIGUOUS"
    )


def test_persist_does_not_start_journal_consume_tock_or_mutate_main(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    main_state_path = kwargs["runtime_root"] / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    result = persist_pending_promotion(**kwargs)
    _assert_persisted(result, runtime=kwargs["runtime_root"])
    _assert_git_unchanged(repo, before)
    assert _read_bytes(main_state_path) == main_before
    _assert_no_effect_files(kwargs["runtime_root"])
    persist_pending_promotion(**kwargs)
    _assert_git_unchanged(repo, before)
    assert _read_bytes(main_state_path) == main_before


def test_missing_candidate_validation_cannot_reinterpret_pending_as_effect(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    assert loaded["artifact_class"] == _ARTIFACT_CLASS
    for forbidden in _FORBIDDEN_DOCUMENT_FIELDS:
        assert forbidden not in loaded
    for name in _FORBIDDEN_EXPORTS:
        assert not hasattr(promotion_module, name)
    _assert_git_unchanged(repo, before)
    _assert_no_effect_files(kwargs["runtime_root"])


def test_persist_does_not_require_task_branch_or_merge_candidate(
    tmp_path: Path,
) -> None:
    target = tmp_path / "plain-directory"
    target.mkdir()
    kwargs = _persist_kwargs(tmp_path, target_worktree=target)
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    assert loaded["result_sha"] == _REFERENCE_RESULT_SHA
    assert loaded["task_branch"] == _REFERENCE_TASK_BRANCH
    assert (target / ".git").exists() is False


def test_persist_does_not_use_process_cwd_git_or_runtime(tmp_path: Path) -> None:
    decoy = _init_target_repo(tmp_path, name="decoy-repo")
    decoy_runtime = tmp_path / "decoy-runtime"
    decoy_runtime.mkdir()
    (decoy_runtime / "decoy.txt").write_text("leave me\n", encoding="utf-8")
    before = _git_snapshot(decoy)
    kwargs = _persist_kwargs(tmp_path)
    original = os.getcwd()
    try:
        os.chdir(decoy)
        result = persist_pending_promotion(**kwargs)
    finally:
        os.chdir(original)
    _assert_persisted(result, runtime=kwargs["runtime_root"])
    _assert_git_unchanged(decoy, before)
    assert list(decoy_runtime.iterdir()) == [decoy_runtime / "decoy.txt"]
    assert _pending_path(decoy_runtime).exists() is False


def test_missing_target_worktree_is_fail_closed(tmp_path: Path) -> None:
    kwargs = _persist_kwargs(tmp_path)
    kwargs["target_worktree"] = tmp_path / "missing-target"
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_TARGET_MISSING")
    assert _pending_path(kwargs["runtime_root"]).exists() is False
    _assert_no_effect_files(kwargs["runtime_root"])


def test_load_missing_pending_is_fail_closed(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path)
    runtime.mkdir()
    with pytest.raises(RunnerGitPromotionError) as excinfo:
        load_pending_promotion(runtime_root=runtime)
    assert "PROMOTION_PENDING_MISSING" in str(excinfo.value) or (
        getattr(excinfo.value, "reason_code", None) == "PROMOTION_PENDING_MISSING"
    )


def test_worker_success_report_is_not_persisted_as_authority(tmp_path: Path) -> None:
    kwargs = _persist_kwargs(tmp_path)
    result = persist_pending_promotion(**kwargs)
    loaded = _assert_persisted(result, runtime=kwargs["runtime_root"])
    raw = yaml.safe_load(_pending_path(kwargs["runtime_root"]).read_text(encoding="utf-8"))
    assert "worker_report" not in loaded
    assert "worker_report" not in raw
    assert "success" not in yaml.safe_dump(raw)


def test_persist_rejects_non_authorizing_artifact_class_without_write(
    tmp_path: Path,
) -> None:
    kwargs = _persist_kwargs(tmp_path)
    kwargs["intent"] = dataclasses.replace(
        kwargs["intent"],
        artifact_class="PROMOTION_ADMISSION",
    )
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_INVALID")
    assert _pending_path(kwargs["runtime_root"]).exists() is False
    _assert_no_effect_files(kwargs["runtime_root"])


def _valid_pending_document() -> dict[str, Any]:
    intent = _qualify_green()
    facts = _direct_green_kwargs()
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "artifact_class": _ARTIFACT_CLASS,
        "task_id": intent.task_id,
        "origin_run_id": intent.origin_run_id,
        "promotion_attempt_id": intent.promotion_attempt_id,
        "result_sha": intent.result_sha,
        "promotion_class": intent.promotion_class,
        "expected_outcome": dataclasses.asdict(facts["expected_outcome"]),
        "expected_main_state_after_promotion": (
            intent.expected_main_state_after_promotion
        ),
        "base_sha": _REFERENCE_BASE_SHA,
        "task_branch": facts["worker_result"].task_branch,
        "main_state_before_promotion": intent.main_state_before_promotion,
        "task_snapshot": facts["task_snapshot"],
        "instruction_snapshot": facts["instruction_snapshot"],
        "orchestrator_decision": dataclasses.asdict(facts["orchestrator_decision"]),
        "validator_decision": dataclasses.asdict(facts["validator_decision"]),
        "allowed_paths": list(intent.allowed_paths),
        "red_lock_closure": intent.red_lock_closure,
    }


def _assert_pending_invalid(exc: BaseException) -> None:
    reason = getattr(exc, "reason_code", None)
    rendered = str(exc)
    assert reason == "PROMOTION_PENDING_INVALID" or rendered.startswith(
        "PROMOTION_PENDING_INVALID"
    )


def _write_corrupt_pending(path: Path, kind: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    document = _valid_pending_document()
    if kind == "malformed":
        path.write_text("broken: [\n", encoding="utf-8")
        return
    if kind == "non_mapping":
        _write_yaml(path, [document])
        return
    if kind == "missing_field":
        document.pop("result_sha")
        _write_yaml(path, document)
        return
    if kind == "unknown_field":
        document["foreign_field"] = "not-in-schema"
        _write_yaml(path, document)
        return
    if kind == "duplicate_key":
        _write_yaml(path, document)
        path.write_text(
            'schema_version: "1.0"\n' + path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return
    raise AssertionError(f"unknown corrupt kind: {kind}")


@pytest.mark.parametrize(
    "kind",
    (
        "malformed",
        "non_mapping",
        "missing_field",
        "unknown_field",
        "duplicate_key",
    ),
)
def test_load_pending_rejects_closed_schema_faults_without_effects(
    tmp_path: Path,
    kind: str,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    runtime = _runtime_root(tmp_path)
    main_state_path = _seed_main_state(runtime)
    main_before = _read_bytes(main_state_path)
    pending = _pending_path(runtime)
    _write_corrupt_pending(pending, kind)
    original = _read_bytes(pending)
    with pytest.raises(RunnerGitPromotionError) as excinfo:
        load_pending_promotion(runtime_root=runtime)
    _assert_pending_invalid(excinfo.value)
    assert _read_bytes(pending) == original
    assert _read_bytes(main_state_path) == main_before
    _assert_git_unchanged(repo, before)
    _assert_no_effect_files(runtime)


_CORRUPT_PENDING_KINDS = (
    "malformed",
    "non_mapping",
    "missing_field",
    "unknown_field",
    "duplicate_key",
)


@pytest.mark.parametrize("kind", _CORRUPT_PENDING_KINDS)
def test_persist_over_corrupt_pending_preserves_bytes_without_effects(
    tmp_path: Path,
    kind: str,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    pending = _pending_path(runtime)
    _write_corrupt_pending(pending, kind)
    original = _read_bytes(pending)
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_INVALID")
    assert _read_bytes(pending) == original
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )
    assert list(runtime.glob("pending_promotion.yaml")) == [pending]


def test_missing_main_state_blocks_before_pending_write(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(
        tmp_path,
        target_worktree=repo,
        seed_main_state=False,
    )
    runtime = kwargs["runtime_root"]
    runtime.mkdir(parents=True, exist_ok=True)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_no_effect_files(runtime, allow_main_state=False)
    _assert_git_unchanged(repo, before)


def test_drifted_main_state_blocks_without_rewrite(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(
        tmp_path,
        target_worktree=repo,
        seed_main_state=False,
    )
    runtime = kwargs["runtime_root"]
    drifted = _reference_main_state(observed_main_head_sha=_FOREIGN_BASE_SHA)
    main_state_path = _seed_main_state(runtime, drifted)
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    assert _read_bytes(main_state_path) == main_before
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


def _instruction_snapshot_drift(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = _reference_instruction_snapshot(
        current=_instruction_view(digest="9" * 64),
    )
    assert snapshot["current"] != snapshot["bound"]
    return {**kwargs, "instruction_snapshot": snapshot}


def _orchestrator_identity_drift(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "orchestrator_decision": _reference_orchestrator(
            task_id=_FOREIGN_TASK_ID,
            origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
            promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
            result_sha=_FOREIGN_RESULT_SHA,
        ),
    }


def _validator_identity_drift(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "validator_decision": _reference_validator(
            task_id=_FOREIGN_TASK_ID,
            origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
            promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
            result_sha=_FOREIGN_RESULT_SHA,
        ),
    }


def _expected_outcome_class_mismatch(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "expected_outcome": ExpectedRedOutcome(
            kind="EXPECTED_RED",
            command_id=_REFERENCE_COMMAND_ID,
            failure_signature=_REFERENCE_FAILURE_SIGNATURE,
        ),
    }


@pytest.mark.parametrize(
    "mutator",
    (
        _instruction_snapshot_drift,
        _orchestrator_identity_drift,
        _validator_identity_drift,
        _expected_outcome_class_mismatch,
    ),
    ids=(
        "instruction_snapshot",
        "orchestrator_identity",
        "validator_identity",
        "expected_outcome_class",
    ),
)
def test_fact_relation_drift_blocks_before_write(
    tmp_path: Path,
    mutator: Any,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = mutator(_persist_kwargs(tmp_path, target_worktree=repo))
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


def _empty_base_sha(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {**kwargs, "base_sha": ""}


def _empty_orchestrator_evidence(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "orchestrator_decision": dataclasses.replace(
            kwargs["orchestrator_decision"],
            evidence=(),
        ),
    }


def _empty_validator_evidence(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "validator_decision": dataclasses.replace(
            kwargs["validator_decision"],
            evidence=(),
        ),
    }


@pytest.mark.parametrize(
    "mutator",
    (
        _empty_base_sha,
        _empty_orchestrator_evidence,
        _empty_validator_evidence,
    ),
    ids=("base_sha", "orchestrator_evidence", "validator_evidence"),
)
def test_empty_base_or_evidence_blocks_before_write(
    tmp_path: Path,
    mutator: Any,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = mutator(_persist_kwargs(tmp_path, target_worktree=repo))
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


@pytest.mark.parametrize(
    "alternate_name",
    ("pending_promotion.yml", "pending_promotion.json"),
)
def test_standalone_alternate_pending_is_ambiguous_without_canonical_sibling(
    tmp_path: Path,
    alternate_name: str,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    alternate = runtime / alternate_name
    if alternate_name.endswith(".json"):
        alternate.write_text('{"artifact_class": "foreign"}\n', encoding="utf-8")
    else:
        alternate.write_text("artifact_class: foreign\n", encoding="utf-8")
    original = _read_bytes(alternate)
    with pytest.raises(RunnerGitPromotionError) as excinfo:
        load_pending_promotion(runtime_root=runtime)
    rendered = str(excinfo.value)
    reason = getattr(excinfo.value, "reason_code", None)
    assert reason == "PROMOTION_PENDING_AMBIGUOUS" or (
        "PROMOTION_PENDING_AMBIGUOUS" in rendered
    )
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_AMBIGUOUS")
    assert _read_bytes(alternate) == original
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip("process retains read access after clearing read mode bits")


def _fail_replace(*_args: Any, **_kwargs: Any) -> None:
    raise OSError("synthetic pending replace failure")


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("artifact_class", "PROMOTION_ADMISSION"),
        ("schema_version", "2.0"),
    ),
    ids=("artifact_class_admission", "schema_version_2_0"),
)
def test_load_pending_rejects_closed_schema_values_without_effects(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    runtime = _runtime_root(tmp_path)
    main_state_path = _seed_main_state(runtime)
    main_before = _read_bytes(main_state_path)
    pending = _pending_path(runtime)
    document = _valid_pending_document()
    document[field] = value
    _write_yaml(pending, document)
    original = _read_bytes(pending)
    with pytest.raises(RunnerGitPromotionError) as excinfo:
        load_pending_promotion(runtime_root=runtime)
    _assert_pending_invalid(excinfo.value)
    assert _read_bytes(pending) == original
    assert _read_bytes(main_state_path) == main_before
    _assert_git_unchanged(repo, before)
    _assert_no_effect_files(runtime)


def test_persist_malformed_main_state_is_fail_closed_without_raw_yaml_error(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_state_path.write_text("broken: [\n", encoding="utf-8")
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


def test_persist_duplicate_key_main_state_is_invalid_even_when_last_win_matches(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    matching = dict(kwargs["intent"].main_state_before_promotion)
    main_state_path.write_text(
        'state: "FOREIGN_STATE"\n' + yaml.safe_dump(matching, sort_keys=False),
        encoding="utf-8",
    )
    original = _read_bytes(main_state_path)
    assert yaml.safe_load(main_state_path.read_text(encoding="utf-8")) == matching
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_INVALID")
    assert _read_bytes(main_state_path) == original
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=original,
    )


def test_persist_unreadable_main_state_is_fail_closed_without_raw_oserror(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    original_mode = main_state_path.stat().st_mode
    try:
        _make_unreadable(main_state_path)
        blocked = persist_pending_promotion(**kwargs)
        _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
        assert _pending_path(runtime).exists() is False
        _assert_protocol_idle(
            runtime,
            repo=repo,
            git_before=before,
        )
    finally:
        if main_state_path.is_file():
            main_state_path.chmod(original_mode)
    assert _read_bytes(main_state_path) == main_before


def test_persist_replace_oserror_is_invalid_without_temp_or_canonical_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    monkeypatch.setattr(os, "replace", _fail_replace)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_INVALID")
    assert _pending_path(runtime).exists() is False
    assert (runtime / f"{_CANONICAL_PENDING_NAME}.tmp").exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


def test_persist_missing_snapshot_task_id_is_fail_closed_without_key_error(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    snapshot = copy.deepcopy(kwargs["task_snapshot"])
    del snapshot["current"]["task_id"]
    kwargs["task_snapshot"] = snapshot
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("artifact_class", "PROMOTION_ADMISSION"),
        ("schema_version", "2.0"),
    ),
    ids=("artifact_class_admission", "schema_version_2_0"),
)
def test_persist_over_closed_schema_values_is_invalid_without_rewrite(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    pending = _pending_path(runtime)
    document = _valid_pending_document()
    document[field] = value
    _write_yaml(pending, document)
    original = _read_bytes(pending)
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked(blocked, "PROMOTION_PENDING_INVALID")
    assert _read_bytes(pending) == original
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )
    assert list(runtime.glob("pending_promotion.yaml")) == [pending]


def test_load_unreadable_canonical_pending_is_fail_closed_without_raw_oserror(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    runtime = _runtime_root(tmp_path)
    main_state_path = _seed_main_state(runtime)
    main_before = _read_bytes(main_state_path)
    pending = _pending_path(runtime)
    _write_yaml(pending, _valid_pending_document())
    original = _read_bytes(pending)
    original_mode = pending.stat().st_mode
    try:
        _make_unreadable(pending)
        with pytest.raises(RunnerGitPromotionError) as excinfo:
            load_pending_promotion(runtime_root=runtime)
        _assert_pending_invalid(excinfo.value)
        _assert_git_unchanged(repo, before)
        _assert_no_effect_files(runtime)
    finally:
        if pending.is_file():
            pending.chmod(original_mode)
    assert _read_bytes(pending) == original
    assert _read_bytes(main_state_path) == main_before


def test_persist_over_unreadable_canonical_pending_is_invalid_without_rewrite(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    runtime = kwargs["runtime_root"]
    pending = _pending_path(runtime)
    _write_yaml(pending, _valid_pending_document())
    original = _read_bytes(pending)
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    original_mode = pending.stat().st_mode
    try:
        _make_unreadable(pending)
        blocked = persist_pending_promotion(**kwargs)
        _assert_blocked(blocked, "PROMOTION_PENDING_INVALID")
        _assert_protocol_idle(
            runtime,
            repo=repo,
            git_before=before,
            main_state_path=main_state_path,
            main_before=main_before,
        )
        assert list(runtime.glob("pending_promotion.yaml")) == [pending]
    finally:
        if pending.is_file():
            pending.chmod(original_mode)
    assert _read_bytes(pending) == original


def test_persist_missing_snapshot_bound_task_id_is_fail_closed_without_key_error(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = _persist_kwargs(tmp_path, target_worktree=repo)
    snapshot = copy.deepcopy(kwargs["task_snapshot"])
    del snapshot["bound"]["task_id"]
    kwargs["task_snapshot"] = snapshot
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


_NON_MAPPING_SNAPSHOT_VIEW = ""


def _task_view_current_none(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = copy.deepcopy(kwargs["task_snapshot"])
    snapshot["current"] = None
    return {**kwargs, "task_snapshot": snapshot}


def _task_view_bound_none(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = copy.deepcopy(kwargs["task_snapshot"])
    snapshot["bound"] = None
    return {**kwargs, "task_snapshot": snapshot}


def _task_view_current_scalar(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = copy.deepcopy(kwargs["task_snapshot"])
    snapshot["current"] = _NON_MAPPING_SNAPSHOT_VIEW
    return {**kwargs, "task_snapshot": snapshot}


def _task_view_bound_scalar(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = copy.deepcopy(kwargs["task_snapshot"])
    snapshot["bound"] = _NON_MAPPING_SNAPSHOT_VIEW
    return {**kwargs, "task_snapshot": snapshot}


def _instruction_views_both_none(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "instruction_snapshot": {"current": None, "bound": None},
    }


def _instruction_views_both_scalar(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "instruction_snapshot": {
            "current": _NON_MAPPING_SNAPSHOT_VIEW,
            "bound": _NON_MAPPING_SNAPSHOT_VIEW,
        },
    }


def _instruction_views_both_empty_mapping(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **kwargs,
        "instruction_snapshot": {"current": {}, "bound": {}},
    }


def _instruction_snapshot_pair_missing(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {**kwargs, "instruction_snapshot": {}}


def _assert_snapshot_shape_blocked(
    tmp_path: Path,
    mutator: Any,
) -> None:
    repo = _init_target_repo(tmp_path)
    before = _git_snapshot(repo)
    kwargs = mutator(_persist_kwargs(tmp_path, target_worktree=repo))
    runtime = kwargs["runtime_root"]
    main_state_path = runtime / "main_state.yaml"
    main_before = _read_bytes(main_state_path)
    blocked = persist_pending_promotion(**kwargs)
    _assert_blocked_one_of(blocked, _RELATION_BLOCK_CODES)
    assert _pending_path(runtime).exists() is False
    _assert_protocol_idle(
        runtime,
        repo=repo,
        git_before=before,
        main_state_path=main_state_path,
        main_before=main_before,
    )


@pytest.mark.parametrize(
    "mutator",
    (
        _task_view_current_none,
        _task_view_bound_none,
        _task_view_current_scalar,
        _task_view_bound_scalar,
    ),
    ids=("current_none", "bound_none", "current_scalar", "bound_scalar"),
)
def test_persist_non_mapping_task_view_is_fail_closed_without_type_error(
    tmp_path: Path,
    mutator: Any,
) -> None:
    _assert_snapshot_shape_blocked(tmp_path, mutator)


@pytest.mark.parametrize(
    ("side", "field"),
    (
        ("current", "fingerprint"),
        ("bound", "fingerprint"),
        ("current", "digest"),
        ("bound", "digest"),
    ),
    ids=(
        "current_fingerprint",
        "bound_fingerprint",
        "current_digest",
        "bound_digest",
    ),
)
def test_persist_incomplete_task_view_is_fail_closed_without_pending(
    tmp_path: Path,
    side: str,
    field: str,
) -> None:
    def mutator(kwargs: dict[str, Any]) -> dict[str, Any]:
        snapshot = copy.deepcopy(kwargs["task_snapshot"])
        del snapshot[side][field]
        return {**kwargs, "task_snapshot": snapshot}

    _assert_snapshot_shape_blocked(tmp_path, mutator)


def test_persist_missing_instruction_pair_is_fail_closed_without_pending(
    tmp_path: Path,
) -> None:
    _assert_snapshot_shape_blocked(tmp_path, _instruction_snapshot_pair_missing)


@pytest.mark.parametrize(
    "mutator",
    (
        _instruction_views_both_none,
        _instruction_views_both_scalar,
        _instruction_views_both_empty_mapping,
    ),
    ids=("both_none", "both_scalar", "both_empty_mapping"),
)
def test_persist_symmetric_non_mapping_instruction_views_are_fail_closed(
    tmp_path: Path,
    mutator: Any,
) -> None:
    _assert_snapshot_shape_blocked(tmp_path, mutator)


@pytest.mark.parametrize(
    "field",
    ("digest", "bindings"),
    ids=("digest", "bindings"),
)
def test_persist_symmetric_incomplete_instruction_views_are_fail_closed(
    tmp_path: Path,
    field: str,
) -> None:
    def mutator(kwargs: dict[str, Any]) -> dict[str, Any]:
        snapshot = copy.deepcopy(kwargs["instruction_snapshot"])
        del snapshot["current"][field]
        del snapshot["bound"][field]
        return {**kwargs, "instruction_snapshot": snapshot}

    _assert_snapshot_shape_blocked(tmp_path, mutator)


def _task_snapshot_fingerprint_drift(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = _reference_task_snapshot(
        current=_task_view(fingerprint="9" * 64),
    )
    assert snapshot["current"]["task_id"] == snapshot["bound"]["task_id"]
    assert snapshot["current"] != snapshot["bound"]
    return {**kwargs, "task_snapshot": snapshot}


def _task_snapshot_digest_drift(kwargs: dict[str, Any]) -> dict[str, Any]:
    snapshot = _reference_task_snapshot(
        current=_task_view(digest="9" * 64),
    )
    assert snapshot["current"]["task_id"] == snapshot["bound"]["task_id"]
    assert snapshot["current"] != snapshot["bound"]
    return {**kwargs, "task_snapshot": snapshot}


@pytest.mark.parametrize(
    "mutator",
    (
        _task_snapshot_fingerprint_drift,
        _task_snapshot_digest_drift,
    ),
    ids=("fingerprint", "digest"),
)
def test_persist_complete_task_view_current_bound_drift_is_fail_closed(
    tmp_path: Path,
    mutator: Any,
) -> None:
    _assert_snapshot_shape_blocked(tmp_path, mutator)


def _noop_validation_command() -> tuple[str, ...]:
    return (sys.executable, "-c", "raise SystemExit(0)")


def _python_repo_command(source: str) -> tuple[str, ...]:
    return (sys.executable, "-c", source)


def _admit_pending_promotion_effect() -> Any:
    return getattr(promotion_module, _EFFECT_ENTRY_NAME)


def _write_owned_process_lock(runtime: Path) -> dict[str, Any]:
    ownership = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_name": "runner_git.lock",
        "run_id": _REFERENCE_ORIGIN_RUN_ID,
        "pid": os.getpid(),
        "started_at_utc": "2026-09-06T08:00:00Z",
    }
    lock_path = runtime / "runner_git.lock"
    lock_path.write_text(json.dumps(ownership) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "status": "owned",
        "reason_code": "LOCK_OK",
        "owned": True,
        "ownership": ownership,
    }


def _assert_main_restored(world: Mapping[str, Any]) -> None:
    repo = world["repo"]
    bound = world["main_sha"]
    git_dir = _git_dir(repo)
    assert (git_dir / "MERGE_HEAD").exists() is False
    assert _run_git(repo, "status", "--porcelain") == ""
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "rev-parse", "HEAD") == bound
    assert _run_git(repo, "rev-parse", "refs/heads/main") == bound


def _validation_dir(world: Mapping[str, Any]) -> Path:
    origin = world["admit_kwargs"]["worker_result"].origin_run_id
    return world["runtime"] / "runs" / origin / "validation"


def _load_validation_record(world: Mapping[str, Any]) -> dict[str, Any]:
    records = sorted(_validation_dir(world).glob("*.yaml"))
    assert records, "persisted runner validation record is required"
    payload = yaml.safe_load(records[0].read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _validation_output_path(world: Mapping[str, Any], payload: Mapping[str, Any]) -> Path:
    ref = payload["output_ref"]
    assert isinstance(ref, str) and ref
    raw = Path(ref)
    candidates = (
        (raw,)
        if raw.is_absolute()
        else (
            world["runtime"] / ref,
            world["repo"] / ref,
            _validation_dir(world) / raw.name,
        )
    )
    existing = [path for path in candidates if path.is_file()]
    assert existing, "persisted validation output_ref must be a readable file"
    return existing[0]


def _validation_output_bytes(world: Mapping[str, Any], payload: Mapping[str, Any]) -> bytes:
    return _validation_output_path(world, payload).read_bytes()


def _assert_persisted_candidate_validation(
    world: Mapping[str, Any],
    *,
    exit_code: int | None = None,
) -> dict[str, Any]:
    payload = _load_validation_record(world)
    for field in _VALIDATION_RECORD_FIELDS:
        assert field in payload
    assert Path(payload["cwd"]) == world["repo"]
    assert payload["timeout_seconds"] == world["admit_kwargs"]["validation_timeout_seconds"]
    assert payload["command"] == list(world["admit_kwargs"]["validation_command"])
    output = _validation_output_bytes(world, payload)
    assert payload["output_sha256"] == hashlib.sha256(output).hexdigest()
    if exit_code is not None:
        assert payload["exit_code"] == exit_code
    return payload


def _assert_effect_idle(world: Mapping[str, Any]) -> None:
    runtime = world["runtime"]
    _assert_protocol_idle(
        runtime,
        main_state_path=world["main_state_path"],
        main_before=world["main_before"],
    )
    assert _read_bytes(_pending_path(runtime)) == world["pending_before"]
    for forbidden in ("tock_sha", "tree_oid", "journal", "consumed_at_utc"):
        assert (runtime / f"{forbidden}.yaml").exists() is False
    _assert_main_restored(world)


def _assert_effect_blocked(
    result: Mapping[str, Any],
    reason_codes: tuple[str, ...],
    world: Mapping[str, Any],
    *,
    require_validation: bool = False,
) -> None:
    _assert_blocked_one_of(result, reason_codes)
    _assert_effect_idle(world)
    if require_validation:
        _assert_persisted_candidate_validation(world)


def _merge_candidate_view_command(
    *,
    main_sha: str,
    result_sha: str,
    stdout: str = "",
    exit_code: int = 0,
) -> tuple[str, ...]:
    example = "candidate example result\n"
    test_body = "candidate test result\n"
    source = (
        "from pathlib import Path\n"
        "import subprocess\n"
        "import sys\n"
        f"main_sha = {main_sha!r}\n"
        f"result_sha = {result_sha!r}\n"
        "git_dir = Path(subprocess.check_output("
        "['git', 'rev-parse', '--absolute-git-dir'], text=True).strip())\n"
        "assert (git_dir / 'MERGE_HEAD').read_text(encoding='utf-8').strip() == result_sha\n"
        "assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == main_sha\n"
        "assert subprocess.check_output(['git', 'rev-parse', 'refs/heads/main'], text=True).strip() == main_sha\n"
        f"assert Path('src/example.py').read_text(encoding='utf-8') == {example!r}\n"
        f"assert Path('tests/test_example.py').read_text(encoding='utf-8') == {test_body!r}\n"
        f"assert subprocess.check_output(['git', 'show', ':src/example.py'], text=True) == {example!r}\n"
        f"assert subprocess.check_output(['git', 'show', ':tests/test_example.py'], text=True) == {test_body!r}\n"
        f"sys.stdout.buffer.write({stdout.encode('utf-8')!r})\n"
        f"raise SystemExit({exit_code})\n"
    )
    return _python_repo_command(source)


def _prepare_effect_world(
    tmp_path: Path,
    *,
    flavor: str = "green",
    conflict_on_main: bool = False,
    extra_result_files: tuple[tuple[str, str], ...] = (),
    allowed_paths: tuple[str, ...] = _REFERENCE_ALLOWED_PATHS,
) -> dict[str, Any]:
    repo = _init_target_repo(tmp_path)
    _run_git(repo, "config", "commit.gpgsign", "false")
    prior_sha = _run_git(repo, "rev-parse", "HEAD")
    src = repo / "src"
    tests_dir = repo / "tests"
    src.mkdir()
    tests_dir.mkdir()
    (src / "example.py").write_text("candidate example\n", encoding="utf-8")
    (tests_dir / "test_example.py").write_text("candidate test\n", encoding="utf-8")
    _run_git(repo, "add", "--", "src/example.py", "tests/test_example.py")
    _run_git(repo, "commit", "-m", "allowed-path baseline")
    main_sha = _run_git(repo, "rev-parse", "HEAD")
    main_tree = _run_git(repo, "rev-parse", f"{main_sha}^{{tree}}")
    _run_git(repo, "checkout", "-b", _REFERENCE_TASK_BRANCH)
    (src / "example.py").write_text("candidate example result\n", encoding="utf-8")
    (tests_dir / "test_example.py").write_text("candidate test result\n", encoding="utf-8")
    extra_paths: list[str] = []
    for relative, content in extra_result_files:
        destination = repo / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")
        extra_paths.append(relative)
    _run_git(
        repo,
        "add",
        "--",
        "src/example.py",
        "tests/test_example.py",
        *extra_paths,
    )
    _run_git(repo, "commit", "-m", "task result")
    result_sha = _run_git(repo, "rev-parse", "HEAD")
    _run_git(repo, "checkout", "main")
    if conflict_on_main:
        (src / "example.py").write_text("main conflicting example\n", encoding="utf-8")
        (tests_dir / "test_example.py").write_text(
            "main conflicting test\n",
            encoding="utf-8",
        )
        _run_git(repo, "add", "--", "src/example.py", "tests/test_example.py")
        _run_git(repo, "commit", "-m", "main conflicting change")
        main_sha = _run_git(repo, "rev-parse", "HEAD")
        main_tree = _run_git(repo, "rev-parse", f"{main_sha}^{{tree}}")
    if flavor == "expected_red":
        worker = _reference_worker_result(
            result_sha=result_sha,
            task_branch=_REFERENCE_TASK_BRANCH,
            validation_results=(
                ValidationResult(
                    command_id=_REFERENCE_COMMAND_ID,
                    exit_code=1,
                    output_ref=_REFERENCE_OUTPUT_REF,
                    output_sha256=_REFERENCE_OUTPUT_SHA256,
                ),
            ),
        )
        orchestrator = _reference_orchestrator(
            promotion_class="RED_TDD_PROMOTION",
            result_sha=result_sha,
        )
        validator = _reference_validator(result_sha=result_sha)
        main_state = _reference_main_state(
            observed_main_head_sha=main_sha,
            last_tock_sha=main_sha,
            validated_result_sha=main_sha,
        )
        red_lock = dict(_opening_red_lock())
        red_lock["result_sha"] = result_sha
        intent = _qualify_expected_red(
            worker_result=worker,
            task_branch_head_sha=result_sha,
            main_state=main_state,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            red_lock=red_lock,
            allowed_paths=allowed_paths,
        )
        persist_kwargs = _persist_kwargs(
            tmp_path,
            intent=intent,
            qualify="expected_red",
            target_worktree=repo,
            worker_result=worker,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            base_sha=main_sha,
        )
        validation_command = _merge_candidate_view_command(
            main_sha=main_sha,
            result_sha=result_sha,
            stdout=_REFERENCE_FAILURE_SIGNATURE,
            exit_code=1,
        )
    elif flavor == "green_closure":
        worker = _reference_worker_result(
            result_sha=result_sha,
            task_branch=_REFERENCE_TASK_BRANCH,
        )
        orchestrator = _reference_orchestrator(result_sha=result_sha)
        validator = _reference_validator(result_sha=result_sha)
        red_lock = dict(_predecessor_red_lock())
        red_lock["result_sha"] = prior_sha
        main_state = _reference_main_state(
            state="EXPECTED_RED_OPEN",
            observed_main_head_sha=main_sha,
            last_tock_sha=main_sha,
            validated_result_sha=prior_sha,
            reason_code="STATE_EXPECTED_RED_OPEN",
        )
        intent = _qualify_green_closure(
            worker_result=worker,
            task_branch_head_sha=result_sha,
            main_state=main_state,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            red_lock=red_lock,
            allowed_paths=allowed_paths,
        )
        persist_kwargs = _persist_kwargs(
            tmp_path,
            intent=intent,
            qualify="green_closure",
            target_worktree=repo,
            worker_result=worker,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            base_sha=main_sha,
        )
        validation_command = _merge_candidate_view_command(
            main_sha=main_sha,
            result_sha=result_sha,
        )
    else:
        worker = _reference_worker_result(
            result_sha=result_sha,
            task_branch=_REFERENCE_TASK_BRANCH,
        )
        orchestrator = _reference_orchestrator(result_sha=result_sha)
        validator = _reference_validator(result_sha=result_sha)
        red_lock = None
        main_state = _reference_main_state(
            observed_main_head_sha=main_sha,
            last_tock_sha=main_sha,
            validated_result_sha=main_sha,
        )
        intent = _qualify_green(
            worker_result=worker,
            task_branch_head_sha=result_sha,
            main_state=main_state,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            allowed_paths=allowed_paths,
        )
        persist_kwargs = _persist_kwargs(
            tmp_path,
            intent=intent,
            target_worktree=repo,
            worker_result=worker,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            base_sha=main_sha,
        )
        validation_command = _merge_candidate_view_command(
            main_sha=main_sha,
            result_sha=result_sha,
        )
    persisted = persist_pending_promotion(**persist_kwargs)
    runtime = persist_kwargs["runtime_root"]
    _assert_persisted(persisted, runtime=runtime)
    lock_ownership = _write_owned_process_lock(runtime)
    admit_kwargs: dict[str, Any] = {
        "runtime_root": runtime,
        "target_worktree": repo,
        "validation_command": validation_command,
        "validation_timeout_seconds": 30,
        "command_id": _REFERENCE_COMMAND_ID,
        "task_snapshot": persist_kwargs["task_snapshot"],
        "instruction_snapshot": persist_kwargs["instruction_snapshot"],
        "worker_result": worker,
        "expected_outcome": persist_kwargs["expected_outcome"],
        "orchestrator_decision": orchestrator,
        "validator_decision": validator,
        "base_sha": main_sha,
        "task_branch_head_sha": result_sha,
        "allowed_paths": allowed_paths,
        "risk_facts": _clear_risk_facts(),
        "lock_ownership": lock_ownership,
        "git_preflight": _clean_preflight(),
        "consumed_payload": _empty_consumed(),
        "red_lock": red_lock,
        "unresolved_foreign_epoch": False,
    }
    return {
        "repo": repo,
        "runtime": runtime,
        "flavor": flavor,
        "prior_sha": prior_sha,
        "main_sha": main_sha,
        "main_tree": main_tree,
        "result_sha": result_sha,
        "git_before": _git_snapshot(repo),
        "main_state_path": runtime / "main_state.yaml",
        "main_before": _read_bytes(runtime / "main_state.yaml"),
        "pending_before": _read_bytes(_pending_path(runtime)),
        "admit_kwargs": admit_kwargs,
        "lock_ownership": lock_ownership,
        "intent": intent,
    }


def _assert_successful_tock(
    world: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    exit_code: int = 0,
) -> None:
    assert result["ok"] is True
    assert result["status"] in {"applied", "promoted", "tocked"}
    validated_tree = result["validated_candidate_tree_oid"]
    tock_sha = result["tock_sha"]
    assert result["tree_oid"] == validated_tree
    repo = world["repo"]
    tock_tree = _run_git(repo, "rev-parse", f"{tock_sha}^{{tree}}")
    assert tock_tree == validated_tree
    main_tree = _run_git(repo, "rev-parse", "refs/heads/main^{tree}")
    assert main_tree == validated_tree
    assert validated_tree != world["main_tree"]
    assert (repo / "src" / "example.py").read_text(encoding="utf-8") == (
        "candidate example result\n"
    )
    assert (repo / "tests" / "test_example.py").read_text(encoding="utf-8") == (
        "candidate test result\n"
    )
    record = _assert_persisted_candidate_validation(world, exit_code=exit_code)
    output = _validation_output_bytes(world, record)
    if world["flavor"] == "expected_red":
        assert record["exit_code"] != 0
        assert _REFERENCE_FAILURE_SIGNATURE in output.decode("utf-8")
    assert record["candidate_tree_oid"] == validated_tree
    assert (world["runtime"] / "promotion_journal.yaml").is_file()
    assert (world["runtime"] / "promotion_consumed.yaml").is_file()
    assert (world["runtime"] / "drain_outcome.yaml").exists() is False
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "status", "--porcelain") == ""
    assert _run_git(repo, "rev-parse", "HEAD") == tock_sha
    assert (_git_dir(repo) / "MERGE_HEAD").exists() is False


_RAW_EFFECT_EXCEPTIONS = (
    subprocess.CalledProcessError,
    subprocess.TimeoutExpired,
    FileNotFoundError,
    PermissionError,
    UnicodeDecodeError,
    AttributeError,
    TypeError,
    RunnerPromotionStateError,
)
_RESULT_BRANCH_BLOCK_CODES = (
    "PROMOTION_RESULT_SHA_MISMATCH",
    "PROMOTION_RESULT_SHA_MISSING",
)
_MERGE_CONFLICT_BLOCK_CODES = _TREE_DRIFT_REASON_CODES
_CONSUMPTION_MALFORMED_BLOCK_CODES = (
    "PROMOTION_CONSUMPTION_INVALID",
    "PROMOTION_CONSUMPTION_UNREADABLE",
    "PROMOTION_CONSUMPTION_VALUE_CONFLICT",
    "PROMOTION_CONSUMPTION_FIELD_UNKNOWN",
    "PROMOTION_CONSUMPTION_CONFLICT",
)
_NON_UTF8_RED_OUTPUT = _REFERENCE_FAILURE_SIGNATURE.encode("utf-8") + b"\xff\xfe"


def _call_admit(world: dict[str, Any], **overrides: Any) -> Mapping[str, Any]:
    kwargs = dict(world["admit_kwargs"])
    kwargs.update(overrides)
    world["admit_kwargs"] = kwargs
    return _admit_pending_promotion_effect()(**kwargs)


def _call_admit_fail_closed(
    world: dict[str, Any],
    **overrides: Any,
) -> Mapping[str, Any]:
    try:
        return _call_admit(world, **overrides)
    except _RAW_EFFECT_EXCEPTIONS as exc:
        raise AssertionError(
            f"raw {type(exc).__name__} leaked past admit_pending_promotion_effect"
        ) from exc


def _non_utf8_expected_red_command() -> tuple[str, ...]:
    return _python_repo_command(
        "import sys; "
        f"sys.stdout.buffer.write({_NON_UTF8_RED_OUTPUT!r}); "
        "raise SystemExit(1)"
    )


def _drift_live_task_branch_after_pending(world: dict[str, Any]) -> str:
    repo = world["repo"]
    bound = world["result_sha"]
    assert world["admit_kwargs"]["task_branch_head_sha"] == bound
    assert world["admit_kwargs"]["worker_result"].result_sha == bound
    _run_git(repo, "checkout", _REFERENCE_TASK_BRANCH)
    (repo / "src" / "example.py").write_text(
        "live drifted task branch\n",
        encoding="utf-8",
    )
    _run_git(repo, "add", "--", "src/example.py")
    _run_git(repo, "commit", "-m", "live task branch drift after pending")
    drifted = _run_git(repo, "rev-parse", "HEAD")
    _run_git(repo, "checkout", "main")
    live_ref = _run_git(repo, "rev-parse", f"refs/heads/{_REFERENCE_TASK_BRANCH}")
    assert drifted != bound
    assert live_ref == drifted
    assert world["admit_kwargs"]["task_branch_head_sha"] == bound
    assert world["admit_kwargs"]["worker_result"].result_sha == bound
    return live_ref


def _drift_live_task_branch_identical_tree(world: dict[str, Any]) -> str:
    repo = world["repo"]
    bound = world["result_sha"]
    bound_tree = _run_git(repo, "rev-parse", f"{bound}^{{tree}}")
    assert world["admit_kwargs"]["task_branch_head_sha"] == bound
    assert world["admit_kwargs"]["worker_result"].result_sha == bound
    _run_git(repo, "checkout", _REFERENCE_TASK_BRANCH)
    _run_git(repo, "commit", "--allow-empty", "-m", "identical tree live drift")
    drifted = _run_git(repo, "rev-parse", "HEAD")
    _run_git(repo, "checkout", "main")
    live_ref = _run_git(repo, "rev-parse", f"refs/heads/{_REFERENCE_TASK_BRANCH}")
    assert drifted != bound
    assert live_ref == drifted
    assert _run_git(repo, "rev-parse", f"{drifted}^{{tree}}") == bound_tree
    assert world["admit_kwargs"]["task_branch_head_sha"] == bound
    assert world["admit_kwargs"]["worker_result"].result_sha == bound
    return live_ref


def _delete_live_task_branch_ref(world: dict[str, Any]) -> None:
    repo = world["repo"]
    bound = world["result_sha"]
    assert world["admit_kwargs"]["task_branch_head_sha"] == bound
    assert world["admit_kwargs"]["worker_result"].result_sha == bound
    _run_git(repo, "cat-file", "-e", f"{bound}^{{commit}}")
    _run_git(repo, "update-ref", "-d", f"refs/heads/{_REFERENCE_TASK_BRANCH}")
    missing = subprocess.run(  # noqa: S603
        ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{_REFERENCE_TASK_BRANCH}"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    assert missing.returncode != 0
    _run_git(repo, "cat-file", "-e", f"{bound}^{{commit}}")
    assert world["admit_kwargs"]["task_branch_head_sha"] == bound
    assert world["admit_kwargs"]["worker_result"].result_sha == bound


def _consumed_path(runtime: Path) -> Path:
    return runtime / "promotion_consumed.yaml"


def _pending_document(world: Mapping[str, Any]) -> dict[str, Any]:
    payload = yaml.safe_load(world["pending_before"])
    assert isinstance(payload, dict)
    return payload


def _live_consumption_entry(
    world: Mapping[str, Any],
    **overrides: Any,
) -> dict[str, Any]:
    pending = _pending_document(world)
    entry = {
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": world["result_sha"],
        "main_state_before_promotion": pending["main_state_before_promotion"],
        "consumed_at_utc": "2026-09-06T12:00:00Z",
        "tock_sha": world["main_sha"],
    }
    entry.update(overrides)
    return entry


def _write_live_consumption(
    world: dict[str, Any],
    *entries: Mapping[str, Any],
) -> Path:
    path = _consumed_path(world["runtime"])
    payload = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "consumed_promotions": [dict(entry) for entry in entries],
    }
    _write_yaml(path, payload)
    world["consumed_live_before"] = _read_bytes(path)
    return path


def _assert_effect_blocked_preserving_live_consumption(
    result: Mapping[str, Any],
    reason_codes: tuple[str, ...],
    world: Mapping[str, Any],
) -> None:
    _assert_blocked_one_of(result, reason_codes)
    runtime = world["runtime"]
    assert (runtime / "promotion_journal.yaml").exists() is False
    assert (runtime / "drain_outcome.yaml").exists() is False
    assert _read_bytes(_pending_path(runtime)) == world["pending_before"]
    assert _read_bytes(world["main_state_path"]) == world["main_before"]
    consumed_path = _consumed_path(runtime)
    assert consumed_path.is_file()
    assert _read_bytes(consumed_path) == world["consumed_live_before"]
    _assert_main_restored(world)


def _assert_effect_blocked_with_preexisting_untracked(
    result: Mapping[str, Any],
    reason_codes: tuple[str, ...],
    world: Mapping[str, Any],
    *,
    untracked_name: str,
    payload: bytes | None = None,
) -> None:
    _assert_blocked_one_of(result, reason_codes)
    runtime = world["runtime"]
    _assert_protocol_idle(
        runtime,
        main_state_path=world["main_state_path"],
        main_before=world["main_before"],
    )
    assert _read_bytes(_pending_path(runtime)) == world["pending_before"]
    repo = world["repo"]
    git_dir = _git_dir(repo)
    assert (git_dir / "MERGE_HEAD").exists() is False
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "rev-parse", "HEAD") == world["main_sha"]
    assert _run_git(repo, "rev-parse", "refs/heads/main") == world["main_sha"]
    porcelain = _run_git(repo, "status", "--porcelain")
    assert untracked_name in porcelain
    untracked_path = repo / untracked_name
    assert untracked_path.is_file()
    if payload is not None:
        assert untracked_path.read_bytes() == payload


_UNTRACKED_VALIDATION_ARGV0_NAME = "untracked-validation-argv0"
_UNTRACKED_VALIDATION_ARGV0_BYTES = (
    b"#!/bin/sh\nexit 0\n# preexisting-untracked-argv0-bytes\n"
)


def _write_in_repo_untracked_validation_argv0(world: Mapping[str, Any]) -> Path:
    path = world["repo"] / _UNTRACKED_VALIDATION_ARGV0_NAME
    path.write_bytes(_UNTRACKED_VALIDATION_ARGV0_BYTES)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    assert _UNTRACKED_VALIDATION_ARGV0_NAME in _run_git(
        world["repo"],
        "status",
        "--porcelain",
    )
    return path


def _unexecutable_validation_path(world: Mapping[str, Any]) -> Path:
    path = (
        world["repo"].resolve().parent
        / "outside-target-repo"
        / "unexecutable-validation"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def _unstaged_validation_command() -> tuple[str, ...]:
    return _python_repo_command(
        "from pathlib import Path; "
        "Path('src/example.py').write_text('unstaged-post-validation\\n')"
    )


def _staged_validation_command() -> tuple[str, ...]:
    return _python_repo_command(
        "from pathlib import Path; "
        "import subprocess; "
        "Path('src/example.py').write_text('staged-post-validation\\n'); "
        "subprocess.run(['git', 'add', '--', 'src/example.py'], check=True)"
    )


def _index_validation_command() -> tuple[str, ...]:
    return _python_repo_command(
        "from pathlib import Path; "
        "import subprocess; "
        "path = Path('src/example.py'); "
        "original = path.read_text(); "
        "path.write_text('index-only-post-validation\\n'); "
        "subprocess.run(['git', 'add', '--', 'src/example.py'], check=True); "
        "path.write_text(original)"
    )


def _install_mutating_pre_commit_hook(repo: Path) -> None:
    hook = repo / ".git" / "hooks" / "pre-commit"
    hook.write_text(
        "#!/bin/sh\nprintf 'hook-mutated-tree\\n' >> src/example.py\n",
        encoding="utf-8",
    )
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def test_effect_admission_surface_is_keyword_only_and_binds_validation() -> None:
    admit = _admit_pending_promotion_effect()
    signature = inspect.signature(admit)
    assert list(signature.parameters) == list(_EFFECT_REQUIRED_PARAMETERS)
    for parameter in signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
    assert len(_MECHANICAL_EFFECT_ADMISSION_CONDITIONS) == 15


def test_effect_admission_blocks_post_validation_tree_drift(tmp_path: Path) -> None:
    drift_cases = (
        (
            "unstaged existing path",
            lambda world: {
                "validation_command": _unstaged_validation_command(),
            },
            ("PROMOTION_CANDIDATE_TREE_DRIFT",),
        ),
        (
            "staged existing path",
            lambda world: {
                "validation_command": _staged_validation_command(),
            },
            ("PROMOTION_CANDIDATE_TREE_DRIFT",),
        ),
        (
            "index drift after validation",
            lambda world: {
                "validation_command": _index_validation_command(),
            },
            ("PROMOTION_CANDIDATE_INDEX_DRIFT", "PROMOTION_CANDIDATE_TREE_DRIFT"),
        ),
        (
            "local hook mutation attempt",
            lambda world: _install_mutating_pre_commit_hook(world["repo"]) or {},
            ("PROMOTION_HOOK_TREE_MUTATION", "PROMOTION_CANDIDATE_TREE_DRIFT"),
        ),
    )
    for _label, mutator, reason_codes in drift_cases:
        world = _prepare_effect_world(tmp_path / _label.replace(" ", "-"))
        result = _call_admit(world, **mutator(world))
        _assert_effect_blocked(
            result,
            reason_codes,
            world,
            require_validation=True,
        )


def _condition_mutators() -> tuple[tuple[str, Any, tuple[str, ...]], ...]:
    missing_commit = "0" * 40

    def snapshots(world: dict[str, Any]) -> dict[str, Any]:
        snapshot = _reference_task_snapshot(current=_task_view(digest="9" * 64))
        return {"task_snapshot": snapshot}

    def missing_result(world: dict[str, Any]) -> dict[str, Any]:
        return {"task_branch_head_sha": missing_commit}

    def result_head_mismatch(world: dict[str, Any]) -> dict[str, Any]:
        repo = world["repo"]
        _run_git(repo, "checkout", _REFERENCE_TASK_BRANCH)
        extra = repo / "src" / "example.py"
        extra.write_text("later task head\n", encoding="utf-8")
        _run_git(repo, "add", "--", "src/example.py")
        _run_git(repo, "commit", "-m", "later task head")
        later = _run_git(repo, "rev-parse", "HEAD")
        _run_git(repo, "checkout", "main")
        return {"task_branch_head_sha": later}

    def allowed_paths(world: dict[str, Any]) -> dict[str, Any]:
        return {"allowed_paths": ("src/example.py",)}

    def risk_facts(world: dict[str, Any]) -> dict[str, Any]:
        return {"risk_facts": {"secret_risk": True, "delete_risk": False, "scope_risk": False}}

    def validation_outcome(world: dict[str, Any]) -> dict[str, Any]:
        return {
            "validation_command": _python_repo_command("raise SystemExit(1)"),
        }

    def orchestrator_not_ready(world: dict[str, Any]) -> dict[str, Any]:
        return {
            "orchestrator_decision": _reference_orchestrator(
                ready=False,
                result_sha=world["result_sha"],
            ),
        }

    def validator_not_ready(world: dict[str, Any]) -> dict[str, Any]:
        return {
            "validator_decision": _reference_validator(
                ready=False,
                result_sha=world["result_sha"],
            ),
        }

    def decision_contradiction(world: dict[str, Any]) -> dict[str, Any]:
        return {
            "orchestrator_decision": _reference_orchestrator(
                promotion_class="RED_TDD_PROMOTION",
                result_sha=world["result_sha"],
            ),
        }

    def main_state_drift(world: dict[str, Any]) -> dict[str, Any]:
        drifted = _reference_main_state(
            observed_main_head_sha=world["main_sha"],
            last_tock_sha=world["main_sha"],
            validated_result_sha=world["main_sha"],
            state="BLOCKED",
        )
        _write_yaml(world["main_state_path"], drifted)
        world["main_before"] = _read_bytes(world["main_state_path"])
        return {}

    def consumption_conflict(world: dict[str, Any]) -> dict[str, Any]:
        return {
            "consumed_payload": {
                "schema_version": _REFERENCE_SCHEMA_VERSION,
                "consumed_promotions": [
                    {
                        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
                        "result_sha": _FOREIGN_RESULT_SHA,
                        "tock_sha": world["main_sha"],
                    }
                ],
            },
        }

    def lock_foreign(world: dict[str, Any]) -> dict[str, Any]:
        ownership = dict(world["lock_ownership"]["ownership"])
        ownership["pid"] = ownership["pid"] + 1
        lock_path = world["runtime"] / "runner_git.lock"
        lock_path.write_text(json.dumps(ownership) + "\n", encoding="utf-8")
        return {
            "lock_ownership": {
                "ok": True,
                "status": "owned",
                "reason_code": "LOCK_OK",
                "owned": True,
                "ownership": ownership,
            },
        }

    def foreign_epoch(world: dict[str, Any]) -> dict[str, Any]:
        return {"unresolved_foreign_epoch": True}

    def red_lock_incomplete(world: dict[str, Any]) -> dict[str, Any]:
        return {
            "red_lock": {
                "schema_version": _REFERENCE_SCHEMA_VERSION,
                "lock_type": "red_lock",
            },
        }

    def git_preflight_dirty(world: dict[str, Any]) -> dict[str, Any]:
        (world["repo"] / "tracked.txt").write_text("dirty-before-effect\n", encoding="utf-8")
        return {"git_preflight": {"ok": False, "reason_code": "GIT_DIRTY"}}

    return (
        ("task_and_instruction_snapshots_digest_valid", snapshots, ("PROMOTION_TASK_SNAPSHOT_MISMATCH", "PROMOTION_PENDING_IDENTITY_MISMATCH")),
        ("result_commit_exists", missing_result, ("PROMOTION_RESULT_SHA_MISMATCH", "PROMOTION_RESULT_SHA_MISSING")),
        ("result_sha_matches_task_branch_head", result_head_mismatch, ("PROMOTION_RESULT_SHA_MISMATCH",)),
        ("changed_paths_within_allowed_paths", allowed_paths,             ("PROMOTION_ALLOWED_PATHS_MISMATCH", "PROMOTION_PENDING_IDENTITY_MISMATCH")),
        ("secret_delete_scope_risks_clear", risk_facts, ("PROMOTION_SECRET_RISK",)),
        ("deterministic_validation_matches_expected_outcome", validation_outcome, ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",)),
        ("orchestrator_decision_ready_identity_bound", orchestrator_not_ready, ("PROMOTION_DECISION_NOT_READY",)),
        ("validator_decision_ready_identity_bound", validator_not_ready, ("PROMOTION_DECISION_NOT_READY",)),
        ("decisions_do_not_contradict", decision_contradiction, ("PROMOTION_DECISION_CONTRADICTION",)),
        ("main_state_matches_bound_pre_promotion_state", main_state_drift, ("PROMOTION_MAIN_STATE_MISMATCH",)),
        ("promotion_attempt_not_contradictory_consumed", consumption_conflict, ("PROMOTION_CONSUMPTION_CONFLICT",)),
        ("local_lock_owned_by_current_process", lock_foreign, ("PROMOTION_LOCK_MISMATCH",)),
        ("no_unresolved_foreign_epoch", foreign_epoch, ("PROMOTION_UNRESOLVED_FOREIGN_EPOCH",)),
        ("red_lock_contract_complete", red_lock_incomplete, ("PROMOTION_RED_LOCK_UNKNOWN", "PROMOTION_RED_LOCK_MISMATCH")),
        ("git_preflight_clean_and_allowed", git_preflight_dirty, ("PROMOTION_GIT_PREFLIGHT_BLOCKED",)),
    )


@pytest.mark.parametrize(
    ("condition_id", "mutator", "reason_codes"),
    _condition_mutators(),
    ids=_MECHANICAL_EFFECT_ADMISSION_CONDITIONS,
)
def test_effect_admission_reproofs_each_mechanical_condition_before_first_effect(
    tmp_path: Path,
    condition_id: str,
    mutator: Any,
    reason_codes: tuple[str, ...],
) -> None:
    assert condition_id in _MECHANICAL_EFFECT_ADMISSION_CONDITIONS
    world = _prepare_effect_world(tmp_path)
    result = _call_admit(world, **mutator(world))
    _assert_effect_blocked(
        result,
        reason_codes,
        world,
        require_validation=condition_id
        == "deterministic_validation_matches_expected_outcome",
    )


def test_effect_admission_blocks_identity_and_evidence_drift(tmp_path: Path) -> None:
    world = _prepare_effect_world(tmp_path)
    drifted_validator = _reference_validator(result_sha=world["result_sha"])
    drifted_validator = dataclasses.replace(
        drifted_validator,
        evidence=(),
    )
    result = _call_admit(world, validator_decision=drifted_validator)
    _assert_effect_blocked(
        result,
        ("PROMOTION_EVIDENCE_MISSING", "PROMOTION_DECISION_IDENTITY_MISMATCH"),
        world,
    )


def test_effect_admission_persists_validation_without_opening_effects_on_drift(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    result = _call_admit(world, validation_command=_unstaged_validation_command())
    _assert_effect_blocked(
        result,
        ("PROMOTION_CANDIDATE_TREE_DRIFT",),
        world,
        require_validation=True,
    )
    payload = _load_validation_record(world)
    assert "tock_sha" not in payload
    assert payload["exit_code"] == 0


def test_effect_admission_blocks_base_and_live_main_drift(tmp_path: Path) -> None:
    world = _prepare_effect_world(tmp_path / "base-kwarg")
    result = _call_admit(world, base_sha=world["result_sha"])
    _assert_effect_blocked(
        result,
        ("PROMOTION_BASE_SHA_MISMATCH", "PROMOTION_MAIN_STATE_MISMATCH"),
        world,
    )
    live = _prepare_effect_world(tmp_path / "live-main")
    repo = live["repo"]
    (repo / "tracked.txt").write_text("live-main-moved\n", encoding="utf-8")
    _run_git(repo, "add", "--", "tracked.txt")
    _run_git(repo, "commit", "-m", "live main moved")
    assert _run_git(repo, "rev-parse", "refs/heads/main") != live["main_sha"]
    drifted = _call_admit(live)
    _assert_effect_blocked(
        drifted,
        ("PROMOTION_BASE_SHA_MISMATCH", "PROMOTION_MAIN_STATE_MISMATCH"),
        live,
    )


def test_effect_admission_blocks_attempt_identity_drift(tmp_path: Path) -> None:
    world = _prepare_effect_world(tmp_path)
    drifted_worker = dataclasses.replace(
        world["admit_kwargs"]["worker_result"],
        promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
    )
    result = _call_admit(world, worker_result=drifted_worker)
    _assert_effect_blocked(result, ("PROMOTION_ATTEMPT_MISMATCH",), world)


def test_effect_admission_blocks_expected_red_exit_zero(tmp_path: Path) -> None:
    world = _prepare_effect_world(tmp_path, flavor="expected_red")
    result = _call_admit(world, validation_command=_noop_validation_command())
    _assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )


def test_effect_admission_blocks_expected_red_exit_zero_with_signature(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path, flavor="expected_red")
    result = _call_admit(
        world,
        validation_command=_merge_candidate_view_command(
            main_sha=world["main_sha"],
            result_sha=world["result_sha"],
            stdout=_REFERENCE_FAILURE_SIGNATURE,
            exit_code=0,
        ),
    )
    _assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )
    payload = _assert_persisted_candidate_validation(world, exit_code=0)
    output = _validation_output_bytes(world, payload)
    assert _REFERENCE_FAILURE_SIGNATURE in output.decode("utf-8")


def test_effect_admission_blocks_expected_red_nonzero_without_signature(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path, flavor="expected_red")
    result = _call_admit(
        world,
        validation_command=_python_repo_command(
            "import sys; "
            "sys.stdout.write('synthetic unexpected passing assertion text'); "
            "raise SystemExit(1)"
        ),
    )
    _assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )


@pytest.mark.parametrize(
    "field",
    ("task_id", "origin_run_id", "promotion_attempt_id", "result_sha"),
)
def test_effect_admission_blocks_red_closure_identity_drift(
    tmp_path: Path,
    field: str,
) -> None:
    world = _prepare_effect_world(tmp_path, flavor="green_closure")
    current_epoch = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": world["result_sha"],
    }
    drifted_lock = dict(world["admit_kwargs"]["red_lock"])
    drifted_lock[field] = current_epoch[field]
    result = _call_admit(world, red_lock=drifted_lock)
    _assert_effect_blocked(result, ("PROMOTION_FOREIGN_RED",), world)


def test_effect_admission_blocks_live_task_branch_ref_drift_after_pending(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    _drift_live_task_branch_after_pending(world)
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, _RESULT_BRANCH_BLOCK_CODES, world)


def test_effect_admission_blocks_no_commit_merge_conflict_without_raw_exception(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path, conflict_on_main=True)
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, _MERGE_CONFLICT_BLOCK_CODES, world)
    _assert_main_restored(world)


@pytest.mark.parametrize(
    ("command_factory", "timeout_seconds"),
    (
        (
            lambda _world: (
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            ),
            1,
        ),
        (
            lambda world: (str(world["repo"] / "missing-validation-executable"),),
            30,
        ),
    ),
    ids=("timeout", "missing_executable"),
)
def test_effect_admission_maps_validation_invocation_failures_fail_closed(
    tmp_path: Path,
    command_factory: Any,
    timeout_seconds: int,
) -> None:
    world = _prepare_effect_world(tmp_path)
    result = _call_admit_fail_closed(
        world,
        validation_command=command_factory(world),
        validation_timeout_seconds=timeout_seconds,
    )
    _assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )
    payload = _assert_persisted_candidate_validation(world)
    assert payload["command"] == list(world["admit_kwargs"]["validation_command"])
    assert payload["timeout_seconds"] == timeout_seconds
    output = _validation_output_bytes(world, payload)
    assert payload["output_sha256"] == hashlib.sha256(output).hexdigest()


def test_effect_admission_blocks_expected_red_non_utf8_output_without_decode_error(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path, flavor="expected_red")
    result = _call_admit_fail_closed(
        world,
        validation_command=_non_utf8_expected_red_command(),
    )
    _assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )
    payload = _assert_persisted_candidate_validation(world, exit_code=1)
    output = _validation_output_bytes(world, payload)
    assert output == _NON_UTF8_RED_OUTPUT
    assert payload["output_sha256"] == hashlib.sha256(_NON_UTF8_RED_OUTPUT).hexdigest()
    with pytest.raises(UnicodeDecodeError):
        output.decode("utf-8")


def test_effect_admission_blocks_live_lock_file_foreign_pid_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    lock_path = world["runtime"] / "runner_git.lock"
    live_ownership = json.loads(lock_path.read_text(encoding="utf-8"))
    assert live_ownership["pid"] == os.getpid()
    live_ownership["pid"] = live_ownership["pid"] + 1
    lock_path.write_text(json.dumps(live_ownership) + "\n", encoding="utf-8")
    assert world["admit_kwargs"]["lock_ownership"]["ownership"]["pid"] == os.getpid()
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)


def test_effect_admission_blocks_live_dirty_porcelain_despite_trusted_preflight(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["git_preflight"]["ok"] is True
    (world["repo"] / "tracked.txt").write_text(
        "dirty-live-porcelain-before-admission\n",
        encoding="utf-8",
    )
    assert _run_git(world["repo"], "status", "--porcelain") != ""
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_GIT_PREFLIGHT_BLOCKED",), world)


def test_effect_admission_blocks_identical_tree_live_task_branch_drift(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    _drift_live_task_branch_identical_tree(world)
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_RESULT_SHA_MISMATCH",), world)


def test_effect_admission_blocks_missing_live_task_branch_ref(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    _delete_live_task_branch_ref(world)
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, _RESULT_BRANCH_BLOCK_CODES, world)


def test_effect_admission_maps_unexecutable_validation_permission_fail_closed(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    executable = _unexecutable_validation_path(world)
    assert executable.is_relative_to(world["repo"].resolve()) is False
    result = _call_admit_fail_closed(
        world,
        validation_command=(str(executable),),
    )
    _assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )
    payload = _assert_persisted_candidate_validation(world)
    assert payload["exit_code"] != 0
    output = _validation_output_bytes(world, payload)
    assert payload["output_sha256"] == hashlib.sha256(output).hexdigest()


def test_effect_admission_blocks_live_staged_porcelain_despite_trusted_preflight(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["git_preflight"]["ok"] is True
    (world["repo"] / "tracked.txt").write_text(
        "staged-live-porcelain-before-admission\n",
        encoding="utf-8",
    )
    _run_git(world["repo"], "add", "--", "tracked.txt")
    porcelain = _run_git(world["repo"], "status", "--porcelain")
    assert "tracked.txt" in porcelain
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_GIT_PREFLIGHT_BLOCKED",), world)


def test_effect_admission_blocks_live_untracked_porcelain_despite_trusted_preflight(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["git_preflight"]["ok"] is True
    untracked_name = "untracked-live-porcelain.txt"
    (world["repo"] / untracked_name).write_text(
        "untracked-live-porcelain-before-admission\n",
        encoding="utf-8",
    )
    assert untracked_name in _run_git(world["repo"], "status", "--porcelain")
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked_with_preexisting_untracked(
        result,
        ("PROMOTION_GIT_PREFLIGHT_BLOCKED",),
        world,
        untracked_name=untracked_name,
    )


def test_effect_admission_blocks_missing_live_lock_file_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    lock_path = world["runtime"] / "runner_git.lock"
    assert lock_path.is_file()
    lock_path.unlink()
    assert lock_path.exists() is False
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(
        result,
        ("PROMOTION_LOCK_MISMATCH", "PROMOTION_LOCK_MISSING"),
        world,
    )


def test_effect_admission_blocks_malformed_live_lock_file_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    lock_path = world["runtime"] / "runner_git.lock"
    lock_path.write_text("{not-valid-lock-json\n", encoding="utf-8")
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)


def test_effect_admission_blocks_live_exact_current_consumption_against_empty_view(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["consumed_payload"] == _empty_consumed()
    _write_live_consumption(world, _live_consumption_entry(world))
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked_preserving_live_consumption(
        result,
        ("PROMOTION_ALREADY_CONSUMED",),
        world,
    )


def test_effect_admission_blocks_live_single_axis_consumption_conflict_against_empty_view(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["consumed_payload"] == _empty_consumed()
    _write_live_consumption(
        world,
        _live_consumption_entry(world, result_sha=_FOREIGN_RESULT_SHA),
    )
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked_preserving_live_consumption(
        result,
        ("PROMOTION_CONSUMPTION_CONFLICT",),
        world,
    )


def test_effect_admission_blocks_malformed_live_consumption_against_empty_view(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["consumed_payload"] == _empty_consumed()
    consumed_path = _consumed_path(world["runtime"])
    consumed_path.write_text("{not: [valid: yaml\n", encoding="utf-8")
    world["consumed_live_before"] = _read_bytes(consumed_path)
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked_preserving_live_consumption(
        result,
        _CONSUMPTION_MALFORMED_BLOCK_CODES,
        world,
    )


def test_effect_admission_allows_independent_live_consumption_against_empty_view(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["consumed_payload"] == _empty_consumed()
    _write_live_consumption(
        world,
        _live_consumption_entry(
            world,
            promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
            result_sha=_FOREIGN_RESULT_SHA,
        ),
    )
    result = _call_admit_fail_closed(world)
    _assert_successful_tock(world, result, exit_code=0)
    consumed = yaml.safe_load(_consumed_path(world["runtime"]).read_text(encoding="utf-8"))
    identities = {
        (entry["promotion_attempt_id"], entry["result_sha"])
        for entry in consumed["consumed_promotions"]
    }
    assert (
        _FOREIGN_PROMOTION_ATTEMPT_ID,
        _FOREIGN_RESULT_SHA,
    ) in identities


def test_effect_admission_blocks_in_repo_untracked_validation_argv0_as_dirty_porcelain(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    assert world["admit_kwargs"]["git_preflight"]["ok"] is True
    executable = _write_in_repo_untracked_validation_argv0(world)
    result = _call_admit_fail_closed(
        world,
        validation_command=(str(executable),),
    )
    _assert_effect_blocked_with_preexisting_untracked(
        result,
        ("PROMOTION_GIT_PREFLIGHT_BLOCKED",),
        world,
        untracked_name=_UNTRACKED_VALIDATION_ARGV0_NAME,
        payload=_UNTRACKED_VALIDATION_ARGV0_BYTES,
    )


def test_effect_admission_preserves_in_repo_untracked_validation_argv0_on_early_base_mismatch(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    executable = _write_in_repo_untracked_validation_argv0(world)
    result = _call_admit_fail_closed(
        world,
        validation_command=(str(executable),),
        base_sha=_FOREIGN_BASE_SHA,
    )
    _assert_effect_blocked_with_preexisting_untracked(
        result,
        ("PROMOTION_BASE_SHA_MISMATCH",),
        world,
        untracked_name=_UNTRACKED_VALIDATION_ARGV0_NAME,
        payload=_UNTRACKED_VALIDATION_ARGV0_BYTES,
    )


@pytest.mark.parametrize(
    "raw_lock",
    (
        "[]",
        "null",
    ),
    ids=("json_array", "json_null"),
)
def test_effect_admission_blocks_live_lock_non_mapping_json_despite_owned_kwargs(
    tmp_path: Path,
    raw_lock: str,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    lock_path = world["runtime"] / "runner_git.lock"
    lock_path.write_text(raw_lock, encoding="utf-8")
    assert json.loads(lock_path.read_text(encoding="utf-8")) in ([], None)
    assert world["admit_kwargs"]["lock_ownership"]["ownership"]["pid"] == os.getpid()
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)
    assert lock_path.read_text(encoding="utf-8") == raw_lock


def test_effect_admission_blocks_live_lock_non_utf8_bytes_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    lock_path = world["runtime"] / "runner_git.lock"
    payload = b"\xff\xfe{not-utf8-lock"
    lock_path.write_bytes(payload)
    with pytest.raises(UnicodeDecodeError):
        lock_path.read_text(encoding="utf-8")
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)
    assert lock_path.read_bytes() == payload


def test_effect_admission_blocks_live_lock_same_pid_foreign_run_id_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    assert ownership_view["run_id"] == _REFERENCE_ORIGIN_RUN_ID
    lock_path = world["runtime"] / "runner_git.lock"
    live_ownership = json.loads(lock_path.read_text(encoding="utf-8"))
    assert live_ownership["pid"] == os.getpid()
    live_ownership["run_id"] = _FOREIGN_ORIGIN_RUN_ID
    lock_path.write_text(json.dumps(live_ownership) + "\n", encoding="utf-8")
    assert world["admit_kwargs"]["lock_ownership"]["ownership"]["run_id"] == (
        _REFERENCE_ORIGIN_RUN_ID
    )
    assert world["admit_kwargs"]["lock_ownership"]["ownership"]["pid"] == os.getpid()
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)


def test_effect_admission_blocks_live_lock_wrong_schema_version_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    assert ownership_view["schema_version"] == _REFERENCE_SCHEMA_VERSION
    lock_path = world["runtime"] / "runner_git.lock"
    live_ownership = json.loads(lock_path.read_text(encoding="utf-8"))
    assert live_ownership["pid"] == os.getpid()
    live_ownership["schema_version"] = "2.0"
    lock_path.write_text(json.dumps(live_ownership) + "\n", encoding="utf-8")
    assert world["admit_kwargs"]["lock_ownership"]["ownership"]["schema_version"] == (
        _REFERENCE_SCHEMA_VERSION
    )
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)


def test_effect_admission_blocks_live_lock_unknown_extra_field_despite_owned_kwargs(
    tmp_path: Path,
) -> None:
    world = _prepare_effect_world(tmp_path)
    ownership_view = world["admit_kwargs"]["lock_ownership"]["ownership"]
    assert ownership_view["pid"] == os.getpid()
    lock_path = world["runtime"] / "runner_git.lock"
    live_ownership = json.loads(lock_path.read_text(encoding="utf-8"))
    assert live_ownership["pid"] == os.getpid()
    live_ownership["orphan_field"] = "not-in-schema"
    lock_path.write_text(json.dumps(live_ownership) + "\n", encoding="utf-8")
    assert "orphan_field" not in world["admit_kwargs"]["lock_ownership"]["ownership"]
    result = _call_admit_fail_closed(world)
    _assert_effect_blocked(result, ("PROMOTION_LOCK_MISMATCH",), world)


_CANONICAL_TOCK_SUBJECT = "TOCK"
_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)
_SYNTHETIC_VALIDATED_TREE_OID = "1111111111111111111111111111111111111111"
_SYNTHETIC_FOREIGN_TREE_OID = "2222222222222222222222222222222222222222"


def _canonical_tock_message(
    *,
    promotion_attempt_id: str = _REFERENCE_PROMOTION_ATTEMPT_ID,
    task_id: str = _REFERENCE_TASK_ID,
    origin_run_id: str = _REFERENCE_ORIGIN_RUN_ID,
    result_sha: str = _REFERENCE_RESULT_SHA,
    promotion_class: str = "GREEN_PROMOTION",
    expected_base_sha: str = _REFERENCE_BASE_SHA,
) -> str:
    values = {
        "Promotion-Attempt": promotion_attempt_id,
        "Task-ID": task_id,
        "Origin-Run": origin_run_id,
        "Result-SHA": result_sha,
        "Promotion-Class": promotion_class,
        "Expected-Base": expected_base_sha,
    }
    trailers = "\n".join(
        f"{key}: {values[key]}" for key in _CANONICAL_TOCK_TRAILER_KEYS
    )
    return f"{_CANONICAL_TOCK_SUBJECT}\n\n{trailers}\n"


def _require_shared_canonical_tock_parser(expected_case: str) -> Any:
    parser = getattr(promotion_module, "parse_canonical_tock", None)
    if not callable(parser):
        pytest.fail(
            "C0 non-shared parser: parse_canonical_tock is not bound; "
            "compact one-parent writer cannot satisfy "
            f"{expected_case}"
        )
    return parser


def _parse_shared_canonical_tock(
    message: str,
    parent_oids: tuple[str, ...],
    tree_oid: str,
    *,
    validated_tree_oid: str,
    expected_case: str,
) -> Mapping[str, Any]:
    parser = _require_shared_canonical_tock_parser(expected_case)
    try:
        parsed = parser(
            message,
            parent_oids,
            tree_oid,
            validated_tree_oid=validated_tree_oid,
        )
    except RunnerGitPromotionError as exc:
        pytest.fail(
            "C0 shared parser rejected a canonical envelope with "
            f"{exc.reason_code}: {expected_case}"
        )
    assert isinstance(parsed, Mapping)
    return parsed


def _rejected_tock_reason(
    message: str,
    parent_oids: tuple[str, ...],
    tree_oid: str,
    *,
    validated_tree_oid: str,
    expected_case: str,
) -> str:
    parser = _require_shared_canonical_tock_parser(expected_case)
    with pytest.raises(RunnerGitPromotionError) as exc_info:
        parser(
            message,
            parent_oids,
            tree_oid,
            validated_tree_oid=validated_tree_oid,
        )
    return exc_info.value.reason_code


def _assert_parsed_canonical_identity(
    parsed: Mapping[str, Any],
    *,
    promotion_attempt_id: str,
    task_id: str,
    origin_run_id: str,
    result_sha: str,
    promotion_class: str,
    expected_base_sha: str,
    tree_oid: str,
) -> None:
    assert parsed["subject"] == _CANONICAL_TOCK_SUBJECT
    assert parsed["promotion_attempt_id"] == promotion_attempt_id
    assert parsed["task_id"] == task_id
    assert parsed["origin_run_id"] == origin_run_id
    assert parsed["result_sha"] == result_sha
    assert parsed["promotion_class"] == promotion_class
    assert parsed["expected_base_sha"] == expected_base_sha
    assert tuple(parsed["parent_oids"]) == (expected_base_sha, result_sha)
    assert parsed["tree_oid"] == tree_oid


@pytest.mark.parametrize("promotion_class", ("GREEN_PROMOTION", "RED_TDD_PROMOTION"))
def test_shared_parser_accepts_exact_subject_and_six_trailers(
    promotion_class: str,
) -> None:
    message = _canonical_tock_message(promotion_class=promotion_class)
    parsed = _parse_shared_canonical_tock(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case=(
            "PROMOTION_TOCK metadata parents and tree from one "
            f"parse_canonical_tock contract for {promotion_class}"
        ),
    )
    _assert_parsed_canonical_identity(
        parsed,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        result_sha=_REFERENCE_RESULT_SHA,
        promotion_class=promotion_class,
        expected_base_sha=_REFERENCE_BASE_SHA,
        tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
    )


def test_shared_parser_rejects_compact_subject_identity() -> None:
    compact = (
        "TOCK "
        f"{_REFERENCE_TASK_ID} "
        f"{_REFERENCE_PROMOTION_ATTEMPT_ID} "
        f"{_REFERENCE_RESULT_SHA}"
    )
    reason = _rejected_tock_reason(
        compact,
        (_REFERENCE_BASE_SHA,),
        _SYNTHETIC_FOREIGN_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_METADATA_INVALID for compact subject tokens",
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_one_parent() -> None:
    reason = _rejected_tock_reason(
        _canonical_tock_message(),
        (_REFERENCE_BASE_SHA,),
        _SYNTHETIC_FOREIGN_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_PARENT_MISMATCH before tree mismatch",
    )
    assert reason == "PROMOTION_TOCK_PARENT_MISMATCH"


def test_shared_parser_rejects_reversed_parents() -> None:
    reason = _rejected_tock_reason(
        _canonical_tock_message(),
        (_REFERENCE_RESULT_SHA, _REFERENCE_BASE_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_PARENT_MISMATCH for reversed [result, base]",
    )
    assert reason == "PROMOTION_TOCK_PARENT_MISMATCH"


def test_shared_parser_rejects_additional_identity_metadata() -> None:
    message = _canonical_tock_message().replace(
        f"Expected-Base: {_REFERENCE_BASE_SHA}\n",
        f"Expected-Base: {_REFERENCE_BASE_SHA}\nIdentity-Note: synthetic-extra\n",
    )
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_METADATA_INVALID for additional identity metadata",
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_duplicate_trailer() -> None:
    message = _canonical_tock_message().replace(
        f"Promotion-Attempt: {_REFERENCE_PROMOTION_ATTEMPT_ID}\n",
        (
            f"Promotion-Attempt: {_REFERENCE_PROMOTION_ATTEMPT_ID}\n"
            f"Promotion-Attempt: {_FOREIGN_PROMOTION_ATTEMPT_ID}\n"
        ),
    )
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_METADATA_INVALID for a duplicate trailer",
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_missing_trailer() -> None:
    message = _canonical_tock_message().replace(
        f"Origin-Run: {_REFERENCE_ORIGIN_RUN_ID}\n",
        "",
    )
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_METADATA_INVALID for a missing trailer",
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_duplicate_task_id_displacing_origin_run() -> None:
    message = _canonical_tock_message().replace(
        f"Origin-Run: {_REFERENCE_ORIGIN_RUN_ID}\n",
        f"Task-ID: {_FOREIGN_TASK_ID}\n",
    )
    prefix = f"{_CANONICAL_TOCK_SUBJECT}\n\n"
    trailer_lines = message[len(prefix):].splitlines()
    trailer_keys = [line.partition(": ")[0] for line in trailer_lines]
    assert message.startswith(prefix)
    assert len(trailer_lines) == len(_CANONICAL_TOCK_TRAILER_KEYS)
    assert all(line.partition(": ")[2] != "" for line in trailer_lines)
    assert trailer_keys.count("Task-ID") == 2
    assert "Origin-Run" not in trailer_keys
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case=(
            "PROMOTION_TOCK_METADATA_INVALID for a duplicate Task-ID "
            "that displaces Origin-Run"
        ),
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_foreign_subject_with_complete_trailers() -> None:
    message = _canonical_tock_message().replace(
        "TOCK\n",
        "synthetic-foreign-subject\n",
        1,
    )
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case=(
            "PROMOTION_TOCK_METADATA_INVALID for a foreign subject "
            "on an otherwise complete envelope"
        ),
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_compact_subject_with_complete_trailers() -> None:
    compact_subject = (
        "TOCK "
        f"{_REFERENCE_TASK_ID} "
        f"{_REFERENCE_PROMOTION_ATTEMPT_ID} "
        f"{_REFERENCE_RESULT_SHA}"
    )
    message = _canonical_tock_message().replace(
        "TOCK\n",
        f"{compact_subject}\n",
        1,
    )
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case=(
            "PROMOTION_TOCK_METADATA_INVALID for the compact writer subject "
            "plus the six trailers"
        ),
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_empty_required_trailer_value() -> None:
    message = _canonical_tock_message().replace(
        f"Origin-Run: {_REFERENCE_ORIGIN_RUN_ID}\n",
        "Origin-Run: \n",
    )
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_METADATA_INVALID for an empty required trailer value",
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_missing_blank_line_before_trailers() -> None:
    message = _canonical_tock_message().replace("TOCK\n\n", "TOCK\n", 1)
    reason = _rejected_tock_reason(
        message,
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_VALIDATED_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case=(
            "PROMOTION_TOCK_METADATA_INVALID when the blank line "
            "between TOCK and the trailer block is missing"
        ),
    )
    assert reason == "PROMOTION_TOCK_METADATA_INVALID"


def test_shared_parser_rejects_tree_mismatch() -> None:
    reason = _rejected_tock_reason(
        _canonical_tock_message(),
        (_REFERENCE_BASE_SHA, _REFERENCE_RESULT_SHA),
        _SYNTHETIC_FOREIGN_TREE_OID,
        validated_tree_oid=_SYNTHETIC_VALIDATED_TREE_OID,
        expected_case="PROMOTION_TOCK_TREE_MISMATCH for a foreign tree",
    )
    assert reason == "PROMOTION_TOCK_TREE_MISMATCH"


def test_admitted_tock_satisfies_shared_parser(tmp_path: Path) -> None:
    world = _prepare_effect_world(tmp_path)
    result = _call_admit(world)
    assert result["ok"] is True
    repo = world["repo"]
    tock_sha = result["tock_sha"]
    message = _run_git(repo, "log", "-1", "--pretty=%B", tock_sha)
    parent_oids = tuple(
        _run_git(repo, "rev-list", "--parents", "-n", "1", tock_sha).split()[1:]
    )
    tree_oid = _run_git(repo, "rev-parse", f"{tock_sha}^{{tree}}")
    intent = world["intent"]
    parsed = _parse_shared_canonical_tock(
        message,
        parent_oids,
        tree_oid,
        validated_tree_oid=result["validated_candidate_tree_oid"],
        expected_case=(
            "admitted commit parsed by parse_canonical_tock; "
            "compact one-parent geometry is not canonical"
        ),
    )
    _assert_parsed_canonical_identity(
        parsed,
        promotion_attempt_id=intent.promotion_attempt_id,
        task_id=intent.task_id,
        origin_run_id=intent.origin_run_id,
        result_sha=world["result_sha"],
        promotion_class=intent.promotion_class,
        expected_base_sha=world["main_sha"],
        tree_oid=result["validated_candidate_tree_oid"],
    )


_RISK_FACT_KEYS = ("secret_risk", "delete_risk", "scope_risk")
_RISK_PRODUCER_PARAMETERS = (
    "repo_root",
    "base_sha",
    "result_sha",
    "allowed_paths",
)


def _assert_clean_main(repo: Path) -> None:
    assert _run_git(repo, "status", "--porcelain", "--untracked-files=all") == ""
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"


def _produce_committed_risk(
    repo: Path,
    *,
    base_sha: str,
    result_sha: str,
    allowed_paths: tuple[str, ...],
) -> dict[str, Any]:
    producer = getattr(promotion_module, "evaluate_task_branch_risk", None)
    assert callable(producer)
    signature = inspect.signature(producer)
    assert tuple(signature.parameters) == _RISK_PRODUCER_PARAMETERS
    for parameter in signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
    produced = producer(
        repo_root=repo,
        base_sha=base_sha,
        result_sha=result_sha,
        allowed_paths=allowed_paths,
    )
    assert isinstance(produced, dict)
    return produced


def _assert_exact_risk(
    produced: Mapping[str, Any],
    *,
    secret_risk: bool,
    delete_risk: bool,
    scope_risk: bool,
) -> None:
    assert set(produced) == set(_RISK_FACT_KEYS)
    expected = {
        "secret_risk": secret_risk,
        "delete_risk": delete_risk,
        "scope_risk": scope_risk,
    }
    for key, value in expected.items():
        assert type(produced[key]) is bool
        assert produced[key] is value


def _assert_risk_source_invalid(produced: Mapping[str, Any]) -> None:
    assert produced["ok"] is False
    assert produced["status"] == "blocked"
    assert produced["reason_code"] == "PROMOTION_RISK_SOURCE_INVALID"
    assert [produced.get(key) for key in _RISK_FACT_KEYS] != [False, False, False]


def _commit_result_and_return_to_main(
    repo: Path,
    *add_paths: str,
    remove: tuple[str, ...] = (),
) -> str:
    _run_git(repo, "checkout", "-b", "result-risk")
    for relative in remove:
        _run_git(repo, "rm", "--", relative)
    if add_paths:
        _run_git(repo, "add", "--", *add_paths)
    _run_git(repo, "commit", "-m", "committed result")
    result_sha = _run_git(repo, "rev-parse", "HEAD")
    _run_git(repo, "checkout", "main")
    _assert_clean_main(repo)
    return result_sha


def test_clean_worktree_still_exposes_committed_secret_risk(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path, name="secret-risk")
    base_sha = _run_git(repo, "rev-parse", "HEAD")
    token = repo / "src" / "example.token"
    token.parent.mkdir(parents=True)
    token.write_text("synthetic-token\n", encoding="utf-8")
    result_sha = _commit_result_and_return_to_main(repo, "src/example.token")
    assert token.exists() is False
    name_status = _run_git(repo, "diff", "--name-status", base_sha, result_sha)
    assert "src/example.token" in name_status
    produced = _produce_committed_risk(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        allowed_paths=("src/example.token",),
    )
    _assert_exact_risk(produced, secret_risk=True, delete_risk=False, scope_risk=False)


def test_clean_worktree_still_exposes_committed_delete_risk(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path, name="delete-risk")
    source = repo / "src" / "example.py"
    source.parent.mkdir(parents=True)
    source.write_text("keep\n", encoding="utf-8")
    _run_git(repo, "add", "--", "src/example.py")
    _run_git(repo, "commit", "-m", "base file")
    base_sha = _run_git(repo, "rev-parse", "HEAD")
    result_sha = _commit_result_and_return_to_main(repo, remove=("src/example.py",))
    assert source.is_file()
    status, path = _run_git(repo, "diff", "--name-status", base_sha, result_sha).split("\t", 1)
    assert status == "D"
    assert path == "src/example.py"
    produced = _produce_committed_risk(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        allowed_paths=("src/example.py",),
    )
    _assert_exact_risk(produced, secret_risk=False, delete_risk=True, scope_risk=False)


def test_clean_worktree_still_exposes_committed_protected_scope_risk(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path, name="scope-risk")
    base_sha = _run_git(repo, "rev-parse", "HEAD")
    workflow = repo / ".github" / "workflows" / "ci.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text("name: ci\n", encoding="utf-8")
    result_sha = _commit_result_and_return_to_main(repo, ".github/workflows/ci.yml")
    assert workflow.exists() is False
    assert ".github/workflows/ci.yml" in _run_git(
        repo,
        "diff",
        "--name-status",
        base_sha,
        result_sha,
    )
    produced = _produce_committed_risk(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        allowed_paths=(".github/workflows/ci.yml",),
    )
    _assert_exact_risk(produced, secret_risk=False, delete_risk=False, scope_risk=True)


def test_clean_committed_result_yields_exact_false_bool_triple(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path, name="clean-risk")
    source = repo / "src" / "example.py"
    source.parent.mkdir(parents=True)
    source.write_text("value = 1\n", encoding="utf-8")
    _run_git(repo, "add", "--", "src/example.py")
    _run_git(repo, "commit", "-m", "base file")
    base_sha = _run_git(repo, "rev-parse", "HEAD")
    source.write_text("value = 2\n", encoding="utf-8")
    result_sha = _commit_result_and_return_to_main(repo, "src/example.py")
    assert source.read_text(encoding="utf-8") == "value = 1\n"
    status, path = _run_git(repo, "diff", "--name-status", base_sha, result_sha).split("\t", 1)
    assert status == "M"
    assert path == "src/example.py"
    allowed_paths = ("src/example.py",)
    produced = _produce_committed_risk(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        allowed_paths=allowed_paths,
    )
    _assert_exact_risk(produced, secret_risk=False, delete_risk=False, scope_risk=False)
    empty = _produce_committed_risk(
        repo,
        base_sha=base_sha,
        result_sha=base_sha,
        allowed_paths=allowed_paths,
    )
    _assert_exact_risk(empty, secret_risk=False, delete_risk=False, scope_risk=False)


@pytest.mark.parametrize(
    "case",
    ("missing_object", "non_commit", "reversed_relation", "glob_allowed_paths"),
)
def test_invalid_committed_risk_source_blocks(tmp_path: Path, case: str) -> None:
    repo = _init_target_repo(tmp_path, name=f"risk-source-{case}")
    base_sha = _run_git(repo, "rev-parse", "HEAD")
    allowed_paths = ("tracked.txt",)
    if case == "missing_object":
        bound = {
            "base_sha": "0" * 40,
            "result_sha": base_sha,
            "allowed_paths": allowed_paths,
        }
    elif case == "non_commit":
        bound = {
            "base_sha": base_sha,
            "result_sha": _run_git(repo, "rev-parse", f"{base_sha}^{{tree}}"),
            "allowed_paths": allowed_paths,
        }
    elif case == "reversed_relation":
        tracked = repo / "tracked.txt"
        tracked.write_text("child\n", encoding="utf-8")
        child_sha = _commit_result_and_return_to_main(repo, "tracked.txt")
        bound = {
            "base_sha": child_sha,
            "result_sha": base_sha,
            "allowed_paths": allowed_paths,
        }
    else:
        bound = {
            "base_sha": base_sha,
            "result_sha": base_sha,
            "allowed_paths": ("src/*",),
        }
    _assert_risk_source_invalid(_produce_committed_risk(repo, **bound))


def test_effect_admission_reproves_committed_secret_over_caller_all_clear(
    tmp_path: Path,
) -> None:
    allowed_paths = _REFERENCE_ALLOWED_PATHS + ("src/example.token",)
    world = _prepare_effect_world(
        tmp_path,
        extra_result_files=(("src/example.token", "synthetic-token\n"),),
        allowed_paths=allowed_paths,
    )
    repo = world["repo"]
    _assert_clean_main(repo)
    assert (repo / "src" / "example.token").exists() is False
    assert "src/example.token" in _run_git(
        repo,
        "diff",
        "--name-status",
        world["main_sha"],
        world["result_sha"],
    )
    assert world["admit_kwargs"]["risk_facts"] == {
        "secret_risk": False,
        "delete_risk": False,
        "scope_risk": False,
    }
    for key, value in world["admit_kwargs"]["risk_facts"].items():
        assert type(value) is bool
    result = _admit_pending_promotion_effect()(**world["admit_kwargs"])
    _assert_effect_blocked(result, ("PROMOTION_SECRET_RISK",), world)


_RECONSTRUCTED_DRAIN_ID = "reconciliation-drain-reconstructed-001"
_RECONSTRUCTED_PENDING_AT = "2026-09-22T00:00:00Z"
_RECONSTRUCTED_APPLYING_AT = "2026-09-22T00:01:00Z"
_PRE_EFFECT_JOURNAL_PHASES = (
    PromotionJournalPhase.PENDING,
    PromotionJournalPhase.APPLYING,
)


def _write_reconstructed_pre_effect_journal(
    world: Mapping[str, Any],
    phase: PromotionJournalPhase,
) -> Any:
    pending = load_pending_promotion(runtime_root=world["runtime"])
    path = world["runtime"] / "promotion_journal.yaml"
    journal = create_promotion_journal(
        task_id=str(pending["task_id"]),
        origin_run_id=str(pending["origin_run_id"]),
        drain_run_id=_RECONSTRUCTED_DRAIN_ID,
        promotion_attempt_id=str(pending["promotion_attempt_id"]),
        promotion_class=str(pending["promotion_class"]),
        base_main_sha=str(pending["base_sha"]),
        result_sha=str(pending["result_sha"]),
        validated_result_sha=str(pending["result_sha"]),
        task_fingerprint=str(pending["task_snapshot"]["bound"]["fingerprint"]),
        task_snapshot_sha256=str(pending["task_snapshot"]["bound"]["digest"]),
        instruction_signature_sha256=str(
            pending["instruction_snapshot"]["bound"]["digest"]
        ),
        pending_at_utc=_RECONSTRUCTED_PENDING_AT,
        reason_code="RECONCILIATION_COMPLETED",
    )
    persist_promotion_journal(path=path, journal=journal)
    if phase is PromotionJournalPhase.APPLYING:
        journal = transition_promotion_journal(
            journal,
            phase=PromotionJournalPhase.APPLYING,
            drain_run_id=_RECONSTRUCTED_DRAIN_ID,
            transitioned_at_utc=_RECONSTRUCTED_APPLYING_AT,
            reason_code="RECONCILIATION_COMPLETED",
        )
        persist_promotion_journal(path=path, journal=journal)
    loaded = load_promotion_journal(path=path)
    assert loaded.phase is phase
    assert loaded.tock_sha is None
    assert loaded.pending_at_utc == _RECONSTRUCTED_PENDING_AT
    assert loaded.base_main_sha == pending["base_sha"]
    assert loaded.validated_result_sha == pending["result_sha"]
    return loaded


@pytest.mark.parametrize("phase", _PRE_EFFECT_JOURNAL_PHASES)
def test_effect_admission_continues_reconstructed_journal_without_second_pending(
    tmp_path: Path,
    phase: PromotionJournalPhase,
) -> None:
    world = _prepare_effect_world(tmp_path)
    reconstructed = _write_reconstructed_pre_effect_journal(world, phase)
    admit = _admit_pending_promotion_effect()
    assert admit is promotion_module.admit_pending_promotion_effect
    try:
        result = admit(**world["admit_kwargs"])
    except Exception as exc:
        pytest.fail(
            "real effect admission did not continue the reconstructed journal: "
            f"{type(exc).__name__}: {exc}"
        )
    assert result["ok"] is True
    assert result["reason_code"] == "PROMOTION_EFFECT_ADMITTED"
    path = world["runtime"] / "promotion_journal.yaml"
    continued = load_promotion_journal(path=path)
    assert continued.pending_at_utc == reconstructed.pending_at_utc
    assert continued.task_id == reconstructed.task_id
    assert continued.origin_run_id == reconstructed.origin_run_id
    assert continued.promotion_attempt_id == reconstructed.promotion_attempt_id
    assert continued.result_sha == reconstructed.result_sha
    assert continued.validated_result_sha == reconstructed.validated_result_sha
    assert continued.promotion_class == reconstructed.promotion_class
    assert continued.base_main_sha == reconstructed.base_main_sha
    assert continued.phase is PromotionJournalPhase.FINALIZED
    if phase is PromotionJournalPhase.APPLYING:
        assert continued.applying_at_utc == reconstructed.applying_at_utc
    assert [item.name for item in world["runtime"].rglob("promotion_journal.yaml")] == [
        "promotion_journal.yaml"
    ]


_FOREIGN_PENDING_JOURNAL_AXES = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
    "base_main_sha",
    "validated_result_sha",
)


def _runtime_tree_bytes(runtime: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for path in sorted(runtime.rglob("*")):
        relative = path.relative_to(runtime).as_posix()
        if path.is_file():
            files[relative] = path.read_bytes()
        elif path.is_dir():
            files[relative + "/"] = b"<dir>"
    return files


def _write_foreign_existing_pending_journal(world: Mapping[str, Any]) -> Any:
    pending = load_pending_promotion(runtime_root=world["runtime"])
    path = world["runtime"] / "promotion_journal.yaml"
    journal = create_promotion_journal(
        task_id=_FOREIGN_TASK_ID,
        origin_run_id=_FOREIGN_ORIGIN_RUN_ID,
        drain_run_id=_RECONSTRUCTED_DRAIN_ID,
        promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
        promotion_class="RED_TDD_PROMOTION",
        base_main_sha=_FOREIGN_BASE_SHA,
        result_sha=_FOREIGN_RESULT_SHA,
        validated_result_sha=_FOREIGN_RESULT_SHA,
        task_fingerprint="a" * 64,
        task_snapshot_sha256="b" * 64,
        instruction_signature_sha256="c" * 64,
        pending_at_utc=_RECONSTRUCTED_PENDING_AT,
        reason_code="RECONCILIATION_COMPLETED",
    )
    persist_promotion_journal(path=path, journal=journal)
    loaded = load_promotion_journal(path=path)
    assert loaded.phase is PromotionJournalPhase.PENDING
    assert loaded.tock_sha is None
    bound = {
        "task_id": str(pending["task_id"]),
        "origin_run_id": str(pending["origin_run_id"]),
        "promotion_attempt_id": str(pending["promotion_attempt_id"]),
        "result_sha": str(pending["result_sha"]),
        "promotion_class": str(pending["promotion_class"]),
        "base_main_sha": str(pending["base_sha"]),
        "validated_result_sha": str(pending["result_sha"]),
    }
    assert bound["promotion_class"] == "GREEN_PROMOTION"
    for field in _FOREIGN_PENDING_JOURNAL_AXES:
        assert getattr(loaded, field) != bound[field]
    return loaded


_ADMISSION_PROGRESS_GIT_COMMANDS = frozenset(
    {
        "merge",
        "commit-tree",
        "update-ref",
        "symbolic-ref",
        "reset",
    }
)


def _git_object_inventory(repo: Path) -> tuple[str, ...]:
    output = _run_git(repo, "cat-file", "--batch-check", "--batch-all-objects")
    return tuple(sorted(line for line in output.splitlines() if line))


def _spy_admission_git(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    original = promotion_module._git
    calls: list[tuple[str, ...]] = []

    def _delegating_git(
        repo: Path,
        *args: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args))
        return original(repo, *args, check=check)

    monkeypatch.setattr(promotion_module, "_git", _delegating_git)
    return calls


def test_effect_admission_rejects_foreign_existing_pending_journal_before_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _prepare_effect_world(tmp_path)
    foreign = _write_foreign_existing_pending_journal(world)
    runtime = world["runtime"]
    repo = world["repo"]
    journal_path = runtime / "promotion_journal.yaml"
    marker = tmp_path / "admission-validation-marker"
    assert repo not in marker.parents
    assert runtime not in marker.parents
    assert marker.exists() is False
    assert foreign.phase is PromotionJournalPhase.PENDING
    assert foreign.tock_sha is None
    assert (runtime / "promotion_consumed.yaml").exists() is False
    admit_kwargs = dict(world["admit_kwargs"])
    admit_kwargs["validation_command"] = _python_repo_command(
        "from pathlib import Path\n"
        f"Path({os.fspath(marker)!r}).write_text('executed\\n', encoding='utf-8')\n"
        "raise SystemExit(0)\n"
    )
    before_files = _runtime_tree_bytes(runtime)
    git_before = _git_snapshot(repo)
    objects_before = _git_object_inventory(repo)
    git_calls = _spy_admission_git(monkeypatch)
    admit = _admit_pending_promotion_effect()
    assert admit is promotion_module.admit_pending_promotion_effect
    result = admit(**admit_kwargs)
    forbidden = [
        call
        for call in git_calls
        if call and call[0] in _ADMISSION_PROGRESS_GIT_COMMANDS
    ]
    assert forbidden == []
    assert marker.exists() is False
    assert _git_object_inventory(repo) == objects_before
    _assert_blocked(result, "PROMOTION_PENDING_IDENTITY_MISMATCH")
    assert _runtime_tree_bytes(runtime) == before_files
    assert _git_snapshot(repo) == git_before
    reloaded = load_promotion_journal(path=journal_path)
    assert reloaded == foreign
    assert reloaded.phase is PromotionJournalPhase.PENDING
    assert reloaded.tock_sha is None
    assert (runtime / "promotion_consumed.yaml").exists() is False
    assert _read_bytes(world["main_state_path"]) == world["main_before"]
    assert _read_bytes(_pending_path(runtime)) == world["pending_before"]


_LATE_EFFECT_JOURNAL_PHASES = (
    PromotionJournalPhase.GIT_APPLIED,
    PromotionJournalPhase.RECONCILED,
    PromotionJournalPhase.FINALIZED,
)
_LATE_EFFECT_PHASE_TIMESTAMPS = {
    PromotionJournalPhase.GIT_APPLIED: "2026-09-22T00:02:00Z",
    PromotionJournalPhase.RECONCILED: "2026-09-22T00:03:00Z",
    PromotionJournalPhase.FINALIZED: "2026-09-22T00:04:00Z",
}


def _write_matching_late_effect_journal(
    world: Mapping[str, Any],
    phase: PromotionJournalPhase,
) -> Any:
    assert phase in _LATE_EFFECT_JOURNAL_PHASES
    journal = _write_reconstructed_pre_effect_journal(
        world,
        PromotionJournalPhase.APPLYING,
    )
    path = world["runtime"] / "promotion_journal.yaml"
    for next_phase in _LATE_EFFECT_JOURNAL_PHASES:
        journal = transition_promotion_journal(
            journal,
            phase=next_phase,
            drain_run_id=_RECONSTRUCTED_DRAIN_ID,
            transitioned_at_utc=_LATE_EFFECT_PHASE_TIMESTAMPS[next_phase],
            reason_code="RECONCILIATION_COMPLETED",
            tock_sha=(
                str(world["result_sha"])
                if next_phase is PromotionJournalPhase.GIT_APPLIED
                else None
            ),
        )
        persist_promotion_journal(path=path, journal=journal)
        if next_phase is phase:
            break

    loaded = load_promotion_journal(path=path)
    pending = load_pending_promotion(runtime_root=world["runtime"])
    bound = {
        "task_id": str(pending["task_id"]),
        "origin_run_id": str(pending["origin_run_id"]),
        "promotion_attempt_id": str(pending["promotion_attempt_id"]),
        "result_sha": str(pending["result_sha"]),
        "promotion_class": str(pending["promotion_class"]),
        "base_main_sha": str(pending["base_sha"]),
        "validated_result_sha": str(pending["result_sha"]),
    }
    assert loaded.phase is phase
    assert loaded.tock_sha == world["result_sha"]
    for field in _FOREIGN_PENDING_JOURNAL_AXES:
        assert getattr(loaded, field) == bound[field]
    return loaded


@pytest.mark.parametrize(
    "phase",
    _LATE_EFFECT_JOURNAL_PHASES,
    ids=lambda phase: phase.value,
)
def test_effect_admission_rejects_matching_late_phase_journal_before_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: PromotionJournalPhase,
) -> None:
    world = _prepare_effect_world(tmp_path)
    late_journal = _write_matching_late_effect_journal(world, phase)
    runtime = world["runtime"]
    repo = world["repo"]
    journal_path = runtime / "promotion_journal.yaml"
    marker = tmp_path / f"admission-{phase.value.lower()}-validation-marker"
    assert repo not in marker.parents
    assert runtime not in marker.parents
    assert marker.exists() is False
    assert (runtime / "promotion_consumed.yaml").exists() is False
    admit_kwargs = dict(world["admit_kwargs"])
    admit_kwargs["validation_command"] = _python_repo_command(
        "from pathlib import Path\n"
        f"Path({os.fspath(marker)!r}).write_text('executed\\n', encoding='utf-8')\n"
        "raise SystemExit(0)\n"
    )
    before_files = _runtime_tree_bytes(runtime)
    git_before = _git_snapshot(repo)
    objects_before = _git_object_inventory(repo)
    git_calls = _spy_admission_git(monkeypatch)
    admit = _admit_pending_promotion_effect()
    assert admit is promotion_module.admit_pending_promotion_effect
    admission_error: Exception | None = None
    result: Mapping[str, Any] | None = None
    try:
        result = admit(**admit_kwargs)
    except Exception as exc:
        admission_error = exc

    forbidden = [
        call
        for call in git_calls
        if call and call[0] in _ADMISSION_PROGRESS_GIT_COMMANDS
    ]
    assert forbidden == []
    assert marker.exists() is False
    assert _git_object_inventory(repo) == objects_before
    assert _runtime_tree_bytes(runtime) == before_files
    assert _git_snapshot(repo) == git_before
    assert admission_error is None
    assert result is not None
    _assert_blocked(result, "UNRESOLVED_PROMOTION_JOURNAL")
    assert load_promotion_journal(path=journal_path) == late_journal
    assert (runtime / "promotion_consumed.yaml").exists() is False
    assert _read_bytes(world["main_state_path"]) == world["main_before"]
    assert _read_bytes(_pending_path(runtime)) == world["pending_before"]


@pytest.mark.parametrize(
    "phase",
    _LATE_EFFECT_JOURNAL_PHASES,
    ids=lambda phase: phase.value,
)
def test_effect_admission_checks_late_phase_before_mechanical_failure_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: PromotionJournalPhase,
) -> None:
    world = _prepare_effect_world(tmp_path)
    late_journal = _write_matching_late_effect_journal(world, phase)
    runtime = world["runtime"]
    repo = world["repo"]
    journal_path = runtime / "promotion_journal.yaml"
    marker = tmp_path / f"admission-{phase.value.lower()}-precedence-marker"
    assert repo not in marker.parents
    assert runtime not in marker.parents
    assert marker.exists() is False
    assert (runtime / "promotion_consumed.yaml").exists() is False
    admit_kwargs = dict(world["admit_kwargs"])
    assert admit_kwargs["base_sha"] != world["result_sha"]
    admit_kwargs["base_sha"] = world["result_sha"]
    admit_kwargs["validation_command"] = _python_repo_command(
        "from pathlib import Path\n"
        f"Path({os.fspath(marker)!r}).write_text('executed\\n', encoding='utf-8')\n"
        "raise SystemExit(0)\n"
    )
    before_files = _runtime_tree_bytes(runtime)
    git_before = _git_snapshot(repo)
    objects_before = _git_object_inventory(repo)
    git_calls = _spy_admission_git(monkeypatch)
    admit = _admit_pending_promotion_effect()
    assert admit is promotion_module.admit_pending_promotion_effect
    admission_error: Exception | None = None
    result: Mapping[str, Any] | None = None
    try:
        result = admit(**admit_kwargs)
    except Exception as exc:
        admission_error = exc

    forbidden = [
        call
        for call in git_calls
        if call and call[0] in _ADMISSION_PROGRESS_GIT_COMMANDS
    ]
    assert forbidden == []
    assert marker.exists() is False
    assert _git_object_inventory(repo) == objects_before
    assert _runtime_tree_bytes(runtime) == before_files
    assert _git_snapshot(repo) == git_before
    assert admission_error is None
    assert result is not None
    _assert_blocked(result, "UNRESOLVED_PROMOTION_JOURNAL")
    assert load_promotion_journal(path=journal_path) == late_journal
    assert (runtime / "promotion_consumed.yaml").exists() is False
    assert _read_bytes(world["main_state_path"]) == world["main_before"]
    assert _read_bytes(_pending_path(runtime)) == world["pending_before"]


def _admitted_anchor_values(pending: Mapping[str, Any]) -> dict[str, str]:
    task_bound = pending["task_snapshot"]["bound"]
    instruction_bound = pending["instruction_snapshot"]["bound"]
    return {
        "task_fingerprint": str(task_bound["fingerprint"]),
        "task_snapshot_sha256": str(task_bound["digest"]),
        "instruction_signature_sha256": str(instruction_bound["digest"]),
    }


def _mutate_pending_document(runtime: Path, mutator: Any) -> bytes:
    path = _pending_path(runtime)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutator(payload)
    _write_yaml(path, payload)
    return path.read_bytes()


def _write_cross_attempt_anchor_journal(world: Mapping[str, Any]) -> Any:
    pending = load_pending_promotion(runtime_root=world["runtime"])
    admitted = _admitted_anchor_values(pending)
    path = world["runtime"] / "promotion_journal.yaml"
    journal = create_promotion_journal(
        task_id=str(pending["task_id"]),
        origin_run_id=str(pending["origin_run_id"]),
        drain_run_id=_RECONSTRUCTED_DRAIN_ID,
        promotion_attempt_id=str(pending["promotion_attempt_id"]),
        promotion_class=str(pending["promotion_class"]),
        base_main_sha=str(pending["base_sha"]),
        result_sha=str(pending["result_sha"]),
        validated_result_sha=str(pending["result_sha"]),
        task_fingerprint="9" * 64,
        task_snapshot_sha256="8" * 64,
        instruction_signature_sha256="7" * 64,
        pending_at_utc=_RECONSTRUCTED_PENDING_AT,
        reason_code="RECONCILIATION_COMPLETED",
    )
    assert journal.task_fingerprint != admitted["task_fingerprint"]
    assert journal.task_snapshot_sha256 != admitted["task_snapshot_sha256"]
    assert journal.instruction_signature_sha256 != admitted[
        "instruction_signature_sha256"
    ]
    persist_promotion_journal(path=path, journal=journal)
    return load_promotion_journal(path=path)


def test_normal_producer_copies_admitted_anchors_before_commit_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _prepare_effect_world(tmp_path)
    pending = load_pending_promotion(runtime_root=world["runtime"])
    admitted = _admitted_anchor_values(pending)
    snapshot_bytes = yaml.safe_dump(pending["task_snapshot"]).encode("utf-8")
    instruction_bytes = yaml.safe_dump(pending["instruction_snapshot"]).encode("utf-8")
    assert admitted["task_fingerprint"] != hashlib.sha256(snapshot_bytes).hexdigest()
    assert admitted["instruction_signature_sha256"] != hashlib.sha256(
        instruction_bytes
    ).hexdigest()
    observed: dict[str, Any] = {}
    original = promotion_module._git

    def _spy_commit_tree(
        repo: Path,
        *args: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        if args and args[0] == "commit-tree":
            journal = load_promotion_journal(
                path=world["runtime"] / "promotion_journal.yaml"
            )
            observed["phase"] = journal.phase
            observed["tock_sha"] = journal.tock_sha
            observed["anchors"] = {
                "task_fingerprint": journal.task_fingerprint,
                "task_snapshot_sha256": journal.task_snapshot_sha256,
                "instruction_signature_sha256": journal.instruction_signature_sha256,
            }
            observed["main"] = _run_git(repo, "rev-parse", "refs/heads/main")
        return original(repo, *args, check=check)

    monkeypatch.setattr(promotion_module, "_git", _spy_commit_tree)
    result = _call_admit(world)
    assert result["ok"] is True
    assert result["reason_code"] == "PROMOTION_EFFECT_ADMITTED"
    assert observed["phase"] is PromotionJournalPhase.PENDING
    assert observed["tock_sha"] is None
    assert observed["anchors"] == admitted
    assert observed["main"] == world["main_sha"]
    finalized = load_promotion_journal(path=world["runtime"] / "promotion_journal.yaml")
    assert finalized.phase is PromotionJournalPhase.FINALIZED
    assert finalized.task_fingerprint == admitted["task_fingerprint"]
    assert finalized.task_snapshot_sha256 == admitted["task_snapshot_sha256"]
    assert (
        finalized.instruction_signature_sha256
        == admitted["instruction_signature_sha256"]
    )


def test_missing_admitted_anchor_is_not_healed_from_existing_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _prepare_effect_world(tmp_path)
    existing = _write_reconstructed_pre_effect_journal(
        world,
        PromotionJournalPhase.PENDING,
    )
    runtime = world["runtime"]
    journal_path = runtime / "promotion_journal.yaml"
    journal_before = journal_path.read_bytes()

    def _drop_fingerprint(payload: dict[str, Any]) -> None:
        del payload["task_snapshot"]["bound"]["fingerprint"]

    pending_bytes = _mutate_pending_document(runtime, _drop_fingerprint)
    git_calls = _spy_admission_git(monkeypatch)
    result = _call_admit_fail_closed(world)
    _assert_blocked(result, "JOURNAL_FIELDS_INVALID")
    assert git_calls == []
    assert journal_path.read_bytes() == journal_before
    assert load_promotion_journal(path=journal_path) == existing
    assert _read_bytes(_pending_path(runtime)) == pending_bytes
    assert (runtime / "promotion_consumed.yaml").exists() is False


def test_cross_attempt_anchor_drift_blocks_before_canonical_git_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _prepare_effect_world(tmp_path)
    drifted = _write_cross_attempt_anchor_journal(world)
    runtime = world["runtime"]
    repo = world["repo"]
    journal_path = runtime / "promotion_journal.yaml"
    journal_before = journal_path.read_bytes()
    pending_before = _read_bytes(_pending_path(runtime))
    git_calls = _spy_admission_git(monkeypatch)
    result = _call_admit_fail_closed(world)
    forbidden = [
        call
        for call in git_calls
        if call and call[0] in _ADMISSION_PROGRESS_GIT_COMMANDS
    ]
    _assert_blocked(result, "JOURNAL_FIELD_DRIFT")
    assert forbidden == []
    assert journal_path.read_bytes() == journal_before
    assert load_promotion_journal(path=journal_path) == drifted
    assert _read_bytes(_pending_path(runtime)) == pending_before
    assert (runtime / "promotion_consumed.yaml").exists() is False
    _assert_main_restored(world)
    assert _git_snapshot(repo) == world["git_before"]


def test_malformed_admitted_anchor_blocks_before_commit_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _prepare_effect_world(tmp_path)
    runtime = world["runtime"]

    def _replace_fingerprint(payload: dict[str, Any]) -> None:
        payload["task_snapshot"]["bound"]["fingerprint"] = "z" * 64

    pending_bytes = _mutate_pending_document(runtime, _replace_fingerprint)
    git_calls = _spy_admission_git(monkeypatch)
    result = _call_admit_fail_closed(world)
    _assert_blocked(result, "JOURNAL_FIELDS_INVALID")
    assert [call for call in git_calls if call and call[0] == "commit-tree"] == []
    assert (runtime / "promotion_journal.yaml").exists() is False
    assert (runtime / "promotion_consumed.yaml").exists() is False
    assert _read_bytes(_pending_path(runtime)) == pending_bytes
    _assert_main_restored(world)


def test_validated_candidate_tree_is_exactly_the_effected_tock_tree(
    tmp_path: Path,
) -> None:
    """Bind validation, the frozen candidate tree, and the effected TOCK."""
    for flavor, expected_exit in (
        ("green", 0),
        ("expected_red", 1),
        ("green_closure", 0),
    ):
        world = _prepare_effect_world(tmp_path / f"positive-{flavor}", flavor=flavor)
        result = _call_admit(world)
        _assert_successful_tock(world, result, exit_code=expected_exit)
        validation = _load_validation_record(world)
        validated_tree = validation["candidate_tree_oid"]
        assert validated_tree == result["validated_candidate_tree_oid"]
        assert validated_tree == result["tree_oid"]
        assert validated_tree == _run_git(
            world["repo"],
            "rev-parse",
            f"{result['tock_sha']}^{{tree}}",
        )
        assert validated_tree == _run_git(
            world["repo"],
            "rev-parse",
            "refs/heads/main^{tree}",
        )

    test_effect_admission_blocks_post_validation_tree_drift(
        tmp_path / "post-validation-drift",
    )
    test_effect_admission_persists_validation_without_opening_effects_on_drift(
        tmp_path / "persisted-validation-no-effect",
    )
    test_effect_admission_blocks_live_task_branch_ref_drift_after_pending(
        tmp_path / "live-result-ref-drift",
    )
    test_effect_admission_blocks_identical_tree_live_task_branch_drift(
        tmp_path / "identical-tree-result-ref-drift",
    )
    test_effect_admission_blocks_live_exact_current_consumption_against_empty_view(
        tmp_path / "already-consumed",
    )
    test_effect_admission_blocks_live_single_axis_consumption_conflict_against_empty_view(
        tmp_path / "consumption-conflict",
    )


def _load_postdecision_harness() -> Any:
    import importlib.util

    path = Path(__file__).resolve().parent / "test_cli_run.py"
    spec = importlib.util.spec_from_file_location("postdecision_cli_harness", path)
    if spec is None or spec.loader is None:
        raise AssertionError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _reject_unbound_reproof(effect: Mapping[str, Any], signature: str) -> None:
    result = effect["result"]
    authorized = result.get("ok") is True or bool(effect.get("tock_sha"))
    if authorized:
        pytest.fail(signature)
    reason = result.get("reason_code")
    if reason in {
        "PROMOTION_LOCK_MISMATCH",
        "PROMOTION_LOCK_MISSING",
        "PROMOTION_GIT_PREFLIGHT_BLOCKED",
        "PROMOTION_BASE_SHA_MISMATCH",
        "PROMOTION_RESULT_SHA_MISMATCH",
        "PROMOTION_RESULT_SHA_MISSING",
        "PROMOTION_VALIDATION_OUTCOME_MISMATCH",
        "PROMOTION_DECISION_CONTRADICTION",
    }:
        raise AssertionError((reason, effect.get("new_validation_files")))


def test_postdecision_command_reproof_rejects_sealed_command_drift(
    tmp_path: Path,
) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        harness._drift_sealed_command_text(world, "printf '%s\\n' 'UNSEALED_COMMAND'\n")
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_log_reproof_rejects_missing_sealed_log(tmp_path: Path) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        harness._remove_sealed_log(world)
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_digest_reproof_rejects_sealed_log_byte_drift(
    tmp_path: Path,
) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        harness._drift_sealed_log_bytes(world)
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_tree_reproof_rejects_sealed_candidate_tree_drift(
    tmp_path: Path,
) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        base_tree = _run_git(world["target"], "rev-parse", f"{world['base_sha']}^{{tree}}")
        if base_tree == world["sealed_tree"]:
            raise AssertionError((base_tree, world["sealed_tree"]))
        harness._drift_sealed_field(world, "candidate_tree_oid", base_tree)
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_base_reproof_rejects_sealed_base_drift(tmp_path: Path) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        drifted_base = str(world["worker"]["result_sha"])
        if drifted_base == world["base_sha"]:
            raise AssertionError((drifted_base, world["base_sha"]))
        harness._drift_sealed_field(world, "base_sha", drifted_base)
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_result_reproof_rejects_sealed_result_drift(tmp_path: Path) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        harness._drift_sealed_field(world, "result_sha", world["base_sha"])
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_attempt_reproof_rejects_sealed_attempt_drift(
    tmp_path: Path,
) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        harness._drift_sealed_field(
            world,
            "promotion_attempt_id",
            f"{world['worker']['promotion_attempt_id']}-unsealed",
        )
        _reject_unbound_reproof(
            harness._admit_sealed_pending(world),
            harness._POSTDECISION_REPROOF_SIGNATURE,
        )
    finally:
        harness._assert_product_draft_stasis()
