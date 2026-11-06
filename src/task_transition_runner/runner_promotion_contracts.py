"""Public pre-candidate intent-qualification contract.

The qualifier binds facts that can be checked without a merge-candidate
and returns an immutable ``NON_AUTHORIZING_PROMOTION_INTENT`` or a
fail-closed ``PROMOTION_*`` reason. It does not persist pending intent,
validate a candidate tree, start a journal, consume a promotion, or
materialize a TOCK.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    OrchestratorDecision,
    ValidatorDecision,
)
from task_transition_runner.worker_result import WorkerResult


_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_SNAPSHOT_KEYS = frozenset({"current", "bound"})
_RISK_KEYS = ("secret_risk", "delete_risk", "scope_risk")
_KNOWN_MAIN_STATES = frozenset(
    {"GREEN_VALIDATED", "EXPECTED_RED_OPEN", "BLOCKED"}
)
_GLOB_MARKERS = ("*", "?", "[", "]")
_RED_LOCK_REQUIRED_FIELDS = (
    "schema_version",
    "expected_main_state_after_promotion",
    "closure_gate",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "expected_failure_signature",
    "command_id",
)
_RED_LOCK_NONEMPTY_FIELDS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "expected_failure_signature",
    "command_id",
)
_OUTCOME_TO_CLASS = {
    "GREEN": "GREEN_PROMOTION",
    "EXPECTED_RED": "RED_TDD_PROMOTION",
}
_SUCCESS_REASON_CODES = (
    "PROMOTION_INTENT_QUALIFIED",
    "PROMOTION_EXPECTED_RED_QUALIFIED",
)
_BLOCKED_REASON_CODES = (
    "PROMOTION_TASK_SNAPSHOT_MISSING",
    "PROMOTION_TASK_SNAPSHOT_UNKNOWN",
    "PROMOTION_TASK_SNAPSHOT_MISMATCH",
    "PROMOTION_TASK_SNAPSHOT_CONTRADICTION",
    "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING",
    "PROMOTION_INSTRUCTION_SNAPSHOT_UNKNOWN",
    "PROMOTION_INSTRUCTION_SNAPSHOT_MISMATCH",
    "PROMOTION_INSTRUCTION_SNAPSHOT_CONTRADICTION",
    "PROMOTION_RESULT_SHA_MISSING",
    "PROMOTION_RESULT_SHA_MISMATCH",
    "PROMOTION_RESULT_CONTRADICTION",
    "PROMOTION_ATTEMPT_MISSING",
    "PROMOTION_ATTEMPT_MISMATCH",
    "PROMOTION_DECISION_MISSING",
    "PROMOTION_DECISION_UNKNOWN",
    "PROMOTION_DECISION_NOT_READY",
    "PROMOTION_DECISION_INVALID",
    "PROMOTION_DECISION_IDENTITY_MISMATCH",
    "PROMOTION_DECISION_CONTRADICTION",
    "PROMOTION_CLASS_BLOCKED",
    "PROMOTION_VALIDATOR_NO_GO",
    "PROMOTION_EVIDENCE_MISSING",
    "PROMOTION_EXPECTED_OUTCOME_MISSING",
    "PROMOTION_EXPECTED_OUTCOME_UNKNOWN",
    "PROMOTION_EXPECTED_OUTCOME_MISMATCH",
    "PROMOTION_WORKER_SUCCESS_NOT_AUTHORITY",
    "PROMOTION_ALLOWED_PATHS_MISSING",
    "PROMOTION_ALLOWED_PATHS_UNKNOWN",
    "PROMOTION_ALLOWED_PATHS_MISMATCH",
    "PROMOTION_RISK_MISSING",
    "PROMOTION_RISK_UNKNOWN",
    "PROMOTION_RISK_CONTRADICTION",
    "PROMOTION_SECRET_RISK",
    "PROMOTION_DELETE_RISK",
    "PROMOTION_SCOPE_RISK",
    "PROMOTION_MAIN_STATE_MISSING",
    "PROMOTION_MAIN_STATE_UNKNOWN",
    "PROMOTION_MAIN_STATE_MISMATCH",
    "PROMOTION_LOCK_MISSING",
    "PROMOTION_LOCK_MISMATCH",
    "PROMOTION_GIT_PREFLIGHT_BLOCKED",
    "PROMOTION_UNRESOLVED_FOREIGN_EPOCH",
    "PROMOTION_ALREADY_CONSUMED",
    "PROMOTION_CONSUMPTION_CONFLICT",
    "PROMOTION_RED_LOCK_MISSING",
    "PROMOTION_RED_LOCK_UNKNOWN",
    "PROMOTION_RED_LOCK_MISMATCH",
    "PROMOTION_FOREIGN_RED",
    "PROMOTION_RED_LOCK_ACTIVE",
)


class RunnerPromotionContractError(Exception):
    """Fail-closed violation of the public promotion-intent contract."""


@dataclass(frozen=True)
class QualifiedPromotionIntent:
    artifact_class: str
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str
    promotion_class: str
    expected_main_state_after_promotion: str
    main_state_before_promotion: dict[str, Any]
    allowed_paths: tuple[str, ...]
    red_lock_closure: dict[str, Any] | None


def allowed_promotion_reason_codes() -> tuple[str, ...]:
    return _SUCCESS_REASON_CODES + _BLOCKED_REASON_CODES


def qualify_pre_candidate_intent(
    *,
    task_snapshot: Mapping[str, Any],
    instruction_snapshot: Mapping[str, Any],
    worker_result: WorkerResult,
    task_branch_head_sha: str,
    expected_outcome: Any,
    allowed_paths: tuple[str, ...],
    risk_facts: Mapping[str, Any],
    orchestrator_state: DecisionArtifactState | str,
    orchestrator_decision: OrchestratorDecision | None,
    validator_state: DecisionArtifactState,
    validator_decision: ValidatorDecision | None,
    main_state: Mapping[str, Any],
    consumed_payload: Mapping[str, Any],
    lock_ownership: Mapping[str, Any],
    git_preflight: Mapping[str, Any],
    red_lock: Mapping[str, Any] | None,
    unresolved_foreign_epoch: bool,
) -> dict[str, Any]:
    snapshot_block = _task_snapshot_block(task_snapshot)
    if snapshot_block is not None:
        return snapshot_block
    instruction_block = _instruction_snapshot_block(instruction_snapshot)
    if instruction_block is not None:
        return instruction_block
    path_block = _allowed_paths_block(allowed_paths, worker_result)
    if path_block is not None:
        return path_block
    risk_block = _risk_facts_block(risk_facts)
    if risk_block is not None:
        return risk_block
    main_block = _main_state_block(main_state)
    if main_block is not None:
        return main_block
    lock_block = _lock_block(lock_ownership)
    if lock_block is not None:
        return lock_block
    if git_preflight.get("ok") is not True:
        return _blocked("PROMOTION_GIT_PREFLIGHT_BLOCKED")
    if unresolved_foreign_epoch:
        return _blocked("PROMOTION_UNRESOLVED_FOREIGN_EPOCH")
    consumption_block = _consumption_block(consumed_payload, worker_result)
    if consumption_block is not None:
        return consumption_block
    if worker_result.promotion_attempt_id == "":
        return _blocked("PROMOTION_ATTEMPT_MISSING")
    if (
        orchestrator_state == DecisionArtifactState.MISSING
        and validator_state == DecisionArtifactState.MISSING
    ):
        return _blocked("PROMOTION_WORKER_SUCCESS_NOT_AUTHORITY")
    if not isinstance(orchestrator_state, DecisionArtifactState) or not isinstance(
        validator_state,
        DecisionArtifactState,
    ):
        return _blocked("PROMOTION_DECISION_UNKNOWN")
    if (
        orchestrator_state == DecisionArtifactState.MISSING
        or validator_state == DecisionArtifactState.MISSING
    ):
        return _blocked("PROMOTION_DECISION_MISSING")
    if (
        orchestrator_state == DecisionArtifactState.DRAFT
        or validator_state == DecisionArtifactState.DRAFT
    ):
        return _blocked("PROMOTION_DECISION_NOT_READY")
    if (
        orchestrator_state == DecisionArtifactState.INVALID
        or validator_state == DecisionArtifactState.INVALID
    ):
        return _blocked("PROMOTION_DECISION_INVALID")
    if orchestrator_decision is None or validator_decision is None:
        return _blocked("PROMOTION_DECISION_INVALID")
    if orchestrator_decision.ready is False or validator_decision.ready is False:
        return _blocked("PROMOTION_DECISION_NOT_READY")
    if task_branch_head_sha == "":
        return _blocked("PROMOTION_RESULT_SHA_MISSING")
    if task_branch_head_sha != worker_result.result_sha:
        if orchestrator_decision.result_sha != worker_result.result_sha:
            return _blocked("PROMOTION_RESULT_CONTRADICTION")
        return _blocked("PROMOTION_RESULT_SHA_MISMATCH")
    if worker_result.promotion_attempt_id != orchestrator_decision.promotion_attempt_id:
        return _blocked("PROMOTION_ATTEMPT_MISMATCH")
    identity_block = _decision_identity_block(
        worker_result,
        orchestrator_decision,
        validator_decision,
    )
    if identity_block is not None:
        return identity_block
    if not orchestrator_decision.evidence or not validator_decision.evidence:
        return _blocked("PROMOTION_EVIDENCE_MISSING")
    if orchestrator_decision.promotion_class == "BLOCKED":
        return _blocked("PROMOTION_CLASS_BLOCKED")
    if validator_decision.verdict == "NO_GO":
        return _blocked("PROMOTION_VALIDATOR_NO_GO")
    outcome_block = _expected_outcome_block(
        expected_outcome,
        orchestrator_decision.promotion_class,
    )
    if outcome_block is not None:
        return outcome_block
    red_lock_result = _red_lock_block(
        red_lock=red_lock,
        expected_outcome=expected_outcome,
        promotion_class=orchestrator_decision.promotion_class,
        main_state=main_state,
        worker_result=worker_result,
    )
    if red_lock_result is not None and red_lock_result.get("ok") is False:
        return red_lock_result
    closure = red_lock_result
    if orchestrator_decision.promotion_class == "RED_TDD_PROMOTION":
        reason_code = "PROMOTION_EXPECTED_RED_QUALIFIED"
        expected_main_state = "EXPECTED_RED_OPEN"
    else:
        reason_code = "PROMOTION_INTENT_QUALIFIED"
        expected_main_state = "GREEN_VALIDATED"
    intent = QualifiedPromotionIntent(
        artifact_class=_ARTIFACT_CLASS,
        task_id=worker_result.task_id,
        origin_run_id=worker_result.origin_run_id,
        promotion_attempt_id=worker_result.promotion_attempt_id,
        result_sha=worker_result.result_sha,
        promotion_class=orchestrator_decision.promotion_class,
        expected_main_state_after_promotion=expected_main_state,
        main_state_before_promotion=copy.deepcopy(dict(main_state)),
        allowed_paths=tuple(allowed_paths),
        red_lock_closure=closure,
    )
    return {
        "ok": True,
        "status": "qualified",
        "reason_code": reason_code,
        "intent": intent,
    }


def _blocked(reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "blocked",
        "reason_code": reason_code,
        "intent": None,
    }


def _task_snapshot_block(task_snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    if any(key not in _SNAPSHOT_KEYS for key in task_snapshot):
        return _blocked("PROMOTION_TASK_SNAPSHOT_UNKNOWN")
    views = _snapshot_current_and_bound(
        task_snapshot,
        "PROMOTION_TASK_SNAPSHOT_MISSING",
    )
    if not isinstance(views, tuple):
        return views
    current, bound = views
    if current["task_id"] != bound["task_id"]:
        return _blocked("PROMOTION_TASK_SNAPSHOT_CONTRADICTION")
    if current != bound:
        return _blocked("PROMOTION_TASK_SNAPSHOT_MISMATCH")
    return None


def _instruction_snapshot_block(
    instruction_snapshot: Mapping[str, Any],
) -> dict[str, Any] | None:
    if any(key not in _SNAPSHOT_KEYS for key in instruction_snapshot):
        return _blocked("PROMOTION_INSTRUCTION_SNAPSHOT_UNKNOWN")
    views = _snapshot_current_and_bound(
        instruction_snapshot,
        "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING",
    )
    if not isinstance(views, tuple):
        return views
    current, bound = views
    if "digest" not in current:
        return _blocked("PROMOTION_INSTRUCTION_SNAPSHOT_MISSING")
    if current.get("bindings") != bound.get("bindings"):
        return _blocked("PROMOTION_INSTRUCTION_SNAPSHOT_CONTRADICTION")
    if current.get("digest") != bound.get("digest"):
        return _blocked("PROMOTION_INSTRUCTION_SNAPSHOT_MISMATCH")
    return None


def _snapshot_current_and_bound(
    snapshot: Mapping[str, Any],
    missing_code: str,
) -> tuple[Any, Any] | dict[str, Any]:
    if "current" not in snapshot or "bound" not in snapshot:
        return _blocked(missing_code)
    current = snapshot["current"]
    bound = snapshot["bound"]
    if current is None or bound is None:
        return _blocked(missing_code)
    return current, bound


def _allowed_paths_block(
    allowed_paths: tuple[str, ...],
    worker_result: WorkerResult,
) -> dict[str, Any] | None:
    if not allowed_paths:
        return _blocked("PROMOTION_ALLOWED_PATHS_MISSING")
    if any(
        marker in path for path in allowed_paths for marker in _GLOB_MARKERS
    ):
        return _blocked("PROMOTION_ALLOWED_PATHS_UNKNOWN")
    if any(path not in allowed_paths for path in worker_result.changed_paths):
        return _blocked("PROMOTION_ALLOWED_PATHS_MISMATCH")
    return None


def _risk_facts_block(risk_facts: Mapping[str, Any]) -> dict[str, Any] | None:
    if any(key not in risk_facts for key in _RISK_KEYS):
        return _blocked("PROMOTION_RISK_MISSING")
    extras = [key for key in risk_facts if key not in _RISK_KEYS]
    if extras:
        if any(any(name in extra for name in _RISK_KEYS) for extra in extras):
            return _blocked("PROMOTION_RISK_CONTRADICTION")
        return _blocked("PROMOTION_RISK_UNKNOWN")
    for key in _RISK_KEYS:
        if type(risk_facts[key]) is not bool:
            return _blocked("PROMOTION_RISK_UNKNOWN")
    if risk_facts["secret_risk"]:
        return _blocked("PROMOTION_SECRET_RISK")
    if risk_facts["delete_risk"]:
        return _blocked("PROMOTION_DELETE_RISK")
    if risk_facts["scope_risk"]:
        return _blocked("PROMOTION_SCOPE_RISK")
    return None


def _main_state_block(main_state: Mapping[str, Any]) -> dict[str, Any] | None:
    if not main_state:
        return _blocked("PROMOTION_MAIN_STATE_MISSING")
    state = main_state.get("state")
    if state not in _KNOWN_MAIN_STATES:
        return _blocked("PROMOTION_MAIN_STATE_UNKNOWN")
    if state == "BLOCKED":
        return _blocked("PROMOTION_MAIN_STATE_MISMATCH")
    return None


def _lock_block(lock_ownership: Mapping[str, Any]) -> dict[str, Any] | None:
    if lock_ownership.get("owned") is True:
        if lock_ownership.get("ok") is not True:
            return _blocked("PROMOTION_LOCK_MISMATCH")
        return None
    if lock_ownership.get("reason_code") == "LOCK_NONE":
        return _blocked("PROMOTION_LOCK_MISSING")
    return _blocked("PROMOTION_LOCK_MISMATCH")


def _consumption_block(
    consumed_payload: Mapping[str, Any],
    worker_result: WorkerResult,
) -> dict[str, Any] | None:
    entries = consumed_payload["consumed_promotions"]
    already = [
        entry
        for entry in entries
        if entry["promotion_attempt_id"] == worker_result.promotion_attempt_id
        and entry["result_sha"] == worker_result.result_sha
    ]
    conflict = [
        entry
        for entry in entries
        if (
            entry["promotion_attempt_id"] == worker_result.promotion_attempt_id
        )
        != (entry["result_sha"] == worker_result.result_sha)
    ]
    if already:
        return _blocked("PROMOTION_ALREADY_CONSUMED")
    if conflict:
        return _blocked("PROMOTION_CONSUMPTION_CONFLICT")
    return None


def _decision_identity_block(
    worker_result: WorkerResult,
    orchestrator_decision: OrchestratorDecision,
    validator_decision: ValidatorDecision,
) -> dict[str, Any] | None:
    worker_identity = _identity_tuple(worker_result)
    orchestrator_identity = _identity_tuple(orchestrator_decision)
    validator_identity = _identity_tuple(validator_decision)
    if (
        orchestrator_identity != worker_identity
        and validator_identity != worker_identity
    ):
        return _blocked("PROMOTION_DECISION_CONTRADICTION")
    if (
        orchestrator_identity != worker_identity
        or validator_identity != worker_identity
    ):
        return _blocked("PROMOTION_DECISION_IDENTITY_MISMATCH")
    return None


def _identity_tuple(
    artifact: WorkerResult | OrchestratorDecision | ValidatorDecision,
) -> tuple[str, str, str, str]:
    return (
        artifact.task_id,
        artifact.origin_run_id,
        artifact.promotion_attempt_id,
        artifact.result_sha,
    )


def _expected_outcome_block(
    expected_outcome: Any,
    promotion_class: str,
) -> dict[str, Any] | None:
    if expected_outcome is None:
        return _blocked("PROMOTION_EXPECTED_OUTCOME_MISSING")
    matched = _OUTCOME_TO_CLASS.get(expected_outcome.kind)
    if matched is None:
        return _blocked("PROMOTION_EXPECTED_OUTCOME_UNKNOWN")
    if matched != promotion_class:
        return _blocked("PROMOTION_EXPECTED_OUTCOME_MISMATCH")
    return None


def _blank(value: Any) -> bool:
    return value is None or value == ""


def _red_lock_completeness_block(
    red_lock: Mapping[str, Any],
) -> dict[str, Any] | None:
    if any(field not in red_lock for field in _RED_LOCK_REQUIRED_FIELDS):
        return _blocked("PROMOTION_RED_LOCK_UNKNOWN")
    if red_lock["schema_version"] != "1.0":
        return _blocked("PROMOTION_RED_LOCK_UNKNOWN")
    if red_lock["expected_main_state_after_promotion"] != "EXPECTED_RED_OPEN":
        return _blocked("PROMOTION_RED_LOCK_MISMATCH")
    if red_lock["closure_gate"] != "GREEN_PROMOTION_REQUIRED":
        return _blocked("PROMOTION_RED_LOCK_MISMATCH")
    if any(_blank(red_lock[field]) for field in _RED_LOCK_NONEMPTY_FIELDS):
        return _blocked("PROMOTION_RED_LOCK_UNKNOWN")
    return None


def _red_lock_block(
    *,
    red_lock: Mapping[str, Any] | None,
    expected_outcome: Any,
    promotion_class: str,
    main_state: Mapping[str, Any],
    worker_result: WorkerResult,
) -> dict[str, Any] | None:
    main_state_name = main_state["state"]
    if red_lock is None:
        if (
            promotion_class == "RED_TDD_PROMOTION"
            or main_state_name == "EXPECTED_RED_OPEN"
        ):
            return _blocked("PROMOTION_RED_LOCK_MISSING")
        return None
    if red_lock.get("lock_type") != "red_lock":
        return _blocked("PROMOTION_RED_LOCK_UNKNOWN")
    if (
        promotion_class == "RED_TDD_PROMOTION"
        or main_state_name == "EXPECTED_RED_OPEN"
    ):
        completeness = _red_lock_completeness_block(red_lock)
        if completeness is not None:
            return completeness
    if promotion_class == "RED_TDD_PROMOTION":
        lock_identity = (
            red_lock["task_id"],
            red_lock["origin_run_id"],
            red_lock["promotion_attempt_id"],
            red_lock["result_sha"],
        )
        worker_identity = (
            worker_result.task_id,
            worker_result.origin_run_id,
            worker_result.promotion_attempt_id,
            worker_result.result_sha,
        )
        if lock_identity != worker_identity:
            return _blocked("PROMOTION_FOREIGN_RED")
        if (
            expected_outcome.failure_signature
            != red_lock["expected_failure_signature"]
            or expected_outcome.command_id != red_lock["command_id"]
        ):
            return _blocked("PROMOTION_RED_LOCK_MISMATCH")
        return None
    if main_state_name == "EXPECTED_RED_OPEN":
        if red_lock["result_sha"] != main_state["validated_result_sha"]:
            return _blocked("PROMOTION_FOREIGN_RED")
        if (
            red_lock["task_id"] == worker_result.task_id
            or red_lock["origin_run_id"] == worker_result.origin_run_id
            or red_lock["promotion_attempt_id"] == worker_result.promotion_attempt_id
        ):
            return _blocked("PROMOTION_FOREIGN_RED")
        return {
            "closure_type": "green_red_lock_closure",
            "prior_red_result_sha": red_lock["result_sha"],
            "green_promotion_identity": {
                "task_id": worker_result.task_id,
                "origin_run_id": worker_result.origin_run_id,
                "promotion_attempt_id": worker_result.promotion_attempt_id,
                "result_sha": worker_result.result_sha,
                "promotion_class": "GREEN_PROMOTION",
            },
        }
    return _blocked("PROMOTION_RED_LOCK_ACTIVE")
