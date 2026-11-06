"""Synthetic public pre-candidate intent-qualification contract.

Keep:
- fail-closed qualification of facts that do not need a merge-candidate
- Task/instruction snapshot, result/attempt, decision/evidence,
  expected_outcome, allowed_paths/risk, main/state/lock/consumption,
  and RED-closure binding
- missing, unknown, mismatch, and contradiction as public negative edges
- worker success, prompt digest, and validator verdict as non-authority
- Direct GREEN without a prior RED obligation
- Expected RED open/close only with the same identity and exact signature
- a risk triple is exactly three ``bool`` values; null, numeric, string,
  and container values stay fail-closed

Drop:
- candidate validation, native tree OID, journal, TOCK, consumption effect
- pending persistence, main mutation, merge-candidate materialization
- private decision forms, parser envelopes, and role-named approvals
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import inspect
from collections.abc import Mapping
from typing import Any

import pytest

from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    EvidenceReference,
    OrchestratorDecision,
    ValidatorDecision,
)
from task_transition_runner.runner_promotion_contracts import (
    QualifiedPromotionIntent,
    RunnerPromotionContractError,
    allowed_promotion_reason_codes,
    qualify_pre_candidate_intent,
)
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
_FOREIGN_TASK_ID = "synthetic-task-002"
_FOREIGN_ORIGIN_RUN_ID = "origin-run-002"
_FOREIGN_PROMOTION_ATTEMPT_ID = "promotion-attempt-002"
_FOREIGN_RESULT_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_PRIOR_TASK_ID = "synthetic-task-000"
_PRIOR_ORIGIN_RUN_ID = "origin-run-000"
_PRIOR_PROMOTION_ATTEMPT_ID = "promotion-attempt-000"
_PRIOR_RESULT_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_THIRD_RESULT_SHA = "dddddddddddddddddddddddddddddddddddddddd"
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
_REFERENCE_UPDATED_AT = "2026-09-04T21:00:00Z"
_REFERENCE_LAST_TOCK_SHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
_REFERENCE_OBSERVED_HEAD_SHA = "ffffffffffffffffffffffffffffffffffffffff"
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_REQUIRED_EXPORTS = (
    "RunnerPromotionContractError",
    "QualifiedPromotionIntent",
    "qualify_pre_candidate_intent",
    "allowed_promotion_reason_codes",
)
_FORBIDDEN_EXPORTS = (
    "admit_promotion",
    "create_pending_promotion",
    "pending_promotion",
    "promote",
    "persist_pending_promotion",
    "start_journal",
    "record_promotion_consumed",
    "write_main_state",
    "materialize_merge_candidate",
    "validate_merge_candidate",
    "tock_sha",
    "tree_oid",
)


def _composed_token(*segments: str) -> str:
    return "".join(segments)


_FORBIDDEN_PARAMETERS = (
    _composed_token("orp", "_", "payload"),
    "pending_promotion",
    "tock_sha",
    "tree_oid",
    "merge_candidate",
    "journal",
    "runtime_snapshot",
    "consume",
)
_QUALIFY_REQUIRED_PARAMETERS = (
    "task_snapshot",
    "instruction_snapshot",
    "worker_result",
    "task_branch_head_sha",
    "expected_outcome",
    "allowed_paths",
    "risk_facts",
    "orchestrator_state",
    "orchestrator_decision",
    "validator_state",
    "validator_decision",
    "main_state",
    "consumed_payload",
    "lock_ownership",
    "git_preflight",
    "red_lock",
    "unresolved_foreign_epoch",
)
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
_INTENT_FIELDS = (
    "artifact_class",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
    "expected_main_state_after_promotion",
    "main_state_before_promotion",
    "allowed_paths",
    "red_lock_closure",
)
_FORBIDDEN_INTENT_FIELDS = (
    "tock_sha",
    "tree_oid",
    "journal_phase",
    "pending_path",
    "consumed_at_utc",
    "merge_candidate",
)


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


def _consumed_entry(
    *,
    attempt_id: str = _REFERENCE_PROMOTION_ATTEMPT_ID,
    result_sha: str = _REFERENCE_RESULT_SHA,
    tock_sha: str = _REFERENCE_LAST_TOCK_SHA,
) -> dict[str, Any]:
    return {
        "promotion_attempt_id": attempt_id,
        "result_sha": result_sha,
        "main_state_before_promotion": _reference_main_state(),
        "consumed_at_utc": "2026-09-04T21:05:00Z",
        "tock_sha": tock_sha,
    }


def _owned_lock() -> dict[str, Any]:
    ownership = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_name": "runner_git.lock",
        "run_id": _REFERENCE_ORIGIN_RUN_ID,
        "pid": 4242,
        "started_at_utc": "2026-09-04T20:00:00Z",
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


def _qualify(**overrides: Any) -> dict[str, Any]:
    return qualify_pre_candidate_intent(**_direct_green_kwargs(**overrides))


def _assert_blocked(result: Mapping[str, Any], reason_code: str) -> None:
    assert result["ok"] is False
    assert result["status"] == "blocked"
    assert result["reason_code"] == reason_code
    assert result.get("intent") is None
    for forbidden in ("tock_sha", "tree_oid", "journal", "pending_promotion"):
        assert forbidden not in result


def _assert_blocked_one_of(
    result: Mapping[str, Any],
    reason_codes: tuple[str, ...],
) -> None:
    assert result["reason_code"] in reason_codes
    _assert_blocked(result, result["reason_code"])


def _assert_qualified(
    result: Mapping[str, Any],
    *,
    reason_code: str,
    promotion_class: str,
    expected_main_state: str,
) -> QualifiedPromotionIntent:
    assert result["ok"] is True
    assert result["status"] == "qualified"
    assert result["reason_code"] == reason_code
    intent = result["intent"]
    assert isinstance(intent, QualifiedPromotionIntent)
    assert intent.artifact_class == _ARTIFACT_CLASS
    assert intent.promotion_class == promotion_class
    assert intent.expected_main_state_after_promotion == expected_main_state
    assert intent.task_id == _REFERENCE_TASK_ID
    assert intent.origin_run_id == _REFERENCE_ORIGIN_RUN_ID
    assert intent.promotion_attempt_id == _REFERENCE_PROMOTION_ATTEMPT_ID
    assert intent.result_sha == _REFERENCE_RESULT_SHA
    assert intent.allowed_paths == _REFERENCE_ALLOWED_PATHS
    for forbidden in _FORBIDDEN_INTENT_FIELDS:
        assert not hasattr(intent, forbidden)
    for forbidden in ("tock_sha", "tree_oid", "journal", "pending_promotion"):
        assert forbidden not in result
    return intent


def test_contract_exports_and_keyword_only_qualification_surface() -> None:
    import task_transition_runner.runner_promotion_contracts as contracts

    for name in _REQUIRED_EXPORTS:
        assert hasattr(contracts, name)
    for name in _FORBIDDEN_EXPORTS:
        assert not hasattr(contracts, name)
    signature = inspect.signature(qualify_pre_candidate_intent)
    assert list(signature.parameters) == list(_QUALIFY_REQUIRED_PARAMETERS)
    for parameter in signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
        for forbidden in _FORBIDDEN_PARAMETERS:
            if (
                forbidden == "consume"
                and parameter.name == "consumed_payload"
            ):
                continue
            assert forbidden not in parameter.name
    codes = allowed_promotion_reason_codes()
    assert codes == _SUCCESS_REASON_CODES + _BLOCKED_REASON_CODES
    assert dataclasses.is_dataclass(QualifiedPromotionIntent)
    field_names = tuple(
        field.name for field in dataclasses.fields(QualifiedPromotionIntent)
    )
    assert field_names == _INTENT_FIELDS
    assert issubclass(RunnerPromotionContractError, Exception)


def test_direct_green_qualifies_without_red_lock_or_effect_authority() -> None:
    main_state = _reference_main_state()
    result = _qualify(main_state=main_state)
    intent = _assert_qualified(
        result,
        reason_code="PROMOTION_INTENT_QUALIFIED",
        promotion_class="GREEN_PROMOTION",
        expected_main_state="GREEN_VALIDATED",
    )
    assert intent.red_lock_closure is None
    assert intent.main_state_before_promotion == main_state
    assert intent.main_state_before_promotion is not main_state
    with pytest.raises(dataclasses.FrozenInstanceError):
        intent.promotion_class = "BLOCKED"


def test_expected_red_qualifies_only_with_bound_lock_and_exact_signature() -> None:
    result = qualify_pre_candidate_intent(**_expected_red_kwargs())
    intent = _assert_qualified(
        result,
        reason_code="PROMOTION_EXPECTED_RED_QUALIFIED",
        promotion_class="RED_TDD_PROMOTION",
        expected_main_state="EXPECTED_RED_OPEN",
    )
    assert intent.red_lock_closure is None


def test_green_closes_predecessor_red_lock_without_granting_effect() -> None:
    result = qualify_pre_candidate_intent(**_green_closure_kwargs())
    intent = _assert_qualified(
        result,
        reason_code="PROMOTION_INTENT_QUALIFIED",
        promotion_class="GREEN_PROMOTION",
        expected_main_state="GREEN_VALIDATED",
    )
    closure = intent.red_lock_closure
    assert closure is not None
    assert closure["closure_type"] == "green_red_lock_closure"
    assert closure["prior_red_result_sha"] == _PRIOR_RESULT_SHA
    assert closure["green_promotion_identity"] == {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "promotion_class": "GREEN_PROMOTION",
    }


def test_green_at_expected_red_open_without_red_lock_blocks() -> None:
    result = qualify_pre_candidate_intent(**_green_closure_kwargs(red_lock=None))
    _assert_blocked(result, "PROMOTION_RED_LOCK_MISSING")


def test_worker_success_alone_never_qualifies_intent() -> None:
    result = _qualify(
        orchestrator_state=DecisionArtifactState.MISSING,
        orchestrator_decision=None,
        validator_state=DecisionArtifactState.MISSING,
        validator_decision=None,
    )
    _assert_blocked(result, "PROMOTION_WORKER_SUCCESS_NOT_AUTHORITY")
    worker = _reference_worker_result(
        worker_report=WorkerReport(
            reported_status="success",
            summary="synthetic worker summary",
        ),
    )
    assert worker.worker_report.reported_status == "success"
    assert "promotion_class" not in result
    assert not hasattr(worker.worker_report, "promotion_class")


def test_instruction_digest_is_provenance_not_promotion_class() -> None:
    result = _qualify(
        orchestrator_state=DecisionArtifactState.MISSING,
        orchestrator_decision=None,
        validator_state=DecisionArtifactState.MISSING,
        validator_decision=None,
        instruction_snapshot=_reference_instruction_snapshot(),
    )
    _assert_blocked(result, "PROMOTION_WORKER_SUCCESS_NOT_AUTHORITY")
    assert result.get("intent") is None


@pytest.mark.parametrize("reported_status", ("failure", "blocked"))
def test_valid_facts_qualify_independent_of_worker_reported_status(
    reported_status: str,
) -> None:
    result = _qualify(
        worker_result=_reference_worker_result(
            worker_report=WorkerReport(
                reported_status=reported_status,
                summary="synthetic worker summary",
            ),
        ),
    )
    intent = _assert_qualified(
        result,
        reason_code="PROMOTION_INTENT_QUALIFIED",
        promotion_class="GREEN_PROMOTION",
        expected_main_state="GREEN_VALIDATED",
    )
    assert intent.promotion_class == "GREEN_PROMOTION"


def test_validator_has_no_promotion_class_and_no_go_blocks() -> None:
    validator = _reference_validator(verdict="NO_GO")
    assert not hasattr(validator, "promotion_class")
    result = _qualify(validator_decision=validator)
    _assert_blocked(result, "PROMOTION_VALIDATOR_NO_GO")


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing", "PROMOTION_DECISION_MISSING"),
        ("draft", "PROMOTION_DECISION_NOT_READY"),
        ("invalid", "PROMOTION_DECISION_INVALID"),
    ),
)
def test_validator_state_negative_edges_with_ready_orchestrator(
    mutation: str,
    reason_code: str,
) -> None:
    overrides: dict[str, Any] = {
        "orchestrator_state": DecisionArtifactState.READY,
        "orchestrator_decision": _reference_orchestrator(),
    }
    if mutation == "missing":
        overrides["validator_state"] = DecisionArtifactState.MISSING
        overrides["validator_decision"] = None
    elif mutation == "draft":
        overrides["validator_state"] = DecisionArtifactState.DRAFT
        overrides["validator_decision"] = _reference_validator(ready=False)
    else:
        overrides["validator_state"] = DecisionArtifactState.INVALID
        overrides["validator_decision"] = None
    _assert_blocked(_qualify(**overrides), reason_code)


def test_red_tdd_class_against_green_expected_outcome_mismatches() -> None:
    result = qualify_pre_candidate_intent(
        **_expected_red_kwargs(expected_outcome=DirectGreenOutcome(kind="GREEN")),
    )
    _assert_blocked(result, "PROMOTION_EXPECTED_OUTCOME_MISMATCH")


def test_blocked_orchestrator_class_never_qualifies() -> None:
    result = _qualify(
        orchestrator_decision=_reference_orchestrator(promotion_class="BLOCKED"),
    )
    _assert_blocked(result, "PROMOTION_CLASS_BLOCKED")


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing_current", "PROMOTION_TASK_SNAPSHOT_MISSING"),
        ("unknown_field", "PROMOTION_TASK_SNAPSHOT_UNKNOWN"),
        ("mismatch_digest", "PROMOTION_TASK_SNAPSHOT_MISMATCH"),
        ("contradiction", "PROMOTION_TASK_SNAPSHOT_CONTRADICTION"),
    ),
)
def test_task_snapshot_negative_edges(mutation: str, reason_code: str) -> None:
    snapshot = _reference_task_snapshot()
    if mutation == "missing_current":
        snapshot.pop("current")
    elif mutation == "unknown_field":
        snapshot["orphan_field"] = "not-in-schema"
    elif mutation == "mismatch_digest":
        snapshot["bound"] = _task_view(digest="9" * 64)
    else:
        snapshot["current"] = _task_view(
            task_id=_FOREIGN_TASK_ID,
            digest="9" * 64,
        )
        snapshot["bound"] = _task_view()
    _assert_blocked(_qualify(task_snapshot=snapshot), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing_digest", "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING"),
        ("unknown_field", "PROMOTION_INSTRUCTION_SNAPSHOT_UNKNOWN"),
        ("mismatch_digest", "PROMOTION_INSTRUCTION_SNAPSHOT_MISMATCH"),
        ("contradiction", "PROMOTION_INSTRUCTION_SNAPSHOT_CONTRADICTION"),
    ),
)
def test_instruction_snapshot_negative_edges(mutation: str, reason_code: str) -> None:
    snapshot = _reference_instruction_snapshot()
    if mutation == "missing_digest":
        snapshot["current"] = {"bindings": snapshot["current"]["bindings"]}
    elif mutation == "unknown_field":
        snapshot["authority"] = "prompt-digest"
    elif mutation == "mismatch_digest":
        snapshot["bound"] = _instruction_view(digest="9" * 64)
    else:
        snapshot["current"] = _instruction_view(
            digest="9" * 64,
            sha256="8" * 64,
            byte_length=99,
        )
        snapshot["bound"] = _instruction_view()
    _assert_blocked(_qualify(instruction_snapshot=snapshot), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing_head", "PROMOTION_RESULT_SHA_MISSING"),
        ("mismatch_head", "PROMOTION_RESULT_SHA_MISMATCH"),
        ("contradiction", "PROMOTION_RESULT_CONTRADICTION"),
        ("missing_attempt", "PROMOTION_ATTEMPT_MISSING"),
        ("mismatch_attempt", "PROMOTION_ATTEMPT_MISMATCH"),
    ),
)
def test_result_and_attempt_negative_edges(mutation: str, reason_code: str) -> None:
    overrides: dict[str, Any] = {}
    if mutation == "missing_head":
        overrides["task_branch_head_sha"] = ""
    elif mutation == "mismatch_head":
        overrides["task_branch_head_sha"] = _FOREIGN_RESULT_SHA
    elif mutation == "contradiction":
        overrides["task_branch_head_sha"] = _FOREIGN_RESULT_SHA
        overrides["orchestrator_decision"] = _reference_orchestrator(
            result_sha=_THIRD_RESULT_SHA,
        )
        overrides["validator_decision"] = _reference_validator(
            result_sha=_THIRD_RESULT_SHA,
        )
    elif mutation == "missing_attempt":
        overrides["worker_result"] = _reference_worker_result(promotion_attempt_id="")
    else:
        overrides["worker_result"] = _reference_worker_result(
            promotion_attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID,
        )
    _assert_blocked(_qualify(**overrides), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing", "PROMOTION_DECISION_MISSING"),
        ("unknown", "PROMOTION_DECISION_UNKNOWN"),
        ("draft", "PROMOTION_DECISION_NOT_READY"),
        ("invalid", "PROMOTION_DECISION_INVALID"),
        ("identity", "PROMOTION_DECISION_IDENTITY_MISMATCH"),
        ("contradiction", "PROMOTION_DECISION_CONTRADICTION"),
        ("evidence", "PROMOTION_EVIDENCE_MISSING"),
        ("outcome_missing", "PROMOTION_EXPECTED_OUTCOME_MISSING"),
        ("outcome_unknown", "PROMOTION_EXPECTED_OUTCOME_UNKNOWN"),
        ("outcome_mismatch", "PROMOTION_EXPECTED_OUTCOME_MISMATCH"),
    ),
)
def test_decision_evidence_and_expected_outcome_negative_edges(
    mutation: str,
    reason_code: str,
) -> None:
    overrides: dict[str, Any] = {}
    if mutation == "missing":
        overrides["orchestrator_state"] = DecisionArtifactState.MISSING
        overrides["orchestrator_decision"] = None
        overrides["worker_result"] = _reference_worker_result(
            worker_report=WorkerReport(
                reported_status="blocked",
                summary="synthetic worker summary",
            ),
        )
    elif mutation == "unknown":
        overrides["orchestrator_state"] = "ready"  # type: ignore[assignment]
    elif mutation == "draft":
        overrides["orchestrator_state"] = DecisionArtifactState.DRAFT
        overrides["orchestrator_decision"] = _reference_orchestrator(ready=False)
    elif mutation == "invalid":
        overrides["orchestrator_state"] = DecisionArtifactState.INVALID
        overrides["orchestrator_decision"] = None
    elif mutation == "identity":
        overrides["validator_decision"] = _reference_validator(
            task_id=_FOREIGN_TASK_ID,
        )
    elif mutation == "contradiction":
        overrides["orchestrator_decision"] = _reference_orchestrator(
            task_id=_FOREIGN_TASK_ID,
            result_sha=_REFERENCE_RESULT_SHA,
        )
        overrides["validator_decision"] = _reference_validator(
            task_id=_REFERENCE_TASK_ID,
            result_sha=_FOREIGN_RESULT_SHA,
        )
    elif mutation == "evidence":
        overrides["orchestrator_decision"] = OrchestratorDecision(
            schema_version=_REFERENCE_SCHEMA_VERSION,
            decision_type="orchestrator",
            task_id=_REFERENCE_TASK_ID,
            origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
            promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
            result_sha=_REFERENCE_RESULT_SHA,
            ready=True,
            promotion_class="GREEN_PROMOTION",
            evidence=(),
            rationale=None,
        )
    elif mutation == "outcome_missing":
        overrides["expected_outcome"] = None
    elif mutation == "outcome_unknown":
        overrides["expected_outcome"] = DirectGreenOutcome(kind="UNKNOWN")
    else:
        overrides["expected_outcome"] = ExpectedRedOutcome(
            kind="EXPECTED_RED",
            command_id=_REFERENCE_COMMAND_ID,
            failure_signature=_REFERENCE_FAILURE_SIGNATURE,
        )
    _assert_blocked(_qualify(**overrides), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing", "PROMOTION_ALLOWED_PATHS_MISSING"),
        ("unknown", "PROMOTION_ALLOWED_PATHS_UNKNOWN"),
        ("mismatch", "PROMOTION_ALLOWED_PATHS_MISMATCH"),
        ("risk_missing", "PROMOTION_RISK_MISSING"),
        ("risk_unknown", "PROMOTION_RISK_UNKNOWN"),
        ("risk_contradiction", "PROMOTION_RISK_CONTRADICTION"),
        ("secret", "PROMOTION_SECRET_RISK"),
        ("delete", "PROMOTION_DELETE_RISK"),
        ("scope", "PROMOTION_SCOPE_RISK"),
    ),
)
def test_allowed_paths_and_risk_negative_edges(mutation: str, reason_code: str) -> None:
    overrides: dict[str, Any] = {}
    if mutation == "missing":
        overrides["allowed_paths"] = ()
    elif mutation == "unknown":
        overrides["allowed_paths"] = (_REFERENCE_ALLOWED_PATHS[0], "src/*")
    elif mutation == "mismatch":
        overrides["worker_result"] = _reference_worker_result(
            changed_paths=_REFERENCE_ALLOWED_PATHS + ("docs/notes.txt",),
        )
    elif mutation == "risk_missing":
        overrides["risk_facts"] = {"secret_risk": False, "delete_risk": False}
    elif mutation == "risk_unknown":
        overrides["risk_facts"] = {**_clear_risk_facts(), "orphan_risk": True}
    elif mutation == "risk_contradiction":
        overrides["risk_facts"] = {
            **_clear_risk_facts(),
            "open_secret_risk": True,
        }
    elif mutation == "secret":
        overrides["risk_facts"] = {**_clear_risk_facts(), "secret_risk": True}
    elif mutation == "delete":
        overrides["risk_facts"] = {**_clear_risk_facts(), "delete_risk": True}
    else:
        overrides["risk_facts"] = {**_clear_risk_facts(), "scope_risk": True}
    _assert_blocked(_qualify(**overrides), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing", "PROMOTION_MAIN_STATE_MISSING"),
        ("unknown", "PROMOTION_MAIN_STATE_UNKNOWN"),
        ("mismatch", "PROMOTION_MAIN_STATE_MISMATCH"),
        ("lock_missing", "PROMOTION_LOCK_MISSING"),
        ("lock_mismatch", "PROMOTION_LOCK_MISMATCH"),
        ("preflight", "PROMOTION_GIT_PREFLIGHT_BLOCKED"),
        ("foreign_epoch", "PROMOTION_UNRESOLVED_FOREIGN_EPOCH"),
    ),
)
def test_main_state_lock_and_preflight_negative_edges(
    mutation: str,
    reason_code: str,
) -> None:
    overrides: dict[str, Any] = {}
    if mutation == "missing":
        overrides["main_state"] = {}
    elif mutation == "unknown":
        overrides["main_state"] = _reference_main_state(state="ORPHAN_STATE")
    elif mutation == "mismatch":
        overrides["main_state"] = _reference_main_state(state="BLOCKED")
    elif mutation == "lock_missing":
        overrides["lock_ownership"] = {
            "ok": True,
            "reason_code": "LOCK_NONE",
            "owned": False,
        }
    elif mutation == "lock_mismatch":
        lock = _owned_lock()
        lock["ok"] = False
        lock["owned"] = False
        lock["reason_code"] = "LOCK_OWNERSHIP_LOST"
        overrides["lock_ownership"] = lock
    elif mutation == "preflight":
        overrides["git_preflight"] = {
            "ok": False,
            "reason_code": "GIT_PREFLIGHT_DIRTY",
        }
    else:
        overrides["unresolved_foreign_epoch"] = True
    _assert_blocked(_qualify(**overrides), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("already_consumed", "PROMOTION_ALREADY_CONSUMED"),
        ("conflict", "PROMOTION_CONSUMPTION_CONFLICT"),
        ("conflict_same_result", "PROMOTION_CONSUMPTION_CONFLICT"),
    ),
)
def test_consumption_negative_edges(mutation: str, reason_code: str) -> None:
    if mutation == "already_consumed":
        consumed = {
            "schema_version": _REFERENCE_SCHEMA_VERSION,
            "consumed_promotions": [_consumed_entry()],
        }
    elif mutation == "conflict":
        consumed = {
            "schema_version": _REFERENCE_SCHEMA_VERSION,
            "consumed_promotions": [
                _consumed_entry(result_sha=_FOREIGN_RESULT_SHA),
            ],
        }
    else:
        consumed = {
            "schema_version": _REFERENCE_SCHEMA_VERSION,
            "consumed_promotions": [
                _consumed_entry(attempt_id=_FOREIGN_PROMOTION_ATTEMPT_ID),
            ],
        }
    _assert_blocked(_qualify(consumed_payload=consumed), reason_code)


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("missing", "PROMOTION_RED_LOCK_MISSING"),
        ("unknown", "PROMOTION_RED_LOCK_UNKNOWN"),
        ("mismatch_signature", "PROMOTION_RED_LOCK_MISMATCH"),
        ("foreign_identity", "PROMOTION_FOREIGN_RED"),
        ("active_leftover", "PROMOTION_RED_LOCK_ACTIVE"),
        ("same_epoch_closure", "PROMOTION_FOREIGN_RED"),
    ),
)
def test_red_closure_negative_edges(mutation: str, reason_code: str) -> None:
    if mutation == "missing":
        result = qualify_pre_candidate_intent(**_expected_red_kwargs(red_lock=None))
    elif mutation == "unknown":
        lock = _opening_red_lock()
        lock["lock_type"] = "orphan_lock"
        result = qualify_pre_candidate_intent(**_expected_red_kwargs(red_lock=lock))
    elif mutation == "mismatch_signature":
        lock = _opening_red_lock()
        lock["expected_failure_signature"] = "foreign failure signature"
        result = qualify_pre_candidate_intent(**_expected_red_kwargs(red_lock=lock))
    elif mutation == "foreign_identity":
        lock = _predecessor_red_lock()
        lock["result_sha"] = _FOREIGN_RESULT_SHA
        result = qualify_pre_candidate_intent(
            **_green_closure_kwargs(red_lock=lock),
        )
    elif mutation == "active_leftover":
        result = _qualify(red_lock=_predecessor_red_lock())
    else:
        result = qualify_pre_candidate_intent(
            **_green_closure_kwargs(red_lock=_opening_red_lock()),
        )
    _assert_blocked(result, reason_code)


def test_expected_red_without_exact_command_binding_blocks() -> None:
    outcome = ExpectedRedOutcome(
        kind="EXPECTED_RED",
        command_id="foreign-command",
        failure_signature=_REFERENCE_FAILURE_SIGNATURE,
    )
    result = qualify_pre_candidate_intent(
        **_expected_red_kwargs(expected_outcome=outcome),
    )
    _assert_blocked(result, "PROMOTION_RED_LOCK_MISMATCH")


def test_qualification_does_not_persist_or_authorize_effects() -> None:
    result = _qualify()
    intent = result["intent"]
    assert set(result) == {"ok", "status", "reason_code", "intent"}
    assert intent.artifact_class == _ARTIFACT_CLASS
    assert result["reason_code"] != "PROMOTION_ALREADY_CONSUMED"
    consumed = _empty_consumed()
    again = _qualify(consumed_payload=consumed)
    assert again["ok"] is True
    assert consumed == _empty_consumed()


@pytest.mark.parametrize(
    "overrides",
    (
        {
            "orchestrator_state": DecisionArtifactState.READY,
            "orchestrator_decision": None,
        },
        {
            "validator_state": DecisionArtifactState.READY,
            "validator_decision": None,
        },
    ),
    ids=("orchestrator", "validator"),
)
def test_ready_state_with_none_decision_is_fail_closed(
    overrides: dict[str, Any],
) -> None:
    result = _qualify(**overrides)
    _assert_blocked_one_of(
        result,
        ("PROMOTION_DECISION_INVALID", "PROMOTION_DECISION_MISSING"),
    )


def test_task_snapshot_missing_bound_is_fail_closed() -> None:
    snapshot = _reference_task_snapshot()
    snapshot.pop("bound")
    result = _qualify(task_snapshot=snapshot)
    _assert_blocked(result, "PROMOTION_TASK_SNAPSHOT_MISSING")


def test_instruction_snapshot_missing_bound_is_fail_closed() -> None:
    snapshot = _reference_instruction_snapshot()
    snapshot.pop("bound")
    result = _qualify(instruction_snapshot=snapshot)
    _assert_blocked(result, "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING")


def test_empty_validator_evidence_is_fail_closed() -> None:
    validator = ValidatorDecision(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        decision_type="validator",
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=_REFERENCE_RESULT_SHA,
        ready=True,
        verdict="GO",
        evidence=(),
        rationale=None,
    )
    result = _qualify(validator_decision=validator)
    _assert_blocked(result, "PROMOTION_EVIDENCE_MISSING")


def test_expected_red_foreign_lock_identity_with_matching_signature_blocks() -> None:
    lock = _opening_red_lock()
    lock["task_id"] = _FOREIGN_TASK_ID
    lock["origin_run_id"] = _FOREIGN_ORIGIN_RUN_ID
    lock["promotion_attempt_id"] = _FOREIGN_PROMOTION_ATTEMPT_ID
    lock["result_sha"] = _FOREIGN_RESULT_SHA
    result = qualify_pre_candidate_intent(**_expected_red_kwargs(red_lock=lock))
    _assert_blocked_one_of(
        result,
        ("PROMOTION_FOREIGN_RED", "PROMOTION_RED_LOCK_MISMATCH"),
    )


def test_lock_owned_true_with_ok_false_is_mismatch() -> None:
    lock = _owned_lock()
    lock["ok"] = False
    result = _qualify(lock_ownership=lock)
    _assert_blocked(result, "PROMOTION_LOCK_MISMATCH")


def test_consumed_payload_inspect_name_is_builtin_str_fact_axis() -> None:
    signature = inspect.signature(qualify_pre_candidate_intent)
    consumed_name = signature.parameters["consumed_payload"].name
    assert consumed_name == "consumed_payload"
    assert type(consumed_name) is str
    assert "consume" in consumed_name
    for parameter in signature.parameters.values():
        assert type(parameter.name) is str
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY


def test_instruction_snapshot_missing_current_is_fail_closed() -> None:
    snapshot = _reference_instruction_snapshot()
    snapshot.pop("current")
    result = _qualify(instruction_snapshot=snapshot)
    _assert_blocked(result, "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING")


@pytest.mark.parametrize(
    "overrides",
    (
        {
            "orchestrator_state": DecisionArtifactState.READY,
            "orchestrator_decision": _reference_orchestrator(ready=False),
        },
        {
            "validator_state": DecisionArtifactState.READY,
            "validator_decision": _reference_validator(ready=False),
        },
    ),
    ids=("orchestrator", "validator"),
)
def test_ready_state_with_unready_decision_is_fail_closed(
    overrides: dict[str, Any],
) -> None:
    result = _qualify(**overrides)
    _assert_blocked_one_of(
        result,
        ("PROMOTION_DECISION_NOT_READY", "PROMOTION_DECISION_INVALID"),
    )


def test_validator_state_unknown_is_fail_closed() -> None:
    result = _qualify(validator_state="ready")  # type: ignore[arg-type]
    _assert_blocked(result, "PROMOTION_DECISION_UNKNOWN")


@pytest.mark.parametrize(
    ("kind", "field", "reason_code"),
    (
        ("task", "current", "PROMOTION_TASK_SNAPSHOT_MISSING"),
        ("task", "bound", "PROMOTION_TASK_SNAPSHOT_MISSING"),
        ("instruction", "current", "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING"),
        ("instruction", "bound", "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING"),
    ),
)
def test_snapshot_none_current_or_bound_is_fail_closed(
    kind: str,
    field: str,
    reason_code: str,
) -> None:
    if kind == "task":
        snapshot = _reference_task_snapshot()
        snapshot[field] = None
        result = _qualify(task_snapshot=snapshot)
    else:
        snapshot = _reference_instruction_snapshot()
        snapshot[field] = None
        result = _qualify(instruction_snapshot=snapshot)
    _assert_blocked(result, reason_code)


@pytest.mark.parametrize(
    "axis",
    ("task_id", "origin_run_id", "promotion_attempt_id"),
)
def test_green_closure_blocks_when_predecessor_lock_axis_matches_current_epoch(
    axis: str,
) -> None:
    lock = _predecessor_red_lock()
    assert lock["result_sha"] == _PRIOR_RESULT_SHA
    lock[axis] = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
    }[axis]
    result = qualify_pre_candidate_intent(**_green_closure_kwargs(red_lock=lock))
    _assert_blocked(result, "PROMOTION_FOREIGN_RED")


@pytest.mark.parametrize("path", ("expected_red", "green_closure"))
@pytest.mark.parametrize(
    "field",
    (
        "schema_version",
        "expected_main_state_after_promotion",
        "closure_gate",
    ),
)
@pytest.mark.parametrize("mode", ("wrong", "missing"))
def test_red_lock_incomplete_schema_state_or_gate_blocks(
    path: str,
    field: str,
    mode: str,
) -> None:
    lock = (
        _opening_red_lock()
        if path == "expected_red"
        else _predecessor_red_lock()
    )
    if mode == "missing":
        lock.pop(field)
    else:
        lock[field] = {
            "schema_version": "2.0",
            "expected_main_state_after_promotion": "GREEN_VALIDATED",
            "closure_gate": "NONE",
        }[field]
    kwargs = (
        _expected_red_kwargs(red_lock=lock)
        if path == "expected_red"
        else _green_closure_kwargs(red_lock=lock)
    )
    result = qualify_pre_candidate_intent(**kwargs)
    _assert_blocked_one_of(
        result,
        (
            "PROMOTION_RED_LOCK_UNKNOWN",
            "PROMOTION_RED_LOCK_MISMATCH",
            "PROMOTION_FOREIGN_RED",
        ),
    )


@pytest.mark.parametrize("path", ("expected_red", "green_closure"))
@pytest.mark.parametrize(
    "field",
    (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
        "expected_failure_signature",
        "command_id",
    ),
)
def test_red_lock_missing_required_identity_result_or_declaration_blocks(
    path: str,
    field: str,
) -> None:
    lock = (
        _opening_red_lock()
        if path == "expected_red"
        else _predecessor_red_lock()
    )
    lock.pop(field)
    kwargs = (
        _expected_red_kwargs(red_lock=lock)
        if path == "expected_red"
        else _green_closure_kwargs(red_lock=lock)
    )
    result = qualify_pre_candidate_intent(**kwargs)
    _assert_blocked_one_of(
        result,
        (
            "PROMOTION_RED_LOCK_UNKNOWN",
            "PROMOTION_RED_LOCK_MISMATCH",
            "PROMOTION_FOREIGN_RED",
        ),
    )


@pytest.mark.parametrize(
    "field",
    (
        "expected_failure_signature",
        "command_id",
    ),
)
@pytest.mark.parametrize("value", ("", None))
def test_green_closure_empty_or_none_declaration_fields_block(
    field: str,
    value: str | None,
) -> None:
    lock = _predecessor_red_lock()
    lock[field] = value
    result = qualify_pre_candidate_intent(**_green_closure_kwargs(red_lock=lock))
    _assert_blocked_one_of(
        result,
        (
            "PROMOTION_RED_LOCK_UNKNOWN",
            "PROMOTION_RED_LOCK_MISMATCH",
            "PROMOTION_FOREIGN_RED",
        ),
    )


@pytest.mark.parametrize(
    "field",
    (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
    ),
)
@pytest.mark.parametrize("value", ("", None))
def test_green_closure_empty_or_none_predecessor_identity_fields_block(
    field: str,
    value: str | None,
) -> None:
    lock = _predecessor_red_lock()
    lock[field] = value
    result = qualify_pre_candidate_intent(**_green_closure_kwargs(red_lock=lock))
    _assert_blocked_one_of(
        result,
        (
            "PROMOTION_RED_LOCK_UNKNOWN",
            "PROMOTION_RED_LOCK_MISMATCH",
            "PROMOTION_FOREIGN_RED",
        ),
    )


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("secret_risk", None),
        ("delete_risk", 0),
        ("scope_risk", 1),
        ("secret_risk", ""),
        ("delete_risk", []),
    ),
    ids=("null", "numeric_zero", "numeric_one", "string", "container"),
)
def test_non_bool_risk_facts_fail_closed(key: str, value: Any) -> None:
    risk_facts = _clear_risk_facts()
    risk_facts[key] = value
    assert type(value) is not bool
    _assert_blocked(_qualify(risk_facts=risk_facts), "PROMOTION_RISK_UNKNOWN")


def test_public_contract_matrix_closes_all_positive_and_fail_closed_cases() -> None:
    """Reprove the complete public qualification matrix through one bound node."""
    for positive_case in (
        test_contract_exports_and_keyword_only_qualification_surface,
        test_direct_green_qualifies_without_red_lock_or_effect_authority,
        test_expected_red_qualifies_only_with_bound_lock_and_exact_signature,
        test_green_closes_predecessor_red_lock_without_granting_effect,
        test_qualification_does_not_persist_or_authorize_effects,
    ):
        positive_case()

    test_green_at_expected_red_open_without_red_lock_blocks()
    test_worker_success_alone_never_qualifies_intent()
    test_instruction_digest_is_provenance_not_promotion_class()
    for reported_status in ("failure", "blocked"):
        test_valid_facts_qualify_independent_of_worker_reported_status(
            reported_status,
        )
    test_validator_has_no_promotion_class_and_no_go_blocks()
    for case in (
        ("missing", "PROMOTION_DECISION_MISSING"),
        ("draft", "PROMOTION_DECISION_NOT_READY"),
        ("invalid", "PROMOTION_DECISION_INVALID"),
    ):
        test_validator_state_negative_edges_with_ready_orchestrator(*case)
    test_red_tdd_class_against_green_expected_outcome_mismatches()
    test_blocked_orchestrator_class_never_qualifies()

    for case in (
        ("missing_current", "PROMOTION_TASK_SNAPSHOT_MISSING"),
        ("unknown_field", "PROMOTION_TASK_SNAPSHOT_UNKNOWN"),
        ("mismatch_digest", "PROMOTION_TASK_SNAPSHOT_MISMATCH"),
        ("contradiction", "PROMOTION_TASK_SNAPSHOT_CONTRADICTION"),
    ):
        test_task_snapshot_negative_edges(*case)
    for case in (
        ("missing_digest", "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING"),
        ("unknown_field", "PROMOTION_INSTRUCTION_SNAPSHOT_UNKNOWN"),
        ("mismatch_digest", "PROMOTION_INSTRUCTION_SNAPSHOT_MISMATCH"),
        ("contradiction", "PROMOTION_INSTRUCTION_SNAPSHOT_CONTRADICTION"),
    ):
        test_instruction_snapshot_negative_edges(*case)
    for case in (
        ("missing_head", "PROMOTION_RESULT_SHA_MISSING"),
        ("mismatch_head", "PROMOTION_RESULT_SHA_MISMATCH"),
        ("contradiction", "PROMOTION_RESULT_CONTRADICTION"),
        ("missing_attempt", "PROMOTION_ATTEMPT_MISSING"),
        ("mismatch_attempt", "PROMOTION_ATTEMPT_MISMATCH"),
    ):
        test_result_and_attempt_negative_edges(*case)
    for case in (
        ("missing", "PROMOTION_DECISION_MISSING"),
        ("unknown", "PROMOTION_DECISION_UNKNOWN"),
        ("draft", "PROMOTION_DECISION_NOT_READY"),
        ("invalid", "PROMOTION_DECISION_INVALID"),
        ("identity", "PROMOTION_DECISION_IDENTITY_MISMATCH"),
        ("contradiction", "PROMOTION_DECISION_CONTRADICTION"),
        ("evidence", "PROMOTION_EVIDENCE_MISSING"),
        ("outcome_missing", "PROMOTION_EXPECTED_OUTCOME_MISSING"),
        ("outcome_unknown", "PROMOTION_EXPECTED_OUTCOME_UNKNOWN"),
        ("outcome_mismatch", "PROMOTION_EXPECTED_OUTCOME_MISMATCH"),
    ):
        test_decision_evidence_and_expected_outcome_negative_edges(*case)
    for case in (
        ("missing", "PROMOTION_ALLOWED_PATHS_MISSING"),
        ("unknown", "PROMOTION_ALLOWED_PATHS_UNKNOWN"),
        ("mismatch", "PROMOTION_ALLOWED_PATHS_MISMATCH"),
        ("risk_missing", "PROMOTION_RISK_MISSING"),
        ("risk_unknown", "PROMOTION_RISK_UNKNOWN"),
        ("risk_contradiction", "PROMOTION_RISK_CONTRADICTION"),
        ("secret", "PROMOTION_SECRET_RISK"),
        ("delete", "PROMOTION_DELETE_RISK"),
        ("scope", "PROMOTION_SCOPE_RISK"),
    ):
        test_allowed_paths_and_risk_negative_edges(*case)
    for case in (
        ("missing", "PROMOTION_MAIN_STATE_MISSING"),
        ("unknown", "PROMOTION_MAIN_STATE_UNKNOWN"),
        ("mismatch", "PROMOTION_MAIN_STATE_MISMATCH"),
        ("lock_missing", "PROMOTION_LOCK_MISSING"),
        ("lock_mismatch", "PROMOTION_LOCK_MISMATCH"),
        ("preflight", "PROMOTION_GIT_PREFLIGHT_BLOCKED"),
        ("foreign_epoch", "PROMOTION_UNRESOLVED_FOREIGN_EPOCH"),
    ):
        test_main_state_lock_and_preflight_negative_edges(*case)
    for case in (
        ("already_consumed", "PROMOTION_ALREADY_CONSUMED"),
        ("conflict", "PROMOTION_CONSUMPTION_CONFLICT"),
        ("conflict_same_result", "PROMOTION_CONSUMPTION_CONFLICT"),
    ):
        test_consumption_negative_edges(*case)
    for case in (
        ("missing", "PROMOTION_RED_LOCK_MISSING"),
        ("unknown", "PROMOTION_RED_LOCK_UNKNOWN"),
        ("mismatch_signature", "PROMOTION_RED_LOCK_MISMATCH"),
        ("foreign_identity", "PROMOTION_FOREIGN_RED"),
        ("active_leftover", "PROMOTION_RED_LOCK_ACTIVE"),
        ("same_epoch_closure", "PROMOTION_FOREIGN_RED"),
    ):
        test_red_closure_negative_edges(*case)

    for case in (
        test_expected_red_without_exact_command_binding_blocks,
        test_task_snapshot_missing_bound_is_fail_closed,
        test_instruction_snapshot_missing_bound_is_fail_closed,
        test_empty_validator_evidence_is_fail_closed,
        test_expected_red_foreign_lock_identity_with_matching_signature_blocks,
        test_lock_owned_true_with_ok_false_is_mismatch,
        test_consumed_payload_inspect_name_is_builtin_str_fact_axis,
        test_instruction_snapshot_missing_current_is_fail_closed,
        test_validator_state_unknown_is_fail_closed,
    ):
        case()
    for overrides in (
        {
            "orchestrator_state": DecisionArtifactState.READY,
            "orchestrator_decision": None,
        },
        {
            "validator_state": DecisionArtifactState.READY,
            "validator_decision": None,
        },
    ):
        test_ready_state_with_none_decision_is_fail_closed(overrides)
    for overrides in (
        {
            "orchestrator_state": DecisionArtifactState.READY,
            "orchestrator_decision": _reference_orchestrator(ready=False),
        },
        {
            "validator_state": DecisionArtifactState.READY,
            "validator_decision": _reference_validator(ready=False),
        },
    ):
        test_ready_state_with_unready_decision_is_fail_closed(overrides)
    for case in (
        ("task", "current", "PROMOTION_TASK_SNAPSHOT_MISSING"),
        ("task", "bound", "PROMOTION_TASK_SNAPSHOT_MISSING"),
        ("instruction", "current", "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING"),
        ("instruction", "bound", "PROMOTION_INSTRUCTION_SNAPSHOT_MISSING"),
    ):
        test_snapshot_none_current_or_bound_is_fail_closed(*case)
    for axis in ("task_id", "origin_run_id", "promotion_attempt_id"):
        test_green_closure_blocks_when_predecessor_lock_axis_matches_current_epoch(
            axis,
        )
    for path in ("expected_red", "green_closure"):
        for field in (
            "schema_version",
            "expected_main_state_after_promotion",
            "closure_gate",
        ):
            for mode in ("wrong", "missing"):
                test_red_lock_incomplete_schema_state_or_gate_blocks(
                    path,
                    field,
                    mode,
                )
        for field in (
            "task_id",
            "origin_run_id",
            "promotion_attempt_id",
            "result_sha",
            "expected_failure_signature",
            "command_id",
        ):
            test_red_lock_missing_required_identity_result_or_declaration_blocks(
                path,
                field,
            )
    for field in ("expected_failure_signature", "command_id"):
        for value in ("", None):
            test_green_closure_empty_or_none_declaration_fields_block(
                field,
                value,
            )
    for field in ("task_id", "origin_run_id", "promotion_attempt_id"):
        for value in ("", None):
            test_green_closure_empty_or_none_predecessor_identity_fields_block(
                field,
                value,
            )
    for key, value in (
        ("secret_risk", None),
        ("delete_risk", 0),
        ("scope_risk", 1),
        ("secret_risk", ""),
        ("delete_risk", []),
    ):
        test_non_bool_risk_facts_fail_closed(key, value)
