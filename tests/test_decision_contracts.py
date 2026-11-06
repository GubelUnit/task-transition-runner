"""Synthetic public decision-contract trust boundary.

The public surface is intentionally small and explicit:

- ``DecisionRequest`` is deterministic handover data. It is not a semantic
  decision and not promotion authority.
- ``OrchestratorDecision`` and ``ValidatorDecision`` are the only semantic
  decision artifacts. They bind the same four identity axes.
- Authority stays asymmetric: only the orchestrator carries
  ``promotion_class``; only the validator carries ``GO`` or ``NO_GO``.
- ``classify_*`` reports artifact states ``missing``, ``draft``, ``ready``,
  and ``invalid``. Those states are not admission or promotion.
- Artifact-backed evidence is checked against an explicit artifact root.
  ``git_result.ref`` binds to ``result_sha`` and is not a Git proof.
- Closed schema rejects unknown, missing, typed, valued, and duplicate-key
  edges generically. Separate files do not authenticate producers.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import inspect
import os
import stat
from pathlib import Path
from typing import Any, Callable, Literal

import pytest
import yaml

from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    DecisionContractError,
    DecisionIdentity,
    classify_decision_request,
    classify_orchestrator_decision,
    classify_validator_decision,
    load_decision_request,
    load_orchestrator_decision,
    load_validator_decision,
)

Kind = Literal["request", "orchestrator", "validator"]

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_REQUEST_TYPE = "decision_request"
_REFERENCE_ORCHESTRATOR_TYPE = "orchestrator"
_REFERENCE_VALIDATOR_TYPE = "validator"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_FOREIGN_TASK_ID = "synthetic-task-002"
_FOREIGN_ORIGIN_RUN_ID = "origin-run-002"
_FOREIGN_PROMOTION_ATTEMPT_ID = "promotion-attempt-002"
_FOREIGN_RESULT_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_REFERENCE_LOG_REF = "runs/origin-run-001/validation/tests.log"
_VALIDATION_LOG_BODY = b"synthetic validation log\n"
_REFERENCE_LOG_SHA256 = hashlib.sha256(_VALIDATION_LOG_BODY).hexdigest()
_FOREIGN_LOG_SHA256 = hashlib.sha256(b"foreign validation log\n").hexdigest()
_ORCHESTRATOR_PATH = "orchestrator_decision.yaml"
_VALIDATOR_PATH = "validator_decision.yaml"
_REQUEST_FILENAME = "decision_request.yaml"

_REQUEST_REQUIRED = (
    "schema_version",
    "request_type",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "required_decisions",
    "evidence",
)
_ORCHESTRATOR_REQUIRED = (
    "schema_version",
    "decision_type",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "ready",
    "promotion_class",
    "evidence",
)
_VALIDATOR_REQUIRED = (
    "schema_version",
    "decision_type",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "ready",
    "verdict",
    "evidence",
)
_IDENTITY_AXES = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
)
_REQUIRED_EXPORTS = (
    "DecisionIdentity",
    "DecisionArtifactState",
    "DecisionContractError",
    "load_decision_request",
    "load_orchestrator_decision",
    "load_validator_decision",
    "classify_decision_request",
    "classify_orchestrator_decision",
    "classify_validator_decision",
)
_FORBIDDEN_EXPORTS = (
    "admit_promotion",
    "create_pending_promotion",
    "pending_promotion",
    "promote",
    "write_decision_request",
    "authenticate_producer",
    "quorum",
    "sandbox",
    "git_proof",
    "status_json",
    "runner_run",
    "consume_continuation",
    "recover",
)
_FOREIGN_IDENTITY = {
    "task_id": _FOREIGN_TASK_ID,
    "origin_run_id": _FOREIGN_ORIGIN_RUN_ID,
    "promotion_attempt_id": _FOREIGN_PROMOTION_ATTEMPT_ID,
    "result_sha": _FOREIGN_RESULT_SHA,
}


def _composed_token(*segments: str) -> str:
    return "".join(segments)


def _composed_absolute(*segments: str) -> str:
    return "/" + "/".join(segments)


def _reference_identity() -> DecisionIdentity:
    return DecisionIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=_REFERENCE_RESULT_SHA,
    )


def _validation_log_evidence() -> dict[str, str]:
    return {
        "kind": "validation_log",
        "ref": _REFERENCE_LOG_REF,
        "sha256": _REFERENCE_LOG_SHA256,
    }


def _git_result_evidence() -> dict[str, str]:
    return {
        "kind": "git_result",
        "ref": _REFERENCE_RESULT_SHA,
    }


def _required_decisions() -> list[dict[str, str]]:
    return [
        {"role": "orchestrator", "path": _ORCHESTRATOR_PATH},
        {"role": "validator", "path": _VALIDATOR_PATH},
    ]


def _request_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "request_type": _REFERENCE_REQUEST_TYPE,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "required_decisions": _required_decisions(),
        "evidence": [_validation_log_evidence()],
    }


def _orchestrator_payload(
    *,
    ready: bool = True,
    promotion_class: str = "GREEN_PROMOTION",
    evidence: list[dict[str, str]] | None = None,
    rationale: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "decision_type": _REFERENCE_ORCHESTRATOR_TYPE,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "ready": ready,
        "promotion_class": promotion_class,
        "evidence": (
            [_validation_log_evidence()] if evidence is None else copy.deepcopy(evidence)
        ),
    }
    if rationale is not None:
        payload["rationale"] = rationale
    return payload


def _validator_payload(
    *,
    ready: bool = True,
    verdict: str = "GO",
    evidence: list[dict[str, str]] | None = None,
    rationale: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "decision_type": _REFERENCE_VALIDATOR_TYPE,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "ready": ready,
        "verdict": verdict,
        "evidence": (
            [_validation_log_evidence()] if evidence is None else copy.deepcopy(evidence)
        ),
    }
    if rationale is not None:
        payload["rationale"] = rationale
    return payload


def _payload_for(kind: Kind) -> dict[str, Any]:
    if kind == "request":
        return _request_payload()
    if kind == "orchestrator":
        return _orchestrator_payload()
    return _validator_payload()


def _payload_with_git_result(kind: Kind) -> dict[str, Any]:
    payload = _payload_for(kind)
    payload["evidence"] = [_git_result_evidence()]
    return payload


def _filename_for(kind: Kind) -> str:
    if kind == "request":
        return _REQUEST_FILENAME
    if kind == "orchestrator":
        return _ORCHESTRATOR_PATH
    return _VALIDATOR_PATH


def _make_root(tmp_path: Path) -> Path:
    root = tmp_path / "artifact-root"
    log_path = root / _REFERENCE_LOG_REF
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_bytes(_VALIDATION_LOG_BODY)
    (
        root / "runs" / _REFERENCE_ORIGIN_RUN_ID / "decisions"
    ).mkdir(parents=True, exist_ok=True)
    return root


def _decisions_dir(root: Path) -> Path:
    return root / "runs" / _REFERENCE_ORIGIN_RUN_ID / "decisions"


def _write_document(
    root: Path,
    payload: dict[str, Any],
    *,
    filename: str,
) -> Path:
    source_path = _decisions_dir(root) / filename
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return source_path


def _write_kind(
    root: Path,
    kind: Kind,
    payload: dict[str, Any] | None = None,
) -> Path:
    document = _payload_for(kind) if payload is None else copy.deepcopy(payload)
    return _write_document(root, document, filename=_filename_for(kind))


def _overwrite_source(source_path: Path, text: str) -> Path:
    source_path.write_text(text, encoding="utf-8")
    return source_path


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip(
            "process retains read access after clearing read mode bits"
        )


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
        if key.isdigit():
            cursor = cursor[int(key)]
        else:
            cursor = cursor[key]
    last = keys[-1]
    if last.isdigit():
        cursor[int(last)] = value
    else:
        cursor[last] = value
    return clone


def _loader(kind: Kind) -> Callable[..., Any]:
    return {
        "request": load_decision_request,
        "orchestrator": load_orchestrator_decision,
        "validator": load_validator_decision,
    }[kind]


def _classifier(kind: Kind) -> Callable[..., Any]:
    return {
        "request": classify_decision_request,
        "orchestrator": classify_orchestrator_decision,
        "validator": classify_validator_decision,
    }[kind]


def _load(
    kind: Kind,
    source_path: Path,
    artifact_root: Path,
    *,
    expected_identity: DecisionIdentity | None = None,
) -> Any:
    identity = _reference_identity() if expected_identity is None else expected_identity
    return _loader(kind)(
        source_path,
        expected_identity=identity,
        artifact_root=artifact_root,
    )


def _classify(
    kind: Kind,
    source_path: Path,
    artifact_root: Path,
    *,
    expected_identity: DecisionIdentity | None = None,
) -> DecisionArtifactState:
    identity = _reference_identity() if expected_identity is None else expected_identity
    return _classifier(kind)(
        source_path,
        expected_identity=identity,
        artifact_root=artifact_root,
    )


def _expect_reason(
    kind: Kind,
    source_path: Path,
    artifact_root: Path,
    reason_code: str,
    *,
    expected_identity: DecisionIdentity | None = None,
) -> DecisionContractError:
    with pytest.raises(DecisionContractError, match=reason_code) as exc_info:
        _load(
            kind,
            source_path,
            artifact_root,
            expected_identity=expected_identity,
        )
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def test_public_exports_are_the_named_contract_surface() -> None:
    module = inspect.getmodule(load_decision_request)
    assert module is not None
    exported = {name for name in dir(module) if not name.startswith("_")}

    for name in _REQUIRED_EXPORTS:
        assert name in exported
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported
    assert "PendingPromotion" not in exported
    assert "Sandbox" not in exported
    assert "Quorum" not in exported


def test_identity_exposes_exactly_the_four_result_axes() -> None:
    fields = tuple(field.name for field in dataclasses.fields(DecisionIdentity))

    assert fields == _IDENTITY_AXES
    assert "promotion_class" not in fields
    assert "verdict" not in fields
    assert "run_id" not in fields
    assert "producer_id" not in fields


def test_artifact_states_are_contract_states_not_admission() -> None:
    values = {state.value for state in DecisionArtifactState}

    assert DecisionArtifactState.MISSING.value == "missing"
    assert DecisionArtifactState.DRAFT.value == "draft"
    assert DecisionArtifactState.READY.value == "ready"
    assert DecisionArtifactState.INVALID.value == "invalid"
    assert values == {"missing", "draft", "ready", "invalid"}
    assert "admitted" not in values
    assert "pending" not in values
    assert "promoted" not in values
    assert "tock" not in values


def test_loaders_and_classifiers_share_the_same_keyword_surface() -> None:
    functions = (
        load_decision_request,
        load_orchestrator_decision,
        load_validator_decision,
        classify_decision_request,
        classify_orchestrator_decision,
        classify_validator_decision,
    )
    for function in functions:
        parameters = inspect.signature(function).parameters
        assert tuple(parameters)[0] == "source_path"
        assert "expected_identity" in parameters
        assert "artifact_root" in parameters
        assert "git" not in parameters
        assert "pending_promotion" not in parameters
        assert "producer" not in parameters
        assert "sandbox" not in parameters
        assert "quorum" not in parameters


def test_reference_request_is_handover_not_authority(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, "request")
    loaded = _load("request", source_path, root)

    assert loaded.schema_version == _REFERENCE_SCHEMA_VERSION
    assert loaded.request_type == _REFERENCE_REQUEST_TYPE
    assert loaded.task_id == _REFERENCE_TASK_ID
    assert loaded.origin_run_id == _REFERENCE_ORIGIN_RUN_ID
    assert loaded.promotion_attempt_id == _REFERENCE_PROMOTION_ATTEMPT_ID
    assert loaded.result_sha == _REFERENCE_RESULT_SHA
    assert not hasattr(loaded, "ready")
    assert not hasattr(loaded, "promotion_class")
    assert not hasattr(loaded, "verdict")
    assert not hasattr(loaded, "pending_promotion")
    roles = tuple(item.role for item in loaded.required_decisions)
    paths = tuple(item.path for item in loaded.required_decisions)
    assert roles == ("orchestrator", "validator")
    assert paths == (_ORCHESTRATOR_PATH, _VALIDATOR_PATH)
    assert _classify("request", source_path, root) == DecisionArtifactState.READY


def test_reference_orchestrator_exposes_promotion_class_not_verdict(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, "orchestrator")
    loaded = _load("orchestrator", source_path, root)

    assert loaded.decision_type == _REFERENCE_ORCHESTRATOR_TYPE
    assert loaded.ready is True
    assert loaded.promotion_class == "GREEN_PROMOTION"
    assert not hasattr(loaded, "verdict")
    assert loaded.evidence[0].kind == "validation_log"
    assert loaded.evidence[0].ref == _REFERENCE_LOG_REF
    assert loaded.evidence[0].sha256 == _REFERENCE_LOG_SHA256
    assert _classify("orchestrator", source_path, root) == DecisionArtifactState.READY


def test_reference_validator_exposes_verdict_not_promotion_class(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, "validator")
    loaded = _load("validator", source_path, root)

    assert loaded.decision_type == _REFERENCE_VALIDATOR_TYPE
    assert loaded.ready is True
    assert loaded.verdict == "GO"
    assert not hasattr(loaded, "promotion_class")
    assert _classify("validator", source_path, root) == DecisionArtifactState.READY


@pytest.mark.parametrize("promotion_class", ("GREEN_PROMOTION", "RED_TDD_PROMOTION"))
def test_ready_orchestrator_classes_require_evidence(
    tmp_path: Path,
    promotion_class: str,
) -> None:
    root = _make_root(tmp_path)
    valid = _write_kind(
        root,
        "orchestrator",
        _orchestrator_payload(promotion_class=promotion_class),
    )
    loaded = _load("orchestrator", valid, root)
    assert loaded.promotion_class == promotion_class
    assert loaded.ready is True

    empty = _write_document(
        root,
        _orchestrator_payload(promotion_class=promotion_class, evidence=[]),
        filename="empty-orchestrator.yaml",
    )
    _expect_reason("orchestrator", empty, root, "DECISION_VALUE_CONFLICT")
    assert _classify("orchestrator", empty, root) == DecisionArtifactState.INVALID


def test_blocked_orchestrator_is_a_ready_blocking_contract_value(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(
        root,
        "orchestrator",
        _orchestrator_payload(promotion_class="BLOCKED", evidence=[]),
    )
    loaded = _load("orchestrator", source_path, root)

    assert loaded.promotion_class == "BLOCKED"
    assert loaded.ready is True
    assert loaded.evidence == ()
    assert _classify("orchestrator", source_path, root) == DecisionArtifactState.READY
    assert list(root.rglob("pending_promotion.yaml")) == []


def test_ready_go_requires_evidence_and_no_go_is_a_blocking_value(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    go_empty = _write_document(
        root,
        _validator_payload(verdict="GO", evidence=[]),
        filename="empty-go.yaml",
    )
    _expect_reason("validator", go_empty, root, "DECISION_VALUE_CONFLICT")

    no_go = _write_kind(
        root,
        "validator",
        _validator_payload(verdict="NO_GO", evidence=[]),
    )
    loaded = _load("validator", no_go, root)
    assert loaded.verdict == "NO_GO"
    assert loaded.ready is True
    assert _classify("validator", no_go, root) == DecisionArtifactState.READY
    assert list(root.rglob("pending_promotion.yaml")) == []


def test_request_missing_ready_and_invalid_states(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    missing = _decisions_dir(root) / _REQUEST_FILENAME
    assert _classify("request", missing, root) == DecisionArtifactState.MISSING
    _expect_reason("request", missing, root, "DECISION_SOURCE_MISSING")

    ready_path = _write_kind(root, "request")
    assert _classify("request", ready_path, root) == DecisionArtifactState.READY

    invalid = _write_document(
        root,
        _set_field(_request_payload(), "request_type", "promotion"),
        filename="invalid-request.yaml",
    )
    assert _classify("request", invalid, root) == DecisionArtifactState.INVALID
    _expect_reason("request", invalid, root, "DECISION_VALUE_CONFLICT")


@pytest.mark.parametrize("kind", ("orchestrator", "validator"))
def test_decision_missing_draft_ready_and_invalid_states(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    missing = _decisions_dir(root) / _filename_for(kind)
    assert _classify(kind, missing, root) == DecisionArtifactState.MISSING
    _expect_reason(kind, missing, root, "DECISION_SOURCE_MISSING")

    if kind == "orchestrator":
        draft_payload = _orchestrator_payload(ready=False, evidence=[])
        ready_payload = _orchestrator_payload()
        invalid_payload = _orchestrator_payload(promotion_class="ALLOWED")
    else:
        draft_payload = _validator_payload(ready=False, evidence=[])
        ready_payload = _validator_payload()
        invalid_payload = _validator_payload(verdict="PASS")

    draft_path = _write_kind(root, kind, draft_payload)
    loaded_draft = _load(kind, draft_path, root)
    assert loaded_draft.ready is False
    assert _classify(kind, draft_path, root) == DecisionArtifactState.DRAFT

    ready_path = _write_kind(root, kind, ready_payload)
    loaded_ready = _load(kind, ready_path, root)
    assert loaded_ready.ready is True
    assert _classify(kind, ready_path, root) == DecisionArtifactState.READY

    invalid_path = _write_document(root, invalid_payload, filename="invalid.yaml")
    assert _classify(kind, invalid_path, root) == DecisionArtifactState.INVALID
    _expect_reason(kind, invalid_path, root, "DECISION_VALUE_CONFLICT")


def test_request_has_no_draft_because_it_has_no_ready_field(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _request_payload()
    payload["ready"] = False
    source_path = _write_kind(root, "request", payload)

    _expect_reason("request", source_path, root, "DECISION_FIELD_UNKNOWN")
    assert _classify("request", source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_loaded_documents_are_immutable(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    loaded = _load(kind, _write_kind(root, kind), root)

    with pytest.raises(dataclasses.FrozenInstanceError):
        loaded.task_id = "mutated-task"  # type: ignore[misc]


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_identical_reload_is_stable(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    first = _load(kind, source_path, root)
    second = _load(kind, source_path, root)

    assert first == second
    assert dataclasses.asdict(first) == dataclasses.asdict(second)


def test_request_load_does_not_create_or_admit_decisions(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, "request")
    before = {path for path in root.rglob("*") if path.is_file()}

    loaded = _load("request", source_path, root)

    after = {path for path in root.rglob("*") if path.is_file()}
    assert after == before
    assert not (_decisions_dir(root) / _ORCHESTRATOR_PATH).exists()
    assert not (_decisions_dir(root) / _VALIDATOR_PATH).exists()
    assert not hasattr(loaded, "promotion_class")
    assert list(root.rglob("pending_promotion.yaml")) == []


def test_two_decision_files_are_roles_not_producer_independence(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    orchestrator = _load("orchestrator", _write_kind(root, "orchestrator"), root)
    validator = _load("validator", _write_kind(root, "validator"), root)

    assert orchestrator.task_id == validator.task_id
    assert orchestrator.origin_run_id == validator.origin_run_id
    assert orchestrator.promotion_attempt_id == validator.promotion_attempt_id
    assert orchestrator.result_sha == validator.result_sha
    assert not hasattr(orchestrator, "producer_id")
    assert not hasattr(validator, "producer_id")
    assert not hasattr(orchestrator, "account_id")
    assert not hasattr(validator, "session_id")
    assert not hasattr(orchestrator, "signature")
    assert not hasattr(validator, "attestation")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize("axis", _IDENTITY_AXES)
def test_expected_identity_axis_drift_is_blocked(
    tmp_path: Path,
    kind: Kind,
    axis: str,
) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    drifted = dataclasses.replace(
        _reference_identity(),
        **{axis: _FOREIGN_IDENTITY[axis]},
    )

    _expect_reason(
        kind,
        source_path,
        root,
        "DECISION_IDENTITY_MISMATCH",
        expected_identity=drifted,
    )
    assert (
        _classify(kind, source_path, root, expected_identity=drifted)
        == DecisionArtifactState.INVALID
    )


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize("axis", _IDENTITY_AXES)
def test_document_identity_axis_drift_is_blocked(
    tmp_path: Path,
    kind: Kind,
    axis: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _set_field(_payload_for(kind), axis, _FOREIGN_IDENTITY[axis])
    source_path = _write_kind(root, kind, payload)

    _expect_reason(kind, source_path, root, "DECISION_IDENTITY_MISMATCH")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize(
    ("kind", "field"),
    tuple(("request", field) for field in _REQUEST_REQUIRED)
    + tuple(("orchestrator", field) for field in _ORCHESTRATOR_REQUIRED)
    + tuple(("validator", field) for field in _VALIDATOR_REQUIRED),
)
def test_missing_required_field_is_blocked(
    tmp_path: Path,
    kind: Kind,
    field: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _drop_field(_payload_for(kind), field)
    source_path = _write_document(root, payload, filename=f"missing-{field}.yaml")
    _expect_reason(kind, source_path, root, "DECISION_FIELD_MISSING")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_generic_unknown_top_level_field_is_blocked(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    payload = _payload_for(kind)
    payload["unexpected_field"] = "unexpected"
    source_path = _write_kind(root, kind, payload)
    _expect_reason(kind, source_path, root, "DECISION_FIELD_UNKNOWN")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize(
    "field",
    (
        "producer_id",
        "account_id",
        "session_id",
        "prompt_digest",
        "sandbox",
        "quorum",
        "signature",
        "pki",
        "attestation",
        "pending_promotion",
    ),
)
def test_authority_and_producer_fields_are_unknown_closed_schema(
    tmp_path: Path,
    kind: Kind,
    field: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _payload_for(kind)
    payload[field] = "unexpected"
    source_path = _write_document(root, payload, filename=f"unknown-{field}.yaml")
    _expect_reason(kind, source_path, root, "DECISION_FIELD_UNKNOWN")


def test_validator_promotion_class_is_an_unknown_field(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _validator_payload()
    payload["promotion_class"] = "GREEN_PROMOTION"
    source_path = _write_kind(root, "validator", payload)
    _expect_reason("validator", source_path, root, "DECISION_FIELD_UNKNOWN")
    assert _classify("validator", source_path, root) == DecisionArtifactState.INVALID


def test_orchestrator_verdict_is_an_unknown_field(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["verdict"] = "GO"
    source_path = _write_kind(root, "orchestrator", payload)
    _expect_reason("orchestrator", source_path, root, "DECISION_FIELD_UNKNOWN")


@pytest.mark.parametrize("field", ("promotion_class", "verdict"))
def test_request_authority_fields_are_unknown(tmp_path: Path, field: str) -> None:
    root = _make_root(tmp_path)
    payload = _request_payload()
    payload[field] = "unexpected"
    source_path = _write_kind(root, "request", payload)
    _expect_reason("request", source_path, root, "DECISION_FIELD_UNKNOWN")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_composed_private_top_level_tokens_are_unknown_fields(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    payload = _payload_for(kind)
    payload[_composed_token("orp", "_", "handover")] = "unexpected"
    one = _write_document(root, payload, filename="composed-one.yaml")
    _expect_reason(kind, one, root, "DECISION_FIELD_UNKNOWN")

    payload_two = _payload_for(kind)
    payload_two[_composed_token("chron", "icle")] = "unexpected"
    two = _write_document(root, payload_two, filename="composed-two.yaml")
    _expect_reason(kind, two, root, "DECISION_FIELD_UNKNOWN")

    payload_three = _payload_for(kind)
    payload_three[_composed_token("Hy", "dra")] = "unexpected"
    three = _write_document(root, payload_three, filename="composed-three.yaml")
    _expect_reason(kind, three, root, "DECISION_FIELD_UNKNOWN")


@pytest.mark.parametrize(
    ("kind", "dotted_field", "value"),
    (
        ("request", "schema_version", 1),
        ("request", "request_type", ["decision_request"]),
        ("request", "task_id", 12),
        ("request", "origin_run_id", {"id": "origin-run-001"}),
        ("request", "promotion_attempt_id", False),
        ("request", "result_sha", 40),
        ("request", "required_decisions", {}),
        ("request", "evidence", {}),
        ("orchestrator", "ready", "true"),
        ("orchestrator", "ready", 1),
        ("orchestrator", "promotion_class", ["GREEN_PROMOTION"]),
        ("orchestrator", "evidence", "validation_log"),
        ("validator", "ready", "false"),
        ("validator", "ready", 0),
        ("validator", "verdict", True),
        ("validator", "evidence", {"kind": "git_result"}),
    ),
)
def test_wrong_field_type_is_blocked(
    tmp_path: Path,
    kind: Kind,
    dotted_field: str,
    value: object,
) -> None:
    root = _make_root(tmp_path)
    payload = _set_field(_payload_for(kind), dotted_field, value)
    source_path = _write_kind(root, kind, payload)
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


def test_boolean_ready_rejects_bool_subclass_integers(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["ready"] = True
    loaded = _load("orchestrator", _write_kind(root, "orchestrator", payload), root)
    assert loaded.ready is True
    assert type(loaded.ready) is bool


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize("version", ("2.0", "1.0.0"))
def test_wrong_schema_version_is_blocked(
    tmp_path: Path,
    kind: Kind,
    version: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _set_field(_payload_for(kind), "schema_version", version)
    source_path = _write_kind(root, kind, payload)
    _expect_reason(kind, source_path, root, "DECISION_VALUE_CONFLICT")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize(
    "promotion_class",
    ("GREEN", "ALLOWED", "GO", "NO_GO", "green_promotion", ""),
)
def test_unknown_promotion_class_is_blocked(
    tmp_path: Path,
    promotion_class: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload(promotion_class=promotion_class)
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


@pytest.mark.parametrize("verdict", ("YES", "PASS", "go", "BLOCKED", "GREEN_PROMOTION"))
def test_unknown_verdict_is_blocked(tmp_path: Path, verdict: str) -> None:
    root = _make_root(tmp_path)
    payload = _validator_payload(verdict=verdict)
    _expect_reason(
        "validator",
        _write_kind(root, "validator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


def test_cross_loading_wrong_decision_type_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    orchestrator_path = _write_kind(root, "orchestrator")
    validator_path = _write_kind(root, "validator")
    request_path = _write_kind(root, "request")

    _expect_reason("validator", orchestrator_path, root, "DECISION_VALUE_CONFLICT")
    _expect_reason("orchestrator", validator_path, root, "DECISION_VALUE_CONFLICT")
    _expect_reason("orchestrator", request_path, root, "DECISION_VALUE_CONFLICT")
    _expect_reason("request", orchestrator_path, root, "DECISION_VALUE_CONFLICT")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize("axis", _IDENTITY_AXES)
def test_blank_identity_string_is_blocked(
    tmp_path: Path,
    kind: Kind,
    axis: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _set_field(_payload_for(kind), axis, "   ")
    expected = dataclasses.replace(_reference_identity(), **{axis: "   "})
    _expect_reason(
        kind,
        _write_kind(root, kind, payload),
        root,
        "DECISION_VALUE_CONFLICT",
        expected_identity=expected,
    )


def test_malformed_result_sha_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _set_field(_orchestrator_payload(), "result_sha", "not-a-sha")
    expected = dataclasses.replace(_reference_identity(), result_sha="not-a-sha")
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
        expected_identity=expected,
    )


def test_required_decisions_accept_swapped_role_order(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    swapped = _request_payload()
    swapped["required_decisions"] = [
        {"role": "validator", "path": _VALIDATOR_PATH},
        {"role": "orchestrator", "path": _ORCHESTRATOR_PATH},
    ]
    source_path = _write_kind(root, "request", swapped)
    loaded = _load("request", source_path, root)
    roles = {item.role for item in loaded.required_decisions}
    paths = {item.path for item in loaded.required_decisions}

    assert roles == {"orchestrator", "validator"}
    assert paths == {_ORCHESTRATOR_PATH, _VALIDATOR_PATH}
    assert _classify("request", source_path, root) == DecisionArtifactState.READY


@pytest.mark.parametrize(
    ("required_decisions", "reason_code"),
    (
        (
            [{"role": "orchestrator", "path": _ORCHESTRATOR_PATH}],
            "DECISION_VALUE_CONFLICT",
        ),
        (
            [
                {"role": "orchestrator", "path": _ORCHESTRATOR_PATH},
                {"role": "validator", "path": _VALIDATOR_PATH},
                {"role": "developer", "path": "developer_decision.yaml"},
            ],
            "DECISION_VALUE_CONFLICT",
        ),
        (
            [
                {"role": "orchestrator", "path": _ORCHESTRATOR_PATH},
                {"role": "reviewer", "path": _VALIDATOR_PATH},
            ],
            "DECISION_VALUE_CONFLICT",
        ),
        (
            [
                {"role": "orchestrator", "path": "../orchestrator_decision.yaml"},
                {"role": "validator", "path": _VALIDATOR_PATH},
            ],
            "DECISION_PATH_TRAVERSAL",
        ),
        (
            [
                {"role": "orchestrator", "path": "decisions/orchestrator_decision.yaml"},
                {"role": "validator", "path": _VALIDATOR_PATH},
            ],
            "DECISION_VALUE_CONFLICT",
        ),
    ),
)
def test_required_decisions_reject_unknown_count_role_and_path(
    tmp_path: Path,
    required_decisions: list[dict[str, str]],
    reason_code: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _request_payload()
    payload["required_decisions"] = required_decisions
    source_path = _write_kind(root, "request", payload)
    _expect_reason("request", source_path, root, reason_code)
    assert _classify("request", source_path, root) == DecisionArtifactState.INVALID


def test_required_decisions_reject_exact_duplicate_pair(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _request_payload()
    payload["required_decisions"] = [
        {"role": "orchestrator", "path": _ORCHESTRATOR_PATH},
        {"role": "validator", "path": _VALIDATOR_PATH},
        {"role": "orchestrator", "path": _ORCHESTRATOR_PATH},
    ]
    source_path = _write_kind(root, "request", payload)
    _expect_reason("request", source_path, root, "DECISION_VALUE_CONFLICT")
    assert _classify("request", source_path, root) == DecisionArtifactState.INVALID


def test_required_decision_item_closed_schema(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    extra = _request_payload()
    extra["required_decisions"][0]["producer"] = "unexpected"
    _expect_reason(
        "request",
        _write_kind(root, "request", extra),
        root,
        "DECISION_FIELD_UNKNOWN",
    )

    missing = _request_payload()
    del missing["required_decisions"][1]["path"]
    _expect_reason(
        "request",
        _write_document(root, missing, filename="missing-path.yaml"),
        root,
        "DECISION_FIELD_MISSING",
    )


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_non_mapping_evidence_item_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    payload = _payload_for(kind)
    payload["evidence"] = ["validation_log"]
    source_path = _write_kind(root, kind, payload)
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


def test_non_mapping_required_decision_item_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _request_payload()
    payload["required_decisions"] = ["orchestrator"]
    source_path = _write_kind(root, "request", payload)
    _expect_reason("request", source_path, root, "DECISION_FIELD_TYPE")
    assert _classify("request", source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_nested_duplicate_evidence_key_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    text = source_path.read_text(encoding="utf-8")
    digest_line = f"sha256: {_REFERENCE_LOG_SHA256}"
    assert digest_line in text
    _overwrite_source(
        source_path,
        text.replace(digest_line, digest_line + "\n  " + digest_line, 1),
    )
    _expect_reason(kind, source_path, root, "DECISION_VALUE_CONFLICT")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


def test_nested_duplicate_required_decision_key_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, "request")
    text = source_path.read_text(encoding="utf-8")
    path_line = f"path: {_ORCHESTRATOR_PATH}"
    assert path_line in text
    _overwrite_source(
        source_path,
        text.replace(path_line, path_line + "\n  " + path_line, 1),
    )
    _expect_reason("request", source_path, root, "DECISION_VALUE_CONFLICT")
    assert _classify("request", source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("orchestrator", "validator"))
def test_rationale_is_optional_opaque_string(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    without = _load(kind, _write_kind(root, kind), root)
    assert getattr(without, "rationale", None) in {None, ""}

    if kind == "orchestrator":
        payload = _orchestrator_payload(rationale="opaque operator prose")
    else:
        payload = _validator_payload(rationale="opaque operator prose")
    with_text = _write_document(root, payload, filename="with-rationale.yaml")
    loaded = _load(kind, with_text, root)
    assert loaded.rationale == "opaque operator prose"
    assert loaded.task_id == _REFERENCE_TASK_ID
    if kind == "orchestrator":
        assert loaded.promotion_class == "GREEN_PROMOTION"
    else:
        assert loaded.verdict == "GO"

    wrong_type = _payload_for(kind)
    wrong_type["rationale"] = ["not", "opaque"]
    _expect_reason(
        kind,
        _write_document(root, wrong_type, filename="rationale-type.yaml"),
        root,
        "DECISION_FIELD_TYPE",
    )


@pytest.mark.parametrize("kind", ("orchestrator", "validator"))
@pytest.mark.parametrize("fault", ("identity", "schema", "evidence"))
def test_ready_false_faults_are_invalid_not_draft(
    tmp_path: Path,
    kind: Kind,
    fault: str,
) -> None:
    root = _make_root(tmp_path)
    if kind == "orchestrator":
        payload = _orchestrator_payload(ready=False, evidence=[])
    else:
        payload = _validator_payload(ready=False, evidence=[])
    expected_identity = _reference_identity()
    reason_code = "DECISION_FIELD_UNKNOWN"
    if fault == "identity":
        payload["task_id"] = _FOREIGN_TASK_ID
        reason_code = "DECISION_IDENTITY_MISMATCH"
    elif fault == "schema":
        payload["unexpected_field"] = "unexpected"
    else:
        payload["evidence"] = [_validation_log_evidence()]
        payload["evidence"][0]["sha256"] = _FOREIGN_LOG_SHA256
        reason_code = "DECISION_DIGEST_MISMATCH"

    source_path = _write_kind(root, kind, payload)
    _expect_reason(
        kind,
        source_path,
        root,
        reason_code,
        expected_identity=expected_identity,
    )
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID
    assert _classify(kind, source_path, root) != DecisionArtifactState.DRAFT


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_git_result_binds_ref_to_result_sha_without_git_or_path(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    decoy = root / _REFERENCE_RESULT_SHA
    decoy.write_bytes(b"not a git object")
    source_path = _write_kind(root, kind, _payload_with_git_result(kind))
    loaded = _load(kind, source_path, root)

    assert loaded.evidence[0].kind == "git_result"
    assert loaded.evidence[0].ref == _REFERENCE_RESULT_SHA
    assert getattr(loaded.evidence[0], "sha256", None) is None
    assert not (root / ".git").exists()
    assert _classify(kind, source_path, root) == DecisionArtifactState.READY


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_git_result_ref_mismatch_against_identity_is_blocked(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    payload = _payload_with_git_result(kind)
    payload["evidence"] = [{"kind": "git_result", "ref": _FOREIGN_RESULT_SHA}]
    source_path = _write_kind(root, kind, payload)
    _expect_reason(kind, source_path, root, "DECISION_VALUE_CONFLICT")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize("extra_field", ("sha256", "path"))
def test_git_result_rejects_kind_foreign_fields(
    tmp_path: Path,
    kind: Kind,
    extra_field: str,
) -> None:
    root = _make_root(tmp_path)
    extra = _git_result_evidence()
    extra[extra_field] = (
        _REFERENCE_LOG_SHA256 if extra_field == "sha256" else _REFERENCE_LOG_REF
    )
    payload = _payload_with_git_result(kind)
    payload["evidence"] = [extra]
    source_path = _write_document(
        root,
        payload,
        filename=f"git-{extra_field}.yaml",
    )
    _expect_reason(kind, source_path, root, "DECISION_FIELD_UNKNOWN")


def test_validation_log_rejects_kind_foreign_and_missing_fields(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    extra = _validation_log_evidence()
    extra["commit"] = _REFERENCE_RESULT_SHA
    _expect_reason(
        "orchestrator",
        _write_document(
            root,
            _orchestrator_payload(evidence=[extra]),
            filename="log-extra.yaml",
        ),
        root,
        "DECISION_FIELD_UNKNOWN",
    )

    missing = {"kind": "validation_log", "ref": _REFERENCE_LOG_REF}
    _expect_reason(
        "orchestrator",
        _write_document(
            root,
            _orchestrator_payload(evidence=[missing]),
            filename="log-missing.yaml",
        ),
        root,
        "DECISION_FIELD_MISSING",
    )


def test_unknown_evidence_kind_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload(
        evidence=[{"kind": "worker_result", "ref": "worker_result.yaml"}],
    )
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


def test_validator_accepts_git_result_and_validation_log_together(
    tmp_path: Path,
) -> None:
    root = _make_root(tmp_path)
    payload = _validator_payload(
        evidence=[_git_result_evidence(), _validation_log_evidence()],
    )
    loaded = _load("validator", _write_kind(root, "validator", payload), root)
    kinds = tuple(item.kind for item in loaded.evidence)
    assert kinds == ("git_result", "validation_log")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_missing_evidence_artifact_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    (root / _REFERENCE_LOG_REF).unlink()
    source_path = _write_kind(root, kind)
    _expect_reason(kind, source_path, root, "DECISION_ARTIFACT_MISSING")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_evidence_byte_drift_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    (root / _REFERENCE_LOG_REF).write_bytes(b"tampered validation log\n")
    source_path = _write_kind(root, kind)
    _expect_reason(kind, source_path, root, "DECISION_DIGEST_MISMATCH")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_evidence_declared_digest_drift_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    payload = _payload_for(kind)
    payload["evidence"][0]["sha256"] = _FOREIGN_LOG_SHA256
    source_path = _write_kind(root, kind, payload)
    _expect_reason(kind, source_path, root, "DECISION_DIGEST_MISMATCH")


def test_uppercase_evidence_digest_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["evidence"][0]["sha256"] = _REFERENCE_LOG_SHA256.upper()
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


@pytest.mark.parametrize(
    "relative",
    (
        "../outside.log",
        "runs/./origin-run-001/validation/tests.log",
        "./runs/origin-run-001/validation/tests.log",
        "runs//origin-run-001/validation/tests.log",
        "runs/origin-run-001/validation/../validation/tests.log",
    ),
)
def test_evidence_ref_traversal_and_normalization_drift_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["evidence"][0]["ref"] = relative
    source_path = _write_kind(root, "orchestrator", payload)
    _expect_reason("orchestrator", source_path, root, "DECISION_PATH_TRAVERSAL")
    assert _classify("orchestrator", source_path, root) == DecisionArtifactState.INVALID


def test_absolute_evidence_ref_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["evidence"][0]["ref"] = str((root / _REFERENCE_LOG_REF).resolve())
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


def test_composed_home_class_absolute_evidence_ref_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["evidence"][0]["ref"] = _composed_absolute(
        "home",
        "synthetic-operator",
        "tests.log",
    )
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


def test_composed_users_class_absolute_evidence_ref_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["evidence"][0]["ref"] = _composed_absolute(
        "Users",
        "synthetic-operator",
        "tests.log",
    )
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


def test_evidence_symlink_escape_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    leaked = tmp_path / "outside.log"
    leaked.write_bytes(_VALIDATION_LOG_BODY)
    evidence = root / _REFERENCE_LOG_REF
    evidence.unlink()
    evidence.symlink_to(leaked)
    source_path = _write_kind(root, "orchestrator")
    _expect_reason("orchestrator", source_path, root, "DECISION_PATH_ESCAPE")
    assert _classify("orchestrator", source_path, root) == DecisionArtifactState.INVALID


def test_evidence_relative_parent_symlink_escape_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    leaked = tmp_path / "outside-parent.log"
    leaked.write_bytes(_VALIDATION_LOG_BODY)
    evidence = root / _REFERENCE_LOG_REF
    evidence.unlink()
    evidence.symlink_to(Path(os.path.relpath(leaked, start=evidence.parent)))
    source_path = _write_kind(root, "orchestrator")
    _expect_reason("orchestrator", source_path, root, "DECISION_PATH_ESCAPE")


def test_broken_evidence_symlink_is_blocked(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    evidence = root / _REFERENCE_LOG_REF
    evidence.unlink()
    evidence.symlink_to(tmp_path / "missing-evidence.log")
    source_path = _write_kind(root, "orchestrator")
    _expect_reason("orchestrator", source_path, root, "DECISION_PATH_ESCAPE")


def test_evidence_symlink_inside_artifact_root_is_accepted(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    canonical = root / "runs" / _REFERENCE_ORIGIN_RUN_ID / "validation" / "canonical.log"
    canonical.write_bytes(_VALIDATION_LOG_BODY)
    evidence = root / _REFERENCE_LOG_REF
    evidence.unlink()
    evidence.symlink_to(canonical)
    loaded = _load("orchestrator", _write_kind(root, "orchestrator"), root)
    assert loaded.evidence[0].ref == _REFERENCE_LOG_REF
    assert loaded.evidence[0].sha256 == _REFERENCE_LOG_SHA256


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_source_symlink_escape_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    leaked = tmp_path / f"escaped-{kind}.yaml"
    leaked.write_text(
        yaml.safe_dump(_payload_for(kind), sort_keys=False),
        encoding="utf-8",
    )
    source_path = _decisions_dir(root) / _filename_for(kind)
    source_path.symlink_to(leaked)
    _expect_reason(kind, source_path, root, "DECISION_PATH_ESCAPE")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_loader_rejects_source_outside_artifact_root(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    outside = tmp_path / f"outside-{kind}.yaml"
    outside.write_text(
        yaml.safe_dump(_payload_for(kind), sort_keys=False),
        encoding="utf-8",
    )
    _expect_reason(kind, outside, root, "DECISION_PATH_ESCAPE")
    assert _classify(kind, outside, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize(
    "relative",
    (".", "./", "runs/", "runs/*", "runs/**/*.log", "*"),
)
def test_root_glob_and_directory_evidence_refs_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    root = _make_root(tmp_path)
    payload = _orchestrator_payload()
    payload["evidence"][0]["ref"] = relative
    _expect_reason(
        "orchestrator",
        _write_kind(root, "orchestrator", payload),
        root,
        "DECISION_VALUE_CONFLICT",
    )


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_duplicate_yaml_keys_are_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    text = source_path.read_text(encoding="utf-8")
    _overwrite_source(source_path, 'schema_version: "1.0"\n' + text)
    _expect_reason(kind, source_path, root, "DECISION_VALUE_CONFLICT")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_non_utf8_source_bytes_are_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _decisions_dir(root) / _filename_for(kind)
    source_path.write_bytes(b"\xff")
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_unhashable_yaml_mapping_key_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    text = source_path.read_text(encoding="utf-8")
    _overwrite_source(source_path, "? []\n: 0\n" + text)
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")
    assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_empty_non_mapping_and_syntax_errors_are_blocked(
    tmp_path: Path,
    kind: Kind,
) -> None:
    root = _make_root(tmp_path)
    source_path = _decisions_dir(root) / _filename_for(kind)
    source_path.write_text("[]\n", encoding="utf-8")
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")
    source_path.write_text("not a mapping\n", encoding="utf-8")
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")
    source_path.write_text("schema_version: [unterminated\n", encoding="utf-8")
    _expect_reason(kind, source_path, root, "DECISION_FIELD_TYPE")


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_directory_source_is_missing(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    source_path.unlink()
    source_path.mkdir()
    _expect_reason(kind, source_path, root, "DECISION_SOURCE_MISSING")
    assert _classify(kind, source_path, root) == DecisionArtifactState.MISSING


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_unreadable_evidence_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    evidence = root / _REFERENCE_LOG_REF
    original_mode = evidence.stat().st_mode
    source_path = _write_kind(root, kind)
    try:
        _make_unreadable(evidence)
        _expect_reason(kind, source_path, root, "DECISION_SOURCE_UNREADABLE")
        assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID
    finally:
        if evidence.is_file():
            evidence.chmod(original_mode)


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
@pytest.mark.parametrize("fault", ("missing", "file"))
def test_invalid_artifact_root_is_blocked(
    tmp_path: Path,
    kind: Kind,
    fault: str,
) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    if fault == "missing":
        artifact_root = tmp_path / "absent-root"
    else:
        artifact_root = tmp_path / "root-file"
        artifact_root.write_text("not-a-directory\n", encoding="utf-8")
    _expect_reason(kind, source_path, artifact_root, "DECISION_PATH_ESCAPE")
    assert (
        _classify(kind, source_path, artifact_root) == DecisionArtifactState.INVALID
    )


@pytest.mark.parametrize("kind", ("request", "orchestrator", "validator"))
def test_unreadable_source_is_blocked(tmp_path: Path, kind: Kind) -> None:
    root = _make_root(tmp_path)
    source_path = _write_kind(root, kind)
    original_mode = source_path.stat().st_mode
    try:
        _make_unreadable(source_path)
        _expect_reason(kind, source_path, root, "DECISION_SOURCE_UNREADABLE")
        assert _classify(kind, source_path, root) == DecisionArtifactState.INVALID
    finally:
        if source_path.is_file():
            source_path.chmod(original_mode)


def test_error_exposes_stable_reason_code(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    error = _expect_reason(
        "request",
        _decisions_dir(root) / "absent.yaml",
        root,
        "DECISION_SOURCE_MISSING",
    )
    assert error.reason_code == "DECISION_SOURCE_MISSING"
    assert str(error) == "DECISION_SOURCE_MISSING"


def test_identities_stay_uncollapsed_on_loaded_artifacts(tmp_path: Path) -> None:
    root = _make_root(tmp_path)
    loaded = _load("request", _write_kind(root, "request"), root)
    values = {
        loaded.task_id,
        loaded.origin_run_id,
        loaded.promotion_attempt_id,
        loaded.result_sha,
    }

    assert len(values) == 4
    assert loaded.task_id != loaded.origin_run_id
    assert loaded.origin_run_id != loaded.promotion_attempt_id
    assert loaded.promotion_attempt_id != loaded.result_sha
    assert not hasattr(loaded, "run_id")
