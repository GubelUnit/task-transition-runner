"""Synthetic public ordinary continuation-contract.

Keep:
- common typed decode/load, identity, immutable binding, digest
- origin ``FINALIZED_PROMOTION_TICK`` only
- monotone PRE_ACK/CONSUMED observation
- identical terminal reproof of the same origin evidence

Drop:
- ``REOPEN_BASELINE_START_TICK``
- ``CONTEXT_BOUND_LEGACY`` and snapshot-binding types
- private ack, runtime-path, consumption-progress, git-proof, recovery
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import inspect
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.runner_continuation_contract as contract
from task_transition_runner.runner_continuation_contract import (
    ContinuationContractView,
    ContinuationError,
    ContinuationIdentity,
    ContinuationOrigin,
    LifecycleObservation,
    LifecyclePhase,
    assert_lifecycle_transition,
    assert_origin_reproof_matches,
    binding_digest,
    decode_continuation_artifact,
    load_continuation_artifact,
    observe_lifecycle,
)

_REFERENCE_SCHEMA_VERSION = "2.0"
_REFERENCE_ORIGIN_KIND = "FINALIZED_PROMOTION_TICK"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_REFERENCE_PROMOTION_CLASS = "GREEN_PROMOTION"
_REFERENCE_TOCK_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_REFERENCE_TICK_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_REFERENCE_SUCCESSOR_TASK_ID = "synthetic-successor-001"
_REFERENCE_PREPARED_AT = "2026-09-18T14:06:33Z"
_REFERENCE_CONSUMED_AT = "2026-09-18T15:21:07Z"
_REFERENCE_PREPARED_AT_SHIFTED = "2026-09-18T16:40:00Z"
_REFERENCE_SUCCESSOR_ATTEMPT_ID = "successor-attempt-001"
_REFERENCE_SUCCESSOR_TOCK_SHA = "dddddddddddddddddddddddddddddddddddddddd"
_FOREIGN_TASK_ID = "synthetic-task-002"
_FOREIGN_TOCK_SHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
_FOREIGN_TICK_SHA = "ffffffffffffffffffffffffffffffffffffffff"
_STATUS_AUTHORIZED = "AUTHORIZED"
_STATUS_CONSUMED = "CONSUMED"

_COMMON_ARTIFACT_FIELDS = (
    "schema_version",
    "origin",
    "original_tick_sha",
    "expected_continuation_head_sha",
    "commit_chain",
    "successor_task_id",
    "status",
    "prepared_at_utc",
    "continuation_digest_sha256",
)
_CONSUMPTION_FIELDS = (
    "successor_promotion_attempt_id",
    "successor_tock_sha",
    "consumed_at_utc",
)
_IDENTITY_FIELDS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
)
_BINDING_FIELDS = (
    "schema_version",
    "origin",
    "original_tick_sha",
    "expected_continuation_head_sha",
    "commit_chain",
    "successor_task_id",
)
_REQUIRED_EXPORTS = (
    "ContinuationError",
    "ContinuationOrigin",
    "LifecyclePhase",
    "ContinuationIdentity",
    "ContinuationContractView",
    "LifecycleObservation",
    "decode_continuation_artifact",
    "load_continuation_artifact",
    "observe_lifecycle",
    "assert_lifecycle_transition",
    "assert_origin_reproof_matches",
    "binding_digest",
)
_FORBIDDEN_OBSERVATION_SLOTS = (
    "issuance_witnesses",
    "issuance_witnesses_match",
    "snapshot_binding",
    "snapshot_context_digest_sha256",
    "current_ack_digests_sha256",
    "current_snapshot_digest_sha256",
)
_FORBIDDEN_EXPORTS = (
    "ReplaySnapshotBinding",
    "IssuanceWitnesses",
    "load_runtime_lifecycle_observation",
    "assert_reopen_consumption_progress",
    "load_optional_pending",
    "runtime_path",
    "file_sha256",
    "load_mapping",
    "create_decision",
    "load_decision",
    "write_decision_request",
    "create_pending_promotion",
    "admit_promotion",
    "promote",
    "prepare_continuation",
    "consume_continuation",
    "dispatch_continuation",
    "recover_continuation",
    "first_parent",
    "git_proof",
    "Decision",
    "PendingPromotion",
)


def _composed_token(*segments: str) -> str:
    return "".join(segments)


def _composed_absolute(*segments: str) -> str:
    return "/" + "/".join(segments)


def _reference_identity() -> dict[str, str]:
    return {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "promotion_class": _REFERENCE_PROMOTION_CLASS,
    }


def _commit_chain(
    *,
    sha: str = _REFERENCE_TICK_SHA,
    parent_sha: str = _REFERENCE_TOCK_SHA,
) -> list[dict[str, str]]:
    return [{"sha": sha, "parent_sha": parent_sha}]


def _binding(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": payload["schema_version"],
        "origin": copy.deepcopy(payload["origin"]),
        "original_tick_sha": payload["original_tick_sha"],
        "expected_continuation_head_sha": payload["expected_continuation_head_sha"],
        "commit_chain": copy.deepcopy(payload["commit_chain"]),
        "successor_task_id": payload["successor_task_id"],
    }


def _refresh_digest(payload: dict[str, Any]) -> None:
    payload["continuation_digest_sha256"] = binding_digest(_binding(payload))


def _finalized_payload(*, consumed: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "origin": {
            "kind": _REFERENCE_ORIGIN_KIND,
            "evidence": {
                "predecessor_identity": _reference_identity(),
                "tock_sha": _REFERENCE_TOCK_SHA,
            },
        },
        "original_tick_sha": _REFERENCE_TICK_SHA,
        "expected_continuation_head_sha": _REFERENCE_TICK_SHA,
        "commit_chain": _commit_chain(),
        "successor_task_id": _REFERENCE_SUCCESSOR_TASK_ID,
        "status": _STATUS_CONSUMED if consumed else _STATUS_AUTHORIZED,
        "prepared_at_utc": _REFERENCE_PREPARED_AT,
    }
    if consumed:
        payload.update(
            {
                "successor_promotion_attempt_id": _REFERENCE_SUCCESSOR_ATTEMPT_ID,
                "successor_tock_sha": _REFERENCE_SUCCESSOR_TOCK_SHA,
                "consumed_at_utc": _REFERENCE_CONSUMED_AT,
            }
        )
    _refresh_digest(payload)
    return payload


def _write_artifact(
    root: Path,
    payload: dict[str, Any] | None = None,
    *,
    filename: str = "post_tick_continuation.yaml",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    document = _finalized_payload() if payload is None else copy.deepcopy(payload)
    source_path = root / filename
    source_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
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


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip(
            "process retains read access after clearing read mode bits"
        )


def _expect_decode(payload: dict[str, Any], reason_code: str) -> ContinuationError:
    with pytest.raises(ContinuationError, match=reason_code) as exc_info:
        decode_continuation_artifact(payload)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _expect_load(source_path: Path, reason_code: str) -> ContinuationError:
    with pytest.raises(ContinuationError, match=reason_code) as exc_info:
        load_continuation_artifact(source_path)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _observe(view: ContinuationContractView) -> LifecycleObservation:
    return observe_lifecycle(view)


def _assert_observation_has_no_private_slots(observation: LifecycleObservation) -> None:
    for slot in _FORBIDDEN_OBSERVATION_SLOTS:
        assert not hasattr(observation, slot)


def test_finalized_artifact_decodes_to_read_only_typed_views_without_rewrite(
    tmp_path: Path,
) -> None:
    payload = _finalized_payload()
    artifact = _write_artifact(tmp_path, payload)
    before = artifact.read_bytes()

    view = load_continuation_artifact(artifact)

    assert artifact.read_bytes() == before
    assert view.artifact_sha256 == hashlib.sha256(before).hexdigest()
    assert view.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert view.origin.value == _REFERENCE_ORIGIN_KIND
    assert view.status == _STATUS_AUTHORIZED
    assert view.successor_task_id == _REFERENCE_SUCCESSOR_TASK_ID
    assert view.original_tick_sha == _REFERENCE_TICK_SHA
    assert view.expected_continuation_head_sha == _REFERENCE_TICK_SHA
    assert view.identity.task_id == _REFERENCE_TASK_ID
    assert view.identity.to_mapping() == _reference_identity()
    assert not hasattr(view, "issuance_witnesses")
    assert view.immutable_binding["origin"]["kind"] == view.origin.value
    assert tuple(view.immutable_binding) == _BINDING_FIELDS
    assert view.continuation_digest_sha256 == binding_digest(view.immutable_binding)
    assert view.to_mapping() == payload
    with pytest.raises(TypeError):
        view.immutable_binding["successor_task_id"] = _FOREIGN_TASK_ID  # type: ignore[index]


def test_finalized_identity_and_view_are_frozen(tmp_path: Path) -> None:
    view = load_continuation_artifact(_write_artifact(tmp_path))

    with pytest.raises(dataclasses.FrozenInstanceError):
        view.identity.task_id = _FOREIGN_TASK_ID  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        view.status = _STATUS_CONSUMED  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        view.successor_task_id = _FOREIGN_TASK_ID  # type: ignore[misc]


def test_identical_terminal_reproof_is_stable(tmp_path: Path) -> None:
    payload = _finalized_payload()
    artifact = _write_artifact(tmp_path, payload)
    first = load_continuation_artifact(artifact)
    second = load_continuation_artifact(artifact)
    decoded = decode_continuation_artifact(payload)

    assert first == second
    assert first.identity == decoded.identity
    assert first.continuation_digest_sha256 == decoded.continuation_digest_sha256
    assert_origin_reproof_matches(
        view=first,
        current_evidence=copy.deepcopy(payload["origin"]["evidence"]),
    )
    assert_origin_reproof_matches(
        view=decoded,
        current_evidence=decoded.to_mapping()["origin"]["evidence"],
    )


def test_consumed_status_is_monotone_observation_not_whole_file_immutability() -> None:
    authorized = decode_continuation_artifact(_finalized_payload())
    consumed_payload = _finalized_payload(consumed=True)
    consumed = decode_continuation_artifact(consumed_payload)

    assert authorized.identity == consumed.identity
    assert authorized.continuation_digest_sha256 == consumed.continuation_digest_sha256
    assert _binding(authorized.to_mapping()) == _binding(consumed.to_mapping())
    assert authorized.status == _STATUS_AUTHORIZED
    assert consumed.status == _STATUS_CONSUMED
    assert consumed.to_mapping()["successor_promotion_attempt_id"] == (
        _REFERENCE_SUCCESSOR_ATTEMPT_ID
    )
    authorized_observation = _observe(authorized)
    consumed_observation = _observe(consumed)
    assert authorized_observation.phase is LifecyclePhase.PRE_ACK
    assert consumed_observation.phase is LifecyclePhase.CONSUMED
    assert authorized_observation.identity == authorized.identity
    assert consumed_observation.identity == consumed.identity
    _assert_observation_has_no_private_slots(authorized_observation)
    _assert_observation_has_no_private_slots(consumed_observation)


def test_prepared_at_may_change_without_digest_refresh() -> None:
    payload = _finalized_payload()
    original_digest = payload["continuation_digest_sha256"]
    payload["prepared_at_utc"] = _REFERENCE_PREPARED_AT_SHIFTED
    view = decode_continuation_artifact(payload)

    assert view.continuation_digest_sha256 == original_digest
    assert view.identity.to_mapping() == _reference_identity()
    assert view.to_mapping()["prepared_at_utc"] == _REFERENCE_PREPARED_AT_SHIFTED


@pytest.mark.parametrize(
    ("previous", "current"),
    (
        (LifecyclePhase.PRE_ACK, LifecyclePhase.PRE_ACK),
        (LifecyclePhase.PRE_ACK, LifecyclePhase.CONSUMED),
        (LifecyclePhase.CONSUMED, LifecyclePhase.CONSUMED),
    ),
)
def test_finalized_lifecycle_allows_idempotent_and_monotone_neighbors(
    previous: LifecyclePhase,
    current: LifecyclePhase,
) -> None:
    assert_lifecycle_transition(
        origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        previous=previous,
        current=current,
    )


def test_finalized_lifecycle_rejects_regression_and_unknown_phases() -> None:
    with pytest.raises(
        ContinuationError,
        match="CONTINUATION_LIFECYCLE_INVALID",
    ) as exc_info:
        assert_lifecycle_transition(
            origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
            previous=LifecyclePhase.CONSUMED,
            current=LifecyclePhase.PRE_ACK,
        )
    assert exc_info.value.reason_code == "CONTINUATION_LIFECYCLE_INVALID"
    with pytest.raises(ValueError):
        LifecyclePhase("ACK_READY")
    with pytest.raises(ValueError):
        LifecyclePhase("PENDING")
    assert {member.name for member in LifecyclePhase} == {"PRE_ACK", "CONSUMED"}


def test_public_origin_admits_only_finalized_promotion_tick() -> None:
    assert {member.name for member in ContinuationOrigin} == {
        "FINALIZED_PROMOTION_TICK"
    }
    assert ContinuationOrigin.FINALIZED_PROMOTION_TICK.value == _REFERENCE_ORIGIN_KIND
    with pytest.raises(ValueError):
        ContinuationOrigin("REOPEN_BASELINE_START_TICK")
    with pytest.raises(ValueError):
        ContinuationOrigin("CONTEXT_BOUND_LEGACY")
    with pytest.raises(ValueError):
        ContinuationOrigin("THIRD_ORIGIN")


def test_public_surface_keeps_common_typed_functions_and_drops_excluded_origins() -> None:
    exported = {name for name in dir(contract) if not name.startswith("_")}
    for name in _REQUIRED_EXPORTS:
        assert name in exported
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported
    origin_names = tuple(member.name for member in ContinuationOrigin)
    assert "REOPEN_BASELINE_START_TICK" not in origin_names
    assert "CONTEXT_BOUND_LEGACY" not in origin_names
    phase_names = tuple(member.name for member in LifecyclePhase)
    assert "ACK_READY" not in phase_names
    assert "PENDING" not in phase_names
    parameters = inspect.signature(load_continuation_artifact).parameters
    assert tuple(parameters)[0] in {"path", "artifact_path", "source_path"}
    assert "runtime_root" not in parameters
    observation_parameters = inspect.signature(observe_lifecycle).parameters
    assert "view" in observation_parameters
    assert "runtime_root" not in observation_parameters
    assert "ack_drafts" not in parameters
    assert "queue" not in parameters
    assert "planner" not in parameters
    assert "decision" not in parameters
    identity_fields = tuple(
        field.name for field in dataclasses.fields(ContinuationIdentity)
    )
    assert identity_fields == _IDENTITY_FIELDS
    assert "run_id" not in identity_fields
    assert "drain_run_id" not in identity_fields
    observation_fields = tuple(
        field.name for field in dataclasses.fields(LifecycleObservation)
    )
    for slot in _FORBIDDEN_OBSERVATION_SLOTS:
        assert slot not in observation_fields


def test_loader_does_not_write_decision_git_or_consumption_artifacts(
    tmp_path: Path,
) -> None:
    source_path = _write_artifact(tmp_path)
    before = {path for path in tmp_path.rglob("*") if path.is_file()}

    view = load_continuation_artifact(source_path)
    observation = _observe(view)

    after = {path for path in tmp_path.rglob("*") if path.is_file()}
    assert after == before
    assert observation.phase is LifecyclePhase.PRE_ACK
    _assert_observation_has_no_private_slots(observation)
    for name in (
        "decision_request.yaml",
        "pending_promotion.yaml",
        "promotion_consumed.yaml",
    ):
        assert list(tmp_path.rglob(name)) == []
    for forbidden in (
        "decision",
        "pending_promotion",
        "run_id",
        "git_proof",
        "recovery",
    ):
        assert not hasattr(view, forbidden)
        assert not hasattr(observation, forbidden)
    assert not hasattr(view, "issuance_witnesses")


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("schema", "CONTINUATION_ARTIFACT_INVALID"),
        ("shape", "CONTINUATION_ARTIFACT_INVALID"),
        ("status", "CONTINUATION_ARTIFACT_INVALID"),
        ("origin", "CONTINUATION_ORIGIN_INVALID"),
        ("reopen", "CONTINUATION_ORIGIN_INVALID"),
        ("legacy", "CONTINUATION_ORIGIN_INVALID"),
        ("digest", "CONTINUATION_DIGEST_MISMATCH"),
        ("identity", "CONTINUATION_IDENTITY_MISMATCH"),
        ("evidence", "CONTINUATION_ARTIFACT_INVALID"),
        ("chain", "CONTINUATION_CHAIN_MISMATCH"),
    ),
)
def test_codec_rejects_schema_shape_origin_digest_identity_and_chain_drift(
    mutation: str,
    reason_code: str,
) -> None:
    candidate = _finalized_payload()
    if mutation == "schema":
        candidate["schema_version"] = "1.0"
        _refresh_digest(candidate)
    elif mutation == "shape":
        candidate["foreign"] = True
    elif mutation == "status":
        candidate["status"] = "PENDING"
    elif mutation == "origin":
        candidate["origin"]["kind"] = "THIRD_ORIGIN"
        _refresh_digest(candidate)
    elif mutation == "reopen":
        candidate["origin"]["kind"] = "REOPEN_BASELINE_START_TICK"
        _refresh_digest(candidate)
    elif mutation == "legacy":
        candidate["origin"]["kind"] = "CONTEXT_BOUND_LEGACY"
        _refresh_digest(candidate)
    elif mutation == "digest":
        candidate["continuation_digest_sha256"] = "0" * 64
    elif mutation == "identity":
        candidate["origin"]["evidence"]["predecessor_identity"]["task_id"] = ""
        _refresh_digest(candidate)
    elif mutation == "evidence":
        candidate["origin"]["evidence"]["reopen_id"] = "unexpected"
        _refresh_digest(candidate)
    else:
        candidate["commit_chain"][0]["parent_sha"] = _FOREIGN_TOCK_SHA
        _refresh_digest(candidate)
    _expect_decode(candidate, reason_code)


@pytest.mark.parametrize("dotted", _COMMON_ARTIFACT_FIELDS)
def test_missing_common_field_is_blocked(dotted: str) -> None:
    payload = _drop_field(_finalized_payload(), dotted)
    if dotted != "continuation_digest_sha256":
        if dotted in _BINDING_FIELDS or dotted.startswith("origin"):
            try:
                _refresh_digest(payload)
            except KeyError:
                pass
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")


@pytest.mark.parametrize("dotted", _CONSUMPTION_FIELDS)
def test_missing_consumed_field_is_blocked(dotted: str) -> None:
    payload = _drop_field(_finalized_payload(consumed=True), dotted)
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")


def test_authorized_artifact_rejects_consumption_fields() -> None:
    payload = _finalized_payload()
    payload["successor_tock_sha"] = _REFERENCE_SUCCESSOR_TOCK_SHA
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("schema_version", 2),
        ("origin", ["FINALIZED_PROMOTION_TICK"]),
        ("origin.kind", False),
        ("origin.evidence", ["tock"]),
        ("original_tick_sha", 40),
        ("expected_continuation_head_sha", None),
        ("commit_chain", {}),
        ("successor_task_id", 1),
        ("status", True),
        ("prepared_at_utc", ["now"]),
        ("continuation_digest_sha256", 64),
        ("origin.evidence.tock_sha", 40),
        ("origin.evidence.predecessor_identity", "legacy"),
        ("origin.evidence.predecessor_identity.task_id", 12),
        ("origin.evidence.predecessor_identity.origin_run_id", {"id": "x"}),
        ("origin.evidence.predecessor_identity.promotion_attempt_id", False),
        ("origin.evidence.predecessor_identity.result_sha", 40),
        ("origin.evidence.predecessor_identity.promotion_class", ["GREEN"]),
    ),
)
def test_wrong_types_are_blocked(dotted_field: str, value: Any) -> None:
    payload = _set_field(_finalized_payload(), dotted_field, value)
    if dotted_field.split(".", maxsplit=1)[0] in _BINDING_FIELDS:
        try:
            _refresh_digest(payload)
        except (TypeError, KeyError, AttributeError):
            pass
    with pytest.raises(ContinuationError) as exc_info:
        decode_continuation_artifact(payload)
    assert exc_info.value.reason_code in {
        "CONTINUATION_ARTIFACT_INVALID",
        "CONTINUATION_IDENTITY_MISMATCH",
        "CONTINUATION_ORIGIN_INVALID",
        "CONTINUATION_CHAIN_MISMATCH",
        "CONTINUATION_DIGEST_MISMATCH",
    }


@pytest.mark.parametrize(
    "field",
    _IDENTITY_FIELDS,
)
def test_blank_identity_field_is_blocked(field: str) -> None:
    payload = _finalized_payload()
    payload["origin"]["evidence"]["predecessor_identity"][field] = "   "
    _refresh_digest(payload)
    _expect_decode(payload, "CONTINUATION_IDENTITY_MISMATCH")


def test_unknown_predecessor_identity_field_is_blocked() -> None:
    payload = _finalized_payload()
    payload["origin"]["evidence"]["predecessor_identity"]["run_id"] = (
        _REFERENCE_ORIGIN_RUN_ID
    )
    _refresh_digest(payload)
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")


def test_missing_tock_and_identity_evidence_are_blocked() -> None:
    missing_tock = _drop_field(_finalized_payload(), "origin.evidence.tock_sha")
    _refresh_digest(missing_tock)
    _expect_decode(missing_tock, "CONTINUATION_ARTIFACT_INVALID")
    missing_identity = _drop_field(
        _finalized_payload(),
        "origin.evidence.predecessor_identity",
    )
    _refresh_digest(missing_identity)
    _expect_decode(missing_identity, "CONTINUATION_ARTIFACT_INVALID")


def test_successor_identity_drift_without_digest_refresh_is_blocked() -> None:
    payload = _finalized_payload()
    payload["successor_task_id"] = _FOREIGN_TASK_ID
    _expect_decode(payload, "CONTINUATION_DIGEST_MISMATCH")


def test_origin_reproof_rejects_tock_and_identity_drift() -> None:
    view = decode_continuation_artifact(_finalized_payload())
    drifted_tock = {
        **view.to_mapping()["origin"]["evidence"],
        "tock_sha": _FOREIGN_TOCK_SHA,
    }
    with pytest.raises(
        ContinuationError,
        match="CONTINUATION_IDENTITY_MISMATCH",
    ) as tock_info:
        assert_origin_reproof_matches(view=view, current_evidence=drifted_tock)
    assert tock_info.value.reason_code == "CONTINUATION_IDENTITY_MISMATCH"

    drifted_identity = copy.deepcopy(view.to_mapping()["origin"]["evidence"])
    drifted_identity["predecessor_identity"]["task_id"] = _FOREIGN_TASK_ID
    with pytest.raises(
        ContinuationError,
        match="CONTINUATION_IDENTITY_MISMATCH",
    ) as identity_info:
        assert_origin_reproof_matches(view=view, current_evidence=drifted_identity)
    assert identity_info.value.reason_code == (
        "CONTINUATION_IDENTITY_MISMATCH"
    )


def test_origin_reproof_accepts_identical_consumed_evidence() -> None:
    authorized = decode_continuation_artifact(_finalized_payload())
    consumed = decode_continuation_artifact(_finalized_payload(consumed=True))
    assert_origin_reproof_matches(
        view=authorized,
        current_evidence=consumed.to_mapping()["origin"]["evidence"],
    )


@pytest.mark.parametrize(
    "chain",
    (
        [],
        [{"sha": _REFERENCE_TICK_SHA}],
        [{"parent_sha": _REFERENCE_TOCK_SHA}],
        [{"sha": _REFERENCE_TICK_SHA, "parent_sha": _REFERENCE_TOCK_SHA, "x": "1"}],
        [{"sha": "", "parent_sha": _REFERENCE_TOCK_SHA}],
        [{"sha": _REFERENCE_TICK_SHA, "parent_sha": ""}],
        "not-a-chain",
    ),
)
def test_malformed_commit_chain_is_blocked(chain: Any) -> None:
    payload = _finalized_payload()
    payload["commit_chain"] = chain
    try:
        _refresh_digest(payload)
    except (TypeError, AttributeError, KeyError):
        pass
    with pytest.raises(ContinuationError) as exc_info:
        decode_continuation_artifact(payload)
    assert exc_info.value.reason_code in {
        "CONTINUATION_CHAIN_MISMATCH",
        "CONTINUATION_ARTIFACT_INVALID",
        "CONTINUATION_DIGEST_MISMATCH",
    }


def test_declared_chain_must_start_at_tick_and_end_at_head() -> None:
    wrong_start = _finalized_payload()
    wrong_start["commit_chain"] = _commit_chain(sha=_FOREIGN_TICK_SHA)
    wrong_start["expected_continuation_head_sha"] = _FOREIGN_TICK_SHA
    _refresh_digest(wrong_start)
    _expect_decode(wrong_start, "CONTINUATION_CHAIN_MISMATCH")

    wrong_head = _finalized_payload()
    wrong_head["expected_continuation_head_sha"] = _FOREIGN_TICK_SHA
    _refresh_digest(wrong_head)
    _expect_decode(wrong_head, "CONTINUATION_CHAIN_MISMATCH")


def test_uppercase_and_short_digest_are_blocked() -> None:
    upper = _finalized_payload()
    upper["continuation_digest_sha256"] = upper["continuation_digest_sha256"].upper()
    _expect_decode(upper, "CONTINUATION_DIGEST_MISMATCH")
    short = _finalized_payload()
    short["continuation_digest_sha256"] = "0" * 63
    _expect_decode(short, "CONTINUATION_DIGEST_MISMATCH")


def test_blank_successor_and_tick_fields_are_blocked() -> None:
    blank_successor = _set_field(_finalized_payload(), "successor_task_id", "  ")
    _refresh_digest(blank_successor)
    _expect_decode(blank_successor, "CONTINUATION_ARTIFACT_INVALID")
    blank_tick = _set_field(_finalized_payload(), "original_tick_sha", "")
    _refresh_digest(blank_tick)
    _expect_decode(blank_tick, "CONTINUATION_ARTIFACT_INVALID")


def test_codec_file_and_mapping_guards(tmp_path: Path) -> None:
    _expect_load(tmp_path / "missing.yaml", "CONTINUATION_ARTIFACT_INVALID")
    sequence = tmp_path / "sequence.yaml"
    sequence.write_text("[]\n", encoding="utf-8")
    _expect_load(sequence, "CONTINUATION_ARTIFACT_INVALID")
    directory = tmp_path / "directory.yaml"
    directory.mkdir()
    _expect_load(directory, "CONTINUATION_ARTIFACT_INVALID")


def test_unreadable_artifact_is_blocked(tmp_path: Path) -> None:
    source_path = _write_artifact(tmp_path)
    original_mode = source_path.stat().st_mode
    try:
        _make_unreadable(source_path)
        _expect_load(source_path, "CONTINUATION_ARTIFACT_INVALID")
    finally:
        if source_path.is_file():
            source_path.chmod(original_mode)


def test_composed_private_top_level_tokens_are_unknown_fields() -> None:
    payload = _finalized_payload()
    payload[_composed_token("orp", "_", "handover")] = "unexpected"
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")
    payload_two = _finalized_payload()
    payload_two[_composed_token("chron", "icle")] = "unexpected"
    _expect_decode(payload_two, "CONTINUATION_ARTIFACT_INVALID")


def test_composed_role_token_in_unknown_field_is_blocked() -> None:
    payload = _finalized_payload()
    payload[_composed_token("Hy", "dra")] = "unexpected"
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")


def test_composed_home_path_token_in_unknown_field_is_blocked() -> None:
    payload = _finalized_payload()
    payload["foreign_path"] = _composed_absolute("home", "runner", "work")
    _expect_decode(payload, "CONTINUATION_ARTIFACT_INVALID")


def test_composed_private_sha_is_not_an_identity_value() -> None:
    token = _composed_token("16e8f70e31bc424e8bde", "0e285d13a6053d9504b6")
    payload = _finalized_payload()
    payload["origin"]["evidence"]["predecessor_identity"]["task_id"] = token
    _refresh_digest(payload)
    view = decode_continuation_artifact(payload)
    assert view.identity.task_id == token
    assert view.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert not hasattr(view, "decision")
    assert not hasattr(view, "pending_promotion")


def test_error_exposes_stable_reason_code() -> None:
    error = _expect_decode({"not": "an artifact"}, "CONTINUATION_ARTIFACT_INVALID")
    assert isinstance(error, ContinuationError)
    assert error.reason_code == "CONTINUATION_ARTIFACT_INVALID"


def test_decode_rejects_non_mapping_artifact() -> None:
    _expect_decode([], "CONTINUATION_ARTIFACT_INVALID")  # type: ignore[arg-type]


def test_commit_chain_non_mapping_entry_is_blocked() -> None:
    payload = _finalized_payload()
    payload["commit_chain"] = [None]
    _expect_decode(payload, "CONTINUATION_CHAIN_MISMATCH")


def test_duplicate_yaml_keys_are_blocked(tmp_path: Path) -> None:
    source_path = _write_artifact(tmp_path)
    text = source_path.read_text(encoding="utf-8")
    source_path.write_text('schema_version: "1.0"\n' + text, encoding="utf-8")
    _expect_load(source_path, "CONTINUATION_ARTIFACT_INVALID")
