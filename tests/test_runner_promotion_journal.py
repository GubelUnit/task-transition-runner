"""Synthetic candidate-only promotion-journal contract."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import task_transition_runner.runner_promotion_journal as journal_module
from task_transition_runner.runner_promotion_journal import (
    PromotionJournal,
    PromotionJournalError,
    PromotionJournalPhase,
    allowed_journal_transitions,
    archive_promotion_journal_epoch,
    create_promotion_journal,
    load_archived_promotion_journal,
    load_promotion_journal,
    persist_promotion_journal,
    promotion_journal_archive_path,
    promotion_journal_digest,
    release_canonical_promotion_journal,
    transition_promotion_journal,
    validate_promotion_journal_transition,
)

import yaml

_SYNTHETIC_PENDING_AT = "2026-08-29T12:00:00Z"
_SYNTHETIC_BEFORE_PENDING_AT = "2026-08-29T11:59:00Z"
_SYNTHETIC_TOCK_SHA = "4444444444444444444444444444444444444444"
_SYNTHETIC_DRAIN_ALPHA = "drain-run-synthetic-alpha"
_A5_TASK_FINGERPRINT = "a" * 64
_A5_TASK_SNAPSHOT_SHA256 = "b" * 64
_A5_INSTRUCTION_SIGNATURE_SHA256 = "c" * 64

BASE_FIELDS = {
    "task_id": "TASK_SYNTHETIC_JOURNAL_CONTRACT_ALPHA",
    "origin_run_id": "origin-run-synthetic-alpha",
    "drain_run_id": _SYNTHETIC_DRAIN_ALPHA,
    "promotion_attempt_id": "promotion-attempt-synthetic-alpha",
    "promotion_class": "GREEN_PROMOTION",
    "base_main_sha": "1111111111111111111111111111111111111111",
    "result_sha": "2222222222222222222222222222222222222222",
    "validated_result_sha": "3333333333333333333333333333333333333333",
    "task_fingerprint": _A5_TASK_FINGERPRINT,
    "task_snapshot_sha256": _A5_TASK_SNAPSHOT_SHA256,
    "instruction_signature_sha256": _A5_INSTRUCTION_SIGNATURE_SHA256,
}


def _pending(**overrides: Any) -> PromotionJournal:
    fields = {**BASE_FIELDS, **overrides}
    return create_promotion_journal(
        **fields,
        pending_at_utc=_SYNTHETIC_PENDING_AT,
        reason_code="JOURNAL_READY_SYNTHETIC",
    )


def _advance(
    journal: PromotionJournal,
    phase: PromotionJournalPhase,
    *,
    index: int,
    drain_run_id: str = _SYNTHETIC_DRAIN_ALPHA,
    tock_sha: str | None = None,
) -> PromotionJournal:
    return transition_promotion_journal(
        journal,
        phase=phase,
        drain_run_id=drain_run_id,
        transitioned_at_utc=f"2026-08-29T12:0{index}:00Z",
        reason_code=f"ENTERED_{phase.value}",
        tock_sha=tock_sha,
    )


def _finalized() -> PromotionJournal:
    journal = _pending()
    journal = _advance(journal, PromotionJournalPhase.APPLYING, index=1)
    journal = _advance(
        journal,
        PromotionJournalPhase.GIT_APPLIED,
        index=2,
        tock_sha=_SYNTHETIC_TOCK_SHA,
    )
    journal = _advance(journal, PromotionJournalPhase.RECONCILED, index=3)
    return _advance(journal, PromotionJournalPhase.FINALIZED, index=4)


def _write_yaml(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_a5_missing_snapshot_authority_anchors_fail_closed(tmp_path: Path) -> None:
    marker = (
        "OSS7_A5_RED::JOURNAL_FIELDS_INVALID::"
        "test_a5_missing_snapshot_authority_anchors_fail_closed"
    )
    path = tmp_path / "promotion_journal.yaml"
    payload = _pending().to_mapping()
    for field in (
        "task_fingerprint",
        "task_snapshot_sha256",
        "instruction_signature_sha256",
    ):
        payload.pop(field, None)
    _write_yaml(path, payload)

    try:
        load_promotion_journal(path=path)
    except PromotionJournalError as exc:
        if exc.reason_code == "JOURNAL_FIELDS_INVALID":
            return
    raise AssertionError(marker)


@pytest.mark.parametrize(
    ("field", "malformed"),
    (
        ("task_fingerprint", _A5_TASK_FINGERPRINT.upper()),
        ("task_snapshot_sha256", "not-lowercase-64-hex"),
        ("instruction_signature_sha256", "d" * 63),
    ),
)
def test_a5_malformed_snapshot_authority_anchors_fail_closed(
    tmp_path: Path,
    field: str,
    malformed: str,
) -> None:
    marker = (
        "OSS7_A5_RED::JOURNAL_FIELDS_INVALID::"
        "test_a5_malformed_snapshot_authority_anchors_fail_closed"
    )
    path = tmp_path / "promotion_journal.yaml"
    payload = _pending().to_mapping()
    payload[field] = malformed
    _write_yaml(path, payload)
    persisted = path.read_bytes()

    try:
        load_promotion_journal(path=path)
    except PromotionJournalError as exc:
        if (
            exc.reason_code == "JOURNAL_FIELDS_INVALID"
            and "unknown fields" not in str(exc)
        ):
            assert path.read_bytes() == persisted
            return
    raise AssertionError(marker)


@pytest.mark.parametrize(
    ("field", "drifted"),
    (
        ("task_fingerprint", "d" * 64),
        ("task_snapshot_sha256", "e" * 64),
        ("instruction_signature_sha256", "f" * 64),
    ),
)
def test_a5_transition_drifted_snapshot_authority_anchors_fail_closed(
    field: str,
    drifted: str,
) -> None:
    marker = (
        "OSS7_A5_RED::JOURNAL_FIELD_DRIFT::"
        "test_a5_transition_drifted_snapshot_authority_anchors_fail_closed"
    )
    pending = _pending()
    pending_mapping = pending.to_mapping()
    try:
        candidate = replace(
            pending,
            phase=PromotionJournalPhase.APPLYING,
            applying_at_utc="2026-08-29T12:01:00Z",
            **{field: drifted},
        )
    except TypeError:
        raise AssertionError(marker) from None

    try:
        validate_promotion_journal_transition(pending, candidate)
    except PromotionJournalError as exc:
        if exc.reason_code == "JOURNAL_FIELD_DRIFT":
            assert pending.to_mapping() == pending_mapping
            return
    raise AssertionError(marker)


@pytest.mark.parametrize(
    "field",
    (
        "task_fingerprint",
        "task_snapshot_sha256",
        "instruction_signature_sha256",
    ),
)
def test_partial_snapshot_authority_anchor_family_fails_closed(
    tmp_path: Path,
    field: str,
) -> None:
    path = tmp_path / "promotion_journal.yaml"
    payload = _pending().to_mapping()
    payload.pop(field)
    _write_yaml(path, payload)

    with pytest.raises(PromotionJournalError, match="JOURNAL_FIELDS_INVALID") as exc_info:
        load_promotion_journal(path=path)

    assert exc_info.value.reason_code == "JOURNAL_FIELDS_INVALID"
    assert field in str(exc_info.value)
    assert "unknown fields" not in str(exc_info.value)


def test_phase_algebra_and_persistence_are_closed_world_contracts() -> None:
    assert allowed_journal_transitions() == {
        PromotionJournalPhase.PENDING: (PromotionJournalPhase.APPLYING,),
        PromotionJournalPhase.APPLYING: (PromotionJournalPhase.GIT_APPLIED,),
        PromotionJournalPhase.GIT_APPLIED: (PromotionJournalPhase.RECONCILED,),
        PromotionJournalPhase.RECONCILED: (PromotionJournalPhase.FINALIZED,),
        PromotionJournalPhase.FINALIZED: (),
    }
    assert journal_module.ALLOWED_PROMOTION_CLASSES == (
        "GREEN_PROMOTION",
        "RED_TDD_PROMOTION",
    )
    assert journal_module.PERSISTENCE_CONTRACT == "SINGLE_FILE_ATOMIC_REPLACE"


@pytest.mark.parametrize(
    "promotion_class",
    ("GREEN_PROMOTION", "RED_TDD_PROMOTION"),
)
def test_journal_accepts_only_tock_producing_promotion_classes(
    promotion_class: str,
) -> None:
    assert _pending(promotion_class=promotion_class).promotion_class == promotion_class


@pytest.mark.parametrize("promotion_class", ("BLOCKED", "UNKNOWN_PROMOTION"))
def test_create_rejects_non_tock_promotion_classes(promotion_class: str) -> None:
    with pytest.raises(
        PromotionJournalError,
        match="JOURNAL_PROMOTION_CLASS_INVALID",
    ) as exc_info:
        _pending(promotion_class=promotion_class)

    assert exc_info.value.reason_code == "JOURNAL_PROMOTION_CLASS_INVALID"


@pytest.mark.parametrize("promotion_class", ("BLOCKED", "UNKNOWN_PROMOTION"))
def test_load_rejects_non_tock_promotion_classes(
    tmp_path: Path,
    promotion_class: str,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    payload = _pending().to_mapping()
    payload["promotion_class"] = promotion_class
    _write_yaml(path, payload)

    with pytest.raises(
        PromotionJournalError,
        match="JOURNAL_PROMOTION_CLASS_INVALID",
    ) as exc_info:
        load_promotion_journal(path=path)

    assert exc_info.value.reason_code == "JOURNAL_PROMOTION_CLASS_INVALID"


def test_every_allowed_transition_persists_and_reloads_deterministically(
    tmp_path: Path,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    pending = _pending()

    assert persist_promotion_journal(path=path, journal=pending) == pending
    assert load_promotion_journal(path=path) == pending
    assert "tock_sha" not in pending.to_mapping()
    assert pending.to_mapping()["task_fingerprint"] == _A5_TASK_FINGERPRINT
    assert pending.to_mapping()["task_snapshot_sha256"] == _A5_TASK_SNAPSHOT_SHA256
    assert (
        pending.to_mapping()["instruction_signature_sha256"]
        == _A5_INSTRUCTION_SIGNATURE_SHA256
    )

    applying = _advance(
        pending,
        PromotionJournalPhase.APPLYING,
        index=1,
        drain_run_id="drain-run-synthetic-bravo",
    )
    git_applied = _advance(
        applying,
        PromotionJournalPhase.GIT_APPLIED,
        index=2,
        drain_run_id="drain-run-synthetic-charlie",
        tock_sha=_SYNTHETIC_TOCK_SHA,
    )
    reconciled = _advance(
        git_applied,
        PromotionJournalPhase.RECONCILED,
        index=3,
        drain_run_id="drain-run-synthetic-delta",
    )
    finalized = _advance(
        reconciled,
        PromotionJournalPhase.FINALIZED,
        index=4,
        drain_run_id="drain-run-synthetic-echo",
    )

    for expected in (applying, git_applied, reconciled, finalized):
        assert persist_promotion_journal(path=path, journal=expected) == expected
        assert load_promotion_journal(path=path) == expected

    persisted = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert persisted["phase"] == "FINALIZED"
    assert persisted["drain_run_id"] == "drain-run-synthetic-echo"
    assert persisted["tock_sha"] == _SYNTHETIC_TOCK_SHA
    assert persisted["task_fingerprint"] == _A5_TASK_FINGERPRINT
    assert persisted["task_snapshot_sha256"] == _A5_TASK_SNAPSHOT_SHA256
    assert persisted["instruction_signature_sha256"] == _A5_INSTRUCTION_SIGNATURE_SHA256
    assert list(persisted["phase_timestamps"]) == [
        "PENDING",
        "APPLYING",
        "GIT_APPLIED",
        "RECONCILED",
        "FINALIZED",
    ]
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    ("source_phase", "target_phase"),
    (
        (PromotionJournalPhase.PENDING, PromotionJournalPhase.GIT_APPLIED),
        (PromotionJournalPhase.APPLYING, PromotionJournalPhase.PENDING),
        (PromotionJournalPhase.GIT_APPLIED, PromotionJournalPhase.FINALIZED),
        (PromotionJournalPhase.FINALIZED, PromotionJournalPhase.FINALIZED),
    ),
)
def test_illegal_backward_jump_and_terminal_transitions_are_side_effect_free(
    tmp_path: Path,
    source_phase: PromotionJournalPhase,
    target_phase: PromotionJournalPhase,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    states = [_pending()]
    states.append(_advance(states[-1], PromotionJournalPhase.APPLYING, index=1))
    states.append(
        _advance(
            states[-1],
            PromotionJournalPhase.GIT_APPLIED,
            index=2,
            tock_sha=_SYNTHETIC_TOCK_SHA,
        )
    )
    states.append(_advance(states[-1], PromotionJournalPhase.RECONCILED, index=3))
    states.append(_advance(states[-1], PromotionJournalPhase.FINALIZED, index=4))
    source = states[list(PromotionJournalPhase).index(source_phase)]
    persist_promotion_journal(path=path, journal=states[0])
    for state in states[1 : list(PromotionJournalPhase).index(source_phase) + 1]:
        persist_promotion_journal(path=path, journal=state)
    before = path.read_bytes()

    candidate = replace(source, phase=target_phase)
    with pytest.raises(PromotionJournalError, match="JOURNAL_TRANSITION_ILLEGAL"):
        persist_promotion_journal(path=path, journal=candidate)

    assert path.read_bytes() == before
    assert load_promotion_journal(path=path) == source


@pytest.mark.parametrize(
    "field",
    (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "promotion_class",
        "base_main_sha",
        "result_sha",
        "validated_result_sha",
        "task_fingerprint",
        "task_snapshot_sha256",
        "instruction_signature_sha256",
    ),
)
def test_stable_attempt_field_drift_is_blocked_without_write(
    tmp_path: Path,
    field: str,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    pending = _pending()
    persist_promotion_journal(path=path, journal=pending)
    applying = _advance(pending, PromotionJournalPhase.APPLYING, index=1)
    candidate = replace(applying, **{field: f"foreign-{field}"})
    before = path.read_bytes()

    with pytest.raises(PromotionJournalError, match="JOURNAL_FIELD_DRIFT"):
        persist_promotion_journal(path=path, journal=candidate)

    assert path.read_bytes() == before


def test_tock_sha_is_forbidden_before_and_required_after_physical_git_proof() -> None:
    pending = _pending()
    with pytest.raises(PromotionJournalError, match="JOURNAL_TRANSITION_ILLEGAL"):
        transition_promotion_journal(
            pending,
            phase=PromotionJournalPhase.GIT_APPLIED,
            drain_run_id="drain-run-synthetic-transient",
            transitioned_at_utc="2026-08-29T12:02:00Z",
            reason_code="SKIPPED_APPLYING",
            tock_sha=_SYNTHETIC_TOCK_SHA,
        )
    with pytest.raises(PromotionJournalError, match="JOURNAL_TOCK_FORBIDDEN"):
        transition_promotion_journal(
            pending,
            phase=PromotionJournalPhase.APPLYING,
            drain_run_id="drain-run-synthetic-transient",
            transitioned_at_utc="2026-08-29T12:01:00Z",
            reason_code="APPLYING",
            tock_sha="5555555555555555555555555555555555555555",
        )

    applying = _advance(pending, PromotionJournalPhase.APPLYING, index=1)
    with pytest.raises(PromotionJournalError, match="JOURNAL_TOCK_REQUIRED"):
        transition_promotion_journal(
            applying,
            phase=PromotionJournalPhase.GIT_APPLIED,
            drain_run_id="drain-run-synthetic-transient",
            transitioned_at_utc="2026-08-29T12:02:00Z",
            reason_code="GIT_APPLIED",
        )


def test_transition_validation_blocks_timeline_and_tock_drift() -> None:
    pending = _pending()
    applying = _advance(pending, PromotionJournalPhase.APPLYING, index=1)
    git_applied = _advance(
        applying,
        PromotionJournalPhase.GIT_APPLIED,
        index=2,
        tock_sha=_SYNTHETIC_TOCK_SHA,
    )
    reconciled = _advance(git_applied, PromotionJournalPhase.RECONCILED, index=3)

    with pytest.raises(PromotionJournalError, match="JOURNAL_TIMELINE_DRIFT"):
        validate_promotion_journal_transition(
            pending,
            replace(applying, pending_at_utc=_SYNTHETIC_BEFORE_PENDING_AT),
        )
    with pytest.raises(PromotionJournalError, match="JOURNAL_FIELD_DRIFT"):
        validate_promotion_journal_transition(
            git_applied,
            replace(reconciled, tock_sha="6666666666666666666666666666666666666666"),
        )


def test_public_transition_boundaries_reject_untyped_values_fail_closed(
    tmp_path: Path,
) -> None:
    pending = _pending()
    invalid_phase_journal = replace(pending, phase="UNKNOWN")  # type: ignore[arg-type]

    with pytest.raises(PromotionJournalError, match="JOURNAL_PHASE_UNKNOWN"):
        persist_promotion_journal(
            path=tmp_path / "candidate_journal.yaml",
            journal=invalid_phase_journal,
        )
    with pytest.raises(PromotionJournalError, match="JOURNAL_PHASE_UNKNOWN"):
        transition_promotion_journal(
            pending,
            phase="UNKNOWN",  # type: ignore[arg-type]
            drain_run_id="drain-run-synthetic-transient",
            transitioned_at_utc="2026-08-29T12:01:00Z",
            reason_code="INVALID_PHASE",
        )
    with pytest.raises(PromotionJournalError, match="JOURNAL_PHASE_UNKNOWN"):
        validate_promotion_journal_transition(pending, invalid_phase_journal)
    with pytest.raises(PromotionJournalError, match="JOURNAL_FIELDS_INVALID"):
        validate_promotion_journal_transition(
            pending,
            "not-a-journal",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("overrides", "reason_code"),
    (
        ({"drain_run_id": ""}, "JOURNAL_IDENTITY_INCOMPLETE"),
        ({"reason_code": ""}, "JOURNAL_REASON_CODE_INVALID"),
        (
            {"transitioned_at_utc": _SYNTHETIC_BEFORE_PENDING_AT},
            "JOURNAL_TIMELINE_INVALID",
        ),
        ({"transitioned_at_utc": "not-a-time"}, "JOURNAL_TIMESTAMP_INVALID"),
    ),
)
def test_transition_rejects_invalid_executor_reason_and_time(
    overrides: dict[str, str],
    reason_code: str,
) -> None:
    arguments = {
        "phase": PromotionJournalPhase.APPLYING,
        "drain_run_id": "drain-run-synthetic-bravo",
        "transitioned_at_utc": "2026-08-29T12:01:00Z",
        "reason_code": "APPLYING",
        **overrides,
    }

    with pytest.raises(PromotionJournalError, match=reason_code):
        transition_promotion_journal(_pending(), **arguments)


@pytest.mark.parametrize(
    ("content", "reason_code"),
    (
        ("broken: [\n", "JOURNAL_YAML_MALFORMED"),
        ("- not\n- a\n- mapping\n", "JOURNAL_MAPPING_REQUIRED"),
        ("{}\n", "JOURNAL_SCHEMA_UNSUPPORTED"),
    ),
)
def test_load_rejects_malformed_yaml_non_mapping_and_unknown_schema(
    tmp_path: Path,
    content: str,
    reason_code: str,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(PromotionJournalError, match=reason_code):
        load_promotion_journal(path=path)


def test_load_rejects_unknown_schema_version_2_0(tmp_path: Path) -> None:
    path = tmp_path / "candidate_journal.yaml"
    payload = _pending().to_mapping()
    payload["schema_version"] = "2.0"
    _write_yaml(path, payload)

    with pytest.raises(PromotionJournalError, match="JOURNAL_SCHEMA_UNSUPPORTED"):
        load_promotion_journal(path=path)


def test_load_rejects_missing_file_and_read_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    with pytest.raises(PromotionJournalError, match="JOURNAL_NOT_FOUND"):
        load_promotion_journal(path=path)

    path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(Path, "read_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()))
    with pytest.raises(PromotionJournalError, match="JOURNAL_READ_FAILED"):
        load_promotion_journal(path=path)


@pytest.mark.parametrize(
    ("mutate", "reason_code"),
    (
        (lambda payload: payload.update(phase="UNKNOWN"), "JOURNAL_PHASE_UNKNOWN"),
        (lambda payload: payload.update(task_id=""), "JOURNAL_IDENTITY_INCOMPLETE"),
        (lambda payload: payload.pop("result_sha"), "JOURNAL_PROMOTION_FIELDS_INCOMPLETE"),
        (lambda payload: payload.update(reason_code=" "), "JOURNAL_REASON_CODE_INVALID"),
        (lambda payload: payload.update(extra_field="open-world"), "JOURNAL_FIELDS_INVALID"),
        (
            lambda payload: payload.update(phase_timestamps=[]),
            "JOURNAL_TIMELINE_INVALID",
        ),
        (
            lambda payload: payload["phase_timestamps"].update(PENDING="not-a-time"),
            "JOURNAL_TIMESTAMP_INVALID",
        ),
        (
            lambda payload: payload["phase_timestamps"].update(PENDING="2026-99-99T12:00:00Z"),
            "JOURNAL_TIMESTAMP_INVALID",
        ),
        (
            lambda payload: payload["phase_timestamps"].update(UNKNOWN="2026-08-29T12:00:00Z"),
            "JOURNAL_TIMELINE_INVALID",
        ),
        (
            lambda payload: payload["phase_timestamps"].update(
                APPLYING="2026-08-29T12:01:00Z"
            ),
            "JOURNAL_TIMELINE_INVALID",
        ),
        (
            lambda payload: payload.update(tock_sha="invented"),
            "JOURNAL_TOCK_FORBIDDEN",
        ),
    ),
)
def test_load_fail_closed_validation_is_specific(
    tmp_path: Path,
    mutate: Any,
    reason_code: str,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    payload = _pending().to_mapping()
    mutate(payload)
    _write_yaml(path, payload)

    with pytest.raises(PromotionJournalError, match=reason_code):
        load_promotion_journal(path=path)


def test_load_requires_tock_and_monotonic_complete_phase_timeline(tmp_path: Path) -> None:
    path = tmp_path / "candidate_journal.yaml"
    pending = _pending()
    applying = _advance(pending, PromotionJournalPhase.APPLYING, index=1)
    git_applied = _advance(
        applying,
        PromotionJournalPhase.GIT_APPLIED,
        index=2,
        tock_sha=_SYNTHETIC_TOCK_SHA,
    )

    missing_tock = git_applied.to_mapping()
    missing_tock.pop("tock_sha")
    _write_yaml(path, missing_tock)
    with pytest.raises(PromotionJournalError, match="JOURNAL_TOCK_REQUIRED"):
        load_promotion_journal(path=path)

    empty_tock = git_applied.to_mapping()
    empty_tock["tock_sha"] = " "
    _write_yaml(path, empty_tock)
    with pytest.raises(PromotionJournalError, match="JOURNAL_FIELDS_INVALID"):
        load_promotion_journal(path=path)

    reversed_time = git_applied.to_mapping()
    reversed_time["phase_timestamps"]["GIT_APPLIED"] = _SYNTHETIC_BEFORE_PENDING_AT
    _write_yaml(path, reversed_time)
    with pytest.raises(PromotionJournalError, match="JOURNAL_TIMELINE_INVALID"):
        load_promotion_journal(path=path)


def test_load_rejects_finalized_timeline_missing_predecessor_timestamp(
    tmp_path: Path,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    payload = _finalized().to_mapping()
    payload["phase_timestamps"].pop("RECONCILED")
    _write_yaml(path, payload)

    with pytest.raises(PromotionJournalError, match="JOURNAL_TIMELINE_INVALID"):
        load_promotion_journal(path=path)


def test_initial_persistence_requires_pending_and_atomic_replace_failure_keeps_old_truth(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    path = tmp_path / "candidate_journal.yaml"
    pending = _pending()
    applying = _advance(pending, PromotionJournalPhase.APPLYING, index=1)

    with pytest.raises(PromotionJournalError, match="JOURNAL_INITIAL_PHASE_INVALID"):
        persist_promotion_journal(path=path, journal=applying)

    persist_promotion_journal(path=path, journal=pending)
    old_truth = path.read_bytes()
    monkeypatch.setattr(
        journal_module.os,
        "replace",
        lambda *_args: (_ for _ in ()).throw(OSError("replace failed")),
    )
    with pytest.raises(PromotionJournalError, match="JOURNAL_WRITE_FAILED"):
        persist_promotion_journal(path=path, journal=applying)

    assert path.read_bytes() == old_truth
    assert not list(tmp_path.glob("*.tmp"))


def test_corrupt_existing_journal_is_never_overwritten(tmp_path: Path) -> None:
    path = tmp_path / "candidate_journal.yaml"
    path.write_text("broken: [\n", encoding="utf-8")
    old_truth = path.read_bytes()

    with pytest.raises(PromotionJournalError, match="JOURNAL_YAML_MALFORMED"):
        persist_promotion_journal(path=path, journal=_pending())

    assert path.read_bytes() == old_truth
    assert not list(tmp_path.glob("*.tmp"))


def test_invalid_model_type_and_pre_temp_write_failure_are_specific(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    with pytest.raises(PromotionJournalError, match="JOURNAL_FIELDS_INVALID"):
        journal_module._validate_journal("not-a-journal")

    monkeypatch.setattr(
        Path,
        "mkdir",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("mkdir failed")),
    )
    with pytest.raises(PromotionJournalError, match="JOURNAL_WRITE_FAILED"):
        persist_promotion_journal(
            path=tmp_path / "missing" / "candidate_journal.yaml",
            journal=_pending(),
        )


def test_finalized_epoch_archive_is_digest_bound_reloadable_and_idempotent(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    runtime = tmp_path / "runtime"
    canonical = runtime / "promotion_journal.yaml"
    journal = _finalized()
    _write_yaml(canonical, journal.to_mapping())

    archive = archive_promotion_journal_epoch(
        runtime_root=runtime,
        journal=journal,
    )

    assert archive == promotion_journal_archive_path(
        runtime_root=runtime,
        journal=journal,
    )
    assert archive.parent == (
        runtime / "runs" / journal.origin_run_id / "promotion_journals"
    )
    assert load_archived_promotion_journal(path=archive) == journal
    payload = yaml.safe_load(archive.read_text(encoding="utf-8"))
    assert payload == {
        "schema_version": "1.0",
        "archive_type": "promotion_journal_epoch",
        "journal_digest_sha256": promotion_journal_digest(journal),
        "journal": journal.to_mapping(),
    }
    archived_bytes = archive.read_bytes()
    monkeypatch.setattr(
        journal_module,
        "_atomic_write_yaml",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("identical archive must not be rewritten")
        ),
    )
    assert archive_promotion_journal_epoch(
        runtime_root=runtime,
        journal=journal,
    ) == archive
    assert archive.read_bytes() == archived_bytes

    release_canonical_promotion_journal(
        runtime_root=runtime,
        journal=journal,
    )
    assert not canonical.exists()
    assert archive.read_bytes() == archived_bytes
    release_canonical_promotion_journal(
        runtime_root=runtime,
        journal=journal,
    )


def test_archive_requires_finalized_exact_canonical_truth(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    canonical = runtime / "promotion_journal.yaml"
    pending = _pending()
    _write_yaml(canonical, pending.to_mapping())

    with pytest.raises(
        PromotionJournalError,
        match="JOURNAL_ARCHIVE_FINALIZED_REQUIRED",
    ):
        archive_promotion_journal_epoch(
            runtime_root=runtime,
            journal=pending,
        )

    finalized = _finalized()
    with pytest.raises(
        PromotionJournalError,
        match="JOURNAL_CANONICAL_DRIFT",
    ):
        archive_promotion_journal_epoch(
            runtime_root=runtime,
            journal=finalized,
        )
    assert canonical.read_bytes() == yaml.safe_dump(
        pending.to_mapping(),
        sort_keys=False,
    ).encode()


@pytest.mark.parametrize(
    "archive_mutation",
    ("malformed", "partial", "digest", "journal", "journal_invalid", "archive_type"),
)
def test_existing_partial_or_drifted_archive_blocks_without_rewrite(
    tmp_path: Path,
    archive_mutation: str,
) -> None:
    runtime = tmp_path / "runtime"
    journal = _finalized()
    canonical = runtime / "promotion_journal.yaml"
    _write_yaml(canonical, journal.to_mapping())
    archive = archive_promotion_journal_epoch(
        runtime_root=runtime,
        journal=journal,
    )
    payload = yaml.safe_load(archive.read_text(encoding="utf-8"))
    if archive_mutation == "malformed":
        archive.write_text("broken: [\n", encoding="utf-8")
    elif archive_mutation == "partial":
        _write_yaml(archive, {"schema_version": "1.0"})
    elif archive_mutation == "digest":
        _write_yaml(archive, {**payload, "journal_digest_sha256": "foreign"})
    elif archive_mutation == "journal":
        payload["journal"]["task_id"] = "TASK_SYNTHETIC_FOREIGN"
        _write_yaml(archive, payload)
    elif archive_mutation == "archive_type":
        _write_yaml(archive, {**payload, "archive_type": "synthetic_epoch_snapshot"})
    else:
        payload["journal"]["task_id"] = ""
        _write_yaml(archive, payload)
    drifted_bytes = archive.read_bytes()

    with pytest.raises(
        PromotionJournalError,
        match="JOURNAL_ARCHIVE_INVALID|JOURNAL_ARCHIVE_DRIFT",
    ):
        archive_promotion_journal_epoch(
            runtime_root=runtime,
            journal=journal,
        )

    assert archive.read_bytes() == drifted_bytes
    assert load_promotion_journal(path=canonical) == journal


def test_canonical_release_requires_verified_archive_and_exact_cas(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    runtime = tmp_path / "runtime"
    canonical = runtime / "promotion_journal.yaml"
    journal = _finalized()
    _write_yaml(canonical, journal.to_mapping())

    with pytest.raises(PromotionJournalError, match="JOURNAL_ARCHIVE_NOT_FOUND"):
        release_canonical_promotion_journal(
            runtime_root=runtime,
            journal=journal,
        )

    archive_promotion_journal_epoch(runtime_root=runtime, journal=journal)
    _write_yaml(canonical, _finalized().to_mapping() | {"task_id": "TASK_SYNTHETIC_FOREIGN"})
    with pytest.raises(PromotionJournalError, match="JOURNAL_CANONICAL_DRIFT"):
        release_canonical_promotion_journal(
            runtime_root=runtime,
            journal=journal,
        )

    canonical.write_text("broken: [\n", encoding="utf-8")
    with pytest.raises(PromotionJournalError, match="JOURNAL_CANONICAL_DRIFT"):
        release_canonical_promotion_journal(
            runtime_root=runtime,
            journal=journal,
        )

    _write_yaml(canonical, journal.to_mapping())
    monkeypatch.setattr(
        Path,
        "unlink",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("unlink failed")),
    )
    with pytest.raises(PromotionJournalError, match="JOURNAL_RELEASE_FAILED"):
        release_canonical_promotion_journal(
            runtime_root=runtime,
            journal=journal,
        )
    assert canonical.is_file()


def test_archive_defensive_reload_identity_and_path_guards(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    journal = _finalized()
    foreign = replace(journal, task_id="TASK_SYNTHETIC_FOREIGN")

    missing_runtime = tmp_path / "missing-canonical"
    with pytest.raises(PromotionJournalError, match="JOURNAL_CANONICAL_DRIFT"):
        archive_promotion_journal_epoch(
            runtime_root=missing_runtime,
            journal=journal,
        )

    foreign_runtime = tmp_path / "foreign-existing"
    foreign_canonical = foreign_runtime / "promotion_journal.yaml"
    _write_yaml(foreign_canonical, journal.to_mapping())
    foreign_archive = promotion_journal_archive_path(
        runtime_root=foreign_runtime,
        journal=journal,
    )
    _write_yaml(
        foreign_archive,
        {
            "schema_version": "1.0",
            "archive_type": "promotion_journal_epoch",
            "journal_digest_sha256": promotion_journal_digest(foreign),
            "journal": foreign.to_mapping(),
        },
    )
    with pytest.raises(PromotionJournalError, match="JOURNAL_ARCHIVE_DRIFT"):
        archive_promotion_journal_epoch(
            runtime_root=foreign_runtime,
            journal=journal,
        )

    reload_runtime = tmp_path / "reload-race"
    _write_yaml(reload_runtime / "promotion_journal.yaml", journal.to_mapping())
    original_loader = journal_module.load_archived_promotion_journal
    monkeypatch.setattr(
        journal_module,
        "load_archived_promotion_journal",
        lambda **_: foreign,
    )
    with pytest.raises(PromotionJournalError, match="JOURNAL_ARCHIVE_DRIFT"):
        archive_promotion_journal_epoch(
            runtime_root=reload_runtime,
            journal=journal,
        )

    release_runtime = tmp_path / "release-race"
    _write_yaml(release_runtime / "promotion_journal.yaml", journal.to_mapping())
    archive_path = promotion_journal_archive_path(
        runtime_root=release_runtime,
        journal=journal,
    )
    _write_yaml(archive_path, {"placeholder": True})
    with pytest.raises(PromotionJournalError, match="JOURNAL_ARCHIVE_DRIFT"):
        release_canonical_promotion_journal(
            runtime_root=release_runtime,
            journal=journal,
        )
    monkeypatch.setattr(
        journal_module,
        "load_archived_promotion_journal",
        original_loader,
    )

    with pytest.raises(PromotionJournalError, match="JOURNAL_IDENTITY_INCOMPLETE"):
        promotion_journal_archive_path(
            runtime_root=tmp_path,
            journal=replace(journal, origin_run_id=":::"),
        )
