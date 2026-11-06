"""Synthetic public promotion-state and consumption contract.

Keep:
- closed fresh ``main_state.yaml`` schema
- atomic persist of Main-State and Consumption
- monotone observed-head and last-TOCK facts
- Start-TICK CAS against the observed Main head
- attempt-/result-bound consumption, identical reproof, conflicting reuse

Drop:
- legacy SHA aliases and partial-schema compatibility
- private artefact names, ack files, runtime snapshots, lock, recovery
- Admission, pending intent, TOCK materialization, CLI, and PUBLIC-P claims
"""

from __future__ import annotations

import inspect
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.runner_promotion_state as state_module
from task_transition_runner.runner_promotion_state import (
    RunnerPromotionStateError,
    is_promotion_consumed,
    load_and_validate_main_state,
    load_promotion_consumed,
    record_promotion_consumed,
    transition_main_state_for_start_tick,
    update_observed_main_head_sha,
    write_main_state,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_STATE = "GREEN_VALIDATED"
_REFERENCE_REASON = "STATE_READY_SYNTHETIC"
_REFERENCE_UPDATED_AT = "2026-09-03T16:00:00Z"
_REFERENCE_VALIDATED_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_REFERENCE_LAST_TOCK_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_REFERENCE_OBSERVED_HEAD_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_REFERENCE_START_TICK_SHA = "dddddddddddddddddddddddddddddddddddddddd"
_POST_TICK_OBSERVED_HEAD_SHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
_FOREIGN_HEAD_SHA = "ffffffffffffffffffffffffffffffffffffffff"
_FOREIGN_TOCK_SHA = "1111111111111111111111111111111111111111"
_FOREIGN_RESULT_SHA = "2222222222222222222222222222222222222222"
_REFERENCE_ATTEMPT_ID = "promotion-attempt-synthetic-alpha"
_FOREIGN_ATTEMPT_ID = "promotion-attempt-synthetic-beta"
_REFERENCE_CONSUMED_AT = "2026-09-03T16:05:00Z"
_FOREIGN_CONSUMED_AT = "2026-09-03T16:15:00Z"
_CANONICAL_MAIN_STATES = ("GREEN_VALIDATED", "EXPECTED_RED_OPEN", "BLOCKED")
_REQUIRED_MAIN_STATE_FIELDS = (
    "schema_version",
    "state",
    "main_branch",
    "validated_result_sha",
    "last_tock_sha",
    "observed_main_head_sha",
    "reason_code",
    "updated_at_utc",
)
_LEGACY_ALIAS_FIELDS = (
    "validate_result_sha",
    "main_head_sha",
)
_REQUIRED_EXPORTS = (
    "RunnerPromotionStateError",
    "load_and_validate_main_state",
    "write_main_state",
    "transition_main_state_for_start_tick",
    "update_observed_main_head_sha",
    "load_promotion_consumed",
    "is_promotion_consumed",
    "record_promotion_consumed",
)
_FORBIDDEN_EXPORTS = (
    "admit_promotion",
    "create_pending_promotion",
    "pending_promotion",
    "promote",
)
_FORBIDDEN_PARAMETERS = (
    "runner_git_context",
    "runtime_snapshot",
    "lock_ownership",
)
_PUBLIC_MUTATORS = (
    load_and_validate_main_state,
    write_main_state,
    transition_main_state_for_start_tick,
    update_observed_main_head_sha,
    load_promotion_consumed,
    record_promotion_consumed,
)


def _fresh_main_state(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": _REFERENCE_STATE,
        "main_branch": _REFERENCE_MAIN_BRANCH,
        "validated_result_sha": _REFERENCE_VALIDATED_RESULT_SHA,
        "last_tock_sha": _REFERENCE_LAST_TOCK_SHA,
        "observed_main_head_sha": _REFERENCE_OBSERVED_HEAD_SHA,
        "reason_code": _REFERENCE_REASON,
        "updated_at_utc": _REFERENCE_UPDATED_AT,
    }
    payload.update(overrides)
    return payload


def _write_yaml(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _main_state_path(tmp_path: Path) -> Path:
    return tmp_path / "runner-runtime" / "main_state.yaml"


def _consumed_path(tmp_path: Path) -> Path:
    return tmp_path / "runner-runtime" / "promotion_consumed.yaml"


def _expect_state_error(reason_code: str, action: Any) -> RunnerPromotionStateError:
    with pytest.raises(RunnerPromotionStateError, match=reason_code) as exc_info:
        action()
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _fail_replace(*_args: Any, **_kwargs: Any) -> None:
    raise OSError("simulated state swap failure")


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip("process retains read access after clearing read mode bits")


def _consumption_entry(
    *,
    attempt_id: str = _REFERENCE_ATTEMPT_ID,
    result_sha: str = _REFERENCE_VALIDATED_RESULT_SHA,
    pre_state: dict[str, Any] | None = None,
    consumed_at_utc: str = _REFERENCE_CONSUMED_AT,
    tock_sha: str = _REFERENCE_LAST_TOCK_SHA,
) -> dict[str, Any]:
    return {
        "promotion_attempt_id": attempt_id,
        "result_sha": result_sha,
        "main_state_before_promotion": (
            _fresh_main_state() if pre_state is None else pre_state
        ),
        "consumed_at_utc": consumed_at_utc,
        "tock_sha": tock_sha,
    }


def _consumption_ledger(*entries: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "consumed_promotions": list(entries),
    }


def _record_consumed(
    path: Path,
    *,
    attempt_id: str = _REFERENCE_ATTEMPT_ID,
    result_sha: str = _REFERENCE_VALIDATED_RESULT_SHA,
    pre_state: dict[str, Any] | None = None,
    consumed_at_utc: str = _REFERENCE_CONSUMED_AT,
    tock_sha: str = _REFERENCE_LAST_TOCK_SHA,
) -> dict[str, Any]:
    return record_promotion_consumed(
        promotion_consumed_path=path,
        promotion_attempt_id=attempt_id,
        result_sha=result_sha,
        main_state_before_promotion=(
            _fresh_main_state() if pre_state is None else pre_state
        ),
        consumed_at_utc=consumed_at_utc,
        tock_sha=tock_sha,
    )


def test_public_exports_are_the_named_contract_surface() -> None:
    exported = {name for name in dir(state_module) if not name.startswith("_")}

    for name in _REQUIRED_EXPORTS:
        assert name in exported
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported
    assert "PendingPromotion" not in exported
    assert "Sandbox" not in exported


def test_public_mutators_take_explicit_paths_not_private_context() -> None:
    for function in _PUBLIC_MUTATORS:
        parameters = inspect.signature(function).parameters
        for forbidden in _FORBIDDEN_PARAMETERS:
            assert forbidden not in parameters


@pytest.mark.parametrize("state", _CANONICAL_MAIN_STATES)
def test_fresh_schema_accepts_each_canonical_main_state(
    tmp_path: Path,
    state: str,
) -> None:
    path = _main_state_path(tmp_path)
    payload = _fresh_main_state(state=state)
    _write_yaml(path, payload)

    loaded = load_and_validate_main_state(main_state_path=path)

    assert loaded == payload
    assert set(loaded) == set(_REQUIRED_MAIN_STATE_FIELDS)


@pytest.mark.parametrize("missing_field", _REQUIRED_MAIN_STATE_FIELDS)
def test_fresh_schema_rejects_missing_required_fields(
    tmp_path: Path,
    missing_field: str,
) -> None:
    path = _main_state_path(tmp_path)
    payload = _fresh_main_state()
    payload.pop(missing_field)
    _write_yaml(path, payload)

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(main_state_path=path),
    )


@pytest.mark.parametrize(
    "unknown_field",
    (
        *_LEGACY_ALIAS_FIELDS,
        "pending_promotion",
        "promotion_journal",
        "origin_run_id",
        "orphan_runtime_field",
    ),
)
def test_fresh_schema_rejects_unknown_and_legacy_alias_fields(
    tmp_path: Path,
    unknown_field: str,
) -> None:
    path = _main_state_path(tmp_path)
    payload = _fresh_main_state()
    payload[unknown_field] = "not-a-fresh-field"
    _write_yaml(path, payload)

    _expect_state_error(
        "MAIN_STATE_FIELD_UNKNOWN",
        lambda: load_and_validate_main_state(main_state_path=path),
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("schema_version", 1.0),
        ("state", 0),
        ("main_branch", ["main"]),
        ("validated_result_sha", None),
        ("last_tock_sha", {"sha": _REFERENCE_LAST_TOCK_SHA}),
        ("observed_main_head_sha", b"not-text"),
        ("reason_code", True),
        ("updated_at_utc", 16),
    ),
)
def test_fresh_schema_rejects_typed_field_errors(
    tmp_path: Path,
    field: str,
    value: Any,
) -> None:
    path = _main_state_path(tmp_path)
    _write_yaml(path, _fresh_main_state(**{field: value}))

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(main_state_path=path),
    )


@pytest.mark.parametrize(
    "payload_override",
    (
        {"schema_version": "2.0"},
        {"state": "GREEN_PROMOTED"},
        {"state": "EXPECTED_RED_CLOSED"},
        {"main_branch": ""},
        {"validated_result_sha": ""},
        {"last_tock_sha": " "},
        {"observed_main_head_sha": ""},
        {"reason_code": ""},
        {"updated_at_utc": ""},
    ),
)
def test_fresh_schema_rejects_invalid_values_and_empty_truth(
    tmp_path: Path,
    payload_override: dict[str, Any],
) -> None:
    path = _main_state_path(tmp_path)
    _write_yaml(path, _fresh_main_state(**payload_override))

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(main_state_path=path),
    )


def test_load_rejects_missing_malformed_and_non_mapping_main_state(
    tmp_path: Path,
) -> None:
    missing = _main_state_path(tmp_path)
    malformed = tmp_path / "malformed.yaml"
    non_mapping = tmp_path / "list.yaml"
    malformed.write_text("broken: [\n", encoding="utf-8")
    _write_yaml(non_mapping, [_fresh_main_state()])

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(main_state_path=missing),
    )
    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(main_state_path=malformed),
    )
    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(main_state_path=non_mapping),
    )


def test_write_main_state_persists_and_reloads_the_same_fresh_payload(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    payload = _fresh_main_state(state="EXPECTED_RED_OPEN")

    written = write_main_state(main_state_path=path, payload=payload)

    assert written == payload
    assert yaml.safe_load(path.read_text(encoding="utf-8")) == payload
    assert load_and_validate_main_state(main_state_path=path) == payload
    assert not list(path.parent.glob("*.tmp"))


def test_write_main_state_rejects_invalid_payload_without_creating_the_file(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    invalid = _fresh_main_state(state="UNKNOWN_STATE")

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: write_main_state(main_state_path=path, payload=invalid),
    )
    assert not path.exists()
    assert not list(tmp_path.rglob("*.tmp"))


def test_observed_head_update_is_monotone_and_keeps_tock_and_result_truth(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    before = _fresh_main_state()
    _write_yaml(path, before)

    after = update_observed_main_head_sha(
        main_state_path=path,
        observed_main_head_sha=_POST_TICK_OBSERVED_HEAD_SHA,
        expected_last_tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )

    assert after == {
        **before,
        "observed_main_head_sha": _POST_TICK_OBSERVED_HEAD_SHA,
    }
    assert after["last_tock_sha"] == _REFERENCE_LAST_TOCK_SHA
    assert after["validated_result_sha"] == _REFERENCE_VALIDATED_RESULT_SHA
    assert yaml.safe_load(path.read_text(encoding="utf-8")) == after


def test_identical_observed_head_reproof_does_not_rewrite_canonical_bytes(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    _write_yaml(path, _fresh_main_state())
    first = update_observed_main_head_sha(
        main_state_path=path,
        observed_main_head_sha=_POST_TICK_OBSERVED_HEAD_SHA,
        expected_last_tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )
    first_bytes = path.read_bytes()

    second = update_observed_main_head_sha(
        main_state_path=path,
        observed_main_head_sha=_POST_TICK_OBSERVED_HEAD_SHA,
        expected_last_tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )

    assert second == first
    assert path.read_bytes() == first_bytes


@pytest.mark.parametrize(
    ("observed", "expected_tock", "reason_code"),
    (
        (_POST_TICK_OBSERVED_HEAD_SHA, _FOREIGN_TOCK_SHA, "MAIN_STATE_DRIFT"),
        ("", _REFERENCE_LAST_TOCK_SHA, "MAIN_STATE_INVALID"),
        ("   ", _REFERENCE_LAST_TOCK_SHA, "MAIN_STATE_INVALID"),
    ),
)
def test_observed_head_update_rejects_tock_drift_and_empty_observation(
    tmp_path: Path,
    observed: str,
    expected_tock: str,
    reason_code: str,
) -> None:
    path = _main_state_path(tmp_path)
    before = _fresh_main_state()
    _write_yaml(path, before)
    before_bytes = path.read_bytes()

    _expect_state_error(
        reason_code,
        lambda: update_observed_main_head_sha(
            main_state_path=path,
            observed_main_head_sha=observed,
            expected_last_tock_sha=expected_tock,
        ),
    )
    assert path.read_bytes() == before_bytes


def test_start_tick_cas_updates_only_observed_head_and_is_idempotent(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    before = _fresh_main_state()
    _write_yaml(path, before)

    first = transition_main_state_for_start_tick(
        main_state_path=path,
        expected_base_sha=_REFERENCE_OBSERVED_HEAD_SHA,
        tick_sha=_REFERENCE_START_TICK_SHA,
    )
    first_bytes = path.read_bytes()
    current_tick_head = _REFERENCE_START_TICK_SHA
    second = transition_main_state_for_start_tick(
        main_state_path=path,
        expected_base_sha=current_tick_head,
        tick_sha=_REFERENCE_START_TICK_SHA,
    )

    expected = {
        **before,
        "observed_main_head_sha": _REFERENCE_START_TICK_SHA,
    }
    assert first == expected
    assert second == expected
    assert path.read_bytes() == first_bytes
    assert first["validated_result_sha"] == before["validated_result_sha"]
    assert first["last_tock_sha"] == before["last_tock_sha"]
    assert first["state"] == before["state"]


@pytest.mark.parametrize(
    ("payload", "expected_base_sha", "tick_sha", "reason_code"),
    (
        (
            _fresh_main_state(observed_main_head_sha=_FOREIGN_HEAD_SHA),
            _REFERENCE_OBSERVED_HEAD_SHA,
            _REFERENCE_START_TICK_SHA,
            "MAIN_STATE_DRIFT",
        ),
        (
            _fresh_main_state(),
            "",
            _REFERENCE_START_TICK_SHA,
            "MAIN_STATE_INVALID",
        ),
        (
            _fresh_main_state(),
            _REFERENCE_OBSERVED_HEAD_SHA,
            "",
            "MAIN_STATE_INVALID",
        ),
        (
            _fresh_main_state(validated_result_sha=""),
            _REFERENCE_OBSERVED_HEAD_SHA,
            _REFERENCE_START_TICK_SHA,
            "MAIN_STATE_INVALID",
        ),
    ),
)
def test_start_tick_cas_rejects_identity_drift_and_invalid_sha_without_write(
    tmp_path: Path,
    payload: dict[str, Any],
    expected_base_sha: str,
    tick_sha: str,
    reason_code: str,
) -> None:
    path = _main_state_path(tmp_path)
    _write_yaml(path, payload)
    before = path.read_bytes()

    _expect_state_error(
        reason_code,
        lambda: transition_main_state_for_start_tick(
            main_state_path=path,
            expected_base_sha=expected_base_sha,
            tick_sha=tick_sha,
        ),
    )
    assert path.read_bytes() == before


def test_start_tick_cas_persists_closed_mapping_without_protocol_comments(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    before = _fresh_main_state()
    _write_yaml(path, before)

    written = transition_main_state_for_start_tick(
        main_state_path=path,
        expected_base_sha=_REFERENCE_OBSERVED_HEAD_SHA,
        tick_sha=_REFERENCE_START_TICK_SHA,
    )

    persisted_text = path.read_text(encoding="utf-8")
    loaded = yaml.safe_load(persisted_text)
    expected = {
        **before,
        "observed_main_head_sha": _REFERENCE_START_TICK_SHA,
    }

    assert written == expected
    assert loaded == expected
    assert list(loaded) == list(_REQUIRED_MAIN_STATE_FIELDS)
    assert "start_tick_cas_expected_base_sha" not in persisted_text
    assert not any(
        line.lstrip().startswith("#") for line in persisted_text.splitlines()
    )
    assert sorted(p.name for p in path.parent.iterdir()) == [path.name]


def test_start_tick_write_failure_preserves_previous_truth_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _main_state_path(tmp_path)
    _write_yaml(path, _fresh_main_state())
    before = path.read_bytes()

    monkeypatch.setattr(os, "replace", _fail_replace)
    _expect_state_error(
        "MAIN_STATE_WRITE_FAILED",
        lambda: transition_main_state_for_start_tick(
            main_state_path=path,
            expected_base_sha=_REFERENCE_OBSERVED_HEAD_SHA,
            tick_sha=_REFERENCE_START_TICK_SHA,
        ),
    )

    assert path.read_bytes() == before
    assert not list(path.parent.glob("*.tmp"))
    assert not list(tmp_path.glob("*.tmp"))


def test_missing_consumption_ledger_is_empty_and_does_not_create_a_file(
    tmp_path: Path,
) -> None:
    path = _consumed_path(tmp_path)

    loaded = load_promotion_consumed(promotion_consumed_path=path)

    assert loaded == {"schema_version": _REFERENCE_SCHEMA_VERSION, "consumed_promotions": []}
    assert not path.exists()
    assert is_promotion_consumed(
        consumed_payload=loaded,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
    ) is False


def test_consumption_record_binds_attempt_result_tock_and_pre_state(
    tmp_path: Path,
) -> None:
    path = _consumed_path(tmp_path)
    pre_state = _fresh_main_state()

    first = record_promotion_consumed(
        promotion_consumed_path=path,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_REFERENCE_CONSUMED_AT,
        tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )

    assert first == {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "consumed_promotions": [
            {
                "promotion_attempt_id": _REFERENCE_ATTEMPT_ID,
                "result_sha": _REFERENCE_VALIDATED_RESULT_SHA,
                "main_state_before_promotion": pre_state,
                "consumed_at_utc": _REFERENCE_CONSUMED_AT,
                "tock_sha": _REFERENCE_LAST_TOCK_SHA,
            }
        ],
    }
    assert load_promotion_consumed(promotion_consumed_path=path) == first
    assert is_promotion_consumed(
        consumed_payload=first,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
    ) is True
    assert is_promotion_consumed(
        consumed_payload=first,
        promotion_attempt_id=_FOREIGN_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
    ) is False
    assert is_promotion_consumed(
        consumed_payload=first,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_FOREIGN_RESULT_SHA,
    ) is False


def test_identical_consumption_reproof_is_stable_and_does_not_rewrite_bytes(
    tmp_path: Path,
) -> None:
    path = _consumed_path(tmp_path)
    pre_state = _fresh_main_state()
    first = record_promotion_consumed(
        promotion_consumed_path=path,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_REFERENCE_CONSUMED_AT,
        tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )
    first_bytes = path.read_bytes()

    second = record_promotion_consumed(
        promotion_consumed_path=path,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
        main_state_before_promotion=pre_state,
        consumed_at_utc="2026-09-03T16:09:00Z",
        tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )

    assert second == first
    assert path.read_bytes() == first_bytes
    assert len(second["consumed_promotions"]) == 1


@pytest.mark.parametrize(
    ("attempt_id", "result_sha", "tock_sha"),
    (
        (_REFERENCE_ATTEMPT_ID, _FOREIGN_RESULT_SHA, _REFERENCE_LAST_TOCK_SHA),
        (_FOREIGN_ATTEMPT_ID, _REFERENCE_VALIDATED_RESULT_SHA, _REFERENCE_LAST_TOCK_SHA),
        (_REFERENCE_ATTEMPT_ID, _REFERENCE_VALIDATED_RESULT_SHA, _FOREIGN_TOCK_SHA),
    ),
)
def test_conflicting_consumption_reuse_is_rejected_without_rewrite(
    tmp_path: Path,
    attempt_id: str,
    result_sha: str,
    tock_sha: str,
) -> None:
    path = _consumed_path(tmp_path)
    pre_state = _fresh_main_state()
    record_promotion_consumed(
        promotion_consumed_path=path,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_REFERENCE_CONSUMED_AT,
        tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )
    before = path.read_bytes()

    _expect_state_error(
        "PROMOTION_CONSUMPTION_CONFLICT",
        lambda: record_promotion_consumed(
            promotion_consumed_path=path,
            promotion_attempt_id=attempt_id,
            result_sha=result_sha,
            main_state_before_promotion=pre_state,
            consumed_at_utc="2026-09-03T16:11:00Z",
            tock_sha=tock_sha,
        ),
    )
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    ("attempt_id", "result_sha", "tock_sha"),
    (
        (_REFERENCE_ATTEMPT_ID, _FOREIGN_RESULT_SHA, _FOREIGN_TOCK_SHA),
        (_FOREIGN_ATTEMPT_ID, _REFERENCE_VALIDATED_RESULT_SHA, _FOREIGN_TOCK_SHA),
        (_FOREIGN_ATTEMPT_ID, _FOREIGN_RESULT_SHA, _REFERENCE_LAST_TOCK_SHA),
    ),
)
def test_single_axis_consumption_identity_reuse_is_conflict_without_rewrite(
    tmp_path: Path,
    attempt_id: str,
    result_sha: str,
    tock_sha: str,
) -> None:
    path = _consumed_path(tmp_path)
    pre_state = _fresh_main_state()
    record_promotion_consumed(
        promotion_consumed_path=path,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_REFERENCE_CONSUMED_AT,
        tock_sha=_REFERENCE_LAST_TOCK_SHA,
    )
    before = path.read_bytes()

    _expect_state_error(
        "PROMOTION_CONSUMPTION_CONFLICT",
        lambda: record_promotion_consumed(
            promotion_consumed_path=path,
            promotion_attempt_id=attempt_id,
            result_sha=result_sha,
            main_state_before_promotion=pre_state,
            consumed_at_utc="2026-09-03T16:11:00Z",
            tock_sha=tock_sha,
        ),
    )
    assert path.read_bytes() == before


def test_consumption_rejects_empty_tock_without_creating_a_partial_ledger(
    tmp_path: Path,
) -> None:
    path = _consumed_path(tmp_path)

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: record_promotion_consumed(
            promotion_consumed_path=path,
            promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
            result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
            main_state_before_promotion=_fresh_main_state(),
            tock_sha="",
        ),
    )
    assert not path.exists()
    assert not list(path.parent.glob("*.tmp")) if path.parent.exists() else True


def test_load_promotion_consumed_rejects_malformed_and_non_mapping_ledgers(
    tmp_path: Path,
) -> None:
    malformed = tmp_path / "malformed_consumed.yaml"
    non_mapping = tmp_path / "list_consumed.yaml"
    malformed.write_text("broken: [\n", encoding="utf-8")
    _write_yaml(non_mapping, {"schema_version": "1.0", "consumed_promotions": {}})

    _expect_state_error(
        "PROMOTION_CONSUMPTION_INVALID",
        lambda: load_promotion_consumed(promotion_consumed_path=malformed),
    )
    _expect_state_error(
        "PROMOTION_CONSUMPTION_INVALID",
        lambda: load_promotion_consumed(promotion_consumed_path=non_mapping),
    )


def test_second_foreign_consumption_appends_and_keeps_both_entries(
    tmp_path: Path,
) -> None:
    path = _consumed_path(tmp_path)
    first_pre = _fresh_main_state()
    second_pre = _fresh_main_state(
        observed_main_head_sha=_POST_TICK_OBSERVED_HEAD_SHA,
        reason_code="STATE_READY_SYNTHETIC_BETA",
    )
    first = _record_consumed(path, pre_state=first_pre)
    second = _record_consumed(
        path,
        attempt_id=_FOREIGN_ATTEMPT_ID,
        result_sha=_FOREIGN_RESULT_SHA,
        pre_state=second_pre,
        consumed_at_utc=_FOREIGN_CONSUMED_AT,
        tock_sha=_FOREIGN_TOCK_SHA,
    )

    assert second == _consumption_ledger(
        _consumption_entry(pre_state=first_pre),
        _consumption_entry(
            attempt_id=_FOREIGN_ATTEMPT_ID,
            result_sha=_FOREIGN_RESULT_SHA,
            pre_state=second_pre,
            consumed_at_utc=_FOREIGN_CONSUMED_AT,
            tock_sha=_FOREIGN_TOCK_SHA,
        ),
    )
    assert first["consumed_promotions"][0] == second["consumed_promotions"][0]
    assert load_promotion_consumed(promotion_consumed_path=path) == second
    assert is_promotion_consumed(
        consumed_payload=second,
        promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
        result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
    ) is True
    assert is_promotion_consumed(
        consumed_payload=second,
        promotion_attempt_id=_FOREIGN_ATTEMPT_ID,
        result_sha=_FOREIGN_RESULT_SHA,
    ) is True


def test_same_identity_reproof_with_shifted_pre_state_is_conflict_without_rewrite(
    tmp_path: Path,
) -> None:
    path = _consumed_path(tmp_path)
    original_pre = _fresh_main_state()
    shifted_pre = _fresh_main_state(
        observed_main_head_sha=_POST_TICK_OBSERVED_HEAD_SHA,
    )
    _record_consumed(path, pre_state=original_pre)
    before = path.read_bytes()

    _expect_state_error(
        "PROMOTION_CONSUMPTION_CONFLICT",
        lambda: _record_consumed(path, pre_state=shifted_pre),
    )
    assert path.read_bytes() == before
    assert load_promotion_consumed(promotion_consumed_path=path) == _consumption_ledger(
        _consumption_entry(pre_state=original_pre),
    )


def test_start_tick_already_on_tick_rejects_foreign_expected_base_without_write(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    payload = _fresh_main_state(observed_main_head_sha=_REFERENCE_START_TICK_SHA)
    _write_yaml(path, payload)
    before = path.read_bytes()

    _expect_state_error(
        "MAIN_STATE_DRIFT",
        lambda: transition_main_state_for_start_tick(
            main_state_path=path,
            expected_base_sha=_FOREIGN_HEAD_SHA,
            tick_sha=_REFERENCE_START_TICK_SHA,
        ),
    )
    assert path.read_bytes() == before


def test_start_tick_already_on_tick_without_prior_cas_is_stable_noop_for_current_head(
    tmp_path: Path,
) -> None:
    path = _main_state_path(tmp_path)
    payload = _fresh_main_state(observed_main_head_sha=_REFERENCE_START_TICK_SHA)
    _write_yaml(path, payload)
    before = path.read_bytes()

    result = transition_main_state_for_start_tick(
        main_state_path=path,
        expected_base_sha=_REFERENCE_START_TICK_SHA,
        tick_sha=_REFERENCE_START_TICK_SHA,
    )

    assert result == payload
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "kind",
    ("write_main_state", "update_observed_main_head_sha", "record_promotion_consumed"),
)
def test_atomic_replace_failure_preserves_truth_for_remaining_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    if kind == "write_main_state":
        path = _main_state_path(tmp_path)
        _write_yaml(path, _fresh_main_state())

        def action() -> Any:
            return write_main_state(
                main_state_path=path,
                payload=_fresh_main_state(reason_code="STATE_READY_SYNTHETIC_BETA"),
            )
    elif kind == "update_observed_main_head_sha":
        path = _main_state_path(tmp_path)
        _write_yaml(path, _fresh_main_state())

        def action() -> Any:
            return update_observed_main_head_sha(
                main_state_path=path,
                observed_main_head_sha=_POST_TICK_OBSERVED_HEAD_SHA,
                expected_last_tock_sha=_REFERENCE_LAST_TOCK_SHA,
            )
    else:
        path = _consumed_path(tmp_path)
        _write_yaml(path, _consumption_ledger(_consumption_entry()))

        def action() -> Any:
            return _record_consumed(
                path,
                attempt_id=_FOREIGN_ATTEMPT_ID,
                result_sha=_FOREIGN_RESULT_SHA,
                tock_sha=_FOREIGN_TOCK_SHA,
                consumed_at_utc=_FOREIGN_CONSUMED_AT,
            )

    before = path.read_bytes()
    monkeypatch.setattr(os, "replace", _fail_replace)
    _expect_state_error("MAIN_STATE_WRITE_FAILED", action)
    assert path.read_bytes() == before
    assert not list(path.parent.glob("*.tmp"))
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    ("override", "reason_code"),
    (
        ({"promotion_attempt_id": ""}, "MAIN_STATE_INVALID"),
        ({"promotion_attempt_id": "  "}, "MAIN_STATE_INVALID"),
        ({"promotion_attempt_id": 1}, "MAIN_STATE_INVALID"),
        ({"result_sha": ""}, "MAIN_STATE_INVALID"),
        ({"result_sha": None}, "MAIN_STATE_INVALID"),
        ({"tock_sha": None}, "MAIN_STATE_INVALID"),
        ({"main_state_before_promotion": []}, "MAIN_STATE_INVALID"),
        (
            {"main_state_before_promotion": _fresh_main_state(state="UNKNOWN_STATE")},
            "MAIN_STATE_INVALID",
        ),
        (
            {
                "main_state_before_promotion": {
                    key: value
                    for key, value in _fresh_main_state().items()
                    if key != "reason_code"
                }
            },
            "MAIN_STATE_INVALID",
        ),
        (
            {
                "main_state_before_promotion": {
                    **_fresh_main_state(),
                    "orphan_runtime_field": "not-fresh",
                }
            },
            "MAIN_STATE_FIELD_UNKNOWN",
        ),
    ),
)
def test_consumption_record_rejects_empty_typed_and_invalid_pre_state(
    tmp_path: Path,
    override: dict[str, Any],
    reason_code: str,
) -> None:
    path = _consumed_path(tmp_path)
    kwargs: dict[str, Any] = {
        "promotion_consumed_path": path,
        "promotion_attempt_id": _REFERENCE_ATTEMPT_ID,
        "result_sha": _REFERENCE_VALIDATED_RESULT_SHA,
        "main_state_before_promotion": _fresh_main_state(),
        "consumed_at_utc": _REFERENCE_CONSUMED_AT,
        "tock_sha": _REFERENCE_LAST_TOCK_SHA,
    }
    kwargs.update(override)

    _expect_state_error(
        reason_code,
        lambda: record_promotion_consumed(**kwargs),
    )
    assert not path.exists()


@pytest.mark.parametrize(
    "consumed_at_utc",
    (
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace"),
        pytest.param(None, id="none"),
        pytest.param(16, id="typed-int"),
    ),
)
def test_consumption_record_rejects_missing_empty_and_typed_consumed_at_utc(
    tmp_path: Path,
    consumed_at_utc: Any,
) -> None:
    path = _consumed_path(tmp_path)

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: record_promotion_consumed(
            promotion_consumed_path=path,
            promotion_attempt_id=_REFERENCE_ATTEMPT_ID,
            result_sha=_REFERENCE_VALIDATED_RESULT_SHA,
            main_state_before_promotion=_fresh_main_state(),
            consumed_at_utc=consumed_at_utc,
            tock_sha=_REFERENCE_LAST_TOCK_SHA,
        ),
    )
    assert not path.exists()


@pytest.mark.parametrize(
    ("entry_override", "reason_code"),
    (
        ({"promotion_attempt_id": ""}, "MAIN_STATE_INVALID"),
        ({"promotion_attempt_id": "  "}, "MAIN_STATE_INVALID"),
        ({"promotion_attempt_id": 1}, "MAIN_STATE_INVALID"),
        ({"_pop": ("promotion_attempt_id",)}, "MAIN_STATE_INVALID"),
        ({"result_sha": ""}, "MAIN_STATE_INVALID"),
        ({"result_sha": None}, "MAIN_STATE_INVALID"),
        ({"_pop": ("result_sha",)}, "MAIN_STATE_INVALID"),
        ({"tock_sha": ""}, "MAIN_STATE_INVALID"),
        ({"tock_sha": None}, "MAIN_STATE_INVALID"),
        ({"tock_sha": 1}, "MAIN_STATE_INVALID"),
        ({"_pop": ("tock_sha",)}, "MAIN_STATE_INVALID"),
        ({"main_state_before_promotion": []}, "MAIN_STATE_INVALID"),
        (
            {"main_state_before_promotion": _fresh_main_state(state="UNKNOWN_STATE")},
            "MAIN_STATE_INVALID",
        ),
        (
            {
                "main_state_before_promotion": {
                    key: value
                    for key, value in _fresh_main_state().items()
                    if key != "reason_code"
                }
            },
            "MAIN_STATE_INVALID",
        ),
        ({"_pop": ("main_state_before_promotion",)}, "MAIN_STATE_INVALID"),
        (
            {
                "main_state_before_promotion": {
                    **_fresh_main_state(),
                    "orphan_runtime_field": "not-fresh",
                }
            },
            "MAIN_STATE_FIELD_UNKNOWN",
        ),
    ),
)
def test_load_promotion_consumed_rejects_persisted_identity_and_pre_state_faults(
    tmp_path: Path,
    entry_override: dict[str, Any],
    reason_code: str,
) -> None:
    mutation = dict(entry_override)
    pop_fields = tuple(mutation.pop("_pop", ()))
    entry = _consumption_entry()
    entry.update(mutation)
    for field in pop_fields:
        entry.pop(field)

    path = _consumed_path(tmp_path)
    _write_yaml(path, _consumption_ledger(entry))
    before = path.read_bytes()

    _expect_state_error(
        reason_code,
        lambda: load_promotion_consumed(promotion_consumed_path=path),
    )
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "entry_override",
    (
        pytest.param({"consumed_at_utc": ""}, id="empty"),
        pytest.param({"consumed_at_utc": "   "}, id="whitespace"),
        pytest.param({"consumed_at_utc": None}, id="none"),
        pytest.param({"consumed_at_utc": 16}, id="typed-int"),
        pytest.param({"_pop": ("consumed_at_utc",)}, id="missing"),
    ),
)
def test_load_promotion_consumed_rejects_missing_empty_and_typed_consumed_at_utc(
    tmp_path: Path,
    entry_override: dict[str, Any],
) -> None:
    mutation = dict(entry_override)
    pop_fields = tuple(mutation.pop("_pop", ()))
    entry = _consumption_entry()
    entry.update(mutation)
    for field in pop_fields:
        entry.pop(field)

    path = _consumed_path(tmp_path)
    _write_yaml(path, _consumption_ledger(entry))
    before = path.read_bytes()

    _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_promotion_consumed(promotion_consumed_path=path),
    )
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    ("payload", "reason_code"),
    (
        (
            {"schema_version": "2.0", "consumed_promotions": [_consumption_entry()]},
            "PROMOTION_CONSUMPTION_INVALID",
        ),
        (
            {
                "schema_version": _REFERENCE_SCHEMA_VERSION,
                "consumed_promotions": [_consumption_entry()],
                "orphan_runtime_field": "not-fresh",
            },
            "PROMOTION_CONSUMPTION_FIELD_UNKNOWN",
        ),
        (
            {
                "schema_version": _REFERENCE_SCHEMA_VERSION,
                "consumed_promotions": [
                    {**_consumption_entry(), "orphan_entry_field": "not-fresh"}
                ],
            },
            "PROMOTION_CONSUMPTION_FIELD_UNKNOWN",
        ),
        (
            {
                "schema_version": _REFERENCE_SCHEMA_VERSION,
                "consumed_promotions": ["not-a-mapping"],
            },
            "PROMOTION_CONSUMPTION_INVALID",
        ),
        (
            {
                "schema_version": _REFERENCE_SCHEMA_VERSION,
                "consumed_promotions": [None],
            },
            "PROMOTION_CONSUMPTION_INVALID",
        ),
    ),
)
def test_consumption_ledger_rejects_schema_unknown_fields_and_non_mapping_entries(
    tmp_path: Path,
    payload: dict[str, Any],
    reason_code: str,
) -> None:
    path = _consumed_path(tmp_path)
    _write_yaml(path, payload)
    before = path.read_bytes()

    _expect_state_error(
        reason_code,
        lambda: load_promotion_consumed(promotion_consumed_path=path),
    )
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    ("kind", "reason_code"),
    (
        ("main_state", "MAIN_STATE_VALUE_CONFLICT"),
        ("consumed", "PROMOTION_CONSUMPTION_VALUE_CONFLICT"),
    ),
)
def test_duplicate_yaml_keys_are_blocked(
    tmp_path: Path,
    kind: str,
    reason_code: str,
) -> None:
    if kind == "main_state":
        path = _main_state_path(tmp_path)
        _write_yaml(path, _fresh_main_state())

        def load() -> Any:
            return load_and_validate_main_state(main_state_path=path)
    else:
        path = _consumed_path(tmp_path)
        _write_yaml(path, _consumption_ledger(_consumption_entry()))

        def load() -> Any:
            return load_promotion_consumed(promotion_consumed_path=path)

    path.write_text(
        'schema_version: "1.0"\n' + path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _expect_state_error(reason_code, load)


@pytest.mark.parametrize(
    ("kind", "reason_code"),
    (
        ("main_state", "MAIN_STATE_UNREADABLE"),
        ("consumed", "PROMOTION_CONSUMPTION_UNREADABLE"),
    ),
)
def test_unreadable_state_and_ledger_files_are_blocked(
    tmp_path: Path,
    kind: str,
    reason_code: str,
) -> None:
    if kind == "main_state":
        path = _main_state_path(tmp_path)
        _write_yaml(path, _fresh_main_state())

        def load() -> Any:
            return load_and_validate_main_state(main_state_path=path)
    else:
        path = _consumed_path(tmp_path)
        _write_yaml(path, _consumption_ledger(_consumption_entry()))

        def load() -> Any:
            return load_promotion_consumed(promotion_consumed_path=path)

    original_mode = path.stat().st_mode
    try:
        _make_unreadable(path)
        _expect_state_error(reason_code, load)
    finally:
        if path.is_file():
            path.chmod(original_mode)


def test_error_exposes_stable_reason_code(tmp_path: Path) -> None:
    error = _expect_state_error(
        "MAIN_STATE_INVALID",
        lambda: load_and_validate_main_state(
            main_state_path=tmp_path / "missing.yaml",
        ),
    )
    assert "MAIN_STATE_INVALID" in str(error)
