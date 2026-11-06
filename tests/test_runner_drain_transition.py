"""Synthetic public ordinary drain-before-dispatch contract.

Keep:
- drain existing ``pending_promotion.yaml`` before dispatch
- hand that intent to the real step-16 entry
  ``admit_pending_promotion_effect``
- persist a stable ``drain_outcome.yaml`` and identical terminal replay
  without rewriting canonical bytes
- no-pending as a neutral no-op: no Git effect, TICK, TOCK, journal,
  consumption, or wake-up marker
- unresolved or inconsistent scheduler state fail-closes
"""

from __future__ import annotations

import hashlib
import inspect
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.runner_git_promotion as git_promotion

import task_transition_runner.runner_drain_transition as drain_module
import test_runner_git_promotion as pending_tests
from task_transition_runner.runner_drain_transition import (
    DrainDecision,
    DrainOutcome,
    DrainOutcomeError,
    DrainStatus,
    SchedulerPhase,
    evaluate_drain_before_dispatch,
    load_identity_marker,
    load_persisted_drain_outcome,
    persist_drain_outcome,
    write_identity_marker,
)
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    admit_pending_promotion_effect,
    load_pending_promotion,
)
from task_transition_runner.runner_promotion_journal import (
    PromotionJournal,
    PromotionJournalPhase,
    create_promotion_journal,
    load_promotion_journal,
    persist_promotion_journal,
    transition_promotion_journal,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_DRAIN_RUN_ID = "drain-run-synthetic-001"
_REPLAY_DRAIN_RUN_ID = "drain-run-synthetic-replay"
_REFERENCE_HEAD_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_REFERENCE_TOCK_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_CLAIMED_TOCK_SHA = "dddddddddddddddddddddddddddddddddddddddd"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_RESULT_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_REFERENCE_PROMOTION_CLASS = "GREEN_PROMOTION"
_WAKEUP_MARKER_TYPE = "wakeup"
_CANONICAL_PENDING_NAME = "pending_promotion.yaml"
_CANONICAL_OUTCOME_NAME = "drain_outcome.yaml"
_EFFECT_ENTRY_NAME = "admit_pending_promotion_effect"
_PUBLIC_MARKER_TYPE = "wakeup"
_REQUIRED_EXPORTS = (
    "DrainDecision",
    "DrainOutcome",
    "DrainOutcomeError",
    "DrainStatus",
    "SchedulerPhase",
    "evaluate_drain_before_dispatch",
    "load_identity_marker",
    "load_persisted_drain_outcome",
    "persist_drain_outcome",
    "write_identity_marker",
)
_EVALUATE_REQUIRED_PARAMETERS = (
    "repo_root",
    "runtime_root",
    "drain_run_id",
)
_EVALUATE_OPTIONAL_PARAMETERS = (
    "effect_admission",
    "main_head_reader",
)
_EFFECT_PATH_NAMES = (
    "promotion_journal.yaml",
    "promotion_consumed.yaml",
)
_ORDINARY_STATUSES = (
    DrainStatus.NO_PENDING,
    DrainStatus.DRAINED,
    DrainStatus.BLOCKED,
)
_INTENT = {
    "task_id": _REFERENCE_TASK_ID,
    "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
    "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
    "result_sha": _REFERENCE_RESULT_SHA,
    "promotion_class": _REFERENCE_PROMOTION_CLASS,
}


def _drained_outcome(
    *,
    drain_run_id: str = _REFERENCE_DRAIN_RUN_ID,
    observed_main_head_sha: str = _REFERENCE_TOCK_SHA,
    tock_sha: str = _REFERENCE_TOCK_SHA,
    reason_code: str = "PROMOTION_EFFECT_ADMITTED",
) -> DrainOutcome:
    return DrainOutcome(
        status=DrainStatus.DRAINED,
        drain_run_id=drain_run_id,
        observed_main_head_sha=observed_main_head_sha,
        reason_code=reason_code,
        tock_sha=tock_sha,
        **_INTENT,
    )


def _no_pending_outcome(
    *,
    drain_run_id: str = _REFERENCE_DRAIN_RUN_ID,
    observed_main_head_sha: str = _REFERENCE_HEAD_SHA,
) -> DrainOutcome:
    return DrainOutcome(
        status=DrainStatus.NO_PENDING,
        drain_run_id=drain_run_id,
        observed_main_head_sha=observed_main_head_sha,
        reason_code="NO_PENDING_PROMOTION",
    )


def _pin_outcome_bytes(runtime: Path, outcome: DrainOutcome) -> tuple[Path, bytes, int, str]:
    persist_drain_outcome(runtime_root=runtime, outcome=outcome)
    path = runtime / _CANONICAL_OUTCOME_NAME
    path.chmod(0o444)
    pinned = path.read_bytes()
    return (
        path,
        pinned,
        stat.S_IMODE(path.stat().st_mode),
        hashlib.sha256(pinned).hexdigest(),
    )


def _assert_no_wakeup_marker(runtime: Path) -> None:
    assert (runtime / "wakeup.yaml").exists() is False
    assert list(runtime.rglob("wakeup.yaml")) == []


def _assert_no_effect_or_tick_files(runtime: Path) -> None:
    for name in _EFFECT_PATH_NAMES:
        assert (runtime / name).exists() is False
    leftover = [
        path.name
        for path in runtime.rglob("*")
        if path.is_file()
        and (
            path.name.endswith(".tmp")
            or path.name.endswith(".temp")
            or "tick" in path.name.lower()
        )
    ]
    assert leftover == []


def _evaluate(
    *,
    repo: Path,
    runtime: Path,
    drain_run_id: str,
    effect_admission: Mapping[str, Any] | None = None,
    main_head_reader: Any | None = None,
) -> DrainDecision:
    kwargs: dict[str, Any] = {
        "repo_root": repo,
        "runtime_root": runtime,
        "drain_run_id": drain_run_id,
    }
    if effect_admission is not None:
        kwargs["effect_admission"] = effect_admission
    if main_head_reader is not None:
        kwargs["main_head_reader"] = main_head_reader
    return evaluate_drain_before_dispatch(**kwargs)


_RAW_STEP_16_EXCEPTIONS = (
    RuntimeError,
    TypeError,
    AttributeError,
    ValueError,
)


def _patch_step_16(monkeypatch: pytest.MonkeyPatch, handler: Any) -> None:
    monkeypatch.setattr(
        git_promotion,
        "admit_pending_promotion_effect",
        handler,
    )
    monkeypatch.setattr(
        drain_module,
        "admit_pending_promotion_effect",
        handler,
        raising=False,
    )


def _evaluate_fail_closed(**kwargs: Any) -> DrainDecision:
    try:
        return _evaluate(**kwargs)
    except _RAW_STEP_16_EXCEPTIONS as exc:
        raise AssertionError(
            f"raw {type(exc).__name__} leaked past evaluate_drain_before_dispatch"
        ) from exc


def _assert_idle_pending_world(
    world: Mapping[str, Any],
    git_before: Mapping[str, str],
) -> None:
    runtime = world["runtime"]
    pending = runtime / _CANONICAL_PENDING_NAME
    assert pending.is_file()
    assert pending.read_bytes() == world["pending_before"]
    assert (runtime / "promotion_consumed.yaml").exists() is False
    pending_tests._assert_git_unchanged(world["repo"], git_before)
    _assert_no_wakeup_marker(runtime)
    leftover = [
        path.name
        for path in runtime.rglob("*")
        if path.is_file()
        and (path.name.endswith(".tmp") or path.name.endswith(".temp"))
    ]
    assert leftover == []


def test_public_exports_are_the_ordinary_drain_scheduler_surface() -> None:
    for name in _REQUIRED_EXPORTS:
        assert hasattr(drain_module, name)
    assert DrainStatus.NO_PENDING.value == "NO_PENDING"
    assert DrainStatus.DRAINED.value == "DRAINED"
    assert DrainStatus.BLOCKED.value == "BLOCKED"
    assert SchedulerPhase.DRAINED_AWAITING_TICK.value == "DRAINED_AWAITING_TICK"
    assert SchedulerPhase.DISPATCH_READY.value == "DISPATCH_READY"
    assert SchedulerPhase.BLOCKED.value == "BLOCKED"
    assert _ORDINARY_STATUSES == (
        DrainStatus.NO_PENDING,
        DrainStatus.DRAINED,
        DrainStatus.BLOCKED,
    )


def test_evaluate_drain_before_dispatch_is_keyword_only_and_hands_off_to_step_16() -> None:
    signature = inspect.signature(evaluate_drain_before_dispatch)
    assert list(signature.parameters)[:3] == list(_EVALUATE_REQUIRED_PARAMETERS)
    for name in _EVALUATE_REQUIRED_PARAMETERS:
        assert name in signature.parameters
    for name in _EVALUATE_OPTIONAL_PARAMETERS:
        assert name in signature.parameters
    for parameter in signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
    source = inspect.getsource(drain_module)
    assert _EFFECT_ENTRY_NAME in source
    assert "runner_git_promotion" in source
    admit_signature = inspect.signature(admit_pending_promotion_effect)
    assert list(admit_signature.parameters) == list(
        pending_tests._EFFECT_REQUIRED_PARAMETERS
    )


def test_drain_outcome_requires_identity_and_tock_matrix() -> None:
    no_pending = _no_pending_outcome()
    drained = _drained_outcome()
    assert no_pending.to_mapping() == {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "status": "NO_PENDING",
        "drain_run_id": _REFERENCE_DRAIN_RUN_ID,
        "observed_main_head_sha": _REFERENCE_HEAD_SHA,
        "reason_code": "NO_PENDING_PROMOTION",
    }
    assert DrainOutcome.from_mapping(drained.to_mapping()) == drained
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_INTENT_INCOMPLETE"):
        DrainOutcome(
            status=DrainStatus.BLOCKED,
            drain_run_id=_REFERENCE_DRAIN_RUN_ID,
            observed_main_head_sha=_REFERENCE_HEAD_SHA,
            reason_code="PENDING_PROMOTION_INVALID",
            promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        )
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_TOCK_REQUIRED"):
        DrainOutcome(
            status=DrainStatus.DRAINED,
            drain_run_id=_REFERENCE_DRAIN_RUN_ID,
            observed_main_head_sha=_REFERENCE_TOCK_SHA,
            reason_code="PROMOTION_EFFECT_ADMITTED",
            **_INTENT,
        )
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_TOCK_FORBIDDEN"):
        DrainOutcome(
            status=DrainStatus.NO_PENDING,
            drain_run_id=_REFERENCE_DRAIN_RUN_ID,
            observed_main_head_sha=_REFERENCE_HEAD_SHA,
            reason_code="NO_PENDING_PROMOTION",
            tock_sha=_REFERENCE_TOCK_SHA,
        )
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_FIELD_EMPTY"):
        DrainOutcome(
            status=DrainStatus.BLOCKED,
            drain_run_id="",
            observed_main_head_sha=_REFERENCE_HEAD_SHA,
            reason_code="BLOCKED_UNCLEAR_GATE",
        )


def test_drain_outcome_persists_authoritative_and_origin_forensic_copy(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    outcome = _drained_outcome()
    paths = persist_drain_outcome(runtime_root=runtime, outcome=outcome)
    assert paths["authoritative"] == runtime / _CANONICAL_OUTCOME_NAME
    assert paths["forensic"] == (
        runtime
        / "runs"
        / _REFERENCE_ORIGIN_RUN_ID
        / "drain_outcomes"
        / f"{_REFERENCE_PROMOTION_ATTEMPT_ID}.yaml"
    )
    assert load_persisted_drain_outcome(runtime_root=runtime) == outcome
    assert yaml.safe_load(paths["authoritative"].read_text(encoding="utf-8")) == (
        outcome.to_mapping()
    )
    assert yaml.safe_load(paths["forensic"].read_text(encoding="utf-8")) == (
        outcome.to_mapping()
    )
    assert not list(runtime.rglob("*.tmp"))


def test_outcome_reload_rejects_invalid_schema_and_bytes(tmp_path: Path) -> None:
    base = _no_pending_outcome().to_mapping()
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_SCHEMA_INVALID"):
        DrainOutcome.from_mapping({**base, "schema_version": "2.0"})
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_STATUS_INVALID"):
        DrainOutcome.from_mapping({**base, "status": "UNKNOWN"})
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_FIELD_EMPTY"):
        DrainOutcome.from_mapping({**base, "reason_code": 7})
    runtime = tmp_path / "runtime"
    assert load_persisted_drain_outcome(runtime_root=runtime) is None
    outcome_path = runtime / _CANONICAL_OUTCOME_NAME
    outcome_path.parent.mkdir()
    outcome_path.write_text("[not, a, mapping]\n", encoding="utf-8")
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_INVALID"):
        load_persisted_drain_outcome(runtime_root=runtime)
    outcome_path.write_text("broken: [\n", encoding="utf-8")
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_INVALID"):
        load_persisted_drain_outcome(runtime_root=runtime)


def test_pending_is_drained_through_real_step_16_and_stops_at_awaiting_tick(
    tmp_path: Path,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = runtime / _CANONICAL_PENDING_NAME
    assert pending.is_file()
    git_before = pending_tests._git_snapshot(world["repo"])
    decision = _evaluate(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    tock_sha = decision.outcome.tock_sha
    assert isinstance(tock_sha, str) and tock_sha
    tock_tree = pending_tests._run_git(world["repo"], "rev-parse", f"{tock_sha}^{{tree}}")
    record = pending_tests._assert_persisted_candidate_validation(world, exit_code=0)
    assert record["candidate_tree_oid"] == tock_tree
    assert tock_tree != world["main_tree"]
    assert pending_tests._run_git(world["repo"], "rev-parse", "HEAD") == tock_sha
    assert pending_tests._run_git(world["repo"], "rev-parse", "refs/heads/main") == tock_sha
    assert pending_tests._run_git(world["repo"], "rev-parse", "--abbrev-ref", "HEAD") == (
        "main"
    )
    assert pending_tests._run_git(world["repo"], "status", "--porcelain") == ""
    assert git_before["head"] != tock_sha
    assert decision.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert decision.outcome.status is DrainStatus.DRAINED
    assert decision.outcome.drain_run_id == _REFERENCE_DRAIN_RUN_ID
    assert decision.outcome.reason_code == "PROMOTION_EFFECT_ADMITTED"
    assert decision.outcome.origin_run_id == world["intent"].origin_run_id
    assert decision.outcome.task_id == world["intent"].task_id
    assert pending.exists() is False
    assert load_persisted_drain_outcome(runtime_root=runtime) == decision.outcome
    assert (runtime / "promotion_journal.yaml").is_file()
    assert (runtime / "promotion_consumed.yaml").is_file()
    _assert_no_wakeup_marker(runtime)


def test_identical_drained_reproof_keeps_canonical_bytes_and_does_not_retock(
    tmp_path: Path,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    first = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert first.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    path, pinned, mode, digest = _pin_outcome_bytes(world["runtime"], first.outcome)
    journal_before = (world["runtime"] / "promotion_journal.yaml").read_bytes()
    consumed_before = (world["runtime"] / "promotion_consumed.yaml").read_bytes()
    git_before = pending_tests._git_snapshot(world["repo"])
    second = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REPLAY_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert second.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert second.outcome == first.outcome
    assert path.read_bytes() == pinned
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert stat.S_IMODE(path.stat().st_mode) == mode
    assert (world["runtime"] / "promotion_journal.yaml").read_bytes() == journal_before
    assert (world["runtime"] / "promotion_consumed.yaml").read_bytes() == consumed_before
    pending_tests._assert_git_unchanged(world["repo"], git_before)
    _assert_no_wakeup_marker(world["runtime"])


def test_failed_step_16_persists_blocked_and_keeps_pending_intent(
    tmp_path: Path,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = runtime / _CANONICAL_PENDING_NAME
    git_before = pending_tests._git_snapshot(world["repo"])
    blocked_kwargs = dict(world["admit_kwargs"])
    blocked_kwargs["unresolved_foreign_epoch"] = True
    decision = _evaluate(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=blocked_kwargs,
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "PROMOTION_UNRESOLVED_FOREIGN_EPOCH"
    assert decision.outcome.tock_sha is None
    assert pending.is_file()
    assert pending.read_bytes() == world["pending_before"]
    assert (runtime / "promotion_journal.yaml").exists() is False
    assert (runtime / "promotion_consumed.yaml").exists() is False
    pending_tests._assert_git_unchanged(world["repo"], git_before)
    _assert_no_wakeup_marker(runtime)


def test_no_pending_is_dispatch_ready_without_git_tick_tock_or_private_marker(
    tmp_path: Path,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    git_before = pending_tests._git_snapshot(repo)
    first = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
    )
    assert first.phase is SchedulerPhase.DISPATCH_READY
    assert first.outcome.status is DrainStatus.NO_PENDING
    assert first.outcome.reason_code == "NO_PENDING_PROMOTION"
    assert first.outcome.tock_sha is None
    assert first.outcome.task_id is None
    assert load_persisted_drain_outcome(runtime_root=runtime) == first.outcome
    path, pinned, mode, digest = _pin_outcome_bytes(runtime, first.outcome)
    second = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id=_REPLAY_DRAIN_RUN_ID,
    )
    assert second.phase is SchedulerPhase.DISPATCH_READY
    assert second.outcome == first.outcome
    assert path.read_bytes() == pinned
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert stat.S_IMODE(path.stat().st_mode) == mode
    pending_tests._assert_git_unchanged(repo, git_before)
    _assert_no_effect_or_tick_files(runtime)
    _assert_no_wakeup_marker(runtime)
    assert list(runtime.glob(_CANONICAL_PENDING_NAME)) == []


def test_prior_drained_with_main_head_drift_is_blocked_not_consumed_again(
    tmp_path: Path,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path)
    runtime = tmp_path / "runtime"
    persist_drain_outcome(runtime_root=runtime, outcome=_drained_outcome())
    git_before = pending_tests._git_snapshot(repo)
    decision = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id=_REPLAY_DRAIN_RUN_ID,
        main_head_reader=lambda _: pending_tests._run_git(repo, "rev-parse", "HEAD"),
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "DRAIN_OUTCOME_MAIN_HEAD_DRIFT"
    assert decision.outcome.tock_sha is None
    pending_tests._assert_git_unchanged(repo, git_before)
    _assert_no_effect_or_tick_files(runtime)
    _assert_no_wakeup_marker(runtime)


def test_ambiguous_invalid_and_prior_blocked_states_fail_closed(
    tmp_path: Path,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    for name in (_CANONICAL_PENDING_NAME, "pending_promotion.json"):
        (runtime / name).write_text("{}\n", encoding="utf-8")
    ambiguous = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id="drain-ambiguous",
    )
    assert ambiguous.phase is SchedulerPhase.BLOCKED
    assert ambiguous.outcome.reason_code == "PENDING_PROMOTION_AMBIGUOUS"
    for name in (_CANONICAL_PENDING_NAME, "pending_promotion.json"):
        (runtime / name).unlink()
    persist_drain_outcome(
        runtime_root=runtime,
        outcome=DrainOutcome(
            status=DrainStatus.BLOCKED,
            drain_run_id="drain-prior",
            observed_main_head_sha=pending_tests._run_git(repo, "rev-parse", "HEAD"),
            reason_code="BLOCKED_UNCLEAR_GATE",
        ),
    )
    blocked = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id="drain-next",
    )
    assert blocked.phase is SchedulerPhase.BLOCKED
    assert blocked.outcome.reason_code == "PRIOR_DRAIN_BLOCKED"
    (runtime / _CANONICAL_OUTCOME_NAME).write_text("broken: [\n", encoding="utf-8")
    invalid_prior = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id="drain-invalid-prior",
    )
    assert invalid_prior.phase is SchedulerPhase.BLOCKED
    assert invalid_prior.outcome.reason_code == "DRAIN_OUTCOME_INVALID"
    _assert_no_wakeup_marker(runtime)


@pytest.mark.parametrize(
    ("pending_text", "expected_reason"),
    (
        ("[]\n", "PENDING_PROMOTION_INVALID"),
        ("broken: [\n", "PENDING_PROMOTION_INVALID"),
        (
            yaml.safe_dump(
                {
                    "schema_version": _REFERENCE_SCHEMA_VERSION,
                    "artifact_class": "NON_AUTHORIZING_PROMOTION_INTENT",
                    "task_id": "",
                    "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
                    "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
                    "result_sha": _REFERENCE_RESULT_SHA,
                    "promotion_class": _REFERENCE_PROMOTION_CLASS,
                }
            ),
            "PENDING_PROMOTION_IDENTITY_INVALID",
        ),
    ),
)
def test_invalid_pending_payload_never_invokes_step_16(
    tmp_path: Path,
    pending_text: str,
    expected_reason: str,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    pending = runtime / _CANONICAL_PENDING_NAME
    pending.write_text(pending_text, encoding="utf-8")
    git_before = pending_tests._git_snapshot(repo)
    decision = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id="drain-invalid",
        effect_admission={"unresolved_foreign_epoch": True},
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.reason_code == expected_reason
    assert pending.exists()
    assert (runtime / "promotion_journal.yaml").exists() is False
    pending_tests._assert_git_unchanged(repo, git_before)


def test_residual_promotion_journal_without_pending_is_unresolved_fail_closed(
    tmp_path: Path,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "promotion_journal.yaml").write_text(
        "schema_version: '1.0'\nphase: APPLYING\n",
        encoding="utf-8",
    )
    git_before = pending_tests._git_snapshot(repo)
    decision = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id="drain-residual",
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "UNRESOLVED_PROMOTION_JOURNAL"
    pending_tests._assert_git_unchanged(repo, git_before)
    _assert_no_wakeup_marker(runtime)


def test_identity_marker_is_neutral_wakeup_and_never_decision_authority(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "wakeup.yaml"
    payload = write_identity_marker(
        marker_path=marker,
        marker_type=_PUBLIC_MARKER_TYPE,
        **_INTENT,
        tock_sha=_REFERENCE_TOCK_SHA,
    )
    assert payload["marker_type"] == _WAKEUP_MARKER_TYPE
    loaded = load_identity_marker(
        marker_path=marker,
        marker_type=_PUBLIC_MARKER_TYPE,
        expected_identity=_INTENT,
    )
    assert loaded is not None
    assert loaded["marker_type"] == _PUBLIC_MARKER_TYPE
    assert (
        load_identity_marker(
            marker_path=marker,
            marker_type="foreign_wakeup",
            expected_identity=_INTENT,
        )
        is None
    )
    foreign = dict(_INTENT)
    foreign["task_id"] = "synthetic-task-002"
    assert (
        load_identity_marker(
            marker_path=marker,
            marker_type=_PUBLIC_MARKER_TYPE,
            expected_identity=foreign,
        )
        is None
    )
    with pytest.raises(DrainOutcomeError, match="IDENTITY_MARKER_FIELD_INVALID"):
        write_identity_marker(
            marker_path=tmp_path / "invalid.yaml",
            marker_type="",
            **_INTENT,
        )


def test_no_pending_does_not_write_wakeup_marker_even_when_helper_exists(
    tmp_path: Path,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    decision = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
    )
    assert decision.phase is SchedulerPhase.DISPATCH_READY
    assert list(runtime.rglob("*.yaml")) == [runtime / _CANONICAL_OUTCOME_NAME]
    _assert_no_wakeup_marker(runtime)
    assert (
        load_identity_marker(
            marker_path=runtime / "wakeup.yaml",
            marker_type=_PUBLIC_MARKER_TYPE,
            expected_identity=_INTENT,
        )
        is None
    )


def test_admission_tock_sha_mismatch_against_live_head_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    git_before = pending_tests._git_snapshot(world["repo"])
    live_head = git_before["head"]
    assert _CLAIMED_TOCK_SHA != live_head

    def fake_admit(**_kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "tocked",
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
            "tock_sha": _CLAIMED_TOCK_SHA,
        }

    _patch_step_16(monkeypatch, fake_admit)
    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "DRAIN_TOCK_SHA_MISMATCH"
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False


def test_admission_ok_without_head_movement_is_not_drained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    git_before = pending_tests._git_snapshot(world["repo"])

    def fake_admit(**_kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "tocked",
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
        }

    _patch_step_16(monkeypatch, fake_admit)
    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "DRAIN_TOCK_NOT_APPLIED"
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False


def test_step_16_exception_and_non_mapping_are_typed_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    git_before = pending_tests._git_snapshot(world["repo"])
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("DRAIN_EFFECT_ADMISSION_FAILED: synthetic")
        ),
    )
    raised = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert raised.phase is SchedulerPhase.BLOCKED
    assert raised.outcome.status is DrainStatus.BLOCKED
    assert raised.outcome.reason_code == "DRAIN_EFFECT_ADMISSION_FAILED"
    assert raised.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)

    world = pending_tests._prepare_effect_world(tmp_path / "non-mapping")
    git_before = pending_tests._git_snapshot(world["repo"])
    _patch_step_16(monkeypatch, lambda **_kwargs: [])
    nonmapping = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert nonmapping.phase is SchedulerPhase.BLOCKED
    assert nonmapping.outcome.status is DrainStatus.BLOCKED
    assert nonmapping.outcome.reason_code == "DRAIN_EFFECT_ADMISSION_INVALID"
    assert nonmapping.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False


def test_missing_or_foreign_rooted_admission_blocks_before_step_16(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path / "missing")
    git_before = pending_tests._git_snapshot(world["repo"])
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("step 16 must not run"),
    )
    missing = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
    )
    assert missing.phase is SchedulerPhase.BLOCKED
    assert missing.outcome.status is DrainStatus.BLOCKED
    assert missing.outcome.reason_code == "DRAIN_EFFECT_ADMISSION_MISSING"
    assert missing.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)

    runtime_world = pending_tests._prepare_effect_world(tmp_path / "runtime-mismatch")
    runtime_git_before = pending_tests._git_snapshot(runtime_world["repo"])
    foreign_runtime = tmp_path / "foreign-runtime"
    foreign_runtime.mkdir()
    runtime_canary = foreign_runtime / "canary.yaml"
    runtime_canary.write_text("untouched\n", encoding="utf-8")
    runtime_kwargs = dict(runtime_world["admit_kwargs"])
    runtime_kwargs["runtime_root"] = foreign_runtime
    assert runtime_kwargs["target_worktree"] == runtime_world["repo"]
    runtime_mismatched = _evaluate_fail_closed(
        repo=runtime_world["repo"],
        runtime=runtime_world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=runtime_kwargs,
    )
    assert runtime_mismatched.phase is SchedulerPhase.BLOCKED
    assert runtime_mismatched.outcome.status is DrainStatus.BLOCKED
    assert runtime_mismatched.outcome.reason_code == (
        "DRAIN_EFFECT_ADMISSION_ROOT_MISMATCH"
    )
    assert runtime_mismatched.outcome.tock_sha is None
    _assert_idle_pending_world(runtime_world, runtime_git_before)
    assert runtime_canary.read_text(encoding="utf-8") == "untouched\n"
    assert (foreign_runtime / _CANONICAL_OUTCOME_NAME).exists() is False
    assert (foreign_runtime / _CANONICAL_PENDING_NAME).exists() is False

    target_world = pending_tests._prepare_effect_world(tmp_path / "target-mismatch")
    target_git_before = pending_tests._git_snapshot(target_world["repo"])
    foreign_repo = pending_tests._init_target_repo(tmp_path / "foreign-target")
    foreign_git_before = pending_tests._git_snapshot(foreign_repo)
    target_kwargs = dict(target_world["admit_kwargs"])
    target_kwargs["target_worktree"] = foreign_repo
    assert target_kwargs["runtime_root"] == target_world["runtime"]
    target_mismatched = _evaluate_fail_closed(
        repo=target_world["repo"],
        runtime=target_world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=target_kwargs,
    )
    assert target_mismatched.phase is SchedulerPhase.BLOCKED
    assert target_mismatched.outcome.status is DrainStatus.BLOCKED
    assert target_mismatched.outcome.reason_code == (
        "DRAIN_EFFECT_ADMISSION_ROOT_MISMATCH"
    )
    assert target_mismatched.outcome.tock_sha is None
    _assert_idle_pending_world(target_world, target_git_before)
    pending_tests._assert_git_unchanged(foreign_repo, foreign_git_before)
    assert (foreign_repo / _CANONICAL_OUTCOME_NAME).exists() is False
    assert (foreign_repo / _CANONICAL_PENDING_NAME).exists() is False


def test_pending_plus_preexisting_journal_is_unresolved_before_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    journal = runtime / "promotion_journal.yaml"
    journal.write_text("schema_version: '1.0'\nphase: APPLYING\n", encoding="utf-8")
    journal_before = journal.read_bytes()
    git_before = pending_tests._git_snapshot(world["repo"])
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("step 16 must not run"),
    )
    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "UNRESOLVED_PROMOTION_JOURNAL"
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert journal.read_bytes() == journal_before


def _fail_replace_destination(
    target: Path,
    real_replace: Any,
) -> Any:
    def _wrapped(src: Any, dst: Any, *args: Any, **kwargs: Any) -> Any:
        if Path(dst).resolve() == target.resolve():
            raise OSError("synthetic drain outcome replace failure")
        return real_replace(src, dst, *args, **kwargs)

    return _wrapped


@pytest.mark.parametrize(
    "target_kind",
    ("authoritative", "forensic"),
)
def test_drain_outcome_replace_failure_preserves_bytes_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
) -> None:
    runtime = tmp_path / "runtime"
    prior = _drained_outcome()
    persist_drain_outcome(runtime_root=runtime, outcome=prior)
    authoritative = runtime / _CANONICAL_OUTCOME_NAME
    forensic = (
        runtime
        / "runs"
        / _REFERENCE_ORIGIN_RUN_ID
        / "drain_outcomes"
        / f"{_REFERENCE_PROMOTION_ATTEMPT_ID}.yaml"
    )
    target = authoritative if target_kind == "authoritative" else forensic
    before = target.read_bytes()
    later = _drained_outcome(
        drain_run_id=_REPLAY_DRAIN_RUN_ID,
        reason_code="PROMOTION_EFFECT_ADMITTED",
    )
    monkeypatch.setattr(
        os,
        "replace",
        _fail_replace_destination(target, os.replace),
    )
    with pytest.raises(DrainOutcomeError, match="DRAIN_OUTCOME_PERSIST_FAILED"):
        persist_drain_outcome(runtime_root=runtime, outcome=later)
    assert target.read_bytes() == before
    leftover = [
        path
        for path in runtime.rglob("*")
        if path.is_file()
        and (path.name.endswith(".tmp") or path.name.endswith(".temp"))
    ]
    assert leftover == []


def test_admission_ok_with_matching_tock_sha_without_head_movement_is_not_drained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    git_before = pending_tests._git_snapshot(world["repo"])
    live_head = git_before["head"]

    def fake_admit(**_kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "tocked",
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
            "tock_sha": live_head,
        }

    _patch_step_16(monkeypatch, fake_admit)
    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert pending_tests._run_git(world["repo"], "rev-parse", "HEAD") == live_head
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.status is not DrainStatus.DRAINED
    assert decision.outcome.reason_code == "DRAIN_TOCK_NOT_APPLIED"
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False


@pytest.mark.parametrize("missing_key", ("runtime_root", "target_worktree"))
def test_incomplete_admission_root_keys_block_before_step_16(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_key: str,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path / missing_key)
    git_before = pending_tests._git_snapshot(world["repo"])
    incomplete = dict(world["admit_kwargs"])
    del incomplete[missing_key]
    remaining_key = (
        "target_worktree" if missing_key == "runtime_root" else "runtime_root"
    )
    assert missing_key not in incomplete
    assert remaining_key in incomplete
    if missing_key == "runtime_root":
        assert incomplete["target_worktree"] == world["repo"]
    else:
        assert incomplete["runtime_root"] == world["runtime"]
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("step 16 must not run"),
    )
    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=incomplete,
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "DRAIN_EFFECT_ADMISSION_MISSING"
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert load_persisted_drain_outcome(runtime_root=world["runtime"]) == (
        decision.outcome
    )
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False


@pytest.mark.parametrize("null_key", ("runtime_root", "target_worktree"))
def test_non_path_admission_root_values_block_before_step_16(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    null_key: str,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path / f"none-{null_key}")
    git_before = pending_tests._git_snapshot(world["repo"])
    incomplete = dict(world["admit_kwargs"])
    incomplete[null_key] = None
    if null_key == "runtime_root":
        assert incomplete["target_worktree"] == world["repo"]
        assert incomplete["runtime_root"] is None
    else:
        assert incomplete["runtime_root"] == world["runtime"]
        assert incomplete["target_worktree"] is None
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("step 16 must not run"),
    )
    decision = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=incomplete,
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "DRAIN_EFFECT_ADMISSION_MISSING"
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert load_persisted_drain_outcome(runtime_root=world["runtime"]) == (
        decision.outcome
    )
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False


def test_persisted_no_pending_is_superseded_by_later_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = pending_tests._init_target_repo(tmp_path / "live")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    git_before = pending_tests._git_snapshot(repo)
    first = _evaluate(
        repo=repo,
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
    )
    assert first.phase is SchedulerPhase.DISPATCH_READY
    assert first.outcome.status is DrainStatus.NO_PENDING
    assert first.outcome.reason_code == "NO_PENDING_PROMOTION"
    outcome_path = runtime / _CANONICAL_OUTCOME_NAME
    no_pending_bytes = outcome_path.read_bytes()
    world = pending_tests._prepare_effect_world(tmp_path / "later-pending")
    pending = runtime / _CANONICAL_PENDING_NAME
    pending.write_bytes(world["pending_before"])
    pending_bytes = pending.read_bytes()
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("step 16 must not run"),
    )
    second = _evaluate_fail_closed(
        repo=repo,
        runtime=runtime,
        drain_run_id=_REPLAY_DRAIN_RUN_ID,
    )
    assert second.phase is SchedulerPhase.BLOCKED
    assert second.phase is not SchedulerPhase.DISPATCH_READY
    assert second.outcome.status is DrainStatus.BLOCKED
    assert second.outcome.reason_code == "DRAIN_EFFECT_ADMISSION_MISSING"
    assert second.outcome.tock_sha is None
    assert second.outcome != first.outcome
    assert pending.is_file()
    assert pending.read_bytes() == pending_bytes
    assert outcome_path.read_bytes() != no_pending_bytes
    assert load_persisted_drain_outcome(runtime_root=runtime) == second.outcome
    pending_tests._assert_git_unchanged(repo, git_before)
    _assert_no_effect_or_tick_files(runtime)
    _assert_no_wakeup_marker(runtime)


def test_post_drain_tick_window_stays_inside_step4_before_pre_dispatch_drain(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_cli_run import _STEP4_WINDOWS, _run_observed_surface

    _stdout, observed, _basis_calls, _lock_events, _target = _run_observed_surface(
        tmp_path,
        capsys,
        monkeypatch,
        active_task=True,
    )
    windows = [label for label in observed if label in _STEP4_WINDOWS]

    assert tuple(windows) == _STEP4_WINDOWS
    assert "POST_DRAIN_TICK_WINDOWS" in observed
    assert "pre_dispatch_drain" in observed
    assert observed.index("POST_DRAIN_TICK_WINDOWS") < observed.index("pre_dispatch_drain")
    assert observed.index("POST_DRAIN_TICK_WINDOWS") < observed.index("task_admissibility")


_RECONSTRUCTED_DRAIN_ID = "reconciliation-drain-reconstructed-001"
_RECONSTRUCTED_PENDING_AT = "2026-09-22T00:00:00Z"
_RECONSTRUCTED_APPLYING_AT = "2026-09-22T00:01:00Z"
_RECONSTRUCTED_GIT_APPLIED_AT = "2026-09-22T00:02:00Z"
_RECONSTRUCTED_RECONCILED_AT = "2026-09-22T00:03:00Z"
_RECONSTRUCTED_FINALIZED_AT = "2026-09-22T00:04:00Z"
_JOURNAL_NAME = "promotion_journal.yaml"
_NON_EFFECT_TOCK_SHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
_FOREIGN_BASE_SHA = "abababababababababababababababababababab"
_FOREIGN_RESULT_SHA = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
_JOURNAL_PHASE_ORDER = (
    PromotionJournalPhase.PENDING,
    PromotionJournalPhase.APPLYING,
    PromotionJournalPhase.GIT_APPLIED,
    PromotionJournalPhase.RECONCILED,
    PromotionJournalPhase.FINALIZED,
)
_LATER_JOURNAL_PHASES = _JOURNAL_PHASE_ORDER[2:]
_PHASE_STAMPS = {
    PromotionJournalPhase.APPLYING: _RECONSTRUCTED_APPLYING_AT,
    PromotionJournalPhase.GIT_APPLIED: _RECONSTRUCTED_GIT_APPLIED_AT,
    PromotionJournalPhase.RECONCILED: _RECONSTRUCTED_RECONCILED_AT,
    PromotionJournalPhase.FINALIZED: _RECONSTRUCTED_FINALIZED_AT,
}


def _journal_fields(pending: Mapping[str, Any]) -> dict[str, str]:
    return {
        "task_id": str(pending["task_id"]),
        "origin_run_id": str(pending["origin_run_id"]),
        "promotion_attempt_id": str(pending["promotion_attempt_id"]),
        "result_sha": str(pending["result_sha"]),
        "promotion_class": str(pending["promotion_class"]),
        "base_main_sha": str(pending["base_sha"]),
        "validated_result_sha": str(pending["result_sha"]),
    }


def _foreign_journal_value(pending: Mapping[str, Any], field: str) -> str:
    if field == "task_id":
        return "synthetic-task-002"
    if field == "origin_run_id":
        return "origin-run-002"
    if field == "promotion_attempt_id":
        return "promotion-attempt-002"
    if field == "promotion_class":
        if pending["promotion_class"] == "GREEN_PROMOTION":
            return "RED_TDD_PROMOTION"
        return "GREEN_PROMOTION"
    if field == "base_main_sha":
        assert _FOREIGN_BASE_SHA != pending["base_sha"]
        return _FOREIGN_BASE_SHA
    assert field in {"result_sha", "validated_result_sha"}
    assert _FOREIGN_RESULT_SHA != pending["result_sha"]
    return _FOREIGN_RESULT_SHA


def _write_reconstructed_journal(
    runtime: Path,
    pending: Mapping[str, Any],
    *,
    phase: PromotionJournalPhase,
    field_overrides: Mapping[str, str] | None = None,
) -> PromotionJournal:
    fields = _journal_fields(pending)
    if field_overrides is not None:
        fields.update(field_overrides)
    path = runtime / _JOURNAL_NAME
    journal = create_promotion_journal(
        drain_run_id=_RECONSTRUCTED_DRAIN_ID,
        pending_at_utc=_RECONSTRUCTED_PENDING_AT,
        reason_code="RECONCILIATION_COMPLETED",
        task_fingerprint=str(pending["task_snapshot"]["bound"]["fingerprint"]),
        task_snapshot_sha256=str(pending["task_snapshot"]["bound"]["digest"]),
        instruction_signature_sha256=str(
            pending["instruction_snapshot"]["bound"]["digest"]
        ),
        **fields,
    )
    persist_promotion_journal(path=path, journal=journal)
    current = journal
    target_index = _JOURNAL_PHASE_ORDER.index(phase)
    for next_phase in _JOURNAL_PHASE_ORDER[1 : target_index + 1]:
        current = transition_promotion_journal(
            current,
            phase=next_phase,
            drain_run_id=_RECONSTRUCTED_DRAIN_ID,
            transitioned_at_utc=_PHASE_STAMPS[next_phase],
            reason_code="RECONCILIATION_COMPLETED",
            tock_sha=(
                _NON_EFFECT_TOCK_SHA
                if next_phase is PromotionJournalPhase.GIT_APPLIED
                else None
            ),
        )
        persist_promotion_journal(path=path, journal=current)
    loaded = load_promotion_journal(path=path)
    assert loaded.phase is phase
    return loaded


def _assert_real_admission_symbol() -> None:
    assert (
        drain_module.admit_pending_promotion_effect
        is admit_pending_promotion_effect
    )
    assert (
        admit_pending_promotion_effect.__module__
        == "task_transition_runner.runner_git_promotion"
    )


def _assert_continued_journal(
    runtime: Path,
    reconstructed: PromotionJournal,
    pending: Mapping[str, Any],
) -> None:
    path = runtime / _JOURNAL_NAME
    continued = load_promotion_journal(path=path)
    assert continued.pending_at_utc == reconstructed.pending_at_utc
    assert continued.task_id == pending["task_id"]
    assert continued.origin_run_id == pending["origin_run_id"]
    assert continued.promotion_attempt_id == pending["promotion_attempt_id"]
    assert continued.result_sha == pending["result_sha"]
    assert continued.validated_result_sha == pending["result_sha"]
    assert continued.promotion_class == pending["promotion_class"]
    assert continued.base_main_sha == pending["base_sha"]
    assert continued.phase is PromotionJournalPhase.FINALIZED
    assert [item.name for item in runtime.rglob(_JOURNAL_NAME)] == [_JOURNAL_NAME]
    assert (runtime / _CANONICAL_PENDING_NAME).exists() is False


@pytest.mark.parametrize(
    "phase",
    (PromotionJournalPhase.PENDING, PromotionJournalPhase.APPLYING),
)
def test_matching_pre_effect_journal_reaches_real_effect_admission(
    tmp_path: Path,
    phase: PromotionJournalPhase,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = load_pending_promotion(runtime_root=runtime)
    reconstructed = _write_reconstructed_journal(runtime, pending, phase=phase)
    assert reconstructed.tock_sha is None
    assert _journal_fields(pending) == {
        "task_id": reconstructed.task_id,
        "origin_run_id": reconstructed.origin_run_id,
        "promotion_attempt_id": reconstructed.promotion_attempt_id,
        "result_sha": reconstructed.result_sha,
        "promotion_class": reconstructed.promotion_class,
        "base_main_sha": reconstructed.base_main_sha,
        "validated_result_sha": reconstructed.validated_result_sha,
    }
    _assert_real_admission_symbol()
    decision = _evaluate(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert decision.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert decision.outcome.status is DrainStatus.DRAINED
    assert decision.outcome.reason_code == "PROMOTION_EFFECT_ADMITTED"
    assert decision.outcome.tock_sha
    _assert_continued_journal(runtime, reconstructed, pending)
    if phase is PromotionJournalPhase.APPLYING:
        continued = load_promotion_journal(path=runtime / _JOURNAL_NAME)
        assert continued.applying_at_utc == reconstructed.applying_at_utc


def _assert_unresolved_before_effect(
    world: Mapping[str, Any],
    *,
    journal_before: bytes,
    monkeypatch: pytest.MonkeyPatch,
    pending_remains: bool,
) -> None:
    runtime = world["runtime"]
    git_before = pending_tests._git_snapshot(world["repo"])
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("step 16 must not run"),
    )
    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id=_REFERENCE_DRAIN_RUN_ID,
        effect_admission=world["admit_kwargs"],
    )
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "UNRESOLVED_PROMOTION_JOURNAL"
    assert decision.outcome.tock_sha is None
    assert (runtime / _JOURNAL_NAME).read_bytes() == journal_before
    assert (runtime / "promotion_consumed.yaml").exists() is False
    pending_tests._assert_git_unchanged(world["repo"], git_before)
    _assert_no_wakeup_marker(runtime)
    if pending_remains:
        _assert_idle_pending_world(world, git_before)
    else:
        assert (runtime / _CANONICAL_PENDING_NAME).exists() is False


@pytest.mark.parametrize(
    "field",
    (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
        "promotion_class",
        "base_main_sha",
        "validated_result_sha",
    ),
)
def test_mismatched_reconstructed_journal_stays_unresolved_before_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = load_pending_promotion(runtime_root=runtime)
    foreign = _foreign_journal_value(pending, field)
    journal = _write_reconstructed_journal(
        runtime,
        pending,
        phase=PromotionJournalPhase.PENDING,
        field_overrides={field: foreign},
    )
    assert getattr(journal, field) == foreign
    assert getattr(journal, field) != _journal_fields(pending)[field]
    _assert_unresolved_before_effect(
        world,
        journal_before=(runtime / _JOURNAL_NAME).read_bytes(),
        monkeypatch=monkeypatch,
        pending_remains=True,
    )


@pytest.mark.parametrize("phase", _LATER_JOURNAL_PHASES)
def test_later_journal_phase_stays_unresolved_before_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: PromotionJournalPhase,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = load_pending_promotion(runtime_root=runtime)
    journal = _write_reconstructed_journal(runtime, pending, phase=phase)
    assert journal.tock_sha == _NON_EFFECT_TOCK_SHA
    _assert_unresolved_before_effect(
        world,
        journal_before=(runtime / _JOURNAL_NAME).read_bytes(),
        monkeypatch=monkeypatch,
        pending_remains=True,
    )


@pytest.mark.parametrize(
    ("phase", "payload_kind"),
    (
        (PromotionJournalPhase.PENDING, "malformed"),
        (PromotionJournalPhase.PENDING, "unknown"),
        (PromotionJournalPhase.PENDING, "tock"),
        (PromotionJournalPhase.APPLYING, "tock"),
    ),
)
def test_non_takeover_journal_stays_unresolved_before_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: PromotionJournalPhase,
    payload_kind: str,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = load_pending_promotion(runtime_root=runtime)
    path = runtime / _JOURNAL_NAME
    if payload_kind == "malformed":
        path.write_text("schema_version: '1.0'\nphase: [\n", encoding="utf-8")
    else:
        journal = _write_reconstructed_journal(
            runtime,
            pending,
            phase=phase,
        )
        payload = journal.to_mapping()
        if payload_kind == "unknown":
            payload["phase"] = "NOT_A_PHASE"
        else:
            payload["tock_sha"] = _NON_EFFECT_TOCK_SHA
        path.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )
    _assert_unresolved_before_effect(
        world,
        journal_before=path.read_bytes(),
        monkeypatch=monkeypatch,
        pending_remains=True,
    )


def test_canonical_journal_without_pending_stays_unresolved_before_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    pending = load_pending_promotion(runtime_root=runtime)
    journal = _write_reconstructed_journal(
        runtime,
        pending,
        phase=PromotionJournalPhase.PENDING,
    )
    assert journal.tock_sha is None
    (runtime / _CANONICAL_PENDING_NAME).unlink()
    _assert_unresolved_before_effect(
        world,
        journal_before=(runtime / _JOURNAL_NAME).read_bytes(),
        monkeypatch=monkeypatch,
        pending_remains=False,
    )


_PREDECESSOR_TASK_ID = "synthetic-task-predecessor"
_PREDECESSOR_ORIGIN_RUN_ID = "origin-run-predecessor"
_PREDECESSOR_PROMOTION_ATTEMPT_ID = "promotion-attempt-predecessor"
_SUCCESSOR_START_MESSAGE = (
    "START synthetic-task-001 origin-run-001 promotion-attempt-001"
)
_SUCCESSOR_UPDATED_AT = "2026-10-07T00:00:00Z"
_PRE_START_BLOCKED_DRAIN_ID = "cli-run"
_PRE_START_BLOCKED_REASON = "UNRESOLVED_PROMOTION_JOURNAL"
_BLOCKED_SUPERSESSION_REASON_NEGATIVES = {
    "prestate_reason_arbitrary": "ARBITRARY_REASON_CODE",
    "prestate_reason_empty": "",
    "prestate_reason_unrelated": "STATE_READY_SYNTHETIC",
}
_BLOCKED_SUPERSESSION_NEGATIVE_CASES = (
    "blocked_reason",
    "blocked_run_id",
    "blocked_observed_head",
    "pending_missing",
    "pending_ambiguous",
    "pending_malformed",
    "pending_identity_incomplete",
    "pending_identity_foreign",
    "pending_base_mismatch",
    "prestate_missing_last_tock",
    "prestate_observed_mismatch",
    *_BLOCKED_SUPERSESSION_REASON_NEGATIVES,
    "live_head_mismatch",
    "admission_missing",
    "admission_incomplete",
)
_QUALIFICATION_NEGATIVE_CASES = (
    "pending_missing",
    "pending_alternate",
    "pending_malformed",
    "pending_incomplete",
    "same_task_id",
    "same_full_identity",
    "base_mismatch",
    "prestate_observed_mismatch",
    "prestate_last_tock_mismatch",
    "prestate_result_mismatch",
    "prestate_reason_mismatch",
    "old_identity_incomplete",
    "old_observed_tock_mismatch",
    "old_reason_not_admitted",
)


def _predecessor_outcome(
    world: Mapping[str, Any],
    **overrides: Any,
) -> DrainOutcome:
    payload: dict[str, Any] = {
        "status": DrainStatus.DRAINED,
        "drain_run_id": "drain-run-predecessor",
        "observed_main_head_sha": world["main_sha"],
        "reason_code": "PROMOTION_EFFECT_ADMITTED",
        "task_id": _PREDECESSOR_TASK_ID,
        "origin_run_id": _PREDECESSOR_ORIGIN_RUN_ID,
        "promotion_attempt_id": _PREDECESSOR_PROMOTION_ATTEMPT_ID,
        "result_sha": world["prior_sha"],
        "promotion_class": _REFERENCE_PROMOTION_CLASS,
        "tock_sha": world["main_sha"],
    }
    payload.update(overrides)
    return DrainOutcome(**payload)


def _materialize_successor_start(world: Mapping[str, Any]) -> str:
    repo = world["repo"]
    predecessor_tock = world["main_sha"]
    tree_sha = pending_tests._run_git(
        repo,
        "rev-parse",
        f"{predecessor_tock}^{{tree}}",
    )
    start_sha = pending_tests._run_git(
        repo,
        "commit-tree",
        tree_sha,
        "-p",
        predecessor_tock,
        "-m",
        f"{_SUCCESSOR_START_MESSAGE} {predecessor_tock}",
    )
    pending_tests._run_git(
        repo,
        "update-ref",
        "refs/heads/main",
        start_sha,
        predecessor_tock,
    )
    pending_tests._run_git(repo, "reset", "--hard", start_sha)
    return start_sha


def _persist_successor_pending(
    world: dict[str, Any],
    *,
    start_sha: str,
    predecessor: DrainOutcome,
    prestate_reason_code: str = "PROMOTION_EFFECT_ADMITTED",
) -> None:
    from task_transition_runner.runner_promotion_state import write_main_state

    runtime = world["runtime"]
    pending_path = runtime / _CANONICAL_PENDING_NAME
    pending_path.unlink()
    prestate = pending_tests._reference_main_state(
        observed_main_head_sha=start_sha,
        last_tock_sha=predecessor.tock_sha,
        validated_result_sha=predecessor.result_sha,
        reason_code=prestate_reason_code,
        updated_at_utc=_SUCCESSOR_UPDATED_AT,
    )
    write_main_state(
        main_state_path=runtime / "main_state.yaml",
        payload=prestate,
    )
    prior_intent = world["intent"]
    intent = type(prior_intent)(
        artifact_class=prior_intent.artifact_class,
        task_id=prior_intent.task_id,
        origin_run_id=prior_intent.origin_run_id,
        promotion_attempt_id=prior_intent.promotion_attempt_id,
        result_sha=prior_intent.result_sha,
        promotion_class=prior_intent.promotion_class,
        expected_main_state_after_promotion=(
            prior_intent.expected_main_state_after_promotion
        ),
        main_state_before_promotion=prestate,
        allowed_paths=prior_intent.allowed_paths,
        red_lock_closure=prior_intent.red_lock_closure,
    )
    admit_kwargs = dict(world["admit_kwargs"])
    persisted = git_promotion.persist_pending_promotion(
        runtime_root=runtime,
        target_worktree=world["repo"],
        intent=intent,
        task_snapshot=admit_kwargs["task_snapshot"],
        instruction_snapshot=admit_kwargs["instruction_snapshot"],
        worker_result=admit_kwargs["worker_result"],
        expected_outcome=admit_kwargs["expected_outcome"],
        orchestrator_decision=admit_kwargs["orchestrator_decision"],
        validator_decision=admit_kwargs["validator_decision"],
        base_sha=start_sha,
    )
    assert persisted["ok"] is True
    loaded = load_pending_promotion(runtime_root=runtime)
    assert loaded == persisted["intent"]
    admit_kwargs["base_sha"] = start_sha
    admit_kwargs["validation_command"] = pending_tests._merge_candidate_view_command(
        main_sha=start_sha,
        result_sha=world["result_sha"],
    )
    world["intent"] = intent
    world["admit_kwargs"] = admit_kwargs
    world["main_before"] = (runtime / "main_state.yaml").read_bytes()
    world["pending_before"] = pending_path.read_bytes()


def _prepare_drain_epoch_world(
    tmp_path: Path,
    *,
    prestate_reason_code: str = "PROMOTION_EFFECT_ADMITTED",
) -> dict[str, Any]:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    predecessor = _predecessor_outcome(world)
    assert predecessor.status is DrainStatus.DRAINED
    assert predecessor.reason_code == "PROMOTION_EFFECT_ADMITTED"
    assert predecessor.observed_main_head_sha == predecessor.tock_sha
    assert all(
        (
            predecessor.task_id,
            predecessor.origin_run_id,
            predecessor.promotion_attempt_id,
            predecessor.result_sha,
            predecessor.promotion_class,
        )
    )
    predecessor_paths = persist_drain_outcome(
        runtime_root=runtime,
        outcome=predecessor,
    )
    predecessor_bytes = {
        name: path.read_bytes()
        for name, path in predecessor_paths.items()
    }
    assert predecessor_bytes["authoritative"] == predecessor_bytes["forensic"]
    start_sha = _materialize_successor_start(world)
    assert start_sha != predecessor.tock_sha
    _persist_successor_pending(
        world,
        start_sha=start_sha,
        predecessor=predecessor,
        prestate_reason_code=prestate_reason_code,
    )
    pending = load_pending_promotion(runtime_root=runtime)
    assert pending["task_id"] != predecessor.task_id
    assert (
        pending["task_id"],
        pending["origin_run_id"],
        pending["promotion_attempt_id"],
        pending["result_sha"],
        pending["promotion_class"],
    ) != (
        predecessor.task_id,
        predecessor.origin_run_id,
        predecessor.promotion_attempt_id,
        predecessor.result_sha,
        predecessor.promotion_class,
    )
    assert pending["base_sha"] == start_sha
    assert pending["main_state_before_promotion"] == {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": predecessor.result_sha,
        "last_tock_sha": predecessor.tock_sha,
        "observed_main_head_sha": start_sha,
        "reason_code": prestate_reason_code,
        "updated_at_utc": _SUCCESSOR_UPDATED_AT,
    }
    assert list(runtime.glob("pending_promotion.*")) == [
        runtime / _CANONICAL_PENDING_NAME
    ]
    world["predecessor"] = predecessor
    world["predecessor_paths"] = predecessor_paths
    world["predecessor_bytes"] = predecessor_bytes
    world["start_sha"] = start_sha
    return world


def _assert_predecessor_forensic_bytes(world: Mapping[str, Any]) -> None:
    forensic = world["predecessor_paths"]["forensic"]
    assert forensic.read_bytes() == world["predecessor_bytes"]["forensic"]


def _assert_no_successor_effect(
    world: Mapping[str, Any],
    *,
    git_before: Mapping[str, str],
    pending_before: bytes | None,
) -> None:
    runtime = world["runtime"]
    pending = runtime / _CANONICAL_PENDING_NAME
    if pending_before is None:
        assert pending.exists() is False
    else:
        assert pending.read_bytes() == pending_before
    assert (runtime / "promotion_journal.yaml").exists() is False
    assert (runtime / "promotion_consumed.yaml").exists() is False
    pending_tests._assert_git_unchanged(world["repo"], git_before)
    _assert_predecessor_forensic_bytes(world)
    _assert_no_wakeup_marker(runtime)


def _rewrite_pending(world: Mapping[str, Any], mutator: Any) -> None:
    path = world["runtime"] / _CANONICAL_PENDING_NAME
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutator(payload)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _rewrite_successor_prestate_reason(
    world: Mapping[str, Any],
    reason_code: str,
) -> None:
    pending_path = world["runtime"] / _CANONICAL_PENDING_NAME
    pending = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    prestate = dict(pending["main_state_before_promotion"])
    prestate["reason_code"] = reason_code
    pending["main_state_before_promotion"] = prestate
    (world["runtime"] / "main_state.yaml").write_text(
        yaml.safe_dump(prestate, sort_keys=False),
        encoding="utf-8",
    )
    pending_path.write_text(
        yaml.safe_dump(pending, sort_keys=False),
        encoding="utf-8",
    )


def _replace_predecessor(
    world: dict[str, Any],
    outcome: DrainOutcome,
) -> None:
    paths = persist_drain_outcome(
        runtime_root=world["runtime"],
        outcome=outcome,
    )
    predecessor_paths = {
        **world["predecessor_paths"],
        **paths,
    }
    predecessor_bytes = {
        **world["predecessor_bytes"],
        **{
            name: path.read_bytes()
            for name, path in paths.items()
        },
    }
    world["predecessor"] = outcome
    world["predecessor_paths"] = predecessor_paths
    world["predecessor_bytes"] = predecessor_bytes


def _pre_start_blocked_outcome(
    world: Mapping[str, Any],
    **overrides: Any,
) -> DrainOutcome:
    predecessor_tock = world.get("pre_start_predecessor_tock_sha")
    if predecessor_tock is None:
        predecessor_tock = world["predecessor"].tock_sha
    assert isinstance(predecessor_tock, str) and predecessor_tock
    payload: dict[str, Any] = {
        "status": DrainStatus.BLOCKED,
        "drain_run_id": _PRE_START_BLOCKED_DRAIN_ID,
        "observed_main_head_sha": predecessor_tock,
        "reason_code": _PRE_START_BLOCKED_REASON,
    }
    payload.update(overrides)
    return DrainOutcome(**payload)


def _bind_pre_start_blocked_drain(world: dict[str, Any]) -> DrainOutcome:
    blocked = _pre_start_blocked_outcome(world)
    world["pre_start_predecessor_tock_sha"] = blocked.observed_main_head_sha
    _replace_predecessor(world, blocked)
    return blocked


def _apply_blocked_supersession_negative(
    world: dict[str, Any],
    case: str,
) -> tuple[Mapping[str, Any] | None, Any | None]:
    runtime = world["runtime"]
    pending_path = runtime / _CANONICAL_PENDING_NAME
    admission: Mapping[str, Any] | None = world["admit_kwargs"]
    main_head_reader = None
    if case == "blocked_reason":
        _replace_predecessor(
            world,
            _pre_start_blocked_outcome(
                world,
                reason_code="BLOCKED_UNCLEAR_GATE",
            ),
        )
    elif case == "blocked_run_id":
        _replace_predecessor(
            world,
            _pre_start_blocked_outcome(
                world,
                drain_run_id="foreign-drain-run",
            ),
        )
    elif case == "blocked_observed_head":
        _replace_predecessor(
            world,
            _pre_start_blocked_outcome(
                world,
                observed_main_head_sha=_REFERENCE_HEAD_SHA,
            ),
        )
    elif case == "pending_missing":
        pending_path.unlink()
    elif case == "pending_ambiguous":
        (runtime / "pending_promotion.json").write_text("{}\n", encoding="utf-8")
    elif case == "pending_malformed":
        pending_path.write_text("broken: [\n", encoding="utf-8")
    elif case == "pending_identity_incomplete":
        _rewrite_pending(world, lambda payload: payload.pop("promotion_class"))
    elif case == "pending_identity_foreign":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__("task_id", "foreign-task"),
        )
    elif case == "pending_base_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__("base_sha", _FOREIGN_BASE_SHA),
        )
    elif case == "prestate_missing_last_tock":
        _rewrite_pending(
            world,
            lambda payload: payload["main_state_before_promotion"].pop(
                "last_tock_sha"
            ),
        )
    elif case == "prestate_observed_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload["main_state_before_promotion"].__setitem__(
                "observed_main_head_sha",
                _REFERENCE_HEAD_SHA,
            ),
        )
    elif case in _BLOCKED_SUPERSESSION_REASON_NEGATIVES:
        _rewrite_successor_prestate_reason(
            world,
            _BLOCKED_SUPERSESSION_REASON_NEGATIVES[case],
        )
    elif case == "live_head_mismatch":
        main_head_reader = lambda _repo: _REFERENCE_HEAD_SHA
    elif case == "admission_missing":
        admission = None
    elif case == "admission_incomplete":
        admission = {"runtime_root": runtime}
    else:
        raise AssertionError(f"unknown blocked-supersession negative: {case}")
    return admission, main_head_reader


def _apply_qualification_negative(
    world: dict[str, Any],
    case: str,
) -> None:
    runtime = world["runtime"]
    pending_path = runtime / _CANONICAL_PENDING_NAME
    predecessor = world["predecessor"]
    pending = load_pending_promotion(runtime_root=runtime)
    if case == "pending_missing":
        pending_path.unlink()
    elif case == "pending_alternate":
        (runtime / "pending_promotion.json").write_text("{}\n", encoding="utf-8")
    elif case == "pending_malformed":
        pending_path.write_text("broken: [\n", encoding="utf-8")
    elif case == "pending_incomplete":
        _rewrite_pending(world, lambda payload: payload.pop("promotion_class"))
    elif case == "same_task_id":
        _replace_predecessor(
            world,
            _predecessor_outcome(world, task_id=pending["task_id"]),
        )
    elif case == "same_full_identity":
        _replace_predecessor(
            world,
            _predecessor_outcome(
                world,
                task_id=pending["task_id"],
                origin_run_id=pending["origin_run_id"],
                promotion_attempt_id=pending["promotion_attempt_id"],
                result_sha=pending["result_sha"],
                promotion_class=pending["promotion_class"],
            ),
        )
    elif case == "base_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__("base_sha", _FOREIGN_BASE_SHA),
        )
    elif case == "prestate_observed_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload["main_state_before_promotion"].__setitem__(
                "observed_main_head_sha",
                _REFERENCE_HEAD_SHA,
            ),
        )
    elif case == "prestate_last_tock_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload["main_state_before_promotion"].__setitem__(
                "last_tock_sha",
                _REFERENCE_HEAD_SHA,
            ),
        )
    elif case == "prestate_result_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload["main_state_before_promotion"].__setitem__(
                "validated_result_sha",
                _FOREIGN_RESULT_SHA,
            ),
        )
    elif case == "prestate_reason_mismatch":
        _rewrite_pending(
            world,
            lambda payload: payload["main_state_before_promotion"].__setitem__(
                "reason_code",
                "STATE_READY_SYNTHETIC",
            ),
        )
    elif case == "old_identity_incomplete":
        old_payload = predecessor.to_mapping()
        old_payload.pop("origin_run_id")
        (runtime / _CANONICAL_OUTCOME_NAME).write_text(
            yaml.safe_dump(old_payload, sort_keys=False),
            encoding="utf-8",
        )
    elif case == "old_observed_tock_mismatch":
        _replace_predecessor(
            world,
            _predecessor_outcome(
                world,
                observed_main_head_sha=_REFERENCE_HEAD_SHA,
            ),
        )
    elif case == "old_reason_not_admitted":
        _replace_predecessor(
            world,
            _predecessor_outcome(
                world,
                reason_code="RECONCILIATION_COMPLETED",
            ),
        )
    else:
        raise AssertionError(f"unknown qualification-negative case: {case}")


def test_completed_drain_epoch_is_superseded_only_by_bound_successor_pending(
    tmp_path: Path,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    runtime = world["runtime"]
    canonical = world["predecessor_paths"]["authoritative"]
    assert canonical.read_bytes() == world["predecessor_bytes"]["authoritative"]
    _assert_predecessor_forensic_bytes(world)
    _assert_real_admission_symbol()

    decision = _evaluate(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id="drain-run-successor",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.outcome.reason_code == "PROMOTION_EFFECT_ADMITTED"
    assert decision.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert decision.outcome.status is DrainStatus.DRAINED
    assert decision.outcome.task_id == world["intent"].task_id
    assert decision.outcome.origin_run_id == world["intent"].origin_run_id
    assert (
        decision.outcome.promotion_attempt_id
        == world["intent"].promotion_attempt_id
    )
    assert decision.outcome.result_sha == world["intent"].result_sha
    assert decision.outcome.promotion_class == world["intent"].promotion_class
    assert decision.outcome.observed_main_head_sha == decision.outcome.tock_sha
    assert decision.outcome.tock_sha != world["predecessor"].tock_sha
    assert (
        pending_tests._run_git(world["repo"], "rev-parse", "HEAD")
        == decision.outcome.tock_sha
    )
    assert (runtime / _CANONICAL_PENDING_NAME).exists() is False
    assert (runtime / "promotion_consumed.yaml").is_file()
    assert canonical.read_bytes() != world["predecessor_bytes"]["authoritative"]
    _assert_predecessor_forensic_bytes(world)


@pytest.mark.parametrize(
    "case",
    ("strict_loader_invalid", "non_mapping_prestate"),
)
def test_completed_drain_epoch_rejects_unbound_pending_projection_without_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    pending_path = world["runtime"] / _CANONICAL_PENDING_NAME
    if case == "strict_loader_invalid":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__(
                "artifact_class",
                "FOREIGN_PROMOTION_INTENT",
            ),
        )
        with pytest.raises(
            RunnerGitPromotionError,
            match="PROMOTION_PENDING_INVALID",
        ):
            load_pending_promotion(runtime_root=world["runtime"])
    else:
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__(
                "main_state_before_promotion",
                ["not", "a", "mapping"],
            ),
        )
        assert not isinstance(
            load_pending_promotion(runtime_root=world["runtime"])[
                "main_state_before_promotion"
            ],
            Mapping,
        )
    pending_before = pending_path.read_bytes()
    git_before = pending_tests._git_snapshot(world["repo"])
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("effect admission must not run"),
    )

    decision = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=f"drain-unbound-pending-{case}",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "DRAIN_OUTCOME_MAIN_HEAD_DRIFT"
    assert decision.outcome.tock_sha is None
    _assert_no_successor_effect(
        world,
        git_before=git_before,
        pending_before=pending_before,
    )


def test_malformed_and_blocked_predecessor_drain_epochs_remain_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed = _prepare_drain_epoch_world(tmp_path / "malformed")
    malformed_path = malformed["runtime"] / _CANONICAL_OUTCOME_NAME
    malformed_path.write_text("broken: [\n", encoding="utf-8")
    malformed_git = pending_tests._git_snapshot(malformed["repo"])
    malformed_pending = (
        malformed["runtime"] / _CANONICAL_PENDING_NAME
    ).read_bytes()
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("effect admission must not run"),
    )
    malformed_decision = _evaluate(
        repo=malformed["repo"],
        runtime=malformed["runtime"],
        drain_run_id="drain-malformed-old",
        effect_admission=malformed["admit_kwargs"],
    )
    assert malformed_decision.phase is SchedulerPhase.BLOCKED
    assert malformed_decision.outcome.reason_code == "DRAIN_OUTCOME_INVALID"
    _assert_no_successor_effect(
        malformed,
        git_before=malformed_git,
        pending_before=malformed_pending,
    )

    blocked = _prepare_drain_epoch_world(tmp_path / "blocked")
    _replace_predecessor(
        blocked,
        DrainOutcome(
            status=DrainStatus.BLOCKED,
            drain_run_id="drain-run-predecessor",
            observed_main_head_sha=blocked["predecessor"].tock_sha,
            reason_code="BLOCKED_UNCLEAR_GATE",
        ),
    )
    blocked_git = pending_tests._git_snapshot(blocked["repo"])
    blocked_pending = (
        blocked["runtime"] / _CANONICAL_PENDING_NAME
    ).read_bytes()
    blocked_decision = _evaluate(
        repo=blocked["repo"],
        runtime=blocked["runtime"],
        drain_run_id="drain-blocked-old",
        effect_admission=blocked["admit_kwargs"],
    )
    assert blocked_decision.phase is SchedulerPhase.BLOCKED
    assert blocked_decision.outcome.reason_code == "PRIOR_DRAIN_BLOCKED"
    _assert_no_successor_effect(
        blocked,
        git_before=blocked_git,
        pending_before=blocked_pending,
    )


def test_identical_predecessor_head_remains_idempotent_without_consuming_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    predecessor = world["predecessor"]
    pending_tests._run_git(
        world["repo"],
        "update-ref",
        "refs/heads/main",
        predecessor.tock_sha,
        world["start_sha"],
    )
    pending_tests._run_git(world["repo"], "reset", "--hard", predecessor.tock_sha)
    git_before = pending_tests._git_snapshot(world["repo"])
    pending_before = (
        world["runtime"] / _CANONICAL_PENDING_NAME
    ).read_bytes()
    canonical_before = (
        world["runtime"] / _CANONICAL_OUTCOME_NAME
    ).read_bytes()
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("effect admission must not run"),
    )

    decision = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id="drain-identical-head",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert decision.outcome == predecessor
    assert (
        world["runtime"] / _CANONICAL_OUTCOME_NAME
    ).read_bytes() == canonical_before
    _assert_no_successor_effect(
        world,
        git_before=git_before,
        pending_before=pending_before,
    )


@pytest.mark.parametrize("case", _QUALIFICATION_NEGATIVE_CASES)
def test_noncanonical_epoch_links_never_reach_effect_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    _apply_qualification_negative(world, case)
    git_before = pending_tests._git_snapshot(world["repo"])
    pending_path = world["runtime"] / _CANONICAL_PENDING_NAME
    pending_before = pending_path.read_bytes() if pending_path.is_file() else None
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("effect admission must not run"),
    )

    decision = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=f"drain-negative-{case}",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.tock_sha is None
    _assert_no_successor_effect(
        world,
        git_before=git_before,
        pending_before=pending_before,
    )


@pytest.mark.parametrize(
    ("case", "expected_reason", "expected_entries"),
    (
        ("missing", "DRAIN_EFFECT_ADMISSION_MISSING", 0),
        ("wrong_root", "DRAIN_EFFECT_ADMISSION_ROOT_MISMATCH", 0),
        ("failing", "PROMOTION_UNRESOLVED_FOREIGN_EPOCH", 1),
    ),
)
def test_admission_negatives_stop_before_effect_consumption_and_tock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_reason: str,
    expected_entries: int,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    runtime = world["runtime"]
    git_before = pending_tests._git_snapshot(world["repo"])
    entries = 0

    def counted_real_admission(**kwargs: Any) -> dict[str, Any]:
        nonlocal entries
        entries += 1
        return admit_pending_promotion_effect(**kwargs)

    _patch_step_16(monkeypatch, counted_real_admission)
    admission: Mapping[str, Any] | None
    if case == "missing":
        admission = None
    else:
        admission = dict(world["admit_kwargs"])
        if case == "wrong_root":
            foreign_runtime = tmp_path / "foreign-runtime"
            foreign_runtime.mkdir()
            admission["runtime_root"] = foreign_runtime
        else:
            admission["unresolved_foreign_epoch"] = True

    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=runtime,
        drain_run_id=f"drain-admission-negative-{case}",
        effect_admission=admission,
    )

    assert entries == expected_entries
    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.reason_code == expected_reason
    assert decision.outcome.tock_sha is None
    _assert_idle_pending_world(world, git_before)
    assert (runtime / "promotion_journal.yaml").exists() is False


def test_bound_successor_pending_supersedes_exact_pre_start_blocked_drain(
    tmp_path: Path,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    predecessor_tock = world["predecessor"].tock_sha
    blocked = _bind_pre_start_blocked_drain(world)
    pending = load_pending_promotion(runtime_root=world["runtime"])
    live_head = pending_tests._run_git(world["repo"], "rev-parse", "HEAD")

    assert blocked.to_mapping() == {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "status": "BLOCKED",
        "drain_run_id": _PRE_START_BLOCKED_DRAIN_ID,
        "observed_main_head_sha": predecessor_tock,
        "reason_code": _PRE_START_BLOCKED_REASON,
    }
    assert live_head == world["start_sha"]
    assert pending["base_sha"] == live_head
    assert (
        pending["main_state_before_promotion"]["observed_main_head_sha"]
        == live_head
    )
    assert (
        pending["main_state_before_promotion"]["last_tock_sha"]
        == predecessor_tock
    )
    assert pending["main_state_before_promotion"]["validated_result_sha"] == (
        world["prior_sha"]
    )

    decision = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id="drain-run-successor",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.outcome.reason_code == "PROMOTION_EFFECT_ADMITTED", (
        "BOUND_SUCCESSOR_PENDING_CANNOT_SUPERSEDE_PRE_START_BLOCKED_DRAIN: "
        f"observed {decision.outcome.reason_code}"
    )
    assert decision.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert decision.outcome.status is DrainStatus.DRAINED
    assert decision.outcome.task_id == world["intent"].task_id
    assert decision.outcome.observed_main_head_sha == decision.outcome.tock_sha
    assert decision.outcome.tock_sha != predecessor_tock
    assert (world["runtime"] / _CANONICAL_PENDING_NAME).exists() is False
    assert (world["runtime"] / "promotion_consumed.yaml").is_file()
    _assert_predecessor_forensic_bytes(world)


def test_reconciliation_completed_pre_start_blocked_drain_requires_exact_successor_supersession(
    tmp_path: Path,
) -> None:
    world = _prepare_drain_epoch_world(
        tmp_path,
        prestate_reason_code="RECONCILIATION_COMPLETED",
    )
    predecessor_tock = world["predecessor"].tock_sha
    blocked = _bind_pre_start_blocked_drain(world)
    pending = load_pending_promotion(runtime_root=world["runtime"])
    live_head = pending_tests._run_git(world["repo"], "rev-parse", "HEAD")
    expected_prestate = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": world["prior_sha"],
        "last_tock_sha": predecessor_tock,
        "observed_main_head_sha": live_head,
        "reason_code": "RECONCILIATION_COMPLETED",
        "updated_at_utc": _SUCCESSOR_UPDATED_AT,
    }

    assert blocked.to_mapping() == {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "status": "BLOCKED",
        "drain_run_id": _PRE_START_BLOCKED_DRAIN_ID,
        "observed_main_head_sha": predecessor_tock,
        "reason_code": _PRE_START_BLOCKED_REASON,
    }
    assert live_head == world["start_sha"]
    assert pending["base_sha"] == live_head
    assert yaml.safe_load(
        (world["runtime"] / "main_state.yaml").read_text(encoding="utf-8")
    ) == expected_prestate
    assert pending["main_state_before_promotion"] == expected_prestate

    decision = _evaluate(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id="drain-run-recovery-successor",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.outcome.reason_code == "PROMOTION_EFFECT_ADMITTED", (
        "RECOVERY_RECONCILED_PRE_START_DRAIN_NOT_SUPERSEDED: "
        "expected PROMOTION_EFFECT_ADMITTED; "
        f"observed {decision.outcome.reason_code}"
    )
    assert decision.phase is SchedulerPhase.DRAINED_AWAITING_TICK
    assert decision.outcome.status is DrainStatus.DRAINED
    assert decision.outcome.task_id == world["intent"].task_id
    assert decision.outcome.observed_main_head_sha == decision.outcome.tock_sha
    assert decision.outcome.tock_sha != predecessor_tock
    assert (world["runtime"] / _CANONICAL_PENDING_NAME).exists() is False
    assert (world["runtime"] / "promotion_consumed.yaml").is_file()
    _assert_predecessor_forensic_bytes(world)


@pytest.mark.parametrize("case", _BLOCKED_SUPERSESSION_NEGATIVE_CASES)
def test_pre_start_blocked_drain_supersession_requires_complete_exact_geometry(
    tmp_path: Path,
    case: str,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    _bind_pre_start_blocked_drain(world)
    admission, main_head_reader = _apply_blocked_supersession_negative(
        world,
        case,
    )
    git_before = pending_tests._git_snapshot(world["repo"])
    pending_path = world["runtime"] / _CANONICAL_PENDING_NAME
    pending_before = pending_path.read_bytes() if pending_path.is_file() else None

    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=f"drain-blocked-negative-{case}",
        effect_admission=admission,
        main_head_reader=main_head_reader,
    )

    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.tock_sha is None
    if case in _BLOCKED_SUPERSESSION_REASON_NEGATIVES:
        assert decision.outcome.reason_code == "PRIOR_DRAIN_BLOCKED"
    _assert_no_successor_effect(
        world,
        git_before=git_before,
        pending_before=pending_before,
    )


_SUCCESSOR_PENDING_SHAPE_GUARD_CASES = (
    "non_mapping_main_state_before_promotion",
    "top_level_identity_incomplete_or_non_string",
    "snapshot_role_not_mapping",
    "snapshot_current_or_bound_not_mapping",
)


def _apply_successor_pending_shape_guard(
    world: Mapping[str, Any],
    case: str,
) -> None:
    if case == "non_mapping_main_state_before_promotion":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__(
                "main_state_before_promotion",
                ["not", "a", "mapping"],
            ),
        )
        return
    if case == "top_level_identity_incomplete_or_non_string":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__(
                "task_id",
                ["incomplete", "identity"],
            ),
        )
        return
    if case == "snapshot_role_not_mapping":
        _rewrite_pending(
            world,
            lambda payload: payload.__setitem__(
                "orchestrator_decision",
                ["not", "a", "mapping"],
            ),
        )
        return
    if case == "snapshot_current_or_bound_not_mapping":
        _rewrite_pending(
            world,
            lambda payload: payload["instruction_snapshot"].__setitem__(
                "bound",
                ["not", "a", "mapping"],
            ),
        )
        return
    raise AssertionError(f"unknown successor pending shape guard: {case}")


def _assert_shape_guard_reached_closed_return(
    world: Mapping[str, Any],
    case: str,
) -> None:
    from task_transition_runner.runner_promotion_state import (
        load_and_validate_main_state,
    )

    runtime = world["runtime"]
    pending_kind, _identity, pending_document = drain_module._inspect_pending(runtime)
    pending = load_pending_promotion(runtime_root=runtime)
    persisted_prestate = load_and_validate_main_state(
        main_state_path=runtime / "main_state.yaml",
    )
    assert pending_kind == "present"
    assert pending_document is not None
    assert isinstance(persisted_prestate, Mapping)
    prestate = pending.get("main_state_before_promotion")
    if case == "non_mapping_main_state_before_promotion":
        assert not isinstance(prestate, Mapping)
    elif case == "top_level_identity_incomplete_or_non_string":
        assert isinstance(prestate, Mapping)
        assert not isinstance(pending.get("task_id"), str)
    elif case == "snapshot_role_not_mapping":
        assert isinstance(prestate, Mapping)
        assert isinstance(pending.get("task_id"), str)
        assert pending.get("task_id") != ""
        assert not isinstance(pending.get("orchestrator_decision"), Mapping)
    else:
        assert isinstance(prestate, Mapping)
        assert isinstance(pending.get("task_snapshot"), Mapping)
        assert isinstance(pending.get("instruction_snapshot"), Mapping)
        assert isinstance(pending.get("orchestrator_decision"), Mapping)
        assert isinstance(pending.get("validator_decision"), Mapping)
        assert not isinstance(pending["instruction_snapshot"].get("bound"), Mapping)
    assert (
        drain_module._bound_successor_pending_supersedes_pre_start_blocked(
            runtime=runtime,
            prior=world["predecessor"],
            live_head=pending_tests._run_git(world["repo"], "rev-parse", "HEAD"),
            pending_kind=pending_kind,
            pending_document=pending_document,
        )
        is False
    )


@pytest.mark.parametrize("case", _SUCCESSOR_PENDING_SHAPE_GUARD_CASES)
def test_successor_pending_supersession_shape_guards_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    world = _prepare_drain_epoch_world(tmp_path)
    _bind_pre_start_blocked_drain(world)
    _apply_successor_pending_shape_guard(world, case)
    _assert_shape_guard_reached_closed_return(world, case)
    git_before = pending_tests._git_snapshot(world["repo"])
    pending_path = world["runtime"] / _CANONICAL_PENDING_NAME
    pending_before = pending_path.read_bytes()
    _patch_step_16(
        monkeypatch,
        lambda **_kwargs: pytest.fail("effect admission must not run"),
    )

    decision = _evaluate_fail_closed(
        repo=world["repo"],
        runtime=world["runtime"],
        drain_run_id=f"drain-shape-guard-{case}",
        effect_admission=world["admit_kwargs"],
    )

    assert decision.phase is SchedulerPhase.BLOCKED
    assert decision.outcome.status is DrainStatus.BLOCKED
    assert decision.outcome.reason_code == "PRIOR_DRAIN_BLOCKED"
    assert decision.outcome.tock_sha is None
    _assert_no_successor_effect(
        world,
        git_before=git_before,
        pending_before=pending_before,
    )
