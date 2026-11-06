"""Canonical First-Parent observation and ordinary reconciliation.

Effect authority exists only for a single parseable TOCK on
``git rev-list --first-parent <main_ref>``. Metadata is accepted only
through ``parse_canonical_tock``. More than one First-Parent envelope
with the same Promotion-Attempt blocks before base, parent, or tree
acceptance. Named backup, recovery, task, and worktree refs, and side
parents, can explain a noncanonical absence. They neither authorize an
effect nor count as a duplicate.

Ordinary reconciliation reads pending, journal, main-state and
consumption through their public contracts. It verifies one canonical
TOCK, then advances journal, main-state and consumption only forward.
An existing TOCK is never rewritten. No-work and identical finalized
truth do not write. Conflicting identity, geometry, main-state or
consumption blocks without repair. After that finalized truth, one
matching ordinary continuation is consumed in place. An AUTHORIZED
continuation stays deferred, without rewrite or consumption, only when
its loaded predecessor identity and bound TOCK are exactly this
finalized journal and its successor task is a different task. Any
other continuation keeps the existing consumption result. An identical
consumed artifact is reproved without rewrite. This is journaled
monotone convergence over the supported step-4 windows, not a
cross-file transaction and not an exactly-once guarantee. The module
does not write refs, the index, or the worktree.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from task_transition_runner.runner_continuation_contract import (
    ContinuationContractView,
    ContinuationError,
    load_continuation_artifact,
)
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    load_pending_promotion,
    parse_canonical_tock,
)
from task_transition_runner.runner_post_tick_continuation import (
    apply_successor_continuation_consumption,
)
from task_transition_runner.runner_promotion_journal import (
    PromotionJournal,
    PromotionJournalError,
    PromotionJournalPhase,
    create_promotion_journal,
    load_promotion_journal,
    persist_promotion_journal,
    transition_promotion_journal,
)
from task_transition_runner.runner_promotion_state import (
    RunnerPromotionStateError,
    load_and_validate_main_state,
    load_promotion_consumed,
    record_promotion_consumed,
    write_main_state,
)


class FirstParentObservationStatus(Enum):
    """Public observation outcomes for one Promotion-Attempt."""

    AUTHORIZED = "AUTHORIZED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class FirstParentTockObservation:
    """Read-only verdict for one configured main ref."""

    status: FirstParentObservationStatus
    effect_authorized: bool
    reason_code: str | None
    tock_sha: str | None
    main_ref: str


@dataclass(frozen=True)
class _CommitView:
    tree_oid: str
    parents: tuple[str, ...]
    message: str


@dataclass(frozen=True)
class _Envelope:
    sha: str
    message: str
    parents: tuple[str, ...]
    tree_oid: str
    promotion_attempt_id: str


def observe_canonical_first_parent_tock(
    *,
    repo: Path | str,
    main_ref: str,
    promotion_attempt_id: str,
    validated_tree_oid: str,
    diagnostic_refs: Mapping[str, str] | None = None,
) -> FirstParentTockObservation:
    """Observe one immutable Promotion-Attempt on the main First-Parent chain."""
    repository = Path(repo)
    chain_shas = _first_parent_shas(repository, main_ref)
    side_parent_shas: list[str] = []
    for sha in chain_shas:
        side_parent_shas.extend(_read_commit(repository, sha).parents[1:])
    sought = _with_attempt(
        _envelopes(repository, chain_shas),
        promotion_attempt_id,
    )
    if len(sought) > 1:
        return _blocked(main_ref, "RECONCILIATION_TOCK_DUPLICATE")
    if len(sought) == 1:
        winner = sought[0]
        parse_canonical_tock(
            winner.message,
            winner.parents,
            winner.tree_oid,
            validated_tree_oid=validated_tree_oid,
        )
        return _authorized(main_ref, winner.sha)
    if _noncanonical_attempt(
        repository,
        promotion_attempt_id,
        side_parent_shas,
        diagnostic_refs,
    ):
        return _blocked(main_ref, "RECONCILIATION_TOCK_NONCANONICAL")
    return _blocked(main_ref, "RECONCILIATION_TOCK_MISSING")


def _blocked(main_ref: str, reason_code: str) -> FirstParentTockObservation:
    return FirstParentTockObservation(
        status=FirstParentObservationStatus.BLOCKED,
        effect_authorized=False,
        reason_code=reason_code,
        tock_sha=None,
        main_ref=main_ref,
    )


def _authorized(main_ref: str, tock_sha: str) -> FirstParentTockObservation:
    return FirstParentTockObservation(
        status=FirstParentObservationStatus.AUTHORIZED,
        effect_authorized=True,
        reason_code=None,
        tock_sha=tock_sha,
        main_ref=main_ref,
    )


def _with_attempt(
    envelopes: list[_Envelope],
    promotion_attempt_id: str,
) -> list[_Envelope]:
    return [
        envelope
        for envelope in envelopes
        if envelope.promotion_attempt_id == promotion_attempt_id
    ]


def _noncanonical_attempt(
    repository: Path,
    promotion_attempt_id: str,
    side_parent_shas: list[str],
    diagnostic_refs: Mapping[str, str] | None,
) -> bool:
    diagnostic_shas = _diagnostic_tip_shas(repository, diagnostic_refs)
    outside = _envelopes(repository, (*side_parent_shas, *diagnostic_shas))
    return bool(_with_attempt(outside, promotion_attempt_id))


def _diagnostic_tip_shas(
    repository: Path,
    diagnostic_refs: Mapping[str, str] | None,
) -> tuple[str, ...]:
    if diagnostic_refs is None:
        return ()
    return tuple(
        _git_text(repository, "rev-parse", "--verify", ref).strip()
        for ref in diagnostic_refs.values()
    )


def _first_parent_shas(repository: Path, main_ref: str) -> tuple[str, ...]:
    output = _git_text(repository, "rev-list", "--first-parent", main_ref)
    return tuple(output.splitlines())


def _envelopes(repository: Path, shas: tuple[str, ...] | list[str]) -> list[_Envelope]:
    envelopes: list[_Envelope] = []
    for sha in shas:
        envelope = _envelope_from_commit(sha, _read_commit(repository, sha))
        if envelope is not None:
            envelopes.append(envelope)
    return envelopes


def _envelope_from_commit(sha: str, commit: _CommitView) -> _Envelope | None:
    try:
        parsed = parse_canonical_tock(
            commit.message,
            commit.parents,
            commit.tree_oid,
            validated_tree_oid=commit.tree_oid,
        )
    except RunnerGitPromotionError:
        return None
    return _Envelope(
        sha=sha,
        message=commit.message,
        parents=commit.parents,
        tree_oid=commit.tree_oid,
        promotion_attempt_id=str(parsed["promotion_attempt_id"]),
    )


def _read_commit(repository: Path, sha: str) -> _CommitView:
    raw = _git_text(repository, "cat-file", "-p", sha)
    header, _, message = raw.partition("\n\n")
    tree_oid = ""
    parents: list[str] = []
    for line in header.splitlines():
        kind, _, value = line.partition(" ")
        if kind == "tree":
            tree_oid = value
        elif kind == "parent":
            parents.append(value)
    return _CommitView(
        tree_oid=tree_oid,
        parents=tuple(parents),
        message=message,
    )


def _git_text(repository: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


_JOURNAL_NAME = "promotion_journal.yaml"
_MAIN_STATE_NAME = "main_state.yaml"
_CONSUMPTION_NAME = "promotion_consumed.yaml"
_CONTINUATION_NAME = "post_tick_continuation.yaml"
_CONTINUATION_AUTHORIZED = "AUTHORIZED"
_CONTINUATION_ALREADY_CONSUMED = "CONTINUATION_ALREADY_CONSUMED"
_POST_STATE = "GREEN_VALIDATED"
_POST_MAIN_BRANCH = "main"
_PHASE_ORDER = tuple(PromotionJournalPhase)
_GIT_APPLIED_INDEX = _PHASE_ORDER.index(PromotionJournalPhase.GIT_APPLIED)
_TOCK_GEOMETRY_REASONS = {
    "PROMOTION_TOCK_PARENT_MISMATCH": "RECONCILIATION_TOCK_PARENT_MISMATCH",
    "PROMOTION_TOCK_TREE_MISMATCH": "RECONCILIATION_TOCK_TREE_MISMATCH",
}


@dataclass(frozen=True)
class OrdinaryReconciliationVerdict:
    """Monotone ordinary outcome. Git effects are never created here."""

    reason_code: str
    git_effect_created: bool
    journal_phase: str | None = None
    tock_sha: str | None = None
    task_id: str | None = None
    origin_run_id: str | None = None
    promotion_attempt_id: str | None = None
    result_sha: str | None = None
    drain_run_id: str | None = None


@dataclass(frozen=True)
class _TockMatch:
    sha: str
    parsed: dict[str, Any]


def reconcile_ordinary_promotion(
    *,
    repo: Path | str,
    runtime_root: Path | str,
    drain_run_id: str,
    main_ref: str,
    diagnostic_refs: Mapping[str, str] | None,
) -> OrdinaryReconciliationVerdict:
    """Converge one ordinary promotion without repeating a Git effect."""
    runtime = Path(runtime_root)
    repository = Path(repo)
    pending, pending_reason = _read_pending(runtime)
    if pending_reason is not None:
        return _blocked_ordinary(pending_reason)
    journal, journal_reason = _read_journal(runtime / _JOURNAL_NAME)
    if journal_reason is not None:
        return _blocked_ordinary(journal_reason)
    if pending is None and journal is None:
        return _blocked_ordinary("RECONCILIATION_NO_WORK")
    if pending is not None and not _pending_axes_agree(pending):
        return _blocked_ordinary("RECONCILIATION_IDENTITY_MISMATCH")
    if (
        pending is not None
        and journal is not None
        and _pending_bound(pending) != _journal_bound(journal)
    ):
        return _blocked_ordinary("RECONCILIATION_IDENTITY_MISMATCH")
    if pending is not None and journal is not None:
        anchor_reason = _pending_journal_anchor_reason(pending, journal)
        if anchor_reason is not None:
            return _blocked_ordinary(anchor_reason)
    if journal is None:
        return _reconstruct_pending_journal(runtime, pending, drain_run_id)
    if pending is None and journal.phase is PromotionJournalPhase.PENDING:
        return _blocked_ordinary("RECONCILIATION_PENDING_INVALID")
    return _converge_existing_journal(
        repository=repository,
        runtime=runtime,
        journal=journal,
        pending=pending,
        drain_run_id=drain_run_id,
        main_ref=main_ref,
        diagnostic_refs=diagnostic_refs,
    )


def _blocked_ordinary(reason_code: str) -> OrdinaryReconciliationVerdict:
    return OrdinaryReconciliationVerdict(
        reason_code=reason_code,
        git_effect_created=False,
    )


def _completed_ordinary(
    journal: PromotionJournal,
    tock_sha: str | None,
) -> OrdinaryReconciliationVerdict:
    return OrdinaryReconciliationVerdict(
        reason_code="RECONCILIATION_COMPLETED",
        git_effect_created=False,
        journal_phase=journal.phase.value,
        tock_sha=tock_sha,
        task_id=journal.task_id,
        origin_run_id=journal.origin_run_id,
        promotion_attempt_id=journal.promotion_attempt_id,
        result_sha=journal.result_sha,
        drain_run_id=journal.drain_run_id,
    )


def _read_pending(runtime: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return load_pending_promotion(runtime_root=runtime), None
    except RunnerGitPromotionError as exc:
        if exc.reason_code == "PROMOTION_PENDING_MISSING":
            return None, None
        return None, "RECONCILIATION_PENDING_INVALID"


def _read_journal(path: Path) -> tuple[PromotionJournal | None, str | None]:
    try:
        return load_promotion_journal(path=path), None
    except PromotionJournalError as exc:
        if exc.reason_code == "JOURNAL_NOT_FOUND":
            return None, None
        if exc.reason_code == "JOURNAL_FIELDS_INVALID":
            return None, "JOURNAL_FIELDS_INVALID"
        return None, "RECONCILIATION_JOURNAL_INVALID"


def _identity_axes(value: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(value["task_id"]),
        str(value["origin_run_id"]),
        str(value["promotion_attempt_id"]),
        str(value["result_sha"]),
    )


def _pending_axes_agree(pending: Mapping[str, Any]) -> bool:
    identities = {
        _identity_axes(pending),
        _identity_axes(pending["orchestrator_decision"]),
        _identity_axes(pending["validator_decision"]),
    }
    return len(identities) == 1


def _pending_bound(pending: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        *_identity_axes(pending),
        str(pending["promotion_class"]),
        str(pending["base_sha"]),
    )


def _journal_bound(journal: PromotionJournal) -> tuple[str, ...]:
    return (
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.base_main_sha,
    )


def _parsed_bound(parsed: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(parsed["task_id"]),
        str(parsed["origin_run_id"]),
        str(parsed["promotion_attempt_id"]),
        str(parsed["result_sha"]),
        str(parsed["promotion_class"]),
        str(parsed["expected_base_sha"]),
    )


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _admitted_pending_journal_anchors(
    pending: Mapping[str, Any],
) -> dict[str, str]:
    """Copy the three anchors already stored on the admitted pending document."""
    task_bound = pending["task_snapshot"]["bound"]
    instruction_bound = pending["instruction_snapshot"]["bound"]
    return {
        "task_fingerprint": task_bound["fingerprint"],
        "task_snapshot_sha256": task_bound["digest"],
        "instruction_signature_sha256": instruction_bound["digest"],
    }


def _pending_journal_anchor_reason(
    pending: Mapping[str, Any],
    journal: PromotionJournal,
) -> str | None:
    try:
        admitted = _admitted_pending_journal_anchors(pending)
    except (KeyError, TypeError):
        return "JOURNAL_FIELDS_INVALID"
    if (
        admitted["task_fingerprint"],
        admitted["task_snapshot_sha256"],
        admitted["instruction_signature_sha256"],
    ) != (
        journal.task_fingerprint,
        journal.task_snapshot_sha256,
        journal.instruction_signature_sha256,
    ):
        return "JOURNAL_FIELD_DRIFT"
    return None


def _reconstruct_pending_journal(
    runtime: Path,
    pending: dict[str, Any],
    drain_run_id: str,
) -> OrdinaryReconciliationVerdict:
    try:
        admitted = _admitted_pending_journal_anchors(pending)
        journal = create_promotion_journal(
            task_id=str(pending["task_id"]),
            origin_run_id=str(pending["origin_run_id"]),
            drain_run_id=drain_run_id,
            promotion_attempt_id=str(pending["promotion_attempt_id"]),
            promotion_class=str(pending["promotion_class"]),
            base_main_sha=str(pending["base_sha"]),
            result_sha=str(pending["result_sha"]),
            validated_result_sha=str(pending["result_sha"]),
            task_fingerprint=admitted["task_fingerprint"],
            task_snapshot_sha256=admitted["task_snapshot_sha256"],
            instruction_signature_sha256=admitted[
                "instruction_signature_sha256"
            ],
            pending_at_utc=_utc_stamp(),
            reason_code="RECONCILIATION_COMPLETED",
        )
    except (KeyError, TypeError):
        return _blocked_ordinary("JOURNAL_FIELDS_INVALID")
    except PromotionJournalError as exc:
        return _blocked_ordinary(exc.reason_code)
    try:
        persist_promotion_journal(path=runtime / _JOURNAL_NAME, journal=journal)
    except PromotionJournalError:
        return _blocked_ordinary("RECONCILIATION_WRITE_FAILED")
    return _completed_ordinary(journal, None)


def _converge_existing_journal(
    *,
    repository: Path,
    runtime: Path,
    journal: PromotionJournal,
    pending: dict[str, Any] | None,
    drain_run_id: str,
    main_ref: str,
    diagnostic_refs: Mapping[str, str] | None,
) -> OrdinaryReconciliationVerdict:
    scanned = _scan_first_parent_tock(
        repository,
        main_ref,
        journal.promotion_attempt_id,
    )
    if isinstance(scanned, str):
        return _blocked_ordinary(scanned)
    if scanned is None:
        return _without_canonical_tock(
            repository=repository,
            runtime=runtime,
            journal=journal,
            main_ref=main_ref,
            diagnostic_refs=diagnostic_refs,
        )
    if _journal_bound(journal) != _parsed_bound(scanned.parsed):
        if journal.phase is PromotionJournalPhase.FINALIZED:
            return _blocked_ordinary("RECONCILIATION_IDENTITY_MISMATCH")
        return _blocked_ordinary("RECONCILIATION_TOCK_METADATA_MISMATCH")
    if (
        _PHASE_ORDER.index(journal.phase) >= _GIT_APPLIED_INDEX
        and journal.tock_sha != scanned.sha
    ):
        return _blocked_ordinary("RECONCILIATION_IDENTITY_MISMATCH")
    return _finish_canonical_tock(
        repository=repository,
        runtime=runtime,
        journal=journal,
        pending=pending,
        drain_run_id=drain_run_id,
        main_ref=main_ref,
        tock_sha=scanned.sha,
    )


def _scan_first_parent_tock(
    repository: Path,
    main_ref: str,
    promotion_attempt_id: str,
) -> _TockMatch | str | None:
    shas = _first_parent_shas(repository, main_ref)
    head = shas[0]
    matches: list[_TockMatch] = []
    head_geometry: str | None = None
    parsed_head: dict[str, Any] | None = None
    for sha in shas:
        commit = _read_commit(repository, sha)
        try:
            parsed = parse_canonical_tock(
                commit.message,
                commit.parents,
                commit.tree_oid,
                validated_tree_oid=commit.tree_oid,
            )
        except RunnerGitPromotionError as exc:
            mapped = _TOCK_GEOMETRY_REASONS.get(exc.reason_code)
            if sha == head and mapped is not None:
                head_geometry = mapped
            continue
        if sha == head:
            parsed_head = parsed
        if str(parsed["promotion_attempt_id"]) != promotion_attempt_id:
            continue
        matches.append(_TockMatch(sha=sha, parsed=parsed))
    if len(matches) > 1:
        return "RECONCILIATION_TOCK_DUPLICATE"
    if len(matches) == 1:
        winner = matches[0]
        if winner.sha != head:
            return "RECONCILIATION_MAIN_STATE_DRIFT"
        result_tree = _read_commit(
            repository,
            str(winner.parsed["result_sha"]),
        ).tree_oid
        commit = _read_commit(repository, winner.sha)
        try:
            accepted = parse_canonical_tock(
                commit.message,
                commit.parents,
                commit.tree_oid,
                validated_tree_oid=result_tree,
            )
        except RunnerGitPromotionError as exc:
            return _TOCK_GEOMETRY_REASONS[exc.reason_code]
        return _TockMatch(sha=winner.sha, parsed=accepted)
    if head_geometry is not None:
        return head_geometry
    if parsed_head is not None:
        return _TockMatch(sha=head, parsed=parsed_head)
    return None


def _without_canonical_tock(
    *,
    repository: Path,
    runtime: Path,
    journal: PromotionJournal,
    main_ref: str,
    diagnostic_refs: Mapping[str, str] | None,
) -> OrdinaryReconciliationVerdict:
    result_tree = _read_commit(repository, journal.result_sha).tree_oid
    observation = observe_canonical_first_parent_tock(
        repo=repository,
        main_ref=main_ref,
        promotion_attempt_id=journal.promotion_attempt_id,
        validated_tree_oid=result_tree,
        diagnostic_refs=diagnostic_refs,
    )
    if observation.reason_code != "RECONCILIATION_TOCK_MISSING":
        return _blocked_ordinary(str(observation.reason_code))
    state = load_and_validate_main_state(
        main_state_path=runtime / _MAIN_STATE_NAME,
    )
    if state["observed_main_head_sha"] != journal.base_main_sha:
        return _blocked_ordinary("RECONCILIATION_MAIN_STATE_DRIFT")
    return _completed_ordinary(journal, None)


def _finish_canonical_tock(
    *,
    repository: Path,
    runtime: Path,
    journal: PromotionJournal,
    pending: dict[str, Any] | None,
    drain_run_id: str,
    main_ref: str,
    tock_sha: str,
) -> OrdinaryReconciliationVerdict:
    state = load_and_validate_main_state(
        main_state_path=runtime / _MAIN_STATE_NAME,
    )
    observed = state["observed_main_head_sha"]
    actual_post = (
        state["state"],
        state["main_branch"],
        state["validated_result_sha"],
        state["last_tock_sha"],
        observed,
    )
    expected_post = (
        _POST_STATE,
        _POST_MAIN_BRANCH,
        journal.result_sha,
        tock_sha,
        tock_sha,
    )
    if actual_post == expected_post:
        already_post = True
    elif observed == journal.base_main_sha:
        already_post = False
    else:
        return _blocked_ordinary("RECONCILIATION_MAIN_STATE_DRIFT")
    consumed_entries = load_promotion_consumed(
        promotion_consumed_path=runtime / _CONSUMPTION_NAME,
    )["consumed_promotions"]
    if pending is None and journal.phase is PromotionJournalPhase.FINALIZED:
        bound_pre = _pending_free_finalized_pre_state(
            consumed_entries,
            journal,
            tock_sha,
        )
        if bound_pre is None:
            return _blocked_ordinary("RECONCILIATION_CONSUMPTION_CONFLICT")
    elif pending is None:
        bound_pre = state
    else:
        bound_pre = pending["main_state_before_promotion"]
    relation = _consumption_relation(
        consumed_entries,
        journal,
        tock_sha,
        bound_pre,
    )
    if relation == "conflict":
        return _blocked_ordinary("RECONCILIATION_CONSUMPTION_CONFLICT")
    stamp = _utc_stamp()
    journal_path = runtime / _JOURNAL_NAME
    current = _advance_journal(
        journal_path,
        journal,
        PromotionJournalPhase.GIT_APPLIED,
        drain_run_id=drain_run_id,
        stamp=stamp,
        tock_sha=tock_sha,
    )
    if current is None:
        return _blocked_ordinary("RECONCILIATION_WRITE_FAILED")
    if not already_post:
        try:
            write_main_state(
                main_state_path=runtime / _MAIN_STATE_NAME,
                payload={
                    **state,
                    "state": _POST_STATE,
                    "main_branch": _POST_MAIN_BRANCH,
                    "validated_result_sha": journal.result_sha,
                    "last_tock_sha": tock_sha,
                    "observed_main_head_sha": tock_sha,
                    "reason_code": "RECONCILIATION_COMPLETED",
                    "updated_at_utc": stamp,
                },
            )
        except RunnerPromotionStateError:
            return _blocked_ordinary("RECONCILIATION_WRITE_FAILED")
    if relation != "identical":
        try:
            record_promotion_consumed(
                promotion_consumed_path=runtime / _CONSUMPTION_NAME,
                promotion_attempt_id=journal.promotion_attempt_id,
                result_sha=journal.result_sha,
                main_state_before_promotion=bound_pre,
                consumed_at_utc=stamp,
                tock_sha=tock_sha,
            )
        except RunnerPromotionStateError:
            return _blocked_ordinary("RECONCILIATION_WRITE_FAILED")
    finalized = _advance_journal(
        journal_path,
        current,
        PromotionJournalPhase.FINALIZED,
        drain_run_id=drain_run_id,
        stamp=stamp,
        tock_sha=tock_sha,
    )
    if finalized is None:
        return _blocked_ordinary("RECONCILIATION_WRITE_FAILED")
    continuation_block = _consume_ordinary_continuation(
        repository=repository,
        runtime=runtime,
        main_ref=main_ref,
        journal=finalized,
        successor_tock_sha=tock_sha,
        consumed_at_utc=stamp,
    )
    if continuation_block is not None:
        return continuation_block
    cleanup_block = _cleanup_reproved_pending(
        runtime=runtime,
        entry_pending=pending,
        finalized_journal=finalized,
        tock_sha=tock_sha,
    )
    if cleanup_block is not None:
        return _blocked_ordinary(cleanup_block)
    return _completed_ordinary(finalized, tock_sha)


def _cleanup_reproved_pending(
    *,
    runtime: Path,
    entry_pending: dict[str, Any] | None,
    finalized_journal: PromotionJournal,
    tock_sha: str,
) -> str | None:
    if entry_pending is None:
        return None
    persisted_journal = load_promotion_journal(path=runtime / _JOURNAL_NAME)
    state = load_and_validate_main_state(
        main_state_path=runtime / _MAIN_STATE_NAME,
    )
    consumed_entries = load_promotion_consumed(
        promotion_consumed_path=runtime / _CONSUMPTION_NAME,
    )["consumed_promotions"]
    reproved_pending, pending_reason = _read_pending(runtime)
    observed = (
        pending_reason,
        reproved_pending,
        persisted_journal,
        (
            state["state"],
            state["main_branch"],
            state["validated_result_sha"],
            state["last_tock_sha"],
            state["observed_main_head_sha"],
        ),
        _consumption_relation(
            consumed_entries,
            finalized_journal,
            tock_sha,
            entry_pending["main_state_before_promotion"],
        ),
    )
    expected = (
        None,
        entry_pending,
        finalized_journal,
        (
            _POST_STATE,
            _POST_MAIN_BRANCH,
            finalized_journal.result_sha,
            tock_sha,
            tock_sha,
        ),
        "identical",
    )
    if observed != expected:
        return {
            "RECONCILIATION_PENDING_INVALID": "RECONCILIATION_PENDING_INVALID",
        }.get(pending_reason, "RECONCILIATION_IDENTITY_MISMATCH")
    try:
        (runtime / "pending_promotion.yaml").unlink()
    except OSError:
        return "RECONCILIATION_WRITE_FAILED"
    return None


def _consume_ordinary_continuation(
    *,
    repository: Path,
    runtime: Path,
    main_ref: str,
    journal: PromotionJournal,
    successor_tock_sha: str,
    consumed_at_utc: str,
) -> OrdinaryReconciliationVerdict | None:
    artifact_path = runtime / _CONTINUATION_NAME
    if not artifact_path.is_file():
        return None
    try:
        loaded = load_continuation_artifact(artifact_path)
        if _finalized_predecessor_continuation_is_deferred(
            loaded,
            journal,
            successor_tock_sha,
        ):
            return None
        apply_successor_continuation_consumption(
            repo=repository,
            runtime=runtime,
            main_ref=main_ref,
            successor_task_id=journal.task_id,
            successor_promotion_attempt_id=journal.promotion_attempt_id,
            successor_tock_sha=successor_tock_sha,
            consumed_at_utc=consumed_at_utc,
        )
    except ContinuationError as exc:
        if exc.reason_code != _CONTINUATION_ALREADY_CONSUMED:
            return _blocked_ordinary(exc.reason_code)
    return None


def _finalized_predecessor_continuation_is_deferred(
    view: ContinuationContractView,
    journal: PromotionJournal,
    observed_tock_sha: str,
) -> bool:
    if view.status != _CONTINUATION_AUTHORIZED:
        return False
    if view.successor_task_id == journal.task_id:
        return False
    evidence = view.to_mapping()["origin"]["evidence"]
    predecessor = evidence["predecessor_identity"]
    loaded = (
        predecessor["task_id"],
        predecessor["origin_run_id"],
        predecessor["promotion_attempt_id"],
        predecessor["result_sha"],
        predecessor["promotion_class"],
        evidence["tock_sha"],
        journal.phase,
        journal.tock_sha,
    )
    bound = (
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        observed_tock_sha,
        PromotionJournalPhase.FINALIZED,
        observed_tock_sha,
    )
    return loaded == bound


def _pending_free_finalized_pre_state(
    entries: list[dict[str, Any]],
    journal: PromotionJournal,
    tock_sha: str,
) -> dict[str, Any] | None:
    identity = (
        journal.promotion_attempt_id,
        journal.result_sha,
        tock_sha,
    )
    matches = [
        entry
        for entry in entries
        if (
            entry["promotion_attempt_id"],
            entry["result_sha"],
            entry["tock_sha"],
        )
        == identity
    ]
    if len(matches) != 1:
        return None
    pre_state = matches[0]["main_state_before_promotion"]
    if pre_state["observed_main_head_sha"] != journal.base_main_sha:
        return None
    return pre_state


def _consumption_relation(
    entries: list[dict[str, Any]],
    journal: PromotionJournal,
    tock_sha: str,
    bound_pre: Any,
) -> str:
    identical = False
    current = (
        journal.promotion_attempt_id,
        journal.result_sha,
        tock_sha,
    )
    for entry in entries:
        identity = (
            entry["promotion_attempt_id"],
            entry["result_sha"],
            entry["tock_sha"],
        )
        if identity != current:
            shared = (
                identity[0] == current[0],
                identity[1] == current[1],
                identity[2] == current[2],
            )
            if shared != (False, False, False):
                return "conflict"
            continue
        if entry["main_state_before_promotion"] != bound_pre:
            return "conflict"
        identical = True
    if identical:
        return "identical"
    return "absent"


def _advance_journal(
    path: Path,
    journal: PromotionJournal,
    target: PromotionJournalPhase,
    *,
    drain_run_id: str,
    stamp: str,
    tock_sha: str,
) -> PromotionJournal | None:
    current = journal
    while _PHASE_ORDER.index(current.phase) < _PHASE_ORDER.index(target):
        nxt = _PHASE_ORDER[_PHASE_ORDER.index(current.phase) + 1]
        current = transition_promotion_journal(
            current,
            phase=nxt,
            drain_run_id=drain_run_id,
            transitioned_at_utc=stamp,
            reason_code="RECONCILIATION_COMPLETED",
            tock_sha=tock_sha if nxt is PromotionJournalPhase.GIT_APPLIED else None,
        )
        try:
            persist_promotion_journal(path=path, journal=current)
        except PromotionJournalError:
            return None
    return current
