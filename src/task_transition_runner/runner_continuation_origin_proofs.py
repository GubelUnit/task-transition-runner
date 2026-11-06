"""Ordinary FINALIZED_PROMOTION_TICK origin proof.

One predecessor is proven only when a FINALIZED journal, one matching
consumption entry, settled main state, and the configured main ref's
HEAD agree. That HEAD must be the only canonical TOCK on
``git rev-list --first-parent``. The proof reads Git and runtime files
and does not write them.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from task_transition_runner.runner_continuation_contract import (
    ContinuationError,
    ContinuationOrigin,
)
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    parse_canonical_tock,
)
from task_transition_runner.runner_promotion_journal import (
    PromotionJournal,
    PromotionJournalError,
    PromotionJournalPhase,
    load_promotion_journal,
)
from task_transition_runner.runner_promotion_state import (
    RunnerPromotionStateError,
    load_and_validate_main_state,
    load_promotion_consumed,
)


_ORIGIN_INVALID = "CONTINUATION_ORIGIN_INVALID"
_IDENTITY_MISMATCH = "CONTINUATION_IDENTITY_MISMATCH"
_JOURNAL_NAME = "promotion_journal.yaml"
_MAIN_STATE_NAME = "main_state.yaml"
_CONSUMPTION_NAME = "promotion_consumed.yaml"
_SETTLED_STATE = "GREEN_VALIDATED"
_MAIN_BRANCH = "main"


@dataclass(frozen=True)
class OriginProof:
    """Immutable ordinary predecessor proof."""

    origin: ContinuationOrigin
    evidence: Mapping[str, Any]
    parent_sha: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", _deep_freeze(self.evidence))

    def to_mapping(self) -> dict[str, Any]:
        return _deep_thaw(self.evidence)


@dataclass(frozen=True)
class _CommitView:
    tree_oid: str
    parents: tuple[str, ...]
    message: str


@dataclass(frozen=True)
class _Envelope:
    sha: str
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str
    promotion_class: str
    expected_base_sha: str


def prove_finalized_promotion_tick(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
) -> OriginProof:
    """Prove one ordinary finalized predecessor from runtime and main."""

    journal = _finalized_journal(runtime)
    consumed = _consumption_entries(runtime)
    main_state = _settled_main_state(runtime)
    if main_state["validated_result_sha"] != journal.result_sha:
        raise ContinuationError(_IDENTITY_MISMATCH)
    chain = _first_parent_shas(repo, main_ref)
    canonical = _canonical_envelopes(repo, chain)
    head = _head_attempt_envelope(canonical, chain)
    _require_journal_matches_head(journal, head)
    _require_consumption_matches_head(consumed, journal, head)
    _require_main_binds_head(main_state, head)
    return OriginProof(
        origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        evidence={
            "predecessor_identity": {
                "task_id": journal.task_id,
                "origin_run_id": journal.origin_run_id,
                "promotion_attempt_id": journal.promotion_attempt_id,
                "result_sha": journal.result_sha,
                "promotion_class": journal.promotion_class,
            },
            "tock_sha": head.sha,
        },
        parent_sha=head.sha,
    )


def _finalized_journal(runtime: Path) -> PromotionJournal:
    try:
        journal = load_promotion_journal(path=runtime / _JOURNAL_NAME)
    except PromotionJournalError:
        raise ContinuationError(_ORIGIN_INVALID) from None
    if journal.phase is not PromotionJournalPhase.FINALIZED:
        raise ContinuationError(_ORIGIN_INVALID)
    return journal


def _consumption_entries(runtime: Path) -> list[dict[str, Any]]:
    try:
        payload = load_promotion_consumed(
            promotion_consumed_path=runtime / _CONSUMPTION_NAME,
        )
    except RunnerPromotionStateError:
        raise ContinuationError(_ORIGIN_INVALID) from None
    return list(payload["consumed_promotions"])


def _settled_main_state(runtime: Path) -> dict[str, Any]:
    try:
        main_state = load_and_validate_main_state(
            main_state_path=runtime / _MAIN_STATE_NAME,
        )
    except RunnerPromotionStateError:
        raise ContinuationError(_ORIGIN_INVALID) from None
    if main_state["state"] != _SETTLED_STATE:
        raise ContinuationError(_ORIGIN_INVALID)
    if main_state["main_branch"] != _MAIN_BRANCH:
        raise ContinuationError(_ORIGIN_INVALID)
    return main_state


def _first_parent_shas(repository: Path, main_ref: str) -> tuple[str, ...]:
    output = _git_text(repository, "rev-list", "--first-parent", main_ref)
    return tuple(output.splitlines())


def _head_attempt_envelope(
    canonical: list[_Envelope],
    chain: tuple[str, ...],
) -> _Envelope:
    head: _Envelope | None = None
    for envelope in canonical:
        if envelope.sha == chain[0]:
            head = envelope
            break
    if head is None:
        raise ContinuationError(_ORIGIN_INVALID)
    same_attempt = [
        envelope
        for envelope in canonical
        if envelope.promotion_attempt_id == head.promotion_attempt_id
    ]
    if len(same_attempt) != 1:
        raise ContinuationError(_ORIGIN_INVALID)
    return head


def _canonical_envelopes(
    repository: Path,
    chain: tuple[str, ...],
) -> list[_Envelope]:
    canonical: list[_Envelope] = []
    for sha in chain:
        envelope = _envelope(repository, sha)
        if envelope is None:
            continue
        canonical.append(envelope)
    return canonical


def _envelope(repository: Path, sha: str) -> _Envelope | None:
    commit = _read_commit(repository, sha)
    try:
        parsed = parse_canonical_tock(
            commit.message,
            commit.parents,
            commit.tree_oid,
            validated_tree_oid=commit.tree_oid,
        )
    except RunnerGitPromotionError:
        return None
    result = _read_commit(repository, str(parsed["result_sha"]))
    if commit.tree_oid != result.tree_oid:
        return None
    return _Envelope(
        sha=sha,
        task_id=str(parsed["task_id"]),
        origin_run_id=str(parsed["origin_run_id"]),
        promotion_attempt_id=str(parsed["promotion_attempt_id"]),
        result_sha=str(parsed["result_sha"]),
        promotion_class=str(parsed["promotion_class"]),
        expected_base_sha=str(parsed["expected_base_sha"]),
    )


def _require_journal_matches_head(journal: PromotionJournal, head: _Envelope) -> None:
    if journal.task_id != head.task_id:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.origin_run_id != head.origin_run_id:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.promotion_attempt_id != head.promotion_attempt_id:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.promotion_class != head.promotion_class:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.base_main_sha != head.expected_base_sha:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.validated_result_sha != head.result_sha:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.result_sha != head.result_sha:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if journal.tock_sha != head.sha:
        raise ContinuationError(_IDENTITY_MISMATCH)


def _require_consumption_matches_head(
    entries: list[dict[str, Any]],
    journal: PromotionJournal,
    head: _Envelope,
) -> None:
    matched = [
        entry
        for entry in entries
        if entry["promotion_attempt_id"] == head.promotion_attempt_id
    ]
    if not matched:
        raise ContinuationError(_ORIGIN_INVALID)
    entry = matched[0]
    for extra in matched[1:]:
        if _consumption_identity(extra) != _consumption_identity(entry):
            raise ContinuationError(_ORIGIN_INVALID)
    if entry["result_sha"] != journal.result_sha:
        raise ContinuationError(_IDENTITY_MISMATCH)
    if entry["tock_sha"] != head.sha:
        raise ContinuationError(_IDENTITY_MISMATCH)


def _consumption_identity(entry: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        entry["promotion_attempt_id"],
        entry["result_sha"],
        entry["tock_sha"],
        entry["main_state_before_promotion"],
    )


def _require_main_binds_head(main_state: dict[str, Any], head: _Envelope) -> None:
    if main_state["last_tock_sha"] != head.sha:
        raise ContinuationError(_ORIGIN_INVALID)
    if main_state["observed_main_head_sha"] != head.sha:
        raise ContinuationError(_ORIGIN_INVALID)


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
    return _CommitView(tree_oid=tree_oid, parents=tuple(parents), message=message)


def _git_text(repository: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _deep_freeze(item) for key, item in value.items()}
        )
    return value


def _deep_thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _deep_thaw(item) for key, item in value.items()}
    return value
