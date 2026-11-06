"""Ordinary drain-before-dispatch barrier.

Existing pending promotion is handed only to the already proven step-16
entry ``admit_pending_promotion_effect``. A missing pending file stays
dispatch-ready and produces no Git, journal, consumption, or wake-up
effect. This module does not recover, reopen, or reconcile an existing
TOCK.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any, cast

import yaml

from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    admit_pending_promotion_effect,
    load_pending_promotion,
)
from task_transition_runner.runner_promotion_journal import (
    PromotionJournalError,
    PromotionJournalPhase,
    load_promotion_journal,
)
from task_transition_runner.runner_promotion_state import (
    RunnerPromotionStateError,
    load_and_validate_main_state,
)

_SCHEMA_VERSION = "1.0"
_CANONICAL_PENDING_NAME = "pending_promotion.yaml"
_ALTERNATE_PENDING_NAME = "pending_promotion.json"
_CANONICAL_OUTCOME_NAME = "drain_outcome.yaml"
_JOURNAL_NAME = "promotion_journal.yaml"
_IDENTITY_FIELDS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
)


class DrainOutcomeError(Exception):
    """Typed drain-outcome or identity-marker failure."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class DrainStatus(Enum):
    """Ordinary drain statuses. Recovery is not a status here."""

    NO_PENDING = "NO_PENDING"
    DRAINED = "DRAINED"
    BLOCKED = "BLOCKED"


class SchedulerPhase(Enum):
    """Phase visible to the next dispatch consumer."""

    DRAINED_AWAITING_TICK = "DRAINED_AWAITING_TICK"
    DISPATCH_READY = "DISPATCH_READY"
    BLOCKED = "BLOCKED"


class DrainOutcome:
    """Stable persisted drain decision identity."""

    def __init__(
        self,
        *,
        status: DrainStatus,
        drain_run_id: str,
        observed_main_head_sha: str,
        reason_code: str,
        task_id: str | None = None,
        origin_run_id: str | None = None,
        promotion_attempt_id: str | None = None,
        result_sha: str | None = None,
        promotion_class: str | None = None,
        tock_sha: str | None = None,
    ) -> None:
        self.status = status
        self.drain_run_id = _required_text(drain_run_id)
        self.observed_main_head_sha = _required_text(observed_main_head_sha)
        self.reason_code = _required_text(reason_code)
        self.task_id = task_id
        self.origin_run_id = origin_run_id
        self.promotion_attempt_id = promotion_attempt_id
        self.result_sha = result_sha
        self.promotion_class = promotion_class
        self.tock_sha = tock_sha
        _validate_identity_matrix(self)
        _validate_tock_matrix(self)

    def to_mapping(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "status": self.status.value,
            "drain_run_id": self.drain_run_id,
            "observed_main_head_sha": self.observed_main_head_sha,
            "reason_code": self.reason_code,
        }
        if _identity_complete(self):
            payload["task_id"] = self.task_id
            payload["origin_run_id"] = self.origin_run_id
            payload["promotion_attempt_id"] = self.promotion_attempt_id
            payload["result_sha"] = self.result_sha
            payload["promotion_class"] = self.promotion_class
        if self.tock_sha is not None:
            payload["tock_sha"] = self.tock_sha
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> DrainOutcome:
        if payload.get("schema_version") != _SCHEMA_VERSION:
            raise DrainOutcomeError("DRAIN_OUTCOME_SCHEMA_INVALID")
        status_text = payload.get("status")
        status = _status_from_text(status_text)
        identity = {
            field: payload.get(field)
            for field in _IDENTITY_FIELDS
            if field in payload
        }
        tock_sha = payload.get("tock_sha") if "tock_sha" in payload else None
        return cls(
            status=status,
            drain_run_id=_required_text(payload.get("drain_run_id")),
            observed_main_head_sha=_required_text(payload.get("observed_main_head_sha")),
            reason_code=_required_text(payload.get("reason_code")),
            task_id=identity.get("task_id"),
            origin_run_id=identity.get("origin_run_id"),
            promotion_attempt_id=identity.get("promotion_attempt_id"),
            result_sha=identity.get("result_sha"),
            promotion_class=identity.get("promotion_class"),
            tock_sha=tock_sha,
        )

    def __eq__(self, other: object) -> bool:
        return self.to_mapping() == other.to_mapping()


class DrainDecision:
    """Scheduler-facing drain result."""

    def __init__(self, *, phase: SchedulerPhase, outcome: DrainOutcome) -> None:
        self.phase = phase
        self.outcome = outcome


def persist_drain_outcome(
    *,
    runtime_root: Path | str,
    outcome: DrainOutcome,
) -> dict[str, Path]:
    runtime = Path(runtime_root)
    mapping = outcome.to_mapping()
    authoritative = runtime / _CANONICAL_OUTCOME_NAME
    _atomic_write_yaml(authoritative, mapping)
    paths = {"authoritative": authoritative}
    if _identity_complete(outcome):
        forensic = (
            runtime
            / "runs"
            / str(outcome.origin_run_id)
            / "drain_outcomes"
            / f"{outcome.promotion_attempt_id}.yaml"
        )
        _atomic_write_yaml(forensic, mapping)
        paths["forensic"] = forensic
    return paths


def load_persisted_drain_outcome(
    *,
    runtime_root: Path | str,
) -> DrainOutcome | None:
    path = Path(runtime_root) / _CANONICAL_OUTCOME_NAME
    if not path.is_file():
        return None
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise DrainOutcomeError("DRAIN_OUTCOME_INVALID") from exc
    if not isinstance(loaded, dict):
        raise DrainOutcomeError("DRAIN_OUTCOME_INVALID")
    return DrainOutcome.from_mapping(loaded)


def write_identity_marker(
    *,
    marker_path: Path | str,
    marker_type: str,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    result_sha: str,
    promotion_class: str,
    tock_sha: str | None = None,
) -> dict[str, Any]:
    if marker_type == "":
        raise DrainOutcomeError("IDENTITY_MARKER_FIELD_INVALID")
    payload: dict[str, Any] = {
        "marker_type": marker_type,
        "task_id": task_id,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
        "result_sha": result_sha,
        "promotion_class": promotion_class,
        "tock_sha": tock_sha,
    }
    path = Path(marker_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return payload


def load_identity_marker(
    *,
    marker_path: Path | str,
    marker_type: str,
    expected_identity: Mapping[str, Any],
) -> dict[str, Any] | None:
    path = Path(marker_path)
    if not path.is_file():
        return None
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded.get("marker_type") != marker_type:
        return None
    for key, expected in expected_identity.items():
        if loaded.get(key) != expected:
            return None
    return loaded


def evaluate_drain_before_dispatch(
    *,
    repo_root: Path | str,
    runtime_root: Path | str,
    drain_run_id: str,
    effect_admission: Mapping[str, Any] | None = None,
    main_head_reader: Any | None = None,
) -> DrainDecision:
    runtime = Path(runtime_root)
    repo = Path(repo_root)
    reader = _read_main_head if main_head_reader is None else main_head_reader
    live_head = reader(repo)
    prior = _load_prior_for_evaluate(runtime)

    if prior is _INVALID_PRIOR:
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="DRAIN_OUTCOME_INVALID",
        )
    pending_kind, identity, pending_document = _inspect_pending(runtime)
    if isinstance(prior, DrainOutcome) and prior.status is DrainStatus.DRAINED:
        if live_head == prior.tock_sha and live_head == prior.observed_main_head_sha:
            return DrainDecision(
                phase=SchedulerPhase.DRAINED_AWAITING_TICK,
                outcome=prior,
            )
        if not _bound_successor_pending_supersedes_epoch(
            runtime=runtime,
            prior=prior,
            live_head=live_head,
            pending_kind=pending_kind,
            pending_document=pending_document,
        ):
            return _blocked(
                runtime=runtime,
                drain_run_id=drain_run_id,
                observed_main_head_sha=live_head,
                reason_code="DRAIN_OUTCOME_MAIN_HEAD_DRIFT",
            )
    if isinstance(prior, DrainOutcome) and prior.status is DrainStatus.BLOCKED:
        if not _bound_successor_pending_supersedes_pre_start_blocked(
            runtime=runtime,
            prior=prior,
            live_head=live_head,
            pending_kind=pending_kind,
            pending_document=pending_document,
        ):
            return _blocked(
                runtime=runtime,
                drain_run_id=drain_run_id,
                observed_main_head_sha=live_head,
                reason_code="PRIOR_DRAIN_BLOCKED",
            )

    if pending_kind == "ambiguous":
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="PENDING_PROMOTION_AMBIGUOUS",
        )
    if pending_kind == "absent":
        if (runtime / _JOURNAL_NAME).is_file():
            return _blocked(
                runtime=runtime,
                drain_run_id=drain_run_id,
                observed_main_head_sha=live_head,
                reason_code="UNRESOLVED_PROMOTION_JOURNAL",
            )
        if isinstance(prior, DrainOutcome) and prior.status is DrainStatus.NO_PENDING:
            return DrainDecision(
                phase=SchedulerPhase.DISPATCH_READY,
                outcome=prior,
            )
        return _persist_decision(
            runtime=runtime,
            phase=SchedulerPhase.DISPATCH_READY,
            outcome=DrainOutcome(
                status=DrainStatus.NO_PENDING,
                drain_run_id=drain_run_id,
                observed_main_head_sha=live_head,
                reason_code="NO_PENDING_PROMOTION",
            ),
        )
    if pending_kind == "invalid":
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="PENDING_PROMOTION_INVALID",
        )
    if pending_kind == "identity_invalid":
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="PENDING_PROMOTION_IDENTITY_INVALID",
        )

    if (runtime / _JOURNAL_NAME).is_file() and not _matching_pre_effect_journal(
        runtime,
        cast(Mapping[str, Any], pending_document),
    ):
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="UNRESOLVED_PROMOTION_JOURNAL",
            identity=identity,
        )
    if not isinstance(effect_admission, Mapping) or not _admission_roots_complete(
        effect_admission
    ):
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="DRAIN_EFFECT_ADMISSION_MISSING",
            identity=identity,
        )
    if _admission_root_mismatch(
        effect_admission=effect_admission,
        runtime=runtime,
        repo=repo,
    ):
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="DRAIN_EFFECT_ADMISSION_ROOT_MISMATCH",
            identity=identity,
        )

    head_before = live_head
    try:
        admitted = admit_pending_promotion_effect(**effect_admission)
    except (RuntimeError, TypeError, AttributeError, ValueError):
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="DRAIN_EFFECT_ADMISSION_FAILED",
            identity=identity,
        )
    if not isinstance(admitted, Mapping):
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code="DRAIN_EFFECT_ADMISSION_INVALID",
            identity=identity,
        )
    if admitted.get("ok") is not True:
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_head,
            reason_code=admitted["reason_code"],
            identity=identity,
        )

    live_after = reader(repo)
    tock_sha = admitted.get("tock_sha")
    if isinstance(tock_sha, str) and tock_sha != live_after:
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_after,
            reason_code="DRAIN_TOCK_SHA_MISMATCH",
            identity=identity,
        )
    if not isinstance(tock_sha, str) or live_after == head_before:
        return _blocked(
            runtime=runtime,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_after,
            reason_code="DRAIN_TOCK_NOT_APPLIED",
            identity=identity,
        )

    decision = _persist_decision(
        runtime=runtime,
        phase=SchedulerPhase.DRAINED_AWAITING_TICK,
        outcome=DrainOutcome(
            status=DrainStatus.DRAINED,
            drain_run_id=drain_run_id,
            observed_main_head_sha=live_after,
            reason_code="PROMOTION_EFFECT_ADMITTED",
            tock_sha=tock_sha,
            **identity,
        ),
    )
    (runtime / _CANONICAL_PENDING_NAME).unlink()
    return decision


_INVALID_PRIOR = object()


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or value == "":
        raise DrainOutcomeError("DRAIN_OUTCOME_FIELD_EMPTY")
    return value


def _status_from_text(status_text: Any) -> DrainStatus:
    for status in DrainStatus:
        if status.value == status_text:
            return status
    raise DrainOutcomeError("DRAIN_OUTCOME_STATUS_INVALID")


def _identity_values(outcome: DrainOutcome) -> tuple[Any, ...]:
    return (
        outcome.task_id,
        outcome.origin_run_id,
        outcome.promotion_attempt_id,
        outcome.result_sha,
        outcome.promotion_class,
    )


def _identity_complete(outcome: DrainOutcome) -> bool:
    values = _identity_values(outcome)
    return all(isinstance(value, str) and value != "" for value in values)


def _validate_identity_matrix(outcome: DrainOutcome) -> None:
    values = _identity_values(outcome)
    present = [value is not None for value in values]
    if any(present) and not all(present):
        raise DrainOutcomeError("DRAIN_OUTCOME_INTENT_INCOMPLETE")


def _validate_tock_matrix(outcome: DrainOutcome) -> None:
    if outcome.status is DrainStatus.DRAINED:
        if not isinstance(outcome.tock_sha, str) or outcome.tock_sha == "":
            raise DrainOutcomeError("DRAIN_OUTCOME_TOCK_REQUIRED")
        return
    if outcome.tock_sha is not None:
        raise DrainOutcomeError("DRAIN_OUTCOME_TOCK_FORBIDDEN")


def _atomic_write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f"{path.name}.tmp")
    try:
        temporary_path.write_text(
            yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        raise DrainOutcomeError("DRAIN_OUTCOME_PERSIST_FAILED") from exc


def _read_main_head(repo_root: Path) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _load_prior_for_evaluate(runtime: Path) -> DrainOutcome | object | None:
    try:
        return load_persisted_drain_outcome(runtime_root=runtime)
    except DrainOutcomeError:
        return _INVALID_PRIOR


def _inspect_pending(
    runtime: Path,
) -> tuple[str, dict[str, str] | None, dict[str, Any] | None]:
    if (runtime / _ALTERNATE_PENDING_NAME).is_file():
        return ("ambiguous", None, None)
    pending_path = runtime / _CANONICAL_PENDING_NAME
    if not pending_path.is_file():
        return ("absent", None, None)
    try:
        loaded = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return ("invalid", None, None)
    if not isinstance(loaded, dict):
        return ("invalid", None, None)
    identity: dict[str, str] = {}
    for field in _IDENTITY_FIELDS:
        value = loaded.get(field, "")
        if value == "":
            return ("identity_invalid", None, None)
        identity[field] = value
    return ("present", identity, loaded)


def _bound_successor_pending_supersedes_epoch(
    *,
    runtime: Path,
    prior: DrainOutcome,
    live_head: str,
    pending_kind: str,
    pending_document: Mapping[str, Any] | None,
) -> bool:
    if pending_kind != "present" or pending_document is None:
        return False
    try:
        pending = load_pending_promotion(runtime_root=runtime)
    except RunnerGitPromotionError:
        return False
    prestate = pending.get("main_state_before_promotion")
    if not isinstance(prestate, Mapping):
        return False
    prior_identity = _identity_values(prior)
    pending_identity = tuple(pending.get(field) for field in _IDENTITY_FIELDS)
    observed = (
        prior.reason_code,
        _identity_complete(prior),
        prior.observed_main_head_sha,
        live_head != prior.tock_sha,
        pending == pending_document,
        pending.get("task_id") != prior.task_id,
        pending_identity != prior_identity,
        pending.get("base_sha"),
        prestate.get("observed_main_head_sha"),
        prestate.get("last_tock_sha"),
        prestate.get("validated_result_sha"),
        prestate.get("reason_code"),
    )
    required = (
        "PROMOTION_EFFECT_ADMITTED",
        True,
        prior.tock_sha,
        True,
        True,
        True,
        True,
        live_head,
        live_head,
        prior.tock_sha,
        prior.result_sha,
        "PROMOTION_EFFECT_ADMITTED",
    )
    return observed == required


def _bound_successor_pending_supersedes_pre_start_blocked(
    *,
    runtime: Path,
    prior: DrainOutcome,
    live_head: str,
    pending_kind: str,
    pending_document: Mapping[str, Any] | None,
) -> bool:
    if pending_kind != "present" or pending_document is None:
        return False
    try:
        pending = load_pending_promotion(runtime_root=runtime)
        persisted_prestate = load_and_validate_main_state(
            main_state_path=runtime / "main_state.yaml",
        )
    except (RunnerGitPromotionError, RunnerPromotionStateError):
        return False
    prestate = pending.get("main_state_before_promotion")
    if not isinstance(prestate, Mapping):
        return False
    if not _pending_successor_identity_is_canonical(pending):
        return False
    observed = (
        prior.drain_run_id,
        prior.reason_code,
        _identity_values(prior),
        prior.observed_main_head_sha,
        live_head != prior.observed_main_head_sha,
        pending == pending_document,
        dict(prestate) == persisted_prestate,
        pending.get("base_sha"),
        prestate.get("state"),
        prestate.get("main_branch"),
        prestate.get("observed_main_head_sha"),
        prestate.get("last_tock_sha"),
        prestate.get("reason_code")
        in ("PROMOTION_EFFECT_ADMITTED", "RECONCILIATION_COMPLETED"),
    )
    required = (
        "cli-run",
        "UNRESOLVED_PROMOTION_JOURNAL",
        (None, None, None, None, None),
        prestate.get("last_tock_sha"),
        True,
        True,
        True,
        live_head,
        "GREEN_VALIDATED",
        "main",
        live_head,
        prior.observed_main_head_sha,
        True,
    )
    return observed == required


def _pending_successor_identity_is_canonical(
    pending: Mapping[str, Any],
) -> bool:
    identity = tuple(pending.get(field) for field in _IDENTITY_FIELDS)
    if not all(isinstance(value, str) and value != "" for value in identity):
        return False
    task_snapshot = pending.get("task_snapshot")
    instruction_snapshot = pending.get("instruction_snapshot")
    orchestrator = pending.get("orchestrator_decision")
    validator = pending.get("validator_decision")
    if not all(
        isinstance(value, Mapping)
        for value in (
            task_snapshot,
            instruction_snapshot,
            orchestrator,
            validator,
        )
    ):
        return False
    task_current = task_snapshot.get("current")
    task_bound = task_snapshot.get("bound")
    instruction_current = instruction_snapshot.get("current")
    instruction_bound = instruction_snapshot.get("bound")
    if not all(
        isinstance(value, Mapping)
        for value in (
            task_current,
            task_bound,
            instruction_current,
            instruction_bound,
        )
    ):
        return False
    decision_identity = identity[:4]
    return (
        task_current == task_bound
        and instruction_current == instruction_bound
        and task_current.get("task_id") == identity[0]
        and tuple(orchestrator.get(field) for field in _IDENTITY_FIELDS[:4])
        == decision_identity
        and tuple(validator.get(field) for field in _IDENTITY_FIELDS[:4])
        == decision_identity
        and orchestrator.get("promotion_class") == identity[4]
    )


def _matching_pre_effect_journal(runtime: Path, pending: Mapping[str, Any]) -> bool:
    try:
        journal = load_promotion_journal(path=runtime / _JOURNAL_NAME)
    except PromotionJournalError:
        return False
    if journal.phase not in (
        PromotionJournalPhase.PENDING,
        PromotionJournalPhase.APPLYING,
    ):
        return False
    return (
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.base_main_sha,
        journal.validated_result_sha,
    ) == (
        pending["task_id"],
        pending["origin_run_id"],
        pending["promotion_attempt_id"],
        pending["result_sha"],
        pending["promotion_class"],
        pending["base_sha"],
        pending["result_sha"],
    )


def _same_root(left: Any, right: Path) -> bool:
    return Path(left).resolve() == right.resolve()


def _admission_roots_complete(effect_admission: Mapping[str, Any]) -> bool:
    if effect_admission.get("runtime_root") is None:
        return False
    if effect_admission.get("target_worktree") is None:
        return False
    return True


def _admission_root_mismatch(
    *,
    effect_admission: Mapping[str, Any],
    runtime: Path,
    repo: Path,
) -> bool:
    if not _same_root(effect_admission["runtime_root"], runtime):
        return True
    if not _same_root(effect_admission["target_worktree"], repo):
        return True
    return False


def _blocked(
    *,
    runtime: Path,
    drain_run_id: str,
    observed_main_head_sha: str,
    reason_code: str,
    identity: Mapping[str, str] | None = None,
) -> DrainDecision:
    outcome_fields: dict[str, Any] = {}
    if identity is not None:
        outcome_fields.update(identity)
    return _persist_decision(
        runtime=runtime,
        phase=SchedulerPhase.BLOCKED,
        outcome=DrainOutcome(
            status=DrainStatus.BLOCKED,
            drain_run_id=drain_run_id,
            observed_main_head_sha=observed_main_head_sha,
            reason_code=reason_code,
            **outcome_fields,
        ),
    )


def _persist_decision(
    *,
    runtime: Path,
    phase: SchedulerPhase,
    outcome: DrainOutcome,
) -> DrainDecision:
    persist_drain_outcome(runtime_root=runtime, outcome=outcome)
    return DrainDecision(phase=phase, outcome=outcome)
