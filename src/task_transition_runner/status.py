"""Public read-only status view.

``project_readonly_status`` projects already classified facts.
``classify_authority_first_status`` proves the existing source-authority
contract before any semantic runtime reading. Neither path writes Git,
runtime, or the lock, and neither treats persisted words as authority.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, cast

import yaml

from task_transition_runner.runner_code_authority import (
    RunnerCodeAuthorityError,
    SourceAuthorityReport,
    verify_steady_state_code_authority,
)


_STATUS_FACTS_INVALID = "STATUS_FACTS_INVALID"
_STATUS_FORBIDDEN_OUTCOME = "STATUS_FORBIDDEN_OUTCOME"
_STATUS_PRESENCE_PAYLOAD_CONFLICT = "STATUS_PRESENCE_PAYLOAD_CONFLICT"
_STATUS_RUNTIME_ROOT_INVALID = "STATUS_RUNTIME_ROOT_INVALID"
_STATUS_TARGET_INVALID = "STATUS_TARGET_INVALID"
_STATUS_VIEW_PROJECTED = "STATUS_VIEW_PROJECTED"
_FORBIDDEN_NEXT_ACTIONS = frozenset(
    {
        "RECOVERED",
        "REOPENED",
        "MIGRATED",
        "SOURCE_AUTHORITY_OK",
    }
)
_FACT_SLOTS = (
    "runner_source_identity",
    "target_main_head",
    "main_state",
    "active_task_identity",
    "run_phase",
    "pending",
    "journal",
    "consumption",
    "drain",
    "decisions",
    "next_external_action",
    "reason_code",
    "drift_warning",
)


class FactPresence(Enum):
    """Presence of one already classified status fact."""

    ABSENT = "ABSENT"
    CLASSIFIED = "CLASSIFIED"
    BLOCKED = "BLOCKED"


class StatusViewError(Exception):
    """Fail-closed violation of the read-only status view contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class ClassifiedFact:
    presence: FactPresence
    payload: dict[str, Any] | None
    reason_code: str | None


@dataclass(frozen=True)
class ClassifiedStatusFacts:
    runner_source_identity: ClassifiedFact
    target_main_head: ClassifiedFact
    main_state: ClassifiedFact
    active_task_identity: ClassifiedFact
    run_phase: ClassifiedFact
    pending: ClassifiedFact
    journal: ClassifiedFact
    consumption: ClassifiedFact
    drain: ClassifiedFact
    decisions: ClassifiedFact
    next_external_action: ClassifiedFact
    reason_code: ClassifiedFact
    drift_warning: ClassifiedFact


@dataclass(frozen=True)
class StatusView:
    projection_reason_code: str
    runner_source_identity: ClassifiedFact
    target_main_head: ClassifiedFact
    main_state: ClassifiedFact
    active_task_identity: ClassifiedFact
    run_phase: ClassifiedFact
    pending: ClassifiedFact
    journal: ClassifiedFact
    consumption: ClassifiedFact
    drain: ClassifiedFact
    decisions: ClassifiedFact
    next_external_action: ClassifiedFact
    reason_code: ClassifiedFact
    drift_warning: ClassifiedFact


def allowed_reason_codes() -> tuple[str, ...]:
    return tuple(
        sorted(
            (
                _STATUS_FACTS_INVALID,
                _STATUS_FORBIDDEN_OUTCOME,
                _STATUS_PRESENCE_PAYLOAD_CONFLICT,
                _STATUS_RUNTIME_ROOT_INVALID,
                _STATUS_TARGET_INVALID,
                _STATUS_VIEW_PROJECTED,
            )
        )
    )


def project_readonly_status(
    *,
    classified_facts: ClassifiedStatusFacts,
    target_worktree: Path,
    runtime_root: Path,
) -> StatusView:
    if not isinstance(classified_facts, ClassifiedStatusFacts):
        raise StatusViewError(_STATUS_FACTS_INVALID)
    _validate_classified_facts(classified_facts)
    _reject_forbidden_next_action(classified_facts)
    if not target_worktree.exists() or not target_worktree.is_dir():
        raise StatusViewError(_STATUS_TARGET_INVALID)
    if not runtime_root.exists():
        raise StatusViewError(_STATUS_RUNTIME_ROOT_INVALID)
    return StatusView(
        projection_reason_code=_STATUS_VIEW_PROJECTED,
        runner_source_identity=_copy_fact(classified_facts.runner_source_identity),
        target_main_head=_copy_fact(classified_facts.target_main_head),
        main_state=_copy_fact(classified_facts.main_state),
        active_task_identity=_copy_fact(classified_facts.active_task_identity),
        run_phase=_copy_fact(classified_facts.run_phase),
        pending=_copy_fact(classified_facts.pending),
        journal=_copy_fact(classified_facts.journal),
        consumption=_copy_fact(classified_facts.consumption),
        drain=_copy_fact(classified_facts.drain),
        decisions=_copy_fact(classified_facts.decisions),
        next_external_action=_copy_fact(classified_facts.next_external_action),
        reason_code=_copy_fact(classified_facts.reason_code),
        drift_warning=_copy_fact(classified_facts.drift_warning),
    )


def _validate_classified_facts(classified_facts: ClassifiedStatusFacts) -> None:
    for slot_name in _FACT_SLOTS:
        _validate_presence_payload(getattr(classified_facts, slot_name))


def _validate_presence_payload(fact: ClassifiedFact) -> None:
    if fact.presence is FactPresence.ABSENT:
        if fact.payload is not None:
            raise StatusViewError(_STATUS_PRESENCE_PAYLOAD_CONFLICT)
        return
    if fact.presence is FactPresence.CLASSIFIED:
        if fact.payload is None:
            raise StatusViewError(_STATUS_PRESENCE_PAYLOAD_CONFLICT)
        return
    if fact.reason_code is None:
        raise StatusViewError(_STATUS_PRESENCE_PAYLOAD_CONFLICT)


def _copy_fact(fact: ClassifiedFact) -> ClassifiedFact:
    return ClassifiedFact(
        presence=fact.presence,
        payload=deepcopy(fact.payload),
        reason_code=fact.reason_code,
    )


def _reject_forbidden_next_action(classified_facts: ClassifiedStatusFacts) -> None:
    next_action = classified_facts.next_external_action
    if next_action.presence is not FactPresence.CLASSIFIED:
        return
    payload = next_action.payload
    if not isinstance(payload, dict) or "value" not in payload:
        raise StatusViewError(_STATUS_PRESENCE_PAYLOAD_CONFLICT)
    if payload["value"] in _FORBIDDEN_NEXT_ACTIONS:
        raise StatusViewError(_STATUS_FORBIDDEN_OUTCOME)


_ABSENT_FACT = ClassifiedFact(
    presence=FactPresence.ABSENT,
    payload=None,
    reason_code=None,
)
_MAIN_HEAD_DRIFT = "MAIN_HEAD_DRIFT"
_AWAITING_DECISIONS = "AWAITING_DECISIONS"
_PENDING_PROMOTION = "PENDING_PROMOTION"
_DECISIONS_INCOMPLETE = "DECISIONS_INCOMPLETE"
_PENDING_INTENT_ONLY = "PENDING_INTENT_ONLY"
_BLOCKED_ACTION = "BLOCKED"
_CALLED_PROCESS_ERROR = subprocess.CalledProcessError
_PUBLIC_DECISION_STATES = frozenset({"missing", "draft", "ready"})
_DECISION_SLOTS = (
    ("request", "decision_request.yaml"),
    ("orchestrator", "orchestrator_decision.yaml"),
    ("validator", "validator_decision.yaml"),
)
_MAIN_STATE_NAME = "main_state.yaml"
_PENDING_NAME = "pending_promotion.yaml"
_JOURNAL_NAME = "promotion_journal.yaml"
_CONSUMPTION_NAME = "promotion_consumed.yaml"
_DRAIN_NAME = "drain_outcome.yaml"


def classify_authority_first_status(
    *,
    entry_point: Path,
    target_worktree: Path,
    runtime_root: Path,
    expected_source_sha: str | None = None,
    loaded_module_files: Mapping[str, Path | str] | None = None,
) -> StatusView:
    """Prove source authority, then project classified runtime facts."""

    try:
        report = verify_steady_state_code_authority(
            entry_point=entry_point,
            target_root=target_worktree,
            runtime_root=runtime_root,
            expected_source_sha=expected_source_sha,
            loaded_module_files=loaded_module_files,
        )
    except RunnerCodeAuthorityError as error:
        return _authority_mismatch_view(error.reason_code)
    return project_readonly_status(
        classified_facts=_classify_authorized_runtime(report),
        target_worktree=target_worktree,
        runtime_root=runtime_root,
    )


def _authority_mismatch_view(reason_code: str) -> StatusView:
    return StatusView(
        projection_reason_code=reason_code,
        runner_source_identity=_classified_fact(
            {
                "reason_code": reason_code,
                "interpreting_runtime": False,
            }
        ),
        target_main_head=_ABSENT_FACT,
        main_state=_ABSENT_FACT,
        active_task_identity=_ABSENT_FACT,
        run_phase=_ABSENT_FACT,
        pending=_ABSENT_FACT,
        journal=_ABSENT_FACT,
        consumption=_ABSENT_FACT,
        drain=_ABSENT_FACT,
        decisions=_ABSENT_FACT,
        next_external_action=_classified_fact({"value": _BLOCKED_ACTION}),
        reason_code=_classified_fact({"value": reason_code}),
        drift_warning=_ABSENT_FACT,
    )


def _classify_authorized_runtime(
    report: SourceAuthorityReport,
) -> ClassifiedStatusFacts:
    runtime_root = report.runtime_root
    main_state = _classify_document(runtime_root / _MAIN_STATE_NAME)
    pending = _classify_document(runtime_root / _PENDING_NAME)
    journal = _classify_document(runtime_root / _JOURNAL_NAME)
    consumption = _classify_document(runtime_root / _CONSUMPTION_NAME)
    drain = _classify_document(runtime_root / _DRAIN_NAME)
    decisions = _classify_decisions(runtime_root)
    head_sha = _read_target_head(report.target_root)
    action, reason = _next_public_action(decisions, pending)
    return ClassifiedStatusFacts(
        runner_source_identity=_classified_fact(
            {
                "source_sha": report.source_sha,
                "source_root": str(report.source_root),
                "entry_point": str(report.entry_point),
                "runtime_pin_sha": report.runtime_pin_sha,
            }
        ),
        target_main_head=_classified_fact({"sha": head_sha}),
        main_state=main_state,
        active_task_identity=_classify_active_task(pending),
        run_phase=_classified_fact({"value": action}),
        pending=pending,
        journal=journal,
        consumption=consumption,
        drain=drain,
        decisions=decisions,
        next_external_action=_classified_fact({"value": action}),
        reason_code=_classified_fact({"value": reason}),
        drift_warning=_classify_drift(main_state, head_sha),
    )


def _classify_document(path: Path) -> ClassifiedFact:
    if not path.is_file():
        return _ABSENT_FACT
    return _classified_fact(_require_mapping(path))


def _require_mapping(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded is None:
        raise StatusViewError(_STATUS_PRESENCE_PAYLOAD_CONFLICT)
    if not isinstance(loaded, dict):
        raise StatusViewError(_STATUS_FACTS_INVALID)
    return loaded


def _classify_decisions(runtime_root: Path) -> ClassifiedFact:
    slots = tuple((key, runtime_root / name) for key, name in _DECISION_SLOTS)
    if not any(path.is_file() for _key, path in slots):
        return _ABSENT_FACT
    return _classified_fact(
        {
            key: {"path": path.name, "state": _decision_state(path)}
            for key, path in slots
        }
    )


def _decision_state(path: Path) -> str:
    if not path.is_file():
        return "missing"
    state = _require_mapping(path).get("state")
    if state in _PUBLIC_DECISION_STATES:
        return cast(str, state)
    return "invalid"


def _classify_active_task(pending: ClassifiedFact) -> ClassifiedFact:
    if pending.presence is not FactPresence.CLASSIFIED:
        return _ABSENT_FACT
    task_id = _require_mapping_key(cast(dict[str, Any], pending.payload), "task_id")
    return _classified_fact({"task_id": task_id})


def _classify_drift(main_state: ClassifiedFact, head_sha: str) -> ClassifiedFact:
    if main_state.presence is not FactPresence.CLASSIFIED:
        return _ABSENT_FACT
    observed = _require_mapping_key(
        cast(dict[str, Any], main_state.payload),
        "observed_main_head_sha",
    )
    warning = _MAIN_HEAD_DRIFT if observed != head_sha else None
    return _classified_fact({"warning": warning})


def _require_mapping_key(payload: dict[str, Any], key: str) -> Any:
    if key not in payload:
        raise StatusViewError(_STATUS_FACTS_INVALID)
    return payload[key]


def _next_public_action(
    decisions: ClassifiedFact,
    pending: ClassifiedFact,
) -> tuple[str, str]:
    if _decisions_are_incomplete(decisions):
        return _AWAITING_DECISIONS, _DECISIONS_INCOMPLETE
    if pending.presence is FactPresence.CLASSIFIED:
        return _PENDING_PROMOTION, _PENDING_INTENT_ONLY
    return _BLOCKED_ACTION, _BLOCKED_ACTION


def _decisions_are_incomplete(decisions: ClassifiedFact) -> bool:
    if decisions.presence is not FactPresence.CLASSIFIED:
        return False
    payload = cast(dict[str, Any], decisions.payload)
    for key, _name in _DECISION_SLOTS:
        if payload[key]["state"] != "ready":
            return True
    return False


def _classified_fact(payload: dict[str, Any]) -> ClassifiedFact:
    return ClassifiedFact(
        presence=FactPresence.CLASSIFIED,
        payload=payload,
        reason_code=None,
    )


def _read_target_head(target_worktree: Path) -> str:
    try:
        completed = subprocess.run(  # noqa: S603
            ["git", "rev-parse", "HEAD"],
            cwd=target_worktree,
            check=True,
            capture_output=True,
            text=True,
        )
    except _CALLED_PROCESS_ERROR:
        raise StatusViewError(_STATUS_TARGET_INVALID) from None
    return completed.stdout.strip()
