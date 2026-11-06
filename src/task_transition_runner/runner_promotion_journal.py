from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml


SCHEMA_VERSION = "1.0"
PERSISTENCE_CONTRACT = "SINGLE_FILE_ATOMIC_REPLACE"
ARCHIVE_CONTRACT = "FINALIZED_DIGEST_BOUND_IDEMPOTENT_CAS"
ALLOWED_PROMOTION_CLASSES = ("GREEN_PROMOTION", "RED_TDD_PROMOTION")
_ARCHIVE_FIELDS = frozenset(
    {
        "schema_version",
        "archive_type",
        "journal_digest_sha256",
        "journal",
    }
)


class PromotionJournalError(RuntimeError):
    """Fail-closed violation of the isolated promotion-journal contract."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


class PromotionJournalPhase(str, Enum):
    PENDING = "PENDING"
    APPLYING = "APPLYING"
    GIT_APPLIED = "GIT_APPLIED"
    RECONCILED = "RECONCILED"
    FINALIZED = "FINALIZED"


_ALLOWED_TRANSITIONS = MappingProxyType(
    {
        PromotionJournalPhase.PENDING: (PromotionJournalPhase.APPLYING,),
        PromotionJournalPhase.APPLYING: (PromotionJournalPhase.GIT_APPLIED,),
        PromotionJournalPhase.GIT_APPLIED: (PromotionJournalPhase.RECONCILED,),
        PromotionJournalPhase.RECONCILED: (PromotionJournalPhase.FINALIZED,),
        PromotionJournalPhase.FINALIZED: (),
    }
)
_PHASE_ORDER = tuple(PromotionJournalPhase)
_IDENTITY_FIELDS = (
    "task_id",
    "origin_run_id",
    "drain_run_id",
    "promotion_attempt_id",
)
_SNAPSHOT_AUTHORITY_FIELDS = (
    "task_fingerprint",
    "task_snapshot_sha256",
    "instruction_signature_sha256",
)
_STABLE_ATTEMPT_FIELDS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "promotion_class",
    "base_main_sha",
    "result_sha",
    "validated_result_sha",
    *_SNAPSHOT_AUTHORITY_FIELDS,
)
_PROMOTION_FIELDS = (
    "promotion_class",
    "base_main_sha",
    "result_sha",
    "validated_result_sha",
)
_TIMESTAMP_ATTRIBUTES = (
    "pending_at_utc",
    "applying_at_utc",
    "git_applied_at_utc",
    "reconciled_at_utc",
    "finalized_at_utc",
)
_SERIALIZED_FIELDS = frozenset(
    {
        "schema_version",
        "phase",
        *_IDENTITY_FIELDS,
        *_PROMOTION_FIELDS,
        *_SNAPSHOT_AUTHORITY_FIELDS,
        "tock_sha",
        "phase_timestamps",
        "reason_code",
    }
)


@dataclass(frozen=True)
class PromotionJournal:
    phase: PromotionJournalPhase
    task_id: str
    origin_run_id: str
    drain_run_id: str
    promotion_attempt_id: str
    promotion_class: str
    base_main_sha: str
    result_sha: str
    validated_result_sha: str
    task_fingerprint: str
    task_snapshot_sha256: str
    instruction_signature_sha256: str
    reason_code: str
    pending_at_utc: str
    applying_at_utc: str | None = None
    git_applied_at_utc: str | None = None
    reconciled_at_utc: str | None = None
    finalized_at_utc: str | None = None
    tock_sha: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "phase": self.phase.value,
            "task_id": self.task_id,
            "origin_run_id": self.origin_run_id,
            "drain_run_id": self.drain_run_id,
            "promotion_attempt_id": self.promotion_attempt_id,
            "promotion_class": self.promotion_class,
            "base_main_sha": self.base_main_sha,
            "result_sha": self.result_sha,
            "validated_result_sha": self.validated_result_sha,
            "task_fingerprint": self.task_fingerprint,
            "task_snapshot_sha256": self.task_snapshot_sha256,
            "instruction_signature_sha256": self.instruction_signature_sha256,
            "phase_timestamps": {
                phase.value: timestamp
                for phase, timestamp in zip(
                    _PHASE_ORDER,
                    self.phase_timestamps,
                    strict=True,
                )
                if timestamp is not None
            },
            "reason_code": self.reason_code,
        }
        if self.tock_sha is not None:
            payload["tock_sha"] = self.tock_sha
        return payload

    @property
    def phase_timestamps(self) -> tuple[str | None, ...]:
        return tuple(getattr(self, attribute) for attribute in _TIMESTAMP_ATTRIBUTES)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> PromotionJournal:
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise PromotionJournalError(
                "JOURNAL_SCHEMA_UNSUPPORTED",
                "promotion journal schema_version is missing or unsupported.",
            )
        unknown_fields = set(payload) - _SERIALIZED_FIELDS
        if unknown_fields:
            raise PromotionJournalError(
                "JOURNAL_FIELDS_INVALID",
                f"promotion journal contains unknown fields: {sorted(unknown_fields)!r}.",
            )
        try:
            phase = PromotionJournalPhase(payload.get("phase"))
        except (TypeError, ValueError) as exc:
            raise PromotionJournalError(
                "JOURNAL_PHASE_UNKNOWN",
                f"promotion journal phase is unknown: {payload.get('phase')!r}.",
            ) from exc

        _require_text_fields(
            payload,
            fields=_IDENTITY_FIELDS,
            reason_code="JOURNAL_IDENTITY_INCOMPLETE",
        )
        _require_text_fields(
            payload,
            fields=_PROMOTION_FIELDS,
            reason_code="JOURNAL_PROMOTION_FIELDS_INCOMPLETE",
        )
        _validate_promotion_class(payload["promotion_class"])
        _required_text(
            payload.get("reason_code"),
            reason_code="JOURNAL_REASON_CODE_INVALID",
            field="reason_code",
        )
        timestamps = _parse_phase_timestamps(
            payload.get("phase_timestamps"),
            phase=phase,
        )
        _require_snapshot_authority_anchors(payload)
        journal = cls(
            phase=phase,
            task_id=str(payload["task_id"]),
            origin_run_id=str(payload["origin_run_id"]),
            drain_run_id=str(payload["drain_run_id"]),
            promotion_attempt_id=str(payload["promotion_attempt_id"]),
            promotion_class=str(payload["promotion_class"]),
            base_main_sha=str(payload["base_main_sha"]),
            result_sha=str(payload["result_sha"]),
            validated_result_sha=str(payload["validated_result_sha"]),
            task_fingerprint=payload["task_fingerprint"],
            task_snapshot_sha256=payload["task_snapshot_sha256"],
            instruction_signature_sha256=payload["instruction_signature_sha256"],
            tock_sha=_optional_text(payload.get("tock_sha"), field="tock_sha"),
            reason_code=str(payload["reason_code"]),
            **dict(zip(_TIMESTAMP_ATTRIBUTES, timestamps, strict=True)),
        )
        _validate_tock_contract(journal)
        return journal


def allowed_journal_transitions() -> dict[PromotionJournalPhase, tuple[PromotionJournalPhase, ...]]:
    return dict(_ALLOWED_TRANSITIONS)


def create_promotion_journal(
    *,
    task_id: str,
    origin_run_id: str,
    drain_run_id: str,
    promotion_attempt_id: str,
    promotion_class: str,
    base_main_sha: str,
    result_sha: str,
    validated_result_sha: str,
    task_fingerprint: str,
    task_snapshot_sha256: str,
    instruction_signature_sha256: str,
    pending_at_utc: str,
    reason_code: str,
) -> PromotionJournal:
    journal = PromotionJournal(
        phase=PromotionJournalPhase.PENDING,
        task_id=task_id,
        origin_run_id=origin_run_id,
        drain_run_id=drain_run_id,
        promotion_attempt_id=promotion_attempt_id,
        promotion_class=promotion_class,
        base_main_sha=base_main_sha,
        result_sha=result_sha,
        validated_result_sha=validated_result_sha,
        task_fingerprint=task_fingerprint,
        task_snapshot_sha256=task_snapshot_sha256,
        instruction_signature_sha256=instruction_signature_sha256,
        pending_at_utc=pending_at_utc,
        reason_code=reason_code,
    )
    _validate_journal(journal)
    return journal


def transition_promotion_journal(
    journal: PromotionJournal,
    *,
    phase: PromotionJournalPhase,
    drain_run_id: str,
    transitioned_at_utc: str,
    reason_code: str,
    tock_sha: str | None = None,
) -> PromotionJournal:
    _validate_journal(journal)
    if not isinstance(phase, PromotionJournalPhase):
        raise PromotionJournalError(
            "JOURNAL_PHASE_UNKNOWN",
            f"promotion journal phase is unknown: {phase!r}.",
        )
    if phase not in _ALLOWED_TRANSITIONS[journal.phase]:
        raise PromotionJournalError(
            "JOURNAL_TRANSITION_ILLEGAL",
            f"transition {journal.phase.value} -> {phase.value} is not allowed.",
        )
    timestamp_attribute = _TIMESTAMP_ATTRIBUTES[_PHASE_ORDER.index(phase)]
    effective_tock_sha = journal.tock_sha if tock_sha is None else tock_sha
    candidate = replace(
        journal,
        phase=phase,
        drain_run_id=drain_run_id,
        reason_code=reason_code,
        tock_sha=effective_tock_sha,
        **{timestamp_attribute: transitioned_at_utc},
    )
    return validate_promotion_journal_transition(journal, candidate)


def validate_promotion_journal_transition(
    previous: PromotionJournal,
    candidate: PromotionJournal,
) -> PromotionJournal:
    _validate_journal(previous)
    if not isinstance(candidate, PromotionJournal):
        raise PromotionJournalError(
            "JOURNAL_FIELDS_INVALID",
            "transition candidate must be a PromotionJournal.",
        )
    if not isinstance(candidate.phase, PromotionJournalPhase):
        raise PromotionJournalError(
            "JOURNAL_PHASE_UNKNOWN",
            f"promotion journal phase is unknown: {candidate.phase!r}.",
        )
    if candidate.phase not in _ALLOWED_TRANSITIONS[previous.phase]:
        raise PromotionJournalError(
            "JOURNAL_TRANSITION_ILLEGAL",
            f"transition {previous.phase.value} -> {candidate.phase.value} is not allowed.",
        )
    drifted_fields = [
        field
        for field in _STABLE_ATTEMPT_FIELDS
        if getattr(previous, field) != getattr(candidate, field)
    ]
    if drifted_fields:
        raise PromotionJournalError(
            "JOURNAL_FIELD_DRIFT",
            f"stable promotion fields changed: {', '.join(drifted_fields)}.",
        )
    previous_phase_index = _PHASE_ORDER.index(previous.phase)
    if (
        candidate.phase_timestamps[: previous_phase_index + 1]
        != previous.phase_timestamps[: previous_phase_index + 1]
    ):
        raise PromotionJournalError(
            "JOURNAL_TIMELINE_DRIFT",
            "timestamps of completed journal phases are immutable.",
        )
    if previous.tock_sha is not None and candidate.tock_sha != previous.tock_sha:
        raise PromotionJournalError(
            "JOURNAL_FIELD_DRIFT",
            "the physically observed tock_sha is immutable.",
        )
    _validate_journal(candidate)
    return candidate


def load_promotion_journal(*, path: Path | str) -> PromotionJournal:
    journal_path = Path(path)
    if not journal_path.is_file():
        raise PromotionJournalError(
            "JOURNAL_NOT_FOUND",
            f"promotion journal does not exist: {journal_path}.",
        )
    try:
        text = journal_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PromotionJournalError(
            "JOURNAL_READ_FAILED",
            f"promotion journal cannot be read: {journal_path}.",
        ) from exc
    try:
        payload = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PromotionJournalError(
            "JOURNAL_YAML_MALFORMED",
            f"promotion journal contains malformed YAML: {journal_path}.",
        ) from exc
    if not isinstance(payload, Mapping):
        raise PromotionJournalError(
            "JOURNAL_MAPPING_REQUIRED",
            "promotion journal YAML must contain one mapping.",
        )
    return PromotionJournal.from_mapping(payload)


def persist_promotion_journal(
    *,
    path: Path | str,
    journal: PromotionJournal,
) -> PromotionJournal:
    journal_path = Path(path)
    if journal_path.exists():
        previous = load_promotion_journal(path=journal_path)
        validate_promotion_journal_transition(previous, journal)
    else:
        _validate_journal(journal)
        if journal.phase is not PromotionJournalPhase.PENDING:
            raise PromotionJournalError(
                "JOURNAL_INITIAL_PHASE_INVALID",
                "a new promotion journal must start in PENDING.",
            )
    _atomic_write_yaml(journal_path, journal.to_mapping())
    return journal


def promotion_journal_digest(journal: PromotionJournal) -> str:
    _validate_journal(journal)
    encoded = json.dumps(
        journal.to_mapping(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def promotion_journal_archive_path(
    *,
    runtime_root: Path | str,
    journal: PromotionJournal,
) -> Path:
    _validate_journal(journal)
    return promotion_journal_archive_path_for_identity(
        runtime_root=runtime_root,
        origin_run_id=journal.origin_run_id,
        promotion_attempt_id=journal.promotion_attempt_id,
    )


def promotion_journal_archive_path_for_identity(
    *,
    runtime_root: Path | str,
    origin_run_id: str,
    promotion_attempt_id: str,
) -> Path:
    _required_text(
        origin_run_id,
        reason_code="JOURNAL_IDENTITY_INCOMPLETE",
        field="origin_run_id",
    )
    _required_text(
        promotion_attempt_id,
        reason_code="JOURNAL_IDENTITY_INCOMPLETE",
        field="promotion_attempt_id",
    )
    attempt_digest = hashlib.sha256(
        promotion_attempt_id.encode("utf-8")
    ).hexdigest()[:12]
    return (
        Path(runtime_root)
        / "runs"
        / _safe_component(origin_run_id)
        / "promotion_journals"
        / (
            f"{_safe_component(promotion_attempt_id)}"
            f".{attempt_digest}.yaml"
        )
    )


def load_archived_promotion_journal(*, path: Path | str) -> PromotionJournal:
    archive_path = Path(path)
    if not archive_path.is_file():
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_NOT_FOUND",
            f"promotion journal archive does not exist: {archive_path}.",
        )
    try:
        payload = yaml.safe_load(archive_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_INVALID",
            f"promotion journal archive cannot be loaded: {archive_path}.",
        ) from exc
    if (
        not isinstance(payload, Mapping)
        or set(payload) != _ARCHIVE_FIELDS
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("archive_type") != "promotion_journal_epoch"
        or not isinstance(payload.get("journal_digest_sha256"), str)
        or not isinstance(payload.get("journal"), Mapping)
    ):
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_INVALID",
            "promotion journal archive envelope is incomplete or open-world.",
        )
    try:
        journal = PromotionJournal.from_mapping(payload["journal"])
    except PromotionJournalError as exc:
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_INVALID",
            "promotion journal archive contains an invalid journal.",
        ) from exc
    if payload["journal_digest_sha256"] != promotion_journal_digest(journal):
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_DRIFT",
            "promotion journal archive digest differs from its content.",
        )
    return journal


def archive_promotion_journal_epoch(
    *,
    runtime_root: Path | str,
    journal: PromotionJournal,
) -> Path:
    _validate_journal(journal)
    if journal.phase is not PromotionJournalPhase.FINALIZED:
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_FINALIZED_REQUIRED",
            "only a FINALIZED promotion journal can be archived.",
        )
    runtime = Path(runtime_root)
    canonical_path = runtime / "promotion_journal.yaml"
    try:
        canonical = load_promotion_journal(path=canonical_path)
    except PromotionJournalError as exc:
        raise PromotionJournalError(
            "JOURNAL_CANONICAL_DRIFT",
            "canonical promotion journal is unavailable for archival CAS.",
        ) from exc
    if canonical != journal:
        raise PromotionJournalError(
            "JOURNAL_CANONICAL_DRIFT",
            "canonical promotion journal differs from the archival candidate.",
        )
    archive_path = promotion_journal_archive_path(
        runtime_root=runtime,
        journal=journal,
    )
    if archive_path.exists():
        archived = load_archived_promotion_journal(path=archive_path)
        if archived != journal:
            raise PromotionJournalError(
                "JOURNAL_ARCHIVE_DRIFT",
                "existing promotion journal archive has a foreign identity.",
            )
        return archive_path
    payload = {
        "schema_version": SCHEMA_VERSION,
        "archive_type": "promotion_journal_epoch",
        "journal_digest_sha256": promotion_journal_digest(journal),
        "journal": journal.to_mapping(),
    }
    _atomic_write_yaml(archive_path, payload)
    archived = load_archived_promotion_journal(path=archive_path)
    if archived != journal:
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_DRIFT",
            "reloaded promotion journal archive differs from its source.",
        )
    return archive_path


def release_canonical_promotion_journal(
    *,
    runtime_root: Path | str,
    journal: PromotionJournal,
) -> None:
    archive_path = promotion_journal_archive_path(
        runtime_root=runtime_root,
        journal=journal,
    )
    archived = load_archived_promotion_journal(path=archive_path)
    if archived != journal:
        raise PromotionJournalError(
            "JOURNAL_ARCHIVE_DRIFT",
            "promotion journal archive differs from the release candidate.",
        )
    canonical_path = Path(runtime_root) / "promotion_journal.yaml"
    if not canonical_path.exists():
        return
    try:
        canonical = load_promotion_journal(path=canonical_path)
    except PromotionJournalError as exc:
        raise PromotionJournalError(
            "JOURNAL_CANONICAL_DRIFT",
            "canonical promotion journal cannot be verified before release.",
        ) from exc
    if canonical != journal:
        raise PromotionJournalError(
            "JOURNAL_CANONICAL_DRIFT",
            "canonical promotion journal changed after archival.",
        )
    try:
        canonical_path.unlink()
    except OSError as exc:
        raise PromotionJournalError(
            "JOURNAL_RELEASE_FAILED",
            "canonical promotion journal could not be released.",
        ) from exc


def _validate_journal(journal: PromotionJournal) -> None:
    if not isinstance(journal, PromotionJournal):
        raise PromotionJournalError(
            "JOURNAL_FIELDS_INVALID",
            "journal value must be a PromotionJournal.",
        )
    if not isinstance(journal.phase, PromotionJournalPhase):
        raise PromotionJournalError(
            "JOURNAL_PHASE_UNKNOWN",
            f"promotion journal phase is unknown: {journal.phase!r}.",
        )
    _require_text_fields(
        journal.__dict__,
        fields=_IDENTITY_FIELDS,
        reason_code="JOURNAL_IDENTITY_INCOMPLETE",
    )
    _require_text_fields(
        journal.__dict__,
        fields=_PROMOTION_FIELDS,
        reason_code="JOURNAL_PROMOTION_FIELDS_INCOMPLETE",
    )
    _validate_promotion_class(journal.promotion_class)
    _require_snapshot_authority_anchors(journal.__dict__)
    _required_text(
        journal.reason_code,
        reason_code="JOURNAL_REASON_CODE_INVALID",
        field="reason_code",
    )
    _validate_phase_timestamps(journal.phase_timestamps, phase=journal.phase)
    _validate_tock_contract(journal)


def _parse_phase_timestamps(
    payload: Any,
    *,
    phase: PromotionJournalPhase,
) -> tuple[str | None, ...]:
    if not isinstance(payload, Mapping):
        raise PromotionJournalError(
            "JOURNAL_TIMELINE_INVALID",
            "phase_timestamps must be a mapping.",
        )
    known_names = {item.value for item in _PHASE_ORDER}
    if set(payload) - known_names:
        raise PromotionJournalError(
            "JOURNAL_TIMELINE_INVALID",
            "phase_timestamps contains an unknown phase.",
        )
    timestamps = tuple(payload.get(item.value) for item in _PHASE_ORDER)
    _validate_phase_timestamps(timestamps, phase=phase)
    return timestamps


def _validate_phase_timestamps(
    timestamps: tuple[Any, ...],
    *,
    phase: PromotionJournalPhase,
) -> None:
    phase_index = _PHASE_ORDER.index(phase)
    expected_presence = tuple(index <= phase_index for index in range(len(_PHASE_ORDER)))
    actual_presence = tuple(timestamp is not None for timestamp in timestamps)
    if actual_presence != expected_presence:
        raise PromotionJournalError(
            "JOURNAL_TIMELINE_INVALID",
            "phase_timestamps must be the complete contiguous prefix through the current phase.",
        )
    parsed = [
        _parse_utc_timestamp(timestamp, phase=_PHASE_ORDER[index])
        for index, timestamp in enumerate(timestamps)
        if timestamp is not None
    ]
    if any(later < earlier for earlier, later in zip(parsed, parsed[1:])):
        raise PromotionJournalError(
            "JOURNAL_TIMELINE_INVALID",
            "phase timestamps must be monotonic.",
        )


def _parse_utc_timestamp(value: Any, *, phase: PromotionJournalPhase) -> datetime:
    if not isinstance(value, str) or not value.strip() or not value.endswith("Z"):
        raise PromotionJournalError(
            "JOURNAL_TIMESTAMP_INVALID",
            f"{phase.value} timestamp must be a non-empty UTC ISO-8601 string.",
        )
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise PromotionJournalError(
            "JOURNAL_TIMESTAMP_INVALID",
            f"{phase.value} timestamp is not valid ISO-8601.",
        ) from exc
    return parsed


def _validate_tock_contract(journal: PromotionJournal) -> None:
    phase_index = _PHASE_ORDER.index(journal.phase)
    git_applied_index = _PHASE_ORDER.index(PromotionJournalPhase.GIT_APPLIED)
    if phase_index < git_applied_index:
        if journal.tock_sha is not None:
            raise PromotionJournalError(
                "JOURNAL_TOCK_FORBIDDEN",
                "tock_sha must not exist before GIT_APPLIED.",
            )
        return
    if not isinstance(journal.tock_sha, str) or not journal.tock_sha.strip():
        raise PromotionJournalError(
            "JOURNAL_TOCK_REQUIRED",
            "GIT_APPLIED and later phases require the actual observed main tock_sha.",
        )


def _require_text_fields(
    payload: Mapping[str, Any],
    *,
    fields: tuple[str, ...],
    reason_code: str,
) -> None:
    invalid = [
        field
        for field in fields
        if not isinstance(payload.get(field), str) or not str(payload.get(field)).strip()
    ]
    if invalid:
        raise PromotionJournalError(
            reason_code,
            f"required fields are missing or empty: {', '.join(invalid)}.",
        )


def _required_text(value: Any, *, reason_code: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PromotionJournalError(
            reason_code,
            f"{field} must be a non-empty string.",
        )
    return value


def _is_lowercase_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and re.fullmatch(r"[0-9a-f]{64}", value) is not None
    )


def _require_snapshot_authority_anchors(source: Mapping[str, Any]) -> None:
    invalid = [
        field
        for field in _SNAPSHOT_AUTHORITY_FIELDS
        if not _is_lowercase_sha256(source.get(field))
    ]
    if invalid:
        raise PromotionJournalError(
            "JOURNAL_FIELDS_INVALID",
            "snapshot authority anchors must be complete lowercase 64-hex sha256: "
            f"{', '.join(invalid)}.",
        )


def _validate_promotion_class(value: str) -> None:
    if value not in ALLOWED_PROMOTION_CLASSES:
        raise PromotionJournalError(
            "JOURNAL_PROMOTION_CLASS_INVALID",
            "promotion_class must be GREEN_PROMOTION or RED_TDD_PROMOTION.",
        )


def _optional_text(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise PromotionJournalError(
            "JOURNAL_FIELDS_INVALID",
            f"{field} must be absent or a non-empty string.",
        )
    return value


def _safe_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    if not normalized:
        raise PromotionJournalError(
            "JOURNAL_IDENTITY_INCOMPLETE",
            "promotion journal identity cannot produce an archive path.",
        )
    return normalized


def _atomic_write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    temporary_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            yaml.safe_dump(
                dict(payload),
                temporary,
                sort_keys=False,
                allow_unicode=False,
            )
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    except OSError as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise PromotionJournalError(
            "JOURNAL_WRITE_FAILED",
            f"promotion journal atomic replace failed: {path}.",
        ) from exc
