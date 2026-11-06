"""Fresh public promotion-state and consumption persistence.

Closed ``main_state.yaml`` and append-only ``promotion_consumed.yaml``
are validated, compared, and replaced atomically. Start-TICK CAS binds
only the current observed head: apply from that head to the tick, or
no-op when expected base, tick, and observed head are already the same.
The module does not admit promotion, persist pending intent, talk to
Git, or import private runner context.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

import yaml


_SCHEMA_VERSION = "1.0"
_CANONICAL_MAIN_STATES = frozenset(
    {"GREEN_VALIDATED", "EXPECTED_RED_OPEN", "BLOCKED"}
)
_MAIN_STATE_FIELDS = (
    "schema_version",
    "state",
    "main_branch",
    "validated_result_sha",
    "last_tock_sha",
    "observed_main_head_sha",
    "reason_code",
    "updated_at_utc",
)
_MAIN_STATE_FIELD_SET = frozenset(_MAIN_STATE_FIELDS)
_CONSUMPTION_LEDGER_FIELDS = frozenset({"schema_version", "consumed_promotions"})
_CONSUMPTION_ENTRY_FIELDS = frozenset(
    {
        "promotion_attempt_id",
        "result_sha",
        "main_state_before_promotion",
        "consumed_at_utc",
        "tock_sha",
    }
)
_CONSUMPTION_ENTRY_REQUIRED = _CONSUMPTION_ENTRY_FIELDS


class RunnerPromotionStateError(RuntimeError):
    """Fail-closed violation of persisted promotion-state truth."""

    def __init__(self, reason_code: str, detail: str = "") -> None:
        self.reason_code = reason_code
        message = reason_code if not detail else f"{reason_code}: {detail}"
        super().__init__(message)


class _DuplicateKeyError(yaml.YAMLError):
    """A mapping node declared the same key more than once."""


class _StrictLoader(yaml.SafeLoader):
    """Safe loader that rejects duplicate mapping keys at every nesting level."""


def _construct_mapping_rejecting_duplicates(
    loader: yaml.Loader,
    node: yaml.nodes.MappingNode,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise _DuplicateKeyError("duplicate mapping key")
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_rejecting_duplicates,
)


def load_and_validate_main_state(*, main_state_path: Path | str) -> dict[str, Any]:
    payload = _load_yaml_mapping(
        Path(main_state_path),
        missing_or_invalid="MAIN_STATE_INVALID",
        duplicate_keys="MAIN_STATE_VALUE_CONFLICT",
        unreadable="MAIN_STATE_UNREADABLE",
        missing_is_error=True,
    )
    return _canonical_main_state(payload)


def write_main_state(
    *,
    main_state_path: Path | str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    canonical = _canonical_main_state(payload)
    _atomic_write_yaml(Path(main_state_path), canonical)
    return canonical


def update_observed_main_head_sha(
    *,
    main_state_path: Path | str,
    observed_main_head_sha: str,
    expected_last_tock_sha: str,
) -> dict[str, Any]:
    path = Path(main_state_path)
    payload = load_and_validate_main_state(main_state_path=path)
    if payload["last_tock_sha"] != expected_last_tock_sha:
        raise RunnerPromotionStateError(
            "MAIN_STATE_DRIFT",
            "last_tock_sha does not match expected_last_tock_sha.",
        )
    if not _valid_text(observed_main_head_sha):
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            "observed_main_head_sha must be a non-empty string.",
        )
    if payload["observed_main_head_sha"] == observed_main_head_sha:
        return payload
    updated = {
        **payload,
        "observed_main_head_sha": observed_main_head_sha,
    }
    return write_main_state(main_state_path=path, payload=updated)


def transition_main_state_for_start_tick(
    *,
    main_state_path: Path | str,
    expected_base_sha: str,
    tick_sha: str,
) -> dict[str, Any]:
    if not _valid_text(expected_base_sha) or not _valid_text(tick_sha):
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            "Start-TICK CAS requires non-empty expected_base_sha and tick_sha.",
        )
    path = Path(main_state_path)
    payload = load_and_validate_main_state(main_state_path=path)
    current = payload["observed_main_head_sha"]
    if current == tick_sha:
        if expected_base_sha != current:
            raise RunnerPromotionStateError(
                "MAIN_STATE_DRIFT",
                "expected_base_sha does not bind the Start-TICK already on observed Main.",
            )
        return payload
    if current != expected_base_sha:
        raise RunnerPromotionStateError(
            "MAIN_STATE_DRIFT",
            "observed_main_head_sha does not match expected_base_sha.",
        )
    updated = {
        **payload,
        "observed_main_head_sha": tick_sha,
    }
    return write_main_state(main_state_path=path, payload=updated)


def load_promotion_consumed(*, promotion_consumed_path: Path | str) -> dict[str, Any]:
    loaded = _load_yaml_mapping(
        Path(promotion_consumed_path),
        missing_or_invalid="PROMOTION_CONSUMPTION_INVALID",
        duplicate_keys="PROMOTION_CONSUMPTION_VALUE_CONFLICT",
        unreadable="PROMOTION_CONSUMPTION_UNREADABLE",
        missing_is_error=False,
    )
    if loaded is None:
        return {
            "schema_version": _SCHEMA_VERSION,
            "consumed_promotions": [],
        }
    return _canonical_consumption_ledger(loaded)


def is_promotion_consumed(
    *,
    consumed_payload: Mapping[str, Any],
    promotion_attempt_id: str,
    result_sha: str,
) -> bool:
    for consumed in consumed_payload["consumed_promotions"]:
        if (
            consumed["promotion_attempt_id"] == promotion_attempt_id
            and consumed["result_sha"] == result_sha
        ):
            return True
    return False


def record_promotion_consumed(
    *,
    promotion_consumed_path: Path | str,
    promotion_attempt_id: str,
    result_sha: str,
    main_state_before_promotion: Mapping[str, Any],
    consumed_at_utc: str | None = None,
    tock_sha: str | None = None,
) -> dict[str, Any]:
    attempt_id = _require_identity_text(promotion_attempt_id, "promotion_attempt_id")
    bound_result_sha = _require_identity_text(result_sha, "result_sha")
    bound_tock_sha = _require_identity_text(tock_sha, "tock_sha")
    pre_state = _canonical_main_state(main_state_before_promotion)
    path = Path(promotion_consumed_path)
    payload = load_promotion_consumed(promotion_consumed_path=path)
    matching = _consumption_identity_match(
        payload["consumed_promotions"],
        attempt_id=attempt_id,
        result_sha=bound_result_sha,
        tock_sha=bound_tock_sha,
        pre_state=pre_state,
    )
    if matching == "identical":
        return payload
    if matching == "conflict":
        raise RunnerPromotionStateError(
            "PROMOTION_CONSUMPTION_CONFLICT",
            "promotion consumption identity is reused with conflicting truth.",
        )
    bound_consumed_at = _require_identity_text(consumed_at_utc, "consumed_at_utc")
    entry = {
        "promotion_attempt_id": attempt_id,
        "result_sha": bound_result_sha,
        "main_state_before_promotion": pre_state,
        "consumed_at_utc": bound_consumed_at,
        "tock_sha": bound_tock_sha,
    }
    updated = {
        "schema_version": _SCHEMA_VERSION,
        "consumed_promotions": [*payload["consumed_promotions"], entry],
    }
    _atomic_write_yaml(path, updated)
    return updated


def _valid_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _require_identity_text(value: Any, field: str) -> str:
    if not _valid_text(value):
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            f"{field} must be a non-empty string.",
        )
    return value


def _canonical_main_state(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            "main_state payload must be a mapping.",
        )
    unknown = [key for key in payload if key not in _MAIN_STATE_FIELD_SET]
    if unknown:
        raise RunnerPromotionStateError(
            "MAIN_STATE_FIELD_UNKNOWN",
            f"main_state payload contains unknown fields: {', '.join(unknown)}.",
        )
    missing = [field for field in _MAIN_STATE_FIELDS if field not in payload]
    if missing:
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            f"main_state payload is missing required fields: {', '.join(missing)}.",
        )
    schema_version = payload["schema_version"]
    state = payload["state"]
    if schema_version != _SCHEMA_VERSION or not isinstance(schema_version, str):
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            "main_state schema_version is invalid.",
        )
    if state not in _CANONICAL_MAIN_STATES or not isinstance(state, str):
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            "main_state state is invalid.",
        )
    canonical: dict[str, Any] = {}
    for field in _MAIN_STATE_FIELDS:
        value = payload[field]
        if field == "schema_version":
            canonical[field] = schema_version
            continue
        if field == "state":
            canonical[field] = state
            continue
        if not _valid_text(value):
            raise RunnerPromotionStateError(
                "MAIN_STATE_INVALID",
                f"main_state field {field} must be a non-empty string.",
            )
        canonical[field] = value
    return canonical


def _canonical_consumption_ledger(payload: Mapping[str, Any]) -> dict[str, Any]:
    unknown = [key for key in payload if key not in _CONSUMPTION_LEDGER_FIELDS]
    if unknown:
        raise RunnerPromotionStateError(
            "PROMOTION_CONSUMPTION_FIELD_UNKNOWN",
            "promotion_consumed payload contains unknown fields.",
        )
    schema_version = payload.get("schema_version")
    consumed = payload.get("consumed_promotions")
    if schema_version != _SCHEMA_VERSION or not isinstance(schema_version, str):
        raise RunnerPromotionStateError(
            "PROMOTION_CONSUMPTION_INVALID",
            "promotion_consumed schema_version is invalid.",
        )
    if not isinstance(consumed, list):
        raise RunnerPromotionStateError(
            "PROMOTION_CONSUMPTION_INVALID",
            "consumed_promotions must be a list.",
        )
    entries = [_canonical_consumption_entry(entry) for entry in consumed]
    return {
        "schema_version": _SCHEMA_VERSION,
        "consumed_promotions": entries,
    }


def _canonical_consumption_entry(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise RunnerPromotionStateError(
            "PROMOTION_CONSUMPTION_INVALID",
            "consumed promotion entry must be a mapping.",
        )
    unknown = [key for key in entry if key not in _CONSUMPTION_ENTRY_FIELDS]
    if unknown:
        raise RunnerPromotionStateError(
            "PROMOTION_CONSUMPTION_FIELD_UNKNOWN",
            "consumed promotion entry contains unknown fields.",
        )
    missing = [field for field in _CONSUMPTION_ENTRY_REQUIRED if field not in entry]
    if missing:
        raise RunnerPromotionStateError(
            "MAIN_STATE_INVALID",
            "consumed promotion entry is missing required identity fields.",
        )
    attempt_id = _require_identity_text(
        entry.get("promotion_attempt_id"),
        "promotion_attempt_id",
    )
    result_sha = _require_identity_text(entry.get("result_sha"), "result_sha")
    tock_sha = _require_identity_text(entry.get("tock_sha"), "tock_sha")
    pre_state = _canonical_main_state(entry.get("main_state_before_promotion"))
    return {
        "promotion_attempt_id": attempt_id,
        "result_sha": result_sha,
        "main_state_before_promotion": pre_state,
        "consumed_at_utc": _require_identity_text(
            entry["consumed_at_utc"],
            "consumed_at_utc",
        ),
        "tock_sha": tock_sha,
    }


def _consumption_identity_match(
    entries: list[dict[str, Any]],
    *,
    attempt_id: str,
    result_sha: str,
    tock_sha: str,
    pre_state: Mapping[str, Any],
) -> str:
    verdict = "none"
    for entry in entries:
        same_attempt = entry["promotion_attempt_id"] == attempt_id
        same_result = entry["result_sha"] == result_sha
        same_tock = entry["tock_sha"] == tock_sha
        if not same_attempt and not same_result and not same_tock:
            continue
        if (
            same_attempt
            and same_result
            and same_tock
            and entry["main_state_before_promotion"] == pre_state
        ):
            verdict = "identical"
            continue
        return "conflict"
    return verdict


def _load_yaml_mapping(
    path: Path,
    *,
    missing_or_invalid: str,
    duplicate_keys: str,
    unreadable: str,
    missing_is_error: bool,
) -> dict[str, Any] | None:
    if not path.is_file():
        if missing_is_error:
            raise RunnerPromotionStateError(
                missing_or_invalid,
                f"YAML artefact is missing: {path}.",
            )
        return None
    try:
        document_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RunnerPromotionStateError(
            unreadable,
            f"YAML artefact is unreadable: {path}.",
        ) from exc
    try:
        loaded = yaml.load(document_text, Loader=_StrictLoader)
    except _DuplicateKeyError as exc:
        raise RunnerPromotionStateError(
            duplicate_keys,
            f"YAML artefact contains duplicate keys: {path}.",
        ) from exc
    except yaml.YAMLError as exc:
        raise RunnerPromotionStateError(
            missing_or_invalid,
            f"YAML artefact is malformed: {path}.",
        ) from exc
    if not isinstance(loaded, dict):
        raise RunnerPromotionStateError(
            missing_or_invalid,
            f"YAML artefact must be a mapping: {path}.",
        )
    return loaded


def _atomic_write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary_path.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        raise RunnerPromotionStateError(
            "MAIN_STATE_WRITE_FAILED",
            f"atomic replace failed: {path}.",
        ) from exc
