"""Public ordinary continuation-contract.

The codec accepts one YAML mapping and materializes a typed, immutable
FINALIZED_PROMOTION_TICK view. Observation is a pure function of that view.
The contract does not load runtime paths, consume artifacts, prove git
history, recover state, or authorize a decision or promotion.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml


_SCHEMA_VERSION = "2.0"
_STATUS_AUTHORIZED = "AUTHORIZED"
_STATUS_CONSUMED = "CONSUMED"
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_FIELDS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
)
_COMMON_ARTIFACT_FIELDS = frozenset(
    {
        "schema_version",
        "origin",
        "original_tick_sha",
        "expected_continuation_head_sha",
        "commit_chain",
        "successor_task_id",
        "status",
        "prepared_at_utc",
        "continuation_digest_sha256",
    }
)
_CONSUMPTION_FIELDS = frozenset(
    {
        "successor_promotion_attempt_id",
        "successor_tock_sha",
        "consumed_at_utc",
    }
)
_TEXT_ARTIFACT_FIELDS = (
    "original_tick_sha",
    "expected_continuation_head_sha",
    "successor_task_id",
    "prepared_at_utc",
    "continuation_digest_sha256",
    "status",
)


class ContinuationError(Exception):
    """Fail-closed violation of the public continuation contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


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


class ContinuationOrigin(str, Enum):
    """Closed public origin set: ordinary finalized promotion tick only."""

    FINALIZED_PROMOTION_TICK = "FINALIZED_PROMOTION_TICK"


class LifecyclePhase(str, Enum):
    """Monotone observations of mutable continuation lifecycle evidence."""

    PRE_ACK = "PRE_ACK"
    CONSUMED = "CONSUMED"


@dataclass(frozen=True)
class ContinuationIdentity:
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str
    promotion_class: str

    def to_mapping(self) -> dict[str, str]:
        return {
            "task_id": self.task_id,
            "origin_run_id": self.origin_run_id,
            "promotion_attempt_id": self.promotion_attempt_id,
            "result_sha": self.result_sha,
            "promotion_class": self.promotion_class,
        }


@dataclass(frozen=True)
class ContinuationContractView:
    origin: ContinuationOrigin
    identity: ContinuationIdentity
    immutable_binding: Mapping[str, Any]
    continuation_digest_sha256: str
    status: str
    successor_task_id: str
    original_tick_sha: str
    expected_continuation_head_sha: str
    artifact_sha256: str | None
    _payload: Mapping[str, Any]

    def to_mapping(self) -> dict[str, Any]:
        return _thaw(self._payload)


@dataclass(frozen=True)
class LifecycleObservation:
    phase: LifecyclePhase
    identity: ContinuationIdentity


_ALLOWED_TRANSITIONS: Mapping[
    ContinuationOrigin, Mapping[LifecyclePhase, tuple[LifecyclePhase, ...]]
] = MappingProxyType(
    {
        ContinuationOrigin.FINALIZED_PROMOTION_TICK: MappingProxyType(
            {
                LifecyclePhase.PRE_ACK: (
                    LifecyclePhase.PRE_ACK,
                    LifecyclePhase.CONSUMED,
                ),
                LifecyclePhase.CONSUMED: (LifecyclePhase.CONSUMED,),
            }
        ),
    }
)


def decode_continuation_artifact(
    payload: Mapping[str, Any],
    *,
    artifact_sha256: str | None = None,
) -> ContinuationContractView:
    """Validate schema 2.0 and expose immutable typed contract views."""

    if not isinstance(payload, Mapping):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    copied = _thaw(_freeze(payload))
    _validate_artifact_shape(copied)
    _validate_declared_chain_binding(copied)
    _validate_artifact_digest(copied)
    origin = _artifact_origin(copied)
    identity = ContinuationIdentity(
        **_normalized_identity(copied["origin"]["evidence"]["predecessor_identity"])
    )
    return ContinuationContractView(
        origin=origin,
        identity=identity,
        immutable_binding=_freeze(_artifact_binding(copied)),
        continuation_digest_sha256=str(copied["continuation_digest_sha256"]),
        status=str(copied["status"]),
        successor_task_id=str(copied["successor_task_id"]),
        original_tick_sha=str(copied["original_tick_sha"]),
        expected_continuation_head_sha=str(copied["expected_continuation_head_sha"]),
        artifact_sha256=artifact_sha256,
        _payload=_freeze(copied),
    )


def load_continuation_artifact(source_path: Path | str) -> ContinuationContractView:
    """Read and decode one artifact without rewriting any source byte."""

    artifact_path = Path(source_path)
    try:
        source = artifact_path.read_bytes()
        payload = yaml.load(source, Loader=_StrictLoader)
    except (OSError, TypeError, yaml.YAMLError):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID") from None
    if not isinstance(payload, Mapping):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    return decode_continuation_artifact(
        payload,
        artifact_sha256=hashlib.sha256(source).hexdigest(),
    )


def observe_lifecycle(view: ContinuationContractView) -> LifecycleObservation:
    """Derive monotone phase facts from a decoded view. No runtime access."""

    if view.status == _STATUS_CONSUMED:
        phase = LifecyclePhase.CONSUMED
    else:
        phase = LifecyclePhase.PRE_ACK
    return LifecycleObservation(phase=phase, identity=view.identity)


def assert_lifecycle_transition(
    *,
    origin: ContinuationOrigin,
    previous: LifecyclePhase,
    current: LifecyclePhase,
) -> None:
    """Accept only idempotent or explicitly monotone origin transitions."""

    allowed = _ALLOWED_TRANSITIONS[origin][previous]
    if current not in allowed:
        raise ContinuationError("CONTINUATION_LIFECYCLE_INVALID")


def assert_origin_reproof_matches(
    *,
    view: ContinuationContractView,
    current_evidence: Mapping[str, Any],
) -> None:
    """Compare immutable origin evidence of the same finalized tick."""

    persisted = _thaw(view.to_mapping()["origin"]["evidence"])
    current = _thaw(_freeze(current_evidence))
    if current != persisted:
        raise ContinuationError("CONTINUATION_IDENTITY_MISMATCH")


def binding_digest(binding: Mapping[str, Any]) -> str:
    serializable = _thaw(binding)
    encoded = json.dumps(
        serializable,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _artifact_origin(payload: Mapping[str, Any]) -> ContinuationOrigin:
    kind = payload["origin"]["kind"]
    try:
        return ContinuationOrigin(str(kind))
    except ValueError:
        raise ContinuationError("CONTINUATION_ORIGIN_INVALID") from None


def _validate_artifact_shape(payload: Mapping[str, Any]) -> None:
    allowed_fields = set(_COMMON_ARTIFACT_FIELDS)
    status = payload.get("status")
    if status == _STATUS_CONSUMED:
        allowed_fields.update(_CONSUMPTION_FIELDS)
    if set(payload) != allowed_fields:
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    if status not in {_STATUS_AUTHORIZED, _STATUS_CONSUMED}:
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    origin = payload.get("origin")
    if not isinstance(origin, Mapping):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    evidence = origin.get("evidence")
    if set(origin) != {"kind", "evidence"} or not isinstance(evidence, Mapping):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    _artifact_origin(payload)
    identity = evidence.get("predecessor_identity")
    if set(evidence) != {"predecessor_identity", "tock_sha"} or not isinstance(
        identity, Mapping
    ):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    if dict(identity) != _normalized_identity(identity):
        raise ContinuationError("CONTINUATION_ARTIFACT_INVALID")
    _require_text(evidence.get("tock_sha"), "CONTINUATION_ARTIFACT_INVALID")
    for key in _TEXT_ARTIFACT_FIELDS:
        _require_text(payload.get(key), "CONTINUATION_ARTIFACT_INVALID")
    if status == _STATUS_CONSUMED:
        for key in _CONSUMPTION_FIELDS:
            _require_text(payload.get(key), "CONTINUATION_ARTIFACT_INVALID")
    chain = payload.get("commit_chain")
    if not isinstance(chain, list) or not chain:
        raise ContinuationError("CONTINUATION_CHAIN_MISMATCH")
    for entry in chain:
        if not isinstance(entry, Mapping) or set(entry) != {"sha", "parent_sha"}:
            raise ContinuationError("CONTINUATION_CHAIN_MISMATCH")
        _require_text(entry.get("sha"), "CONTINUATION_CHAIN_MISMATCH")
        _require_text(entry.get("parent_sha"), "CONTINUATION_CHAIN_MISMATCH")


def _validate_declared_chain_binding(payload: Mapping[str, Any]) -> None:
    chain = payload["commit_chain"]
    if chain[0]["sha"] != payload["original_tick_sha"]:
        raise ContinuationError("CONTINUATION_CHAIN_MISMATCH")
    expected_parent = payload["origin"]["evidence"]["tock_sha"]
    for entry in chain:
        if entry["parent_sha"] != expected_parent:
            raise ContinuationError("CONTINUATION_CHAIN_MISMATCH")
        expected_parent = entry["sha"]
    if chain[-1]["sha"] != payload["expected_continuation_head_sha"]:
        raise ContinuationError("CONTINUATION_CHAIN_MISMATCH")


def _artifact_binding(payload: Mapping[str, Any]) -> dict[str, Any]:
    origin = payload["origin"]
    return {
        "schema_version": payload["schema_version"],
        "origin": {
            "kind": origin["kind"],
            "evidence": dict(origin["evidence"]),
        },
        "original_tick_sha": payload["original_tick_sha"],
        "expected_continuation_head_sha": payload["expected_continuation_head_sha"],
        "commit_chain": [dict(entry) for entry in payload["commit_chain"]],
        "successor_task_id": payload["successor_task_id"],
    }


def _validate_artifact_digest(payload: Mapping[str, Any]) -> None:
    digest = payload["continuation_digest_sha256"]
    if (
        _DIGEST_PATTERN.fullmatch(digest) is None
        or digest != binding_digest(_artifact_binding(payload))
    ):
        raise ContinuationError("CONTINUATION_DIGEST_MISMATCH")


def _normalized_identity(payload: Mapping[str, Any]) -> dict[str, str]:
    identity: dict[str, str] = {}
    for field in _IDENTITY_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ContinuationError("CONTINUATION_IDENTITY_MISMATCH")
        identity[field] = value
    return identity


def _require_text(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContinuationError(reason_code)
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value
