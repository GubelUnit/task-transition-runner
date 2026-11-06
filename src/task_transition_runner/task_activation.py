"""Public fresh-activation and terminal-rest decision.

``evaluate_task_activation`` consumes already loaded public task-source
contract ``TaskSourceFacts``. It does not parse a queue, invent a task, select
among tasks, persist state, or create a Start-TICK or task branch.

``active_task: null`` after a settled predecessor is terminal rest.
Exactly one external active task may become exactly one fresh identity
bound to the immutable task fingerprint and instruction-snapshot
sources. An identical already-settled task is a no-op only when
``task_id``, contract fingerprint, and the immutable public task-source
contract signature all match. That signature is ``logical_ref``, ``sha256``, and
``byte_length``. ``snapshot_path`` is run-local persistence, not
identity.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4 as _uuid4

from task_transition_runner.task_source import (
    InstructionSourceBinding,
    TaskSourceFacts,
)


_PREDECESSOR_FIELDS = frozenset(
    {
        "settled",
        "origin_run_id",
        "promotion_attempt_id",
        "task_id",
        "task_fingerprint",
        "instruction_sources",
    }
)
_SOURCE_IDENTITY_FIELDS = frozenset({"logical_ref", "sha256", "byte_length"})
_MISSING = object()
_ACTIVATION_SCHEMA_VERSION = "1.0"
_ORIGIN_DOMAIN = "task-transition-runner/activation-origin/v1"
_ATTEMPT_DOMAIN = "task-transition-runner/promotion-attempt/v1"
_FRESH_INIT_KIND = "FRESH_INIT_BASELINE"
_SETTLED_PROMOTION_KIND = "SETTLED_PROMOTION"
_FRESH_BASIS_KEYS = frozenset({"kind", "main_state"})
_SETTLED_BASIS_KEYS = frozenset({"kind", "predecessor"})
_SETTLED_BASIS_PREDECESSOR_KEYS = frozenset(
    {
        "settled",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "task_fingerprint",
        "instruction_sources",
    }
)
_FRESH_MAIN_STATE_KEYS = frozenset(
    {
        "schema_version",
        "state",
        "main_branch",
        "validated_result_sha",
        "last_tock_sha",
        "observed_main_head_sha",
        "reason_code",
        "updated_at_utc",
    }
)
class TaskActivationError(Exception):
    """Fail-closed violation of the public activation contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class ActivationStatus(Enum):
    """The only public activation outcomes."""

    TERMINAL_REST = "TERMINAL_REST"
    ACTIVATED = "ACTIVATED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class ActivationIdentity:
    task_id: str
    task_fingerprint: str
    instruction_sources: tuple[InstructionSourceBinding, ...]
    origin_run_id: str
    promotion_attempt_id: str


@dataclass(frozen=True)
class ActivationDecision:
    status: ActivationStatus
    reason_code: str
    facts: TaskSourceFacts
    identity: ActivationIdentity | None


def evaluate_task_activation(
    facts: object,
    *,
    predecessor: object,
) -> ActivationDecision:
    if not isinstance(facts, TaskSourceFacts):
        raise TaskActivationError("FACTS_FIELD_TYPE")
    if isinstance(predecessor, Mapping) and "kind" in predecessor:
        basis = _closed_activation_basis(predecessor)
        return _decide_activation_basis(facts, basis)
    closed = _closed_predecessor(predecessor)
    if not closed["settled"]:
        return _decision(
            facts,
            status=ActivationStatus.BLOCKED,
            reason_code="UNRESOLVED_PREDECESSOR",
        )
    active = facts.contract.active_task
    if active is None:
        return _decision(
            facts,
            status=ActivationStatus.TERMINAL_REST,
            reason_code="ACTIVE_TASK_NULL",
        )
    predecessor_task_id = closed["task_id"]
    if predecessor_task_id is not _MISSING and predecessor_task_id == active.task_id:
        current_sources = _facts_source_signature(facts.sources)
        if (
            closed["task_fingerprint"] == facts.contract.fingerprint
            and closed["instruction_sources"] == current_sources
        ):
            return _decision(
                facts,
                status=ActivationStatus.TERMINAL_REST,
                reason_code="IDENTICAL_SETTLED_TASK",
            )
        return _decision(
            facts,
            status=ActivationStatus.BLOCKED,
            reason_code="SETTLED_TASK_IDENTITY_DRIFT",
        )
    return _decision(
        facts,
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        identity=_fresh_identity(
            task_id=active.task_id,
            task_fingerprint=facts.contract.fingerprint,
            instruction_sources=facts.sources,
        ),
    )


def _closed_predecessor(predecessor: object) -> dict[str, object]:
    if not isinstance(predecessor, Mapping):
        raise TaskActivationError("PREDECESSOR_FIELD_TYPE")
    unknown = frozenset(predecessor) - _PREDECESSOR_FIELDS
    if unknown:
        raise TaskActivationError("PREDECESSOR_FIELD_UNKNOWN")
    if "settled" not in predecessor:
        raise TaskActivationError("PREDECESSOR_FIELD_MISSING")
    settled = predecessor["settled"]
    if not isinstance(settled, bool):
        raise TaskActivationError("PREDECESSOR_FIELD_TYPE")
    origin_run_id = _require_predecessor_text(predecessor, "origin_run_id")
    promotion_attempt_id = _require_predecessor_text(
        predecessor,
        "promotion_attempt_id",
    )
    task_id: object = _MISSING
    task_fingerprint: object = None
    instruction_sources: object = None
    if "task_id" in predecessor:
        task_id = _require_predecessor_text(predecessor, "task_id")
    if settled and task_id is not _MISSING:
        task_fingerprint = _require_predecessor_text(predecessor, "task_fingerprint")
        instruction_sources = _require_source_signature(predecessor)
    return {
        "settled": settled,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
        "task_id": task_id,
        "task_fingerprint": task_fingerprint,
        "instruction_sources": instruction_sources,
    }


def _require_predecessor_text(predecessor: Mapping[object, object], field: str) -> str:
    if field not in predecessor:
        raise TaskActivationError("PREDECESSOR_FIELD_MISSING")
    value = predecessor[field]
    if not isinstance(value, str) or value == "":
        raise TaskActivationError("PREDECESSOR_FIELD_TYPE")
    return value


def _require_source_signature(
    predecessor: Mapping[object, object],
) -> tuple[tuple[object, str, object], ...]:
    if "instruction_sources" not in predecessor:
        raise TaskActivationError("PREDECESSOR_FIELD_MISSING")
    signature = _source_identity_signature(predecessor["instruction_sources"])
    if signature is None:
        raise TaskActivationError("PREDECESSOR_FIELD_TYPE")
    return signature


def _source_identity_signature(
    value: object,
) -> tuple[tuple[object, str, object], ...] | None:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        return None
    if len(value) == 0:
        return None
    signature: list[tuple[object, str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            return None
        if not _SOURCE_IDENTITY_FIELDS.issubset(item):
            return None
        logical_ref = item["logical_ref"]
        sha256 = item["sha256"]
        byte_length = item["byte_length"]
        if not isinstance(logical_ref, str):
            return None
        if not isinstance(sha256, str) or sha256 == "":
            return None
        if type(byte_length) is not int:
            return None
        signature.append((logical_ref, sha256, byte_length))
    return tuple(signature)


def _facts_source_signature(
    sources: tuple[InstructionSourceBinding, ...],
) -> tuple[tuple[str, str, int], ...]:
    return tuple(
        (item.logical_ref, item.sha256, item.byte_length) for item in sources
    )


def _closed_activation_basis(basis: Mapping[object, object]) -> Mapping[str, object]:
    kind = basis.get("kind")
    if kind == _FRESH_INIT_KIND:
        if frozenset(basis) != _FRESH_BASIS_KEYS or not _fresh_main_state_bound(
            basis["main_state"]
        ):
            raise TaskActivationError("ACTIVATION_BASIS_INVALID")
        return basis
    if kind == _SETTLED_PROMOTION_KIND:
        if frozenset(basis) != _SETTLED_BASIS_KEYS or not _settled_basis_predecessor_bound(
            basis["predecessor"]
        ):
            raise TaskActivationError("ACTIVATION_BASIS_INVALID")
        return basis
    raise TaskActivationError("ACTIVATION_BASIS_INVALID")


def _fresh_main_state_bound(state: object) -> bool:
    if not isinstance(state, Mapping) or frozenset(state) != _FRESH_MAIN_STATE_KEYS:
        return False
    if state["schema_version"] != _ACTIVATION_SCHEMA_VERSION:
        return False
    if state["state"] != "GREEN_VALIDATED":
        return False
    if state["reason_code"] != "BOOTSTRAP_OK":
        return False
    bound = (
        state["validated_result_sha"],
        state["last_tock_sha"],
        state["observed_main_head_sha"],
    )
    return len(set(bound)) == 1


def _settled_basis_predecessor_bound(predecessor: Mapping[object, object]) -> bool:
    if frozenset(predecessor) != _SETTLED_BASIS_PREDECESSOR_KEYS:
        return False
    if predecessor["settled"] is not True:
        return False
    sources = predecessor["instruction_sources"]
    if len(sources) == 0:
        return False
    for item in sources:
        if frozenset(item) != _SOURCE_IDENTITY_FIELDS:
            return False
    return True


def select_cli_activation_basis(
    *,
    main_state: object,
    settled_predecessor: object,
) -> dict[str, object]:
    """Choose one closed basis. Mixed, missing, or invented identity blocks."""

    if isinstance(main_state, Mapping) and (
        "origin_run_id" in main_state or "promotion_attempt_id" in main_state
    ):
        raise TaskActivationError("ACTIVATION_BASIS_INVALID")
    if _fresh_main_state_bound(main_state) and settled_predecessor is None:
        return {"kind": _FRESH_INIT_KIND, "main_state": main_state}
    if main_state is None and isinstance(settled_predecessor, Mapping):
        return {
            "kind": _SETTLED_PROMOTION_KIND,
            "predecessor": settled_predecessor,
        }
    raise TaskActivationError("ACTIVATION_BASIS_INVALID")


def _decide_activation_basis(
    facts: TaskSourceFacts,
    basis: Mapping[str, object],
) -> ActivationDecision:
    active = facts.contract.active_task
    if active is None:
        return _decision(
            facts,
            status=ActivationStatus.TERMINAL_REST,
            reason_code="ACTIVE_TASK_NULL",
        )
    if basis["kind"] == _FRESH_INIT_KIND:
        return _activated_from_basis(facts, basis)
    predecessor = basis["predecessor"]
    assert isinstance(predecessor, Mapping)
    if active.task_id == predecessor["task_id"]:
        current = (
            facts.contract.fingerprint,
            _facts_source_signature(facts.sources),
        )
        bound_sources = _basis_source_signature(predecessor["instruction_sources"])
        bound = (predecessor["task_fingerprint"], bound_sources)
        if current == bound:
            return _decision(
                facts,
                status=ActivationStatus.TERMINAL_REST,
                reason_code="IDENTICAL_SETTLED_TASK",
            )
        return _decision(
            facts,
            status=ActivationStatus.BLOCKED,
            reason_code="SETTLED_TASK_IDENTITY_DRIFT",
        )
    return _activated_from_basis(facts, basis)


def _basis_source_signature(sources: object) -> tuple[tuple[object, object, object], ...]:
    assert isinstance(sources, Sequence)
    signature: list[tuple[object, object, object]] = []
    for item in sources:
        assert isinstance(item, Mapping)
        signature.append((item["logical_ref"], item["sha256"], item["byte_length"]))
    return tuple(signature)


def _activated_from_basis(
    facts: TaskSourceFacts,
    basis: Mapping[str, object],
) -> ActivationDecision:
    active = facts.contract.active_task
    assert active is not None
    origin_run_id, promotion_attempt_id = _derived_identity_ids(facts, basis)
    return _decision(
        facts,
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        identity=ActivationIdentity(
            task_id=active.task_id,
            task_fingerprint=facts.contract.fingerprint,
            instruction_sources=facts.sources,
            origin_run_id=origin_run_id,
            promotion_attempt_id=promotion_attempt_id,
        ),
    )


def _derived_identity_ids(
    facts: TaskSourceFacts,
    basis: Mapping[str, object],
) -> tuple[str, str]:
    payload = _derivation_payload(facts, basis)
    return (
        "origin-" + _domain_digest(_ORIGIN_DOMAIN, payload),
        "attempt-" + _domain_digest(_ATTEMPT_DOMAIN, payload),
    )


def _derivation_payload(
    facts: TaskSourceFacts,
    basis: Mapping[str, object],
) -> dict[str, object]:
    active = facts.contract.active_task
    assert active is not None
    return {
        "schema_version": _ACTIVATION_SCHEMA_VERSION,
        "task": {
            "task_id": active.task_id,
            "task_fingerprint": facts.contract.fingerprint,
            "instruction_sources": [
                {
                    "logical_ref": item.logical_ref,
                    "sha256": item.sha256,
                    "byte_length": item.byte_length,
                }
                for item in facts.sources
            ],
        },
        "activation_basis": basis,
    }


def _domain_digest(domain: str, payload: Mapping[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(domain.encode("utf-8") + b"\x00" + canonical).hexdigest()


def _fresh_identity(
    *,
    task_id: str,
    task_fingerprint: str,
    instruction_sources: tuple[InstructionSourceBinding, ...],
) -> ActivationIdentity:
    return ActivationIdentity(
        task_id=task_id,
        task_fingerprint=task_fingerprint,
        instruction_sources=instruction_sources,
        origin_run_id="origin-" + _uuid4().hex,
        promotion_attempt_id="attempt-" + _uuid4().hex,
    )


def _decision(
    facts: TaskSourceFacts,
    *,
    status: ActivationStatus,
    reason_code: str,
    identity: ActivationIdentity | None = None,
) -> ActivationDecision:
    return ActivationDecision(
        status=status,
        reason_code=reason_code,
        facts=facts,
        identity=identity,
    )
