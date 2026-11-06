"""Public decision-contract trust boundary.

DecisionRequest is deterministic handover data. It is not a semantic
decision and not promotion authority.

OrchestratorDecision and ValidatorDecision are the only semantic decision
artifacts. They bind the same four identity axes. Authority stays
asymmetric: only the orchestrator carries ``promotion_class``; only the
validator carries ``GO`` or ``NO_GO``.

``classify_*`` reports artifact states ``missing``, ``draft``, ``ready``,
and ``invalid``. Those states are not admission or promotion.

Artifact-backed evidence is checked against an explicit artifact root.
``git_result.ref`` binds to ``result_sha`` and is not a Git proof.

Closed schema rejects unknown, missing, typed, valued, and duplicate-key
edges. Separate files do not authenticate producers. Rationale is opaque
and unvalued. This contract does not perform promotion.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import yaml


_SCHEMA_VERSION = "1.0"
_REQUEST_TYPE = "decision_request"
_ORCHESTRATOR_TYPE = "orchestrator"
_VALIDATOR_TYPE = "validator"
_ORCHESTRATOR_PATH = "orchestrator_decision.yaml"
_VALIDATOR_PATH = "validator_decision.yaml"
_PROMOTION_CLASSES = frozenset(
    {"GREEN_PROMOTION", "RED_TDD_PROMOTION", "BLOCKED"}
)
_READY_EVIDENCE_CLASSES = frozenset({"GREEN_PROMOTION", "RED_TDD_PROMOTION"})
_VERDICTS = frozenset({"GO", "NO_GO"})
_RESULT_SHA = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GLOB_MARKERS = ("*", "?", "[", "]")
_REQUEST_REQUIRED = frozenset(
    {
        "schema_version",
        "request_type",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
        "required_decisions",
        "evidence",
    }
)
_ORCHESTRATOR_REQUIRED = frozenset(
    {
        "schema_version",
        "decision_type",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
        "ready",
        "promotion_class",
        "evidence",
    }
)
_VALIDATOR_REQUIRED = frozenset(
    {
        "schema_version",
        "decision_type",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
        "ready",
        "verdict",
        "evidence",
    }
)
_OPTIONAL_RATIONALE = frozenset({"rationale"})
_REQUIRED_DECISION_FIELDS = frozenset({"role", "path"})
_VALIDATION_LOG_FIELDS = frozenset({"kind", "ref", "sha256"})
_GIT_RESULT_FIELDS = frozenset({"kind", "ref"})
_REQUIRED_DECISION_PAIRS = frozenset(
    {
        (_ORCHESTRATOR_TYPE, _ORCHESTRATOR_PATH),
        (_VALIDATOR_TYPE, _VALIDATOR_PATH),
    }
)

Kind = Literal["request", "orchestrator", "validator"]


class DecisionContractError(Exception):
    """Fail-closed violation of the public decision contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class _DuplicateKeyError(yaml.YAMLError):
    """A mapping node declared the same key more than once."""


class _UnhashableKeyError(yaml.YAMLError):
    """A mapping node declared a key that cannot be used in a closed mapping."""


class _StrictLoader(yaml.SafeLoader):
    """Safe loader that rejects duplicate mapping keys at every nesting level."""


def _construct_mapping_rejecting_duplicates(
    loader: yaml.Loader,
    node: yaml.nodes.MappingNode,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        try:
            hash(key)
        except TypeError:
            raise _UnhashableKeyError("unhashable mapping key") from None
        if key in mapping:
            raise _DuplicateKeyError("duplicate mapping key")
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_rejecting_duplicates,
)


class DecisionArtifactState(Enum):
    MISSING = "missing"
    DRAFT = "draft"
    READY = "ready"
    INVALID = "invalid"


@dataclass(frozen=True)
class DecisionIdentity:
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str


@dataclass(frozen=True)
class RequiredDecision:
    role: str
    path: str


@dataclass(frozen=True)
class EvidenceReference:
    kind: str
    ref: str
    sha256: str | None = None


@dataclass(frozen=True)
class DecisionRequest:
    schema_version: str
    request_type: str
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str
    required_decisions: tuple[RequiredDecision, ...]
    evidence: tuple[EvidenceReference, ...]


@dataclass(frozen=True)
class OrchestratorDecision:
    schema_version: str
    decision_type: str
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str
    ready: bool
    promotion_class: str
    evidence: tuple[EvidenceReference, ...]
    rationale: str | None = None


@dataclass(frozen=True)
class ValidatorDecision:
    schema_version: str
    decision_type: str
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str
    ready: bool
    verdict: str
    evidence: tuple[EvidenceReference, ...]
    rationale: str | None = None


def load_decision_request(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> DecisionRequest:
    document, identity, evidence = _load_shared(
        source_path,
        expected_identity=expected_identity,
        artifact_root=artifact_root,
        kind="request",
    )
    return DecisionRequest(
        schema_version=_SCHEMA_VERSION,
        request_type=_REQUEST_TYPE,
        task_id=identity.task_id,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
        result_sha=identity.result_sha,
        required_decisions=_load_required_decisions(document["required_decisions"]),
        evidence=evidence,
    )


def load_orchestrator_decision(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> OrchestratorDecision:
    document, identity, evidence = _load_shared(
        source_path,
        expected_identity=expected_identity,
        artifact_root=artifact_root,
        kind="orchestrator",
    )
    ready = _require_bool(document["ready"])
    promotion_class = _require_str(document["promotion_class"])
    if promotion_class not in _PROMOTION_CLASSES:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    if ready and promotion_class in _READY_EVIDENCE_CLASSES and len(evidence) == 0:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    return OrchestratorDecision(
        schema_version=_SCHEMA_VERSION,
        decision_type=_ORCHESTRATOR_TYPE,
        task_id=identity.task_id,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
        result_sha=identity.result_sha,
        ready=ready,
        promotion_class=promotion_class,
        evidence=evidence,
        rationale=_optional_rationale(document),
    )


def load_validator_decision(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> ValidatorDecision:
    document, identity, evidence = _load_shared(
        source_path,
        expected_identity=expected_identity,
        artifact_root=artifact_root,
        kind="validator",
    )
    ready = _require_bool(document["ready"])
    verdict = _require_str(document["verdict"])
    if verdict not in _VERDICTS:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    if ready and verdict == "GO" and len(evidence) == 0:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    return ValidatorDecision(
        schema_version=_SCHEMA_VERSION,
        decision_type=_VALIDATOR_TYPE,
        task_id=identity.task_id,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
        result_sha=identity.result_sha,
        ready=ready,
        verdict=verdict,
        evidence=evidence,
        rationale=_optional_rationale(document),
    )


def classify_decision_request(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> DecisionArtifactState:
    return _classify(
        load_decision_request,
        source_path,
        expected_identity=expected_identity,
        artifact_root=artifact_root,
    )


def classify_orchestrator_decision(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> DecisionArtifactState:
    return _classify(
        load_orchestrator_decision,
        source_path,
        expected_identity=expected_identity,
        artifact_root=artifact_root,
    )


def classify_validator_decision(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> DecisionArtifactState:
    return _classify(
        load_validator_decision,
        source_path,
        expected_identity=expected_identity,
        artifact_root=artifact_root,
    )


def _classify(
    loader: Callable[..., Any],
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
) -> DecisionArtifactState:
    try:
        loaded = loader(
            source_path,
            expected_identity=expected_identity,
            artifact_root=artifact_root,
        )
    except DecisionContractError as error:
        if error.reason_code == "DECISION_SOURCE_MISSING":
            return DecisionArtifactState.MISSING
        return DecisionArtifactState.INVALID
    if getattr(loaded, "ready", True) is False:
        return DecisionArtifactState.DRAFT
    return DecisionArtifactState.READY


def _load_shared(
    source_path: Path,
    *,
    expected_identity: DecisionIdentity,
    artifact_root: Path,
    kind: Kind,
) -> tuple[dict[Any, Any], DecisionIdentity, tuple[EvidenceReference, ...]]:
    root = _resolved_artifact_root(artifact_root)
    document = _parse_mapping_document(_read_source_document(source_path, root))
    _reject_schema_version(document)
    _reject_foreign_document_kind(document, kind)
    _reject_unknown_and_missing(
        document,
        _required_fields(kind),
        _allowed_fields(kind),
    )
    identity = DecisionIdentity(
        task_id=_require_non_blank_str(document["task_id"]),
        origin_run_id=_require_non_blank_str(document["origin_run_id"]),
        promotion_attempt_id=_require_non_blank_str(document["promotion_attempt_id"]),
        result_sha=_require_result_sha(document["result_sha"]),
    )
    if identity != expected_identity:
        raise DecisionContractError("DECISION_IDENTITY_MISMATCH")
    if kind == "request":
        request_type = _require_str(document["request_type"])
        if request_type != _REQUEST_TYPE:
            raise DecisionContractError("DECISION_VALUE_CONFLICT")
    evidence = _load_evidence(
        document["evidence"],
        result_sha=identity.result_sha,
        artifact_root=root,
    )
    return document, identity, evidence


def _required_fields(kind: Kind) -> frozenset[str]:
    if kind == "request":
        return _REQUEST_REQUIRED
    if kind == "orchestrator":
        return _ORCHESTRATOR_REQUIRED
    return _VALIDATOR_REQUIRED


def _allowed_fields(kind: Kind) -> frozenset[str]:
    required = _required_fields(kind)
    if kind == "request":
        return required
    return required | _OPTIONAL_RATIONALE


def _resolved_artifact_root(artifact_root: Path) -> Path:
    root = Path(artifact_root).resolve()
    if not root.is_dir():
        raise DecisionContractError("DECISION_PATH_ESCAPE")
    return root


def _read_source_document(source_path: Path, root: Path) -> str:
    resolved = Path(source_path).resolve()
    if not resolved.is_relative_to(root):
        raise DecisionContractError("DECISION_PATH_ESCAPE")
    if not resolved.is_file():
        raise DecisionContractError("DECISION_SOURCE_MISSING")
    try:
        return resolved.read_bytes().decode("utf-8")
    except OSError:
        raise DecisionContractError("DECISION_SOURCE_UNREADABLE") from None
    except UnicodeDecodeError:
        raise DecisionContractError("DECISION_FIELD_TYPE") from None


def _parse_mapping_document(document_text: str) -> dict[Any, Any]:
    try:
        loaded = yaml.load(document_text, Loader=_StrictLoader)
    except _DuplicateKeyError:
        raise DecisionContractError("DECISION_VALUE_CONFLICT") from None
    except yaml.YAMLError:
        raise DecisionContractError("DECISION_FIELD_TYPE") from None
    if not isinstance(loaded, dict):
        raise DecisionContractError("DECISION_FIELD_TYPE")
    return loaded


def _reject_schema_version(document: Mapping[Any, Any]) -> None:
    if "schema_version" not in document:
        return
    version = document["schema_version"]
    if type(version) is not str:
        raise DecisionContractError("DECISION_FIELD_TYPE")
    if version != _SCHEMA_VERSION:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")


def _reject_foreign_document_kind(document: Mapping[Any, Any], kind: Kind) -> None:
    if kind == "request":
        if "decision_type" in document:
            raise DecisionContractError("DECISION_VALUE_CONFLICT")
        return
    if "request_type" in document:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    observed = document.get("decision_type")
    if observed is not None and observed != kind:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")


def _load_required_decisions(raw: object) -> tuple[RequiredDecision, ...]:
    if not isinstance(raw, list):
        raise DecisionContractError("DECISION_FIELD_TYPE")
    items: list[RequiredDecision] = []
    for entry in raw:
        item = _require_mapping(entry)
        _reject_unknown_and_missing(
            item,
            _REQUIRED_DECISION_FIELDS,
            _REQUIRED_DECISION_FIELDS,
        )
        items.append(
            RequiredDecision(
                role=_require_str(item["role"]),
                path=_validated_relative_path(item["path"]),
            )
        )
    observed = {(item.role, item.path) for item in items}
    if len(items) != 2 or observed != _REQUIRED_DECISION_PAIRS:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    return tuple(items)


def _load_evidence(
    raw: object,
    *,
    result_sha: str,
    artifact_root: Path,
) -> tuple[EvidenceReference, ...]:
    if not isinstance(raw, list):
        raise DecisionContractError("DECISION_FIELD_TYPE")
    items: list[EvidenceReference] = []
    for entry in raw:
        items.append(
            _load_evidence_item(
                entry,
                result_sha=result_sha,
                artifact_root=artifact_root,
            )
        )
    return tuple(items)


def _load_evidence_item(
    entry: object,
    *,
    result_sha: str,
    artifact_root: Path,
) -> EvidenceReference:
    item = _require_mapping(entry)
    kind = item.get("kind")
    if kind == "validation_log":
        _reject_unknown_and_missing(item, _VALIDATION_LOG_FIELDS, _VALIDATION_LOG_FIELDS)
        ref = _validated_relative_path(item["ref"])
        digest = _require_sha256(item["sha256"])
        payload = _read_rooted_artifact(artifact_root / ref, artifact_root)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise DecisionContractError("DECISION_DIGEST_MISMATCH")
        return EvidenceReference(kind=kind, ref=ref, sha256=digest)
    if kind == "git_result":
        _reject_unknown_and_missing(item, _GIT_RESULT_FIELDS, _GIT_RESULT_FIELDS)
        ref = _require_str(item["ref"])
        if ref != result_sha:
            raise DecisionContractError("DECISION_VALUE_CONFLICT")
        return EvidenceReference(kind=kind, ref=ref)
    raise DecisionContractError("DECISION_VALUE_CONFLICT")


def _read_rooted_artifact(candidate: Path, root: Path) -> bytes:
    if not candidate.is_symlink() and not candidate.is_file():
        raise DecisionContractError("DECISION_ARTIFACT_MISSING")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        raise DecisionContractError("DECISION_PATH_ESCAPE") from None
    if not resolved.is_relative_to(root):
        raise DecisionContractError("DECISION_PATH_ESCAPE")
    try:
        return resolved.read_bytes()
    except OSError:
        raise DecisionContractError("DECISION_SOURCE_UNREADABLE") from None


def _validated_relative_path(raw: object) -> str:
    text = _require_str(raw)
    if (
        text == ""
        or text in {".", "./"}
        or text.endswith("/")
        or any(marker in text for marker in _GLOB_MARKERS)
        or Path(text).is_absolute()
    ):
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    if _has_relative_normalization_drift(text):
        raise DecisionContractError("DECISION_PATH_TRAVERSAL")
    return text


def _has_relative_normalization_drift(raw: str) -> bool:
    declared = Path(raw)
    return declared.as_posix() != raw or any(
        part in {".", ".."} for part in declared.parts
    )


def _optional_rationale(document: Mapping[Any, Any]) -> str | None:
    if "rationale" not in document:
        return None
    return _require_str(document["rationale"])


def _reject_unknown_and_missing(
    mapping: Mapping[Any, Any],
    required: frozenset[str],
    allowed: frozenset[str],
) -> None:
    if set(mapping) - allowed:
        raise DecisionContractError("DECISION_FIELD_UNKNOWN")
    if required - set(mapping):
        raise DecisionContractError("DECISION_FIELD_MISSING")


def _require_mapping(value: object) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise DecisionContractError("DECISION_FIELD_TYPE")
    return value


def _require_str(value: object) -> str:
    if not isinstance(value, str):
        raise DecisionContractError("DECISION_FIELD_TYPE")
    return value


def _require_non_blank_str(value: object) -> str:
    text = _require_str(value)
    if text.strip() == "":
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    return text


def _require_bool(value: object) -> bool:
    if type(value) is not bool:
        raise DecisionContractError("DECISION_FIELD_TYPE")
    return value


def _require_result_sha(value: object) -> str:
    text = _require_str(value)
    if _RESULT_SHA.fullmatch(text) is None:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    return text


def _require_sha256(value: object) -> str:
    text = _require_str(value)
    if _SHA256.fullmatch(text) is None:
        raise DecisionContractError("DECISION_VALUE_CONFLICT")
    return text
