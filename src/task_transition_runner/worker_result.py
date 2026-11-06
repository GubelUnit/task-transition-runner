"""Public worker-result trust-boundary contract.

The loader accepts one YAML document of caller-supplied mechanical facts
plus an untrusted worker report. It validates schema, identities, exact
relative paths, and caller-supplied digest context. It does not generate
those inputs, judge worker prose, or create a decision or promotion.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


_SCHEMA_VERSION = "1.0"
_RESULT_TYPE = "worker_result"
_RESULT_SHA = re.compile(r"^[0-9a-f]{40}$")
_OUTPUT_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GLOB_MARKERS = ("*", "?", "[", "]")
_TOP_LEVEL_REQUIRED = frozenset(
    {
        "schema_version",
        "result_type",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "task_branch",
        "result_sha",
        "changed_paths",
        "validation_results",
        "worker_report",
    }
)
_WORKER_REPORT_REQUIRED = frozenset({"reported_status", "summary"})
_VALIDATION_ITEM_REQUIRED = frozenset(
    {
        "command_id",
        "exit_code",
        "output_ref",
        "output_sha256",
    }
)


class WorkerResultError(Exception):
    """Fail-closed violation of the public worker-result contract."""

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


@dataclass(frozen=True)
class WorkerResultIdentity:
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    result_sha: str


@dataclass(frozen=True)
class ValidationResult:
    command_id: str
    exit_code: int
    output_ref: str
    output_sha256: str


@dataclass(frozen=True)
class WorkerReport:
    reported_status: str
    summary: str


@dataclass(frozen=True)
class WorkerResult:
    schema_version: str
    result_type: str
    task_id: str
    origin_run_id: str
    promotion_attempt_id: str
    task_branch: str
    result_sha: str
    changed_paths: tuple[str, ...]
    validation_results: tuple[ValidationResult, ...]
    worker_report: WorkerReport


def load_worker_result(
    source_path: Path,
    *,
    expected_identity: WorkerResultIdentity,
    expected_validation_digests: Mapping[str, str],
) -> WorkerResult:
    document = _parse_mapping_document(_read_source_document(source_path))
    _reject_unknown_and_missing(document, _TOP_LEVEL_REQUIRED)

    schema_version = _require_str(document["schema_version"])
    if schema_version != _SCHEMA_VERSION:
        raise WorkerResultError("RESULT_VALUE_CONFLICT")
    result_type = _require_str(document["result_type"])
    if result_type != _RESULT_TYPE:
        raise WorkerResultError("RESULT_VALUE_CONFLICT")

    task_id = _require_non_blank_str(document["task_id"])
    origin_run_id = _require_non_blank_str(document["origin_run_id"])
    promotion_attempt_id = _require_non_blank_str(document["promotion_attempt_id"])
    task_branch = _require_non_blank_str(document["task_branch"])
    result_sha = _require_result_sha(document["result_sha"])
    changed_paths = _load_changed_paths(document["changed_paths"])
    validation_results = _load_validation_results(document["validation_results"])
    worker_report = _load_worker_report(document["worker_report"])

    observed_identity = WorkerResultIdentity(
        task_id=task_id,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
        result_sha=result_sha,
    )
    if observed_identity != expected_identity:
        raise WorkerResultError("RESULT_IDENTITY_MISMATCH")

    observed_digests = {
        item.command_id: item.output_sha256 for item in validation_results
    }
    if dict(expected_validation_digests) != observed_digests:
        raise WorkerResultError("RESULT_DIGEST_MISMATCH")

    return WorkerResult(
        schema_version=schema_version,
        result_type=result_type,
        task_id=task_id,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
        task_branch=task_branch,
        result_sha=result_sha,
        changed_paths=changed_paths,
        validation_results=validation_results,
        worker_report=worker_report,
    )


def _read_source_document(source_path: Path) -> str:
    resolved = Path(source_path).resolve()
    if not resolved.is_file():
        raise WorkerResultError("RESULT_SOURCE_MISSING")
    try:
        source_bytes = resolved.read_bytes()
    except PermissionError:
        raise WorkerResultError("RESULT_SOURCE_UNREADABLE") from None
    return source_bytes.decode("utf-8")


def _parse_mapping_document(document_text: str) -> dict[Any, Any]:
    try:
        loaded = yaml.load(document_text, Loader=_StrictLoader)
    except _DuplicateKeyError:
        raise WorkerResultError("RESULT_VALUE_CONFLICT") from None
    except yaml.YAMLError:
        raise WorkerResultError("RESULT_FIELD_TYPE") from None
    if not isinstance(loaded, dict):
        raise WorkerResultError("RESULT_FIELD_TYPE")
    return loaded


def _load_changed_paths(paths_raw: object) -> tuple[str, ...]:
    if not isinstance(paths_raw, list):
        raise WorkerResultError("RESULT_FIELD_TYPE")
    paths: list[str] = []
    seen: set[str] = set()
    for entry in paths_raw:
        relative = _validated_relative_path(entry)
        if relative in seen:
            raise WorkerResultError("RESULT_VALUE_CONFLICT")
        seen.add(relative)
        paths.append(relative)
    return tuple(paths)


def _load_validation_results(raw: object) -> tuple[ValidationResult, ...]:
    if not isinstance(raw, list):
        raise WorkerResultError("RESULT_FIELD_TYPE")
    items: list[ValidationResult] = []
    seen_ids: set[str] = set()
    for entry in raw:
        item = _load_validation_result(entry)
        if item.command_id in seen_ids:
            raise WorkerResultError("RESULT_VALUE_CONFLICT")
        seen_ids.add(item.command_id)
        items.append(item)
    return tuple(items)


def _load_validation_result(entry: object) -> ValidationResult:
    item = _require_mapping(entry)
    _reject_unknown_and_missing(item, _VALIDATION_ITEM_REQUIRED)
    return ValidationResult(
        command_id=_require_non_blank_str(item["command_id"]),
        exit_code=_require_int(item["exit_code"]),
        output_ref=_validated_relative_path(item["output_ref"]),
        output_sha256=_require_output_sha256(item["output_sha256"]),
    )


def _load_worker_report(raw: object) -> WorkerReport:
    report = _require_mapping(raw)
    _reject_unknown_and_missing(report, _WORKER_REPORT_REQUIRED)
    return WorkerReport(
        reported_status=_require_non_blank_str(report["reported_status"]),
        summary=_require_non_blank_str(report["summary"]),
    )


def _validated_relative_path(raw: object) -> str:
    if not isinstance(raw, str):
        raise WorkerResultError("RESULT_FIELD_TYPE")
    if (
        raw == ""
        or raw in {".", "./"}
        or raw.endswith("/")
        or any(marker in raw for marker in _GLOB_MARKERS)
        or Path(raw).is_absolute()
    ):
        raise WorkerResultError("RESULT_VALUE_CONFLICT")
    if _has_relative_normalization_drift(raw):
        raise WorkerResultError("RESULT_PATH_TRAVERSAL")
    return raw


def _has_relative_normalization_drift(raw: str) -> bool:
    declared = Path(raw)
    return declared.as_posix() != raw or any(
        part in {".", ".."} for part in declared.parts
    )


def _reject_unknown_and_missing(
    mapping: Mapping[Any, Any],
    required: frozenset[str],
) -> None:
    if set(mapping) - required:
        raise WorkerResultError("RESULT_FIELD_UNKNOWN")
    if required - set(mapping):
        raise WorkerResultError("RESULT_FIELD_MISSING")


def _require_mapping(value: object) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise WorkerResultError("RESULT_FIELD_TYPE")
    return value


def _require_str(value: object) -> str:
    if not isinstance(value, str):
        raise WorkerResultError("RESULT_FIELD_TYPE")
    return value


def _require_non_blank_str(value: object) -> str:
    text = _require_str(value)
    if text.strip() == "":
        raise WorkerResultError("RESULT_VALUE_CONFLICT")
    return text


def _require_int(value: object) -> int:
    if type(value) is not int:
        raise WorkerResultError("RESULT_FIELD_TYPE")
    return value


def _require_result_sha(value: object) -> str:
    text = _require_str(value)
    if _RESULT_SHA.fullmatch(text) is None:
        raise WorkerResultError("RESULT_VALUE_CONFLICT")
    return text


def _require_output_sha256(value: object) -> str:
    text = _require_str(value)
    if _OUTPUT_SHA256.fullmatch(text) is None:
        raise WorkerResultError("RESULT_VALUE_CONFLICT")
    return text
