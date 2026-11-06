"""Synthetic public worker-result trust-boundary contract.

The public surface is intentionally small and explicit:

- ``load_worker_result`` validates one YAML document of caller-supplied
  mechanical facts plus an untrusted worker report.
- ``WorkerResult`` is the loaded canonical value object.
- ``WorkerResultError.reason_code`` is the stable ``RESULT_*`` failure token.
- Mechanical fields are branch, result SHA, changed paths, exit codes, and
  log digests. They stay distinct from ``worker_report``.
- ``worker_report`` admits exactly ``reported_status`` and ``summary``.
  Worker assessment is the semantic reading of those two values, not a
  third schema field.
- Identity and digest checks compare the document against caller-supplied
  expected task, origin, attempt, result, and validation-digest context.
  The contract does not generate those inputs or judge their meaning.
- Worker success does not create a decision, pending promotion, or
  canonical promotion. There is no universal ``run_id``.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import inspect
import os
import stat
from pathlib import Path
from typing import Any, Mapping

import pytest
import yaml

from task_transition_runner.worker_result import (
    ValidationResult,
    WorkerReport,
    WorkerResult,
    WorkerResultError,
    WorkerResultIdentity,
    load_worker_result,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_RESULT_TYPE = "worker_result"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_TASK_BRANCH = "runner/task/synthetic-task-001/attempt-001"
_REFERENCE_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_REFERENCE_CHANGED_PATHS = ("src/example.py", "tests/test_example.py")
_REFERENCE_COMMAND_ID = "tests"
_REFERENCE_OUTPUT_REF = "runs/synthetic-origin/validation/tests.log"
_VALIDATION_LOG_BODY = b"synthetic validation log\n"
_REFERENCE_OUTPUT_SHA256 = hashlib.sha256(_VALIDATION_LOG_BODY).hexdigest()
_REFERENCE_REPORTED_STATUS = "success"
_REFERENCE_SUMMARY = "synthetic worker summary"
_FOREIGN_RESULT_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_FOREIGN_OUTPUT_SHA256 = hashlib.sha256(b"foreign validation log\n").hexdigest()

_TOP_LEVEL_REQUIRED = (
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
)
_WORKER_REPORT_REQUIRED = ("reported_status", "summary")
_VALIDATION_ITEM_REQUIRED = (
    "command_id",
    "exit_code",
    "output_ref",
    "output_sha256",
)


def _reference_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "result_type": _REFERENCE_RESULT_TYPE,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "task_branch": _REFERENCE_TASK_BRANCH,
        "result_sha": _REFERENCE_RESULT_SHA,
        "changed_paths": list(_REFERENCE_CHANGED_PATHS),
        "validation_results": [
            {
                "command_id": _REFERENCE_COMMAND_ID,
                "exit_code": 0,
                "output_ref": _REFERENCE_OUTPUT_REF,
                "output_sha256": _REFERENCE_OUTPUT_SHA256,
            }
        ],
        "worker_report": {
            "reported_status": _REFERENCE_REPORTED_STATUS,
            "summary": _REFERENCE_SUMMARY,
        },
    }


def _reference_identity() -> WorkerResultIdentity:
    return WorkerResultIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=_REFERENCE_RESULT_SHA,
    )


def _reference_digests() -> dict[str, str]:
    return {_REFERENCE_COMMAND_ID: _REFERENCE_OUTPUT_SHA256}


def _write_document(
    root: Path,
    payload: dict[str, Any] | None = None,
    *,
    filename: str = "worker_result.yaml",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    document = _reference_payload() if payload is None else copy.deepcopy(payload)
    source_path = root / filename
    source_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    return source_path


def _overwrite_source(source_path: Path, text: str) -> Path:
    source_path.write_text(text, encoding="utf-8")
    return source_path


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip(
            "process retains read access after clearing read mode bits"
        )


def _drop_field(payload: dict[str, Any], dotted: str) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    keys = dotted.split(".")
    cursor: Any = clone
    for key in keys[:-1]:
        cursor = cursor[key]
    del cursor[keys[-1]]
    return clone


def _set_field(payload: dict[str, Any], dotted: str, value: Any) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    keys = dotted.split(".")
    cursor: Any = clone
    for key in keys[:-1]:
        cursor = cursor[key]
    cursor[keys[-1]] = value
    return clone


def _load(
    source_path: Path,
    *,
    expected_identity: WorkerResultIdentity | None = None,
    expected_validation_digests: Mapping[str, str] | None = None,
) -> WorkerResult:
    identity = _reference_identity() if expected_identity is None else expected_identity
    digests = (
        _reference_digests()
        if expected_validation_digests is None
        else expected_validation_digests
    )
    return load_worker_result(
        source_path,
        expected_identity=identity,
        expected_validation_digests=digests,
    )


def _expect_reason(
    source_path: Path,
    reason_code: str,
    *,
    expected_identity: WorkerResultIdentity | None = None,
    expected_validation_digests: Mapping[str, str] | None = None,
) -> WorkerResultError:
    with pytest.raises(WorkerResultError, match=reason_code) as exc_info:
        _load(
            source_path,
            expected_identity=expected_identity,
            expected_validation_digests=expected_validation_digests,
        )
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _composed_token(*segments: str) -> str:
    return "".join(segments)


def _composed_absolute(*segments: str) -> str:
    return "/" + "/".join(segments)


def _authority_names() -> tuple[str, ...]:
    return (
        "decision",
        "decision_request",
        "pending_promotion",
        "promotion_class",
        "promotion",
        "ready",
        "verdict",
        "run_id",
        "instruction_bytes",
        "spec_bytes",
        "prompt_bytes",
    )


def test_reference_document_exposes_mechanical_fields_apart_from_the_report(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)

    assert isinstance(loaded, WorkerResult)
    assert loaded.schema_version == _REFERENCE_SCHEMA_VERSION
    assert loaded.result_type == _REFERENCE_RESULT_TYPE
    assert loaded.task_id == _REFERENCE_TASK_ID
    assert loaded.origin_run_id == _REFERENCE_ORIGIN_RUN_ID
    assert loaded.promotion_attempt_id == _REFERENCE_PROMOTION_ATTEMPT_ID
    assert loaded.task_branch == _REFERENCE_TASK_BRANCH
    assert loaded.result_sha == _REFERENCE_RESULT_SHA
    assert loaded.changed_paths == _REFERENCE_CHANGED_PATHS
    assert len(loaded.validation_results) == 1
    item = loaded.validation_results[0]
    assert isinstance(item, ValidationResult)
    assert item.command_id == _REFERENCE_COMMAND_ID
    assert item.exit_code == 0
    assert item.output_ref == _REFERENCE_OUTPUT_REF
    assert item.output_sha256 == _REFERENCE_OUTPUT_SHA256
    assert isinstance(loaded.worker_report, WorkerReport)
    assert loaded.worker_report.reported_status == _REFERENCE_REPORTED_STATUS
    assert loaded.worker_report.summary == _REFERENCE_SUMMARY


def test_changed_paths_are_duplicate_free_and_exact_relative(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)

    assert loaded.changed_paths == tuple(_REFERENCE_CHANGED_PATHS)
    assert len(loaded.changed_paths) == len(set(loaded.changed_paths))
    for relative in loaded.changed_paths:
        declared = Path(relative)
        assert not declared.is_absolute()
        assert ".." not in declared.parts
        assert relative == declared.as_posix()
        assert "*" not in relative


def test_worker_report_exposes_exactly_the_two_bound_keys(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)

    field_names = tuple(
        field.name for field in dataclasses.fields(loaded.worker_report)
    )
    assert field_names == _WORKER_REPORT_REQUIRED
    assert set(dataclasses.asdict(loaded.worker_report)) == set(_WORKER_REPORT_REQUIRED)


def test_worker_assessment_is_not_a_third_schema_field(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)

    assert not hasattr(loaded.worker_report, "assessment")
    assert not hasattr(loaded, "assessment")
    payload = _reference_payload()
    payload["worker_report"]["assessment"] = "worker believes this is done"
    _expect_reason(_write_document(tmp_path / "assessment", payload), "RESULT_FIELD_UNKNOWN")


def test_identical_reload_is_stable(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    first = _load(source_path)
    second = _load(source_path)

    assert first == second
    assert dataclasses.asdict(first) == dataclasses.asdict(second)


def test_worker_result_and_report_are_immutable(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)

    with pytest.raises(dataclasses.FrozenInstanceError):
        loaded.task_id = "mutated-task"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        loaded.worker_report.reported_status = "failure"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        loaded.validation_results[0].exit_code = 1  # type: ignore[misc]


def test_public_loader_has_no_universal_run_id() -> None:
    parameters = inspect.signature(load_worker_result).parameters
    identity_fields = tuple(
        field.name for field in dataclasses.fields(WorkerResultIdentity)
    )
    result_fields = tuple(field.name for field in dataclasses.fields(WorkerResult))

    assert tuple(parameters)[0] == "source_path"
    assert "expected_identity" in parameters
    assert "expected_validation_digests" in parameters
    assert "run_id" not in parameters
    assert "queue" not in parameters
    assert "planner" not in parameters
    assert "sandbox" not in parameters
    assert "decision" not in parameters
    assert "pending_promotion" not in parameters
    assert identity_fields == (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
    )
    assert "run_id" not in identity_fields
    assert "drain_run_id" not in identity_fields
    assert "tock_sha" not in identity_fields
    assert "run_id" not in result_fields
    assert "drain_run_id" not in result_fields
    assert "tock_sha" not in result_fields


def test_loader_does_not_write_decision_or_promotion_artifacts(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    before = {path for path in tmp_path.rglob("*") if path.is_file()}

    loaded = _load(source_path)

    after = {path for path in tmp_path.rglob("*") if path.is_file()}
    assert after == before
    assert loaded.worker_report.reported_status == "success"
    for name in ("decision_request.yaml", "pending_promotion.yaml"):
        assert list(tmp_path.rglob(name)) == []
    for forbidden in _authority_names():
        assert not hasattr(loaded, forbidden)
        assert not hasattr(loaded.worker_report, forbidden)
        assert not hasattr(load_worker_result, forbidden)


def test_reported_success_does_not_authorize_decision_or_promotion(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)
    module = inspect.getmodule(load_worker_result)
    assert module is not None
    exported = set(name for name in dir(module) if not name.startswith("_"))

    assert loaded.worker_report.reported_status == "success"
    assert loaded.validation_results[0].exit_code == 0
    for name in (
        "create_decision",
        "load_decision",
        "write_decision_request",
        "create_pending_promotion",
        "admit_promotion",
        "promote",
    ):
        assert name not in exported
    assert "Decision" not in exported
    assert "PendingPromotion" not in exported


def test_reported_success_with_nonzero_exit_remains_untrusted_prose(
    tmp_path: Path,
) -> None:
    payload = _set_field(_reference_payload(), "validation_results", [
        {
            "command_id": _REFERENCE_COMMAND_ID,
            "exit_code": 1,
            "output_ref": _REFERENCE_OUTPUT_REF,
            "output_sha256": _REFERENCE_OUTPUT_SHA256,
        }
    ])
    source_path = _write_document(tmp_path, payload)
    loaded = _load(source_path)

    assert loaded.validation_results[0].exit_code == 1
    assert loaded.worker_report.reported_status == "success"
    assert not hasattr(loaded, "pending_promotion")
    assert not hasattr(loaded, "decision")
    assert list(tmp_path.rglob("pending_promotion.yaml")) == []


def test_result_object_carries_no_instruction_bytes(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)
    dumped = yaml.safe_dump(dataclasses.asdict(loaded), sort_keys=True)

    assert not hasattr(loaded, "instruction_bytes")
    assert not hasattr(loaded, "spec_bytes")
    assert not hasattr(loaded, "prompt_bytes")
    assert not hasattr(loaded.worker_report, "instruction_bytes")
    assert _VALIDATION_LOG_BODY.decode("utf-8") not in dumped
    assert "synthetic spec body" not in dumped


@pytest.mark.parametrize(
    "dotted_field",
    _TOP_LEVEL_REQUIRED
    + tuple(f"worker_report.{name}" for name in _WORKER_REPORT_REQUIRED)
    + tuple(f"validation_results.0.{name}" for name in _VALIDATION_ITEM_REQUIRED),
)
def test_missing_required_field_is_blocked(tmp_path: Path, dotted_field: str) -> None:
    payload = _reference_payload()
    if dotted_field.startswith("validation_results.0."):
        key = dotted_field.rsplit(".", 1)[1]
        del payload["validation_results"][0][key]
    else:
        payload = _drop_field(payload, dotted_field)
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_MISSING")


def test_empty_mapping_document_is_blocked(tmp_path: Path) -> None:
    source_path = tmp_path / "worker_result.yaml"
    source_path.write_text("{}\n", encoding="utf-8")
    _expect_reason(source_path, "RESULT_FIELD_MISSING")


@pytest.mark.parametrize(
    "field",
    (
        "run_id",
        "drain_run_id",
        "tock_sha",
        "decision",
        "pending_promotion",
        "promotion_class",
        "learning",
        "handover",
        "persona",
        "ack",
    ),
)
def test_unknown_top_level_field_is_blocked(tmp_path: Path, field: str) -> None:
    payload = _reference_payload()
    payload[field] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


@pytest.mark.parametrize(
    "field",
    ("assessment", "confidence", "verdict", "rationale", "ready"),
)
def test_unknown_worker_report_field_is_blocked(tmp_path: Path, field: str) -> None:
    payload = _reference_payload()
    payload["worker_report"][field] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_unknown_validation_result_field_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["semantic_pass"] = True
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_generic_unknown_top_level_field_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["unexpected_field"] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_generic_unknown_worker_report_field_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["worker_report"]["unexpected_field"] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_generic_unknown_validation_result_field_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["unexpected_field"] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_composed_private_top_level_tokens_are_unknown_fields(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload[_composed_token("orp", "_", "handover")] = "unexpected"
    payload_two = _reference_payload()
    payload_two[_composed_token("chron", "icle")] = "unexpected"
    _expect_reason(_write_document(tmp_path / "one", payload), "RESULT_FIELD_UNKNOWN")
    _expect_reason(_write_document(tmp_path / "two", payload_two), "RESULT_FIELD_UNKNOWN")


def test_composed_private_worker_report_tokens_are_unknown_fields(
    tmp_path: Path,
) -> None:
    payload = _reference_payload()
    payload["worker_report"][_composed_token("orp", "_", "annex")] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_composed_role_token_in_unknown_field_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload[_composed_token("Hy", "dra")] = "unexpected"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_UNKNOWN")


def test_composed_private_sha_in_summary_stays_untrusted_prose(tmp_path: Path) -> None:
    token = _composed_token("16e8f70e31bc424e8bde", "0e285d13a6053d9504b6")
    payload = _set_field(_reference_payload(), "worker_report.summary", token)
    loaded = _load(_write_document(tmp_path, payload))

    assert loaded.worker_report.summary == token
    assert loaded.result_sha == _REFERENCE_RESULT_SHA
    assert not hasattr(loaded, "decision")
    assert not hasattr(loaded, "pending_promotion")


def test_malformed_result_sha_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "result_sha", "not-a-sha")
    expected = WorkerResultIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha="not-a-sha",
    )
    _expect_reason(
        _write_document(tmp_path, payload),
        "RESULT_VALUE_CONFLICT",
        expected_identity=expected,
    )


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("schema_version", 1),
        ("result_type", ["worker_result"]),
        ("task_id", 12),
        ("origin_run_id", {"id": "origin-run-001"}),
        ("promotion_attempt_id", False),
        ("task_branch", 3),
        ("result_sha", 40),
        ("changed_paths", "src/example.py"),
        ("changed_paths", [1]),
        ("validation_results", {}),
        ("worker_report", ["success"]),
        ("worker_report.reported_status", True),
        ("worker_report.reported_status", None),
        ("worker_report.summary", 0),
        ("worker_report.summary", None),
    ),
)
def test_wrong_field_type_is_blocked(
    tmp_path: Path,
    dotted_field: str,
    value: object,
) -> None:
    payload = _set_field(_reference_payload(), dotted_field, value)
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_TYPE")


def test_boolean_exit_code_is_not_accepted_as_an_integer(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["exit_code"] = True
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_TYPE")


def test_string_exit_code_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["exit_code"] = "0"
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_TYPE")


def test_validation_result_entry_must_be_a_mapping(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "validation_results", ["tests"])
    _expect_reason(_write_document(tmp_path, payload), "RESULT_FIELD_TYPE")


@pytest.mark.parametrize("version", ("2.0", "1.0.0"))
def test_wrong_schema_version_is_blocked(tmp_path: Path, version: str) -> None:
    payload = _set_field(_reference_payload(), "schema_version", version)
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_wrong_result_type_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "result_type", "decision")
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


@pytest.mark.parametrize(
    "dotted_field",
    (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "task_branch",
        "result_sha",
        "worker_report.reported_status",
        "worker_report.summary",
    ),
)
def test_blank_required_string_is_blocked(tmp_path: Path, dotted_field: str) -> None:
    payload = _set_field(_reference_payload(), dotted_field, "   ")
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_empty_changed_path_entry_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "changed_paths", [""])
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_duplicate_changed_paths_are_blocked(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "changed_paths",
        ["src/example.py", "src/example.py"],
    )
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_duplicate_validation_command_ids_are_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"].append(
        {
            "command_id": _REFERENCE_COMMAND_ID,
            "exit_code": 0,
            "output_ref": "runs/synthetic-origin/validation/tests-again.log",
            "output_sha256": _REFERENCE_OUTPUT_SHA256,
        }
    )
    _expect_reason(
        _write_document(tmp_path, payload),
        "RESULT_VALUE_CONFLICT",
        expected_validation_digests={
            _REFERENCE_COMMAND_ID: _REFERENCE_OUTPUT_SHA256,
        },
    )


@pytest.mark.parametrize(
    "relative",
    (
        "../outside.py",
        "src/foo/../example.py",
        "src/./example.py",
        "./src/example.py",
        "src//example.py",
        "src/example.py/..",
    ),
)
def test_changed_path_traversal_and_normalization_drift_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    payload = _set_field(_reference_payload(), "changed_paths", [relative])
    _expect_reason(_write_document(tmp_path, payload), "RESULT_PATH_TRAVERSAL")


@pytest.mark.parametrize(
    "relative",
    (
        ".",
        "./",
        "src/",
        "src/*",
        "src/*.py",
        "src/**",
        "src/**/*.py",
        "src/example?.py",
        "src/example[ab].py",
        "*",
    ),
)
def test_root_glob_and_directory_changed_paths_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    payload = _set_field(_reference_payload(), "changed_paths", [relative])
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_absolute_changed_path_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "changed_paths",
        [str((tmp_path / "src" / "example.py").resolve())],
    )
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_composed_home_class_absolute_changed_path_is_blocked(tmp_path: Path) -> None:
    absolute = _composed_absolute("home", "synthetic-operator", "escaped.py")
    payload = _set_field(_reference_payload(), "changed_paths", [absolute])
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_composed_users_class_absolute_changed_path_is_blocked(tmp_path: Path) -> None:
    absolute = _composed_absolute("Users", "synthetic-operator", "escaped.py")
    payload = _set_field(_reference_payload(), "changed_paths", [absolute])
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


@pytest.mark.parametrize(
    "relative",
    (
        "../outside.log",
        "runs/./synthetic-origin/validation/tests.log",
        "./runs/synthetic-origin/validation/tests.log",
        "runs//synthetic-origin/validation/tests.log",
    ),
)
def test_output_ref_traversal_and_normalization_drift_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["output_ref"] = relative
    _expect_reason(_write_document(tmp_path, payload), "RESULT_PATH_TRAVERSAL")


def test_absolute_output_ref_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["output_ref"] = str(
        (tmp_path / "tests.log").resolve()
    )
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_composed_home_class_absolute_output_ref_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["output_ref"] = _composed_absolute(
        "home",
        "synthetic-operator",
        "tests.log",
    )
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_output_digest_mismatch_against_caller_expectation_is_blocked(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    _expect_reason(
        source_path,
        "RESULT_DIGEST_MISMATCH",
        expected_validation_digests={_REFERENCE_COMMAND_ID: _FOREIGN_OUTPUT_SHA256},
    )


def test_missing_caller_digest_for_declared_command_is_blocked(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    _expect_reason(
        source_path,
        "RESULT_DIGEST_MISMATCH",
        expected_validation_digests={},
    )


def test_malformed_output_digest_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["output_sha256"] = "not-a-digest"
    _expect_reason(
        _write_document(tmp_path, payload),
        "RESULT_VALUE_CONFLICT",
        expected_validation_digests={_REFERENCE_COMMAND_ID: "not-a-digest"},
    )


def test_task_id_mismatch_against_expected_context_is_blocked(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    expected = WorkerResultIdentity(
        task_id="foreign-task-009",
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=_REFERENCE_RESULT_SHA,
    )
    _expect_reason(
        source_path,
        "RESULT_IDENTITY_MISMATCH",
        expected_identity=expected,
    )


def test_origin_run_id_mismatch_against_expected_context_is_blocked(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    expected = WorkerResultIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id="origin-run-999",
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=_REFERENCE_RESULT_SHA,
    )
    _expect_reason(
        source_path,
        "RESULT_IDENTITY_MISMATCH",
        expected_identity=expected,
    )


def test_promotion_attempt_id_mismatch_against_expected_context_is_blocked(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    expected = WorkerResultIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id="promotion-attempt-999",
        result_sha=_REFERENCE_RESULT_SHA,
    )
    _expect_reason(
        source_path,
        "RESULT_IDENTITY_MISMATCH",
        expected_identity=expected,
    )


def test_result_sha_mismatch_against_expected_context_is_blocked(
    tmp_path: Path,
) -> None:
    source_path = _write_document(tmp_path)
    expected = WorkerResultIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=_FOREIGN_RESULT_SHA,
    )
    _expect_reason(
        source_path,
        "RESULT_IDENTITY_MISMATCH",
        expected_identity=expected,
    )


def test_matching_identity_context_is_accepted(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path, expected_identity=_reference_identity())

    assert loaded.task_id == _REFERENCE_TASK_ID
    assert loaded.origin_run_id == _REFERENCE_ORIGIN_RUN_ID
    assert loaded.promotion_attempt_id == _REFERENCE_PROMOTION_ATTEMPT_ID
    assert loaded.result_sha == _REFERENCE_RESULT_SHA


def test_identities_stay_uncollapsed_on_the_loaded_result(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)
    values = {
        loaded.task_id,
        loaded.origin_run_id,
        loaded.promotion_attempt_id,
        loaded.result_sha,
    }

    assert len(values) == 4
    assert loaded.task_id != loaded.origin_run_id
    assert loaded.origin_run_id != loaded.promotion_attempt_id
    assert loaded.promotion_attempt_id != loaded.result_sha
    assert not hasattr(loaded, "run_id")


@pytest.mark.parametrize(
    "document",
    (
        "",
        "\n",
        "[]\n",
        "null\n",
        "- just a list\n",
        "not a mapping\n",
        ":\n  - broken\n",
        "schema_version: [unterminated\n",
    ),
)
def test_empty_non_mapping_and_syntax_errors_are_blocked(
    tmp_path: Path,
    document: str,
) -> None:
    source_path = tmp_path / "worker_result.yaml"
    source_path.write_text(document, encoding="utf-8")
    _expect_reason(source_path, "RESULT_FIELD_TYPE")


def test_duplicate_yaml_keys_are_blocked(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    text = source_path.read_text(encoding="utf-8")
    _overwrite_source(source_path, 'schema_version: "1.0"\n' + text)
    _expect_reason(source_path, "RESULT_VALUE_CONFLICT")


def test_duplicate_worker_report_key_is_blocked(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    text = source_path.read_text(encoding="utf-8")
    _overwrite_source(source_path, text + "worker_report: {}\n")
    _expect_reason(source_path, "RESULT_VALUE_CONFLICT")


def test_empty_changed_paths_are_accepted(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "changed_paths", [])
    loaded = _load(_write_document(tmp_path, payload))

    assert loaded.changed_paths == ()
    assert not hasattr(loaded, "pending_promotion")


def test_empty_validation_results_are_accepted_with_empty_caller_digests(
    tmp_path: Path,
) -> None:
    payload = _set_field(_reference_payload(), "validation_results", [])
    loaded = _load(
        _write_document(tmp_path, payload),
        expected_validation_digests={},
    )

    assert loaded.validation_results == ()
    assert loaded.worker_report.reported_status == "success"
    assert not hasattr(loaded, "decision")


def test_changed_paths_need_not_exist_because_they_are_not_a_sandbox(
    tmp_path: Path,
) -> None:
    loaded = _load(_write_document(tmp_path))

    for relative in loaded.changed_paths:
        assert not (tmp_path / relative).exists()
    assert not hasattr(loaded, "sandbox")
    assert not hasattr(load_worker_result, "sandbox")


def test_output_ref_need_not_exist_and_is_not_opened(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    loaded = _load(source_path)
    declared_log = tmp_path / loaded.validation_results[0].output_ref

    assert not declared_log.exists()
    assert loaded.validation_results[0].output_sha256 == _REFERENCE_OUTPUT_SHA256


def test_reported_failure_status_remains_untrusted_prose(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "worker_report.reported_status",
        "failure",
    )
    loaded = _load(_write_document(tmp_path, payload))

    assert loaded.worker_report.reported_status == "failure"
    assert loaded.validation_results[0].exit_code == 0
    assert not hasattr(loaded, "decision")
    assert not hasattr(loaded, "pending_promotion")
    assert list(tmp_path.rglob("pending_promotion.yaml")) == []


def test_validation_item_wrong_types_are_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["command_id"] = 1
    _expect_reason(_write_document(tmp_path / "command", payload), "RESULT_FIELD_TYPE")
    payload = _reference_payload()
    payload["validation_results"][0]["output_ref"] = ["log"]
    _expect_reason(_write_document(tmp_path / "ref", payload), "RESULT_FIELD_TYPE")
    payload = _reference_payload()
    payload["validation_results"][0]["output_sha256"] = 64
    _expect_reason(_write_document(tmp_path / "digest", payload), "RESULT_FIELD_TYPE")


def test_blank_validation_command_id_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["command_id"] = "   "
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_empty_output_ref_is_blocked(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["output_ref"] = ""
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


@pytest.mark.parametrize(
    "relative",
    (
        ".",
        "./",
        "runs/",
        "runs/*",
        "runs/**/*.log",
        "runs/synthetic-origin/validation/*.log",
        "*",
    ),
)
def test_root_glob_and_directory_output_refs_are_blocked(
    tmp_path: Path,
    relative: str,
) -> None:
    payload = _reference_payload()
    payload["validation_results"][0]["output_ref"] = relative
    _expect_reason(_write_document(tmp_path, payload), "RESULT_VALUE_CONFLICT")


def test_surplus_caller_digest_for_unknown_command_is_blocked(tmp_path: Path) -> None:
    _expect_reason(
        _write_document(tmp_path),
        "RESULT_DIGEST_MISMATCH",
        expected_validation_digests={
            _REFERENCE_COMMAND_ID: _REFERENCE_OUTPUT_SHA256,
            "lint": _FOREIGN_OUTPUT_SHA256,
        },
    )


def test_uppercase_output_digest_is_blocked(tmp_path: Path) -> None:
    upper = _REFERENCE_OUTPUT_SHA256.upper()
    payload = _reference_payload()
    payload["validation_results"][0]["output_sha256"] = upper
    _expect_reason(
        _write_document(tmp_path, payload),
        "RESULT_VALUE_CONFLICT",
        expected_validation_digests={_REFERENCE_COMMAND_ID: upper},
    )


def test_short_result_sha_is_blocked(tmp_path: Path) -> None:
    short_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    payload = _set_field(_reference_payload(), "result_sha", short_sha)
    expected = WorkerResultIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=short_sha,
    )
    _expect_reason(
        _write_document(tmp_path, payload),
        "RESULT_VALUE_CONFLICT",
        expected_identity=expected,
    )


def test_missing_result_document_is_blocked(tmp_path: Path) -> None:
    _expect_reason(tmp_path / "worker_result.yaml", "RESULT_SOURCE_MISSING")


def test_result_document_directory_is_blocked(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    source_path.unlink()
    source_path.mkdir()
    _expect_reason(source_path, "RESULT_SOURCE_MISSING")


def test_unreadable_result_document_is_blocked(tmp_path: Path) -> None:
    source_path = _write_document(tmp_path)
    original_mode = source_path.stat().st_mode
    try:
        _make_unreadable(source_path)
        _expect_reason(source_path, "RESULT_SOURCE_UNREADABLE")
    finally:
        if source_path.is_file():
            source_path.chmod(original_mode)
