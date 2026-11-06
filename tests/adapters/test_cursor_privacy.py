"""Synthetic positive publish-allowlist privacy contract.

Keep:
- reconstruction of exactly eight publishable fields
- type-and-value fidelity of safe allowlist values
- visible separation of raw-local, local-facts, and publishable
- unknown and local-only fields dropped, including prompt_frame_matches
- session published only as the session_observed boolean
- fail-closed ADAPTER_PROJECTION_UNSAFE for unprojectable shapes
- canary keys inside event_counts fail closed even with integer values

Drop:
- regex-only, blocklist, or "everything except" redaction as the contract
- raw bytes, raw digests, absolute paths, argv, executable, version, model
- prompt, spec, prompt-frame, auth, environment, endpoint, request, call
- assistant, tool, result, stdout, stderr, or a real session identifier
- WorkerResult, promotion, ORP, recovery, or a stand-in product helper
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Mapping
from typing import Any

import pytest

from task_transition_runner.adapters.cursor_stream import build_publish_projection

_REASON_UNSAFE = "ADAPTER_PROJECTION_UNSAFE"
_PUBLISHABLE_KEYS = (
    "adapter_status",
    "neutral_reason_code",
    "process_exit_code",
    "timeout",
    "interrupted",
    "event_counts",
    "session_observed",
    "terminal_success",
)
_FORBIDDEN_PARAMETER_NAMES = {
    "argv",
    "auth",
    "endpoint",
    "environment",
    "file",
    "file_path",
    "logical_ref",
    "model",
    "path",
    "prompt",
    "session_id",
    "source",
    "source_path",
    "stderr",
    "stdin",
    "stdout",
}
_CANARY_PROMPT = "SYNTHETIC-OSS5-CANARY-PROMPT"
_CANARY_SPEC = "SYNTHETIC-OSS5-CANARY-SPEC"
_CANARY_PROMPTFRAME = "SYNTHETIC-OSS5-CANARY-PROMPTFRAME"
_CANARY_ARGV = "SYNTHETIC-OSS5-CANARY-ARGV"
_CANARY_PATH = "/synthetic-oss5-canary-absolute-path/workspace"
_CANARY_EXECUTABLE = "/synthetic-oss5-canary-absolute-path/cursor"
_CANARY_VERSION = "SYNTHETIC-OSS5-CANARY-VERSION"
_CANARY_MODEL = "SYNTHETIC-OSS5-CANARY-MODEL"
_CANARY_AUTH = "SYNTHETIC-OSS5-CANARY-AUTH"
_CANARY_ENVIRONMENT_NAME = "SYNTHETIC-OSS5-CANARY-ENVIRONMENT-NAME"
_CANARY_ENVIRONMENT_VALUE = "SYNTHETIC-OSS5-CANARY-ENVIRONMENT-VALUE"
_CANARY_ENDPOINT = "SYNTHETIC-OSS5-CANARY-ENDPOINT"
_CANARY_REQUEST = "SYNTHETIC-OSS5-CANARY-REQUEST"
_CANARY_CALL = "SYNTHETIC-OSS5-CANARY-CALL"
_CANARY_SESSION = "SYNTHETIC-OSS5-CANARY-SESSION"
_CANARY_ASSISTANT = "SYNTHETIC-OSS5-CANARY-ASSISTANT"
_CANARY_TOOL = "SYNTHETIC-OSS5-CANARY-TOOL"
_CANARY_RESULT = "SYNTHETIC-OSS5-CANARY-RESULT"
_CANARY_STDOUT = "SYNTHETIC-OSS5-CANARY-STDOUT"
_CANARY_STDERR = "SYNTHETIC-OSS5-CANARY-STDERR"
_CANARY_STDOUT_DIGEST = "SYNTHETIC-OSS5-CANARY-STDOUT-DIGEST"
_CANARY_UNKNOWN = "SYNTHETIC-OSS5-CANARY-UNKNOWN-FIELD"
_CANARY_EVENT_COUNTS_KEY = "SYNTHETIC-OSS5-CANARY-EVENT-COUNTS-KEY"
_STRING_CANARIES = (
    _CANARY_PROMPT,
    _CANARY_SPEC,
    _CANARY_PROMPTFRAME,
    _CANARY_ARGV,
    _CANARY_PATH,
    _CANARY_EXECUTABLE,
    _CANARY_VERSION,
    _CANARY_MODEL,
    _CANARY_AUTH,
    _CANARY_ENVIRONMENT_NAME,
    _CANARY_ENVIRONMENT_VALUE,
    _CANARY_ENDPOINT,
    _CANARY_REQUEST,
    _CANARY_CALL,
    _CANARY_SESSION,
    _CANARY_ASSISTANT,
    _CANARY_TOOL,
    _CANARY_RESULT,
    _CANARY_STDOUT,
    _CANARY_STDERR,
    _CANARY_STDOUT_DIGEST,
    _CANARY_UNKNOWN,
    _CANARY_EVENT_COUNTS_KEY,
)


def _publishable() -> dict[str, Any]:
    return {
        "adapter_status": "completed",
        "neutral_reason_code": "ADAPTER_COMPLETED",
        "process_exit_code": 0,
        "timeout": False,
        "interrupted": False,
        "event_counts": {
            "assistant": 1,
            "result": 1,
            "system": 1,
            "tool_call": 1,
            "unknown": 1,
            "user": 1,
        },
        "session_observed": True,
        "terminal_success": True,
    }


def _raw_local() -> dict[str, Any]:
    return {
        "stdout": _CANARY_STDOUT.encode("utf-8"),
        "stderr": _CANARY_STDERR.encode("utf-8"),
        "stdout_digest": _CANARY_STDOUT_DIGEST,
        "stderr_excerpt": _CANARY_STDERR,
        "nested_raw": {
            "assistant": _CANARY_ASSISTANT,
            "records": [_CANARY_STDOUT, {"tool": _CANARY_TOOL}],
        },
    }


def _local_only_facts() -> dict[str, Any]:
    return {
        "prompt": _CANARY_PROMPT,
        "task_spec": _CANARY_SPEC,
        "prompt_frame": _CANARY_PROMPTFRAME,
        "prompt_frame_matches": True,
        "argv": [_CANARY_ARGV, _CANARY_EXECUTABLE, _CANARY_PATH],
        "executable_path": _CANARY_EXECUTABLE,
        "executable_version": _CANARY_VERSION,
        "model": _CANARY_MODEL,
        "auth": _CANARY_AUTH,
        "environment": {_CANARY_ENVIRONMENT_NAME: _CANARY_ENVIRONMENT_VALUE},
        "endpoint": _CANARY_ENDPOINT,
        "request_id": _CANARY_REQUEST,
        "call_id": _CANARY_CALL,
        "session_id": _CANARY_SESSION,
        "assistant_text": _CANARY_ASSISTANT,
        "tool_payload": _CANARY_TOOL,
        "result_text": _CANARY_RESULT,
        "target_path": _CANARY_PATH,
    }


def _local_facts_body() -> dict[str, Any]:
    body: dict[str, Any] = {}
    body.update(_raw_local())
    body.update(_local_only_facts())
    body.update(_publishable())
    body["future_unknown_field"] = _CANARY_UNKNOWN
    body["promotion_authorized"] = True
    body["worker_result"] = {"detail": _CANARY_RESULT}
    return body


def _project(local_facts: object) -> object:
    return build_publish_projection(local_facts=local_facts)


def _walk(value: object) -> list[object]:
    nodes = [value]
    if isinstance(value, Mapping):
        for key, item in value.items():
            nodes.append(key)
            nodes.extend(_walk(item))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            nodes.extend(_walk(item))
    return nodes


def _serialized(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _as_search_blob(node: object) -> str:
    if isinstance(node, bytes):
        return node.decode("utf-8", "replace")
    if isinstance(node, str):
        return node
    return str(node)


def _assert_canaries_absent(value: object) -> None:
    serialized = _serialized(value)
    blobs = [serialized]
    for node in _walk(value):
        blobs.append(_as_search_blob(node))
    haystack = "\n".join(blobs)
    for canary in _STRING_CANARIES:
        assert canary not in haystack
    assert "prompt_frame_matches" not in serialized
    assert '"session_id"' not in serialized


def _expect_unsafe(local_facts: object) -> None:
    with pytest.raises(Exception) as exc_info:
        _project(local_facts)
    assert getattr(exc_info.value, "reason_code") == _REASON_UNSAFE
    assert str(exc_info.value) == _REASON_UNSAFE
    _assert_canaries_absent(str(exc_info.value))


def test_build_publish_projection_binds_keyword_local_facts_only() -> None:
    parameters = inspect.signature(build_publish_projection).parameters

    assert "local_facts" in parameters
    assert parameters["local_facts"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["local_facts"].default is inspect.Parameter.empty
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)


def test_build_publish_projection_emits_exactly_the_eight_allowlist_fields() -> None:
    projection = _project(_local_facts_body())

    assert isinstance(projection, dict)
    assert list(projection) == list(_PUBLISHABLE_KEYS)
    assert tuple(projection.keys()) == _PUBLISHABLE_KEYS
    _assert_canaries_absent(projection)


def test_build_publish_projection_preserves_safe_allowlist_values_without_redaction() -> None:
    expected = _publishable()
    projection = _project(_local_facts_body())

    assert projection == expected
    assert projection["adapter_status"] == "completed"
    assert type(projection["adapter_status"]) is str
    assert projection["neutral_reason_code"] == "ADAPTER_COMPLETED"
    assert type(projection["neutral_reason_code"]) is str
    assert projection["process_exit_code"] == 0
    assert type(projection["process_exit_code"]) is int
    assert projection["timeout"] is False
    assert projection["interrupted"] is False
    assert projection["session_observed"] is True
    assert projection["terminal_success"] is True
    assert projection["event_counts"] == expected["event_counts"]
    assert all(type(count) is int for count in projection["event_counts"].values())


def test_build_publish_projection_reconstructs_instead_of_aliasing_input() -> None:
    local_facts = _local_facts_body()
    projection = _project(local_facts)

    local_facts["adapter_status"] = _CANARY_PROMPT
    local_facts["event_counts"]["assistant"] = 99
    local_facts["session_observed"] = False

    assert projection["adapter_status"] == "completed"
    assert projection["event_counts"]["assistant"] == 1
    assert projection["session_observed"] is True
    _assert_canaries_absent(projection)


def test_build_publish_projection_drops_unknown_and_local_only_fields() -> None:
    local_facts = _local_facts_body()
    local_facts["adapter_latency_ms"] = 12
    local_facts["session_observed_raw"] = _CANARY_SESSION
    local_facts["terminal_success_reason"] = _CANARY_RESULT
    projection = _project(local_facts)

    assert list(projection) == list(_PUBLISHABLE_KEYS)
    assert "future_unknown_field" not in projection
    assert "adapter_latency_ms" not in projection
    assert "prompt_frame_matches" not in projection
    assert "session_id" not in projection
    assert "session_observed_raw" not in projection
    assert "terminal_success_reason" not in projection
    assert "promotion_authorized" not in projection
    assert "worker_result" not in projection
    assert "stdout" not in projection
    assert "stderr" not in projection
    _assert_canaries_absent(projection)


def test_build_publish_projection_never_publishes_prompt_frame_matches() -> None:
    local_facts = _local_facts_body()
    assert local_facts["prompt_frame_matches"] is True
    projection = _project(local_facts)

    assert "prompt_frame_matches" not in projection
    assert "prompt_frame_matches" not in _serialized(projection)
    for node in _walk(projection):
        if isinstance(node, str):
            assert node != "prompt_frame_matches"


def test_build_publish_projection_publishes_session_only_as_boolean() -> None:
    local_facts = _local_facts_body()
    assert local_facts["session_id"] == _CANARY_SESSION
    projection = _project(local_facts)

    assert projection["session_observed"] is True
    assert "session_id" not in projection
    assert _CANARY_SESSION not in _serialized(projection)
    _assert_canaries_absent(projection)


def test_build_publish_projection_excludes_every_sensitive_canary_class() -> None:
    projection = _project(_local_facts_body())
    serialized = _serialized(projection)
    nodes = _walk(projection)

    _assert_canaries_absent(projection)
    assert _CANARY_PROMPT not in serialized
    assert _CANARY_SPEC not in serialized
    assert _CANARY_PROMPTFRAME not in serialized
    assert _CANARY_ARGV not in serialized
    assert _CANARY_PATH not in serialized
    assert _CANARY_EXECUTABLE not in serialized
    assert _CANARY_VERSION not in serialized
    assert _CANARY_MODEL not in serialized
    assert _CANARY_AUTH not in serialized
    assert _CANARY_ENVIRONMENT_NAME not in serialized
    assert _CANARY_ENVIRONMENT_VALUE not in serialized
    assert _CANARY_ENDPOINT not in serialized
    assert _CANARY_REQUEST not in serialized
    assert _CANARY_CALL not in serialized
    assert _CANARY_SESSION not in serialized
    assert _CANARY_ASSISTANT not in serialized
    assert _CANARY_TOOL not in serialized
    assert _CANARY_RESULT not in serialized
    assert _CANARY_STDOUT not in serialized
    assert _CANARY_STDERR not in serialized
    assert _CANARY_STDOUT_DIGEST not in serialized
    assert all(_CANARY_STDOUT.encode("utf-8") != node for node in nodes)
    assert all(_CANARY_STDERR.encode("utf-8") != node for node in nodes)


def test_build_publish_projection_rejects_non_mapping_local_facts() -> None:
    _expect_unsafe(["adapter_status", "completed"])
    _expect_unsafe("completed")
    _expect_unsafe(None)


def test_build_publish_projection_rejects_missing_allowlist_field() -> None:
    for key in _PUBLISHABLE_KEYS:
        local_facts = _local_facts_body()
        del local_facts[key]
        _expect_unsafe(local_facts)


def test_build_publish_projection_rejects_wrong_allowlist_types() -> None:
    cases: list[dict[str, Any]] = []
    status = _local_facts_body()
    status["adapter_status"] = {"nested": _CANARY_PROMPT}
    cases.append(status)
    reason = _local_facts_body()
    reason["neutral_reason_code"] = [_CANARY_SPEC]
    cases.append(reason)
    exit_bool = _local_facts_body()
    exit_bool["process_exit_code"] = True
    cases.append(exit_bool)
    exit_str = _local_facts_body()
    exit_str["process_exit_code"] = "0"
    cases.append(exit_str)
    timeout_int = _local_facts_body()
    timeout_int["timeout"] = 0
    cases.append(timeout_int)
    interrupted_str = _local_facts_body()
    interrupted_str["interrupted"] = "false"
    cases.append(interrupted_str)
    session_str = _local_facts_body()
    session_str["session_observed"] = "true"
    cases.append(session_str)
    terminal_int = _local_facts_body()
    terminal_int["terminal_success"] = 1
    cases.append(terminal_int)
    counts_list = _local_facts_body()
    counts_list["event_counts"] = [1]
    cases.append(counts_list)
    counts_str = _local_facts_body()
    counts_str["event_counts"] = {"system": _CANARY_STDERR}
    cases.append(counts_str)
    counts_bool = _local_facts_body()
    counts_bool["event_counts"] = {"system": True}
    cases.append(counts_bool)
    counts_nested = _local_facts_body()
    counts_nested["event_counts"] = {"system": {"stderr": _CANARY_STDERR}}
    cases.append(counts_nested)

    for local_facts in cases:
        _expect_unsafe(local_facts)


def test_build_publish_projection_rejects_canary_key_inside_event_counts() -> None:
    local_facts = _local_facts_body()
    event_counts = dict(local_facts["event_counts"])
    event_counts[_CANARY_EVENT_COUNTS_KEY] = 1
    local_facts["event_counts"] = event_counts

    assert type(event_counts[_CANARY_EVENT_COUNTS_KEY]) is int
    assert _CANARY_EVENT_COUNTS_KEY in event_counts

    try:
        projection = _project(local_facts)
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_UNSAFE
        assert str(exc) == _REASON_UNSAFE
        _assert_canaries_absent(str(exc))
        return

    _assert_canaries_absent(projection)
    raise AssertionError("expected fail-closed ADAPTER_PROJECTION_UNSAFE")


def test_build_publish_projection_does_not_emit_partial_projection_on_unsafe_input() -> None:
    local_facts = _publishable()
    local_facts["adapter_status"] = {"leak": _CANARY_PROMPT}
    _expect_unsafe(local_facts)
