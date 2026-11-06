"""Synthetic physical Cursor NDJSON envelope contract.

Keep:
- stdout bytes, resolved Target, and already serialized prompt-frame bytes
- strict UTF-8, LF records, no blanks, one JSON object per record
- physical order, one consistent synthetic session, one terminal success
- unknown events after init without authority

Drop:
- parser implementation, private stream fixtures, real session IDs
- path/file inputs, stderr parsing, process, evidence, or publish projection
"""

from __future__ import annotations

import hashlib
import inspect
import json
from typing import Any

import pytest

from task_transition_runner.adapters.cursor_stream import (
    BoundPromptBytes,
    parse_cursor_stream,
    serialize_prompt_frame,
)

_MAX_RECORDS = 100000
_MAX_LINE_BYTES_EXCLUDING_LF = 4194304
_SYNTHETIC_TARGET = "/synthetic-oss5-cursor-target"
_SYNTHETIC_OTHER_TARGET = "/synthetic-oss5-other-target"
_SYNTHETIC_SESSION_ID = "synthetic-oss5-cursor-session-id"
_SYNTHETIC_OTHER_SESSION_ID = "synthetic-oss5-other-session-id"
_SYNTHETIC_DEVELOPER_TEXT = "synthetic developer prompt frame"
_SYNTHETIC_TASK_SPEC_TEXT = "synthetic task spec frame"
_SYNTHETIC_ASSISTANT_TEXT = "synthetic-assistant-text-must-not-authorize"
_SYNTHETIC_TOOL_NAME = "synthetic-tool-must-not-authorize"
_SYNTHETIC_RESULT_TEXT = "synthetic-result-text-must-not-authorize"
_SYNTHETIC_UNKNOWN_TYPE = "synthetic_future_event"
_REASON_EMPTY = "ADAPTER_STREAM_EMPTY"
_REASON_UTF8 = "ADAPTER_STREAM_UTF8_INVALID"
_REASON_JSON = "ADAPTER_STREAM_JSON_INVALID"
_REASON_EVENT = "ADAPTER_STREAM_EVENT_INVALID"
_REASON_ENVELOPE = "ADAPTER_STREAM_ENVELOPE_INVALID"
_REASON_LIMIT = "ADAPTER_STREAM_LIMIT_EXCEEDED"
_REASON_WORKSPACE = "ADAPTER_WORKSPACE_MISMATCH"
_REASON_SESSION_MISSING = "ADAPTER_SESSION_MISSING"
_REASON_SESSION_CONFLICT = "ADAPTER_SESSION_CONFLICT"
_REASON_TERMINAL_MISSING = "ADAPTER_TERMINAL_MISSING"
_REASON_TERMINAL_DUPLICATE = "ADAPTER_TERMINAL_DUPLICATE"
_REASON_TERMINAL_FAILURE = "ADAPTER_TERMINAL_FAILURE"
_FORBIDDEN_PARAMETER_NAMES = {
    "file",
    "file_path",
    "logical_ref",
    "path",
    "source",
    "source_path",
    "stderr",
    "stdin",
}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _bound(payload: bytes) -> BoundPromptBytes:
    return BoundPromptBytes(
        payload_bytes=payload,
        sha256=_sha256(payload),
        byte_length=len(payload),
    )


_PROMPT_FRAME = serialize_prompt_frame(
    developer=_bound(_SYNTHETIC_DEVELOPER_TEXT.encode("utf-8")),
    task_spec=_bound(_SYNTHETIC_TASK_SPEC_TEXT.encode("utf-8")),
)


def _encode(event: dict[str, Any]) -> bytes:
    return json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _ndjson(*records: dict[str, Any] | bytes, final_lf: bool = True) -> bytes:
    lines = [record if isinstance(record, bytes) else _encode(record) for record in records]
    body = b"\n".join(lines)
    if final_lf:
        return body + b"\n"
    return body


def _init(
    *,
    cwd: str = _SYNTHETIC_TARGET,
    session_id: str | None = _SYNTHETIC_SESSION_ID,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "system",
        "subtype": "init",
        "cwd": cwd,
        "synthetic_init_note": "synthetic-unknown-field-tolerated",
    }
    if session_id is not None:
        event["session_id"] = session_id
    if extra:
        event.update(extra)
    return event


def _task_notification(
    *,
    session_id: str | None = _SYNTHETIC_SESSION_ID,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "system",
        "subtype": "task_notification",
        "synthetic_progress_note": "synthetic-task-notification-must-not-authorize",
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _user(
    *,
    text: bytes | None = None,
    session_id: str | None = _SYNTHETIC_SESSION_ID,
    content: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    frame_text = (text if text is not None else _PROMPT_FRAME).decode("utf-8")
    event: dict[str, Any] = {
        "type": "user",
        "message": {
            "content": content
            if content is not None
            else [
                {
                    "type": "text",
                    "text": frame_text,
                    "synthetic_content_note": "synthetic-unknown-field-tolerated",
                }
            ]
        },
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _assistant(*, session_id: str | None = _SYNTHETIC_SESSION_ID) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "assistant",
        "message": {
            "content": [{"type": "text", "text": _SYNTHETIC_ASSISTANT_TEXT}]
        },
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _tool(*, session_id: str | None = _SYNTHETIC_SESSION_ID) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "tool_call",
        "subtype": "started",
        "synthetic_tool_name": _SYNTHETIC_TOOL_NAME,
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _result(
    *,
    session_id: str | None = _SYNTHETIC_SESSION_ID,
    subtype: str = "success",
    is_error: bool = False,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "result",
        "subtype": subtype,
        "is_error": is_error,
        "result": _SYNTHETIC_RESULT_TEXT,
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _unknown() -> dict[str, Any]:
    return {
        "type": _SYNTHETIC_UNKNOWN_TYPE,
        "payload": "synthetic-unknown-event-must-not-authorize",
    }


def _valid_envelope(*middle: dict[str, Any] | bytes) -> bytes:
    return _ndjson(_init(), _user(), *middle, _result())


def _padded_assistant_line(size: int) -> bytes:
    prefix = b'{"pad":"'
    suffix = (
        b'","session_id":"'
        + _SYNTHETIC_SESSION_ID.encode("ascii")
        + b'","type":"assistant"}'
    )
    pad_length = size - len(prefix) - len(suffix)
    assert pad_length >= 0
    line = prefix + (b"a" * pad_length) + suffix
    assert len(line) == size
    return line


def _parse(
    stdout: bytes,
    *,
    target: str = _SYNTHETIC_TARGET,
    prompt_frame: bytes = _PROMPT_FRAME,
) -> object:
    return parse_cursor_stream(
        stdout=stdout,
        target=target,
        prompt_frame=prompt_frame,
    )


def _expect_reason(
    stdout: bytes,
    reason_code: str,
    *,
    target: str = _SYNTHETIC_TARGET,
    prompt_frame: bytes = _PROMPT_FRAME,
) -> None:
    with pytest.raises(Exception) as exc_info:
        _parse(stdout, target=target, prompt_frame=prompt_frame)
    assert getattr(exc_info.value, "reason_code") == reason_code


def test_parse_cursor_stream_binds_stdout_target_and_serialized_prompt_frame() -> None:
    parameters = inspect.signature(parse_cursor_stream).parameters

    assert "stdout" in parameters
    assert "target" in parameters
    assert "prompt_frame" in parameters
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)

    _parse(_valid_envelope(_assistant(), _tool(), _unknown()))


def test_parse_cursor_stream_accepts_synthetic_success_envelope() -> None:
    _parse(
        _valid_envelope(
            _assistant(),
            _tool(),
            _unknown(),
        )
    )


def test_parse_cursor_stream_accepts_inclusive_record_and_line_limits() -> None:
    filler = _encode(_assistant())
    inclusive_records = _ndjson(
        _init(),
        _user(),
        *([filler] * (_MAX_RECORDS - 3)),
        _result(),
    )
    inclusive_line = _valid_envelope(_padded_assistant_line(_MAX_LINE_BYTES_EXCLUDING_LF))

    assert inclusive_records.count(b"\n") == _MAX_RECORDS
    assert len(_padded_assistant_line(_MAX_LINE_BYTES_EXCLUDING_LF)) == (
        _MAX_LINE_BYTES_EXCLUDING_LF
    )

    _parse(inclusive_records)
    _parse(inclusive_line)


def test_parse_cursor_stream_rejects_empty_stream() -> None:
    _expect_reason(b"", _REASON_EMPTY)


def test_parse_cursor_stream_rejects_blank_intermediate_record() -> None:
    _expect_reason(_ndjson(_init(), b"", _user(), _result()), _REASON_EVENT)
    _expect_reason(_ndjson(_init(), b" ", _user(), _result()), _REASON_EVENT)


def test_parse_cursor_stream_rejects_missing_final_lf() -> None:
    _expect_reason(_ndjson(_init(), _user(), _result(), final_lf=False), _REASON_EVENT)


def test_parse_cursor_stream_rejects_invalid_utf8() -> None:
    _expect_reason(_valid_envelope() + b"\xff\n", _REASON_UTF8)
    _expect_reason(b"\xff\n", _REASON_UTF8)


def test_parse_cursor_stream_rejects_malformed_truncated_and_mixed_json() -> None:
    _expect_reason(_ndjson(_init(), b"{not json}", _user(), _result()), _REASON_JSON)
    _expect_reason(_ndjson(_init(), b'{"type":"assistant"', _user(), _result()), _REASON_JSON)
    _expect_reason(
        _ndjson(_init(), _encode(_user()) + _encode(_assistant()), _result()),
        _REASON_JSON,
    )


def test_parse_cursor_stream_rejects_non_object_records() -> None:
    _expect_reason(_ndjson(_init(), b"[]", _user(), _result()), _REASON_JSON)
    _expect_reason(_ndjson(_init(), b'"string"', _user(), _result()), _REASON_JSON)
    _expect_reason(_ndjson(_init(), b"42", _user(), _result()), _REASON_JSON)
    _expect_reason(_ndjson(_init(), b"true", _user(), _result()), _REASON_JSON)
    _expect_reason(_ndjson(_init(), b"null", _user(), _result()), _REASON_JSON)


def test_parse_cursor_stream_rejects_duplicate_keys_and_non_finite_numbers() -> None:
    duplicate_type = (
        b'{"cwd":"'
        + _SYNTHETIC_TARGET.encode("ascii")
        + b'","session_id":"'
        + _SYNTHETIC_SESSION_ID.encode("ascii")
        + b'","subtype":"init","type":"system","type":"user"}'
    )
    nan_record = (
        b'{"n":NaN,"session_id":"'
        + _SYNTHETIC_SESSION_ID.encode("ascii")
        + b'","type":"assistant"}'
    )
    infinity_record = (
        b'{"n":Infinity,"session_id":"'
        + _SYNTHETIC_SESSION_ID.encode("ascii")
        + b'","type":"assistant"}'
    )
    neg_infinity_record = (
        b'{"n":-Infinity,"session_id":"'
        + _SYNTHETIC_SESSION_ID.encode("ascii")
        + b'","type":"assistant"}'
    )

    _expect_reason(_ndjson(duplicate_type, _user(), _result()), _REASON_JSON)
    _expect_reason(_valid_envelope(nan_record), _REASON_JSON)
    _expect_reason(_valid_envelope(infinity_record), _REASON_JSON)
    _expect_reason(_valid_envelope(neg_infinity_record), _REASON_JSON)


def test_parse_cursor_stream_rejects_record_and_line_limit_overflow() -> None:
    overflow_records = _ndjson(
        _init(),
        _user(),
        *([_encode(_assistant())] * (_MAX_RECORDS - 2)),
        _result(),
    )
    overflow_line = _valid_envelope(
        _padded_assistant_line(_MAX_LINE_BYTES_EXCLUDING_LF + 1)
    )

    assert overflow_records.count(b"\n") == _MAX_RECORDS + 1
    assert len(_padded_assistant_line(_MAX_LINE_BYTES_EXCLUDING_LF + 1)) == (
        _MAX_LINE_BYTES_EXCLUDING_LF + 1
    )

    _expect_reason(overflow_records, _REASON_LIMIT)
    _expect_reason(overflow_line, _REASON_LIMIT)


def test_parse_cursor_stream_rejects_user_event_without_exactly_one_text_item() -> None:
    _expect_reason(
        _ndjson(_init(), _user(content=[]), _result()),
        _REASON_EVENT,
    )
    _expect_reason(
        _ndjson(
            _init(),
            _user(
                content=[
                    {"type": "text", "text": _PROMPT_FRAME.decode("utf-8")},
                    {"type": "text", "text": _PROMPT_FRAME.decode("utf-8")},
                ]
            ),
            _result(),
        ),
        _REASON_EVENT,
    )
    _expect_reason(
        _ndjson(
            _init(),
            _user(content=[{"type": "image", "text": _PROMPT_FRAME.decode("utf-8")}]),
            _result(),
        ),
        _REASON_EVENT,
    )


def test_parse_cursor_stream_rejects_init_not_first_and_duplicate_init() -> None:
    _expect_reason(_ndjson(_user(), _init(), _result()), _REASON_ENVELOPE)
    _expect_reason(
        _ndjson(_init(extra={"subtype": "not-init"}), _user(), _result()),
        _REASON_ENVELOPE,
    )
    _expect_reason(_ndjson(_init(), _init(), _user(), _result()), _REASON_ENVELOPE)


def test_parse_cursor_stream_rejects_unknown_before_init() -> None:
    _expect_reason(_ndjson(_unknown(), _init(), _user(), _result()), _REASON_ENVELOPE)


def test_parse_cursor_stream_rejects_missing_or_duplicate_user() -> None:
    _expect_reason(_ndjson(_init(), _assistant(), _result()), _REASON_ENVELOPE)
    _expect_reason(_ndjson(_init(), _unknown(), _result()), _REASON_ENVELOPE)
    _expect_reason(_ndjson(_init(), _user(), _user(), _result()), _REASON_ENVELOPE)


def test_parse_cursor_stream_rejects_assistant_tool_or_result_before_user() -> None:
    _expect_reason(_ndjson(_init(), _assistant(), _user(), _result()), _REASON_ENVELOPE)
    _expect_reason(_ndjson(_init(), _tool(), _user(), _result()), _REASON_ENVELOPE)
    _expect_reason(_ndjson(_init(), _result(), _user()), _REASON_ENVELOPE)


def test_parse_cursor_stream_rejects_user_text_byte_mismatch() -> None:
    _expect_reason(
        _ndjson(_init(), _user(text=b"synthetic-user-text-mismatch"), _result()),
        _REASON_ENVELOPE,
    )


def test_parse_cursor_stream_rejects_trailing_event_after_result() -> None:
    _expect_reason(_ndjson(_init(), _user(), _result(), _assistant()), _REASON_ENVELOPE)
    _expect_reason(_ndjson(_init(), _user(), _result(), _unknown()), _REASON_ENVELOPE)


def test_parse_cursor_stream_rejects_init_cwd_mismatch() -> None:
    _expect_reason(
        _ndjson(_init(cwd=_SYNTHETIC_OTHER_TARGET), _user(), _result()),
        _REASON_WORKSPACE,
    )
    _expect_reason(
        _valid_envelope(),
        _REASON_WORKSPACE,
        target=_SYNTHETIC_OTHER_TARGET,
    )


def test_parse_cursor_stream_rejects_missing_and_blank_session_ids() -> None:
    _expect_reason(_ndjson(_init(session_id=None), _user(), _result()), _REASON_SESSION_MISSING)
    _expect_reason(_ndjson(_init(), _user(session_id=None), _result()), _REASON_SESSION_MISSING)
    _expect_reason(
        _ndjson(_init(), _user(), _assistant(session_id=None), _result()),
        _REASON_SESSION_MISSING,
    )
    _expect_reason(_ndjson(_init(), _user(), _tool(session_id=None), _result()), _REASON_SESSION_MISSING)
    _expect_reason(_ndjson(_init(), _user(), _result(session_id=None)), _REASON_SESSION_MISSING)
    _expect_reason(_ndjson(_init(session_id=""), _user(), _result()), _REASON_SESSION_MISSING)
    _expect_reason(_ndjson(_init(), _user(session_id=" \t"), _result()), _REASON_SESSION_MISSING)


def test_parse_cursor_stream_rejects_session_conflict_across_known_events() -> None:
    _expect_reason(
        _ndjson(_init(), _user(session_id=_SYNTHETIC_OTHER_SESSION_ID), _result()),
        _REASON_SESSION_CONFLICT,
    )
    _expect_reason(
        _ndjson(_init(), _user(), _assistant(session_id=_SYNTHETIC_OTHER_SESSION_ID), _result()),
        _REASON_SESSION_CONFLICT,
    )
    _expect_reason(
        _ndjson(_init(), _user(), _tool(session_id=_SYNTHETIC_OTHER_SESSION_ID), _result()),
        _REASON_SESSION_CONFLICT,
    )
    _expect_reason(
        _ndjson(_init(), _user(), _result(session_id=_SYNTHETIC_OTHER_SESSION_ID)),
        _REASON_SESSION_CONFLICT,
    )


def test_parse_cursor_stream_rejects_missing_duplicate_and_failed_terminal() -> None:
    _expect_reason(_ndjson(_init(), _user(), _assistant()), _REASON_TERMINAL_MISSING)
    _expect_reason(_ndjson(_init(), _user(), _unknown()), _REASON_TERMINAL_MISSING)
    _expect_reason(
        _ndjson(_init(), _user(), _result(), _result()),
        _REASON_TERMINAL_DUPLICATE,
    )
    _expect_reason(
        _ndjson(_init(), _user(), _result(subtype="error")),
        _REASON_TERMINAL_FAILURE,
    )
    _expect_reason(
        _ndjson(_init(), _user(), _result(is_error=True)),
        _REASON_TERMINAL_FAILURE,
    )
    omitted_is_error = _result()
    del omitted_is_error["is_error"]
    _expect_reason(_ndjson(_init(), _user(), omitted_is_error), _REASON_TERMINAL_FAILURE)


def test_parse_cursor_stream_prefers_utf8_over_json_and_event_codes() -> None:
    _expect_reason(_ndjson(_init(), b"\xff", b"{", final_lf=False), _REASON_UTF8)


def test_parse_cursor_stream_prefers_json_invalid_over_event_invalid() -> None:
    _expect_reason(_ndjson(_init(), b"{not json}", b"", _user(), _result()), _REASON_JSON)


def test_parse_cursor_stream_prefers_event_invalid_over_envelope_invalid() -> None:
    _expect_reason(_ndjson(_user(), _init(), _result(), final_lf=False), _REASON_EVENT)


def test_parse_cursor_stream_prefers_limit_over_json_invalid() -> None:
    overflow_line = _padded_assistant_line(_MAX_LINE_BYTES_EXCLUDING_LF + 1)[:-1] + b"{"
    _expect_reason(_ndjson(_init(), _user(), overflow_line, _result()), _REASON_LIMIT)


def test_parse_cursor_stream_prefers_session_missing_over_conflict() -> None:
    _expect_reason(
        _ndjson(
            _init(session_id=None),
            _user(),
            _result(session_id=_SYNTHETIC_OTHER_SESSION_ID),
        ),
        _REASON_SESSION_MISSING,
    )


def test_parse_cursor_stream_prefers_terminal_duplicate_over_failure() -> None:
    _expect_reason(
        _ndjson(_init(), _user(), _result(is_error=True), _result()),
        _REASON_TERMINAL_DUPLICATE,
    )


def test_parse_cursor_stream_accepts_post_user_system_task_notification_without_authority() -> None:
    baseline = _parse(_valid_envelope())
    observed = _parse(_valid_envelope(_task_notification()))

    assert observed.event_counts["system"] == baseline.event_counts["system"] + 1
    assert observed.event_counts["user"] == baseline.event_counts["user"] == 1
    assert observed.event_counts["assistant"] == baseline.event_counts["assistant"]
    assert observed.event_counts["tool_call"] == baseline.event_counts["tool_call"]
    assert observed.event_counts["result"] == baseline.event_counts["result"] == 1
    assert observed.event_counts["unknown"] == baseline.event_counts["unknown"] == 0
    assert observed.session_observed is baseline.session_observed is True
    assert observed.terminal_success is baseline.terminal_success is True
    assert observed.prompt_frame_matches is baseline.prompt_frame_matches is True


def test_parse_cursor_stream_rejects_task_notification_outside_non_authoritative_window() -> None:
    _expect_reason(
        _ndjson(_init(), _task_notification(), _user(), _result()),
        _REASON_ENVELOPE,
    )
    _expect_reason(
        _ndjson(_init(), _user(), _result(), _task_notification()),
        _REASON_ENVELOPE,
    )


def test_parse_cursor_stream_requires_task_notification_session_identity() -> None:
    _expect_reason(
        _ndjson(_init(), _user(), _task_notification(session_id=None), _result()),
        _REASON_SESSION_MISSING,
    )
    _expect_reason(
        _ndjson(
            _init(),
            _user(),
            _task_notification(session_id=_SYNTHETIC_OTHER_SESSION_ID),
            _result(),
        ),
        _REASON_SESSION_CONFLICT,
    )


def test_parse_cursor_stream_rejects_other_system_subtype_inside_task_notification_window() -> None:
    other_system_subtype = _task_notification()
    other_system_subtype["subtype"] = "synthetic_other_system_subtype"
    _expect_reason(
        _valid_envelope(other_system_subtype),
        _REASON_ENVELOPE,
    )
