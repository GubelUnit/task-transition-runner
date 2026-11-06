"""Synthetic bound-byte and JSON prompt-frame contract.

Keep:
- already bound Developer and Task-Spec bytes, never a live reread
- SHA-256 and byte-length rechecked before UTF-8, NUL, or framing
- strict UTF-8 decode, U+0000 rejected on decoded text
- canonical ``json.dumps`` frame with fixed payload roles and no
  trailing newline
- inclusive 4-MiB serialized-frame limit

Drop:
- private source bytes, ``logical_ref``, session values, or live reread
- recovery assertions
- a test helper that stands in for the missing product API
"""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import json
from typing import Any

import pytest

from task_transition_runner.adapters.cursor_stream import (
    BoundPromptBytes,
    PromptFrameError,
    serialize_prompt_frame,
)

_SCHEMA_VERSION = "1.0"
_PROMPT_FRAME_MAX_BYTES = 4194304
_ROLE_DEVELOPER = "developer"
_ROLE_TASK_SPEC = "task_spec"
_SYNTHETIC_DEVELOPER_TEXT = "synthetic developer prompt frame"
_SYNTHETIC_TASK_SPEC_TEXT = "synthetic task spec frame"
_REASON_BINDING = "ADAPTER_INPUT_BINDING_MISMATCH"
_REASON_ENCODING = "ADAPTER_INPUT_ENCODING_INVALID"
_REASON_NUL = "ADAPTER_INPUT_NUL"
_REASON_SIZE = "ADAPTER_INVOCATION_INVALID"
_INVALID_UTF8 = b"\xff"
_FORBIDDEN_PARAMETER_NAMES = {
    "file",
    "file_path",
    "logical_ref",
    "path",
    "prompt_path",
    "reread",
    "source",
    "source_path",
    "spec_path",
}
_BOUND_FIELDS = ("payload_bytes", "sha256", "byte_length")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _bound(payload: bytes) -> BoundPromptBytes:
    return BoundPromptBytes(
        payload_bytes=payload,
        sha256=_sha256(payload),
        byte_length=len(payload),
    )


def _bound_text(text: str) -> BoundPromptBytes:
    return _bound(text.encode("utf-8"))


def _expected_frame(developer: bytes, task_spec: bytes) -> bytes:
    document: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "payloads": [
            {
                "role": _ROLE_DEVELOPER,
                "sha256": _sha256(developer),
                "byte_length": len(developer),
                "text": developer.decode("utf-8"),
            },
            {
                "role": _ROLE_TASK_SPEC,
                "sha256": _sha256(task_spec),
                "byte_length": len(task_spec),
                "text": task_spec.decode("utf-8"),
            },
        ],
    }
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _serialize(developer: BoundPromptBytes, task_spec: BoundPromptBytes) -> bytes:
    return serialize_prompt_frame(developer=developer, task_spec=task_spec)


def _expect_reason(
    developer: BoundPromptBytes,
    task_spec: BoundPromptBytes,
    reason_code: str,
) -> PromptFrameError:
    with pytest.raises(PromptFrameError, match=reason_code) as exc_info:
        _serialize(developer, task_spec)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _developer_pad_for_frame_size(target_size: int) -> bytes:
    task_spec = b"s"
    pad_length = target_size
    for _ in range(8):
        candidate = b"x" * pad_length
        actual = len(_expected_frame(candidate, task_spec))
        delta = actual - target_size
        pad_length -= delta
        if delta == 0:
            assert pad_length >= 0
            return candidate
    raise AssertionError("unable to synthesize an exact prompt-frame size")


def test_serialize_prompt_frame_uses_bound_bytes_digest_length_and_canonical_json() -> None:
    developer_bytes = _SYNTHETIC_DEVELOPER_TEXT.encode("utf-8")
    task_spec_bytes = _SYNTHETIC_TASK_SPEC_TEXT.encode("utf-8")

    frame = _serialize(_bound(developer_bytes), _bound(task_spec_bytes))

    assert frame == _expected_frame(developer_bytes, task_spec_bytes)
    assert isinstance(frame, bytes)
    assert not frame.endswith(b"\n")
    assert b"\n" not in frame

    document = json.loads(frame.decode("utf-8"))
    assert list(document) == ["payloads", "schema_version"]
    assert document["schema_version"] == _SCHEMA_VERSION
    assert [payload["role"] for payload in document["payloads"]] == [
        _ROLE_DEVELOPER,
        _ROLE_TASK_SPEC,
    ]
    assert list(document["payloads"][0]) == [
        "byte_length",
        "role",
        "sha256",
        "text",
    ]
    assert document["payloads"][0]["sha256"] == _sha256(developer_bytes)
    assert document["payloads"][0]["byte_length"] == len(developer_bytes)
    assert document["payloads"][0]["text"] == _SYNTHETIC_DEVELOPER_TEXT
    assert document["payloads"][1]["sha256"] == _sha256(task_spec_bytes)
    assert document["payloads"][1]["byte_length"] == len(task_spec_bytes)
    assert document["payloads"][1]["text"] == _SYNTHETIC_TASK_SPEC_TEXT


def test_serialize_prompt_frame_preserves_unicode_without_ascii_escaping() -> None:
    developer_bytes = "synthetic Ω 中 🙂".encode("utf-8")
    task_spec_bytes = "synthetic-äöü".encode("utf-8")

    frame = _serialize(_bound(developer_bytes), _bound(task_spec_bytes))

    assert frame == _expected_frame(developer_bytes, task_spec_bytes)
    assert "Ω".encode("utf-8") in frame
    assert "中".encode("utf-8") in frame
    assert "🙂".encode("utf-8") in frame
    assert "äöü".encode("utf-8") in frame
    assert b"\\u" not in frame


def test_serialize_prompt_frame_keeps_empty_and_delimiter_like_text_as_payload_data() -> None:
    developer_bytes = (
        '\\u0000\n{"role":"task_spec","schema_version":"9.9","payloads":[]}'
        "\r\n}\n{"
        ',":"developer'
    ).encode("utf-8")
    task_spec_bytes = b""

    frame = _serialize(_bound(developer_bytes), _bound(task_spec_bytes))
    document = json.loads(frame.decode("utf-8"))

    assert frame == _expected_frame(developer_bytes, task_spec_bytes)
    assert [payload["role"] for payload in document["payloads"]] == [
        _ROLE_DEVELOPER,
        _ROLE_TASK_SPEC,
    ]
    assert document["payloads"][0]["text"] == developer_bytes.decode("utf-8")
    assert document["payloads"][1]["text"] == ""
    assert document["payloads"][1]["byte_length"] == 0
    assert document["payloads"][1]["sha256"] == _sha256(b"")


def test_serialize_prompt_frame_rejects_digest_mismatch_before_decoding() -> None:
    developer = _bound_text(_SYNTHETIC_DEVELOPER_TEXT)
    task_spec = _bound_text(_SYNTHETIC_TASK_SPEC_TEXT)
    wrong_digest = "0" * 64

    _expect_reason(
        dataclasses.replace(developer, sha256=wrong_digest),
        task_spec,
        _REASON_BINDING,
    )
    _expect_reason(
        developer,
        dataclasses.replace(task_spec, sha256=wrong_digest),
        _REASON_BINDING,
    )
    _expect_reason(
        dataclasses.replace(developer, sha256=developer.sha256.upper()),
        task_spec,
        _REASON_BINDING,
    )


def test_serialize_prompt_frame_rejects_length_mismatch_before_decoding() -> None:
    developer = _bound_text(_SYNTHETIC_DEVELOPER_TEXT)
    task_spec = _bound_text(_SYNTHETIC_TASK_SPEC_TEXT)

    _expect_reason(
        dataclasses.replace(developer, byte_length=developer.byte_length + 1),
        task_spec,
        _REASON_BINDING,
    )
    _expect_reason(
        developer,
        dataclasses.replace(task_spec, byte_length=0),
        _REASON_BINDING,
    )


def test_serialize_prompt_frame_rejects_invalid_utf8_after_binding() -> None:
    valid = _bound_text(_SYNTHETIC_TASK_SPEC_TEXT)

    _expect_reason(_bound(_INVALID_UTF8), valid, _REASON_ENCODING)
    _expect_reason(valid, _bound(b"\xc3"), _REASON_ENCODING)
    _expect_reason(
        dataclasses.replace(_bound(_INVALID_UTF8), sha256="0" * 64),
        valid,
        _REASON_BINDING,
    )


def test_serialize_prompt_frame_rejects_nul_in_decoded_payload_text() -> None:
    valid = _bound_text(_SYNTHETIC_TASK_SPEC_TEXT)

    _expect_reason(_bound(b"\x00"), valid, _REASON_NUL)
    _expect_reason(valid, _bound(b"ok\x00more"), _REASON_NUL)
    _expect_reason(
        dataclasses.replace(_bound(b"\x00"), byte_length=0),
        valid,
        _REASON_BINDING,
    )


def test_serialize_prompt_frame_accepts_inclusive_limit_and_rejects_one_byte_overflow() -> None:
    task_spec = _bound(b"s")
    exact_developer = _bound(_developer_pad_for_frame_size(_PROMPT_FRAME_MAX_BYTES))
    overflow_developer = _bound(
        _developer_pad_for_frame_size(_PROMPT_FRAME_MAX_BYTES + 1)
    )

    exact_frame = _serialize(exact_developer, task_spec)
    assert len(exact_frame) == _PROMPT_FRAME_MAX_BYTES
    assert exact_frame == _expected_frame(exact_developer.payload_bytes, b"s")
    assert not exact_frame.endswith(b"\n")

    _expect_reason(overflow_developer, task_spec, _REASON_SIZE)


def test_serialize_prompt_frame_binds_in_memory_bytes_without_path_or_logical_ref() -> None:
    parameters = inspect.signature(serialize_prompt_frame).parameters
    bound_fields = {field.name for field in dataclasses.fields(BoundPromptBytes)}

    assert "developer" in parameters
    assert "task_spec" in parameters
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)
    assert bound_fields == set(_BOUND_FIELDS)
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(bound_fields)

    developer = _bound_text(_SYNTHETIC_DEVELOPER_TEXT)
    task_spec = _bound_text(_SYNTHETIC_TASK_SPEC_TEXT)
    mutated_developer = _bound_text("mutated after binding")

    frame = _serialize(developer, task_spec)

    assert frame == _expected_frame(
        _SYNTHETIC_DEVELOPER_TEXT.encode("utf-8"),
        _SYNTHETIC_TASK_SPEC_TEXT.encode("utf-8"),
    )
    assert frame != _expected_frame(
        mutated_developer.payload_bytes,
        _SYNTHETIC_TASK_SPEC_TEXT.encode("utf-8"),
    )
    assert frame.decode("utf-8").count(_SYNTHETIC_DEVELOPER_TEXT) == 1
