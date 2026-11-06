"""Canonical prompt frame, stream envelope, publish projection, and evidence.

Verify already-bound Developer and Task-Spec bytes, then serialize the
ratified schema-1.0 frame. Parse already-captured stdout bytes as a
physical Cursor stream-json envelope and return local inspection facts.
Construct a new publish
projection from eight allowlisted local facts. Reserve an attempt-owned
evidence bundle and publish raw parts by exclusive create, append, and
no-replace hard link. This module does not spawn processes or read stderr.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCHEMA_VERSION = "1.0"
_PROMPT_FRAME_MAX_BYTES = 4194304
_MAX_RECORDS = 100000
_MAX_LINE_BYTES_EXCLUDING_LF = 4194304
_ROLE_DEVELOPER = "developer"
_ROLE_TASK_SPEC = "task_spec"
_TYPE_SYSTEM = "system"
_TYPE_USER = "user"
_TYPE_ASSISTANT = "assistant"
_TYPE_TOOL_CALL = "tool_call"
_TYPE_RESULT = "result"
_SUBTYPE_INIT = "init"
_SUBTYPE_TASK_NOTIFICATION = "task_notification"
_SUBTYPE_SUCCESS = "success"
_CONTENT_TEXT = "text"
_REASON_BINDING = "ADAPTER_INPUT_BINDING_MISMATCH"
_REASON_ENCODING = "ADAPTER_INPUT_ENCODING_INVALID"
_REASON_NUL = "ADAPTER_INPUT_NUL"
_REASON_SIZE = "ADAPTER_INVOCATION_INVALID"
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
_REASON_PROJECTION_UNSAFE = "ADAPTER_PROJECTION_UNSAFE"
_REASON_EVIDENCE_CONFLICT = "ADAPTER_EVIDENCE_CONFLICT"
_REASON_EVIDENCE_IO_FAILED = "ADAPTER_EVIDENCE_IO_FAILED"
_DIR_OPEN_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_PART_CREATE_FLAGS = os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC
_PART_APPEND_FLAGS = os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC
_CONFLICT_EXISTENCE_ERRNOS = frozenset({errno.EEXIST})
_CONFLICT_OPEN_ERRNOS = frozenset({errno.EEXIST, errno.ELOOP})
_CONFLICT_LOOKUP_ERRNOS = frozenset({errno.ENOENT, errno.ELOOP, errno.ENOTDIR})
_EMPTY_CONFLICT_ERRNOS: frozenset[int] = frozenset()
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
_EVENT_COUNT_KEYS = (
    "assistant",
    "result",
    "system",
    "tool_call",
    "unknown",
    "user",
)


class PromptFrameError(Exception):
    """Fail-closed violation of the prompt-frame contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class CursorStreamError(Exception):
    """Fail-closed violation of the physical stream envelope contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class AdapterProjectionError(Exception):
    """Fail-closed violation of the publish-allowlist contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class AdapterEvidenceError(Exception):
    """Fail-closed violation of the evidence reservation contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class _StrictJsonError(Exception):
    """Internal JSON-contract failure; never exposed to callers."""


@dataclass(frozen=True)
class BoundPromptBytes:
    payload_bytes: bytes
    sha256: str
    byte_length: int


@dataclass(frozen=True)
class CursorStreamInspection:
    event_counts: dict[str, int]
    session_observed: bool
    terminal_success: bool
    prompt_frame_matches: bool


def serialize_prompt_frame(
    *,
    developer: BoundPromptBytes,
    task_spec: BoundPromptBytes,
) -> bytes:
    _verify_binding(developer)
    _verify_binding(task_spec)
    developer_text = _decode_utf8(developer)
    task_spec_text = _decode_utf8(task_spec)
    _reject_nul(developer_text)
    _reject_nul(task_spec_text)
    frame = _serialize_canonical_frame(
        developer=developer,
        developer_text=developer_text,
        task_spec=task_spec,
        task_spec_text=task_spec_text,
    )
    if len(frame) > _PROMPT_FRAME_MAX_BYTES:
        raise PromptFrameError(_REASON_SIZE)
    return frame


def parse_cursor_stream(
    *,
    stdout: bytes,
    target: str,
    prompt_frame: bytes,
) -> CursorStreamInspection:
    records, has_final_lf = _scan_physical_records(stdout)
    decoded = _decode_record_texts(records)
    events, has_blank = _load_json_objects(decoded)
    _reject_event_invalid(events, has_blank=has_blank, has_final_lf=has_final_lf)
    _evaluate_envelope(
        events,
        target=target,
        prompt_frame=prompt_frame,
    )
    return CursorStreamInspection(
        event_counts=_count_events(events),
        session_observed=True,
        terminal_success=True,
        prompt_frame_matches=True,
    )


def build_publish_projection(*, local_facts: object) -> dict[str, Any]:
    if not isinstance(local_facts, Mapping):
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    for key in _PUBLISHABLE_KEYS:
        if key not in local_facts:
            raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    adapter_status = _require_str(local_facts["adapter_status"])
    reason_code = _require_str(local_facts["neutral_reason_code"])
    exit_code = _require_int(local_facts["process_exit_code"])
    timeout = _require_bool(local_facts["timeout"])
    interrupted = _require_bool(local_facts["interrupted"])
    event_counts = _project_event_counts(local_facts["event_counts"])
    session_observed = _require_bool(local_facts["session_observed"])
    terminal_success = _require_bool(local_facts["terminal_success"])
    return {
        "adapter_status": adapter_status,
        "neutral_reason_code": reason_code,
        "process_exit_code": exit_code,
        "timeout": timeout,
        "interrupted": interrupted,
        "event_counts": event_counts,
        "session_observed": session_observed,
        "terminal_success": terminal_success,
    }


class _EvidencePart:
    def __init__(
        self,
        *,
        bundle: _EvidenceBundle,
        basename: str,
        fd: int,
        dev: int,
        ino: int,
    ) -> None:
        self.basename = basename
        self._bundle = bundle
        self._fd = fd
        self._dev = dev
        self._ino = ino
        self._published = False


class _EvidenceBundle:
    def __init__(
        self,
        *,
        directory: str,
        owner_token: str,
        dir_fd: int,
    ) -> None:
        self.directory = directory
        self.owner_token = owner_token
        self._dir_fd = dir_fd

    def create_part(self, *, basename: str) -> _EvidencePart:
        _reject_invalid_basename(basename)
        fd = _syscall(
            lambda: os.open(
                basename,
                _PART_CREATE_FLAGS,
                0o600,
                dir_fd=self._dir_fd,
            ),
            conflict_errnos=_CONFLICT_OPEN_ERRNOS,
        )
        _syscall(lambda: os.fchmod(fd, 0o600))
        info = os.fstat(fd)
        return _EvidencePart(
            bundle=self,
            basename=basename,
            fd=fd,
            dev=info.st_dev,
            ino=info.st_ino,
        )

    def write_part(self, *, part: _EvidencePart, payload: bytes) -> None:
        self._require_active_part(part)
        fd = self._open_owned_part(part, _PART_APPEND_FLAGS)
        try:
            _syscall(lambda: _append_and_fsync(fd, payload))
        finally:
            os.close(fd)

    def publish(self, *, part: _EvidencePart, final_basename: str) -> None:
        self._require_active_part(part)
        _reject_invalid_basename(final_basename)
        if final_basename == part.basename:
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
        fd = self._open_owned_part(part, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        os.close(fd)
        try:
            _syscall(
                lambda: os.link(
                    part.basename,
                    final_basename,
                    src_dir_fd=self._dir_fd,
                    dst_dir_fd=self._dir_fd,
                    follow_symlinks=False,
                ),
                conflict_errnos=_CONFLICT_EXISTENCE_ERRNOS,
            )
        except AdapterEvidenceError as exc:
            if exc.reason_code == _REASON_EVIDENCE_CONFLICT:
                part._published = True
                self._release_pin(part)
            raise
        part._published = True
        info = _syscall(
            lambda: os.stat(
                final_basename,
                dir_fd=self._dir_fd,
                follow_symlinks=False,
            ),
            conflict_errnos=_CONFLICT_LOOKUP_ERRNOS,
        )
        if (info.st_dev, info.st_ino) != (part._dev, part._ino):
            self._release_pin(part)
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
        _syscall(lambda: os.fsync(self._dir_fd))
        self._release_pin(part)
        _syscall(lambda: os.unlink(part.basename, dir_fd=self._dir_fd))
        _syscall(lambda: os.fsync(self._dir_fd))

    def _release_pin(self, part: _EvidencePart) -> None:
        os.close(part._fd)
        part._fd = None

    def _require_active_part(self, part: _EvidencePart) -> None:
        if part._bundle is not self or part._published:
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)

    def _open_owned_part(self, part: _EvidencePart, flags: int) -> int:
        fd = _syscall(
            lambda: os.open(
                part.basename,
                flags,
                dir_fd=self._dir_fd,
            ),
            conflict_errnos=_CONFLICT_LOOKUP_ERRNOS,
        )
        info = os.fstat(fd)
        if (info.st_dev, info.st_ino) != (part._dev, part._ino):
            os.close(fd)
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
        return fd


def reserve_evidence_bundle(
    *,
    runtime_root: object,
    target_root: object,
    runner_source_root: object,
    attempt_token: str,
) -> _EvidenceBundle:
    runtime_resolved = _resolved_real_directory(runtime_root)
    target_resolved = _resolved_real_directory(target_root)
    source_resolved = _resolved_real_directory(runner_source_root)
    if _is_same_or_below(runtime_resolved, target_resolved):
        raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
    if _is_same_or_below(runtime_resolved, source_resolved):
        raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
    _reject_invalid_basename(attempt_token)
    runtime_fd = _syscall(
        lambda: os.open(runtime_resolved, _DIR_OPEN_FLAGS),
    )
    try:
        _syscall(
            lambda: os.mkdir(attempt_token, 0o700, dir_fd=runtime_fd),
            conflict_errnos=_CONFLICT_EXISTENCE_ERRNOS,
        )
        bundle_fd = _syscall(
            lambda: os.open(
                attempt_token,
                _DIR_OPEN_FLAGS,
                dir_fd=runtime_fd,
            ),
        )
    finally:
        os.close(runtime_fd)
    return _EvidenceBundle(
        directory=os.path.join(runtime_resolved, attempt_token),
        owner_token=attempt_token,
        dir_fd=bundle_fd,
    )


def _syscall(
    action: Callable[[], Any],
    *,
    conflict_errnos: frozenset[int] = _EMPTY_CONFLICT_ERRNOS,
) -> Any:
    try:
        return action()
    except OSError as exc:
        if exc.errno in conflict_errnos:
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT) from None
        raise AdapterEvidenceError(_REASON_EVIDENCE_IO_FAILED) from None


def _append_and_fsync(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view.nbytes:
        written = os.write(fd, view)
        view = view[written:]
    os.fsync(fd)


def _reject_invalid_basename(name: object) -> None:
    if "\x00" in name or name in {"", ".", ".."}:
        raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
    if os.path.basename(name) != name:
        raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)


def _is_same_or_below(path: str, root: str) -> bool:
    candidate = Path(path)
    boundary = Path(root)
    return candidate == boundary or boundary in candidate.parents


def _resolved_real_directory(path: object) -> str:
    absolute = os.path.abspath(os.fspath(path))
    components: list[str] = []
    rest = absolute
    while True:
        parent, name = os.path.split(rest)
        if parent == rest:
            break
        components.append(name)
        rest = parent
    current = rest
    info = os.lstat(rest)
    for name in reversed(components):
        current = os.path.join(current, name)
        try:
            info = os.lstat(current)
        except OSError:
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT) from None
        if stat.S_ISLNK(info.st_mode):
            raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
    if not stat.S_ISDIR(info.st_mode):
        raise AdapterEvidenceError(_REASON_EVIDENCE_CONFLICT)
    return current


def _require_str(value: object) -> str:
    if type(value) is not str:
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    return value


def _require_int(value: object) -> int:
    if type(value) is not int:
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    return value


def _require_bool(value: object) -> bool:
    if type(value) is not bool:
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    return value


def _project_event_counts(raw: object) -> dict[str, int]:
    if not isinstance(raw, Mapping):
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    for key in _EVENT_COUNT_KEYS:
        if key in raw and type(raw[key]) is not int:
            raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    if set(raw) != set(_EVENT_COUNT_KEYS):
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)
    projected: dict[str, int] = {}
    for key in _EVENT_COUNT_KEYS:
        projected[key] = raw[key]
    return projected


def _verify_binding(bound: BoundPromptBytes) -> None:
    digest = hashlib.sha256(bound.payload_bytes).hexdigest()
    if bound.sha256 != digest:
        raise PromptFrameError(_REASON_BINDING)
    if bound.byte_length != len(bound.payload_bytes):
        raise PromptFrameError(_REASON_BINDING)


def _decode_utf8(bound: BoundPromptBytes) -> str:
    try:
        return bound.payload_bytes.decode("utf-8")
    except UnicodeDecodeError:
        pass
    raise PromptFrameError(_REASON_ENCODING)


def _reject_nul(text: str) -> None:
    if "\x00" in text:
        raise PromptFrameError(_REASON_NUL)


def _serialize_canonical_frame(
    *,
    developer: BoundPromptBytes,
    developer_text: str,
    task_spec: BoundPromptBytes,
    task_spec_text: str,
) -> bytes:
    document = {
        "schema_version": _SCHEMA_VERSION,
        "payloads": [
            {
                "role": _ROLE_DEVELOPER,
                "sha256": developer.sha256,
                "byte_length": developer.byte_length,
                "text": developer_text,
            },
            {
                "role": _ROLE_TASK_SPEC,
                "sha256": task_spec.sha256,
                "byte_length": task_spec.byte_length,
                "text": task_spec_text,
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


def _scan_physical_records(stdout: bytes) -> tuple[tuple[bytes, ...], bool]:
    if stdout == b"":
        raise CursorStreamError(_REASON_EMPTY)
    has_final_lf = stdout.endswith(b"\n")
    if has_final_lf:
        body = stdout[:-1]
    else:
        body = stdout
    records = body.split(b"\n")
    if len(records) > _MAX_RECORDS:
        raise CursorStreamError(_REASON_LIMIT)
    for record in records:
        if len(record) > _MAX_LINE_BYTES_EXCLUDING_LF:
            raise CursorStreamError(_REASON_LIMIT)
    return tuple(records), has_final_lf


def _decode_record_texts(records: tuple[bytes, ...]) -> tuple[str, ...]:
    decoded: list[str] = []
    for record in records:
        try:
            decoded.append(record.decode("utf-8"))
        except UnicodeDecodeError:
            raise CursorStreamError(_REASON_UTF8) from None
    return tuple(decoded)


def _reject_non_finite(_name: str) -> object:
    raise _StrictJsonError


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _StrictJsonError
        result[key] = value
    return result


def _load_object(text: str) -> dict[str, Any]:
    try:
        value = json.loads(
            text,
            parse_constant=_reject_non_finite,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except json.JSONDecodeError:
        raise _StrictJsonError from None
    if type(value) is not dict:
        raise _StrictJsonError
    return value


def _load_json_objects(decoded: tuple[str, ...]) -> tuple[list[dict[str, Any]], bool]:
    events: list[dict[str, Any]] = []
    has_blank = False
    for text in decoded:
        if text.strip() == "":
            has_blank = True
            continue
        try:
            events.append(_load_object(text))
        except _StrictJsonError:
            raise CursorStreamError(_REASON_JSON) from None
    return events, has_blank


def _user_content_invalid(event: dict[str, Any]) -> bool:
    try:
        content = event["message"]["content"]
        item = content[0]
        text = item["text"]
        text.encode("utf-8")
    except (KeyError, TypeError, AttributeError, IndexError):
        return True
    if len(content) != 1:
        return True
    return item.get("type") != _CONTENT_TEXT


def _reject_event_invalid(
    events: list[dict[str, Any]],
    *,
    has_blank: bool,
    has_final_lf: bool,
) -> None:
    if has_blank or not has_final_lf:
        raise CursorStreamError(_REASON_EVENT)
    for event in events:
        if event.get("type") == _TYPE_USER and _user_content_invalid(event):
            raise CursorStreamError(_REASON_EVENT)


def _evaluate_envelope(
    events: list[dict[str, Any]],
    *,
    target: str,
    prompt_frame: bytes,
) -> None:
    seen_init = False
    seen_user = False
    seen_result = False
    envelope_invalid = False
    terminal_duplicate = False
    init_event: dict[str, Any] | None = None
    result_event: dict[str, Any] | None = None
    known_events: list[dict[str, Any]] = []

    for event in events:
        event_type = event.get("type")
        if seen_result:
            if event_type == _TYPE_RESULT:
                terminal_duplicate = True
                known_events.append(event)
            else:
                envelope_invalid = True
            continue

        if event_type == _TYPE_SYSTEM:
            known_events.append(event)
            if event.get("subtype") == _SUBTYPE_INIT and not seen_init:
                seen_init = True
                init_event = event
            elif (
                event.get("subtype") == _SUBTYPE_TASK_NOTIFICATION and seen_user
            ):
                pass
            else:
                envelope_invalid = True
            continue

        if event_type == _TYPE_USER:
            known_events.append(event)
            if not seen_init or seen_user:
                envelope_invalid = True
            seen_user = True
            user_text = event["message"]["content"][0]["text"]
            if user_text.encode("utf-8") != prompt_frame:
                envelope_invalid = True
            continue

        if event_type == _TYPE_ASSISTANT or event_type == _TYPE_TOOL_CALL:
            known_events.append(event)
            if not seen_user:
                envelope_invalid = True
            continue

        if event_type == _TYPE_RESULT:
            known_events.append(event)
            if not seen_user:
                envelope_invalid = True
            seen_result = True
            result_event = event
            continue

        if not seen_init:
            envelope_invalid = True

    if envelope_invalid:
        raise CursorStreamError(_REASON_ENVELOPE)
    if init_event.get("cwd") != target:
        raise CursorStreamError(_REASON_WORKSPACE)
    _reject_session_failures(known_events)
    if not seen_result:
        raise CursorStreamError(_REASON_TERMINAL_MISSING)
    if terminal_duplicate:
        raise CursorStreamError(_REASON_TERMINAL_DUPLICATE)
    if (
        result_event.get("subtype") != _SUBTYPE_SUCCESS
        or result_event.get("is_error") is not False
    ):
        raise CursorStreamError(_REASON_TERMINAL_FAILURE)


def _count_events(events: list[dict[str, Any]]) -> dict[str, int]:
    counts = {key: 0 for key in _EVENT_COUNT_KEYS}
    for event in events:
        event_type = event.get("type")
        if event_type in counts:
            counts[event_type] += 1
        else:
            counts["unknown"] += 1
    return counts


def _reject_session_failures(known_events: list[dict[str, Any]]) -> None:
    canonical: str | None = None
    session_missing = False
    session_conflict = False
    for event in known_events:
        session_id = event.get("session_id")
        try:
            stripped = session_id.strip()
        except AttributeError:
            session_missing = True
            continue
        if stripped == "":
            session_missing = True
            continue
        if canonical is None:
            canonical = session_id
        elif session_id != canonical:
            session_conflict = True
    if session_missing:
        raise CursorStreamError(_REASON_SESSION_MISSING)
    if session_conflict:
        raise CursorStreamError(_REASON_SESSION_CONFLICT)
