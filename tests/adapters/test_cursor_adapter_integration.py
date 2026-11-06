"""Synthetic fake-process integration to a nonauthorizing adapter outcome.

Keep:
- keyword-only ``run_cursor_adapter`` as the sole composed product API
- pre-spawn order: bind/UTF-8/NUL/frame, executable and roots, endpoint,
  conflict-free evidence reservation, 0600 parts, then spawn
- exactly one user event whose text bytes equal the serialized prompt frame
- system/init cwd equal to the resolved Target
- Exit-nonzero before stream, session, or terminal interpretation
- complete post-spawn reason precedence and ratified tie-breaks
- atomic no-replace finalization before success
- allowlist publish projection and a typed outcome without authority fields
- post-capture owned stdout.part open/read/close OSError maps content-free to
  ADAPTER_EVIDENCE_IO_FAILED after the process is already reaped
- post-capture owned stdout.part open/read/close KeyboardInterrupt maps to
  ADAPTER_INTERRUPTED after the process is already reaped
- exact prompt frame appears once as the final positional spawn argv item
  without argv or frame retention
- a nonempty pending capture batch is retained to EOF when the inclusive
  stdout limit is breached without lowering product limits

Drop:
- WorkerResult, Decision, Promotion, Consumption, Recovery, or CLI imports
- treating a fake process as a real Cursor component proof
- projecting prompt, frame-match, argv, path, model, auth, environment,
  session id, tool, result, or stderr canaries
- a test helper that stands in for ``run_cursor_adapter``
"""

from __future__ import annotations

import errno
import hashlib
import inspect
import json
import os
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from task_transition_runner.adapters import cursor_headless as cursor_headless_mod
from task_transition_runner.adapters.cursor_headless import run_cursor_adapter
from task_transition_runner.adapters.cursor_stream import (
    AdapterEvidenceError,
    AdapterProjectionError,
    BoundPromptBytes,
    serialize_prompt_frame,
)

_ATTEMPT = "synthetic-oss5-attempt-adapter-01"
_SCRIPT_NAME = "synthetic-oss5-adapter-script"
_SCENARIO_NAME = "synthetic-oss5-adapter-scenario.json"
_IDENTITY_NAME = "synthetic-oss5-adapter-identity.json"
_STDOUT_PAYLOAD_NAME = "synthetic-oss5-adapter-stdout.bin"
_STDERR_PAYLOAD_NAME = "synthetic-oss5-adapter-stderr.bin"
_SESSION_ID = "synthetic-oss5-adapter-session-id"
_OTHER_SESSION_ID = "synthetic-oss5-adapter-other-session-id"
_SYNTHETIC_DEVELOPER = "SYNTHETIC-OSS5-CANARY-PROMPT"
_SYNTHETIC_TASK_SPEC = "SYNTHETIC-OSS5-CANARY-SPEC"
_CANARY_MODEL = "SYNTHETIC-OSS5-CANARY-MODEL"
_CANARY_AUTH = "SYNTHETIC-OSS5-CANARY-AUTH"
_CANARY_ENV_NAME = "SYNTHETIC_OSS5_CANARY_ENVIRONMENT_NAME"
_CANARY_ENV_VALUE = "SYNTHETIC-OSS5-CANARY-ENVIRONMENT-VALUE"
_CANARY_ENDPOINT = "SYNTHETIC-OSS5-CANARY-ENDPOINT"
_CANARY_TOOL = "SYNTHETIC-OSS5-CANARY-TOOL"
_CANARY_RESULT = "SYNTHETIC-OSS5-CANARY-RESULT"
_CANARY_ASSISTANT = "SYNTHETIC-OSS5-CANARY-ASSISTANT"
_CANARY_STDERR = "SYNTHETIC-OSS5-CANARY-STDERR"
_CANARY_ARGV = "SYNTHETIC-OSS5-CANARY-ARGV"
_OTHER_CWD = "/synthetic-oss5-other-target"
_WATCHDOG_SECONDS = 60
_REASON_NOT_FOUND = "ADAPTER_EXECUTABLE_NOT_FOUND"
_REASON_BINDING = "ADAPTER_INPUT_BINDING_MISMATCH"
_REASON_ENCODING = "ADAPTER_INPUT_ENCODING_INVALID"
_REASON_NUL = "ADAPTER_INPUT_NUL"
_REASON_INVALID = "ADAPTER_INVOCATION_INVALID"
_REASON_WORKSPACE = "ADAPTER_WORKSPACE_MISMATCH"
_REASON_EXIT = "ADAPTER_PROCESS_EXIT_NONZERO"
_REASON_TIMEOUT = "ADAPTER_TIMEOUT"
_REASON_INTERRUPT = "ADAPTER_INTERRUPTED"
_REASON_TERM = "ADAPTER_TERMINATION_FAILED"
_REASON_STDOUT_IO = "ADAPTER_STDOUT_IO_FAILED"
_REASON_STDERR_IO = "ADAPTER_STDERR_IO_FAILED"
_REASON_LIMIT = "ADAPTER_STREAM_LIMIT_EXCEEDED"
_REASON_EMPTY = "ADAPTER_STREAM_EMPTY"
_REASON_UTF8 = "ADAPTER_STREAM_UTF8_INVALID"
_REASON_JSON = "ADAPTER_STREAM_JSON_INVALID"
_REASON_EVENT = "ADAPTER_STREAM_EVENT_INVALID"
_REASON_ENVELOPE = "ADAPTER_STREAM_ENVELOPE_INVALID"
_REASON_SESSION_MISSING = "ADAPTER_SESSION_MISSING"
_REASON_SESSION_CONFLICT = "ADAPTER_SESSION_CONFLICT"
_REASON_TERMINAL_MISSING = "ADAPTER_TERMINAL_MISSING"
_REASON_TERMINAL_DUPLICATE = "ADAPTER_TERMINAL_DUPLICATE"
_REASON_TERMINAL_FAILURE = "ADAPTER_TERMINAL_FAILURE"
_REASON_EVIDENCE_IO = "ADAPTER_EVIDENCE_IO_FAILED"
_REASON_EVIDENCE_CONFLICT = "ADAPTER_EVIDENCE_CONFLICT"
_REASON_PROJECTION_UNSAFE = "ADAPTER_PROJECTION_UNSAFE"
_REASON_COMPLETED = "ADAPTER_COMPLETED"
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
    "child",
    "consumption",
    "decision",
    "decisions",
    "endpoint",
    "env",
    "environment",
    "file",
    "file_path",
    "journal",
    "logical_ref",
    "orp",
    "path",
    "pending",
    "process",
    "promotion",
    "prompt",
    "recovery",
    "resume",
    "session_id",
    "source",
    "source_path",
    "spawn",
    "stderr",
    "stdin",
    "stdout",
    "tock",
    "worker_result",
}
_FORBIDDEN_OUTCOME_FIELDS = {
    "code_authority",
    "consumption",
    "continuation",
    "decision",
    "decisions",
    "exactly_once",
    "harvest",
    "journal",
    "learning",
    "orp",
    "pending",
    "persona",
    "promotion",
    "promotion_authorized",
    "reconciliation",
    "recovery",
    "resume_token",
    "tock",
    "validation",
    "worker_result",
}
_AUTHORITY_MODULES = (
    "task_transition_runner.cli",
    "task_transition_runner.worker_result",
    "task_transition_runner.decision_contracts",
    "task_transition_runner.task_end_promotion",
    "task_transition_runner.runner_git_promotion",
    "task_transition_runner.runner_continuation_contract",
    "task_transition_runner.runner_promotion_journal",
)
_STRING_CANARIES = (
    _SYNTHETIC_DEVELOPER,
    _SYNTHETIC_TASK_SPEC,
    _CANARY_MODEL,
    _CANARY_AUTH,
    _CANARY_ENV_NAME,
    _CANARY_ENV_VALUE,
    _CANARY_ENDPOINT,
    _CANARY_TOOL,
    _CANARY_RESULT,
    _CANARY_ASSISTANT,
    _CANARY_STDERR,
    _CANARY_ARGV,
    _SESSION_ID,
    _OTHER_CWD,
)
_SCRIPT = r"""
import json
import os
import signal
import sys
import time
from pathlib import Path

_SCENARIO = "synthetic-oss5-adapter-scenario.json"
_IDENTITY = "synthetic-oss5-adapter-identity.json"
_STDOUT_PAYLOAD = "synthetic-oss5-adapter-stdout.bin"
_STDERR_PAYLOAD = "synthetic-oss5-adapter-stderr.bin"


def _write_identity() -> None:
    payload = {
        "pid": os.getpid(),
        "pgid": os.getpgid(0),
        "sid": os.getsid(0),
        "cwd": os.getcwd(),
        "stdin_eof": os.read(0, 1) == b"",
    }
    path = Path(_IDENTITY)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    fd = os.open(tmp, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    tmp.replace(path)


def main() -> None:
    scenario = json.loads(Path(_SCENARIO).read_text(encoding="utf-8"))
    if scenario.get("ignore_term"):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _write_identity()
    stdout_path = Path(_STDOUT_PAYLOAD)
    stderr_path = Path(_STDERR_PAYLOAD)
    if stdout_path.is_file():
        os.write(1, stdout_path.read_bytes())
    if stderr_path.is_file():
        os.write(2, stderr_path.read_bytes())
    if scenario.get("signal_exit"):
        os.kill(os.getpid(), signal.SIGUSR1)
    sleep_seconds = float(scenario.get("sleep_seconds", 0))
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    os._exit(int(scenario.get("exit_code", 0)))


if __name__ == "__main__":
    main()
"""


class _Roots:
    def __init__(
        self,
        *,
        runtime_root: Path,
        target_root: Path,
        runner_source_root: Path,
        executable: Path,
        attempt_token: str,
        developer: BoundPromptBytes,
        task_spec: BoundPromptBytes,
        prompt_frame: bytes,
    ) -> None:
        self.runtime_root = runtime_root
        self.target_root = target_root
        self.runner_source_root = runner_source_root
        self.executable = executable
        self.attempt_token = attempt_token
        self.developer = developer
        self.task_spec = task_spec
        self.prompt_frame = prompt_frame

    def bundle_dir(self) -> Path:
        return self.runtime_root / self.attempt_token

    def snippets(self) -> tuple[str, ...]:
        return (
            str(self.runtime_root),
            str(self.target_root),
            str(self.runner_source_root),
            str(self.executable),
            *_STRING_CANARIES,
        )


def _linux_or_fail() -> None:
    if sys.platform != "linux":
        pytest.fail("adapter integration contract requires Linux; no platform fallback")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _bound(payload: bytes) -> BoundPromptBytes:
    return BoundPromptBytes(
        payload_bytes=payload,
        sha256=_sha256(payload),
        byte_length=len(payload),
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
    cwd: str,
    session_id: str | None = _SESSION_ID,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "system",
        "subtype": "init",
        "cwd": cwd,
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _user(
    *,
    frame: bytes,
    session_id: str | None = _SESSION_ID,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "user",
        "message": {
            "content": [
                {
                    "type": "text",
                    "text": frame.decode("utf-8"),
                }
            ]
        },
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _assistant(*, session_id: str | None = _SESSION_ID) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": _CANARY_ASSISTANT}]},
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _tool(*, session_id: str | None = _SESSION_ID) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "tool_call",
        "subtype": "started",
        "synthetic_tool_name": _CANARY_TOOL,
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _result(
    *,
    session_id: str | None = _SESSION_ID,
    subtype: str = "success",
    is_error: bool = False,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "result",
        "subtype": subtype,
        "is_error": is_error,
        "result": _CANARY_RESULT,
    }
    if session_id is not None:
        event["session_id"] = session_id
    return event


def _unknown() -> dict[str, Any]:
    return {"type": "synthetic_future_event", "payload": "synthetic-unknown-must-not-authorize"}


def _success_stream(roots: _Roots) -> bytes:
    cwd = str(roots.target_root.resolve())
    return _ndjson(
        _init(cwd=cwd),
        _user(frame=roots.prompt_frame),
        _unknown(),
        _assistant(),
        _tool(),
        _result(),
    )


def _write_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!{sys.executable}\n{_SCRIPT}", encoding="utf-8")
    path.chmod(0o755)
    return path


def _layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Roots:
    _linux_or_fail()
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    runtime_root.mkdir()
    target_root.mkdir()
    runner_source_root.mkdir()
    executable = _write_executable(tmp_path / "bin" / _SCRIPT_NAME)
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    monkeypatch.setenv(_CANARY_ENV_NAME, _CANARY_ENV_VALUE)
    monkeypatch.setenv("SYNTHETIC_OSS5_CANARY_AUTH", _CANARY_AUTH)
    developer = _bound(_SYNTHETIC_DEVELOPER.encode("utf-8"))
    task_spec = _bound(_SYNTHETIC_TASK_SPEC.encode("utf-8"))
    prompt_frame = serialize_prompt_frame(developer=developer, task_spec=task_spec)
    return _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
        executable=executable,
        attempt_token=_ATTEMPT,
        developer=developer,
        task_spec=task_spec,
        prompt_frame=prompt_frame,
    )


def _write_scenario(roots: _Roots, scenario: dict[str, object]) -> None:
    (roots.target_root / _SCENARIO_NAME).write_text(
        json.dumps(scenario, separators=(",", ":")),
        encoding="utf-8",
    )


def _write_payloads(
    roots: _Roots,
    *,
    stdout: bytes | None = None,
    stderr: bytes | None = None,
) -> None:
    if stdout is not None:
        (roots.target_root / _STDOUT_PAYLOAD_NAME).write_bytes(stdout)
    if stderr is not None:
        (roots.target_root / _STDERR_PAYLOAD_NAME).write_bytes(stderr)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _identity(roots: _Roots) -> dict[str, Any]:
    path = roots.target_root / _IDENTITY_NAME
    assert path.is_file()
    return json.loads(path.read_text(encoding="utf-8"))


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _field_names(payload: object) -> set[str]:
    if isinstance(payload, Mapping):
        return set(payload)
    fields = getattr(payload, "__dataclass_fields__", None)
    if fields:
        return set(fields)
    raw = getattr(payload, "__dict__", None)
    if isinstance(raw, dict):
        return set(raw)
    return set()


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
        default=str,
    )


def _assert_canaries_absent(value: object, extra: tuple[str, ...] = ()) -> None:
    blobs = [_serialized(value), repr(value)]
    for node in _walk(value):
        if isinstance(node, bytes):
            blobs.append(node.decode("utf-8", "replace"))
        else:
            blobs.append(str(node))
    haystack = "\n".join(blobs)
    for canary in _STRING_CANARIES + extra:
        assert canary not in haystack
    assert "prompt_frame_matches" not in _serialized(value)


def _assert_no_authority_fields(payload: object) -> None:
    names = _field_names(payload)
    assert names.isdisjoint(_FORBIDDEN_OUTCOME_FIELDS)
    for name in _FORBIDDEN_OUTCOME_FIELDS:
        assert not hasattr(payload, name)


def _projection(outcome: Any) -> dict[str, Any]:
    projection = outcome.publish_projection
    assert isinstance(projection, dict)
    assert tuple(projection.keys()) == _PUBLISHABLE_KEYS
    _assert_canaries_absent(projection)
    _assert_no_authority_fields(projection)
    return projection


def _expect_reason(
    reason: str,
    action: Callable[[], object],
    *,
    snippets: tuple[str, ...] = (),
) -> BaseException:
    with pytest.raises(Exception) as exc_info:
        action()
    exc = exc_info.value
    message = str(exc)
    assert getattr(exc, "reason_code") == reason
    assert message == reason
    for snippet in snippets:
        assert snippet not in message
        assert snippet not in repr(exc)
    return exc


def _run(
    roots: _Roots,
    *,
    configured_executable: str | None = None,
    developer: BoundPromptBytes | None = None,
    task_spec: BoundPromptBytes | None = None,
    timeout_seconds: int = 5,
    model: str | None = None,
    attempt_token: str | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "configured_executable": configured_executable or str(roots.executable),
        "target_root": roots.target_root,
        "runner_source_root": roots.runner_source_root,
        "runtime_root": roots.runtime_root,
        "developer": developer if developer is not None else roots.developer,
        "task_spec": task_spec if task_spec is not None else roots.task_spec,
        "attempt_token": attempt_token or roots.attempt_token,
        "timeout_seconds": timeout_seconds,
    }
    if model is not None:
        kwargs["model"] = model
    return run_cursor_adapter(**kwargs)


def _prepare_fake(
    roots: _Roots,
    *,
    stdout: bytes | None = None,
    stderr: bytes | None = None,
    exit_code: int = 0,
    sleep_seconds: float = 0,
    ignore_term: bool = False,
    signal_exit: bool = False,
) -> None:
    _write_scenario(
        roots,
        {
            "exit_code": exit_code,
            "sleep_seconds": sleep_seconds,
            "ignore_term": ignore_term,
            "signal_exit": signal_exit,
        },
    )
    _write_payloads(roots, stdout=stdout, stderr=stderr)


def _install_spawn_spy(
    monkeypatch: pytest.MonkeyPatch,
    roots: _Roots,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    real = cursor_headless_mod.subprocess.Popen

    def spy(*args: Any, **kwargs: Any) -> Any:
        directory = roots.bundle_dir()
        events.append(
            {
                "cwd": kwargs.get("cwd"),
                "shell": kwargs.get("shell"),
                "stdin": kwargs.get("stdin"),
                "start_new_session": kwargs.get("start_new_session"),
                "bundle_exists": directory.is_dir(),
                "stdout_part": (directory / "stdout.part").is_file(),
                "stderr_part": (directory / "stderr.part").is_file(),
                "stdout_final": (directory / "stdout").exists(),
                "stderr_final": (directory / "stderr").exists(),
            }
        )
        return real(*args, **kwargs)

    monkeypatch.setattr(cursor_headless_mod.subprocess, "Popen", spy)
    return events


def _assert_not_spawned(roots: _Roots, spawn_events: list[dict[str, object]]) -> None:
    assert spawn_events == []
    assert not (roots.target_root / _IDENTITY_NAME).exists()
    assert not roots.bundle_dir().exists()


def _assert_finalized(roots: _Roots) -> None:
    directory = roots.bundle_dir()
    stdout_final = directory / "stdout"
    stderr_final = directory / "stderr"
    assert directory.is_dir()
    assert _mode(directory) == 0o700
    assert stdout_final.is_file()
    assert stderr_final.is_file()
    assert not stdout_final.is_symlink()
    assert not stderr_final.is_symlink()
    assert _mode(stdout_final) == 0o600
    assert _mode(stderr_final) == 0o600
    assert not (directory / "stdout.part").exists()
    assert not (directory / "stderr.part").exists()


def _assert_reaped(roots: _Roots) -> None:
    identity = _identity(roots)
    assert _pid_alive(int(identity["pid"])) is False


def _assert_parts_retained_without_finals(roots: _Roots) -> None:
    directory = roots.bundle_dir()
    assert directory.is_dir()
    assert (directory / "stdout.part").is_file()
    assert (directory / "stderr.part").is_file()
    assert not (directory / "stdout").exists()
    assert not (directory / "stderr").exists()


def _in_read_owned_part() -> bool:
    frame = inspect.currentframe()
    try:
        current = None if frame is None else frame.f_back
        while current is not None:
            if current.f_code.co_name == "_read_owned_part":
                return True
            current = current.f_back
        return False
    finally:
        del frame


def _is_post_capture_stdout_part_open(
    path: str | bytes | os.PathLike[str],
    flags: int,
    dir_fd: int | None,
) -> bool:
    return (
        os.fsdecode(path) == "stdout.part"
        and dir_fd is not None
        and flags == cursor_headless_mod._PART_READ_FLAGS
        and _in_read_owned_part()
    )


@pytest.fixture(autouse=True)
def _linux_contract() -> None:
    _linux_or_fail()


def test_run_cursor_adapter_binds_keyword_only_request_without_authority_parameters() -> None:
    parameters = inspect.signature(run_cursor_adapter).parameters
    required = {
        "configured_executable",
        "target_root",
        "runner_source_root",
        "runtime_root",
        "developer",
        "task_spec",
        "attempt_token",
        "timeout_seconds",
    }
    allowed = required | {"model"}
    assert required <= set(parameters)
    assert set(parameters) <= allowed
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)
    for name, parameter in parameters.items():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        if name == "model":
            continue
        assert parameter.default is inspect.Parameter.empty


def test_pre_spawn_binding_mismatch_short_circuits_before_reservation_or_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    mismatched = BoundPromptBytes(
        payload_bytes=roots.developer.payload_bytes,
        sha256="0" * 64,
        byte_length=roots.developer.byte_length,
    )
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_BINDING,
        lambda: _run(roots, developer=mismatched),
        snippets=roots.snippets(),
    )
    _assert_not_spawned(roots, spawn_events)


def test_pre_spawn_nul_short_circuits_before_reservation_or_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    nulled = _bound(b"synthetic\x00nul")
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_NUL,
        lambda: _run(roots, developer=nulled),
        snippets=roots.snippets(),
    )
    _assert_not_spawned(roots, spawn_events)


def test_pre_spawn_invalid_utf8_short_circuits_before_reservation_or_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    invalid = _bound(b"\xff")
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_ENCODING,
        lambda: _run(roots, task_spec=invalid),
        snippets=roots.snippets(),
    )
    _assert_not_spawned(roots, spawn_events)


def test_pre_spawn_executable_not_found_short_circuits_before_reservation_or_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    missing = tmp_path / "missing-cursor-binary"
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_NOT_FOUND,
        lambda: _run(roots, configured_executable=str(missing)),
        snippets=roots.snippets(),
    )
    _assert_not_spawned(roots, spawn_events)


def test_pre_spawn_endpoint_override_short_circuits_before_reservation_or_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    monkeypatch.setenv("CURSOR_API_ENDPOINT", _CANARY_ENDPOINT)
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_INVALID,
        lambda: _run(roots),
        snippets=roots.snippets(),
    )
    _assert_not_spawned(roots, spawn_events)


def test_pre_spawn_evidence_conflict_does_not_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    roots.bundle_dir().mkdir()
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_EVIDENCE_CONFLICT,
        lambda: _run(roots),
        snippets=roots.snippets(),
    )
    assert spawn_events == []
    assert not (roots.target_root / _IDENTITY_NAME).exists()


def test_pre_spawn_reserves_bundle_and_unfinalized_parts_before_popen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    _prepare_fake(roots, stdout=_success_stream(roots))
    outcome = _run(roots)
    assert len(spawn_events) == 1
    observed = spawn_events[0]
    assert observed["bundle_exists"] is True
    assert observed["stdout_part"] is True
    assert observed["stderr_part"] is True
    assert observed["stdout_final"] is False
    assert observed["stderr_final"] is False
    assert observed["cwd"] == str(roots.target_root.resolve())
    assert observed["shell"] is False
    assert observed["stdin"] is subprocess.DEVNULL
    assert observed["start_new_session"] is True
    _projection(outcome)
    _assert_finalized(roots)


def test_success_binds_byte_identical_single_user_event_and_init_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _success_stream(roots)
    _prepare_fake(roots, stdout=stdout)
    outcome = _run(roots)
    identity = _identity(roots)
    projection = _projection(outcome)
    assert identity["cwd"] == str(roots.target_root.resolve())
    assert identity["stdin_eof"] is True
    assert identity["sid"] != os.getsid(0)
    assert outcome.prompt_frame_matches is True
    assert projection["adapter_status"] == "completed"
    assert projection["neutral_reason_code"] == _REASON_COMPLETED
    assert projection["process_exit_code"] == 0
    assert projection["timeout"] is False
    assert projection["interrupted"] is False
    assert projection["session_observed"] is True
    assert projection["terminal_success"] is True
    assert projection["event_counts"]["user"] == 1
    assert projection["event_counts"]["system"] == 1
    assert projection["event_counts"]["result"] == 1
    assert projection["event_counts"]["assistant"] == 1
    assert projection["event_counts"]["tool_call"] == 1
    assert projection["event_counts"]["unknown"] == 1
    user_records = [
        json.loads(line) for line in stdout.decode("utf-8").splitlines() if line
    ]
    user_events = [event for event in user_records if event.get("type") == "user"]
    assert len(user_events) == 1
    user_text = user_events[0]["message"]["content"][0]["text"]
    assert user_text.encode("utf-8") == roots.prompt_frame
    init_event = user_records[0]
    assert init_event["type"] == "system"
    assert init_event["subtype"] == "init"
    assert init_event["cwd"] == str(roots.target_root.resolve())
    _assert_reaped(roots)


def test_success_finalizes_evidence_before_returning_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, stdout=_success_stream(roots))
    outcome = _run(roots)
    _assert_finalized(roots)
    _projection(outcome)
    _assert_no_authority_fields(outcome)
    assert "prompt_frame_matches" not in _projection(outcome)
    _assert_reaped(roots)


def test_success_publish_projection_drops_prompt_spec_argv_path_model_auth_and_stderr_canaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(
        roots,
        stdout=_success_stream(roots),
        stderr=_CANARY_STDERR.encode("utf-8"),
    )
    outcome = _run(roots, model=_CANARY_MODEL)
    projection = _projection(outcome)
    _assert_canaries_absent(projection, extra=(str(roots.executable), str(roots.target_root)))
    _assert_canaries_absent(outcome.publish_projection)
    assert "prompt_frame_matches" not in projection
    assert "session_id" not in json.dumps(projection)
    _assert_no_authority_fields(outcome)
    _assert_reaped(roots)


def test_success_outcome_has_no_worker_result_decision_promotion_or_recovery_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    before = set(sys.modules)
    _prepare_fake(roots, stdout=_success_stream(roots))
    outcome = _run(roots)
    _assert_no_authority_fields(outcome)
    _assert_no_authority_fields(_projection(outcome))
    loaded = set(sys.modules) - before
    for module_name in _AUTHORITY_MODULES:
        assert module_name not in loaded
        assert module_name not in sys.modules or module_name in before
    _assert_reaped(roots)


def test_success_prompt_frame_matches_stays_local_and_unpublished(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, stdout=_success_stream(roots))
    outcome = _run(roots)
    assert outcome.prompt_frame_matches is True
    projection = _projection(outcome)
    assert "prompt_frame_matches" not in projection
    assert "prompt_frame_matches" not in json.dumps(projection)
    _assert_reaped(roots)


def test_success_keeps_exit_zero_with_stderr_and_still_finalizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(
        roots,
        stdout=_success_stream(roots),
        stderr=_CANARY_STDERR.encode("utf-8"),
    )
    outcome = _run(roots)
    projection = _projection(outcome)
    assert projection["process_exit_code"] == 0
    assert projection["neutral_reason_code"] == _REASON_COMPLETED
    _assert_canaries_absent(projection)
    _assert_finalized(roots)
    _assert_reaped(roots)


def test_nonzero_exit_with_success_shaped_stream_is_exit_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    spawn_events = _install_spawn_spy(monkeypatch, roots)
    _prepare_fake(roots, stdout=_success_stream(roots), exit_code=7)
    _expect_reason(_REASON_EXIT, lambda: _run(roots), snippets=roots.snippets())
    assert spawn_events
    assert not (roots.bundle_dir() / "stdout").exists()
    _assert_reaped(roots)


def test_nonzero_exit_with_malformed_stream_is_exit_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, stdout=b"{this is not json\n", exit_code=2)
    _expect_reason(_REASON_EXIT, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_negative_signal_return_is_exit_nonzero_before_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, stdout=_success_stream(roots), signal_exit=True)
    _expect_reason(_REASON_EXIT, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_empty_stream_on_exit_zero_is_stream_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, exit_code=0)
    _expect_reason(_REASON_EMPTY, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_init_cwd_mismatch_is_workspace_mismatch_after_exit_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=_OTHER_CWD),
        _user(frame=roots.prompt_frame),
        _result(),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_WORKSPACE, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_user_event_byte_mismatch_is_envelope_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        _user(frame=b"synthetic-mismatched-frame"),
        _result(),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_ENVELOPE, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_timeout_before_output_beats_empty_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, sleep_seconds=120)
    _expect_reason(
        _REASON_TIMEOUT,
        lambda: _run(roots, timeout_seconds=1),
        snippets=roots.snippets(),
    )
    _assert_reaped(roots)


def test_timeout_after_apparent_terminal_still_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _prepare_fake(roots, stdout=_success_stream(roots), sleep_seconds=120)
    _expect_reason(
        _REASON_TIMEOUT,
        lambda: _run(roots, timeout_seconds=1),
        snippets=roots.snippets(),
    )
    _assert_reaped(roots)


def test_termination_failure_overrides_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real_kill = os.kill
    real_killpg = os.killpg

    def swallow_kill(pid: int, sig: int) -> None:
        if sig in (signal.SIGKILL, 9):
            return
        real_kill(pid, sig)

    def swallow_killpg(pgid: int, sig: int) -> None:
        if sig in (signal.SIGKILL, 9):
            return
        real_killpg(pgid, sig)

    monkeypatch.setattr(os, "kill", swallow_kill)
    monkeypatch.setattr(os, "killpg", swallow_killpg)
    _prepare_fake(roots, sleep_seconds=120, ignore_term=True)
    _expect_reason(
        _REASON_TERM,
        lambda: _run(roots, timeout_seconds=1),
        snippets=roots.snippets(),
    )


def test_interrupt_beats_success_shaped_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity = roots.target_root / _IDENTITY_NAME
    real_monotonic = time.monotonic

    def interrupting_monotonic() -> float:
        if identity.is_file():
            raise KeyboardInterrupt
        return real_monotonic()

    monkeypatch.setattr(time, "monotonic", interrupting_monotonic)
    _prepare_fake(roots, stdout=_success_stream(roots), sleep_seconds=30)
    _expect_reason(
        _REASON_INTERRUPT,
        lambda: _run(roots, timeout_seconds=30),
        snippets=roots.snippets(),
    )
    _assert_reaped(roots)


def test_stdout_limit_beats_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    overflow = b"A" * (67108864 + 1)
    _prepare_fake(roots, stdout=overflow, exit_code=3)
    _expect_reason(_REASON_LIMIT, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_capture_stdout_io_beats_stderr_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    from task_transition_runner.adapters import cursor_stream as cursor_stream_mod

    def write_part(self: Any, *, part: Any, payload: bytes) -> None:
        del self, payload
        name = getattr(part, "basename", "")
        if name == "stdout.part":
            raise OSError("synthetic-stdout-io")
        raise OSError("synthetic-stderr-io")

    monkeypatch.setattr(cursor_stream_mod._EvidenceBundle, "write_part", write_part)
    _prepare_fake(
        roots,
        stdout=_success_stream(roots),
        stderr=_CANARY_STDERR.encode("utf-8"),
    )
    _expect_reason(_REASON_STDOUT_IO, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_capture_stderr_io_beats_stream_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    from task_transition_runner.adapters import cursor_stream as cursor_stream_mod

    original_write = cursor_stream_mod._EvidenceBundle.write_part

    def write_part(self: Any, *, part: Any, payload: bytes) -> None:
        name = getattr(part, "basename", "")
        if name == "stderr.part":
            raise OSError("synthetic-stderr-io")
        return original_write(self, part=part, payload=payload)

    monkeypatch.setattr(cursor_stream_mod._EvidenceBundle, "write_part", write_part)
    overflow = b"B" * (8388608 + 1)
    _prepare_fake(roots, stdout=_success_stream(roots), stderr=overflow)
    _expect_reason(_REASON_STDERR_IO, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_stream_utf8_invalid_beats_json_event_and_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        b"\xff",
        b"{",
        _user(frame=roots.prompt_frame),
        _result(),
        final_lf=False,
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_UTF8, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_stream_json_invalid_beats_event_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        b"{not json}",
        b"",
        _user(frame=roots.prompt_frame),
        _result(),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_JSON, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_stream_event_invalid_beats_envelope_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _user(frame=roots.prompt_frame),
        _init(cwd=str(roots.target_root.resolve())),
        _result(),
        final_lf=False,
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_EVENT, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_session_missing_beats_session_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve()), session_id=None),
        _user(frame=roots.prompt_frame),
        _result(session_id=_OTHER_SESSION_ID),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_SESSION_MISSING, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_session_conflict_beats_terminal_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        _user(frame=roots.prompt_frame, session_id=_OTHER_SESSION_ID),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_SESSION_CONFLICT, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_terminal_missing_beats_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        _user(frame=roots.prompt_frame),
        _assistant(),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_TERMINAL_MISSING, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_terminal_duplicate_beats_terminal_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        _user(frame=roots.prompt_frame),
        _result(is_error=True),
        _result(),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_TERMINAL_DUPLICATE, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_terminal_failure_beats_evidence_and_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    from task_transition_runner.adapters import cursor_stream as cursor_stream_mod

    def fail_publish(self: Any, *, part: Any, final_basename: str) -> None:
        del self, part, final_basename
        raise AdapterEvidenceError(_REASON_EVIDENCE_IO)

    def fail_projection(*, local_facts: object) -> dict[str, Any]:
        del local_facts
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)

    monkeypatch.setattr(cursor_stream_mod._EvidenceBundle, "publish", fail_publish)
    monkeypatch.setattr(cursor_stream_mod, "build_publish_projection", fail_projection)
    stdout = _ndjson(
        _init(cwd=str(roots.target_root.resolve())),
        _user(frame=roots.prompt_frame),
        _result(subtype="error", is_error=True),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_TERMINAL_FAILURE, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_workspace_mismatch_beats_session_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=_OTHER_CWD, session_id=None),
        _user(frame=roots.prompt_frame),
        _result(),
    )
    _prepare_fake(roots, stdout=stdout)
    _expect_reason(_REASON_WORKSPACE, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_exit_nonzero_beats_workspace_and_stream_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout = _ndjson(
        _init(cwd=_OTHER_CWD),
        b"\xff",
        _user(frame=b"mismatch"),
        _result(is_error=True),
    )
    _prepare_fake(roots, stdout=stdout, exit_code=9)
    _expect_reason(_REASON_EXIT, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_evidence_finalization_failure_beats_projection_unsafe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    from task_transition_runner.adapters import cursor_stream as cursor_stream_mod

    def fail_publish(self: Any, *, part: Any, final_basename: str) -> None:
        del self, part, final_basename
        raise AdapterEvidenceError(_REASON_EVIDENCE_IO)

    monkeypatch.setattr(cursor_stream_mod._EvidenceBundle, "publish", fail_publish)

    def fail_projection(*, local_facts: object) -> dict[str, Any]:
        del local_facts
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)

    monkeypatch.setattr(cursor_stream_mod, "build_publish_projection", fail_projection)
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(_REASON_EVIDENCE_IO, lambda: _run(roots), snippets=roots.snippets())
    _assert_reaped(roots)


def test_projection_unsafe_is_last_and_blocks_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    from task_transition_runner.adapters import cursor_stream as cursor_stream_mod

    def fail_projection(*, local_facts: object) -> dict[str, Any]:
        del local_facts
        raise AdapterProjectionError(_REASON_PROJECTION_UNSAFE)

    monkeypatch.setattr(cursor_stream_mod, "build_publish_projection", fail_projection)
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_PROJECTION_UNSAFE,
        lambda: _run(roots),
        snippets=roots.snippets(),
    )
    _assert_reaped(roots)


def test_success_is_not_emitted_when_final_stdout_preexists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real = cursor_headless_mod.subprocess.Popen
    spawned = {"n": 0}

    def spy(*args: Any, **kwargs: Any) -> Any:
        spawned["n"] += 1
        directory = roots.bundle_dir()
        assert directory.is_dir()
        assert (directory / "stdout.part").is_file()
        assert (directory / "stderr.part").is_file()
        (directory / "stdout").write_bytes(b"preexisting-final")
        return real(*args, **kwargs)

    monkeypatch.setattr(cursor_headless_mod.subprocess, "Popen", spy)
    _prepare_fake(roots, stdout=_success_stream(roots))
    _expect_reason(
        _REASON_EVIDENCE_CONFLICT,
        lambda: _run(roots),
        snippets=roots.snippets(),
    )
    assert spawned["n"] == 1
    leftover = roots.bundle_dir() / "stdout"
    assert leftover.read_bytes() == b"preexisting-final"
    _assert_reaped(roots)


def test_post_capture_owned_stdout_part_open_oserror_is_evidence_io_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-owned-part-open"
    real_open = os.open
    fired = {"n": 0}

    def failing_open(
        path: str | bytes | os.PathLike[str],
        flags: int,
        mode: int = 0o777,
        *args: Any,
        dir_fd: int | None = None,
        **kwargs: Any,
    ) -> int:
        if _is_post_capture_stdout_part_open(path, flags, dir_fd):
            fired["n"] += 1
            raise OSError(errno.EIO, io_secret)
        return real_open(path, flags, mode, *args, dir_fd=dir_fd, **kwargs)

    monkeypatch.setattr(os, "open", failing_open)
    _prepare_fake(roots, stdout=_success_stream(roots))
    try:
        _run(roots)
    except OSError:
        assert fired["n"] == 1
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        pytest.fail(
            "owned stdout.part open OSError escaped without ADAPTER_EVIDENCE_IO_FAILED"
        )
    except Exception as exc:
        assert fired["n"] == 1
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        assert getattr(exc, "reason_code", None) == _REASON_EVIDENCE_IO
        assert str(exc) == _REASON_EVIDENCE_IO
        assert io_secret not in str(exc)
        assert io_secret not in repr(exc)
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        return
    pytest.fail(
        "owned stdout.part open OSError escaped without ADAPTER_EVIDENCE_IO_FAILED"
    )


def test_post_capture_owned_stdout_part_read_oserror_is_evidence_io_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-owned-part-read"
    real_open = os.open
    real_read = os.read
    owned_fds: set[int] = set()
    fired = {"n": 0}

    def tracking_open(
        path: str | bytes | os.PathLike[str],
        flags: int,
        mode: int = 0o777,
        *args: Any,
        dir_fd: int | None = None,
        **kwargs: Any,
    ) -> int:
        fd = real_open(path, flags, mode, *args, dir_fd=dir_fd, **kwargs)
        if _is_post_capture_stdout_part_open(path, flags, dir_fd):
            owned_fds.add(fd)
        return fd

    def failing_read(fd: int, n: int) -> bytes:
        if fd in owned_fds and _in_read_owned_part():
            fired["n"] += 1
            raise OSError(errno.EIO, io_secret)
        return real_read(fd, n)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "read", failing_read)
    _prepare_fake(roots, stdout=_success_stream(roots))
    try:
        _run(roots)
    except OSError:
        assert fired["n"] == 1
        assert owned_fds
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        pytest.fail(
            "owned stdout.part read OSError escaped without ADAPTER_EVIDENCE_IO_FAILED"
        )
    except Exception as exc:
        assert fired["n"] == 1
        assert owned_fds
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        assert getattr(exc, "reason_code", None) == _REASON_EVIDENCE_IO
        assert str(exc) == _REASON_EVIDENCE_IO
        assert io_secret not in str(exc)
        assert io_secret not in repr(exc)
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        return
    pytest.fail(
        "owned stdout.part read OSError escaped without ADAPTER_EVIDENCE_IO_FAILED"
    )


def test_post_capture_owned_stdout_part_close_oserror_is_evidence_io_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-owned-part-close"
    real_open = os.open
    real_read = os.read
    real_close = os.close
    owned_fds: set[int] = set()
    owned_read_eof = {"n": 0}
    fired = {"n": 0}

    def tracking_open(
        path: str | bytes | os.PathLike[str],
        flags: int,
        mode: int = 0o777,
        *args: Any,
        dir_fd: int | None = None,
        **kwargs: Any,
    ) -> int:
        fd = real_open(path, flags, mode, *args, dir_fd=dir_fd, **kwargs)
        if _is_post_capture_stdout_part_open(path, flags, dir_fd):
            owned_fds.add(fd)
        return fd

    def tracking_read(fd: int, n: int) -> bytes:
        data = real_read(fd, n)
        if fd in owned_fds and _in_read_owned_part() and data == b"":
            owned_read_eof["n"] += 1
        return data

    def failing_close(fd: int) -> None:
        if fd in owned_fds and _in_read_owned_part():
            fired["n"] += 1
            raise OSError(errno.EIO, io_secret)
        real_close(fd)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "read", tracking_read)
    monkeypatch.setattr(os, "close", failing_close)
    _prepare_fake(roots, stdout=_success_stream(roots))
    try:
        _run(roots)
    except OSError:
        assert fired["n"] == 1
        assert owned_fds
        assert owned_read_eof["n"] == 1
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        pytest.fail(
            "owned stdout.part close OSError escaped without ADAPTER_EVIDENCE_IO_FAILED"
        )
    except Exception as exc:
        assert fired["n"] == 1
        assert owned_fds
        assert owned_read_eof["n"] == 1
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        assert getattr(exc, "reason_code", None) == _REASON_EVIDENCE_IO
        assert str(exc) == _REASON_EVIDENCE_IO
        assert io_secret not in str(exc)
        assert io_secret not in repr(exc)
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        return
    pytest.fail(
        "owned stdout.part close OSError escaped without ADAPTER_EVIDENCE_IO_FAILED"
    )


@pytest.mark.parametrize("fault", ("open", "read", "close"))
def test_post_capture_owned_stdout_part_keyboard_interrupt_is_adapter_interrupted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real_open = os.open
    real_read = os.read
    real_close = os.close
    owned_fds: set[int] = set()
    fired = {"n": 0}
    escaped = (
        f"owned stdout.part {fault} KeyboardInterrupt escaped without ADAPTER_INTERRUPTED"
    )

    def interrupting_open(
        path: str | bytes | os.PathLike[str],
        flags: int,
        mode: int = 0o777,
        *args: Any,
        dir_fd: int | None = None,
        **kwargs: Any,
    ) -> int:
        if fault == "open" and _is_post_capture_stdout_part_open(path, flags, dir_fd):
            fired["n"] += 1
            raise KeyboardInterrupt
        fd = real_open(path, flags, mode, *args, dir_fd=dir_fd, **kwargs)
        if _is_post_capture_stdout_part_open(path, flags, dir_fd):
            owned_fds.add(fd)
        return fd

    def interrupting_read(fd: int, n: int) -> bytes:
        if fault == "read" and fd in owned_fds and _in_read_owned_part():
            fired["n"] += 1
            raise KeyboardInterrupt
        return real_read(fd, n)

    def interrupting_close(fd: int) -> None:
        if fault == "close" and fd in owned_fds and _in_read_owned_part():
            fired["n"] += 1
            raise KeyboardInterrupt
        real_close(fd)

    monkeypatch.setattr(os, "open", interrupting_open)
    monkeypatch.setattr(os, "read", interrupting_read)
    monkeypatch.setattr(os, "close", interrupting_close)
    _prepare_fake(roots, stdout=_success_stream(roots))
    try:
        _run(roots)
    except KeyboardInterrupt:
        assert fired["n"] == 1
        if fault != "open":
            assert owned_fds
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        pytest.fail(escaped)
    except Exception as exc:
        assert fired["n"] == 1
        if fault != "open":
            assert owned_fds
        _assert_parts_retained_without_finals(roots)
        _assert_reaped(roots)
        assert getattr(exc, "reason_code", None) == _REASON_INTERRUPT
        assert str(exc) == _REASON_INTERRUPT
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        return
    pytest.fail(escaped)


def test_spawn_passes_exact_prompt_frame_once_as_final_positional_argv_without_retention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    comparisons: list[bool] = []
    real = cursor_headless_mod.subprocess.Popen

    def spy(*args: Any, **kwargs: Any) -> Any:
        argv = args[0] if args else kwargs.get("args")
        frame_text = roots.prompt_frame.decode("utf-8")
        last_is_exact_frame = (
            isinstance(argv, (list, tuple))
            and len(argv) >= 1
            and argv[-1] == frame_text
            and sum(1 for item in argv if item == frame_text) == 1
        )
        comparisons.append(last_is_exact_frame)
        return real(*args, **kwargs)

    monkeypatch.setattr(cursor_headless_mod.subprocess, "Popen", spy)
    _prepare_fake(roots, stdout=_success_stream(roots))
    outcome = _run(roots)
    assert comparisons == [True]
    assert all(item is True for item in comparisons)
    _projection(outcome)
    _assert_canaries_absent(comparisons)
    _assert_finalized(roots)
    _assert_reaped(roots)


def test_nonempty_pending_batch_is_retained_to_eof_on_stdout_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    pending = b"synthetic-pending-batch"
    stdout_fd = {"value": None}
    phase = {"pending_seen": False, "overflow_injected": False}
    real = cursor_headless_mod.subprocess.Popen
    real_read = os.read

    class _OversizeChunk(bytes):
        def __len__(self) -> int:
            return cursor_headless_mod._STDOUT_LIMIT + 1

    def spy(*args: Any, **kwargs: Any) -> Any:
        proc = real(*args, **kwargs)
        stdout_fd["value"] = proc.stdout.fileno()
        return proc

    def scripted_read(fd: int, n: int) -> bytes:
        if stdout_fd["value"] is None or fd != stdout_fd["value"]:
            return real_read(fd, n)
        data = real_read(fd, n)
        if data != b"":
            phase["pending_seen"] = True
            return data
        if phase["pending_seen"] and not phase["overflow_injected"]:
            phase["overflow_injected"] = True
            return _OversizeChunk(b"\x01")
        return data

    monkeypatch.setattr(cursor_headless_mod.subprocess, "Popen", spy)
    monkeypatch.setattr(os, "read", scripted_read)
    assert cursor_headless_mod._STDOUT_LIMIT == 67108864
    assert cursor_headless_mod._STDERR_LIMIT == 8388608
    assert cursor_headless_mod._WRITE_BATCH == 1048576
    assert cursor_headless_mod._READ_SIZE == 65536
    _prepare_fake(roots, stdout=pending)
    _expect_reason(_REASON_LIMIT, lambda: _run(roots), snippets=roots.snippets())
    assert phase == {"pending_seen": True, "overflow_injected": True}
    _assert_parts_retained_without_finals(roots)
    assert (roots.bundle_dir() / "stdout.part").read_bytes() == pending
    _assert_reaped(roots)
