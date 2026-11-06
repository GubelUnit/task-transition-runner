"""Cursor executable resolution, argv policy, and process-group capture.

Resolve exactly the configured absolute path or one PATH basename.
Ignore empty PATH components. Follow a valid executable symlink once to
its canonical regular target. Build the ratified non-shell argv with
Target as cwd and --workspace. Spawn one new Linux session, drain stdout
and stderr concurrently into owned parts, and reap the full process
group. Compose those cold APIs into one keyword-only adapter run that
appends the UTF-8 prompt frame as the final positional argv item. This
module does not copy, hash, persist, or project Environment, argv, or
the prompt frame.
"""

from __future__ import annotations

import os
import signal
import stat
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any

from task_transition_runner.adapters import cursor_stream
from task_transition_runner.adapters.cursor_stream import BoundPromptBytes

_REASON_NOT_FOUND = "ADAPTER_EXECUTABLE_NOT_FOUND"
_REASON_NOT_EXECUTABLE = "ADAPTER_EXECUTABLE_NOT_EXECUTABLE"
_REASON_INVALID = "ADAPTER_INVOCATION_INVALID"
_REASON_WORKSPACE = "ADAPTER_WORKSPACE_MISMATCH"
_REASON_SPAWN = "ADAPTER_SPAWN_FAILED"
_REASON_EXIT = "ADAPTER_PROCESS_EXIT_NONZERO"
_REASON_TIMEOUT = "ADAPTER_TIMEOUT"
_REASON_INTERRUPT = "ADAPTER_INTERRUPTED"
_REASON_TERM = "ADAPTER_TERMINATION_FAILED"
_REASON_STDOUT_IO = "ADAPTER_STDOUT_IO_FAILED"
_REASON_STDERR_IO = "ADAPTER_STDERR_IO_FAILED"
_REASON_LIMIT = "ADAPTER_STREAM_LIMIT_EXCEEDED"
_REASON_EVIDENCE_IO = "ADAPTER_EVIDENCE_IO_FAILED"
_MODE_ABSOLUTE = "absolute"
_MODE_PATH = "path"
_ENDPOINT_NAME = "CURSOR_API_ENDPOINT"
_FIXED_FLAGS = (
    "--trust",
    "--print",
    "--force",
    "--output-format=stream-json",
    "--disable-auto-update",
)
_STDOUT_LIMIT = 67108864
_STDERR_LIMIT = 8388608
_TERM_GRACE_SECONDS = 5
_KILL_WAIT_SECONDS = 1
_POLL_SECONDS = 0.05
_READ_SIZE = 65536
_WRITE_BATCH = 1048576
_JOIN_TIMEOUT_SECONDS = 1.0
_STDOUT_PART_NAME = "stdout.part"
_STDERR_PART_NAME = "stderr.part"
_STDOUT_FINAL_NAME = "stdout"
_STDERR_FINAL_NAME = "stderr"
_ADAPTER_COMPLETED = "ADAPTER_COMPLETED"
_ADAPTER_STATUS_COMPLETED = "completed"
_PART_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC


class CursorInvocationError(Exception):
    """Fail-closed violation of the pre-spawn invocation contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class ResolvedCursorExecutable:
    path: str
    resolution_mode: str


@dataclass(frozen=True)
class CursorArgvPlan:
    argv: tuple[str, ...]
    cwd: str
    shell: bool
    stdin: Any


@dataclass(frozen=True)
class CursorProcessResult:
    process_exit_code: int
    timeout: bool
    interrupted: bool
    stdout_byte_length: int
    stderr_byte_length: int


@dataclass(frozen=True)
class CursorAdapterOutcome:
    publish_projection: dict[str, Any]
    prompt_frame_matches: bool
    stdout_artifact: str
    stderr_artifact: str


class _CaptureState:
    def __init__(self) -> None:
        self.stdout_bytes = 0
        self.stderr_bytes = 0
        self.stdout_io = False
        self.stderr_io = False
        self.limit = False
        self.interrupted = False
        self.timeout = False
        self.term_failed = False


def resolve_cursor_executable(*, configured_executable: str) -> ResolvedCursorExecutable:
    _reject_nul(configured_executable)
    if configured_executable.strip() == "":
        raise CursorInvocationError(_REASON_INVALID)
    if os.path.isabs(configured_executable):
        return _resolve_absolute(configured_executable)
    if os.sep in configured_executable:
        raise CursorInvocationError(_REASON_INVALID)
    return _resolve_path_basename(configured_executable)


def build_cursor_argv(
    *,
    executable_path: str,
    target_root: object,
    runner_source_root: object,
    runtime_root: object,
    model: str | None = None,
) -> CursorArgvPlan:
    executable = os.fspath(executable_path)
    _reject_nul(executable)
    if not os.path.isabs(executable):
        raise CursorInvocationError(_REASON_INVALID)
    if model is not None:
        _reject_nul(model)
    target = _root_text(target_root)
    source = _root_text(runner_source_root)
    runtime = _root_text(runtime_root)
    if _endpoint_present():
        raise CursorInvocationError(_REASON_INVALID)
    target_abs = os.path.abspath(target)
    source_abs = os.path.abspath(source)
    runtime_abs = os.path.abspath(runtime)
    if _has_symlink_component(target_abs):
        raise CursorInvocationError(_REASON_INVALID)
    if not os.path.isdir(target_abs):
        raise CursorInvocationError(_REASON_INVALID)
    if _roots_overlap(target_abs, source_abs) or _roots_overlap(target_abs, runtime_abs):
        raise CursorInvocationError(_REASON_WORKSPACE)
    argv = (
        os.path.abspath(executable),
        *_FIXED_FLAGS,
        f"--workspace={target_abs}",
    )
    if model is not None and model.strip() != "":
        argv = (*argv, f"--model={model}")
    return CursorArgvPlan(
        argv=argv,
        cwd=target_abs,
        shell=False,
        stdin=subprocess.DEVNULL,
    )


def execute_cursor_process(
    *,
    plan: CursorArgvPlan,
    bundle: Any,
    stdout_part: Any,
    stderr_part: Any,
    timeout_seconds: int,
) -> CursorProcessResult:
    if _endpoint_present():
        raise CursorInvocationError(_REASON_INVALID)
    pgid: int | None = None
    state: _CaptureState | None = None
    stdout_thread: threading.Thread | None = None
    stderr_thread: threading.Thread | None = None
    try:
        try:
            proc = subprocess.Popen(
                plan.argv,
                cwd=plan.cwd,
                stdin=plan.stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                start_new_session=True,
                bufsize=0,
                close_fds=True,
            )
        except OSError:
            raise CursorInvocationError(_REASON_SPAWN) from None
        pgid = proc.pid
        state = _CaptureState()
        stdout_thread = threading.Thread(
            target=_drain_stream,
            kwargs={
                "stream": proc.stdout,
                "part": stdout_part,
                "bundle": bundle,
                "limit": _STDOUT_LIMIT,
                "is_stdout": True,
                "state": state,
            },
        )
        stderr_thread = threading.Thread(
            target=_drain_stream,
            kwargs={
                "stream": proc.stderr,
                "part": stderr_part,
                "bundle": bundle,
                "limit": _STDERR_LIMIT,
                "is_stdout": False,
                "state": state,
            },
        )
        _observe_spawned_process(
            proc=proc,
            pgid=pgid,
            timeout_seconds=timeout_seconds,
            state=state,
            stdout_thread=stdout_thread,
            stderr_thread=stderr_thread,
            start_drains=True,
        )
    except KeyboardInterrupt:
        if state is None:
            state = _CaptureState()
        state.interrupted = True
        if pgid is None:
            pgid = proc.pid
        _observe_spawned_process(
            proc=proc,
            pgid=pgid,
            timeout_seconds=timeout_seconds,
            state=state,
            stdout_thread=stdout_thread,
            stderr_thread=stderr_thread,
            start_drains=False,
        )
    if state.term_failed:
        raise CursorInvocationError(_REASON_TERM)
    if state.timeout:
        raise CursorInvocationError(_REASON_TIMEOUT)
    if state.interrupted:
        raise CursorInvocationError(_REASON_INTERRUPT)
    if state.stdout_io:
        raise CursorInvocationError(_REASON_STDOUT_IO)
    if state.stderr_io:
        raise CursorInvocationError(_REASON_STDERR_IO)
    if state.limit:
        raise CursorInvocationError(_REASON_LIMIT)
    exit_code = proc.returncode
    if exit_code != 0:
        raise CursorInvocationError(_REASON_EXIT)
    return CursorProcessResult(
        process_exit_code=exit_code,
        timeout=False,
        interrupted=False,
        stdout_byte_length=state.stdout_bytes,
        stderr_byte_length=state.stderr_bytes,
    )


def run_cursor_adapter(
    *,
    configured_executable: str,
    target_root: object,
    runner_source_root: object,
    runtime_root: object,
    developer: BoundPromptBytes,
    task_spec: BoundPromptBytes,
    attempt_token: str,
    timeout_seconds: int,
    model: str | None = None,
) -> CursorAdapterOutcome:
    prompt_frame = cursor_stream.serialize_prompt_frame(
        developer=developer,
        task_spec=task_spec,
    )
    resolved = resolve_cursor_executable(configured_executable=configured_executable)
    plan = build_cursor_argv(
        executable_path=resolved.path,
        target_root=target_root,
        runner_source_root=runner_source_root,
        runtime_root=runtime_root,
        model=model,
    )
    spawn_plan = CursorArgvPlan(
        argv=(*plan.argv, prompt_frame.decode("utf-8")),
        cwd=plan.cwd,
        shell=plan.shell,
        stdin=plan.stdin,
    )
    bundle = cursor_stream.reserve_evidence_bundle(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
        attempt_token=attempt_token,
    )
    stdout_part = bundle.create_part(basename=_STDOUT_PART_NAME)
    stderr_part = bundle.create_part(basename=_STDERR_PART_NAME)
    process = execute_cursor_process(
        plan=spawn_plan,
        bundle=bundle,
        stdout_part=stdout_part,
        stderr_part=stderr_part,
        timeout_seconds=timeout_seconds,
    )
    stdout_bytes = _read_owned_part(bundle=bundle, part=stdout_part)
    inspection = cursor_stream.parse_cursor_stream(
        stdout=stdout_bytes,
        target=plan.cwd,
        prompt_frame=prompt_frame,
    )
    bundle.publish(part=stdout_part, final_basename=_STDOUT_FINAL_NAME)
    bundle.publish(part=stderr_part, final_basename=_STDERR_FINAL_NAME)
    projection = cursor_stream.build_publish_projection(
        local_facts={
            "adapter_status": _ADAPTER_STATUS_COMPLETED,
            "neutral_reason_code": _ADAPTER_COMPLETED,
            "process_exit_code": process.process_exit_code,
            "timeout": process.timeout,
            "interrupted": process.interrupted,
            "event_counts": inspection.event_counts,
            "session_observed": inspection.session_observed,
            "terminal_success": inspection.terminal_success,
        }
    )
    return CursorAdapterOutcome(
        publish_projection=projection,
        prompt_frame_matches=inspection.prompt_frame_matches,
        stdout_artifact=_STDOUT_FINAL_NAME,
        stderr_artifact=_STDERR_FINAL_NAME,
    )


def _read_owned_part(*, bundle: Any, part: Any) -> bytes:
    try:
        fd = os.open(part.basename, _PART_READ_FLAGS, dir_fd=bundle._dir_fd)
    except OSError:
        raise cursor_stream.AdapterEvidenceError(_REASON_EVIDENCE_IO) from None
    except KeyboardInterrupt:
        raise CursorInvocationError(_REASON_INTERRUPT) from None
    try:
        chunks: list[bytes] = []
        while True:
            try:
                chunk = os.read(fd, _READ_SIZE)
            except OSError:
                raise cursor_stream.AdapterEvidenceError(_REASON_EVIDENCE_IO) from None
            except KeyboardInterrupt:
                raise CursorInvocationError(_REASON_INTERRUPT) from None
            if chunk == b"":
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        try:
            os.close(fd)
        except OSError:
            raise cursor_stream.AdapterEvidenceError(_REASON_EVIDENCE_IO) from None
        except KeyboardInterrupt:
            raise CursorInvocationError(_REASON_INTERRUPT) from None


def _reject_nul(text: str) -> None:
    if "\0" in text:
        raise CursorInvocationError(_REASON_INVALID)


def _root_text(value: object) -> str:
    text = os.fspath(value)
    _reject_nul(text)
    return text


def _endpoint_present() -> bool:
    return _ENDPOINT_NAME in os.environ


def _resolve_absolute(configured_executable: str) -> ResolvedCursorExecutable:
    inspected = _inspect_candidate(os.path.abspath(configured_executable))
    if inspected is None:
        raise CursorInvocationError(_REASON_NOT_FOUND)
    canonical, ok = inspected
    if not ok:
        raise CursorInvocationError(_REASON_NOT_EXECUTABLE)
    return ResolvedCursorExecutable(path=canonical, resolution_mode=_MODE_ABSOLUTE)


def _resolve_path_basename(name: str) -> ResolvedCursorExecutable:
    path_value = os.environ.get("PATH")
    directories = [] if path_value is None else path_value.split(os.pathsep)
    saw_non_executable = False
    for directory in directories:
        if directory == "":
            continue
        candidate = os.path.abspath(os.path.join(directory, name))
        inspected = _inspect_candidate(candidate)
        if inspected is None:
            continue
        canonical, ok = inspected
        if not ok:
            saw_non_executable = True
            continue
        return ResolvedCursorExecutable(path=canonical, resolution_mode=_MODE_PATH)
    if saw_non_executable:
        raise CursorInvocationError(_REASON_NOT_EXECUTABLE)
    raise CursorInvocationError(_REASON_NOT_FOUND)


def _inspect_candidate(path: str) -> tuple[str, bool] | None:
    try:
        mode = os.lstat(path).st_mode
    except OSError:
        return None
    if stat.S_ISLNK(mode):
        try:
            canonical = os.path.realpath(path)
            final_mode = os.lstat(canonical).st_mode
        except OSError:
            return None
        if stat.S_ISLNK(final_mode):
            return None
        ok = stat.S_ISREG(final_mode) and os.access(canonical, os.X_OK)
        return (canonical, ok)
    ok = stat.S_ISREG(mode) and os.access(path, os.X_OK)
    return (os.path.abspath(path), ok)


def _has_symlink_component(path: str) -> bool:
    current = path
    while True:
        if os.path.islink(current):
            return True
        parent = os.path.dirname(current)
        if parent == current:
            return False
        current = parent


def _roots_overlap(left: str, right: str) -> bool:
    return _is_same_or_below(left, right) or _is_same_or_below(right, left)


def _is_same_or_below(candidate: str, root: str) -> bool:
    if candidate == root:
        return True
    return candidate.startswith(root + os.sep)


def _read_monotonic() -> tuple[float | None, bool]:
    try:
        return time.monotonic(), False
    except KeyboardInterrupt:
        return None, True


def _signal_group(pgid: int, sig: int) -> bool:
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return False


def _drain_finished(thread: threading.Thread | None) -> bool:
    if thread is None:
        return True
    return not thread.is_alive()


def _observe_spawned_process(
    *,
    proc: subprocess.Popen[bytes],
    pgid: int,
    timeout_seconds: int,
    state: _CaptureState,
    stdout_thread: threading.Thread | None,
    stderr_thread: threading.Thread | None,
    start_drains: bool,
) -> None:
    if start_drains:
        stdout_thread.start()
        stderr_thread.start()
    _supervise_process(
        proc=proc,
        pgid=pgid,
        timeout_seconds=timeout_seconds,
        state=state,
        stdout_thread=stdout_thread,
        stderr_thread=stderr_thread,
    )
    if state.term_failed:
        proc.stdout.close()
        proc.stderr.close()
        _join_drain(stdout_thread, timeout=_JOIN_TIMEOUT_SECONDS)
        _join_drain(stderr_thread, timeout=_JOIN_TIMEOUT_SECONDS)
        return
    _join_drain(stdout_thread)
    _join_drain(stderr_thread)


def _join_drain(thread: threading.Thread | None, timeout: float | None = None) -> None:
    if thread is None or thread.ident is None:
        return
    if timeout is None:
        thread.join()
        return
    thread.join(timeout=timeout)


def _write_captured_payload(
    *,
    bundle: Any,
    part: Any,
    payload: bytes,
    is_stdout: bool,
    state: _CaptureState,
) -> bool:
    try:
        bundle.write_part(part=part, payload=payload)
    except KeyboardInterrupt:
        state.interrupted = True
        return False
    except Exception:
        if is_stdout:
            state.stdout_io = True
        else:
            state.stderr_io = True
        return False
    return True


def _drain_stream(
    *,
    stream: Any,
    part: Any,
    bundle: Any,
    limit: int,
    is_stdout: bool,
    state: _CaptureState,
) -> None:
    fd = stream.fileno()
    total = 0
    pending = bytearray()
    try:
        while True:
            try:
                chunk = os.read(fd, _READ_SIZE)
            except KeyboardInterrupt:
                state.interrupted = True
                return
            except OSError:
                if is_stdout:
                    state.stdout_io = True
                else:
                    state.stderr_io = True
                return
            if chunk == b"":
                if not pending:
                    return
                if not _write_captured_payload(
                    bundle=bundle,
                    part=part,
                    payload=bytes(pending),
                    is_stdout=is_stdout,
                    state=state,
                ):
                    return
                total += len(pending)
                pending.clear()
                return
            if total + len(pending) + len(chunk) > limit:
                state.limit = True
                continue
            pending.extend(chunk)
            if len(pending) < _WRITE_BATCH:
                continue
            if not _write_captured_payload(
                bundle=bundle,
                part=part,
                payload=bytes(pending),
                is_stdout=is_stdout,
                state=state,
            ):
                return
            total += len(pending)
            pending.clear()
    finally:
        if is_stdout:
            state.stdout_bytes = total
        else:
            state.stderr_bytes = total


def _supervise_process(
    *,
    proc: subprocess.Popen[bytes],
    pgid: int,
    timeout_seconds: int,
    state: _CaptureState,
    stdout_thread: threading.Thread | None,
    stderr_thread: threading.Thread | None,
) -> None:
    deadline: float | None = None
    term_sent = False
    kill_sent = False
    grace_deadline: float | None = None
    kill_deadline: float | None = None
    while True:
        try:
            ts, interrupted = _read_monotonic()
            if interrupted:
                state.interrupted = True
                retry, again = _read_monotonic()
                if not again:
                    ts = retry
            if deadline is None and ts is not None:
                deadline = ts + timeout_seconds
            parent_exited = proc.poll() is not None
            group_alive = _signal_group(pgid, 0)
            drains_done = _drain_finished(stdout_thread) and _drain_finished(stderr_thread)
            if parent_exited and not group_alive and drains_done:
                return
            need_stop = False
            if state.interrupted:
                need_stop = True
            if state.limit:
                need_stop = True
            if state.stdout_io:
                need_stop = True
            if state.stderr_io:
                need_stop = True
            if parent_exited and group_alive:
                need_stop = True
            if not parent_exited and deadline is not None and ts is not None and ts >= deadline:
                state.timeout = True
                need_stop = True
            if need_stop and not term_sent:
                _signal_group(pgid, signal.SIGTERM)
                term_sent = True
                if ts is not None:
                    grace_deadline = ts + _TERM_GRACE_SECONDS
            if term_sent and not kill_sent and group_alive:
                if ts is not None and grace_deadline is not None and ts >= grace_deadline:
                    _signal_group(pgid, signal.SIGKILL)
                    kill_sent = True
                    kill_deadline = ts + _KILL_WAIT_SECONDS
            if kill_sent and group_alive:
                if ts is not None and kill_deadline is not None and ts >= kill_deadline:
                    state.term_failed = True
                    return
            time.sleep(_POLL_SECONDS)
        except KeyboardInterrupt:
            state.interrupted = True
