"""Synthetic Linux process-group, capture, timeout, and interrupt contract.

Keep:
- keyword-only ``execute_cursor_process`` over a ratified argv plan
- already reserved owned stdout/stderr spools; no new evidence reservation
- new Linux session and process group with stdin DEVNULL and shell=false
- concurrent binary stdout/stderr drains until EOF, never sequential
- inclusive stdout 64-MiB and stderr 8-MiB capture limits, one-byte overflow
- typed stdout/stderr I/O failures and capture tie-break
- stdout/stderr ``os.read`` OSError mapped separately from ``write_part`` I/O
- stdout I/O beats a simultaneous stream-limit breach
- endpoint presence after plan construction and before spawn
- interrupt and overflow keep owned ``.part`` files and never publish finals
- drain-write KeyboardInterrupt after a retained part never becomes success
- supervisor-wait KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- supervisor proc.poll KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- supervisor os.killpg probe KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- post-spawn drain Thread construction KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- post-spawn drain Thread.start KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- post-spawn pid initialization KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- post-spawn capture-state initialization KeyboardInterrupt uses the same TERM-then-KILL group cleanup
- post-supervise drain Thread.join KeyboardInterrupt maps to ADAPTER_INTERRUPTED
- Exit 0 with stderr, nonzero Exit, and negative signal returncode
- Exit-nonzero before any stream/session/terminal interpretation
- monotonic deadline, TERM then at most five seconds, then KILL, then reap
- interrupt uses the same group cleanup and precedence as timeout
- termination failure overrides timeout and interrupt
- content-free local process facts only

Drop:
- sequential drain, wall-clock deadline, or direct-child-only kill
- psutil, new dependencies, or a real Cursor executable
- reduced product limits or a shortened TERM-grace parameter
- Environment copy/hash/project, argv persistence, or raw stream excerpts
- stream/session/terminal pairwise coverage owned by later adapter tasks
- a test helper that stands in for ``execute_cursor_process``
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
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from task_transition_runner.adapters import cursor_headless as cursor_headless_mod
from task_transition_runner.adapters.cursor_headless import (
    CursorArgvPlan,
    build_cursor_argv,
    execute_cursor_process,
    resolve_cursor_executable,
)
from task_transition_runner.adapters.cursor_stream import reserve_evidence_bundle

_STDOUT_LIMIT = 67108864
_STDERR_LIMIT = 8388608
_RECORD_LIMIT = 100000
_RECORD_LINE_LIMIT = 4194304
_TERM_GRACE_SECONDS = 5
_WATCHDOG_SECONDS = 60
_ATTEMPT = "synthetic-oss5-attempt-process-01"
_SCRIPT_NAME = "synthetic-oss5-process-script"
_SCENARIO_NAME = "synthetic-oss5-process-scenario.json"
_IDENTITY_NAME = "synthetic-oss5-process-identity.json"
_STDOUT_PART = "stdout.part"
_STDERR_PART = "stderr.part"
_STDOUT_STAMP = b"SYNTHETIC-OSS5-STDOUT-FILL:"
_STDERR_STAMP = b"SYNTHETIC-OSS5-STDERR-FILL:"
_STDOUT_CANARY = "SYNTHETIC-OSS5-CANARY-STDOUT"
_STDERR_CANARY = "SYNTHETIC-OSS5-CANARY-STDERR"
_AUTH_CANARY = "SYNTHETIC-OSS5-CANARY-AUTH-VALUE"
_ENV_NAME = "SYNTHETIC_OSS5_CANARY_ENVIRONMENT_NAME"
_ENV_VALUE = "SYNTHETIC-OSS5-CANARY-ENVIRONMENT-VALUE"
_ENDPOINT_CANARY = "SYNTHETIC-OSS5-CANARY-ENDPOINT-VALUE"
_REASON_INVALID = "ADAPTER_INVOCATION_INVALID"
_REASON_SPAWN = "ADAPTER_SPAWN_FAILED"
_REASON_EXIT = "ADAPTER_PROCESS_EXIT_NONZERO"
_REASON_TIMEOUT = "ADAPTER_TIMEOUT"
_REASON_INTERRUPT = "ADAPTER_INTERRUPTED"
_REASON_TERM = "ADAPTER_TERMINATION_FAILED"
_REASON_STDOUT_IO = "ADAPTER_STDOUT_IO_FAILED"
_REASON_STDERR_IO = "ADAPTER_STDERR_IO_FAILED"
_REASON_LIMIT = "ADAPTER_STREAM_LIMIT_EXCEEDED"
_SIG_STDOUT_READ_IO = (
    "stdout os.read OSError returned success instead of ADAPTER_STDOUT_IO_FAILED"
)
_SIG_STDERR_READ_IO = (
    "stderr os.read OSError returned success instead of ADAPTER_STDERR_IO_FAILED"
)
_SIG_DRAIN_WRITE_INTERRUPT = (
    "KeyboardInterrupt from drain write returned success instead of ADAPTER_INTERRUPTED"
)
_SIG_SUPERVISOR_WAIT_INTERRUPT = (
    "KeyboardInterrupt from supervisor wait leaked or left a process-group survivor"
)
_SIG_SUPERVISOR_POLL_INTERRUPT = (
    "KeyboardInterrupt from supervisor proc.poll leaked or left process-group survivors"
)
_SIG_SUPERVISOR_KILLPG_PROBE_INTERRUPT = (
    "KeyboardInterrupt from supervisor os.killpg probe leaked or left process-group survivors"
)
_SIG_DRAIN_THREAD_CONSTRUCTION_INTERRUPT = (
    "KeyboardInterrupt from post-spawn drain thread construction leaked or left process-group survivors"
)
_SIG_DRAIN_THREAD_START_INTERRUPT = (
    "KeyboardInterrupt from post-spawn drain thread start leaked or left process-group survivors"
)
_SIG_DRAIN_THREAD_JOIN_INTERRUPT = (
    "KeyboardInterrupt from post-supervise drain thread join leaked instead of ADAPTER_INTERRUPTED"
)
_SIG_POST_SPAWN_PID_INIT_INTERRUPT = (
    "KeyboardInterrupt from post-spawn pid initialization leaked or left process-group survivors"
)
_SIG_POST_SPAWN_CAPTURE_STATE_INIT_INTERRUPT = (
    "KeyboardInterrupt from post-spawn capture-state initialization leaked or left process-group survivors"
)
_REAL_KILL = os.kill
_FORBIDDEN_PARAMETER_NAMES = {
    "argv",
    "auth",
    "child",
    "endpoint",
    "env",
    "environment",
    "file",
    "file_path",
    "grace",
    "grace_seconds",
    "logical_ref",
    "model",
    "path",
    "process",
    "prompt",
    "psutil",
    "session_id",
    "source",
    "source_path",
    "spawn",
    "stderr",
    "stdin",
    "stdout",
    "termination_grace_seconds",
    "stdout_limit",
    "stderr_limit",
    "ndjson_record_count",
    "ndjson_record_line_bytes_excluding_lf",
}
_SCRIPT = r"""
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

_SCENARIO = "synthetic-oss5-process-scenario.json"
_IDENTITY = "synthetic-oss5-process-identity.json"
_STDOUT_STAMP = b"SYNTHETIC-OSS5-STDOUT-FILL:"
_STDERR_STAMP = b"SYNTHETIC-OSS5-STDERR-FILL:"


def _write_json(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_suffix(".tmp")
    text = json.dumps(payload, separators=(",", ":"))
    tmp.write_text(text, encoding="utf-8")
    fd = os.open(tmp, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    tmp.replace(path)


def _self_identity() -> dict[str, int]:
    return {
        "pid": os.getpid(),
        "pgid": os.getpgid(0),
        "sid": os.getsid(0),
        "ppid": os.getppid(),
    }


def _fill(fd: int, size: int, stamp: bytes) -> None:
    block = (stamp * (4096 // len(stamp) + 1))[:4096]
    left = size
    while left > 0:
        chunk = block if left >= len(block) else block[:left]
        written = os.write(fd, chunk)
        left -= written


def _start_grandchild(scenario: dict[str, object]) -> list[dict[str, int]]:
    if not scenario.get("grandchild"):
        return []
    reader, writer = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(reader)
        if scenario.get("grandchild_ignore_term"):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        payload = json.dumps(_self_identity()).encode("utf-8")
        os.write(writer, payload)
        os.close(writer)
        while True:
            time.sleep(0.05)
        os._exit(0)
    os.close(writer)
    raw = os.read(reader, 4096)
    os.close(reader)
    return [json.loads(raw.decode("utf-8"))]


def _publish_identity(scenario: dict[str, object]) -> None:
    identity = _self_identity()
    identity["grandchildren"] = _start_grandchild(scenario)
    identity["stdin_eof"] = os.read(0, 1) == b""
    _write_json(Path(_IDENTITY), identity)


def _success_shaped(cwd: str) -> bytes:
    lines = (
        '{"type":"system","subtype":"init","cwd":"%s","session_id":"synthetic-oss5-session"}'
        % cwd,
        '{"type":"user","session_id":"synthetic-oss5-session","message":{"content":[{"type":"text","text":"synthetic-oss5-frame"}]}}',
        '{"type":"result","subtype":"success","is_error":false,"session_id":"synthetic-oss5-session"}',
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def main() -> None:
    scenario = json.loads(Path(_SCENARIO).read_text(encoding="utf-8"))
    if scenario.get("ignore_term"):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _publish_identity(scenario)
    kind = scenario["kind"]
    if kind == "exit_zero_with_stderr":
        os.write(2, b"SYNTHETIC-OSS5-CANARY-STDERR")
        os._exit(0)
    if kind == "exit_nonzero":
        os.write(1, b"SYNTHETIC-OSS5-CANARY-STDOUT")
        os._exit(int(scenario.get("exit_code", 7)))
    if kind == "exit_signal":
        os.kill(os.getpid(), signal.SIGUSR1)
        os._exit(0)
    if kind == "hang_silent":
        time.sleep(float(scenario.get("sleep_seconds", 120)))
        os._exit(0)
    if kind == "hang_after_terminal":
        os.write(1, _success_shaped(os.getcwd()))
        time.sleep(float(scenario.get("sleep_seconds", 120)))
        os._exit(0)
    if kind == "fill_parallel":
        stdout_size = int(scenario["stdout_bytes"])
        stderr_size = int(scenario["stderr_bytes"])
        threads = [
            threading.Thread(target=_fill, args=(1, stdout_size, _STDOUT_STAMP), daemon=False),
            threading.Thread(target=_fill, args=(2, stderr_size, _STDERR_STAMP), daemon=False),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        os._exit(0)
    if kind == "fill_stdout":
        _fill(1, int(scenario["stdout_bytes"]), _STDOUT_STAMP)
        os._exit(0)
    if kind == "fill_stderr":
        _fill(2, int(scenario["stderr_bytes"]), _STDERR_STAMP)
        os._exit(0)
    if kind == "success_shaped_nonzero":
        os.write(1, _success_shaped(os.getcwd()))
        os._exit(1)
    if kind == "malformed_nonzero":
        os.write(1, b"{this is not json\n")
        os._exit(2)
    os._exit(99)


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
        plan: CursorArgvPlan,
    ) -> None:
        self.runtime_root = runtime_root
        self.target_root = target_root
        self.runner_source_root = runner_source_root
        self.executable = executable
        self.plan = plan

    def snippets(self) -> tuple[str, ...]:
        return (
            str(self.runtime_root),
            str(self.target_root),
            str(self.runner_source_root),
            str(self.executable),
            str(self.plan.cwd),
            _STDOUT_CANARY,
            _STDERR_CANARY,
            _AUTH_CANARY,
            _ENV_NAME,
            _ENV_VALUE,
            _ENDPOINT_CANARY,
            "synthetic-oss5-session",
            "synthetic-oss5-frame",
        )


def _linux_or_fail() -> None:
    if sys.platform != "linux":
        pytest.fail("process-group contract requires Linux; no platform fallback")


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
    monkeypatch.setenv(_ENV_NAME, _ENV_VALUE)
    resolved = resolve_cursor_executable(configured_executable=str(executable))
    plan = build_cursor_argv(
        executable_path=resolved.path,
        target_root=target_root,
        runner_source_root=runner_source_root,
        runtime_root=runtime_root,
    )
    return _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
        executable=executable,
        plan=plan,
    )


def _write_scenario(target_root: Path, scenario: dict[str, object]) -> None:
    (target_root / _SCENARIO_NAME).write_text(
        json.dumps(scenario, separators=(",", ":")),
        encoding="utf-8",
    )


def _reserve(roots: _Roots) -> Any:
    return reserve_evidence_bundle(
        runtime_root=roots.runtime_root,
        target_root=roots.target_root,
        runner_source_root=roots.runner_source_root,
        attempt_token=_ATTEMPT,
    )


def _part_path(bundle: Any, basename: str) -> Path:
    return Path(bundle.directory) / basename


def _read_part(bundle: Any, basename: str) -> bytes:
    path = _part_path(bundle, basename)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        chunks: list[bytes] = []
        while True:
            block = os.read(fd, 1024 * 1024)
            if block == b"":
                break
            chunks.append(block)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _assert_parts_retained_without_finals(directory: Path) -> None:
    stdout_part = directory / _STDOUT_PART
    stderr_part = directory / _STDERR_PART
    assert stdout_part.is_file()
    assert stderr_part.is_file()
    assert not stdout_part.is_symlink()
    assert not stderr_part.is_symlink()
    assert _mode(stdout_part) == 0o600
    assert _mode(stderr_part) == 0o600
    assert not (directory / "stdout").exists()
    assert not (directory / "stderr").exists()
    assert not (directory / "stdout").is_symlink()
    assert not (directory / "stderr").is_symlink()


def _identity(roots: _Roots) -> dict[str, Any]:
    path = roots.target_root / _IDENTITY_NAME
    assert path.is_file()
    return json.loads(path.read_text(encoding="utf-8"))


def _pid_alive(pid: int) -> bool:
    try:
        _REAL_KILL(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _cmdline(pid: int) -> bytes:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return b""


def _synthetic_pids(executable: Path) -> list[int]:
    marker = os.fsencode(executable.name)
    found: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == os.getpid():
            continue
        if marker in _cmdline(pid):
            found.append(pid)
    return found


def _kill_pids(pids: list[int]) -> None:
    for pid in pids:
        try:
            _REAL_KILL(pid, signal.SIGKILL)
        except ProcessLookupError:
            continue
        except OSError:
            continue
    for pid in pids:
        try:
            os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            continue
        except OSError:
            continue


def _recorded_pids(identity: dict[str, Any]) -> list[int]:
    pids = [int(identity["pid"])]
    for child in identity.get("grandchildren", []):
        pids.append(int(child["pid"]))
    return pids


def _is_drain_thread(thread: threading.Thread) -> bool:
    kwargs = getattr(thread, "_kwargs", None) or {}
    return isinstance(kwargs, dict) and "is_stdout" in kwargs


def _assert_reaped(identity: dict[str, Any]) -> None:
    living = [pid for pid in _recorded_pids(identity) if _pid_alive(pid)]
    assert living == [], f"process group members survived: {living}"


def _assert_new_session(roots: _Roots, identity: dict[str, Any]) -> None:
    parent_sid = os.getsid(0)
    child_pid = int(identity["pid"])
    child_pgid = int(identity["pgid"])
    child_sid = int(identity["sid"])
    assert child_sid != parent_sid
    assert child_pgid == child_pid
    assert child_sid == child_pgid
    for grandchild in identity.get("grandchildren", []):
        assert int(grandchild["pgid"]) == child_pgid
        assert int(grandchild["sid"]) == child_sid
        assert int(grandchild["pid"]) != child_pid


def _assert_keyword_only(func: Callable[..., object], required: set[str]) -> None:
    parameters = inspect.signature(func).parameters
    assert required <= set(parameters)
    assert set(parameters) <= required
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)
    for name, parameter in parameters.items():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty


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


def _assert_content_free(payload: object, snippets: tuple[str, ...]) -> None:
    rendered = repr(payload)
    for snippet in snippets:
        assert snippet not in rendered
    assert not hasattr(payload, "argv")
    assert not hasattr(payload, "stdout")
    assert not hasattr(payload, "stderr")
    assert not hasattr(payload, "environment")
    assert not hasattr(payload, "env")
    assert not hasattr(payload, "session_id")


def _assert_success_facts(
    result: Any,
    *,
    exit_code: int,
    stdout_len: int,
    stderr_len: int,
    snippets: tuple[str, ...],
) -> None:
    assert result.process_exit_code == exit_code
    assert type(result.process_exit_code) is int
    assert result.timeout is False
    assert result.interrupted is False
    assert result.stdout_byte_length == stdout_len
    assert result.stderr_byte_length == stderr_len
    _assert_content_free(result, snippets)


def _stamp_hash(stamp: bytes, size: int) -> str:
    block = (stamp * (4096 // len(stamp) + 1))[:4096]
    digest = hashlib.sha256()
    left = size
    while left > 0:
        chunk = block if left >= len(block) else block[:left]
        digest.update(chunk)
        left -= len(chunk)
    return digest.hexdigest()


def _invoke(
    roots: _Roots,
    scenario: dict[str, object],
    *,
    timeout_seconds: int,
    prepare: Callable[[Any, Any, Any], None] | None = None,
    allow_survivors: bool = False,
) -> tuple[Any, Any]:
    _write_scenario(roots.target_root, scenario)
    bundle = _reserve(roots)
    stdout_part = bundle.create_part(basename=_STDOUT_PART)
    stderr_part = bundle.create_part(basename=_STDERR_PART)
    if prepare is not None:
        prepare(bundle, stdout_part, stderr_part)
    fired = threading.Event()
    stop = threading.Event()

    def watch() -> None:
        if not stop.wait(_WATCHDOG_SECONDS):
            fired.set()
            _kill_pids(_synthetic_pids(roots.executable))

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        result = execute_cursor_process(
            plan=roots.plan,
            bundle=bundle,
            stdout_part=stdout_part,
            stderr_part=stderr_part,
            timeout_seconds=timeout_seconds,
        )
    finally:
        stop.set()
        watcher.join(timeout=1)
        leftovers = _synthetic_pids(roots.executable)
        if leftovers:
            _kill_pids(leftovers)
            if not allow_survivors:
                pytest.fail(f"process group members survived: {leftovers}")
        if fired.is_set():
            pytest.fail("watchdog fired; sequential drain deadlock or unreaped group")
    return bundle, result


def _invoke_failure(
    roots: _Roots,
    scenario: dict[str, object],
    *,
    timeout_seconds: int,
    reason: str,
    prepare: Callable[[Any, Any, Any], None] | None = None,
    allow_survivors: bool = False,
) -> tuple[Path, BaseException]:
    def action() -> object:
        _bundle, result = _invoke(
            roots,
            scenario,
            timeout_seconds=timeout_seconds,
            prepare=prepare,
            allow_survivors=allow_survivors,
        )
        return result

    exc = _expect_reason(reason, action, snippets=roots.snippets())
    directory = roots.runtime_root / _ATTEMPT
    assert directory.is_dir()
    return directory, exc


@pytest.fixture(autouse=True)
def _reap_synthetic_scripts() -> Any:
    _linux_or_fail()
    before = set(_synthetic_pids(Path(_SCRIPT_NAME)))
    yield
    after = [pid for pid in _synthetic_pids(Path(_SCRIPT_NAME)) if pid not in before]
    if after:
        _kill_pids(after)
        pytest.fail(f"autouse reap found survivors: {after}")


def test_execute_cursor_process_binds_keyword_only_plan_bundle_parts_and_timeout() -> None:
    _assert_keyword_only(
        execute_cursor_process,
        {"plan", "bundle", "stdout_part", "stderr_part", "timeout_seconds"},
    )
    source = inspect.getsource(sys.modules["task_transition_runner.adapters.cursor_headless"])
    assert "psutil" not in source
    assert "time.time" not in source


def test_execute_cursor_process_opens_new_session_and_group_with_grandchild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    bundle, result = _invoke(
        roots,
        {
            "kind": "exit_zero_with_stderr",
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=5,
    )
    identity = _identity(roots)
    _assert_new_session(roots, identity)
    assert identity["stdin_eof"] is True
    _assert_success_facts(
        result,
        exit_code=0,
        stdout_len=0,
        stderr_len=len(_STDERR_CANARY),
        snippets=roots.snippets(),
    )
    assert _mode(_part_path(bundle, _STDOUT_PART)) == 0o600
    assert _mode(_part_path(bundle, _STDERR_PART)) == 0o600
    _assert_reaped(identity)


def test_execute_cursor_process_drains_stdout_and_stderr_concurrently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    stdout_size = 1024 * 1024
    stderr_size = 1024 * 1024
    bundle, result = _invoke(
        roots,
        {
            "kind": "fill_parallel",
            "stdout_bytes": stdout_size,
            "stderr_bytes": stderr_size,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
    )
    stdout = _read_part(bundle, _STDOUT_PART)
    stderr = _read_part(bundle, _STDERR_PART)
    assert len(stdout) == stdout_size
    assert len(stderr) == stderr_size
    assert hashlib.sha256(stdout).hexdigest() == _stamp_hash(_STDOUT_STAMP, stdout_size)
    assert hashlib.sha256(stderr).hexdigest() == _stamp_hash(_STDERR_STAMP, stderr_size)
    assert _STDERR_STAMP not in stdout
    assert _STDOUT_STAMP not in stderr
    _assert_success_facts(
        result,
        exit_code=0,
        stdout_len=stdout_size,
        stderr_len=stderr_size,
        snippets=roots.snippets(),
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_accepts_stdout_at_inclusive_64_mib_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    bundle, result = _invoke(
        roots,
        {"kind": "fill_stdout", "stdout_bytes": _STDOUT_LIMIT},
        timeout_seconds=30,
    )
    stdout = _read_part(bundle, _STDOUT_PART)
    assert len(stdout) == _STDOUT_LIMIT
    assert hashlib.sha256(stdout).hexdigest() == _stamp_hash(_STDOUT_STAMP, _STDOUT_LIMIT)
    _assert_success_facts(
        result,
        exit_code=0,
        stdout_len=_STDOUT_LIMIT,
        stderr_len=0,
        snippets=roots.snippets(),
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_rejects_stdout_one_byte_over_limit_and_reaps_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {
            "kind": "fill_stdout",
            "stdout_bytes": _STDOUT_LIMIT + 1,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_LIMIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_accepts_stderr_at_inclusive_8_mib_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    bundle, result = _invoke(
        roots,
        {"kind": "fill_stderr", "stderr_bytes": _STDERR_LIMIT},
        timeout_seconds=30,
    )
    stderr = _read_part(bundle, _STDERR_PART)
    assert len(stderr) == _STDERR_LIMIT
    assert hashlib.sha256(stderr).hexdigest() == _stamp_hash(_STDERR_STAMP, _STDERR_LIMIT)
    _assert_success_facts(
        result,
        exit_code=0,
        stdout_len=0,
        stderr_len=_STDERR_LIMIT,
        snippets=roots.snippets(),
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_rejects_stderr_one_byte_over_limit_and_reaps_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {
            "kind": "fill_stderr",
            "stderr_bytes": _STDERR_LIMIT + 1,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_LIMIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_does_not_expose_record_limit_overrides() -> None:
    parameters = inspect.signature(execute_cursor_process).parameters
    assert "ndjson_record_count" not in parameters
    assert "ndjson_record_line_bytes_excluding_lf" not in parameters
    assert _RECORD_LIMIT == 100000
    assert _RECORD_LINE_LIMIT == 4194304


def test_execute_cursor_process_maps_stdout_write_failure_without_io_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"

    def prepare(bundle: Any, stdout_part: Any, stderr_part: Any) -> None:
        original = bundle.write_part

        def failing(*, part: Any, payload: bytes) -> None:
            if part is stdout_part or getattr(part, "basename", "") == _STDOUT_PART:
                raise OSError(errno.EIO, io_secret)
            return original(part=part, payload=payload)

        monkeypatch.setattr(bundle, "write_part", failing, raising=False)

    _invoke_failure(
        roots,
        {"kind": "fill_parallel", "stdout_bytes": 4096, "stderr_bytes": 4096},
        timeout_seconds=5,
        reason=_REASON_STDOUT_IO,
        prepare=prepare,
    )
    identity = _identity(roots)
    _assert_reaped(identity)


def test_execute_cursor_process_maps_stderr_write_failure_without_io_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"

    def prepare(bundle: Any, stdout_part: Any, stderr_part: Any) -> None:
        original = bundle.write_part

        def failing(*, part: Any, payload: bytes) -> None:
            if part is stderr_part or getattr(part, "basename", "") == _STDERR_PART:
                raise OSError(errno.EIO, io_secret)
            return original(part=part, payload=payload)

        monkeypatch.setattr(bundle, "write_part", failing, raising=False)

    _invoke_failure(
        roots,
        {"kind": "fill_stderr", "stderr_bytes": 4096},
        timeout_seconds=5,
        reason=_REASON_STDERR_IO,
        prepare=prepare,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_capture_tie_break_prefers_stdout_io_over_stderr_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"

    def prepare(bundle: Any, stdout_part: Any, stderr_part: Any) -> None:
        def failing(*, part: Any, payload: bytes) -> None:
            raise OSError(errno.EIO, io_secret)

        monkeypatch.setattr(bundle, "write_part", failing, raising=False)

    _invoke_failure(
        roots,
        {"kind": "fill_parallel", "stdout_bytes": 4096, "stderr_bytes": 4096},
        timeout_seconds=5,
        reason=_REASON_STDOUT_IO,
        prepare=prepare,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_capture_tie_break_prefers_stderr_io_over_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"

    def prepare(bundle: Any, stdout_part: Any, stderr_part: Any) -> None:
        original = bundle.write_part

        def failing(*, part: Any, payload: bytes) -> None:
            if getattr(part, "basename", "") == _STDERR_PART:
                raise OSError(errno.EIO, io_secret)
            return original(part=part, payload=payload)

        monkeypatch.setattr(bundle, "write_part", failing, raising=False)

    _invoke_failure(
        roots,
        {
            "kind": "fill_parallel",
            "stdout_bytes": _STDOUT_LIMIT + 1,
            "stderr_bytes": 4096,
        },
        timeout_seconds=30,
        reason=_REASON_STDERR_IO,
        prepare=prepare,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_capture_tie_break_prefers_stdout_io_over_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"

    def prepare(bundle: Any, stdout_part: Any, stderr_part: Any) -> None:
        original = bundle.write_part

        def failing(*, part: Any, payload: bytes) -> None:
            if part is stdout_part or getattr(part, "basename", "") == _STDOUT_PART:
                raise OSError(errno.EIO, io_secret)
            return original(part=part, payload=payload)

        monkeypatch.setattr(bundle, "write_part", failing, raising=False)

    _invoke_failure(
        roots,
        {
            "kind": "fill_stdout",
            "stdout_bytes": _STDOUT_LIMIT + 1,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_STDOUT_IO,
        prepare=prepare,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_keeps_exit_zero_with_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    bundle, result = _invoke(
        roots,
        {"kind": "exit_zero_with_stderr"},
        timeout_seconds=5,
    )
    assert _read_part(bundle, _STDERR_PART) == _STDERR_CANARY.encode("ascii")
    assert _read_part(bundle, _STDOUT_PART) == b""
    _assert_success_facts(
        result,
        exit_code=0,
        stdout_len=0,
        stderr_len=len(_STDERR_CANARY),
        snippets=roots.snippets(),
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_raises_nonzero_exit_before_stream_interpretation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {"kind": "success_shaped_nonzero"},
        timeout_seconds=5,
        reason=_REASON_EXIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_raises_nonzero_exit_for_malformed_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {"kind": "malformed_nonzero"},
        timeout_seconds=5,
        reason=_REASON_EXIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_raises_nonzero_exit_for_plain_nonzero_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {"kind": "exit_nonzero", "exit_code": 7},
        timeout_seconds=5,
        reason=_REASON_EXIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_raises_nonzero_exit_for_negative_signal_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {"kind": "exit_signal"},
        timeout_seconds=5,
        reason=_REASON_EXIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_uses_monotonic_deadline_not_wall_clock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real_time = time.time

    def jumped_wall() -> float:
        return real_time() + 10_000

    monkeypatch.setattr(time, "time", jumped_wall)
    bundle, result = _invoke(
        roots,
        {"kind": "fill_stdout", "stdout_bytes": 64},
        timeout_seconds=5,
    )
    _assert_success_facts(
        result,
        exit_code=0,
        stdout_len=64,
        stderr_len=0,
        snippets=roots.snippets(),
    )
    assert len(_read_part(bundle, _STDOUT_PART)) == 64
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_times_out_before_output_and_reaps_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    started = time.monotonic()
    _invoke_failure(
        roots,
        {
            "kind": "hang_silent",
            "grandchild": True,
            "grandchild_ignore_term": False,
        },
        timeout_seconds=1,
        reason=_REASON_TIMEOUT,
    )
    elapsed = time.monotonic() - started
    assert elapsed >= 1
    assert elapsed < _TERM_GRACE_SECONDS
    _assert_new_session(roots, _identity(roots))
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_times_out_after_apparent_terminal_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {"kind": "hang_after_terminal"},
        timeout_seconds=1,
        reason=_REASON_TIMEOUT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_escalates_term_resistant_group_after_five_seconds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    started = time.monotonic()
    _invoke_failure(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=1,
        reason=_REASON_TIMEOUT,
    )
    elapsed = time.monotonic() - started
    assert elapsed >= 1 + _TERM_GRACE_SECONDS
    assert elapsed < 1 + _TERM_GRACE_SECONDS + 3
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_termination_failure_overrides_timeout(
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
    _invoke_failure(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=1,
        reason=_REASON_TERM,
        allow_survivors=True,
    )


def test_execute_cursor_process_interrupts_during_both_drains_and_reaps_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real_read = os.read
    identity = roots.target_root / _IDENTITY_NAME
    reads = {"n": 0}

    def interrupting_read(fd: int, n: int) -> bytes:
        data = real_read(fd, n)
        if identity.is_file() and fd not in (0,):
            reads["n"] += 1
            if reads["n"] >= 1 and data:
                raise KeyboardInterrupt
        return data

    monkeypatch.setattr(os, "read", interrupting_read)
    _invoke_failure(
        roots,
        {
            "kind": "fill_parallel",
            "stdout_bytes": 1024 * 1024,
            "stderr_bytes": 1024 * 1024,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_INTERRUPT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_interrupt_equals_timeout_for_term_compliant_group(
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
    started = real_monotonic()
    _invoke_failure(
        roots,
        {
            "kind": "hang_silent",
            "grandchild": True,
            "grandchild_ignore_term": False,
        },
        timeout_seconds=30,
        reason=_REASON_INTERRUPT,
    )
    elapsed = real_monotonic() - started
    assert elapsed < _TERM_GRACE_SECONDS
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_interrupt_escalates_term_resistant_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity = roots.target_root / _IDENTITY_NAME
    real_monotonic = time.monotonic
    armed = {"value": False}

    def interrupting_monotonic() -> float:
        now = real_monotonic()
        if identity.is_file() and not armed["value"]:
            armed["value"] = True
            raise KeyboardInterrupt
        return now

    monkeypatch.setattr(time, "monotonic", interrupting_monotonic)
    started = real_monotonic()
    _invoke_failure(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_INTERRUPT,
    )
    elapsed = real_monotonic() - started
    assert elapsed >= _TERM_GRACE_SECONDS
    assert elapsed < _TERM_GRACE_SECONDS + 3
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_termination_failure_overrides_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity = roots.target_root / _IDENTITY_NAME
    real_monotonic = time.monotonic
    real_kill = os.kill
    real_killpg = os.killpg
    armed = {"value": False}

    def interrupting_monotonic() -> float:
        now = real_monotonic()
        if identity.is_file() and not armed["value"]:
            armed["value"] = True
            raise KeyboardInterrupt
        return now

    def swallow_kill(pid: int, sig: int) -> None:
        if sig in (signal.SIGKILL, 9):
            return
        real_kill(pid, sig)

    def swallow_killpg(pgid: int, sig: int) -> None:
        if sig in (signal.SIGKILL, 9):
            return
        real_killpg(pgid, sig)

    monkeypatch.setattr(time, "monotonic", interrupting_monotonic)
    monkeypatch.setattr(os, "kill", swallow_kill)
    monkeypatch.setattr(os, "killpg", swallow_killpg)
    _invoke_failure(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_TERM,
        allow_survivors=True,
    )


def test_execute_cursor_process_limit_exceeded_overrides_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    _invoke_failure(
        roots,
        {"kind": "fill_stdout", "stdout_bytes": _STDOUT_LIMIT + 1},
        timeout_seconds=30,
        reason=_REASON_LIMIT,
    )
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_spawn_failure_is_typed_and_content_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    missing = tmp_path / "bin" / "synthetic-oss5-missing-process"
    roots.plan = CursorArgvPlan(
        argv=(
            str(missing),
            "--trust",
            "--print",
            "--force",
            "--output-format=stream-json",
            f"--workspace={roots.plan.cwd}",
        ),
        cwd=roots.plan.cwd,
        shell=False,
        stdin=subprocess.DEVNULL,
    )
    _invoke_failure(
        roots,
        {"kind": "exit_zero_with_stderr"},
        timeout_seconds=5,
        reason=_REASON_SPAWN,
    )


def test_execute_cursor_process_rejects_endpoint_set_after_plan_before_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    monkeypatch.setenv("CURSOR_API_ENDPOINT", _ENDPOINT_CANARY)

    def forbidden_spawn(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("child spawned after endpoint presence")

    monkeypatch.setattr(subprocess, "Popen", forbidden_spawn)
    monkeypatch.setattr(os, "fork", forbidden_spawn)
    monkeypatch.setattr(os, "posix_spawn", forbidden_spawn)
    if hasattr(os, "posix_spawnp"):
        monkeypatch.setattr(os, "posix_spawnp", forbidden_spawn)

    directory, exc = _invoke_failure(
        roots,
        {"kind": "exit_zero_with_stderr"},
        timeout_seconds=5,
        reason=_REASON_INVALID,
    )
    assert not (roots.target_root / _IDENTITY_NAME).exists()
    assert _synthetic_pids(roots.executable) == []
    assert _ENDPOINT_CANARY not in str(exc)
    assert _ENDPOINT_CANARY not in repr(exc)
    assert not (directory / "stdout").exists()
    assert not (directory / "stderr").exists()


def test_execute_cursor_process_does_not_publish_final_names_on_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    directory, _exc = _invoke_failure(
        roots,
        {"kind": "hang_silent"},
        timeout_seconds=1,
        reason=_REASON_TIMEOUT,
    )
    assert not (directory / "stdout").exists()
    assert not (directory / "stderr").exists()
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_keeps_partial_parts_without_finals_on_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real_read = os.read
    identity = roots.target_root / _IDENTITY_NAME
    reads = {"n": 0}

    def interrupting_read(fd: int, n: int) -> bytes:
        data = real_read(fd, n)
        if identity.is_file() and fd not in (0,):
            reads["n"] += 1
            if reads["n"] >= 1 and data:
                raise KeyboardInterrupt
        return data

    monkeypatch.setattr(os, "read", interrupting_read)
    directory, _exc = _invoke_failure(
        roots,
        {
            "kind": "fill_parallel",
            "stdout_bytes": 1024 * 1024,
            "stderr_bytes": 1024 * 1024,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_INTERRUPT,
    )
    _assert_parts_retained_without_finals(directory)
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_keeps_partial_parts_without_finals_on_stdout_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    directory, _exc = _invoke_failure(
        roots,
        {
            "kind": "fill_stdout",
            "stdout_bytes": _STDOUT_LIMIT + 1,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_LIMIT,
    )
    _assert_parts_retained_without_finals(directory)
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_keeps_partial_parts_without_finals_on_stderr_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    directory, _exc = _invoke_failure(
        roots,
        {
            "kind": "fill_stderr",
            "stderr_bytes": _STDERR_LIMIT + 1,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
        reason=_REASON_LIMIT,
    )
    _assert_parts_retained_without_finals(directory)
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_maps_stdout_os_read_failure_without_io_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"
    real_read = os.read
    identity = roots.target_root / _IDENTITY_NAME
    fired = {"value": False}

    def failing_read(fd: int, n: int) -> bytes:
        data = real_read(fd, n)
        if not fired["value"] and identity.is_file() and _STDOUT_STAMP in data:
            fired["value"] = True
            raise OSError(errno.EIO, io_secret)
        return data

    monkeypatch.setattr(os, "read", failing_read)
    try:
        _invoke(
            roots,
            {
                "kind": "fill_stdout",
                "stdout_bytes": 4096,
                "grandchild": True,
            },
            timeout_seconds=5,
        )
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_STDOUT_IO
        assert str(exc) == _REASON_STDOUT_IO
        assert io_secret not in str(exc)
        assert io_secret not in repr(exc)
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        directory = roots.runtime_root / _ATTEMPT
        _assert_parts_retained_without_finals(directory)
        _assert_reaped(_identity(roots))
        return
    pytest.fail(_SIG_STDOUT_READ_IO)


def test_execute_cursor_process_maps_stderr_os_read_failure_without_io_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    io_secret = "synthetic-oss5-io-secret"
    real_read = os.read
    identity = roots.target_root / _IDENTITY_NAME
    fired = {"value": False}

    def failing_read(fd: int, n: int) -> bytes:
        data = real_read(fd, n)
        if not fired["value"] and identity.is_file() and _STDERR_STAMP in data:
            fired["value"] = True
            raise OSError(errno.EIO, io_secret)
        return data

    monkeypatch.setattr(os, "read", failing_read)
    try:
        _invoke(
            roots,
            {
                "kind": "fill_stderr",
                "stderr_bytes": 4096,
                "grandchild": True,
            },
            timeout_seconds=5,
        )
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_STDERR_IO
        assert str(exc) == _REASON_STDERR_IO
        assert io_secret not in str(exc)
        assert io_secret not in repr(exc)
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        directory = roots.runtime_root / _ATTEMPT
        _assert_parts_retained_without_finals(directory)
        _assert_reaped(_identity(roots))
        return
    pytest.fail(_SIG_STDERR_READ_IO)


def test_execute_cursor_process_maps_drain_write_interrupt_after_retained_part(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    writes = {"n": 0}

    def prepare(bundle: Any, stdout_part: Any, stderr_part: Any) -> None:
        original = bundle.write_part

        def interrupting(*, part: Any, payload: bytes) -> None:
            original(part=part, payload=payload)
            writes["n"] += 1
            if writes["n"] >= 1:
                raise KeyboardInterrupt

        monkeypatch.setattr(bundle, "write_part", interrupting, raising=False)

    try:
        _invoke(
            roots,
            {
                "kind": "fill_parallel",
                "stdout_bytes": 4096,
                "stderr_bytes": 4096,
                "grandchild": True,
            },
            timeout_seconds=5,
            prepare=prepare,
        )
    except Exception as exc:
        assert writes["n"] >= 1
        assert getattr(exc, "reason_code") == _REASON_INTERRUPT
        assert str(exc) == _REASON_INTERRUPT
        for snippet in roots.snippets():
            assert snippet not in str(exc)
            assert snippet not in repr(exc)
        directory = roots.runtime_root / _ATTEMPT
        _assert_parts_retained_without_finals(directory)
        stdout_len = (directory / _STDOUT_PART).stat().st_size
        stderr_len = (directory / _STDERR_PART).stat().st_size
        assert stdout_len > 0 or stderr_len > 0
        _assert_reaped(_identity(roots))
        return
    pytest.fail(_SIG_DRAIN_WRITE_INTERRUPT)


def test_execute_cursor_process_supervisor_wait_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_sleep = time.sleep
    armed = {"value": False}

    def interrupting_sleep(seconds: float) -> None:
        if identity_path.is_file() and not armed["value"]:
            armed["value"] = True
            raise KeyboardInterrupt
        real_sleep(seconds)

    monkeypatch.setattr(time, "sleep", interrupting_sleep)
    _write_scenario(
        roots.target_root,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
    )
    bundle = _reserve(roots)
    stdout_part = bundle.create_part(basename=_STDOUT_PART)
    stderr_part = bundle.create_part(basename=_STDERR_PART)
    started = time.monotonic()
    caught: BaseException | None = None
    result: Any = None
    survivors: list[int] = []
    fired = threading.Event()
    stop = threading.Event()

    def watch() -> None:
        if not stop.wait(_WATCHDOG_SECONDS):
            fired.set()
            _kill_pids(_synthetic_pids(roots.executable))

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        result = execute_cursor_process(
            plan=roots.plan,
            bundle=bundle,
            stdout_part=stdout_part,
            stderr_part=stderr_part,
            timeout_seconds=30,
        )
    except BaseException as exc:
        caught = exc
    finally:
        stop.set()
        watcher.join(timeout=1)
        if identity_path.is_file():
            survivors = [
                pid for pid in _recorded_pids(_identity(roots)) if _pid_alive(pid)
            ]
        leftovers = _synthetic_pids(roots.executable)
        if leftovers:
            _kill_pids(leftovers)

    if fired.is_set():
        pytest.fail("watchdog fired; sequential drain deadlock or unreaped group")
    if isinstance(caught, KeyboardInterrupt) or survivors or result is not None:
        pytest.fail(_SIG_SUPERVISOR_WAIT_INTERRUPT)

    assert caught is not None
    assert getattr(caught, "reason_code") == _REASON_INTERRUPT
    assert str(caught) == _REASON_INTERRUPT
    for snippet in roots.snippets():
        assert snippet not in str(caught)
        assert snippet not in repr(caught)
    elapsed = time.monotonic() - started
    assert elapsed >= _TERM_GRACE_SECONDS
    assert elapsed < _TERM_GRACE_SECONDS + 3
    _assert_parts_retained_without_finals(Path(bundle.directory))
    _assert_reaped(_identity(roots))


def _invoke_resistant_group_supervisor_interrupt(
    roots: _Roots,
) -> tuple[BaseException | None, Any, list[int], float, Any]:
    identity_path = roots.target_root / _IDENTITY_NAME
    _write_scenario(
        roots.target_root,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
    )
    bundle = _reserve(roots)
    stdout_part = bundle.create_part(basename=_STDOUT_PART)
    stderr_part = bundle.create_part(basename=_STDERR_PART)
    started = time.monotonic()
    caught: BaseException | None = None
    result: Any = None
    survivors: list[int] = []
    fired = threading.Event()
    stop = threading.Event()

    def watch() -> None:
        if not stop.wait(_WATCHDOG_SECONDS):
            fired.set()
            _kill_pids(_synthetic_pids(roots.executable))

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        result = execute_cursor_process(
            plan=roots.plan,
            bundle=bundle,
            stdout_part=stdout_part,
            stderr_part=stderr_part,
            timeout_seconds=30,
        )
    except BaseException as exc:
        caught = exc
    finally:
        stop.set()
        watcher.join(timeout=1)
        if identity_path.is_file():
            survivors = [
                pid for pid in _recorded_pids(_identity(roots)) if _pid_alive(pid)
            ]
        leftovers = _synthetic_pids(roots.executable)
        if leftovers:
            _kill_pids(leftovers)

    if fired.is_set():
        pytest.fail("watchdog fired; sequential drain deadlock or unreaped group")
    elapsed = time.monotonic() - started
    return caught, result, survivors, elapsed, bundle


def _assert_shared_supervisor_interrupt_cleanup(
    *,
    roots: _Roots,
    caught: BaseException | None,
    result: Any,
    survivors: list[int],
    elapsed: float,
    bundle: Any,
    failure_signature: str,
) -> None:
    if isinstance(caught, KeyboardInterrupt) or survivors or result is not None:
        pytest.fail(failure_signature)
    assert caught is not None
    assert getattr(caught, "reason_code") == _REASON_INTERRUPT
    assert str(caught) == _REASON_INTERRUPT
    for snippet in roots.snippets():
        assert snippet not in str(caught)
        assert snippet not in repr(caught)
    assert elapsed >= _TERM_GRACE_SECONDS
    assert elapsed < _TERM_GRACE_SECONDS + 3
    _assert_parts_retained_without_finals(Path(bundle.directory))
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_supervisor_poll_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_poll = subprocess.Popen.poll
    armed = {"value": False}

    def interrupting_poll(self: subprocess.Popen[bytes]) -> int | None:
        if identity_path.is_file() and not armed["value"]:
            armed["value"] = True
            raise KeyboardInterrupt
        return real_poll(self)

    monkeypatch.setattr(subprocess.Popen, "poll", interrupting_poll)
    caught, result, survivors, elapsed, bundle = (
        _invoke_resistant_group_supervisor_interrupt(roots)
    )
    _assert_shared_supervisor_interrupt_cleanup(
        roots=roots,
        caught=caught,
        result=result,
        survivors=survivors,
        elapsed=elapsed,
        bundle=bundle,
        failure_signature=_SIG_SUPERVISOR_POLL_INTERRUPT,
    )


def test_execute_cursor_process_supervisor_killpg_probe_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_killpg = os.killpg
    armed = {"value": False}

    def interrupting_killpg(pgid: int, sig: int) -> None:
        if identity_path.is_file() and sig == 0 and not armed["value"]:
            armed["value"] = True
            raise KeyboardInterrupt
        return real_killpg(pgid, sig)

    monkeypatch.setattr(os, "killpg", interrupting_killpg)
    caught, result, survivors, elapsed, bundle = (
        _invoke_resistant_group_supervisor_interrupt(roots)
    )
    _assert_shared_supervisor_interrupt_cleanup(
        roots=roots,
        caught=caught,
        result=result,
        survivors=survivors,
        elapsed=elapsed,
        bundle=bundle,
        failure_signature=_SIG_SUPERVISOR_KILLPG_PROBE_INTERRUPT,
    )


def _invoke_visible_group_interrupt(
    roots: _Roots,
    scenario: dict[str, object],
    *,
    timeout_seconds: int,
) -> tuple[BaseException | None, Any, list[int], float, Any]:
    identity_path = roots.target_root / _IDENTITY_NAME
    _write_scenario(roots.target_root, scenario)
    bundle = _reserve(roots)
    stdout_part = bundle.create_part(basename=_STDOUT_PART)
    stderr_part = bundle.create_part(basename=_STDERR_PART)
    started = time.monotonic()
    caught: BaseException | None = None
    result: Any = None
    survivors: list[int] = []
    fired = threading.Event()
    stop = threading.Event()

    def watch() -> None:
        if not stop.wait(_WATCHDOG_SECONDS):
            fired.set()
            _kill_pids(_synthetic_pids(roots.executable))

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        result = execute_cursor_process(
            plan=roots.plan,
            bundle=bundle,
            stdout_part=stdout_part,
            stderr_part=stderr_part,
            timeout_seconds=timeout_seconds,
        )
    except BaseException as exc:
        caught = exc
    finally:
        stop.set()
        watcher.join(timeout=1)
        if identity_path.is_file():
            survivors = [
                pid for pid in _recorded_pids(_identity(roots)) if _pid_alive(pid)
            ]
        leftovers = _synthetic_pids(roots.executable)
        if leftovers:
            _kill_pids(leftovers)

    if fired.is_set():
        pytest.fail("watchdog fired; sequential drain deadlock or unreaped group")
    elapsed = time.monotonic() - started
    return caught, result, survivors, elapsed, bundle


def test_execute_cursor_process_drain_thread_construction_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_init = threading.Thread.__init__
    armed = {"value": False}

    def interrupting_init(self: threading.Thread, *args: Any, **kwargs: Any) -> None:
        real_init(self, *args, **kwargs)
        if _is_drain_thread(self) and not armed["value"]:
            deadline = time.monotonic() + 5
            while not identity_path.is_file() and time.monotonic() < deadline:
                time.sleep(0.01)
            if identity_path.is_file() and not armed["value"]:
                armed["value"] = True
                raise KeyboardInterrupt

    monkeypatch.setattr(threading.Thread, "__init__", interrupting_init)
    caught, result, survivors, elapsed, bundle = _invoke_visible_group_interrupt(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
    )
    _assert_shared_supervisor_interrupt_cleanup(
        roots=roots,
        caught=caught,
        result=result,
        survivors=survivors,
        elapsed=elapsed,
        bundle=bundle,
        failure_signature=_SIG_DRAIN_THREAD_CONSTRUCTION_INTERRUPT,
    )


def test_execute_cursor_process_drain_thread_start_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_start = threading.Thread.start
    armed = {"value": False}

    def interrupting_start(self: threading.Thread) -> None:
        if _is_drain_thread(self) and not armed["value"]:
            deadline = time.monotonic() + 5
            while not identity_path.is_file() and time.monotonic() < deadline:
                time.sleep(0.01)
            if identity_path.is_file() and not armed["value"]:
                armed["value"] = True
                raise KeyboardInterrupt
        return real_start(self)

    monkeypatch.setattr(threading.Thread, "start", interrupting_start)
    caught, result, survivors, elapsed, bundle = _invoke_visible_group_interrupt(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
    )
    _assert_shared_supervisor_interrupt_cleanup(
        roots=roots,
        caught=caught,
        result=result,
        survivors=survivors,
        elapsed=elapsed,
        bundle=bundle,
        failure_signature=_SIG_DRAIN_THREAD_START_INTERRUPT,
    )


def test_execute_cursor_process_drain_thread_join_interrupt_maps_to_adapter_interrupted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    real_start = threading.Thread.start
    real_join = threading.Thread.join
    drain_threads: list[threading.Thread] = []
    armed = {"value": False}

    def tracking_start(self: threading.Thread) -> None:
        if _is_drain_thread(self):
            drain_threads.append(self)
        return real_start(self)

    def interrupting_join(
        self: threading.Thread,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if self in drain_threads and not armed["value"]:
            armed["value"] = True
            raise KeyboardInterrupt
        return real_join(self, *args, **kwargs)

    monkeypatch.setattr(threading.Thread, "start", tracking_start)
    monkeypatch.setattr(threading.Thread, "join", interrupting_join)
    caught, result, survivors, elapsed, bundle = _invoke_visible_group_interrupt(
        roots,
        {
            "kind": "fill_parallel",
            "stdout_bytes": 4096,
            "stderr_bytes": 4096,
        },
        timeout_seconds=30,
    )
    if isinstance(caught, KeyboardInterrupt) or survivors or result is not None:
        pytest.fail(_SIG_DRAIN_THREAD_JOIN_INTERRUPT)
    assert armed["value"] is True
    assert drain_threads
    assert caught is not None
    assert getattr(caught, "reason_code") == _REASON_INTERRUPT
    assert str(caught) == _REASON_INTERRUPT
    for snippet in roots.snippets():
        assert snippet not in str(caught)
        assert snippet not in repr(caught)
    assert elapsed < _TERM_GRACE_SECONDS
    _assert_parts_retained_without_finals(Path(bundle.directory))
    _assert_reaped(_identity(roots))


def test_execute_cursor_process_post_spawn_pid_initialization_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_popen = subprocess.Popen
    armed = {"value": False}

    def interrupting_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        proc = real_popen(*args, **kwargs)
        real_pid = int(proc.pid)

        class PostSpawnPidPopen(type(proc)):
            @property
            def pid(self) -> int:
                if not armed["value"]:
                    deadline = time.monotonic() + 5
                    while not identity_path.is_file() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    if identity_path.is_file() and not armed["value"]:
                        armed["value"] = True
                        raise KeyboardInterrupt
                return int(self.__dict__.get("pid", real_pid))

        proc.__class__ = PostSpawnPidPopen
        return proc

    monkeypatch.setattr(subprocess, "Popen", interrupting_popen)
    caught, result, survivors, elapsed, bundle = _invoke_visible_group_interrupt(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
    )
    assert armed["value"] is True
    _assert_shared_supervisor_interrupt_cleanup(
        roots=roots,
        caught=caught,
        result=result,
        survivors=survivors,
        elapsed=elapsed,
        bundle=bundle,
        failure_signature=_SIG_POST_SPAWN_PID_INIT_INTERRUPT,
    )


def test_execute_cursor_process_post_spawn_capture_state_initialization_interrupt_uses_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path, monkeypatch)
    identity_path = roots.target_root / _IDENTITY_NAME
    real_popen = subprocess.Popen
    real_capture_state = cursor_headless_mod._CaptureState
    spawned = {"value": False}
    armed = {"value": False}

    def tracking_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        proc = real_popen(*args, **kwargs)
        spawned["value"] = True
        return proc

    def interrupting_capture_state(*args: Any, **kwargs: Any) -> Any:
        if spawned["value"] and not armed["value"]:
            deadline = time.monotonic() + 5
            while not identity_path.is_file() and time.monotonic() < deadline:
                time.sleep(0.01)
            if identity_path.is_file() and not armed["value"]:
                armed["value"] = True
                raise KeyboardInterrupt
        return real_capture_state(*args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", tracking_popen)
    monkeypatch.setattr(cursor_headless_mod, "_CaptureState", interrupting_capture_state)
    caught, result, survivors, elapsed, bundle = _invoke_visible_group_interrupt(
        roots,
        {
            "kind": "hang_silent",
            "ignore_term": True,
            "grandchild": True,
            "grandchild_ignore_term": True,
        },
        timeout_seconds=30,
    )
    assert armed["value"] is True
    _assert_shared_supervisor_interrupt_cleanup(
        roots=roots,
        caught=caught,
        result=result,
        survivors=survivors,
        elapsed=elapsed,
        bundle=bundle,
        failure_signature=_SIG_POST_SPAWN_CAPTURE_STATE_INIT_INTERRUPT,
    )
