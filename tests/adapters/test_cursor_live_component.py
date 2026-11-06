"""Opt-in live Cursor component harness.

Keep:
- module-level opt-in and prerequisite checks before any
  ``cursor_headless`` or ``run_cursor_adapter`` import
- skip is never Live-GREEN and never replaces a real component run
- missing installation or auth is BLOCKED_PREREQUISITE
- a disposable Target, Runtime outside Candidate/Target/Runner Source
- maximum truthful claim REAL_CURSOR_ADAPTER_COMPONENT_RUN_GREEN

Drop:
- fake process as live proof
- WorkerResult, Promotion, Recovery, PUBLIC-P, or full-run claims
- importing the adapter before the opt-in skip
"""

from __future__ import annotations

import ast
import hashlib
import os
import stat
from pathlib import Path

import pytest

_OPT_IN_NAME = "CURSOR_ADAPTER_LIVE_COMPONENT"
_OPT_IN_VALUE = "1"
_EXECUTABLE_ENV = "CURSOR_ADAPTER_LIVE_EXECUTABLE"
_AUTH_ENV = "CURSOR_ADAPTER_LIVE_AUTH_READY"
_TARGET_ENV = "CURSOR_ADAPTER_LIVE_TARGET"
_RUNTIME_ENV = "CURSOR_ADAPTER_LIVE_RUNTIME"
_RUNNER_SOURCE_ENV = "CURSOR_ADAPTER_LIVE_RUNNER_SOURCE"
_REASON_PREREQUISITE = "ADAPTER_LIVE_PREREQUISITE_MISSING"
_REASON_PROTOCOL = "ADAPTER_LIVE_PROTOCOL_BLOCKED"
_SKIP_REASON = (
    "CURSOR_ADAPTER_LIVE_COMPONENT opt-in absent; skip is not Live-GREEN "
    "and does not replace REAL_CURSOR_ADAPTER_COMPONENT_RUN_GREEN"
)


def _this_file() -> Path:
    return Path(__file__).resolve()


def _assert_module_level_skip_before_adapter_import() -> None:
    tree = ast.parse(_this_file().read_text(encoding="utf-8"))
    skip_lines: list[int] = []
    import_lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            is_skip = isinstance(func, ast.Attribute) and func.attr == "skip"
            if is_skip and any(keyword.arg == "allow_module_level" for keyword in node.keywords):
                skip_lines.append(node.lineno)
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
            names.extend(alias.name for alias in node.names)
        blob = " ".join(names)
        if "cursor_headless" in blob or "run_cursor_adapter" in blob:
            import_lines.append(node.lineno)
    assert skip_lines, "live harness must skip at module level"
    assert import_lines, "live harness must import run_cursor_adapter only after skip"
    assert min(skip_lines) < min(import_lines)


def _blocked_prerequisite(detail: str) -> None:
    pytest.fail(
        f"BLOCKED_PREREQUISITE {_REASON_PREREQUISITE}: {detail}; "
        "missing installation or auth is never Live-GREEN"
    )


def _is_same_or_below(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


_assert_module_level_skip_before_adapter_import()

if os.environ.get(_OPT_IN_NAME) != _OPT_IN_VALUE:
    pytest.skip(_SKIP_REASON, allow_module_level=True)

_LIVE_EXECUTABLE = os.environ.get(_EXECUTABLE_ENV, "").strip()
if _LIVE_EXECUTABLE == "" or not os.path.isabs(_LIVE_EXECUTABLE):
    _blocked_prerequisite("configured live executable is absent")
if not os.path.isfile(_LIVE_EXECUTABLE) or not os.access(_LIVE_EXECUTABLE, os.X_OK):
    _blocked_prerequisite("configured live executable is not installed")
if os.environ.get(_AUTH_ENV) != "1":
    _blocked_prerequisite("live auth was not marked ready by the operator")

_LIVE_TARGET = os.environ.get(_TARGET_ENV, "").strip()
_LIVE_RUNTIME = os.environ.get(_RUNTIME_ENV, "").strip()
_LIVE_RUNNER_SOURCE = os.environ.get(_RUNNER_SOURCE_ENV, "").strip()
if _LIVE_TARGET == "" or not os.path.isabs(_LIVE_TARGET):
    _blocked_prerequisite("disposable live Target is absent")
if _LIVE_RUNTIME == "" or not os.path.isabs(_LIVE_RUNTIME):
    _blocked_prerequisite("live Runtime root is absent")
if _LIVE_RUNNER_SOURCE == "" or not os.path.isabs(_LIVE_RUNNER_SOURCE):
    _blocked_prerequisite("live Runner Source root is absent")

_LIVE_TARGET_PATH = Path(_LIVE_TARGET).resolve()
_LIVE_RUNTIME_PATH = Path(_LIVE_RUNTIME).resolve()
_LIVE_RUNNER_SOURCE_PATH = Path(_LIVE_RUNNER_SOURCE).resolve()
_CANDIDATE_ROOT = _this_file().parents[2]
if not _LIVE_TARGET_PATH.is_dir():
    _blocked_prerequisite("disposable live Target is not a directory")
if not _LIVE_RUNTIME_PATH.is_dir():
    _blocked_prerequisite("live Runtime root is not a directory")
if not _LIVE_RUNNER_SOURCE_PATH.is_dir():
    _blocked_prerequisite("live Runner Source root is not a directory")
if _is_same_or_below(_LIVE_TARGET_PATH, _CANDIDATE_ROOT):
    _blocked_prerequisite("live Target must be a disposable directory outside Candidate")
if _is_same_or_below(_LIVE_RUNTIME_PATH, _CANDIDATE_ROOT):
    _blocked_prerequisite("live Runtime must remain outside Candidate")
if _is_same_or_below(_LIVE_RUNTIME_PATH, _LIVE_TARGET_PATH) or _is_same_or_below(
    _LIVE_RUNTIME_PATH, _LIVE_RUNNER_SOURCE_PATH
):
    _blocked_prerequisite("live Runtime must remain outside Target and Runner Source")

from task_transition_runner.adapters.cursor_headless import run_cursor_adapter
from task_transition_runner.adapters.cursor_stream import BoundPromptBytes


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _bound(payload: bytes) -> BoundPromptBytes:
    return BoundPromptBytes(
        payload_bytes=payload,
        sha256=_sha256(payload),
        byte_length=len(payload),
    )


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def test_live_skip_is_never_counted_as_component_green() -> None:
    assert os.environ.get(_OPT_IN_NAME) == _OPT_IN_VALUE
    assert "not Live-GREEN" in _SKIP_REASON
    assert "does not replace REAL_CURSOR_ADAPTER_COMPONENT_RUN_GREEN" in _SKIP_REASON


def test_live_real_cursor_component_run_in_disposable_target() -> None:
    developer = _bound(b"synthetic live developer prompt")
    task_spec = _bound(b"synthetic live task spec")
    attempt = "synthetic-oss5-live-attempt-01"
    try:
        outcome = run_cursor_adapter(
            configured_executable=_LIVE_EXECUTABLE,
            target_root=_LIVE_TARGET_PATH,
            runner_source_root=_LIVE_RUNNER_SOURCE_PATH,
            runtime_root=_LIVE_RUNTIME_PATH,
            developer=developer,
            task_spec=task_spec,
            attempt_token=attempt,
            timeout_seconds=120,
        )
    except Exception as exc:
        reason = getattr(exc, "reason_code", "")
        if reason in {_REASON_PREREQUISITE}:
            pytest.fail(
                f"BLOCKED_PREREQUISITE {reason}; missing installation or auth is never Live-GREEN"
            )
        pytest.fail(
            f"BLOCKED {_REASON_PROTOCOL}: {reason or type(exc).__name__}; "
            "protocol failure is never Live-GREEN"
        )

    projection = outcome.publish_projection
    assert projection["adapter_status"] == "completed"
    assert projection["neutral_reason_code"] == "ADAPTER_COMPLETED"
    assert projection["process_exit_code"] == 0
    assert projection["timeout"] is False
    assert projection["interrupted"] is False
    assert projection["session_observed"] is True
    assert projection["terminal_success"] is True
    assert outcome.prompt_frame_matches is True
    assert "prompt_frame_matches" not in projection
    assert not hasattr(outcome, "worker_result")
    assert not hasattr(outcome, "decision")
    assert not hasattr(outcome, "promotion")
    assert not hasattr(outcome, "recovery")

    bundle = _LIVE_RUNTIME_PATH / attempt
    stdout_final = bundle / "stdout"
    stderr_final = bundle / "stderr"
    assert stdout_final.is_file()
    assert stderr_final.is_file()
    assert _mode(stdout_final) == 0o600
    assert _mode(stderr_final) == 0o600
    assert not (bundle / "stdout.part").exists()
    assert not (bundle / "stderr.part").exists()
    assert not _is_same_or_below(stdout_final.resolve(), _CANDIDATE_ROOT)
    assert not _is_same_or_below(stdout_final.resolve(), _LIVE_TARGET_PATH)
