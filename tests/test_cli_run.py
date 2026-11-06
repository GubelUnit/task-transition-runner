"""Synthetic public thin CLI composition contract.

Keep:
- exactly three commands: ``init``, ``run``, ``status``
- ``--config <path>`` as the only required option; ``status`` may add ``--json``
- machine-readable YAML (or JSON for ``status --json``) outcomes
- fail-closed seams that stop when ``ok`` is false
- test-only successful traversal doubles for D-wiring
- call order authority, lock acquire, reconciliation, drain-before-dispatch,
  then activation before continuation before adapter
- Task-End ``prepare``/``finalize`` only after a successful adapter traversal
- Drain as the only pending/effect composition; no direct pending or effect
  CLI calls
- ConfigError as a blocked CLI outcome
- lock release with the acquired ownership on a blocked downstream path
- read-only ``status`` mechanically separated from init/run mutation

Drop:
- a fourth command and private source/export inspection
- default-pass for authority, reconciliation, continuation, or adapter
- complete ``init`` / ``run`` / ``status`` surface evidence
- copying private trigger ``main()`` or private trigger test bytes
- defining the twelve Task-End symbols inside ``cli.py``
- rewriting composition block codes before they reach the run outcome
- init-lock ordering against empty runtime
- Task-End work after the unpatched adapter seam
- continuation before activation
- explicit false ``source_authority_ok`` or ``authorizes_run`` on successful
  traversal doubles

The complete-surface cases below bind the real 18-step run. They do not
replace components with success doubles. Step 4 is the closed window
order INCOMPLETE_PROMOTION, CANONICAL_TOCK, MAIN_STATE, CONSUMPTION,
START_TICK_WINDOWS, POST_DRAIN_TICK_WINDOWS, and it precedes pre-dispatch
drain and task admissibility. The first task after init is
``FRESH_INIT_BASELINE`` and does not visit continuation. A fresh run
stops after the decision request and the matching lock release. It does
not visit decision validation, pending intent, return-to-main, the drain
effect, continuation consumption, or settlement. A later run, once external decisions are ready, may persist real
Task-End pending and must stop there. Only that pending predecessor may
authorize return-to-main and then a successful drain decision. Fresh init
without a continuation ends that successful drain as
``DRAINED_AWAITING_TASK`` and does not claim settlement. An ordinary
continuation ends ``SETTLED`` only after that drain and a successful
continuation consumption in the same run. A blocked admission result
stops continuation consumption and settlement. A failed or invalid
consumption result after a real successful drain stops before ``SETTLED``.
Reconciliation precedes pending resume, return-to-main, and drain.
Public success outcomes come from the canonical loaders. The closed
top-level outcome set has exactly nine members.
The promotion effect is observed on ``runner_drain_transition``, and
source authority precedes every persisted-state projection. The explicit step-4
``POST_DRAIN_TICK_WINDOWS`` visit stays distinct from the internal
``PRE_DISPATCH_DRAIN_RELOAD`` that follows ``pre_dispatch_drain`` and
precedes task admissibility. A valid fresh fixture pins the managed
runtime to the current runner source commit and reaches the real
``run_cursor_adapter`` process path through a local synthetic executable.
Public outcomes observe source authority, target preflight, lock acquire,
and reconciliation before state projection, and release that same lock
ownership on every exit. A settled predecessor is loaded from
``settled_predecessor.yaml``, passed as ``SETTLED_PROMOTION``, and
continuation follows task admissibility before the fresh start tick.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import inspect
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest
import yaml

from task_transition_runner.adapters.cursor_headless import CursorAdapterOutcome
from task_transition_runner.adapters.cursor_stream import BoundPromptBytes
from task_transition_runner.cli import COMMANDS, main
from task_transition_runner.runner_drain_transition import SchedulerPhase
from task_transition_runner.task_activation import ActivationStatus
from task_transition_runner.task_end_promotion import (
    compose_adapter_git_worker_result,
    prepare_git_promotion_task_end_artifacts,
)
from task_transition_runner.worker_result import WorkerReport, WorkerResult

_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_GIT_AUTHOR_NAME = "Thin CLI Composition Contract"
_GIT_AUTHOR_EMAIL = "thin-cli-run@task-transition-runner.invalid"
_PROMPT_ROLES = ("developer", "orchestrator", "validator")
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_TRAVERSAL_REASON = "D_COMPOSITION_TRAVERSAL"
_PUBLIC_COMMANDS = ("init", "run", "status")
_UNKNOWN_COMMAND = "unknown"
_FORBIDDEN_OUTCOMES = (
    "RECOVERED",
    "REOPENED",
    "MIGRATED",
    "SOURCE_AUTHORITY_OK",
)
_TASK_END_HELPERS = (
    "_atomic_write_yaml",
    "_copy_yaml_payload",
    "_git_stdout",
    "_load_yaml_mapping_if_present",
    "_non_runtime_dirty_paths",
    "_pending_matches_context",
    "_promotion_blocked",
    "_validate_current_red_lock_closure",
    "_validate_ready_promotion_drafts",
    "_wait_for_ready_promotion_drafts",
)
_RUNTIME_ARTIFACT_NAMES = (
    "main_state.yaml",
    "promotion_consumed.yaml",
    "pending_promotion.yaml",
    "promotion_journal.yaml",
    "drain_outcome.yaml",
    "runner_source_identity.yaml",
    "runner_git.lock",
)
_PREFIX_ORDER = (
    "code_authority",
    "lock_acquire",
    "reconciliation",
    "drain_before_dispatch",
)
_LOCK_OWNERSHIP = {
    "schema_version": _SCHEMA_VERSION,
    "lock_name": "runner_git.lock",
    "run_id": "synthetic-cli-run",
    "pid": 4242,
    "started_at_utc": "2026-09-12T12:00:00Z",
}
_SYNTHETIC_ADAPTER_TIMEOUT_SECONDS = 5
_SYNTHETIC_ADAPTER_PROGRAM = r"""
import json
import os
import sys


def main() -> None:
    frame = sys.argv[-1]
    workspace = ""
    prefix = "--workspace="
    for argument in sys.argv[1:-1]:
        if argument.startswith(prefix):
            workspace = argument[len(prefix) :]
    session_id = "synthetic-cli-real-component-session"
    records = (
        {
            "type": "system",
            "subtype": "init",
            "cwd": workspace,
            "session_id": session_id,
        },
        {
            "type": "user",
            "message": {"content": [{"type": "text", "text": frame}]},
            "session_id": session_id,
        },
        {
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "synthetic-cli-assistant"}]
            },
            "session_id": session_id,
        },
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "synthetic-cli-result",
            "session_id": session_id,
        },
    )
    payload = b"".join(
        json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
        for record in records
    )
    os.write(1, payload)


if __name__ == "__main__":
    main()
"""


def test_private_local_pid_probe_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    import task_transition_runner.cli as cli_module

    probe = cli_module._is_local_pid_alive
    observed: list[tuple[int, int]] = []

    def alive(pid: int, signal: int) -> None:
        observed.append((pid, signal))

    monkeypatch.setattr(cli_module.os, "kill", alive)
    assert probe(4242) is True
    assert observed == [(4242, 0)]

    def raise_probe_error(error: OSError) -> Any:
        def _raise(_pid: int, _signal: int) -> None:
            raise error

        return _raise

    for error in (ProcessLookupError(), OSError(errno.ESRCH, "missing")):
        monkeypatch.setattr(cli_module.os, "kill", raise_probe_error(error))
        assert probe(4242) is False

    for error in (PermissionError(), OSError(errno.EPERM, "denied")):
        monkeypatch.setattr(cli_module.os, "kill", raise_probe_error(error))
        assert probe(4242) is True

    class GenericProbeOSError(OSError):
        pass

    for error_number, expected in (
        (errno.ESRCH, False),
        (errno.EPERM, True),
    ):
        generic = GenericProbeOSError(error_number, "generic pid probe")
        monkeypatch.setattr(cli_module.os, "kill", raise_probe_error(generic))
        assert probe(4242) is expected

    unexpected = OSError(errno.EIO, "probe failed")
    monkeypatch.setattr(cli_module.os, "kill", raise_probe_error(unexpected))
    with pytest.raises(OSError, match="probe failed") as raised:
        probe(4242)
    assert raised.value is unexpected


def _run_git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _init_target_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "target-repo"
    repo_root.mkdir(parents=True)
    subprocess.run(  # noqa: S603
        ["git", "-c", f"init.defaultBranch={_REFERENCE_MAIN_BRANCH}", "init"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    _run_git(repo_root, "config", "user.name", _GIT_AUTHOR_NAME)
    _run_git(repo_root, "config", "user.email", _GIT_AUTHOR_EMAIL)
    (repo_root / "tracked.txt").write_text("public cli baseline\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "tracked.txt")
    _run_git(repo_root, "commit", "-m", "public cli baseline")
    return repo_root


def _write_validation_command(tmp_path: Path) -> str:
    script = tmp_path / "baseline-validate.sh"
    script.write_text("#!/bin/sh\nset -eu\nexit 0\n", encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _write_public_world(tmp_path: Path) -> tuple[Path, Path, Path]:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    runtime.mkdir(parents=True)
    config_root = tmp_path / "config-root"
    prompts = config_root / "prompts"
    prompts.mkdir(parents=True)
    task_source = config_root / "tasks.yaml"
    task_source.write_text("active_task: null\n", encoding="utf-8")
    for role in _PROMPT_ROLES:
        (prompts / f"{role}.md").write_text(f"synthetic {role} prompt\n", encoding="utf-8")
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "target": {
            "repo_root": str(target.resolve()),
            "main_branch": _REFERENCE_MAIN_BRANCH,
        },
        "runtime_root": str(runtime.resolve()),
        "task_source": str(task_source.resolve()),
        "baseline_validation": {
            "command": _write_validation_command(tmp_path),
            "timeout_seconds": 30,
        },
        "adapter": {
            "kind": "cursor",
            "executable": "cursor-agent",
            "model": None,
            "timeout_seconds": 1800,
        },
        "prompts": {
            "developer": str((prompts / "developer.md").resolve()),
            "orchestrator": str((prompts / "orchestrator.md").resolve()),
            "validator": str((prompts / "validator.md").resolve()),
        },
    }
    config_path = config_root / "runner.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path, target, runtime


def _read_stdout_yaml(stdout: str) -> dict[str, Any]:
    payload = yaml.safe_load(stdout)
    assert isinstance(payload, dict)
    return payload


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_fingerprint(path: Path) -> str:
    mode = stat.S_IMODE(path.stat().st_mode)
    return f"{mode:o}:{_sha256(path.read_bytes())}"


def _tree_digest(root: Path) -> dict[str, str]:
    digest: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if ".git" in path.parts:
            continue
        digest[path.relative_to(root).as_posix()] = _file_fingerprint(path)
    return digest


def _git_identity(repo_root: Path) -> dict[str, str]:
    git_dir = repo_root / ".git"
    index_path = git_dir / "index"
    return {
        "head_sha": _run_git(repo_root, "rev-parse", "HEAD"),
        "head_ref": (git_dir / "HEAD").read_text(encoding="utf-8"),
        "index_bytes": _sha256(index_path.read_bytes()) if index_path.exists() else "",
        "refs": _run_git(repo_root, "for-each-ref", "--format=%(refname) %(objectname)"),
        "porcelain": _run_git(repo_root, "status", "--porcelain=v1", "-uall"),
    }


def _runtime_identity(runtime_root: Path) -> dict[str, str]:
    identity: dict[str, str] = {}
    for name in _RUNTIME_ARTIFACT_NAMES:
        path = runtime_root / name
        identity[f"exists:{name}"] = str(path.exists())
        if path.exists():
            identity[f"bytes:{name}"] = _file_fingerprint(path)
    identity["tree"] = yaml.safe_dump(_tree_digest(runtime_root), sort_keys=True)
    return identity


def _snapshot(repo_root: Path, runtime_root: Path) -> dict[str, Any]:
    return {
        "git": _git_identity(repo_root),
        "worktree": _tree_digest(repo_root),
        "runtime": _runtime_identity(runtime_root),
    }


def _blocked_stdout(stdout: dict[str, Any], reason_code: str) -> None:
    assert stdout["ok"] is False
    assert stdout["outcome"] == "BLOCKED"
    assert stdout["status"] == "fail-closed"
    assert stdout["reason_code"] == reason_code
    assert stdout["outcome"] not in _FORBIDDEN_OUTCOMES
    assert stdout.get("source_authority_ok") is not True


def _traversal(label: str) -> dict[str, Any]:
    return {
        "ok": True,
        "status": "traversal",
        "reason_code": _TRAVERSAL_REASON,
        "seam": label,
    }


def _forbid_direct_bypass(name: str) -> Any:
    def _inner(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"cli must not call {name} directly")

    return _inner


def _install_doubles(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cli_module: Any,
    calls: list[str],
    released_ownership: list[Any],
    patch_authority: bool = True,
    patch_reconciliation: bool = True,
    patch_continuation: bool = True,
    patch_adapter: bool = False,
    patch_activation: bool = True,
    patch_task_end: bool = False,
    patch_composition: bool = True,
) -> None:
    def verify_code_authority(config: object) -> dict[str, Any]:
        calls.append("code_authority")
        return _traversal("code_authority")

    def reconcile_ordinary_promotion(config: object) -> dict[str, Any]:
        calls.append("reconciliation")
        return _traversal("reconciliation")

    def bind_ordinary_continuation(config: object) -> dict[str, Any]:
        calls.append("continuation")
        return _traversal("continuation")

    def acquire_runner_git_lock(**kwargs: Any) -> dict[str, Any]:
        calls.append("lock_acquire")
        return {
            "ok": True,
            "reason_code": "LOCK_OK",
            "acquired": True,
            "ownership": dict(_LOCK_OWNERSHIP),
            "reclaimed_stale_lock": False,
        }

    def release_runner_git_lock(**kwargs: Any) -> dict[str, Any]:
        calls.append("lock_release")
        released_ownership.append(kwargs.get("ownership"))
        return {
            "ok": True,
            "status": "released",
            "reason_code": "LOCK_OK",
            "released": True,
        }

    def evaluate_drain_before_dispatch(**kwargs: Any) -> Any:
        calls.append("drain_before_dispatch")
        return SimpleNamespace(
            phase=SchedulerPhase.DISPATCH_READY,
            outcome=SimpleNamespace(
                status=SimpleNamespace(value="NO_PENDING"),
                reason_code="NO_PENDING_PROMOTION",
                tock_sha=None,
            ),
        )

    def evaluate_task_activation(*args: Any, **kwargs: Any) -> Any:
        calls.append("activation")
        return SimpleNamespace(
            status=ActivationStatus.ACTIVATED,
            reason_code="FRESH_TASK_ACTIVATED",
            identity=SimpleNamespace(
                task_id="synthetic-cli-task",
                origin_run_id="origin-run-cli",
                promotion_attempt_id="promotion-attempt-cli",
            ),
        )

    def prepare_git_promotion_task_end_artifacts(**kwargs: Any) -> dict[str, Any]:
        calls.append("task_end_prepare")
        return {
            "ok": True,
            "outcome": "WORKER_RESULT_READY",
            "replayed": True,
            "result_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }

    def finalize_git_promotion_task_end_artifacts(**kwargs: Any) -> dict[str, Any]:
        calls.append("task_end_finalize")
        return {
            "ok": True,
            "artifact_class": _ARTIFACT_CLASS,
            "reason_code": "PENDING_INTENT_PERSISTED",
            "journal_start": False,
            "tock_sha": None,
        }

    def return_to_main(config: object) -> dict[str, Any]:
        calls.append("return_to_main")
        return {
            "ok": True,
            "reason_code": "RETURNED_TO_MAIN",
            "branch": _REFERENCE_MAIN_BRANCH,
        }

    def start_cursor_adapter(config: object) -> dict[str, Any]:
        calls.append("cursor_adapter")
        return _traversal("cursor_adapter")

    monkeypatch.setattr(cli_module, "acquire_runner_git_lock", acquire_runner_git_lock)
    monkeypatch.setattr(cli_module, "release_runner_git_lock", release_runner_git_lock)
    monkeypatch.setattr(
        cli_module, "evaluate_drain_before_dispatch", evaluate_drain_before_dispatch
    )
    monkeypatch.setattr(cli_module, "return_to_main", return_to_main)
    monkeypatch.setattr(
        cli_module,
        "persist_pending_promotion",
        _forbid_direct_bypass("persist_pending_promotion"),
        raising=False,
    )
    monkeypatch.setattr(
        cli_module,
        "admit_pending_promotion_effect",
        _forbid_direct_bypass("admit_pending_promotion_effect"),
        raising=False,
    )

    if patch_authority:
        monkeypatch.setattr(cli_module, "verify_code_authority", verify_code_authority)
    if patch_reconciliation:
        monkeypatch.setattr(
            cli_module, "reconcile_ordinary_promotion", reconcile_ordinary_promotion
        )
    if patch_continuation:
        monkeypatch.setattr(
            cli_module, "bind_ordinary_continuation", bind_ordinary_continuation
        )
    if patch_activation:
        monkeypatch.setattr(cli_module, "evaluate_task_activation", evaluate_task_activation)
    if patch_task_end:
        monkeypatch.setattr(
            cli_module,
            "prepare_git_promotion_task_end_artifacts",
            prepare_git_promotion_task_end_artifacts,
        )
        monkeypatch.setattr(
            cli_module,
            "finalize_git_promotion_task_end_artifacts",
            finalize_git_promotion_task_end_artifacts,
        )
    if patch_adapter:
        monkeypatch.setattr(cli_module, "start_cursor_adapter", start_cursor_adapter)
    if patch_composition:
        import task_transition_runner.task_end_promotion as task_end_module

        def compose_adapter_git_worker_result(**kwargs: Any) -> dict[str, Any]:
            calls.append("adapter_result_compose")
            return {
                "ok": True,
                "status": "traversal",
                "reason_code": _TRAVERSAL_REASON,
                "outcome": "WORKER_RESULT_READY",
                "promotion_authorized": False,
                "replayed": False,
                "worker_rerun": False,
                "resumed": False,
                "worker_result": None,
                "received": kwargs,
            }

        monkeypatch.setattr(
            task_end_module,
            "compose_adapter_git_worker_result",
            compose_adapter_git_worker_result,
            raising=False,
        )
        monkeypatch.setattr(
            cli_module,
            "compose_adapter_git_worker_result",
            compose_adapter_git_worker_result,
            raising=False,
        )


def _assert_prefix_order(calls: list[str]) -> None:
    for name in _PREFIX_ORDER:
        assert name in calls
    assert calls.index("code_authority") < calls.index("lock_acquire")
    assert calls.index("lock_acquire") < calls.index("reconciliation")
    assert calls.index("reconciliation") < calls.index("drain_before_dispatch")


def _assert_released_same_ownership(released_ownership: list[Any]) -> None:
    assert released_ownership == [_LOCK_OWNERSHIP]


def test_public_commands_are_exactly_init_run_status() -> None:
    assert COMMANDS == _PUBLIC_COMMANDS
    assert len(COMMANDS) == 3
    signature = inspect.signature(main)
    assert "argv" in signature.parameters or len(signature.parameters) <= 1


def test_each_command_requires_config_path(capsys: Any) -> None:
    for command in _PUBLIC_COMMANDS:
        exit_code = main([command])
        stdout = _read_stdout_yaml(capsys.readouterr().out)
        assert exit_code == 1
        _blocked_stdout(stdout, "CLI_CONFIG_REQUIRED")
        assert stdout["command"] == command


def test_unknown_command_is_rejected(tmp_path: Path, capsys: Any) -> None:
    config_path, target, runtime = _write_public_world(tmp_path)
    before = _snapshot(target, runtime)

    exit_code = main([_UNKNOWN_COMMAND, "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "CLI_COMMAND_UNKNOWN")
    assert stdout.get("command") == _UNKNOWN_COMMAND
    assert _snapshot(target, runtime) == before


def test_config_error_is_machine_readable_block(tmp_path: Path, capsys: Any) -> None:
    missing = tmp_path / "missing-runner.yaml"
    exit_code = main(["run", "--config", str(missing)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "CONFIG_SOURCE_MISSING")
    assert stdout["command"] == "run"


def test_unpatched_run_stops_at_code_authority_before_lock(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    calls: list[str] = []
    released_ownership: list[Any] = []
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_authority=False,
        patch_reconciliation=False,
        patch_continuation=False,
        patch_adapter=False,
        patch_activation=False,
    )

    (runtime / "main_state.yaml").write_bytes(b"")

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "SOURCE_AUTHORITY_RUNTIME_PIN_MISSING")
    assert stdout["reason_code"] != "CODE_AUTHORITY_UNAVAILABLE"
    assert "SOURCE_AUTHORITY_OK" not in yaml.safe_dump(stdout)
    assert "lock_acquire" not in calls
    assert "reconciliation" not in calls
    assert "drain_before_dispatch" not in calls
    assert released_ownership == []
    assert not (runtime / "runner_git.lock").exists()
    assert _run_git(target, "rev-parse", "HEAD") == _git_identity(target)["head_sha"]


def test_unpatched_reconciliation_releases_acquired_lock(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    calls: list[str] = []
    released_ownership: list[Any] = []
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_reconciliation=False,
        patch_continuation=False,
        patch_adapter=False,
        patch_activation=False,
    )

    (runtime / "promotion_journal.yaml").write_text(
        "phase: not-a-journal\n",
        encoding="utf-8",
    )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "RECONCILIATION_JOURNAL_INVALID")
    assert stdout["reason_code"] != "RECONCILIATION_UNAVAILABLE"
    assert calls == ["code_authority", "lock_acquire", "lock_release"]
    _assert_released_same_ownership(released_ownership)
    assert "drain_before_dispatch" not in calls
    assert not (runtime / "runner_git.lock").exists()
    assert _run_git(target, "status", "--porcelain") == ""


def test_unpatched_continuation_is_fail_closed(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    _lifecycle, target, runtime = _rebind_canonical_predecessor_world(
        config_path,
        tmp_path,
    )
    _write_active_task(config_path, target)
    (runtime / "promotion_consumed.yaml").unlink()
    head = _run_git(target, "rev-parse", "HEAD")
    calls: list[str] = []
    released_ownership: list[Any] = []
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_continuation=False,
        patch_adapter=False,
    )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "CONTINUATION_ORIGIN_INVALID")
    assert stdout["reason_code"] != "CONTINUATION_UNAVAILABLE"
    assert calls == [
        "code_authority",
        "lock_acquire",
        "reconciliation",
        "lock_release",
    ]
    assert "drain_before_dispatch" not in calls
    assert "continuation" not in calls
    assert "cursor_adapter" not in calls
    assert "task_end_prepare" not in calls
    assert "task_end_finalize" not in calls
    _assert_released_same_ownership(released_ownership)
    assert not (runtime / "runner_git.lock").exists()
    assert _run_git(target, "status", "--porcelain") == ""
    assert _run_git(target, "rev-parse", "HEAD") == head


def test_unpatched_adapter_is_fail_closed(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    _point_adapter_at_missing_executable(config_path, tmp_path)
    head = _run_git(target, "rev-parse", "HEAD")
    calls: list[str] = []
    released_ownership: list[Any] = []
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_adapter=False,
    )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "ADAPTER_EXECUTABLE_NOT_FOUND")
    assert stdout["reason_code"] != "CURSOR_ADAPTER_UNAVAILABLE"
    _assert_prefix_order(calls)
    assert "cursor_adapter" not in calls
    assert "task_end_prepare" not in calls
    assert "task_end_finalize" not in calls
    _assert_released_same_ownership(released_ownership)
    assert not (runtime / "promotion_journal.yaml").exists()
    assert not (runtime / "runner_git.lock").exists()
    assert _run_git(target, "status", "--porcelain") == ""
    assert _run_git(target, "rev-parse", "HEAD") == head


def test_task_end_finalize_and_drain_encapsulate_pending_and_effect(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_git_promotion as promotion_module

    config_path, _target, runtime = _write_public_world(tmp_path)
    calls: list[str] = []
    released_ownership: list[Any] = []
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_adapter=True,
        patch_task_end=True,
    )
    monkeypatch.setattr(
        promotion_module,
        "persist_pending_promotion",
        _forbid_direct_bypass("persist_pending_promotion"),
    )
    monkeypatch.setattr(
        promotion_module,
        "admit_pending_promotion_effect",
        _forbid_direct_bypass("admit_pending_promotion_effect"),
    )

    _ = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert stdout.get("reason_code") != "CURSOR_ADAPTER_UNAVAILABLE"
    assert stdout["outcome"] not in _FORBIDDEN_OUTCOMES
    assert stdout.get("source_authority_ok") is not True
    assert stdout.get("journal_start") is not True
    assert stdout.get("tock_sha") in {None, ""}
    _assert_prefix_order(calls)
    assert calls.index("drain_before_dispatch") < calls.index("activation")
    assert calls.index("activation") < calls.index("continuation")
    assert calls.index("continuation") < calls.index("cursor_adapter")
    assert "cursor_adapter" in calls
    assert "task_end_prepare" in calls
    assert "task_end_finalize" in calls
    assert "return_to_main" in calls
    assert calls.index("cursor_adapter") < calls.index("task_end_prepare")
    assert calls.index("task_end_prepare") < calls.index("task_end_finalize")
    assert calls.index("task_end_finalize") < calls.index("return_to_main")
    assert calls.index("return_to_main") < calls.index("lock_release")
    assert stdout.get("pending", {}).get("artifact_class") == _ARTIFACT_CLASS
    _assert_released_same_ownership(released_ownership)
    assert not (runtime / "promotion_journal.yaml").exists()


def test_status_is_read_only_and_separated_from_init_run_effect(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    (runtime / "pending_promotion.yaml").write_text(
        yaml.safe_dump(
            {
                "artifact_class": _ARTIFACT_CLASS,
                "task_id": "synthetic-status-task",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    before = _snapshot(target, runtime)

    def _must_not_run(name: str) -> Any:
        def _inner(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError(f"status must not call {name}")

        return _inner

    for name in (
        "acquire_runner_git_lock",
        "bootstrap_main_state",
        "evaluate_drain_before_dispatch",
        "prepare_git_promotion_task_end_artifacts",
        "finalize_git_promotion_task_end_artifacts",
        "persist_pending_promotion",
        "admit_pending_promotion_effect",
        "evaluate_task_activation",
        "return_to_main",
        "start_cursor_adapter",
        "reconcile_ordinary_promotion",
        "bind_ordinary_continuation",
    ):
        monkeypatch.setattr(cli_module, name, _must_not_run(name), raising=False)

    exit_code = main(["status", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 0
    assert stdout["ok"] is True
    assert stdout["command"] == "status"
    assert stdout["outcome"] == "STATUS_VIEW_PROJECTED"
    assert stdout["reason_code"] == "STATUS_VIEW_PROJECTED"
    assert stdout["outcome"] not in _FORBIDDEN_OUTCOMES
    assert stdout.get("source_authority_ok") is not True
    assert "SOURCE_AUTHORITY_OK" not in yaml.safe_dump(stdout)
    assert _snapshot(target, runtime) == before
    assert (runtime / "pending_promotion.yaml").is_file()


def test_status_json_flag_emits_json_without_mutation(
    tmp_path: Path,
    capsys: Any,
) -> None:
    config_path, target, runtime = _write_public_world(tmp_path)
    before = _snapshot(target, runtime)

    exit_code = main(["status", "--config", str(config_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["command"] == "status"
    assert payload["outcome"] == "STATUS_VIEW_PROJECTED"
    assert payload["reason_code"] == "STATUS_VIEW_PROJECTED"
    assert payload["outcome"] not in _FORBIDDEN_OUTCOMES
    assert _snapshot(target, runtime) == before


def test_cli_does_not_define_task_end_helpers() -> None:
    import task_transition_runner.cli as cli_module

    source = Path(cli_module.__file__).read_text(encoding="utf-8")
    for helper in _TASK_END_HELPERS:
        assert f"def {helper}(" not in source
    assert "def prepare_git_promotion_task_end_artifacts(" not in source
    assert "def finalize_git_promotion_task_end_artifacts(" not in source
    assert "def compose_adapter_git_worker_result(" not in source
    exported = {name for name in dir(cli_module) if not name.startswith("_")}
    for helper in _TASK_END_HELPERS:
        assert helper not in exported


_CLI_TASK_ID = "synthetic-task-001"
_CLI_ORIGIN = "origin-run-621"
_CLI_ATTEMPT = "promotion-attempt-621"
_CLI_BRANCH = "runner/task/synthetic-task-001/attempt-621"
_CLI_ALLOWED = ("src/example.py", "tests/test_example.py")
_SYNTHETIC_NONMUTATING_TASK_COMMAND = f"{sys.executable} -c pass"
_FROZEN_J_DRAFT_SHA256 = {
    "src/task_transition_runner/task_end_promotion.py": (
        "47ec04529d643f4f33d81a97e228051bec124e19f66c2beb347fdf5f0a89c536"
    ),
    "src/task_transition_runner/cli.py": (
        "0b130d7277bf67ef3bc3b5a92b66e7a8eeb5361e6b0d4eeb5c4434656d144d98"
    ),
    "src/task_transition_runner/runner_git_promotion.py": (
        "d5d22dd6b25c059447db57e321099e88fd33e00449ceb825d6a9ec842f1d9316"
    ),
}
_FROZEN_J_DRAFT_BINARY_DIFF_SHA256 = (
    "bebd631a322f28ae09c4832ae34f07e0a6c8a7b78b44b8ee966571f2d1df0bbb"
)
_FROZEN_READ_ONLY_COMPANION_SHA256 = {
    "tests/test_runner_git_promotion.py": (
        "2b78153f13b263960b623683c4ba0b2c2661ad0ddb2faaec12bf427fac14be08"
    ),
    "tests/integration/test_direct_green.py": (
        "2611416b207aea4f7461084d03f9277865382b21dfe493c0d5adfd457cc86b17"
    ),
}
_CLI_LIVE = "SYNTHETIC-OSS621-CANARY-LIVE-REREAD"
_CLI_DEVELOPER_SNAPSHOT = "instructions/prompts/developer"
_CLI_SPEC_SNAPSHOT = "instructions/task_spec"
_CLI_STREAM_BLOCK = "adapter_stream_block.yaml"
_CLI_PRODUCT_CANARIES = (
    "worker_result.py",
    "task_end_promotion.py",
    "cli.py",
    "adapters/cursor_headless.py",
    "adapters/cursor_stream.py",
)


def _cli_product_canary() -> dict[str, str]:
    import task_transition_runner.cli as cli_module

    package = Path(cli_module.__file__).resolve().parent
    return {
        relative: hashlib.sha256((package / relative).read_bytes()).hexdigest()
        for relative in _CLI_PRODUCT_CANARIES
    }


def _cli_identity() -> SimpleNamespace:
    return SimpleNamespace(
        task_id=_CLI_TASK_ID,
        origin_run_id=_CLI_ORIGIN,
        promotion_attempt_id=_CLI_ATTEMPT,
    )


def _cli_activation(calls: list[str]) -> Any:
    def evaluate_task_activation(*args: Any, **kwargs: Any) -> Any:
        calls.append("activation")
        return SimpleNamespace(
            status=ActivationStatus.ACTIVATED,
            reason_code="FRESH_TASK_ACTIVATED",
            identity=_cli_identity(),
        )

    return evaluate_task_activation


def _commit_cli_result(repo: Path) -> tuple[str, tuple[str, ...]]:
    _run_git(repo, "checkout", "-b", _CLI_BRANCH)
    (repo / "src").mkdir(parents=True)
    (repo / "tests").mkdir(parents=True)
    (repo / "src" / "example.py").write_text("value = 1\n", encoding="utf-8")
    (repo / "tests" / "test_example.py").write_text(
        "def test_example() -> None:\n    assert True\n",
        encoding="utf-8",
    )
    _run_git(repo, "add", "--", "src/example.py", "tests/test_example.py")
    _run_git(repo, "commit", "-m", "synthetic cli task result")
    names = _run_git(repo, "diff", "--name-only", _REFERENCE_MAIN_BRANCH, "HEAD")
    return _run_git(repo, "rev-parse", "HEAD"), tuple(names.splitlines())


def _write_active_task(config_path: Path, target: Path) -> tuple[Path, Path]:
    config_root = config_path.parent
    spec_path = config_root / "tasks" / "synthetic-task-001.md"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_bytes(b"BOUND-TASK-SPEC-SNAPSHOT\n")
    task_source = config_root / "tasks.yaml"
    task_source.write_text(
        yaml.safe_dump(
            {
                "schema_version": _SCHEMA_VERSION,
                "active_task": {
                    "task_id": _CLI_TASK_ID,
                    "spec_ref": "./tasks/synthetic-task-001.md",
                    "allowed_paths": list(_CLI_ALLOWED),
                    "validation": {
                        "commands": [
                            {
                                "id": "tests",
                                "command": _SYNTHETIC_NONMUTATING_TASK_COMMAND,
                                "timeout_seconds": 30,
                            }
                        ]
                    },
                    "expected_outcome": {"kind": "GREEN"},
                    "adapter": {"kind": "cursor"},
                    "metadata": {},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    assert spec_path.is_file()
    assert target.is_dir()
    return task_source, spec_path


def _bind_cli_snapshots(
    runtime: Path,
    *,
    developer_bytes: bytes,
    spec_bytes: bytes,
) -> Path:
    root = runtime / "runs" / _CLI_ORIGIN
    developer = root / _CLI_DEVELOPER_SNAPSHOT
    spec = root / _CLI_SPEC_SNAPSHOT
    developer.parent.mkdir(parents=True, exist_ok=True)
    spec.parent.mkdir(parents=True, exist_ok=True)
    developer.write_bytes(developer_bytes)
    spec.write_bytes(spec_bytes)
    return root


def _cli_outcome(*, completed: bool) -> CursorAdapterOutcome:
    if completed:
        projection = {
            "adapter_status": "completed",
            "neutral_reason_code": "ADAPTER_COMPLETED",
            "process_exit_code": 0,
            "timeout": False,
            "interrupted": False,
            "event_counts": {
                "assistant": 1,
                "result": 1,
                "system": 1,
                "tool_call": 0,
                "unknown": 0,
                "user": 1,
            },
            "session_observed": True,
            "terminal_success": True,
        }
    else:
        projection = {
            "adapter_status": "interrupted",
            "neutral_reason_code": "ADAPTER_INTERRUPTED",
            "process_exit_code": 1,
            "timeout": False,
            "interrupted": True,
            "event_counts": {
                "assistant": 0,
                "result": 0,
                "system": 0,
                "tool_call": 0,
                "unknown": 0,
                "user": 0,
            },
            "session_observed": False,
            "terminal_success": False,
        }
    return CursorAdapterOutcome(
        publish_projection=projection,
        prompt_frame_matches=True,
        stdout_artifact="SYNTHETIC-OSS621-CANARY-STDOUT",
        stderr_artifact="SYNTHETIC-OSS621-CANARY-STDERR",
    )


def _install_composition_spy(
    monkeypatch: pytest.MonkeyPatch,
    cli_module: Any,
    captured: list[dict[str, Any]],
    *,
    worker_result: WorkerResult,
) -> None:
    import task_transition_runner.task_end_promotion as task_end_module

    def compose_adapter_git_worker_result(**kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {
            "ok": True,
            "outcome": "WORKER_RESULT_READY",
            "reason_code": "ADAPTER_COMPLETED",
            "promotion_authorized": False,
            "replayed": kwargs.get("adapter_outcome") is None,
            "worker_rerun": kwargs.get("adapter_outcome") is not None,
            "resumed": False,
            "result_sha": worker_result.result_sha,
            "changed_paths": worker_result.changed_paths,
            "worker_result": worker_result,
            "publish_projection": None,
            "snapshot_binding": {
                "developer_sha256": kwargs["developer"].sha256,
                "developer_byte_length": kwargs["developer"].byte_length,
                "task_spec_sha256": kwargs["task_spec"].sha256,
                "task_spec_byte_length": kwargs["task_spec"].byte_length,
            },
        }

    monkeypatch.setattr(
        task_end_module,
        "compose_adapter_git_worker_result",
        compose_adapter_git_worker_result,
        raising=False,
    )
    monkeypatch.setattr(
        cli_module,
        "compose_adapter_git_worker_result",
        compose_adapter_git_worker_result,
        raising=False,
    )


def test_run_composes_bound_snapshots_before_persisting_worker_result(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    _write_active_task(config_path, target)
    head, paths = _commit_cli_result(target)
    developer_path = config_path.parent / "prompts" / "developer.md"
    spec_path = config_path.parent / "tasks" / "synthetic-task-001.md"
    bound_developer = developer_path.read_bytes()
    bound_spec = spec_path.read_bytes()
    _bind_cli_snapshots(
        runtime,
        developer_bytes=bound_developer,
        spec_bytes=bound_spec,
    )
    developer_path.write_text(_CLI_LIVE + "\n", encoding="utf-8")
    spec_path.write_text(_CLI_LIVE + "\n", encoding="utf-8")
    calls: list[str] = []
    released_ownership: list[Any] = []
    captured: list[dict[str, Any]] = []
    outcome = _cli_outcome(completed=True)
    worker_result = WorkerResult(
        schema_version=_SCHEMA_VERSION,
        result_type="worker_result",
        task_id=_CLI_TASK_ID,
        origin_run_id=_CLI_ORIGIN,
        promotion_attempt_id=_CLI_ATTEMPT,
        task_branch=_CLI_BRANCH,
        result_sha=head,
        changed_paths=paths,
        validation_results=(),
        worker_report=WorkerReport(
            reported_status="success",
            summary="ADAPTER_COMPLETED",
        ),
    )
    product = _cli_product_canary()
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_adapter=False,
        patch_task_end=False,
        patch_composition=False,
    )
    monkeypatch.setattr(cli_module, "evaluate_task_activation", _cli_activation(calls))
    _install_composition_spy(
        monkeypatch,
        cli_module,
        captured,
        worker_result=worker_result,
    )

    def start_cursor_adapter(config: object) -> dict[str, Any]:
        calls.append("cursor_adapter")
        return {
            "ok": True,
            "status": "traversal",
            "reason_code": "ADAPTER_COMPLETED",
            "adapter_outcome": outcome,
        }

    def finalize_git_promotion_task_end_artifacts(**kwargs: Any) -> dict[str, Any]:
        calls.append("task_end_finalize")
        return {
            "ok": True,
            "artifact_class": _ARTIFACT_CLASS,
            "reason_code": "PENDING_INTENT_PERSISTED",
            "journal_start": False,
            "tock_sha": None,
        }

    monkeypatch.setattr(cli_module, "start_cursor_adapter", start_cursor_adapter)
    monkeypatch.setattr(
        cli_module,
        "finalize_git_promotion_task_end_artifacts",
        finalize_git_promotion_task_end_artifacts,
    )

    try:
        exit_code = main(["run", "--config", str(config_path)])
        stdout = _read_stdout_yaml(capsys.readouterr().out)
    except TypeError as exc:
        pytest.fail(f"run reached prepare without a composed WorkerResult: {exc}")

    assert captured, "run did not compose bound adapter facts into WorkerResult"
    assert len(captured) == 1
    sent = captured[0]
    assert Path(sent["repo_root"]).resolve() == target.resolve()
    assert Path(sent["runtime_root"]).resolve() == runtime.resolve()
    assert sent["task_id"] == _CLI_TASK_ID
    assert sent["origin_run_id"] == _CLI_ORIGIN
    assert sent["promotion_attempt_id"] == _CLI_ATTEMPT
    assert sent["task_branch"] == _CLI_BRANCH
    assert sent["claimed_identity"] is None
    assert tuple(sent["allowed_paths"]) == _CLI_ALLOWED
    assert sent["adapter_outcome"] is outcome
    assert isinstance(sent["developer"], BoundPromptBytes)
    assert isinstance(sent["task_spec"], BoundPromptBytes)
    assert sent["developer"].payload_bytes == bound_developer
    assert sent["task_spec"].payload_bytes == bound_spec
    assert sent["developer"].sha256 == hashlib.sha256(bound_developer).hexdigest()
    assert sent["task_spec"].sha256 == hashlib.sha256(bound_spec).hexdigest()
    worker_path = runtime / "runs" / _CLI_ORIGIN / "worker_result.yaml"
    persisted = worker_path.read_text(encoding="utf-8")
    assert head in persisted
    assert _CLI_LIVE not in persisted
    assert "SYNTHETIC-OSS621-CANARY-STDOUT" not in persisted
    assert not (runtime / "pending_promotion.yaml").exists()
    assert stdout.get("promotion_authorized") is not True
    assert exit_code == 0
    assert _run_git(target, "rev-parse", "HEAD") == head
    assert _cli_product_canary() == product
    _assert_released_same_ownership(released_ownership)


def test_run_does_not_rerun_adapter_for_identical_verified_result(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    _write_active_task(config_path, target)
    head, paths = _commit_cli_result(target)
    developer_path = config_path.parent / "prompts" / "developer.md"
    spec_path = config_path.parent / "tasks" / "synthetic-task-001.md"
    bound_developer = developer_path.read_bytes()
    bound_spec = spec_path.read_bytes()
    _bind_cli_snapshots(
        runtime,
        developer_bytes=bound_developer,
        spec_bytes=bound_spec,
    )
    developer_path.write_text(_CLI_LIVE + "\n", encoding="utf-8")
    spec_path.write_text(_CLI_LIVE + "\n", encoding="utf-8")
    worker_result = WorkerResult(
        schema_version=_SCHEMA_VERSION,
        result_type="worker_result",
        task_id=_CLI_TASK_ID,
        origin_run_id=_CLI_ORIGIN,
        promotion_attempt_id=_CLI_ATTEMPT,
        task_branch=_CLI_BRANCH,
        result_sha=head,
        changed_paths=paths,
        validation_results=(),
        worker_report=WorkerReport(
            reported_status="success",
            summary="ADAPTER_COMPLETED",
        ),
    )
    prepare_git_promotion_task_end_artifacts(
        repo_root=target,
        runtime_root=runtime,
        worker_result=worker_result,
        allowed_paths=_CLI_ALLOWED,
    )
    worker_path = runtime / "runs" / _CLI_ORIGIN / "worker_result.yaml"
    before_worker = worker_path.read_bytes()
    calls: list[str] = []
    released_ownership: list[Any] = []
    captured: list[dict[str, Any]] = []
    adapter_calls: list[str] = []
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_adapter=False,
        patch_task_end=True,
        patch_composition=False,
    )
    monkeypatch.setattr(cli_module, "evaluate_task_activation", _cli_activation(calls))
    _install_composition_spy(
        monkeypatch,
        cli_module,
        captured,
        worker_result=worker_result,
    )
    original_adapter = cli_module.start_cursor_adapter

    def start_cursor_adapter(config: object) -> dict[str, Any]:
        adapter_calls.append("cursor_adapter")
        return original_adapter(config)

    monkeypatch.setattr(cli_module, "start_cursor_adapter", start_cursor_adapter)

    _ = main(["run", "--config", str(config_path)])
    _ = capsys.readouterr()

    assert adapter_calls == []
    assert len(captured) == 1
    assert captured[0]["adapter_outcome"] is None
    assert captured[0]["developer"].payload_bytes == bound_developer
    assert captured[0]["task_spec"].payload_bytes == bound_spec
    assert worker_path.read_bytes() == before_worker
    assert _run_git(target, "rev-parse", "HEAD") == head
    _assert_released_same_ownership(released_ownership)


def test_run_does_not_resume_interrupted_unverified_stream(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime = _write_public_world(tmp_path)
    calls: list[str] = []
    released_ownership: list[Any] = []
    adapter_calls: list[int] = []
    product = _cli_product_canary()
    head = _run_git(target, "rev-parse", "HEAD")
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_adapter=False,
        patch_composition=False,
    )
    monkeypatch.setattr(cli_module, "evaluate_task_activation", _cli_activation(calls))
    outcome = _cli_outcome(completed=False)

    def start_cursor_adapter(config: object) -> dict[str, Any]:
        adapter_calls.append(1)
        return {
            "ok": False,
            "status": "fail-closed",
            "reason_code": "ADAPTER_INTERRUPTED",
            "resumed": False,
            "adapter_outcome": outcome,
        }

    monkeypatch.setattr(cli_module, "start_cursor_adapter", start_cursor_adapter)

    first_exit = main(["run", "--config", str(config_path)])
    first_stdout = _read_stdout_yaml(capsys.readouterr().out)
    marker = runtime / "runs" / _CLI_ORIGIN / _CLI_STREAM_BLOCK

    assert first_exit == 1
    _blocked_stdout(first_stdout, "ADAPTER_INTERRUPTED")
    assert first_stdout.get("resumed") is not True
    assert marker.is_file()
    loaded = yaml.safe_load(marker.read_text(encoding="utf-8"))
    assert loaded == {
        "schema_version": _SCHEMA_VERSION,
        "kind": "interrupted_unverified_cursor_session",
        "task_id": _CLI_TASK_ID,
        "origin_run_id": _CLI_ORIGIN,
        "promotion_attempt_id": _CLI_ATTEMPT,
        "resumed": False,
    }
    assert not (runtime / "runs" / _CLI_ORIGIN / "worker_result.yaml").exists()

    second_exit = main(["run", "--config", str(config_path)])
    second_stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert second_exit == 1
    _blocked_stdout(second_stdout, "ADAPTER_INTERRUPTED")
    assert adapter_calls == [1]
    assert yaml.safe_load(marker.read_text(encoding="utf-8")) == loaded
    assert _run_git(target, "rev-parse", "HEAD") == head
    assert _cli_product_canary() == product
    assert not (runtime / "pending_promotion.yaml").exists()


@pytest.mark.parametrize(
    "reason_code",
    [
        "RESULT_IDENTITY_MISMATCH",
        "RESULT_GIT_MISMATCH",
        "RESULT_PATHS_OUTSIDE_ALLOWED",
    ],
)
def test_run_forwards_composition_block_codes_fail_closed(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    reason_code: str,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.task_end_promotion as task_end_module

    config_path, target, runtime = _write_public_world(tmp_path)
    _write_active_task(config_path, target)
    head, _paths = _commit_cli_result(target)
    developer_path = config_path.parent / "prompts" / "developer.md"
    spec_path = config_path.parent / "tasks" / "synthetic-task-001.md"
    bound_developer = developer_path.read_bytes()
    bound_spec = spec_path.read_bytes()
    _bind_cli_snapshots(
        runtime,
        developer_bytes=bound_developer,
        spec_bytes=bound_spec,
    )
    calls: list[str] = []
    released_ownership: list[Any] = []
    captured: list[dict[str, Any]] = []
    outcome = _cli_outcome(completed=True)
    product = _cli_product_canary()
    _install_doubles(
        monkeypatch,
        cli_module=cli_module,
        calls=calls,
        released_ownership=released_ownership,
        patch_adapter=False,
        patch_task_end=False,
        patch_composition=False,
    )
    monkeypatch.setattr(cli_module, "evaluate_task_activation", _cli_activation(calls))

    def compose_adapter_git_worker_result(**kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {
            "ok": False,
            "outcome": "BLOCKED",
            "reason_code": reason_code,
            "replayed": False,
            "worker_rerun": False,
            "resumed": False,
            "promotion_authorized": False,
        }

    monkeypatch.setattr(
        task_end_module,
        "compose_adapter_git_worker_result",
        compose_adapter_git_worker_result,
        raising=False,
    )
    monkeypatch.setattr(
        cli_module,
        "compose_adapter_git_worker_result",
        compose_adapter_git_worker_result,
        raising=False,
    )

    def start_cursor_adapter(config: object) -> dict[str, Any]:
        calls.append("cursor_adapter")
        return {
            "ok": True,
            "status": "traversal",
            "reason_code": "ADAPTER_COMPLETED",
            "adapter_outcome": outcome,
        }

    monkeypatch.setattr(cli_module, "start_cursor_adapter", start_cursor_adapter)

    try:
        exit_code = main(["run", "--config", str(config_path)])
        stdout = _read_stdout_yaml(capsys.readouterr().out)
    except TypeError as exc:
        pytest.fail(f"run reached prepare without the composition block: {exc}")

    assert captured, "run did not forward the composition block"
    _blocked_stdout(stdout, reason_code)
    assert stdout.get("resumed") is not True
    assert stdout.get("promotion_authorized") is not True
    assert exit_code == 1
    assert not (runtime / "runs" / _CLI_ORIGIN / "worker_result.yaml").exists()
    assert not (runtime / "pending_promotion.yaml").exists()
    assert _run_git(target, "rev-parse", "HEAD") == head
    assert _cli_product_canary() == product
    _assert_released_same_ownership(released_ownership)


_C10_RUN_STEPS = (
    "source_authority",
    "target_runtime_preflight",
    "lock_acquire",
    "reconciliation",
    "rest_or_task_admissibility",
    "task_and_snapshot",
    "continuation",
    "start_tick",
    "adapter",
    "worker_result",
    "decision_request",
    "awaiting_decisions",
    "validate_decisions",
    "pending_intent",
    "return_to_main",
    "promotion_drain",
    "continuation_consumption",
    "settlement",
)
_STEP4_WINDOWS = (
    "INCOMPLETE_PROMOTION",
    "CANONICAL_TOCK",
    "MAIN_STATE",
    "CONSUMPTION",
    "START_TICK_WINDOWS",
    "POST_DRAIN_TICK_WINDOWS",
)
_FRESH_UNTIL_AWAITING = (
    "source_authority",
    "target_runtime_preflight",
    "lock_acquire",
    *_STEP4_WINDOWS,
    "pre_dispatch_drain",
    "PRE_DISPATCH_DRAIN_RELOAD",
    "task_admissibility",
    "task_and_snapshot",
    "start_tick",
    "adapter",
    "worker_result",
    "decision_request",
    "lock_release",
)
_TERMINAL_REST_ORDER = (
    "source_authority",
    "target_runtime_preflight",
    "lock_acquire",
    *_STEP4_WINDOWS,
    "pre_dispatch_drain",
    "PRE_DISPATCH_DRAIN_RELOAD",
    "task_admissibility",
    "lock_release",
)
_TRANSITIONAL_SEAM_REASONS = (
    "CODE_AUTHORITY_UNAVAILABLE",
    "RECONCILIATION_UNAVAILABLE",
    "CONTINUATION_UNAVAILABLE",
    "CURSOR_ADAPTER_UNAVAILABLE",
    "RETURN_TO_MAIN_UNAVAILABLE",
    "D_COMPOSITION_TRAVERSAL",
)
_CLOSED_TOP_LEVEL_OUTCOMES = (
    "INITIALIZED",
    "TERMINAL_REST",
    "WORKER_RESULT_READY",
    "AWAITING_DECISIONS",
    "PENDING_PROMOTION",
    "DRAINED_AWAITING_TASK",
    "SETTLED",
    "RECOVERED",
    "BLOCKED",
)
_ALLOWED_RUN_OUTCOMES = _CLOSED_TOP_LEVEL_OUTCOMES
_FROZEN_READ_ONLY_PRODUCT_SHA256 = {
    "runner_drain_transition.py": "22ceb72c839aa42a18fba41a2234a64d16f0ad6052dd6498292441f36046b9f3",
}
_DESTRUCTIVE_GIT_ARGUMENTS = (
    ("reset", "--hard"),
    ("checkout", "-f"),
    ("checkout", "--force"),
    ("clean", "-fd"),
    ("clean", "-fdx"),
)


def _assert_frozen_read_only_product_bytes() -> None:
    import task_transition_runner.cli as cli_module

    package = Path(cli_module.__file__).resolve().parent
    for name, expected in _FROZEN_READ_ONLY_PRODUCT_SHA256.items():
        actual = hashlib.sha256((package / name).read_bytes()).hexdigest()
        assert actual == expected, name


def _write_fresh_main_state(runtime: Path, target: Path) -> dict[str, str]:
    head = _run_git(target, "rev-parse", "HEAD")
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": _REFERENCE_MAIN_BRANCH,
        "validated_result_sha": head,
        "last_tock_sha": head,
        "observed_main_head_sha": head,
        "reason_code": "BOOTSTRAP_OK",
        "updated_at_utc": "2026-09-30T12:00:00Z",
    }
    (runtime / "main_state.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return payload


def _bind_fresh_synthetic_adapter(config_path: Path, tmp_path: Path) -> None:
    executable = tmp_path / "synthetic-cursor-adapter"
    executable.write_text(
        f"#!{sys.executable}\n{_SYNTHETIC_ADAPTER_PROGRAM}",
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["adapter"]["executable"] = str(executable.resolve())
    payload["adapter"]["timeout_seconds"] = _SYNTHETIC_ADAPTER_TIMEOUT_SECONDS
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _prepare_fresh_surface(
    tmp_path: Path,
    *,
    active_task: bool,
    pin_source: bool = True,
) -> tuple[Path, Path, Path]:
    config_path, target, runtime = _write_public_world(tmp_path)
    _write_fresh_main_state(runtime, target)
    _bind_fresh_synthetic_adapter(config_path, tmp_path)
    if pin_source:
        _pin_runtime_to_source(runtime)
    if active_task:
        _write_active_task(config_path, target)
    return config_path, target, runtime


def _install_complete_surface_observers(
    monkeypatch: pytest.MonkeyPatch,
    observed: list[str],
    basis_calls: list[dict[str, Any]],
    lock_events: dict[str, Any],
    git_argv: list[tuple[str, ...]],
    results: dict[str, Any] | None = None,
) -> None:
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    import task_transition_runner.adapters.cursor_headless as adapter_module
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_code_authority as authority_module
    import task_transition_runner.runner_drain_transition as drain_module
    import task_transition_runner.runner_git_context as git_context_module
    import task_transition_runner.runner_promotion_reconciliation as reconciliation_module
    import task_transition_runner.runner_promotion_state as state_module
    import task_transition_runner.runner_start_tick_reconciliation as start_tick_module
    import task_transition_runner.task_end_promotion as task_end_module
    import task_transition_runner.task_source as task_source_module

    def _keep(label: str, value: Any) -> Any:
        if results is not None:
            results[label] = value
        return value

    def _record(label: str, original: Any) -> Any:
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            observed.append(label)
            return _keep(label, original(*args, **kwargs))

        return _wrapped

    def _record_sentinel(original: Any) -> Any:
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            reason = result.get("reason_code") if isinstance(result, dict) else None
            if reason in _TRANSITIONAL_SEAM_REASONS:
                observed.append(f"sentinel:{reason}")
            return result

        return _wrapped

    start_tick_calls = {"count": 0}
    later_start_tick = {"active": False}
    original_start_tick = start_tick_module.reconcile_start_tick

    def _reconcile_start_tick(*args: Any, **kwargs: Any) -> Any:
        start_tick_calls["count"] += 1
        later = start_tick_calls["count"] > 1
        observed.append("start_tick" if later else "START_TICK_WINDOWS")
        later_start_tick["active"] = later
        try:
            return original_start_tick(*args, **kwargs)
        finally:
            later_start_tick["active"] = False

    def _acquire(**kwargs: Any) -> Any:
        observed.append("lock_acquire")
        result = git_context_module.acquire_runner_git_lock(**kwargs)
        lock_events["acquired"] = result.get("ownership")
        return result

    def _release(**kwargs: Any) -> Any:
        observed.append("lock_release")
        lock_events["released"] = kwargs.get("ownership")
        return git_context_module.release_runner_git_lock(**kwargs)

    def _run_git(argv: list[str], **kwargs: Any) -> Any:
        git_argv.append(tuple(argv))
        return original_run(argv, **kwargs)

    original_run = subprocess.run
    monkeypatch.setattr(subprocess, "run", _run_git)
    monkeypatch.setattr(
        authority_module,
        "verify_steady_state_code_authority",
        _record("source_authority", authority_module.verify_steady_state_code_authority),
    )
    monkeypatch.setattr(
        git_context_module,
        "evaluate_branch_preflight",
        _record("target_runtime_preflight", git_context_module.evaluate_branch_preflight),
    )
    monkeypatch.setattr(
        reconciliation_module,
        "reconcile_ordinary_promotion",
        _record("INCOMPLETE_PROMOTION", reconciliation_module.reconcile_ordinary_promotion),
    )
    monkeypatch.setattr(
        reconciliation_module,
        "observe_canonical_first_parent_tock",
        _record("CANONICAL_TOCK", reconciliation_module.observe_canonical_first_parent_tock),
    )
    original_main_state = state_module.load_and_validate_main_state

    def _load_main_state(*args: Any, **kwargs: Any) -> Any:
        label = "START_TICK_MAIN_STATE" if later_start_tick["active"] else "MAIN_STATE"
        observed.append(label)
        return _keep(label, original_main_state(*args, **kwargs))

    monkeypatch.setattr(state_module, "load_and_validate_main_state", _load_main_state)
    monkeypatch.setattr(
        state_module,
        "load_promotion_consumed",
        _record("CONSUMPTION", state_module.load_promotion_consumed),
    )
    monkeypatch.setattr(start_tick_module, "reconcile_start_tick", _reconcile_start_tick)
    drain_reload_scope = {"inside_pre_dispatch": False}
    original_load = drain_module.load_persisted_drain_outcome
    original_pre_dispatch = drain_module.evaluate_drain_before_dispatch

    def _load_persisted_drain_outcome(*args: Any, **kwargs: Any) -> Any:
        if drain_reload_scope["inside_pre_dispatch"]:
            observed.append("PRE_DISPATCH_DRAIN_RELOAD")
        else:
            observed.append("POST_DRAIN_TICK_WINDOWS")
        return original_load(*args, **kwargs)

    def _pre_dispatch_drain(*args: Any, **kwargs: Any) -> Any:
        observed.append("pre_dispatch_drain")
        drain_reload_scope["inside_pre_dispatch"] = True
        try:
            decision = original_pre_dispatch(*args, **kwargs)
        finally:
            drain_reload_scope["inside_pre_dispatch"] = False
        return _keep("pre_dispatch_drain", decision)

    monkeypatch.setattr(
        drain_module,
        "load_persisted_drain_outcome",
        _load_persisted_drain_outcome,
    )
    monkeypatch.setattr(
        task_source_module,
        "load_task_source",
        _record("task_and_snapshot", task_source_module.load_task_source),
    )
    monkeypatch.setattr(
        adapter_module,
        "run_cursor_adapter",
        _record("adapter", adapter_module.run_cursor_adapter),
    )
    monkeypatch.setattr(
        drain_module,
        "admit_pending_promotion_effect",
        _record("promotion_drain", drain_module.admit_pending_promotion_effect),
    )
    compose = _record("worker_result", task_end_module.compose_adapter_git_worker_result)
    monkeypatch.setattr(task_end_module, "compose_adapter_git_worker_result", compose)
    monkeypatch.setattr(cli_module, "compose_adapter_git_worker_result", compose)
    prepare = _record(
        "decision_request",
        task_end_module.prepare_git_promotion_task_end_artifacts,
    )
    monkeypatch.setattr(task_end_module, "prepare_git_promotion_task_end_artifacts", prepare)
    monkeypatch.setattr(cli_module, "prepare_git_promotion_task_end_artifacts", prepare)
    monkeypatch.setattr(cli_module, "acquire_runner_git_lock", _acquire)
    monkeypatch.setattr(cli_module, "release_runner_git_lock", _release)
    monkeypatch.setattr(cli_module, "evaluate_drain_before_dispatch", _pre_dispatch_drain)
    monkeypatch.setattr(cli_module, "verify_code_authority", _record_sentinel(cli_module.verify_code_authority))
    monkeypatch.setattr(
        cli_module,
        "reconcile_ordinary_promotion",
        _record_sentinel(cli_module.reconcile_ordinary_promotion),
    )
    monkeypatch.setattr(
        cli_module,
        "bind_ordinary_continuation",
        _record_sentinel(cli_module.bind_ordinary_continuation),
    )
    monkeypatch.setattr(
        cli_module,
        "start_cursor_adapter",
        _record_sentinel(cli_module.start_cursor_adapter),
    )
    original_return = cli_module.return_to_main

    def _return_to_main(config: object) -> Any:
        result = original_return(config)
        reason = result.get("reason_code") if isinstance(result, dict) else None
        if reason in _TRANSITIONAL_SEAM_REASONS:
            observed.append(f"sentinel:{reason}")
        else:
            observed.append("return_to_main")
        return _keep("return_to_main", result)

    monkeypatch.setattr(cli_module, "return_to_main", _return_to_main)
    import task_transition_runner.decision_contracts as decision_module
    import task_transition_runner.runner_continuation_lifecycle as continuation_module
    import task_transition_runner.runner_post_tick_continuation as post_tick_module

    continuation = _record(
        "continuation",
        continuation_module.prepare_post_tick_continuation,
    )
    monkeypatch.setattr(continuation_module, "prepare_post_tick_continuation", continuation)
    monkeypatch.setattr(cli_module, "prepare_post_tick_continuation", continuation, raising=False)
    consumption = _record(
        "continuation_consumption",
        post_tick_module.consume_post_tick_continuation,
    )
    monkeypatch.setattr(post_tick_module, "consume_post_tick_continuation", consumption)
    monkeypatch.setattr(cli_module, "consume_post_tick_continuation", consumption, raising=False)
    validate = _record(
        "validate_decisions",
        decision_module.classify_orchestrator_decision,
    )
    monkeypatch.setattr(decision_module, "classify_orchestrator_decision", validate)
    monkeypatch.setattr(cli_module, "classify_orchestrator_decision", validate, raising=False)
    pending_intent = _record(
        "pending_intent",
        task_end_module.finalize_git_promotion_task_end_artifacts,
    )
    monkeypatch.setattr(
        task_end_module,
        "finalize_git_promotion_task_end_artifacts",
        pending_intent,
    )
    monkeypatch.setattr(cli_module, "finalize_git_promotion_task_end_artifacts", pending_intent)
    original_activation = cli_module.evaluate_task_activation

    def _activate_once(*args: Any, **kwargs: Any) -> Any:
        observed.append("task_admissibility")
        basis_calls.append({"args": args, "kwargs": dict(kwargs)})
        return original_activation(*args, **kwargs)

    monkeypatch.setattr(cli_module, "evaluate_task_activation", _activate_once)
    original_dump = yaml.safe_dump

    def _dump_settled(*args: Any, **kwargs: Any) -> Any:
        payload = args[0] if args else kwargs.get("data")
        if (
            isinstance(payload, dict)
            and payload.get("command") == "run"
            and payload.get("outcome") == "SETTLED"
        ):
            observed.append("settlement")
        return original_dump(*args, **kwargs)

    monkeypatch.setattr(yaml, "safe_dump", _dump_settled)


def _foreign_bytes(target: Path) -> tuple[str, str]:
    foreign = target / "foreign-user.txt"
    foreign_text = foreign.read_text(encoding="utf-8") if foreign.is_file() else ""
    return foreign_text, (target / "tracked.txt").read_text(encoding="utf-8")


def _assert_no_destructive_git(git_argv: list[tuple[str, ...]]) -> None:
    for argv in git_argv:
        for destructive in _DESTRUCTIVE_GIT_ARGUMENTS:
            assert destructive != argv[-len(destructive) :], argv


def _first_positions(observed: list[str], required: tuple[str, ...]) -> list[int]:
    positions: list[int] = []
    for label in required:
        assert label in observed, (label, observed)
        positions.append(observed.index(label))
    return positions


def _run_observed_surface(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    active_task: bool,
) -> tuple[dict[str, Any], list[str], list[dict[str, Any]], dict[str, Any], Path]:
    _assert_frozen_read_only_product_bytes()
    assert len(_C10_RUN_STEPS) == 18
    config_path, target, _runtime = _prepare_fresh_surface(tmp_path, active_task=active_task)
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
    )
    before_foreign = _foreign_bytes(target)
    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    assert _foreign_bytes(target) == before_foreign
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()
    return stdout, observed, basis_calls, lock_events, target


def test_complete_run_order_binds_real_components_until_awaiting_decisions(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout, observed, _basis_calls, _lock_events, _target = _run_observed_surface(
        tmp_path,
        capsys,
        monkeypatch,
        active_task=True,
    )

    sentinels = [label for label in observed if label.startswith("sentinel:")]
    assert sentinels == []
    assert "continuation" not in observed
    assert _first_positions(observed, _FRESH_UNTIL_AWAITING) == sorted(
        _first_positions(observed, _FRESH_UNTIL_AWAITING)
    )
    assert observed[-1] == "lock_release"
    assert observed.index("decision_request") < observed.index("lock_release")
    for label in _TAIL_LABELS:
        assert label not in observed, (label, observed)
    assert stdout["reason_code"] not in _TRANSITIONAL_SEAM_REASONS
    assert stdout["outcome"] == "AWAITING_DECISIONS"
    assert stdout["outcome"] in _ALLOWED_RUN_OUTCOMES


def test_step4_windows_precede_pre_dispatch_drain_and_task(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stdout, observed, _basis_calls, _lock_events, _target = _run_observed_surface(
        tmp_path,
        capsys,
        monkeypatch,
        active_task=True,
    )
    windows = [label for label in observed if label in _STEP4_WINDOWS]

    assert tuple(windows) == _STEP4_WINDOWS
    assert observed.index("POST_DRAIN_TICK_WINDOWS") < observed.index("pre_dispatch_drain")
    assert observed.index("pre_dispatch_drain") < observed.index("PRE_DISPATCH_DRAIN_RELOAD")
    assert observed.index("PRE_DISPATCH_DRAIN_RELOAD") < observed.index("task_admissibility")
    assert observed.index("pre_dispatch_drain") < observed.index("task_admissibility")
    assert "sentinel:RECONCILIATION_UNAVAILABLE" not in observed


def test_fresh_init_basis_is_selected_before_task_admissibility(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stdout, observed, basis_calls, _lock_events, _target = _run_observed_surface(
        tmp_path,
        capsys,
        monkeypatch,
        active_task=True,
    )

    assert basis_calls, observed
    sent = basis_calls[0]["kwargs"]["predecessor"]
    assert sent["kind"] == "FRESH_INIT_BASELINE"
    assert set(sent) == {"kind", "main_state"}
    assert "predecessor" not in sent
    assert "origin_run_id" not in sent["main_state"]
    assert "promotion_attempt_id" not in sent["main_state"]
    assert observed.index("task_admissibility") < observed.index("start_tick")


def test_null_active_task_ends_as_mutation_free_terminal_rest(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout, observed, basis_calls, lock_events, target = _run_observed_surface(
        tmp_path,
        capsys,
        monkeypatch,
        active_task=False,
    )

    assert stdout["outcome"] == "TERMINAL_REST"
    assert stdout["reason_code"] == "ACTIVE_TASK_NULL"
    assert stdout["reason_code"] not in _TRANSITIONAL_SEAM_REASONS
    assert observed == list(_TERMINAL_REST_ORDER)
    assert basis_calls[0]["kwargs"]["predecessor"]["kind"] == "FRESH_INIT_BASELINE"
    assert "adapter" not in observed
    assert "promotion_drain" not in observed
    assert lock_events["released"] == lock_events["acquired"]
    assert _run_git(target, "status", "--porcelain") == ""


def test_return_to_main_keeps_foreign_bytes_and_is_not_the_unavailable_sentinel(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout, observed, _basis_calls, lock_events, _target = _run_observed_surface(
        tmp_path,
        capsys,
        monkeypatch,
        active_task=True,
    )

    assert "sentinel:RETURN_TO_MAIN_UNAVAILABLE" not in observed
    assert "return_to_main" not in observed
    assert "promotion_drain" not in observed
    for label in _TAIL_LABELS:
        assert label not in observed, (label, observed)
    assert stdout["outcome"] in _ALLOWED_RUN_OUTCOMES
    assert stdout["outcome"] != "D_COMPOSITION_TRAVERSAL"
    assert lock_events["released"] == lock_events["acquired"]


def test_fresh_negative_adapter_exit_is_blocked_without_tail(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _assert_frozen_read_only_product_bytes()
    config_path, target, _runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    _point_adapter_at_missing_executable(config_path, tmp_path)
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
    )
    before_foreign = _foreign_bytes(target)

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert _foreign_bytes(target) == before_foreign
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()
    assert exit_code == 1
    assert stdout["outcome"] == "BLOCKED"
    assert stdout["outcome"] in _ALLOWED_RUN_OUTCOMES
    for label in _TAIL_LABELS:
        assert label not in observed, (label, observed)


def test_fresh_composition_exception_is_blocked_without_tail(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.task_end_promotion as task_end_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, _runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
    )

    def _raise_composition(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("composition failed")

    monkeypatch.setattr(
        task_end_module,
        "compose_adapter_git_worker_result",
        _raise_composition,
    )
    monkeypatch.setattr(
        cli_module,
        "compose_adapter_git_worker_result",
        _raise_composition,
    )
    before_foreign = _foreign_bytes(target)

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert _foreign_bytes(target) == before_foreign
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()
    assert exit_code == 1
    assert stdout["outcome"] == "BLOCKED"
    assert stdout["outcome"] in _ALLOWED_RUN_OUTCOMES
    for label in _TAIL_LABELS:
        assert label not in observed, (label, observed)


_SETTLED_PREDECESSOR_NAME = "settled_predecessor.yaml"
_TAIL_LABELS = (
    "validate_decisions",
    "pending_intent",
    "return_to_main",
    "promotion_drain",
    "continuation_consumption",
    "settlement",
)
_PUBLIC_OUTCOMES = (
    "WORKER_RESULT_READY",
    "PENDING_PROMOTION",
    "DRAINED_AWAITING_TASK",
    "SETTLED",
    "RECOVERED",
)


def _write_settled_predecessor(
    runtime: Path,
    *,
    task_id: str | None,
    settled: bool,
    origin_run_id: str = "origin-settled-predecessor",
    promotion_attempt_id: str = "attempt-settled-predecessor",
    task_fingerprint: str | None = None,
    instruction_sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "settled": settled,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
    }
    if task_id is not None:
        document["task_id"] = task_id
    if settled:
        document["task_fingerprint"] = task_fingerprint or ("ab" * 32)
        document["instruction_sources"] = instruction_sources or [
            {
                "logical_ref": "predecessor-spec",
                "sha256": "cd" * 32,
                "byte_length": 24,
            }
        ]
    (runtime / _SETTLED_PREDECESSOR_NAME).write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    return document


def _point_adapter_at_missing_executable(config_path: Path, tmp_path: Path) -> None:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["adapter"]["executable"] = str(tmp_path / "missing-cursor-agent")
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _active_task_identity(
    config_path: Path,
    runtime: Path,
) -> tuple[str, str, list[dict[str, Any]]]:
    from task_transition_runner.config import load_config
    from task_transition_runner.task_source import load_task_source

    config = load_config(config_path)
    facts = load_task_source(config, run_root=runtime / "identity-probe")
    active = facts.contract.active_task
    assert active is not None
    sources = [
        {
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "byte_length": item.byte_length,
        }
        for item in facts.sources
    ]
    return active.task_id, facts.contract.fingerprint, sources


def _rebind_canonical_predecessor_world(
    config_path: Path,
    tmp_path: Path,
) -> tuple[Any, Path, Path]:
    lifecycle = _load_continuation_lifecycle()
    seeded = lifecycle._seed(
        tmp_path / "canonical-predecessor",
        chain="none",
        lock=False,
    )
    target = seeded["repo"]
    runtime = seeded["runtime"]
    _retarget_public_config(config_path, target, runtime)
    _pin_runtime_to_source(runtime)
    return lifecycle, target, runtime


def _bind_active_task_to_canonical_predecessor(
    config_path: Path,
    runtime: Path,
    *,
    origin_run_id: str,
) -> tuple[str, str, list[dict[str, Any]]]:
    run_root = runtime / "runs" / origin_run_id
    task_snapshot = run_root / "task_snapshot.yaml"
    task_source = config_path.parent / "tasks.yaml"
    task_source.write_bytes(task_snapshot.read_bytes())

    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    instruction_snapshot = yaml.safe_load(
        (run_root / "instructions" / "instruction_snapshot.yaml").read_text(
            encoding="utf-8"
        )
    )
    prompt_roles = {
        "instructions/prompts/developer": "developer",
        "instructions/prompts/orchestrator": "orchestrator",
        "instructions/prompts/validator": "validator",
    }
    for source in instruction_snapshot["sources"]:
        logical_ref = str(source["logical_ref"])
        destination = (config_path.parent / logical_ref).resolve()
        assert destination.is_relative_to(config_path.parent.resolve())
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((run_root / source["snapshot_path"]).read_bytes())
        role = prompt_roles.get(str(source["snapshot_path"]))
        if role is not None:
            config["prompts"][role] = logical_ref
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return _active_task_identity(config_path, runtime)


def _runtime_snapshot(runtime: Path) -> tuple[tuple[str, bytes], ...]:
    files: list[tuple[str, bytes]] = []
    for path in sorted(runtime.rglob("*")):
        if path.is_file():
            files.append((str(path.relative_to(runtime)), path.read_bytes()))
    return tuple(files)


def _pin_runtime_to_source(runtime: Path) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_code_authority import (
        derive_source_root,
        persist_runtime_source_pin,
    )

    source = derive_source_root(Path(cli_module.__file__).resolve())
    persist_runtime_source_pin(
        runtime_root=runtime,
        source_sha=_run_git(source, "rev-parse", "HEAD"),
    )


def _plant_canonical_consumed_continuation(runtime: Path) -> None:
    from task_transition_runner.runner_continuation_contract import (
        binding_digest,
        load_continuation_artifact,
    )
    from task_transition_runner.runner_continuation_lifecycle import (
        prepare_post_tick_continuation,
    )

    lifecycle = _load_continuation_lifecycle()
    seeded = lifecycle._seed(runtime.parent / "canonical-consumed-source", lock=True)
    prepare_post_tick_continuation(**lifecycle._prepare_kwargs(seeded))
    payload = yaml.safe_load(
        (seeded["runtime"] / "post_tick_continuation.yaml").read_text(encoding="utf-8")
    )
    payload["status"] = "CONSUMED"
    payload["successor_promotion_attempt_id"] = lifecycle._SUCCESSOR_ATTEMPT_ID
    payload["successor_tock_sha"] = seeded["tock_sha"]
    payload["consumed_at_utc"] = "2026-09-26T12:05:00Z"
    origin = payload["origin"]
    payload["continuation_digest_sha256"] = binding_digest(
        {
            "schema_version": payload["schema_version"],
            "origin": {
                "kind": origin["kind"],
                "evidence": {
                    "predecessor_identity": dict(origin["evidence"]["predecessor_identity"]),
                    "tock_sha": origin["evidence"]["tock_sha"],
                },
            },
            "original_tick_sha": payload["original_tick_sha"],
            "expected_continuation_head_sha": payload["expected_continuation_head_sha"],
            "commit_chain": [dict(entry) for entry in payload["commit_chain"]],
            "successor_task_id": payload["successor_task_id"],
        }
    )
    destination = runtime / "post_tick_continuation.yaml"
    destination.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    loaded = load_continuation_artifact(destination)
    assert loaded.status == "CONSUMED"


def _plant_public_outcome_artifact(runtime: Path, target: Path, outcome: str) -> None:
    head = _run_git(target, "rev-parse", "HEAD")
    if outcome == "WORKER_RESULT_READY":
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "result_type": "worker_result",
            "task_id": _CLI_TASK_ID,
            "origin_run_id": "origin-public-outcome",
            "promotion_attempt_id": "attempt-public-outcome",
            "task_branch": _REFERENCE_MAIN_BRANCH,
            "result_sha": head,
            "changed_paths": [],
            "validation_results": [],
            "worker_report": {
                "reported_status": "success",
                "summary": "public projection worker",
            },
        }
        run_dir = runtime / "runs" / "origin-public-outcome"
        run_dir.mkdir(parents=True)
        text = yaml.safe_dump(payload, sort_keys=False)
        (run_dir / "worker_result.yaml").write_text(text, encoding="utf-8")
        (runtime / "worker_result.yaml").write_text(text, encoding="utf-8")
        return
    if outcome == "PENDING_PROMOTION":
        (runtime / "pending_promotion.yaml").write_text(
            yaml.safe_dump(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "artifact_class": _ARTIFACT_CLASS,
                    "task_id": _CLI_TASK_ID,
                    "origin_run_id": "origin-public-outcome",
                    "promotion_attempt_id": "attempt-public-outcome",
                    "result_sha": head,
                    "promotion_class": "GREEN_PROMOTION",
                    "expected_outcome": {"kind": "GREEN"},
                    "expected_main_state_after_promotion": {"state": "GREEN_VALIDATED"},
                    "base_sha": head,
                    "task_branch": _REFERENCE_MAIN_BRANCH,
                    "main_state_before_promotion": {"state": "GREEN_VALIDATED"},
                    "task_snapshot": {"current": {}, "bound": {}},
                    "instruction_snapshot": {"current": {}, "bound": {}},
                    "orchestrator_decision": {"state": "READY"},
                    "validator_decision": {"state": "READY"},
                    "allowed_paths": [],
                    "red_lock_closure": {"closed": True},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return
    if outcome == "DRAINED_AWAITING_TASK":
        from task_transition_runner.runner_drain_transition import (
            DrainOutcome,
            DrainStatus,
            persist_drain_outcome,
        )

        persist_drain_outcome(
            runtime_root=runtime,
            outcome=DrainOutcome(
                status=DrainStatus.DRAINED,
                drain_run_id="drain-public-outcome",
                observed_main_head_sha=head,
                reason_code="PROMOTION_EFFECT_ADMITTED",
                task_id=_CLI_TASK_ID,
                origin_run_id="origin-public-outcome",
                promotion_attempt_id="attempt-public-outcome",
                result_sha=head,
                promotion_class="GREEN_PROMOTION",
                tock_sha="b" * 40,
            ),
        )
        return
    if outcome == "SETTLED":
        _plant_canonical_consumed_continuation(runtime)
        return
    if outcome == "RECOVERED":
        (runtime / "main_state.yaml").write_text(
            yaml.safe_dump(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "state": "GREEN_VALIDATED",
                    "main_branch": _REFERENCE_MAIN_BRANCH,
                    "validated_result_sha": head,
                    "last_tock_sha": head,
                    "observed_main_head_sha": head,
                    "reason_code": "RECONCILIATION_COMPLETED",
                    "updated_at_utc": "2026-09-30T12:00:00Z",
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        _pin_runtime_to_source(runtime)
        return
    raise AssertionError(outcome)


def test_settled_predecessor_basis_visits_continuation_after_admissibility(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_start_tick_reconciliation as start_tick_module

    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=False,
    )
    lifecycle, target, runtime = _rebind_canonical_predecessor_world(
        config_path,
        tmp_path,
    )
    _write_active_task(config_path, target)
    journal = yaml.safe_load(
        (runtime / "promotion_journal.yaml").read_text(encoding="utf-8")
    )
    assert journal["task_id"] == lifecycle._TASK_ID
    origin = lifecycle.prove_finalized_promotion_tick(
        repo=target,
        runtime=runtime,
        main_ref=f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    )
    assert origin.to_mapping()["predecessor_identity"]["task_id"] == journal["task_id"]
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    binds: list[dict[str, Any]] = []
    transition_events: list[str] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
    )
    observed_bind = cli_module.bind_ordinary_continuation
    observed_start = start_tick_module.reconcile_start_tick

    def _bind(config: object) -> dict[str, Any]:
        transition_events.append("bind_gate")
        result = observed_bind(config)
        binds.append(result)
        return result

    def _start(*args: Any, **kwargs: Any) -> Any:
        callback = kwargs.get("before_main_adoption")
        if callback is None:
            return observed_start(*args, **kwargs)
        transition_events.append("start_reconciliation_entry")

        def _prepare_continuation() -> None:
            transition_events.append("continuation_preparation_callback")
            callback()

        rebound = dict(kwargs)
        rebound["before_main_adoption"] = _prepare_continuation
        return observed_start(*args, **rebound)

    monkeypatch.setattr(cli_module, "bind_ordinary_continuation", _bind)
    monkeypatch.setattr(start_tick_module, "reconcile_start_tick", _start)
    before_foreign = _foreign_bytes(target)
    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert _foreign_bytes(target) == before_foreign
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()
    assert basis_calls, observed
    basis = basis_calls[0]["kwargs"]["predecessor"]
    assert basis["kind"] == "SETTLED_PROMOTION"
    assert set(basis) == {"kind", "predecessor"}
    assert basis["predecessor"]["task_id"] == journal["task_id"]
    assert basis["predecessor"]["settled"] is True
    assert observed.index("task_admissibility") < observed.index("continuation")
    assert binds, observed
    assert binds[-1] == {
        "ok": True,
        "reason_code": "FINALIZED_PROMOTION_TICK",
    }
    assert transition_events == [
        "bind_gate",
        "start_reconciliation_entry",
        "continuation_preparation_callback",
    ]
    assert "adapter" in observed
    continuation = yaml.safe_load(
        (runtime / "post_tick_continuation.yaml").read_text(encoding="utf-8")
    )
    assert continuation["status"] == "AUTHORIZED"
    assert continuation["origin"]["kind"] == "FINALIZED_PROMOTION_TICK"
    assert (
        continuation["origin"]["evidence"]["predecessor_identity"]["task_id"]
        == journal["task_id"]
    )
    assert exit_code == 1
    assert stdout["outcome"] == "AWAITING_DECISIONS"
    assert stdout["reason_code"] == "PROMOTION_DECISION_MISSING"
    assert stdout["reason_code"] not in _TRANSITIONAL_SEAM_REASONS
    assert lock_events["released"] == lock_events["acquired"]
    assert not (runtime / "runner_git.lock").exists()


@pytest.mark.parametrize(
    ("reason_code", "outcome", "settled"),
    [
        pytest.param(
            "IDENTICAL_SETTLED_TASK",
            "TERMINAL_REST",
            True,
            id="IDENTICAL_SETTLED_TASK-TERMINAL_REST-True",
        ),
        pytest.param(
            "ACTIVATION_PREDECESSOR_UNAVAILABLE",
            "BLOCKED",
            False,
            id="UNRESOLVED_PREDECESSOR-BLOCKED-False",
        ),
    ],
)
def test_cli_projects_rest_and_unresolved_predecessor_without_mutation(
    tmp_path: Path,
    capsys: Any,
    reason_code: str,
    outcome: str,
    settled: bool,
) -> None:
    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _write_public_world(tmp_path)
    lifecycle, target, runtime = _rebind_canonical_predecessor_world(
        config_path,
        tmp_path,
    )
    _write_active_task(config_path, target)
    if settled:
        journal = yaml.safe_load(
            (runtime / "promotion_journal.yaml").read_text(encoding="utf-8")
        )
        active_identity = _bind_active_task_to_canonical_predecessor(
            config_path,
            runtime,
            origin_run_id=journal["origin_run_id"],
        )
        reproved = lifecycle.reprove_run_local_snapshot(
            run_root=runtime / "runs" / journal["origin_run_id"],
            expected_task_fingerprint=journal["task_fingerprint"],
            expected_task_snapshot_sha256=journal["task_snapshot_sha256"],
            expected_instruction_signature_sha256=(
                journal["instruction_signature_sha256"]
            ),
        )
        assert active_identity == (
            reproved.task_id,
            reproved.task_fingerprint,
            [
                {
                    "logical_ref": source.logical_ref,
                    "sha256": source.sha256,
                    "byte_length": source.byte_length,
                }
                for source in reproved.instruction_sources
            ],
        )
    else:
        (runtime / "drain_outcome.yaml").unlink()
    head = _run_git(target, "rev-parse", "HEAD")
    porcelain = _run_git(target, "status", "--porcelain")
    runtime_before = _runtime_snapshot(runtime)

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert stdout["outcome"] == outcome
    assert stdout["reason_code"] == reason_code
    assert stdout["reason_code"] not in _TRANSITIONAL_SEAM_REASONS
    assert _run_git(target, "rev-parse", "HEAD") == head
    assert _run_git(target, "status", "--porcelain") == porcelain
    assert _runtime_snapshot(runtime) == runtime_before
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


def _canonical_decision_document(
    identity: Any,
    result_sha: str,
    *,
    decision_type: str,
) -> dict[str, Any]:
    document = {
        "schema_version": _SCHEMA_VERSION,
        "decision_type": decision_type,
        "task_id": identity.task_id,
        "origin_run_id": identity.origin_run_id,
        "promotion_attempt_id": identity.promotion_attempt_id,
        "result_sha": result_sha,
        "ready": True,
        "evidence": [{"kind": "git_result", "ref": result_sha}],
    }
    if decision_type == "orchestrator":
        document["promotion_class"] = "GREEN_PROMOTION"
    else:
        document["verdict"] = "GO"
    return document


def _write_canonical_runtime_decisions(
    runtime: Path,
    identity: Any,
    result_sha: str,
    *,
    orchestrator_result_sha: str | None = None,
    locations: tuple[str, ...] = ("run", "legacy"),
) -> Path:
    orchestrator_sha = result_sha if orchestrator_result_sha is None else orchestrator_result_sha
    documents = {
        "orchestrator_decision.yaml": _canonical_decision_document(
            identity,
            orchestrator_sha,
            decision_type="orchestrator",
        ),
        "validator_decision.yaml": _canonical_decision_document(
            identity,
            result_sha,
            decision_type="validator",
        ),
    }
    folders = {
        "run": runtime / "runs" / identity.origin_run_id / "decisions",
        "legacy": runtime / "decisions",
    }
    orchestrator_path: Path | None = None
    for location in locations:
        folder = folders[location]
        folder.mkdir(parents=True, exist_ok=True)
        for filename, document in documents.items():
            path = folder / filename
            path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
            if filename == "orchestrator_decision.yaml":
                orchestrator_path = path
    if orchestrator_path is None:
        raise AssertionError(locations)
    return orchestrator_path


def _capture_activated_identity(
    monkeypatch: pytest.MonkeyPatch,
    store: list[Any],
) -> None:
    import task_transition_runner.cli as cli_module

    current = cli_module.evaluate_task_activation

    def _capture(*args: Any, **kwargs: Any) -> Any:
        decision = current(*args, **kwargs)
        store.append(decision)
        return decision

    monkeypatch.setattr(cli_module, "evaluate_task_activation", _capture)


def _expected_decision_identity(identity: Any, result_sha: str) -> Any:
    from task_transition_runner.decision_contracts import DecisionIdentity

    return DecisionIdentity(
        task_id=identity.task_id,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
        result_sha=result_sha,
    )


def _mapping_succeeded(result: Any) -> bool:
    return isinstance(result, dict) and result.get("ok") is True


def _assert_closed_top_level_outcome(outcome: object) -> None:
    assert len(_CLOSED_TOP_LEVEL_OUTCOMES) == 9
    assert len(set(_CLOSED_TOP_LEVEL_OUTCOMES)) == 9
    assert "D_COMPOSITION_TRAVERSAL" not in _CLOSED_TOP_LEVEL_OUTCOMES
    assert None not in _CLOSED_TOP_LEVEL_OUTCOMES
    assert outcome in _CLOSED_TOP_LEVEL_OUTCOMES
    assert outcome != "D_COMPOSITION_TRAVERSAL"
    assert outcome is not None


def _assert_emitted_closed_run(exit_code: int | None, stdout_text: str) -> dict[str, Any]:
    assert exit_code is not None
    stdout = _read_stdout_yaml(stdout_text)
    _assert_closed_top_level_outcome(stdout.get("outcome"))
    return stdout


def _return_to_main_succeeded(result: Any) -> bool:
    return (
        isinstance(result, dict)
        and result.get("ok") is True
        and result.get("reason_code") == "RETURNED_TO_MAIN"
        and result.get("branch") == _REFERENCE_MAIN_BRANCH
    )


def _drain_decision_admitted(decision: Any) -> bool:
    outcome = getattr(decision, "outcome", None)
    phase = getattr(decision, "phase", None)
    status = getattr(outcome, "status", None)
    return (
        getattr(phase, "value", None) == "DRAINED_AWAITING_TICK"
        and getattr(status, "value", None) == "DRAINED"
        and getattr(outcome, "reason_code", None) == "PROMOTION_EFFECT_ADMITTED"
    )


def _continuation_consumption_succeeded(result: Any) -> bool:
    return getattr(result, "status", None) == "CONSUMED"


def _qualify_task_end_pending(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    from task_transition_runner.decision_contracts import (
        DecisionArtifactState,
        classify_orchestrator_decision,
    )
    from task_transition_runner.runner_git_promotion import load_pending_promotion

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    results: dict[str, Any] = {}
    activations: list[Any] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
        results,
    )
    _capture_activated_identity(monkeypatch, activations)

    main(["run", "--config", str(config_path)])
    fresh_stdout = _read_stdout_yaml(capsys.readouterr().out)
    fresh_observed = list(observed)
    observed.clear()
    assert activations, fresh_observed
    identity = activations[0].identity
    head = _run_git(target, "rev-parse", "HEAD")
    orchestrator = _write_canonical_runtime_decisions(runtime, identity, head)
    assert (
        classify_orchestrator_decision(
            orchestrator,
            expected_identity=_expected_decision_identity(identity, head),
            artifact_root=runtime,
        )
        is DecisionArtifactState.READY
    )
    main(["run", "--config", str(config_path)])
    pending_stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert fresh_stdout["outcome"] == "AWAITING_DECISIONS"
    for label in _TAIL_LABELS:
        assert label not in fresh_observed, (label, fresh_observed)
    assert results["validate_decisions"] is DecisionArtifactState.READY
    finalize = results["pending_intent"]
    assert isinstance(finalize, dict)
    assert finalize.get("ok") is True
    assert finalize.get("outcome") == "PENDING_PROMOTION"
    pending_path = Path(str(finalize["pending_path"])).resolve()
    assert pending_path == (runtime / "pending_promotion.yaml").resolve()
    assert pending_path.is_file()
    loaded = load_pending_promotion(runtime_root=runtime)
    assert loaded["artifact_class"] == _ARTIFACT_CLASS
    pending_bytes = pending_path.read_bytes()
    assert pending_bytes
    for label in (
        "return_to_main",
        "promotion_drain",
        "continuation_consumption",
        "settlement",
    ):
        assert label not in observed, (label, observed)
    assert not (runtime / "post_tick_continuation.yaml").is_file()
    assert pending_stdout["ok"] is True
    assert pending_stdout["outcome"] == "PENDING_PROMOTION"
    assert pending_stdout["reason_code"] == finalize["reason_code"]
    assert pending_stdout["reason_code"] not in _TRANSITIONAL_SEAM_REASONS
    return {
        "config_path": config_path,
        "target": target,
        "runtime": runtime,
        "observed": observed,
        "results": results,
        "git_argv": git_argv,
        "pending_path": pending_path,
        "pending_bytes": pending_bytes,
        "finalize": finalize,
    }


def _assert_pending_matches_reached_phase(world: dict[str, Any]) -> None:
    pending_path = world["pending_path"]
    if _drain_decision_admitted(world["results"].get("pre_dispatch_drain")):
        assert not pending_path.is_file()
        return
    assert pending_path.is_file()
    assert pending_path.read_bytes() == world["pending_bytes"]


def _resume_after_real_pending(world: dict[str, Any], capsys: Any) -> dict[str, Any]:
    world["observed"].clear()
    world["results"].pop("pre_dispatch_drain", None)
    world["results"].pop("promotion_drain", None)
    world["results"].pop("continuation_consumption", None)
    try:
        main(["run", "--config", str(world["config_path"])])
        return _read_stdout_yaml(capsys.readouterr().out)
    finally:
        _assert_pending_matches_reached_phase(world)


def test_later_runs_bind_decisions_pending_return_drain_consumption_and_settlement(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    resume_stdout = _resume_after_real_pending(world, capsys)
    observed = world["observed"]

    gaps: list[str] = []
    if "INCOMPLETE_PROMOTION" not in observed:
        gaps.append("reconciliation_absent_before_pending_resume")
    elif "return_to_main" not in observed or observed.index("INCOMPLETE_PROMOTION") > observed.index(
        "return_to_main"
    ):
        gaps.append("reconciliation_not_before_return_to_main")
    if "pre_dispatch_drain" not in observed or "return_to_main" not in observed:
        gaps.append("return_or_drain_decision_not_visited")
    elif observed.index("return_to_main") > observed.index("pre_dispatch_drain"):
        gaps.append("return_to_main_not_before_drain_decision")
    if not _return_to_main_succeeded(world["results"].get("return_to_main")):
        gaps.append("return_to_main_result_is_not_the_success_gate")
    decision = world["results"].get("pre_dispatch_drain")
    if not _drain_decision_admitted(decision):
        gaps.append("drain_decision_is_not_the_success_gate")
    if _mapping_succeeded(world["results"].get("promotion_drain")) and not _drain_decision_admitted(
        decision
    ):
        gaps.append("effect_admission_bypassed_blocked_drain_decision")
    if _continuation_consumption_succeeded(world["results"].get("continuation_consumption")):
        gaps.append("fresh_init_must_not_consume_a_continuation")
    if "settlement" in observed:
        gaps.append("fresh_init_must_not_claim_settlement")
    if (world["runtime"] / "post_tick_continuation.yaml").is_file():
        gaps.append("fresh_init_has_a_continuation_artifact")
    _assert_closed_top_level_outcome(resume_stdout.get("outcome"))
    if resume_stdout.get("outcome") != "DRAINED_AWAITING_TASK":
        gaps.append(
            "fresh_drain_outcome="
            f"{resume_stdout.get('outcome')}:{resume_stdout.get('reason_code')}"
        )
    assert gaps == [], gaps
    _assert_no_destructive_git(world["git_argv"])
    _assert_frozen_read_only_product_bytes()


def test_a5_consistently_replaced_post_drain_snapshot_family_cannot_self_authenticate(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = (
        "OSS7_A5_RED::ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID::"
        "test_a5_consistently_replaced_post_drain_snapshot_family_cannot_"
        "self_authenticate"
    )
    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    drained = _resume_after_real_pending(world, capsys)
    assert drained["outcome"] == "DRAINED_AWAITING_TASK", drained
    runtime = world["runtime"]
    assert not (runtime / _SETTLED_PREDECESSOR_NAME).exists()
    journal = yaml.safe_load(
        (runtime / "promotion_journal.yaml").read_text(encoding="utf-8")
    )
    run_root = next(
        path
        for path in (
            runtime / "runs" / journal["origin_run_id"],
            runtime / "runs" / "cli-activation",
            runtime / "identity-probe",
        )
        if (path / "task_snapshot.yaml").is_file()
    )
    task_snapshot = run_root / "task_snapshot.yaml"
    task_snapshot.write_bytes(
        task_snapshot.read_bytes() + b"# consistent family replacement\n"
    )
    instruction_snapshot = (
        run_root / "instructions" / "instruction_snapshot.yaml"
    )
    instruction_document = yaml.safe_load(
        instruction_snapshot.read_text(encoding="utf-8")
    )
    for source in instruction_document["sources"]:
        payload_path = run_root / source["snapshot_path"]
        replacement = payload_path.read_bytes() + b"\nconsistent replacement\n"
        payload_path.write_bytes(replacement)
        source["sha256"] = hashlib.sha256(replacement).hexdigest()
        source["byte_length"] = len(replacement)
    instruction_snapshot.write_text(
        yaml.safe_dump(instruction_document, sort_keys=False),
        encoding="utf-8",
    )
    target = world["target"]
    head_before = _run_git(target, "rev-parse", "HEAD")
    first_parent_before = _run_git(
        target,
        "rev-list",
        "--first-parent",
        "--count",
        f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    )
    runtime_before = _runtime_snapshot(runtime)
    assert not world["pending_path"].exists()

    exit_code = main(["run", "--config", str(world["config_path"])])
    blocked = _read_stdout_yaml(capsys.readouterr().out)

    if not (
        exit_code == 1
        and blocked.get("outcome") == "BLOCKED"
        and blocked.get("reason_code")
        == "ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID"
    ):
        raise AssertionError(marker)
    assert _run_git(target, "rev-parse", "HEAD") == head_before
    assert (
        _run_git(
            target,
            "rev-list",
            "--first-parent",
            "--count",
            f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
        )
        == first_parent_before
    )
    assert _runtime_snapshot(runtime) == runtime_before
    assert not world["pending_path"].exists()
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    ("identity_axis", "foreign_value"),
    (
        ("origin_run_id", "origin-cross-run"),
        ("promotion_attempt_id", "attempt-cross-attempt"),
    ),
)
def test_a5_cross_run_and_attempt_journal_identity_block_without_follow_up_effect(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    identity_axis: str,
    foreign_value: str,
) -> None:
    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    drained = _resume_after_real_pending(world, capsys)
    assert drained["outcome"] == "DRAINED_AWAITING_TASK", drained
    runtime = world["runtime"]
    target = world["target"]
    journal_path = runtime / "promotion_journal.yaml"
    journal = yaml.safe_load(journal_path.read_text(encoding="utf-8"))
    journal[identity_axis] = foreign_value
    journal_path.write_text(
        yaml.safe_dump(journal, sort_keys=False),
        encoding="utf-8",
    )
    head_before = _run_git(target, "rev-parse", "HEAD")
    first_parent_before = _run_git(
        target,
        "rev-list",
        "--first-parent",
        "--count",
        f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    )
    runtime_before = _runtime_snapshot(runtime)
    assert not world["pending_path"].exists()

    exit_code = main(["run", "--config", str(world["config_path"])])
    blocked = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    assert blocked["outcome"] == "BLOCKED"
    assert blocked["reason_code"] == "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"
    assert _run_git(target, "rev-parse", "HEAD") == head_before
    assert (
        _run_git(
            target,
            "rev-list",
            "--first-parent",
            "--count",
            f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
        )
        == first_parent_before
    )
    assert _runtime_snapshot(runtime) == runtime_before
    assert not world["pending_path"].exists()
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


def _load_continuation_lifecycle() -> Any:
    import importlib.util

    lifecycle_path = Path(__file__).with_name("test_runner_continuation_lifecycle.py")
    spec = importlib.util.spec_from_file_location(
        "cli_ordinary_continuation_seed",
        lifecycle_path,
    )
    assert spec is not None and spec.loader is not None
    lifecycle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lifecycle)
    return lifecycle


def test_ordinary_continuation_settles_only_after_real_consumption(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_continuation_contract import load_continuation_artifact
    from task_transition_runner.runner_continuation_lifecycle import (
        prepare_post_tick_continuation,
    )
    from task_transition_runner.runner_continuation_origin_proofs import (
        prove_finalized_promotion_tick,
    )
    from task_transition_runner.runner_git_context import release_runner_git_lock
    from task_transition_runner.runner_git_promotion import load_pending_promotion

    from task_transition_runner.config import load_config
    from task_transition_runner.task_activation import (
        evaluate_task_activation,
    )
    from task_transition_runner.task_source import load_task_source

    _assert_frozen_read_only_product_bytes()
    lifecycle = _load_continuation_lifecycle()
    seeded = lifecycle._seed(tmp_path / "ordinary-successor", chain="none", lock=True)
    runtime = seeded["runtime"]
    repo = seeded["repo"]
    main_ref = f"refs/heads/{_REFERENCE_MAIN_BRANCH}"
    proof = prove_finalized_promotion_tick(
        repo=repo,
        runtime=runtime,
        main_ref=main_ref,
    )
    assert proof.origin.value == "FINALIZED_PROMOTION_TICK"
    predecessor = proof.to_mapping()["predecessor_identity"]
    config_path, _public_target, _public_runtime = _write_public_world(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["target"]["repo_root"] = str(repo)
    payload["runtime_root"] = str(runtime)
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _bind_fresh_synthetic_adapter(config_path, tmp_path)
    _write_active_task(config_path, repo)
    task_source = yaml.safe_load((config_path.parent / "tasks.yaml").read_text(encoding="utf-8"))
    task_source["active_task"]["task_id"] = lifecycle._SUCCESSOR_TASK_ID
    (config_path.parent / "tasks.yaml").write_text(
        yaml.safe_dump(task_source, sort_keys=False),
        encoding="utf-8",
    )
    _pin_runtime_to_source(runtime)
    config = load_config(config_path)
    basis = cli_module._canonical_settled_predecessor_basis(config)
    assert basis is not None
    canonical_predecessor = basis["predecessor"]
    assert (
        canonical_predecessor["task_id"],
        canonical_predecessor["origin_run_id"],
        canonical_predecessor["promotion_attempt_id"],
    ) == (
        predecessor["task_id"],
        predecessor["origin_run_id"],
        predecessor["promotion_attempt_id"],
    )
    successor = evaluate_task_activation(
        load_task_source(
            config,
            run_root=tmp_path / "successor-identity-probe",
        ),
        predecessor=basis,
    )
    assert successor.status is ActivationStatus.ACTIVATED
    bound = successor.identity
    assert bound.task_id == lifecycle._SUCCESSOR_TASK_ID
    start_shas = lifecycle._linear_starts(
        repo,
        seeded["tock_sha"],
        hops=1,
        task_id=bound.task_id,
        attempt_id=bound.promotion_attempt_id,
        tree_mode="same",
        origin_run_id=bound.origin_run_id,
    )
    start_head = start_shas[-1]
    lifecycle._update_ref(
        repo,
        lifecycle._task_ref(bound.task_id, bound.origin_run_id),
        start_head,
    )
    prepared = prepare_post_tick_continuation(
        **lifecycle._prepare_kwargs(
            seeded,
            successor_task_id=bound.task_id,
            successor_origin_run_id=bound.origin_run_id,
            successor_promotion_attempt_id=bound.promotion_attempt_id,
            expected_continuation_head_sha=start_head,
        )
    )
    assert prepared.contract.status == "AUTHORIZED"
    assert prepared.contract.successor_task_id == bound.task_id
    continuation_predecessor = prepared.contract.to_mapping()["origin"]["evidence"][
        "predecessor_identity"
    ]
    assert continuation_predecessor == predecessor
    start_message = _run_git(repo, "log", "-1", "--format=%s", start_head)
    assert start_message == (
        f"START {bound.task_id} {bound.origin_run_id} "
        f"{bound.promotion_attempt_id} {seeded['tock_sha']}"
    )
    assert not (runtime / "pending_promotion.yaml").is_file()
    assert list(runtime.glob("runs/*/worker_result.yaml")) == []
    assert list(runtime.glob("runs/*/decisions/*.yaml")) == []

    _run_git(repo, "checkout", "-f", _REFERENCE_MAIN_BRANCH)
    assert _run_git(repo, "status", "--porcelain") == ""
    release_runner_git_lock(
        lock_path=runtime / "runner_git.lock",
        ownership=seeded["lock_ownership"],
    )
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    results: dict[str, Any] = {}
    activations: list[Any] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
        results,
    )
    _capture_activated_identity(monkeypatch, activations)

    def _run_closed() -> dict[str, Any]:
        exit_code = main(["run", "--config", str(config_path)])
        return _assert_emitted_closed_run(exit_code, capsys.readouterr().out)

    _run_closed()
    current_main_head = _run_git(repo, "rev-parse", main_ref)
    assert current_main_head == start_head
    main_state_path = runtime / "main_state.yaml"
    successor_prestate = yaml.safe_load(main_state_path.read_text(encoding="utf-8"))
    successor_prestate.update(
        {
            "validated_result_sha": seeded["result_sha"],
            "last_tock_sha": seeded["tock_sha"],
            "observed_main_head_sha": current_main_head,
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
        }
    )
    main_state_path.write_text(
        yaml.safe_dump(successor_prestate, sort_keys=False),
        encoding="utf-8",
    )
    if activations:
        activated = activations[0].identity
        assert activated.task_id == bound.task_id
        assert activated.origin_run_id == bound.origin_run_id
        assert activated.promotion_attempt_id == bound.promotion_attempt_id
    worker_paths = sorted(runtime.glob("runs/*/worker_result.yaml"))
    if worker_paths:
        worker = yaml.safe_load(worker_paths[0].read_text(encoding="utf-8"))
        assert worker["task_id"] == bound.task_id
        assert worker["origin_run_id"] == bound.origin_run_id
        assert worker["promotion_attempt_id"] == bound.promotion_attempt_id
        _write_canonical_runtime_decisions(
            runtime,
            bound,
            str(worker["result_sha"]),
        )
        _run_closed()

    prepared_view = load_continuation_artifact(runtime / "post_tick_continuation.yaml")
    assert prepared_view.status == "AUTHORIZED"
    assert prepared_view.successor_task_id == bound.task_id
    pending_path = runtime / "pending_promotion.yaml"
    assert pending_path.is_file()
    pending = load_pending_promotion(runtime_root=runtime)
    assert pending["artifact_class"] == _ARTIFACT_CLASS
    assert pending["task_id"] == bound.task_id
    assert pending["origin_run_id"] == bound.origin_run_id
    assert pending["promotion_attempt_id"] == bound.promotion_attempt_id
    assert (
        pending["main_state_before_promotion"]["observed_main_head_sha"],
        pending["main_state_before_promotion"]["last_tock_sha"],
        pending["main_state_before_promotion"]["validated_result_sha"],
        pending["main_state_before_promotion"]["reason_code"],
    ) == (
        start_head,
        seeded["tock_sha"],
        seeded["result_sha"],
        "PROMOTION_EFFECT_ADMITTED",
    )

    observed.clear()
    results.pop("promotion_drain", None)
    results.pop("continuation_consumption", None)
    results.pop("return_to_main", None)
    results.pop("pre_dispatch_drain", None)
    stdout = _run_closed()

    assert stdout["outcome"] == "SETTLED"
    assert stdout["ok"] is True
    assert not pending_path.is_file()
    assert _continuation_consumption_succeeded(results.get("continuation_consumption"))
    assert _return_to_main_succeeded(results.get("return_to_main"))
    assert _drain_decision_admitted(results.get("pre_dispatch_drain"))
    assert "INCOMPLETE_PROMOTION" in observed
    assert observed.index("INCOMPLETE_PROMOTION") < observed.index("return_to_main")
    assert observed.index("return_to_main") < observed.index("pre_dispatch_drain")
    assert observed.index("pre_dispatch_drain") < observed.index("promotion_drain")
    assert observed.index("promotion_drain") < observed.index("continuation_consumption")
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()


def test_blocked_return_to_main_stops_before_admission_consumption_and_settlement(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    blocked = {
        "ok": False,
        "status": "fail-closed",
        "reason_code": "RETURN_TO_MAIN_UNAVAILABLE",
    }

    def _blocked_return(_config: object) -> dict[str, Any]:
        world["observed"].append("return_to_main")
        world["results"]["return_to_main"] = blocked
        return blocked

    monkeypatch.setattr(cli_module, "return_to_main", _blocked_return)
    stdout = _resume_after_real_pending(world, capsys)
    assert world["results"]["return_to_main"]["ok"] is False
    for label in ("promotion_drain", "continuation_consumption", "settlement"):
        assert label not in world["observed"], (label, world["observed"])
    _blocked_stdout(stdout, "RETURN_TO_MAIN_UNAVAILABLE")
    _assert_closed_top_level_outcome(stdout["outcome"])
    _assert_no_destructive_git(world["git_argv"])
    _assert_frozen_read_only_product_bytes()


def test_blocked_drain_decision_stops_before_admission_consumption_and_settlement(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_drain_transition as drain_module

    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    real_drain = cli_module.evaluate_drain_before_dispatch

    def _withdraw_effect_admission(*args: Any, **kwargs: Any) -> Any:
        withdrawn = dict(kwargs)
        withdrawn.pop("effect_admission", None)
        return real_drain(*args, **withdrawn)

    monkeypatch.setattr(
        cli_module,
        "evaluate_drain_before_dispatch",
        _withdraw_effect_admission,
    )
    stdout = _resume_after_real_pending(world, capsys)
    decision = world["results"]["pre_dispatch_drain"]
    assert isinstance(decision, drain_module.DrainDecision)
    assert not _drain_decision_admitted(decision)
    assert getattr(decision.phase, "value", None) == "BLOCKED"
    assert getattr(decision.outcome, "reason_code", None) == "DRAIN_EFFECT_ADMISSION_MISSING"
    for label in ("promotion_drain", "continuation_consumption", "settlement"):
        assert label not in world["observed"], (label, world["observed"])
    _blocked_stdout(stdout, "DRAIN_EFFECT_ADMISSION_MISSING")
    _assert_closed_top_level_outcome(stdout["outcome"])
    assert stdout["outcome"] != "SETTLED"
    _assert_no_destructive_git(world["git_argv"])
    _assert_frozen_read_only_product_bytes()


def test_invalid_decision_stops_before_pending(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from task_transition_runner.decision_contracts import (
        DecisionArtifactState,
        classify_orchestrator_decision,
    )

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    results: dict[str, Any] = {}
    activations: list[Any] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
        results,
    )
    _capture_activated_identity(monkeypatch, activations)
    main(["run", "--config", str(config_path)])
    _read_stdout_yaml(capsys.readouterr().out)
    observed.clear()
    identity = activations[0].identity
    head = _run_git(target, "rev-parse", "HEAD")
    orchestrator = _write_canonical_runtime_decisions(
        runtime,
        identity,
        head,
        orchestrator_result_sha="0" * 40,
    )
    assert (
        classify_orchestrator_decision(
            orchestrator,
            expected_identity=_expected_decision_identity(identity, head),
            artifact_root=runtime,
        )
        is DecisionArtifactState.INVALID
    )

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert results["validate_decisions"] is DecisionArtifactState.INVALID
    assert "pending_intent" not in observed, observed
    assert "return_to_main" not in observed, observed
    assert "pre_dispatch_drain" not in observed, observed
    assert "promotion_drain" not in observed, observed
    assert "continuation_consumption" not in observed, observed
    assert "settlement" not in observed, observed
    assert stdout["ok"] is False
    assert stdout["outcome"] == "BLOCKED"
    assert stdout["outcome"] != "SETTLED"
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()


def test_run_path_decisions_authorize_later_tail(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.runner_git_context as git_context_module
    from task_transition_runner.decision_contracts import (
        DecisionArtifactState,
        classify_orchestrator_decision,
    )

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    preflight = git_context_module.evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )
    assert preflight["ok"] is True
    assert preflight["reason_code"] == "GIT_PREFLIGHT_OK"
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    results: dict[str, Any] = {}
    activations: list[Any] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
        results,
    )
    _capture_activated_identity(monkeypatch, activations)
    main(["run", "--config", str(config_path)])
    _read_stdout_yaml(capsys.readouterr().out)
    observed.clear()
    identity = activations[0].identity
    head = _run_git(target, "rev-parse", "HEAD")
    orchestrator = _write_canonical_runtime_decisions(
        runtime,
        identity,
        head,
        locations=("run",),
    )
    assert not (runtime / "decisions" / "orchestrator_decision.yaml").exists()
    assert not (runtime / "decisions" / "validator_decision.yaml").exists()
    assert (
        classify_orchestrator_decision(
            orchestrator,
            expected_identity=_expected_decision_identity(identity, head),
            artifact_root=runtime,
        )
        is DecisionArtifactState.READY
    )

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert results["validate_decisions"] is DecisionArtifactState.READY
    assert "pending_intent" in observed, observed
    assert stdout["outcome"] != "AWAITING_DECISIONS"
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()


def test_blocked_admission_after_pending_stops_before_consumption(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.runner_drain_transition as drain_module

    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    blocked_results: list[dict[str, Any]] = []

    def _blocked_admission(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        blocked = {
            "ok": False,
            "status": "blocked",
            "reason_code": "PROMOTION_LOCK_MISMATCH",
        }
        blocked_results.append(blocked)
        world["observed"].append("promotion_drain")
        world["results"]["promotion_drain"] = blocked
        return blocked

    monkeypatch.setattr(
        drain_module,
        "admit_pending_promotion_effect",
        _blocked_admission,
    )
    resume_stdout = _resume_after_real_pending(world, capsys)

    assert blocked_results, world["observed"]
    assert blocked_results[0]["ok"] is False
    assert resume_stdout["ok"] is False
    assert resume_stdout["outcome"] == "BLOCKED"
    assert resume_stdout["reason_code"] == blocked_results[0]["reason_code"]
    assert resume_stdout["outcome"] != "SETTLED"
    assert "continuation_consumption" not in world["observed"], world["observed"]
    assert "settlement" not in world["observed"], world["observed"]
    _assert_no_destructive_git(world["git_argv"])
    _assert_frozen_read_only_product_bytes()


def test_failed_consumption_after_real_drain_stops_before_settled(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.runner_post_tick_continuation as post_tick_module
    from task_transition_runner.runner_continuation_contract import ContinuationError

    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    current = post_tick_module.consume_post_tick_continuation
    failures: list[ContinuationError] = []

    def _fail_after_real_drain(*args: Any, **kwargs: Any) -> Any:
        if not _mapping_succeeded(world["results"].get("promotion_drain")):
            return current(*args, **kwargs)
        failure = ContinuationError("CONTINUATION_ARTIFACT_INVALID")
        failures.append(failure)
        world["results"]["continuation_consumption"] = failure
        raise failure

    monkeypatch.setattr(
        post_tick_module,
        "consume_post_tick_continuation",
        _fail_after_real_drain,
    )
    (world["runtime"] / "post_tick_continuation.yaml").write_text(
        "schema_version: '1.0'\nstatus: AUTHORIZED\n",
        encoding="utf-8",
    )
    escaped: list[BaseException] = []
    try:
        resume_stdout: dict[str, Any] | None = _resume_after_real_pending(world, capsys)
    except ContinuationError as exc:
        escaped.append(exc)
        resume_stdout = None

    assert _mapping_succeeded(world["results"].get("promotion_drain")), (
        "REACHABILITY_GAP: consumption has no real successful drain predecessor"
    )
    assert failures or escaped
    if escaped:
        assert getattr(escaped[0], "reason_code", None) == "CONTINUATION_ARTIFACT_INVALID"
    else:
        assert resume_stdout is not None
        assert resume_stdout["ok"] is False
        assert resume_stdout["outcome"] != "SETTLED"
        assert resume_stdout["reason_code"] == "CONTINUATION_ARTIFACT_INVALID"
    assert "settlement" not in world["observed"], world["observed"]
    _assert_no_destructive_git(world["git_argv"])
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    ("component", "reason_code"),
    [
        ("promotion", "PROMOTION_PENDING_INVALID"),
        ("continuation", "CONTINUATION_ARTIFACT_INVALID"),
    ],
)
def test_call_component_does_not_swallow_later_tail_exception(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    component: str,
    reason_code: str,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_post_tick_continuation as post_tick_module
    from task_transition_runner.runner_continuation_contract import ContinuationError
    from task_transition_runner.runner_git_promotion import RunnerGitPromotionError

    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    calls: list[str] = []
    if component == "promotion":

        def _raise_promotion(*_args: Any, **_kwargs: Any) -> Any:
            calls.append(component)
            raise RunnerGitPromotionError(reason_code)

        monkeypatch.setattr(cli_module, "evaluate_drain_before_dispatch", _raise_promotion)
    else:

        def _raise_continuation(*_args: Any, **_kwargs: Any) -> Any:
            calls.append(component)
            raise ContinuationError(reason_code)

        monkeypatch.setattr(
            post_tick_module,
            "consume_post_tick_continuation",
            _raise_continuation,
        )
        (world["runtime"] / "post_tick_continuation.yaml").write_text(
            "schema_version: '1.0'\nstatus: AUTHORIZED\n",
            encoding="utf-8",
        )

    escaped: list[BaseException] = []
    try:
        stdout: dict[str, Any] | None = _resume_after_real_pending(world, capsys)
    except (RunnerGitPromotionError, ContinuationError) as exc:
        escaped.append(exc)
        stdout = None

    assert calls, (component, world["observed"])
    if escaped:
        assert getattr(escaped[0], "reason_code", None) == reason_code
        assert "settlement" not in world["observed"], world["observed"]
    else:
        assert stdout is not None
        assert stdout["ok"] is False
        assert stdout["outcome"] == "BLOCKED"
        assert stdout["reason_code"] == reason_code
        assert stdout["outcome"] != "SETTLED"
        assert "settlement" not in world["observed"], world["observed"]
        if component == "promotion":
            assert "continuation_consumption" not in world["observed"], world["observed"]
    _assert_no_destructive_git(world["git_argv"])
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    "outcome",
    tuple(outcome for outcome in _PUBLIC_OUTCOMES if outcome != "SETTLED"),
)
def test_public_run_outcome_follows_persisted_state_artifact(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_code_authority as authority_module
    import task_transition_runner.runner_git_context as git_context_module

    _assert_frozen_read_only_product_bytes()
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    config_path, target, runtime = _write_public_world(tmp_path)
    if outcome in {"WORKER_RESULT_READY", "PENDING_PROMOTION"}:
        _write_active_task(config_path, target)
    _plant_public_outcome_artifact(runtime, target, outcome)
    head = _run_git(target, "rev-parse", "HEAD")
    if outcome == "WORKER_RESULT_READY":
        from task_transition_runner.worker_result import (
            WorkerResultIdentity,
            load_worker_result,
        )

        loaded_worker = load_worker_result(
            runtime / "worker_result.yaml",
            expected_identity=WorkerResultIdentity(
                task_id=_CLI_TASK_ID,
                origin_run_id="origin-public-outcome",
                promotion_attempt_id="attempt-public-outcome",
                result_sha=head,
            ),
            expected_validation_digests={},
        )
        assert loaded_worker.result_type == "worker_result"
    elif outcome == "PENDING_PROMOTION":
        from task_transition_runner.config import load_config
        from task_transition_runner.runner_git_promotion import load_pending_promotion

        loaded_pending = load_pending_promotion(runtime_root=runtime)
        assert loaded_pending["artifact_class"] == _ARTIFACT_CLASS
        assert set(loaded_pending) >= {
            "task_id",
            "origin_run_id",
            "promotion_attempt_id",
            "result_sha",
        }
        assert list(runtime.glob("runs/*/worker_result.yaml")) == []
        assert not (runtime / "promotion_journal.yaml").is_file()
        pending_bytes = (runtime / "pending_promotion.yaml").read_bytes()
        pending_refs = _run_git(
            target,
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        )
        reconciled = cli_module.reconcile_ordinary_promotion(load_config(config_path))
        assert reconciled == {
            "ok": False,
            "status": "fail-closed",
            "reason_code": "RECONCILIATION_PENDING_INVALID",
        }
    elif outcome == "SETTLED":
        from task_transition_runner.runner_continuation_contract import (
            load_continuation_artifact,
        )

        loaded_continuation = load_continuation_artifact(
            runtime / "post_tick_continuation.yaml"
        )
        assert loaded_continuation.status == "CONSUMED"
    visits: list[str] = []
    lock_events: dict[str, Any] = {}
    original_authority = authority_module.verify_steady_state_code_authority
    original_preflight = git_context_module.evaluate_branch_preflight
    original_reconcile = cli_module.reconcile_ordinary_promotion
    original_acquire = cli_module.acquire_runner_git_lock
    original_release = cli_module.release_runner_git_lock

    def _authority(*args: Any, **kwargs: Any) -> Any:
        visits.append("source_authority")
        return original_authority(*args, **kwargs)

    def _preflight(*args: Any, **kwargs: Any) -> Any:
        visits.append("target_runtime_preflight")
        return original_preflight(*args, **kwargs)

    def _reconcile(*args: Any, **kwargs: Any) -> Any:
        visits.append("reconciliation")
        return original_reconcile(*args, **kwargs)

    def _acquire(*args: Any, **kwargs: Any) -> Any:
        visits.append("lock_acquire")
        result = original_acquire(*args, **kwargs)
        lock_events["acquired"] = result.get("ownership")
        return result

    def _release(*args: Any, **kwargs: Any) -> Any:
        visits.append("lock_release")
        lock_events["released"] = kwargs.get("ownership")
        return original_release(*args, **kwargs)

    monkeypatch.setattr(
        authority_module,
        "verify_steady_state_code_authority",
        _authority,
    )
    monkeypatch.setattr(git_context_module, "evaluate_branch_preflight", _preflight)
    monkeypatch.setattr(cli_module, "reconcile_ordinary_promotion", _reconcile)
    monkeypatch.setattr(cli_module, "acquire_runner_git_lock", _acquire)
    monkeypatch.setattr(cli_module, "release_runner_git_lock", _release)
    original_dump = yaml.safe_dump

    def _dump(*args: Any, **kwargs: Any) -> Any:
        payload = args[0] if args else kwargs.get("data")
        if (
            isinstance(payload, dict)
            and payload.get("command") == "run"
            and payload.get("outcome") == outcome
        ):
            visits.append("state_projection")
        return original_dump(*args, **kwargs)

    monkeypatch.setattr(yaml, "safe_dump", _dump)

    if outcome == "PENDING_PROMOTION":
        main(["run", "--config", str(config_path)])
        stdout = _read_stdout_yaml(capsys.readouterr().out)
        blocked_prefix = (
            "source_authority",
            "target_runtime_preflight",
            "lock_acquire",
            "reconciliation",
        )
        assert [label for label in blocked_prefix if label in visits] == list(
            blocked_prefix
        ), visits
        assert [visits.index(label) for label in blocked_prefix] == sorted(
            visits.index(label) for label in blocked_prefix
        )
        assert "state_projection" not in visits, visits
        assert "lock_release" in visits, visits
        assert visits.index("reconciliation") < visits.index("lock_release")
        assert lock_events["released"] == lock_events["acquired"]
        assert not (runtime / "runner_git.lock").exists()
        _blocked_stdout(stdout, "RECONCILIATION_PENDING_INVALID")
        assert stdout["outcome"] != "PENDING_PROMOTION"
        assert stdout["outcome"] not in _PUBLIC_OUTCOMES
        assert (runtime / "pending_promotion.yaml").read_bytes() == pending_bytes
        assert _run_git(target, "rev-parse", "HEAD") == head
        assert _run_git(target, "status", "--porcelain") == ""
        assert (
            _run_git(target, "for-each-ref", "--format=%(refname) %(objectname)")
            == pending_refs
        )
        _assert_frozen_read_only_product_bytes()
        return

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    precedence = (
        "source_authority",
        "target_runtime_preflight",
        "lock_acquire",
        "reconciliation",
        "state_projection",
    )
    assert [label for label in precedence if label in visits] == list(precedence), visits
    assert [visits.index(label) for label in precedence] == sorted(
        visits.index(label) for label in precedence
    )
    assert "lock_release" in visits, visits
    assert lock_events["released"] == lock_events["acquired"]
    assert "source_authority" in visits, visits
    assert "state_projection" in visits, visits
    assert visits.index("source_authority") < visits.index("state_projection")
    assert stdout["outcome"] == outcome
    _assert_closed_top_level_outcome(stdout["outcome"])
    assert stdout["reason_code"] not in _TRANSITIONAL_SEAM_REASONS
    assert _run_git(target, "rev-parse", "HEAD") == head
    assert _run_git(target, "status", "--porcelain") == ""
    _assert_frozen_read_only_product_bytes()


def test_isolated_consumed_without_d1_provenance_blocks_without_mutation(
    tmp_path: Path,
    capsys: Any,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_continuation_contract import (
        load_continuation_artifact,
    )

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _write_public_world(tmp_path)
    _plant_canonical_consumed_continuation(runtime)
    continuation = runtime / "post_tick_continuation.yaml"
    loaded = load_continuation_artifact(continuation)
    assert loaded.status == "CONSUMED"
    for absent_name in (
        "settled_predecessor.yaml",
        "promotion_journal.yaml",
        "main_state.yaml",
        "promotion_consumed.yaml",
    ):
        assert not (runtime / absent_name).exists(), absent_name

    package = Path(cli_module.__file__).resolve().parent
    frozen_product_names = (
        "cli.py",
        "runner_start_tick_reconciliation.py",
        "runner_drain_transition.py",
        "runner_continuation_contract.py",
        "runner_continuation_lifecycle.py",
        "runner_promotion_reconciliation.py",
        "runner_promotion_state.py",
        "task_activation.py",
    )
    product_before = {
        name: (package / name).read_bytes() for name in frozen_product_names
    }
    state_before = _snapshot(target, runtime)

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert _snapshot(target, runtime) == state_before
    assert {
        name: (package / name).read_bytes() for name in frozen_product_names
    } == product_before
    _assert_frozen_read_only_product_bytes()
    assert (
        exit_code,
        stdout.get("ok"),
        stdout.get("outcome"),
        stdout.get("reason_code"),
    ) == (
        1,
        False,
        "BLOCKED",
        "RECONCILIATION_IDENTITY_MISMATCH",
    )
    assert stdout["outcome"] != "SETTLED"


def test_fresh_missing_source_pin_blocks_before_preflight_and_lock(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=True,
        pin_source=False,
    )
    assert not (runtime / "runner_source_pin.yaml").exists()
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
    )
    head = _run_git(target, "rev-parse", "HEAD")
    porcelain = _run_git(target, "status", "--porcelain")
    runtime_before = _runtime_snapshot(runtime)

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    _blocked_stdout(stdout, "SOURCE_AUTHORITY_RUNTIME_PIN_MISSING")
    assert "source_authority" in observed, observed
    assert "target_runtime_preflight" not in observed, observed
    assert "lock_acquire" not in observed, observed
    assert "lock_release" not in observed, observed
    assert _run_git(target, "rev-parse", "HEAD") == head
    assert _run_git(target, "status", "--porcelain") == porcelain
    assert _runtime_snapshot(runtime) == runtime_before
    assert not (runtime / "runner_git.lock").exists()
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()


def _prepare_pinned_clean_run(tmp_path: Path) -> tuple[Path, Path, Path]:
    config_path, target, runtime = _write_public_world(tmp_path)
    _write_fresh_main_state(runtime, target)
    _pin_runtime_to_source(runtime)
    return config_path, target, runtime


def test_dirty_target_preflight_blocks_before_lock_and_projection(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.runner_git_context as git_context_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, _runtime = _prepare_pinned_clean_run(tmp_path)
    (target / "dirty-target.txt").write_text("DIRTY\n", encoding="utf-8")
    preflight = git_context_module.evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
    )

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert preflight["ok"] is False
    assert preflight["reason_code"] == "GIT_DIRTY"
    _blocked_stdout(stdout, "GIT_DIRTY")
    assert "lock_acquire" not in observed, observed
    assert "INCOMPLETE_PROMOTION" not in observed, observed
    assert "reconciliation" not in observed, observed
    assert stdout["outcome"] not in _PUBLIC_OUTCOMES
    assert stdout["outcome"] != "AWAITING_DECISIONS"
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()


def test_blocked_prepare_does_not_emit_awaiting_decisions(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_git_context as git_context_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    preflight = git_context_module.evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )
    assert preflight["ok"] is True
    assert preflight["reason_code"] == "GIT_PREFLIGHT_OK"
    observed: list[str] = []
    basis_calls: list[dict[str, Any]] = []
    lock_events: dict[str, Any] = {}
    git_argv: list[tuple[str, ...]] = []
    results: dict[str, Any] = {}
    _install_complete_surface_observers(
        monkeypatch,
        observed,
        basis_calls,
        lock_events,
        git_argv,
        results,
    )

    def _blocked_prepare(**_kwargs: Any) -> dict[str, Any]:
        blocked = {
            "ok": False,
            "status": "blocked",
            "reason_code": "PROMOTION_RESULT_SHA_MISMATCH",
            "outcome": "BLOCKED",
        }
        results["decision_request"] = blocked
        return blocked

    monkeypatch.setattr(
        cli_module,
        "prepare_git_promotion_task_end_artifacts",
        _blocked_prepare,
    )

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    prepare = results["decision_request"]

    assert isinstance(prepare, dict)
    assert prepare.get("ok") is not True
    assert list(runtime.rglob("decision_request.yaml")) == []
    assert stdout["outcome"] != "AWAITING_DECISIONS"
    _blocked_stdout(stdout, str(prepare["reason_code"]))
    for label in _TAIL_LABELS:
        assert label not in observed, (label, observed)
    _assert_no_destructive_git(git_argv)
    _assert_frozen_read_only_product_bytes()


def test_blocked_reconciliation_wins_over_persisted_public_artifact(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.runner_continuation_contract as continuation_contract
    import task_transition_runner.runner_promotion_reconciliation as reconciliation_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    (runtime / "pending_promotion.yaml").write_text(
        "schema_version: '1.0'\npartial: true\n",
        encoding="utf-8",
    )
    continuation_path = runtime / "post_tick_continuation.yaml"
    continuation_path.write_text(
        "schema_version: '1.0'\nstatus: CONSUMED\n",
        encoding="utf-8",
    )
    verdict = reconciliation_module.reconcile_ordinary_promotion(
        repo=target,
        runtime_root=runtime,
        drain_run_id="cli-run",
        main_ref=f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
        diagnostic_refs=None,
    )
    with pytest.raises(continuation_contract.ContinuationError) as caught:
        continuation_contract.load_continuation_artifact(continuation_path)

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert verdict.reason_code == "RECONCILIATION_PENDING_INVALID"
    assert caught.value.reason_code == "CONTINUATION_ARTIFACT_INVALID"
    _blocked_stdout(stdout, verdict.reason_code)
    assert stdout["outcome"] not in _PUBLIC_OUTCOMES
    _assert_frozen_read_only_product_bytes()

    import task_transition_runner.cli as cli_module

    world = _qualify_task_end_pending(tmp_path / "pending-reconciliation", capsys, monkeypatch)
    blocked_reason = "RECONCILIATION_PENDING_INVALID"

    def _blocked_reconciliation(_config: object) -> dict[str, Any]:
        world["observed"].append("reconciliation_block")
        return {"ok": False, "reason_code": blocked_reason}

    monkeypatch.setattr(cli_module, "reconcile_ordinary_promotion", _blocked_reconciliation)
    resume_stdout = _resume_after_real_pending(world, capsys)
    assert "reconciliation_block" in world["observed"], world["observed"]
    for label in (
        "return_to_main",
        "pre_dispatch_drain",
        "promotion_drain",
        "continuation_consumption",
        "settlement",
    ):
        assert label not in world["observed"], (label, world["observed"])
    _blocked_stdout(resume_stdout, blocked_reason)
    _assert_closed_top_level_outcome(resume_stdout["outcome"])


@pytest.mark.parametrize(
    ("kind", "forbidden_outcome"),
    [
        ("worker_result", "WORKER_RESULT_READY"),
        ("pending", "PENDING_PROMOTION"),
        ("drain", "DRAINED_AWAITING_TASK"),
        ("recovered", "RECOVERED"),
        ("continuation", "SETTLED"),
    ],
)
def test_public_success_requires_canonical_artifact(
    tmp_path: Path,
    capsys: Any,
    kind: str,
    forbidden_outcome: str,
) -> None:
    import task_transition_runner.runner_continuation_contract as continuation_contract
    import task_transition_runner.runner_drain_transition as drain_module
    import task_transition_runner.runner_git_promotion as git_promotion_module
    import task_transition_runner.runner_promotion_state as state_module
    import task_transition_runner.worker_result as worker_result_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    if kind == "worker_result":
        worker_path = runtime / "worker_result.yaml"
        worker_path.write_text(
            yaml.safe_dump(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "task_id": "partial",
                    "origin_run_id": "origin-public-outcome",
                    "promotion_attempt_id": "attempt-public-outcome",
                    "task_branch": _REFERENCE_MAIN_BRANCH,
                    "result_sha": "ab" * 20,
                    "changed_paths": [],
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        with pytest.raises(worker_result_module.WorkerResultError):
            worker_result_module.load_worker_result(
                worker_path,
                expected_identity=worker_result_module.WorkerResultIdentity(
                    task_id="partial",
                    origin_run_id="missing-origin",
                    promotion_attempt_id="missing-attempt",
                    result_sha="a" * 40,
                ),
                expected_validation_digests={},
            )
    elif kind == "pending":
        pending_path = runtime / "pending_promotion.yaml"
        pending_path.write_text(
            "schema_version: '1.0'\n"
            "artifact_class: NON_AUTHORIZING_PROMOTION_INTENT\n",
            encoding="utf-8",
        )
        with pytest.raises(git_promotion_module.RunnerGitPromotionError):
            git_promotion_module.load_pending_promotion(runtime_root=runtime)
    elif kind == "drain":
        (runtime / "drain_outcome.yaml").write_text(
            "status: DRAINED\n",
            encoding="utf-8",
        )
        with pytest.raises(drain_module.DrainOutcomeError):
            drain_module.load_persisted_drain_outcome(runtime_root=runtime)
    elif kind == "recovered":
        main_state_path = runtime / "main_state.yaml"
        main_state_path.write_text(
            "reason_code: RECONCILIATION_COMPLETED\n",
            encoding="utf-8",
        )
        with pytest.raises(state_module.RunnerPromotionStateError):
            state_module.load_and_validate_main_state(main_state_path=main_state_path)
    else:
        continuation_path = runtime / "post_tick_continuation.yaml"
        continuation_path.write_text(
            "schema_version: '1.0'\nstatus: CONSUMED\n",
            encoding="utf-8",
        )
        with pytest.raises(continuation_contract.ContinuationError) as caught:
            continuation_contract.load_continuation_artifact(continuation_path)
        assert caught.value.reason_code == "CONTINUATION_ARTIFACT_INVALID"
        (runtime / "main_state.yaml").write_text(
            "reason_code: RECONCILIATION_COMPLETED\n",
            encoding="utf-8",
        )
    head = _run_git(target, "rev-parse", "HEAD")
    loader_errors = (
        worker_result_module.WorkerResultError,
        git_promotion_module.RunnerGitPromotionError,
        drain_module.DrainOutcomeError,
        state_module.RunnerPromotionStateError,
        continuation_contract.ContinuationError,
    )
    escaped: list[BaseException] = []
    try:
        main(["run", "--config", str(config_path)])
    except loader_errors as exc:
        escaped.append(exc)

    assert escaped == []
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert stdout["outcome"] != forbidden_outcome
    assert stdout["outcome"] not in _PUBLIC_OUTCOMES
    assert _run_git(target, "rev-parse", "HEAD") == head
    _assert_frozen_read_only_product_bytes()


def test_return_to_main_succeeds_only_on_proven_main_branch(
    tmp_path: Path,
) -> None:
    from task_transition_runner.cli import return_to_main
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, target, _runtime = _prepare_pinned_clean_run(tmp_path)
    _run_git(target, "checkout", "-b", "runner/task/not-main")
    result = return_to_main(load_config(config_path))
    head = _run_git(target, "rev-parse", "HEAD")
    main_head = _run_git(target, "rev-parse", f"refs/heads/{_REFERENCE_MAIN_BRANCH}")

    claimed_success = result.get("ok") is True or result.get("reason_code") == "RETURNED_TO_MAIN"
    assert claimed_success is False or head == main_head
    if claimed_success:
        assert result["branch"] == _REFERENCE_MAIN_BRANCH
    _assert_frozen_read_only_product_bytes()


def test_successful_continuation_proof_is_not_relabeled_origin_invalid(
    tmp_path: Path,
) -> None:
    import importlib.util

    from task_transition_runner.cli import bind_ordinary_continuation
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_origin_proofs import (
        prove_finalized_promotion_tick,
    )
    from task_transition_runner.task_activation import _fresh_main_state_bound

    _assert_frozen_read_only_product_bytes()
    lifecycle_path = Path(__file__).with_name("test_runner_continuation_lifecycle.py")
    spec = importlib.util.spec_from_file_location(
        "cli_causal_continuation_seed",
        lifecycle_path,
    )
    assert spec is not None and spec.loader is not None
    lifecycle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lifecycle)
    world = lifecycle._seed(tmp_path / "continuation-proof")
    runtime = world["runtime"]
    main_state = yaml.safe_load((runtime / "main_state.yaml").read_text(encoding="utf-8"))
    assert _fresh_main_state_bound(main_state) is False
    _write_settled_predecessor(
        runtime,
        task_id="synthetic-settled-predecessor",
        settled=True,
    )
    proof = prove_finalized_promotion_tick(
        repo=world["repo"],
        runtime=runtime,
        main_ref=f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    )
    config_path, _target, _public_runtime = _write_public_world(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["target"]["repo_root"] = str(world["repo"])
    payload["runtime_root"] = str(runtime)
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    bound = bind_ordinary_continuation(load_config(config_path))

    assert proof.origin.value == "FINALIZED_PROMOTION_TICK"
    assert bound["ok"] is True
    assert bound["reason_code"] != "CONTINUATION_ORIGIN_INVALID"
    _assert_frozen_read_only_product_bytes()


def _tail_calls_after(calls: list[tuple[str, ...]], label: str) -> list[tuple[str, ...]]:
    for index, item in enumerate(calls):
        if item[0] == label:
            return calls[index + 1 :]
    return calls


def test_blocked_continuation_bind_stops_before_start_tick_git_and_adapter(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.adapters.cursor_headless as adapter_module
    import task_transition_runner.cli as cli_module
    import task_transition_runner.runner_start_tick_reconciliation as start_tick_module

    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=False,
    )
    _lifecycle, target, runtime = _rebind_canonical_predecessor_world(
        config_path,
        tmp_path,
    )
    _write_active_task(config_path, target)
    main_state_path = runtime / "main_state.yaml"
    main_state = yaml.safe_load(main_state_path.read_text(encoding="utf-8"))
    main_state["reason_code"] = "RECONCILIATION_COMPLETED"
    main_state_path.write_text(yaml.safe_dump(main_state, sort_keys=False), encoding="utf-8")
    calls: list[tuple[str, ...]] = []
    original_bind = cli_module.bind_ordinary_continuation
    original_start = start_tick_module.reconcile_start_tick
    original_adapter = adapter_module.run_cursor_adapter
    original_activation = cli_module.evaluate_task_activation

    def _bind(config: object) -> dict[str, Any]:
        result = original_bind(config)
        calls.append(("continuation", str(result.get("ok")), str(result.get("reason_code"))))
        return result

    def _start(*args: Any, **kwargs: Any) -> Any:
        calls.append(("start_tick",))
        return original_start(*args, **kwargs)

    def _adapter(*args: Any, **kwargs: Any) -> Any:
        calls.append(("adapter",))
        return original_adapter(*args, **kwargs)

    def _invalidate_after_admissibility(*args: Any, **kwargs: Any) -> Any:
        result = original_activation(*args, **kwargs)
        snapshot = (
            runtime
            / "runs"
            / "origin-synthetic-lifecycle-001"
            / "task_snapshot.yaml"
        )
        snapshot.write_bytes(snapshot.read_bytes() + b"# post-admissibility tamper\n")
        return result

    monkeypatch.setattr(cli_module, "bind_ordinary_continuation", _bind)
    monkeypatch.setattr(
        cli_module,
        "evaluate_task_activation",
        _invalidate_after_admissibility,
    )
    monkeypatch.setattr(start_tick_module, "reconcile_start_tick", _start)
    monkeypatch.setattr(adapter_module, "run_cursor_adapter", _adapter)
    before_head = _run_git(target, "rev-parse", "HEAD")
    before_main = _run_git(target, "rev-parse", f"refs/heads/{_REFERENCE_MAIN_BRANCH}")
    before_state = main_state_path.read_bytes()
    before_refs = _run_git(target, "for-each-ref", "--format=%(refname) %(objectname)")

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    blocked = [item for item in calls if item[0] == "continuation" and item[1] != "True"]

    assert blocked, calls
    assert _tail_calls_after(calls, "continuation") == []
    assert ("adapter",) not in calls
    assert _run_git(target, "rev-parse", "HEAD") == before_head
    assert _run_git(target, "rev-parse", f"refs/heads/{_REFERENCE_MAIN_BRANCH}") == before_main
    assert main_state_path.read_bytes() == before_state
    assert _run_git(target, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
    assert stdout["outcome"] == "BLOCKED"
    _assert_frozen_read_only_product_bytes()


def test_blocked_start_tick_stops_before_checkout_and_adapter(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.adapters.cursor_headless as adapter_module
    import task_transition_runner.runner_start_tick_reconciliation as start_tick_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    main_state_path = runtime / "main_state.yaml"
    main_state = yaml.safe_load(main_state_path.read_text(encoding="utf-8"))
    main_state["main_branch"] = "not-main"
    main_state_path.write_text(yaml.safe_dump(main_state, sort_keys=False), encoding="utf-8")
    calls: list[tuple[str, ...]] = []
    git_argv: list[tuple[str, ...]] = []
    original_start = start_tick_module.reconcile_start_tick
    original_adapter = adapter_module.run_cursor_adapter
    original_run = subprocess.run

    def _start(*args: Any, **kwargs: Any) -> Any:
        decision = original_start(*args, **kwargs)
        calls.append(("start_tick", str(getattr(decision.status, "value", ""))))
        return decision

    def _adapter(*args: Any, **kwargs: Any) -> Any:
        calls.append(("adapter",))
        return original_adapter(*args, **kwargs)

    def _run(argv: list[str], **kwargs: Any) -> Any:
        git_argv.append(tuple(argv))
        return original_run(argv, **kwargs)

    monkeypatch.setattr(start_tick_module, "reconcile_start_tick", _start)
    monkeypatch.setattr(adapter_module, "run_cursor_adapter", _adapter)
    monkeypatch.setattr(subprocess, "run", _run)
    before_head = _run_git(target, "rev-parse", "HEAD")
    escaped: list[BaseException] = []
    try:
        main(["run", "--config", str(config_path)])
        stdout = _read_stdout_yaml(capsys.readouterr().out)
    except subprocess.CalledProcessError as exc:
        escaped.append(exc)
        stdout = {}

    blocked_tail = [item for item in calls if item == ("start_tick", "BLOCKED")]
    checkout = [argv for argv in git_argv if "checkout" in argv]
    assert blocked_tail, calls
    assert checkout == []
    assert ("adapter",) not in calls
    assert escaped == []
    assert stdout.get("outcome") == "BLOCKED"
    assert _run_git(target, "rev-parse", "HEAD") == before_head
    _assert_frozen_read_only_product_bytes()


def _retarget_public_config(config_path: Path, repo: Path, runtime: Path) -> None:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["target"]["repo_root"] = str(repo)
    payload["runtime_root"] = str(runtime)
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _pending_document(
    *,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    result_sha: str,
    base_sha: str,
) -> dict[str, Any]:
    identity = {
        "task_id": task_id,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
        "result_sha": result_sha,
    }
    return {
        "schema_version": _SCHEMA_VERSION,
        "artifact_class": _ARTIFACT_CLASS,
        "task_id": task_id,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
        "result_sha": result_sha,
        "promotion_class": "GREEN_PROMOTION",
        "expected_outcome": {"kind": "GREEN"},
        "expected_main_state_after_promotion": {"state": "GREEN_VALIDATED"},
        "base_sha": base_sha,
        "task_branch": _REFERENCE_MAIN_BRANCH,
        "main_state_before_promotion": {"state": "GREEN_VALIDATED"},
        "task_snapshot": {
            "current": {"task_id": task_id, "fingerprint": "ab" * 32, "digest": "cd" * 32},
            "bound": {"task_id": task_id, "fingerprint": "ab" * 32, "digest": "cd" * 32},
        },
        "instruction_snapshot": {
            "current": {"digest": "ef" * 32, "bindings": []},
            "bound": {"digest": "ef" * 32, "bindings": []},
        },
        "orchestrator_decision": {"state": "READY", **identity},
        "validator_decision": {"state": "READY", **identity},
        "allowed_paths": [],
        "red_lock_closure": {"closed": True},
    }


def _write_canonical_worker(
    runtime: Path,
    *,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    result_sha: str,
    canonical: bool,
) -> Path:
    payload: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "task_id": task_id,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
        "task_branch": _REFERENCE_MAIN_BRANCH,
        "result_sha": result_sha,
        "changed_paths": [],
        "validation_results": [],
        "worker_report": {
            "reported_status": "success",
            "summary": "deferred successor worker",
        },
    }
    if canonical:
        payload["result_type"] = "worker_result"
    path = runtime / "runs" / origin_run_id / "worker_result.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    decisions = path.parent / "decisions"
    decisions.mkdir(parents=True, exist_ok=True)
    (decisions / "orchestrator_decision.yaml").write_text("state: READY\n", encoding="utf-8")
    (decisions / "validator_decision.yaml").write_text("state: READY\n", encoding="utf-8")
    return path


def _deferred_successor_world(tmp_path: Path, defect: str) -> dict[str, Any]:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_contract import (
        binding_digest,
        load_continuation_artifact,
    )
    from task_transition_runner.runner_continuation_lifecycle import (
        prepare_post_tick_continuation,
    )
    from task_transition_runner.runner_continuation_origin_proofs import (
        prove_finalized_promotion_tick,
    )
    from task_transition_runner.runner_git_context import release_runner_git_lock
    from task_transition_runner.runner_git_promotion import load_pending_promotion
    from task_transition_runner.runner_promotion_journal import load_promotion_journal
    from task_transition_runner.task_source import load_task_source
    from task_transition_runner.worker_result import (
        WorkerResultError,
        WorkerResultIdentity,
        load_worker_result,
    )

    lifecycle = _load_continuation_lifecycle()
    seeded = lifecycle._seed(tmp_path / "deferred-successor", chain="none", lock=True)
    repo = seeded["repo"]
    runtime = seeded["runtime"]
    main_ref = f"refs/heads/{_REFERENCE_MAIN_BRANCH}"
    successor_task_id = lifecycle._SUCCESSOR_TASK_ID
    config_path, _target, _runtime = _write_public_world(tmp_path)
    _retarget_public_config(config_path, repo, runtime)
    _write_active_task(config_path, repo)
    task_source = yaml.safe_load((config_path.parent / "tasks.yaml").read_text(encoding="utf-8"))
    task_source["active_task"]["task_id"] = successor_task_id
    (config_path.parent / "tasks.yaml").write_text(
        yaml.safe_dump(task_source, sort_keys=False),
        encoding="utf-8",
    )
    _pin_runtime_to_source(runtime)
    config = load_config(config_path)
    basis = cli_module._canonical_settled_predecessor_basis(config)
    assert basis is not None
    successor = cli_module.evaluate_task_activation(config, predecessor=basis)
    assert successor.status is ActivationStatus.ACTIVATED
    successor_task_id = successor.identity.task_id
    origin_run_id = successor.identity.origin_run_id
    promotion_attempt_id = successor.identity.promotion_attempt_id
    worker_task_id = (
        lifecycle._FOREIGN_TASK_ID if defect == "foreign_worker" else successor_task_id
    )
    start_sha = lifecycle._linear_starts(
        repo,
        seeded["tock_sha"],
        hops=1,
        task_id=worker_task_id,
        attempt_id=promotion_attempt_id,
        tree_mode="same",
        origin_run_id=origin_run_id,
    )[0]
    result_sha = lifecycle._commit(
        repo,
        tree=lifecycle._tree_of(repo, start_sha),
        parents=(start_sha,),
        message="synthetic deferred successor result\n",
    )
    lifecycle._update_ref(repo, lifecycle._task_ref(worker_task_id, origin_run_id), start_sha)
    if defect == "foreign_worker":
        proof = prove_finalized_promotion_tick(repo=repo, runtime=runtime, main_ref=main_ref)
        predecessor = dict(proof.to_mapping()["predecessor_identity"])
        binding = {
            "schema_version": "2.0",
            "origin": {
                "kind": "FINALIZED_PROMOTION_TICK",
                "evidence": {
                    "predecessor_identity": predecessor,
                    "tock_sha": seeded["tock_sha"],
                },
            },
            "original_tick_sha": start_sha,
            "expected_continuation_head_sha": start_sha,
            "commit_chain": [{"sha": start_sha, "parent_sha": seeded["tock_sha"]}],
            "successor_task_id": successor_task_id,
        }
        artifact = {
            **binding,
            "status": "AUTHORIZED",
            "prepared_at_utc": "2026-09-26T12:00:00Z",
            "continuation_digest_sha256": binding_digest(binding),
        }
        (runtime / "post_tick_continuation.yaml").write_text(
            yaml.safe_dump(artifact, sort_keys=False),
            encoding="utf-8",
        )
    else:
        prepare_post_tick_continuation(
            **lifecycle._prepare_kwargs(
                seeded,
                successor_task_id=successor_task_id,
                successor_origin_run_id=origin_run_id,
                successor_promotion_attempt_id=promotion_attempt_id,
                expected_continuation_head_sha=start_sha,
            )
        )
    loaded_continuation = load_continuation_artifact(runtime / "post_tick_continuation.yaml")
    assert loaded_continuation.successor_task_id == successor_task_id
    lifecycle._update_ref(repo, lifecycle._task_ref(worker_task_id, origin_run_id), result_sha)
    lifecycle._update_ref(repo, main_ref, start_sha)
    current_main_head = _run_git(repo, "rev-parse", main_ref)
    assert current_main_head == start_sha
    main_state = yaml.safe_load((runtime / "main_state.yaml").read_text(encoding="utf-8"))
    main_state["observed_main_head_sha"] = current_main_head
    (runtime / "main_state.yaml").write_text(
        yaml.safe_dump(main_state, sort_keys=False),
        encoding="utf-8",
    )
    journal_path = runtime / "promotion_journal.yaml"
    if defect == "promotion_class":
        journal_payload = yaml.safe_load(journal_path.read_text(encoding="utf-8"))
        journal_payload["promotion_class"] = "RED_TDD_PROMOTION"
        journal_path.write_text(yaml.safe_dump(journal_payload, sort_keys=False), encoding="utf-8")
        assert (
            loaded_continuation.identity.promotion_class
            != load_promotion_journal(path=journal_path).promotion_class
        )
    worker_path = _write_canonical_worker(
        runtime,
        task_id=worker_task_id,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
        result_sha=result_sha,
        canonical=defect != "malformed_worker",
    )
    load_task_source(config, run_root=runtime / "runs" / origin_run_id)
    expected_identity = WorkerResultIdentity(
        task_id=successor_task_id,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
        result_sha=result_sha,
    )
    if defect in {"malformed_worker", "foreign_worker"}:
        with pytest.raises(WorkerResultError):
            load_worker_result(
                worker_path,
                expected_identity=expected_identity,
                expected_validation_digests={},
            )
    else:
        loaded_worker = load_worker_result(
            worker_path,
            expected_identity=expected_identity,
            expected_validation_digests={},
        )
        assert loaded_worker.task_id == successor_task_id
    if defect == "malformed_continuation":
        (runtime / "post_tick_continuation.yaml").write_text(
            "schema_version: '1.0'\nstatus: AUTHORIZED\n",
            encoding="utf-8",
        )
    pending = _pending_document(
        task_id=worker_task_id,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
        result_sha=result_sha,
        base_sha=start_sha,
    )
    if defect == "none":
        pending["main_state_before_promotion"] = {
            **main_state,
            "observed_main_head_sha": current_main_head,
            "last_tock_sha": seeded["tock_sha"],
            "validated_result_sha": seeded["result_sha"],
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
        }
    (runtime / "pending_promotion.yaml").write_text(
        yaml.safe_dump(pending, sort_keys=False),
        encoding="utf-8",
    )
    assert load_pending_promotion(runtime_root=runtime)["task_id"] == worker_task_id
    release_runner_git_lock(
        lock_path=runtime / "runner_git.lock",
        ownership=seeded["lock_ownership"],
    )
    _run_git(repo, "checkout", "-f", _REFERENCE_MAIN_BRANCH)
    assert _run_git(repo, "status", "--porcelain") == ""
    return {"config_path": config_path, "repo": repo, "runtime": runtime}


@pytest.mark.parametrize(
    "defect",
    ["promotion_class", "malformed_worker", "foreign_worker", "malformed_continuation"],
)
def test_deferred_successor_reproof_keeps_foreign_worlds_byte_identical(
    tmp_path: Path,
    capsys: Any,
    defect: str,
) -> None:
    _assert_frozen_read_only_product_bytes()
    world = _deferred_successor_world(tmp_path, defect)
    before = _snapshot(world["repo"], world["runtime"])
    escaped: list[BaseException] = []
    try:
        main(["run", "--config", str(world["config_path"])])
        capsys.readouterr()
    except Exception as exc:
        escaped.append(exc)
    after = _snapshot(world["repo"], world["runtime"])
    assert escaped == []
    assert after == before, defect
    _assert_frozen_read_only_product_bytes()


def test_task_snapshot_reproof_binds_persisted_bytes_against_current_source(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.task_source import load_task_source

    _assert_frozen_read_only_product_bytes()
    config_path, _target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    config = load_config(config_path)
    origin_run_id = "origin-task-snapshot-reproof"
    load_task_source(config, run_root=runtime / "runs" / origin_run_id)
    snapshot_path = runtime / "runs" / origin_run_id / "task_snapshot.yaml"
    assert snapshot_path.is_file()
    persisted = b"PERSISTED-TASK-SNAPSHOT-NOT-THE-LIVE-SOURCE\n"
    snapshot_path.write_bytes(persisted)
    current_digest = hashlib.sha256(config.task_source.read_bytes()).hexdigest()
    persisted_digest = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()

    facts = cli_module._task_snapshot_facts(config, origin_run_id)

    assert facts["current"]["digest"] == current_digest
    assert facts["bound"]["digest"] == persisted_digest
    assert facts["bound"] != facts["current"]
    _assert_frozen_read_only_product_bytes()


def _plant_reconciliation_block(runtime: Path) -> None:
    (runtime / "promotion_journal.yaml").write_text(
        "schema_version: '1.0'\nphase: NOT_A_PHASE\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize("outcome", _PUBLIC_OUTCOMES)
def test_blocked_reconciliation_wins_without_pending_resume_or_inflight(
    tmp_path: Path,
    capsys: Any,
    outcome: str,
) -> None:
    import task_transition_runner.runner_promotion_reconciliation as reconciliation_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    if outcome in {"WORKER_RESULT_READY", "PENDING_PROMOTION"}:
        _write_active_task(config_path, target)
    _plant_public_outcome_artifact(runtime, target, outcome)
    _plant_reconciliation_block(runtime)
    if outcome == "PENDING_PROMOTION":
        (runtime / "runs").mkdir(exist_ok=True)
        assert list(runtime.glob("runs/*/worker_result.yaml")) == []
    verdict = reconciliation_module.reconcile_ordinary_promotion(
        repo=target,
        runtime_root=runtime,
        drain_run_id="cli-run",
        main_ref=f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
        diagnostic_refs=None,
    )

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert verdict.reason_code not in {"RECONCILIATION_NO_WORK", "RECONCILIATION_COMPLETED"}
    _blocked_stdout(stdout, verdict.reason_code)
    assert stdout["outcome"] != outcome
    assert stdout["outcome"] not in _PUBLIC_OUTCOMES
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    ("field", "foreign_value"),
    [
        ("task_id", "foreign-drain-task"),
        ("origin_run_id", "foreign-drain-origin"),
        ("promotion_attempt_id", "foreign-drain-attempt"),
        ("result_sha", "ab" * 20),
        ("promotion_class", "RED_TDD_PROMOTION"),
        ("tock_sha", "LIVE_HEAD_NOT_PENDING_RESULT"),
    ],
)
def test_foreign_drained_outcome_does_not_authorize_pending_resume(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    foreign_value: str,
) -> None:
    from task_transition_runner.runner_drain_transition import (
        DrainOutcome,
        DrainStatus,
        load_persisted_drain_outcome,
        persist_drain_outcome,
    )
    from task_transition_runner.runner_git_promotion import load_pending_promotion

    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    pending = load_pending_promotion(runtime_root=world["runtime"])
    live_head = _run_git(world["target"], "rev-parse", "HEAD")
    identity = {
        "task_id": str(pending["task_id"]),
        "origin_run_id": str(pending["origin_run_id"]),
        "promotion_attempt_id": str(pending["promotion_attempt_id"]),
        "result_sha": str(pending["result_sha"]),
        "promotion_class": str(pending["promotion_class"]),
        "tock_sha": live_head,
    }
    if field == "tock_sha":
        assert live_head != str(pending["result_sha"])
        identity["tock_sha"] = live_head
    else:
        identity[field] = foreign_value
    persist_drain_outcome(
        runtime_root=world["runtime"],
        outcome=DrainOutcome(
            status=DrainStatus.DRAINED,
            drain_run_id="foreign-drain",
            observed_main_head_sha=live_head,
            reason_code="PROMOTION_EFFECT_ADMITTED",
            task_id=identity["task_id"],
            origin_run_id=identity["origin_run_id"],
            promotion_attempt_id=identity["promotion_attempt_id"],
            result_sha=identity["result_sha"],
            promotion_class=identity["promotion_class"],
            tock_sha=identity["tock_sha"],
        ),
    )
    loaded = load_persisted_drain_outcome(runtime_root=world["runtime"])
    assert loaded is not None
    if field == "tock_sha":
        assert loaded.tock_sha == live_head
        assert loaded.tock_sha != str(pending["result_sha"])
    else:
        assert getattr(loaded, field) == foreign_value
        assert getattr(loaded, field) != pending[field]
    before_pending = world["pending_bytes"]
    world["observed"].clear()

    main(["run", "--config", str(world["config_path"])])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert stdout["outcome"] != "DRAINED_AWAITING_TASK"
    assert stdout["outcome"] != "SETTLED"
    assert stdout["ok"] is False
    assert world["pending_path"].is_file()
    assert world["pending_path"].read_bytes() == before_pending
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    "task_source_text",
    [
        pytest.param(
            "schema_version: '1.0'\nactive_task: [malformed]\n",
            id="parseable_active_task_list",
        ),
        pytest.param(
            "schema_version: '1.0'\nactive_task: [\n",
            id="unclosed_sequence",
        ),
    ],
)
def test_malformed_task_source_is_not_terminal_rest(
    tmp_path: Path,
    capsys: Any,
    task_source_text: str,
) -> None:
    from task_transition_runner.config import load_config
    from task_transition_runner.task_contract import TaskContractError, load_task_contract

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    config = load_config(config_path)
    config.task_source.write_text(task_source_text, encoding="utf-8")
    if task_source_text == "schema_version: '1.0'\nactive_task: [\n":
        with pytest.raises(TaskContractError) as captured:
            load_task_contract(config.task_source, target_worktree=target)
        assert captured.value.reason_code == "TASK_FIELD_TYPE"
        before = _snapshot(target, runtime)
        escaped: list[BaseException] = []
        try:
            main(["run", "--config", str(config_path)])
        except Exception as exc:
            escaped.append(exc)
        after = _snapshot(target, runtime)
        assert after["git"]["head_sha"] == before["git"]["head_sha"]
        assert after["git"]["head_ref"] == before["git"]["head_ref"]
        assert after["git"]["refs"] == before["git"]["refs"]
        assert after["git"]["porcelain"] == before["git"]["porcelain"]
        assert after["runtime"] == before["runtime"]
        assert (
            after["runtime"]["exists:runner_git.lock"]
            == before["runtime"]["exists:runner_git.lock"]
        )
        assert after == before
        if escaped:
            raise escaped[0]
        stdout = _read_stdout_yaml(capsys.readouterr().out)
        _blocked_stdout(stdout, "TASK_FIELD_TYPE")
        assert stdout["outcome"] != "TERMINAL_REST"
        assert stdout["reason_code"] != "ACTIVE_TASK_NULL"
        assert stdout["ok"] is False
        assert stdout["outcome"] not in _PUBLIC_OUTCOMES
        _assert_frozen_read_only_product_bytes()
        return
    with pytest.raises(TaskContractError):
        load_task_contract(config.task_source, target_worktree=target)
    before = _snapshot(target, runtime)

    main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert stdout["outcome"] != "TERMINAL_REST"
    assert stdout["reason_code"] != "ACTIVE_TASK_NULL"
    assert stdout["ok"] is False
    assert _snapshot(target, runtime)["git"] == before["git"]
    _assert_frozen_read_only_product_bytes()


def _assert_frozen_j_draft_bytes() -> None:
    root = _candidate_root()
    for relative, expected in _FROZEN_J_DRAFT_SHA256.items():
        actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        assert actual == expected, relative
    for relative, expected in _FROZEN_READ_ONLY_COMPANION_SHA256.items():
        actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        assert actual == expected, relative
    product_paths = tuple(_FROZEN_J_DRAFT_SHA256)
    diff = subprocess.run(
        ["git", "-C", str(root), "diff", "--", *product_paths],
        check=True,
        capture_output=True,
    ).stdout
    if diff:
        assert hashlib.sha256(diff).hexdigest() == _FROZEN_J_DRAFT_BINARY_DIFF_SHA256
        return
    product_status = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "status",
            "--porcelain=v1",
            "-uall",
            "--",
            *product_paths,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert product_status == ""


_CONSUME_SPY_PROGRAM = """\
import json
import os
import sys
from pathlib import Path

witness = Path(os.environ["OSS6_CONSUME_SPY_WITNESS"])
artifact = Path(os.environ["OSS6_CONTINUATION_ARTIFACT"])
import task_transition_runner.runner_post_tick_continuation as post_tick_module
from task_transition_runner.runner_continuation_contract import (
    load_continuation_artifact,
)


def _return_authorized_view(*args, **kwargs):
    view = load_continuation_artifact(artifact)
    witness.write_text(
        json.dumps({"called": True, "status": view.status}),
        encoding="utf-8",
    )
    return view


post_tick_module.consume_post_tick_continuation = _return_authorized_view
from task_transition_runner.cli import main

raise SystemExit(main(sys.argv[1:]))
"""


def _open_deferred_continuation_gate(tmp_path: Path) -> dict[str, Any]:
    from task_transition_runner.runner_continuation_contract import (
        load_continuation_artifact,
    )
    from task_transition_runner.runner_git_promotion import load_pending_promotion

    world = _deferred_successor_world(tmp_path, "none")
    worker_path = next(world["runtime"].glob("runs/*/worker_result.yaml"))
    worker = yaml.safe_load(worker_path.read_text(encoding="utf-8"))
    task_branch = f"runner/task/{worker['task_id']}/{worker['origin_run_id']}"
    task_ref = f"refs/heads/{task_branch}"
    result_sha = str(worker["result_sha"])
    main_head = _run_git(
        world["repo"],
        "rev-parse",
        f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    )
    assert _run_git(world["repo"], "rev-parse", task_ref) == result_sha
    assert main_head != result_sha
    worker["task_branch"] = task_branch
    worker_path.write_text(
        yaml.safe_dump(worker, sort_keys=False),
        encoding="utf-8",
    )
    pending_path = world["runtime"] / "pending_promotion.yaml"
    pending = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    assert pending["base_sha"] == main_head
    assert pending["result_sha"] == result_sha
    task_snapshot_path = (
        world["runtime"] / "runs" / worker["origin_run_id"] / "task_snapshot.yaml"
    )
    bound_task_snapshot = yaml.safe_load(
        task_snapshot_path.read_text(encoding="utf-8")
    )
    assert bound_task_snapshot["active_task"]["task_id"] == worker["task_id"]
    bound_allowed_paths = bound_task_snapshot["active_task"]["allowed_paths"]
    assert bound_allowed_paths
    assert all(
        marker not in path
        for path in bound_allowed_paths
        for marker in ("*", "?", "[")
    )
    assert set(worker["changed_paths"]).issubset(bound_allowed_paths)
    assert worker["validation_results"] == []
    pending["task_branch"] = task_branch
    pending["allowed_paths"] = bound_allowed_paths
    pending_path.write_text(
        yaml.safe_dump(pending, sort_keys=False),
        encoding="utf-8",
    )
    rebound_pending = load_pending_promotion(runtime_root=world["runtime"])
    assert rebound_pending["task_branch"] == worker["task_branch"]
    assert rebound_pending["allowed_paths"] == bound_allowed_paths
    effect_admitted_pre_state = rebound_pending["main_state_before_promotion"]
    assert effect_admitted_pre_state["observed_main_head_sha"] == main_head
    assert effect_admitted_pre_state["reason_code"] == "PROMOTION_EFFECT_ADMITTED"
    preserved_epoch = (
        effect_admitted_pre_state["observed_main_head_sha"],
        effect_admitted_pre_state["validated_result_sha"],
        effect_admitted_pre_state["last_tock_sha"],
    )
    main_state_path = world["runtime"] / "main_state.yaml"
    effect_admitted_pre_state_bytes = yaml.safe_dump(
        effect_admitted_pre_state,
        sort_keys=False,
    ).encode("utf-8")
    main_state_path.write_bytes(effect_admitted_pre_state_bytes)
    assert main_state_path.read_bytes() == effect_admitted_pre_state_bytes
    persisted_main_state = yaml.safe_load(main_state_path.read_bytes())
    assert persisted_main_state == effect_admitted_pre_state
    assert (
        persisted_main_state["observed_main_head_sha"],
        persisted_main_state["validated_result_sha"],
        persisted_main_state["last_tock_sha"],
    ) == preserved_epoch
    _write_canonical_runtime_decisions(
        world["runtime"],
        SimpleNamespace(
            task_id=worker["task_id"],
            origin_run_id=worker["origin_run_id"],
            promotion_attempt_id=worker["promotion_attempt_id"],
        ),
        result_sha,
        locations=("run",),
    )
    artifact = world["runtime"] / "post_tick_continuation.yaml"
    authorized = load_continuation_artifact(artifact)
    assert authorized.status == "AUTHORIZED"
    return {
        "world": world,
        "worker_path": worker_path,
        "worker": worker,
        "task_branch": task_branch,
        "result_sha": result_sha,
        "main_head": main_head,
        "artifact": artifact,
        "continuation_bytes": artifact.read_bytes(),
        "main_state_bytes": main_state_path.read_bytes(),
        "consumed_bytes": (world["runtime"] / "promotion_consumed.yaml").read_bytes(),
    }


def _pin_runtime_to_draft_source(runtime: Path, source_root: Path) -> None:
    from task_transition_runner.runner_code_authority import persist_runtime_source_pin

    pin = runtime / "runner_source_pin.yaml"
    pin.unlink()
    persist_runtime_source_pin(
        runtime_root=runtime,
        source_sha=_run_git(source_root, "rev-parse", "HEAD"),
    )


def _run_draft_cli_consume_spy(tmp_path: Path, gate: Mapping[str, Any]) -> dict[str, Any]:
    world = gate["world"]
    draft_root = tmp_path / "frozen-draft-source"
    draft_root.mkdir()
    source_root = _bind_draft_source(draft_root)
    _pin_runtime_to_draft_source(world["runtime"], source_root)
    witness = tmp_path / "consume-spy.json"
    env = os.environ.copy()
    env["OSS6_CONSUME_SPY_WITNESS"] = str(witness)
    env["OSS6_CONTINUATION_ARTIFACT"] = str(gate["artifact"])
    env.pop("CURSOR_API_ENDPOINT", None)
    env["PYTHONPATH"] = str((source_root / "src").resolve())
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _CONSUME_SPY_PROGRAM,
            "run",
            "--config",
            str(world["config_path"]),
        ],
        cwd=source_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    called = False
    status = None
    if witness.is_file():
        payload = json.loads(witness.read_text(encoding="utf-8"))
        called = payload["called"] is True
        status = payload["status"]
    stdout = _read_stdout_yaml(completed.stdout) if completed.stdout.strip() else {}
    return {
        "exit_code": completed.returncode,
        "stdout": stdout,
        "stderr": completed.stderr,
        "called": called,
        "status": status,
    }


def _seal_canonical_producer_evidence(gate: Mapping[str, Any]) -> dict[str, Any]:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    world = gate["world"]
    runtime = world["runtime"]
    repo = world["repo"]
    worker = yaml.safe_load(gate["worker_path"].read_text(encoding="utf-8"))
    assert worker["validation_results"] == []
    origin_run_id = str(worker["origin_run_id"])
    run_dir = runtime / "runs" / origin_run_id
    snapshot_path = run_dir / "task_snapshot.yaml"
    snapshot_before = snapshot_path.read_bytes()
    developer = run_dir / "instructions" / "prompts" / "developer"
    task_spec = run_dir / "instructions" / "task_spec"
    assert developer.is_file()
    assert task_spec.is_file()
    gate["worker_path"].unlink()
    _run_git(repo, "checkout", "-f", str(worker["task_branch"]))
    assert _run_git(repo, "rev-parse", "HEAD") == worker["result_sha"]
    allowed_paths = tuple(
        yaml.safe_load(snapshot_before)["active_task"]["allowed_paths"]
    )
    composed = compose_adapter_git_worker_result(
        repo_root=repo,
        runtime_root=runtime,
        task_id=str(worker["task_id"]),
        origin_run_id=origin_run_id,
        promotion_attempt_id=str(worker["promotion_attempt_id"]),
        task_branch=str(worker["task_branch"]),
        developer=cli_module._bound_prompt_file(developer),
        task_spec=cli_module._bound_prompt_file(task_spec),
        adapter_outcome=_cli_outcome(completed=True),
        allowed_paths=allowed_paths,
        claimed_identity=None,
    )
    assert composed.get("ok") is True, composed
    prepared = prepare_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=runtime,
        worker_result=composed["worker_result"],
        allowed_paths=allowed_paths,
    )
    assert prepared.get("ok") is True, prepared
    assert snapshot_path.read_bytes() == snapshot_before
    _run_git(repo, "checkout", "-f", _REFERENCE_MAIN_BRANCH)
    assert _run_git(repo, "status", "--porcelain") == ""
    sealed = yaml.safe_load(gate["worker_path"].read_text(encoding="utf-8"))
    results = sealed["validation_results"]
    assert isinstance(results, list) and results
    manifest_path = run_dir / "validation" / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "passed"
    assert manifest["reason_code"] == "PREDECISION_VALIDATION_PASSED"
    command_text = manifest["records"][0]["command_text"]
    assert command_text == _SYNTHETIC_NONMUTATING_TASK_COMMAND
    assert "pytest" not in command_text
    for item, record in zip(results, manifest["records"], strict=True):
        log_path = runtime / str(item["output_ref"])
        log_bytes = log_path.read_bytes()
        assert log_bytes
        assert hashlib.sha256(log_bytes).hexdigest() == item["output_sha256"]
        assert item["output_sha256"] == record["output_sha256"]
        assert item["command_id"] == record["command_id"]
        assert record["exit_code"] == 0
    request = yaml.safe_load(
        (run_dir / "decisions" / "decision_request.yaml").read_text(encoding="utf-8")
    )
    assert request["request_type"] == "decision_request"
    assert request["evidence"][0]["sha256"] == results[0]["output_sha256"]
    config = load_config(world["config_path"])
    task_facts = cli_module._task_snapshot_facts(config, origin_run_id)
    instruction_facts = cli_module._instruction_snapshot_facts(config, origin_run_id)
    assert task_facts["current"] == task_facts["bound"]
    assert instruction_facts["current"] == instruction_facts["bound"]
    pending_path = runtime / "pending_promotion.yaml"
    pending = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    pending["task_snapshot"] = task_facts
    pending["instruction_snapshot"] = instruction_facts
    pending_path.write_text(
        yaml.safe_dump(pending, sort_keys=False),
        encoding="utf-8",
    )
    return {"worker": sealed, "manifest": manifest}


def test_empty_validation_evidence_blocks_before_tock_consumption_and_settled(
    tmp_path: Path,
) -> None:
    _assert_frozen_j_draft_bytes()
    _assert_frozen_read_only_product_bytes()
    gate = _open_deferred_continuation_gate(tmp_path)
    world = gate["world"]
    assert yaml.safe_load(gate["worker_path"].read_text(encoding="utf-8"))[
        "validation_results"
    ] == []
    assert not (
        world["runtime"] / "runs" / gate["worker"]["origin_run_id"] / "validation" / "manifest.yaml"
    ).exists()

    result = _run_draft_cli_consume_spy(tmp_path, gate)

    assert result["stdout"].get("reason_code") == "PROMOTION_EVIDENCE_MISSING", result
    assert result["stdout"].get("outcome") != "SETTLED"
    assert result["stdout"].get("ok") is False
    assert result["called"] is False
    assert result["status"] is None
    assert gate["artifact"].read_bytes() == gate["continuation_bytes"]
    assert (world["runtime"] / "main_state.yaml").read_bytes() == gate["main_state_bytes"]
    assert (
        world["runtime"] / "promotion_consumed.yaml"
    ).read_bytes() == gate["consumed_bytes"]
    assert _run_git(world["repo"], "rev-parse", "HEAD") == gate["main_head"]
    assert _run_git(
        world["repo"],
        "rev-parse",
        f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    ) == gate["main_head"]
    _assert_frozen_j_draft_bytes()
    _assert_frozen_read_only_product_bytes()


def test_non_consumed_result_does_not_authorize_settled(tmp_path: Path) -> None:
    _assert_frozen_j_draft_bytes()
    _assert_frozen_read_only_product_bytes()
    gate = _open_deferred_continuation_gate(tmp_path)
    sealed = _seal_canonical_producer_evidence(gate)
    assert sealed["worker"]["validation_results"]
    assert gate["artifact"].read_bytes() == gate["continuation_bytes"]

    result = _run_draft_cli_consume_spy(tmp_path, gate)

    assert result["called"] is True, result
    assert result["status"] != "CONSUMED"
    assert gate["artifact"].read_bytes() == gate["continuation_bytes"]
    assert result["stdout"].get("outcome") != "SETTLED", result
    assert result["stdout"].get("ok") is False
    _assert_frozen_j_draft_bytes()
    _assert_frozen_read_only_product_bytes()


def test_init_uses_real_bootstrap_and_emits_canonical_artifacts(
    tmp_path: Path,
    capsys: Any,
) -> None:
    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _write_public_world(tmp_path)

    exit_code = main(["init", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 0
    assert stdout["ok"] is True
    assert stdout["command"] == "init"
    assert stdout["outcome"] == "INITIALIZED"
    assert stdout["reason_code"] == "BOOTSTRAP_OK"
    assert Path(stdout["artifacts_written"]["main_state"]).read_bytes() == (
        runtime / "main_state.yaml"
    ).read_bytes()
    assert Path(stdout["artifacts_written"]["promotion_consumed"]).read_bytes() == (
        runtime / "promotion_consumed.yaml"
    ).read_bytes()
    assert _run_git(target, "status", "--porcelain") == ""
    _assert_frozen_read_only_product_bytes()


def test_init_dirty_preflight_blocks_without_runtime_artifacts(
    tmp_path: Path,
    capsys: Any,
) -> None:
    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _write_public_world(tmp_path)
    (target / "uncommitted.txt").write_text("dirty\n", encoding="utf-8")
    before = _snapshot(target, runtime)

    exit_code = main(["init", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "GIT_DIRTY")
    assert _snapshot(target, runtime) == before
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    (
        ("absent", "CONTINUATION_NOT_REQUIRED"),
        ("invalid", "CONTINUATION_ARTIFACT_INVALID"),
        ("consumed", "RECONCILIATION_IDENTITY_MISMATCH"),
        ("authorized", "CONTINUATION_NOT_REQUIRED"),
    ),
)
def test_continuation_without_settled_basis_is_fail_closed_or_not_required(
    tmp_path: Path,
    case: str,
    expected_reason: str,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_contract import (
        load_continuation_artifact,
    )

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    continuation = runtime / "post_tick_continuation.yaml"
    if case == "invalid":
        continuation.write_text(
            "schema_version: '1.0'\nstatus: AUTHORIZED\n",
            encoding="utf-8",
        )
    elif case in {"consumed", "authorized"}:
        _plant_canonical_consumed_continuation(runtime)
        if case == "authorized":
            payload = yaml.safe_load(continuation.read_text(encoding="utf-8"))
            payload["status"] = "AUTHORIZED"
            for key in (
                "successor_promotion_attempt_id",
                "successor_tock_sha",
                "consumed_at_utc",
            ):
                payload.pop(key, None)
            continuation.write_text(
                yaml.safe_dump(payload, sort_keys=False),
                encoding="utf-8",
            )
            assert load_continuation_artifact(continuation).status == "AUTHORIZED"
    if case == "absent":
        assert not continuation.exists()
    assert not (runtime / "settled_predecessor.yaml").exists()
    before = _snapshot(target, runtime)

    result = cli_module.bind_ordinary_continuation(load_config(config_path))

    assert result["reason_code"] == expected_reason
    assert result["ok"] is (case in {"absent", "authorized"})
    assert _snapshot(target, runtime) == before
    _assert_frozen_read_only_product_bytes()


def test_invalid_terminal_task_contract_blocks_public_run_without_writes(
    tmp_path: Path,
    capsys: Any,
) -> None:
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    config = load_config(config_path)
    _write_settled_predecessor(
        runtime,
        task_id="synthetic-predecessor",
        settled=False,
    )
    config.task_source.write_text(
        "schema_version: '1.0'\nactive_task: [malformed]\n",
        encoding="utf-8",
    )
    before = _snapshot(target, runtime)

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "TASK_FIELD_TYPE")
    assert _snapshot(target, runtime) == before
    _assert_frozen_read_only_product_bytes()


def test_wrong_branch_with_invalid_task_contract_stops_before_lock(
    tmp_path: Path,
    capsys: Any,
) -> None:
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    config = load_config(config_path)
    config.task_source.write_text(
        "schema_version: '1.0'\nactive_task: [malformed]\n",
        encoding="utf-8",
    )
    _run_git(target, "checkout", "-b", "foreign/preflight")
    before = _snapshot(target, runtime)

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "GIT_WRONG_BRANCH")
    assert not (runtime / "runner_git.lock").exists()
    assert _snapshot(target, runtime) == before
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize("decisions_ready", (False, True))
def test_run_without_activation_basis_blocks_on_predecessor_contract(
    tmp_path: Path,
    capsys: Any,
    decisions_ready: bool,
) -> None:
    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _write_public_world(tmp_path)
    _pin_runtime_to_source(runtime)
    if decisions_ready:
        decisions = runtime / "decisions"
        decisions.mkdir()
        (decisions / "orchestrator_decision.yaml").write_text(
            "state: READY\n",
            encoding="utf-8",
        )
        (decisions / "validator_decision.yaml").write_text(
            "state: READY\n",
            encoding="utf-8",
        )
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "settled_predecessor.yaml").exists()

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "PREDECESSOR_FIELD_MISSING")
    assert _run_git(target, "status", "--porcelain") == ""
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


def test_invalid_terminal_contract_private_gate_matches_public_fail_closed_reason(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.task_contract import (
        TaskContractError,
        load_task_contract,
    )

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    config = load_config(config_path)
    _write_settled_predecessor(
        runtime,
        task_id="synthetic-predecessor",
        settled=True,
    )
    config.task_source.write_text(
        "schema_version: '1.0'\nactive_task: [malformed]\n",
        encoding="utf-8",
    )
    with pytest.raises(TaskContractError) as captured:
        load_task_contract(config.task_source, target_worktree=target)
    before = _snapshot(target, runtime)

    terminal = cli_module._terminal_without_successor_activation(config)

    assert captured.value.reason_code == "TASK_FIELD_TYPE"
    assert terminal is False
    assert _snapshot(target, runtime) == before
    _assert_frozen_read_only_product_bytes()


def test_projection_classifiers_fail_closed_without_artifact_rewrites(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_drain_transition import (
        DrainOutcome,
        DrainStatus,
        persist_drain_outcome,
    )

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    config = load_config(config_path)
    before = _snapshot(target, runtime)

    assert cli_module._projected_run_outcome(config) is None
    assert _snapshot(target, runtime) == before

    persist_drain_outcome(
        runtime_root=runtime,
        outcome=DrainOutcome(
            status=DrainStatus.NO_PENDING,
            drain_run_id="projection-no-pending",
            observed_main_head_sha=_run_git(target, "rev-parse", "HEAD"),
            reason_code="NO_PENDING_PROMOTION",
        ),
    )
    drain_before = _snapshot(target, runtime)
    assert cli_module._drained_projection(runtime) is None
    assert _snapshot(target, runtime) == drain_before

    continuation = runtime / "post_tick_continuation.yaml"
    continuation.write_text(
        "schema_version: '1.0'\nstatus: CONSUMED\n",
        encoding="utf-8",
    )
    continuation_before = _snapshot(target, runtime)
    assert cli_module._settled_projection(runtime) is None
    assert _snapshot(target, runtime) == continuation_before
    _assert_frozen_read_only_product_bytes()


def test_settled_projection_rejects_consumed_without_canonical_provenance(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    _write_active_task(config_path, target)
    _plant_canonical_consumed_continuation(runtime)
    assert not (runtime / "settled_predecessor.yaml").exists()
    before = _snapshot(target, runtime)

    projected = cli_module._projected_run_outcome(load_config(config_path))

    assert projected is None
    assert _snapshot(target, runtime) == before
    _assert_frozen_read_only_product_bytes()


def _retarget_to_consumed_provenance_world(
    tmp_path: Path,
) -> tuple[Path, Path, Path]:
    from task_transition_runner.runner_continuation_contract import (
        load_continuation_artifact,
    )

    config_path, _target, public_runtime = _write_public_world(tmp_path)
    _plant_canonical_consumed_continuation(public_runtime)
    source_root = tmp_path / "canonical-consumed-source"
    repo = source_root / "repo"
    runtime = source_root / "runtime"
    continuation_path = runtime / "post_tick_continuation.yaml"
    continuation_path.write_bytes(
        (public_runtime / "post_tick_continuation.yaml").read_bytes()
    )
    continuation = load_continuation_artifact(continuation_path)
    assert continuation.status == "CONSUMED"
    _retarget_public_config(config_path, repo, runtime)
    state_path = runtime / "main_state.yaml"
    state = yaml.safe_load(state_path.read_text(encoding="utf-8"))
    state["reason_code"] = "PROMOTION_EFFECT_ADMITTED"
    state_path.write_text(
        yaml.safe_dump(state, sort_keys=False),
        encoding="utf-8",
    )
    _pin_runtime_to_source(runtime)
    return config_path, repo, runtime


@pytest.mark.parametrize(
    ("defect", "reason_code"),
    (
        pytest.param(
            "invalid_main_state",
            "CONTINUATION_ORIGIN_INVALID",
            id="invalid_main_state",
        ),
        pytest.param(
            "observed_head_drift",
            "CONTINUATION_ORIGIN_INVALID",
            id="observed_head_drift",
        ),
        pytest.param(
            "active_task",
            "RECONCILIATION_IDENTITY_MISMATCH",
            id="active_task",
        ),
    ),
)
def test_consumed_provenance_errors_are_fail_closed_and_byte_stable(
    tmp_path: Path,
    defect: str,
    reason_code: str,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, repo, runtime = _retarget_to_consumed_provenance_world(tmp_path)
    state_path = runtime / "main_state.yaml"
    if defect == "invalid_main_state":
        state_path.write_text(
            "schema_version: '1.0'\nstate: [invalid]\n",
            encoding="utf-8",
        )
    else:
        state = yaml.safe_load(state_path.read_text(encoding="utf-8"))
        if defect == "observed_head_drift":
            state["observed_main_head_sha"] = "f" * 40
            state_path.write_text(
                yaml.safe_dump(state, sort_keys=False),
                encoding="utf-8",
            )
        else:
            _write_active_task(config_path, repo)
    before = _snapshot(repo, runtime)

    result = cli_module._deferred_successor_provenance_gate(
        load_config(config_path)
    )

    assert result == {
        "ok": False,
        "status": "fail-closed",
        "reason_code": reason_code,
    }
    assert _snapshot(repo, runtime) == before
    _assert_frozen_read_only_product_bytes()


def test_invalid_deferred_provenance_artifacts_are_fail_closed_without_writes(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_pinned_clean_run(tmp_path)
    continuation = runtime / "post_tick_continuation.yaml"
    continuation.write_text(
        "schema_version: '1.0'\nstatus: AUTHORIZED\n",
        encoding="utf-8",
    )
    before = _snapshot(target, runtime)

    invalid_continuation = cli_module._deferred_successor_provenance_gate(
        load_config(config_path)
    )

    assert invalid_continuation["ok"] is False
    assert invalid_continuation["reason_code"] == "CONTINUATION_ARTIFACT_INVALID"
    assert _snapshot(target, runtime) == before

    _plant_canonical_consumed_continuation(runtime)
    consumed_before = _snapshot(target, runtime)
    consumed_without_basis = cli_module._deferred_successor_provenance_gate(
        load_config(config_path)
    )
    assert consumed_without_basis == {
        "ok": False,
        "status": "fail-closed",
        "reason_code": "RECONCILIATION_IDENTITY_MISMATCH",
    }
    assert _snapshot(target, runtime) == consumed_before

    payload = yaml.safe_load(continuation.read_text(encoding="utf-8"))
    payload["status"] = "AUTHORIZED"
    for key in (
        "successor_promotion_attempt_id",
        "successor_tock_sha",
        "consumed_at_utc",
    ):
        payload.pop(key, None)
    continuation.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    authorized_before = _snapshot(target, runtime)
    no_basis = cli_module._deferred_successor_provenance_gate(
        load_config(config_path)
    )
    assert no_basis == {
        "ok": True,
        "reason_code": "CONTINUATION_NOT_REQUIRED",
    }
    assert _snapshot(target, runtime) == authorized_before

    _write_settled_predecessor(
        runtime,
        task_id="synthetic-predecessor",
        settled=True,
    )
    (runtime / "promotion_journal.yaml").write_text(
        "schema_version: '1.0'\nphase: NOT_A_PHASE\n",
        encoding="utf-8",
    )
    journal_before = _snapshot(target, runtime)
    invalid_journal = cli_module._deferred_successor_provenance_gate(
        load_config(config_path)
    )
    assert invalid_journal["ok"] is False
    assert invalid_journal["reason_code"] == "JOURNAL_PHASE_UNKNOWN"
    assert _snapshot(target, runtime) == journal_before
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    "defect",
    ("malformed_worker", "invalid_task_contract", "successor_identity"),
)
def test_deferred_successor_reproof_errors_are_fail_closed_without_writes(
    tmp_path: Path,
    defect: str,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    world = _deferred_successor_world(
        tmp_path,
        "malformed_worker" if defect == "malformed_worker" else "none",
    )
    config = load_config(world["config_path"])
    if defect == "invalid_task_contract":
        config.task_source.write_text(
            "schema_version: '1.0'\nactive_task: [malformed]\n",
            encoding="utf-8",
        )
    elif defect == "successor_identity":
        from task_transition_runner.runner_continuation_contract import (
            binding_digest,
            load_continuation_artifact,
        )

        continuation_path = world["runtime"] / "post_tick_continuation.yaml"
        payload = yaml.safe_load(continuation_path.read_text(encoding="utf-8"))
        payload["successor_task_id"] = "foreign-successor-task"
        binding = {
            key: payload[key]
            for key in (
                "schema_version",
                "origin",
                "original_tick_sha",
                "expected_continuation_head_sha",
                "commit_chain",
                "successor_task_id",
            )
        }
        payload["continuation_digest_sha256"] = binding_digest(binding)
        continuation_path.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )
        assert (
            load_continuation_artifact(continuation_path).successor_task_id
            == "foreign-successor-task"
        )
    before = _snapshot(world["repo"], world["runtime"])

    result = cli_module._deferred_successor_provenance_gate(config)

    assert result == {
        "ok": False,
        "status": "fail-closed",
        "reason_code": "RECONCILIATION_IDENTITY_MISMATCH",
    }
    assert _snapshot(world["repo"], world["runtime"]) == before
    _assert_frozen_read_only_product_bytes()


def test_pending_successor_does_not_bypass_predecessor_drain_gate(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    world = _deferred_successor_world(tmp_path, "none")
    config = load_config(world["config_path"])
    before = _snapshot(world["repo"], world["runtime"])

    allowed = cli_module._predecessor_drain_allows_successor_activation(
        config,
        SimpleNamespace(
            phase=SimpleNamespace(value="DRAINED_AWAITING_TICK"),
            outcome=SimpleNamespace(
                status=SimpleNamespace(value="DRAINED"),
                reason_code="PROMOTION_EFFECT_ADMITTED",
            ),
        ),
    )

    assert allowed is False
    assert (world["runtime"] / "pending_promotion.yaml").is_file()
    assert _snapshot(world["repo"], world["runtime"]) == before
    _assert_frozen_read_only_product_bytes()


def test_pre_main_adoption_continuation_error_blocks_without_adopting_start(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_contract import ContinuationError
    from task_transition_runner.runner_continuation_lifecycle import (
        prepare_post_tick_continuation,
    )
    from task_transition_runner.runner_continuation_origin_proofs import (
        prove_finalized_promotion_tick,
    )
    from task_transition_runner.runner_git_context import release_runner_git_lock
    from task_transition_runner.task_activation import (
        evaluate_task_activation,
    )
    from task_transition_runner.task_source import load_task_source

    _assert_frozen_read_only_product_bytes()
    lifecycle = _load_continuation_lifecycle()
    seeded = lifecycle._seed(tmp_path / "pre-main-failure", chain="none", lock=True)
    repo = seeded["repo"]
    runtime = seeded["runtime"]
    _run_git(repo, "reset", "--hard", f"refs/heads/{_REFERENCE_MAIN_BRANCH}")
    proof = prove_finalized_promotion_tick(
        repo=repo,
        runtime=runtime,
        main_ref=f"refs/heads/{_REFERENCE_MAIN_BRANCH}",
    )
    predecessor = proof.to_mapping()["predecessor_identity"]
    config_path, _target, _public_runtime = _write_public_world(tmp_path / "public")
    _retarget_public_config(config_path, repo, runtime)
    _bind_fresh_synthetic_adapter(config_path, tmp_path / "public")
    _write_active_task(config_path, repo)
    task_source = yaml.safe_load(
        (config_path.parent / "tasks.yaml").read_text(encoding="utf-8")
    )
    task_source["active_task"]["task_id"] = lifecycle._SUCCESSOR_TASK_ID
    (config_path.parent / "tasks.yaml").write_text(
        yaml.safe_dump(task_source, sort_keys=False),
        encoding="utf-8",
    )
    _pin_runtime_to_source(runtime)
    config = load_config(config_path)
    basis = cli_module._canonical_settled_predecessor_basis(config)
    assert basis is not None
    canonical_predecessor = basis["predecessor"]
    assert (
        canonical_predecessor["task_id"],
        canonical_predecessor["origin_run_id"],
        canonical_predecessor["promotion_attempt_id"],
    ) == (
        predecessor["task_id"],
        predecessor["origin_run_id"],
        predecessor["promotion_attempt_id"],
    )
    successor = evaluate_task_activation(
        load_task_source(
            config,
            run_root=tmp_path / "successor-identity-probe",
        ),
        predecessor=basis,
    )
    identity = successor.identity
    start_sha = lifecycle._linear_starts(
        repo,
        seeded["tock_sha"],
        hops=1,
        task_id=identity.task_id,
        attempt_id=identity.promotion_attempt_id,
        tree_mode="same",
        origin_run_id=identity.origin_run_id,
    )[0]
    exact_ref = lifecycle._task_ref(identity.task_id, identity.origin_run_id)
    lifecycle._update_ref(repo, exact_ref, start_sha)
    prepared = prepare_post_tick_continuation(
        **lifecycle._prepare_kwargs(
            seeded,
            successor_task_id=identity.task_id,
            successor_origin_run_id=identity.origin_run_id,
            successor_promotion_attempt_id=identity.promotion_attempt_id,
            expected_continuation_head_sha=start_sha,
        )
    )
    assert prepared.contract.status == "AUTHORIZED"
    _run_git(repo, "checkout", "-f", _REFERENCE_MAIN_BRANCH)
    release_runner_git_lock(
        lock_path=runtime / "runner_git.lock",
        ownership=seeded["lock_ownership"],
    )
    before_main = _run_git(repo, "rev-parse", f"refs/heads/{_REFERENCE_MAIN_BRANCH}")
    before_state = (runtime / "main_state.yaml").read_bytes()
    before_refs = _run_git(repo, "for-each-ref", "--format=%(refname) %(objectname)")

    def _fail_pre_main_adoption(
        _config: object,
        *,
        expected_continuation_head_sha: str | None = None,
    ) -> None:
        assert expected_continuation_head_sha == start_sha
        raise ContinuationError("CONTINUATION_HEAD_MISMATCH")

    monkeypatch.setattr(
        cli_module,
        "_prepare_settled_continuation",
        _fail_pre_main_adoption,
    )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "CONTINUATION_HEAD_MISMATCH")
    assert _run_git(repo, "rev-parse", f"refs/heads/{_REFERENCE_MAIN_BRANCH}") == before_main
    assert (runtime / "main_state.yaml").read_bytes() == before_state
    assert (
        _run_git(repo, "for-each-ref", "--format=%(refname) %(objectname)")
        == before_refs
    )
    assert _run_git(repo, "rev-parse", exact_ref) == start_sha
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


def test_terminal_recheck_observes_real_task_source_transition(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=False,
    )
    _lifecycle, target, runtime = _rebind_canonical_predecessor_world(
        config_path,
        tmp_path,
    )
    task_source_path, _spec_path = _write_active_task(config_path, target)
    real_drain = cli_module.evaluate_drain_before_dispatch
    drain_calls: list[bool] = []

    def _transition_after_real_drain(**kwargs: object) -> object:
        result = real_drain(**kwargs)
        drain_calls.append(True)
        task_source_path.write_text(
            yaml.safe_dump(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "active_task": None,
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        cli_module,
        "evaluate_drain_before_dispatch",
        _transition_after_real_drain,
    )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert drain_calls == [True]
    assert exit_code == 0
    assert stdout["ok"] is True
    assert stdout["outcome"] == "TERMINAL_REST"
    assert stdout["reason_code"] == "ACTIVE_TASK_NULL"
    assert not (runtime / "runner_git.lock").exists()
    assert _run_git(target, "status", "--porcelain") == ""
    _assert_frozen_read_only_product_bytes()


def test_deferred_provenance_reaches_second_terminal_recheck(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=True,
    )
    before_git = _git_identity(target)
    real_drain = cli_module.evaluate_drain_before_dispatch
    drain_calls: list[bool] = []

    def _transition_after_real_drain(**kwargs: object) -> object:
        result = real_drain(**kwargs)
        drain_calls.append(True)
        _write_settled_predecessor(
            runtime,
            task_id="settled-predecessor-task",
            settled=True,
        )
        task_source_path = config_path.parent / "tasks.yaml"
        task_source = yaml.safe_load(task_source_path.read_text(encoding="utf-8"))
        task_source["active_task"] = None
        task_source_path.write_text(
            yaml.safe_dump(task_source, sort_keys=False),
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        cli_module,
        "evaluate_drain_before_dispatch",
        _transition_after_real_drain,
    )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert drain_calls == [True]
    assert exit_code == 0
    assert stdout["outcome"] == "TERMINAL_REST"
    assert stdout["reason_code"] == "ACTIVE_TASK_NULL"
    assert not (runtime / "runner_git.lock").exists()
    assert _git_identity(target) == before_git
    _assert_frozen_read_only_product_bytes()


def test_bound_coverage_continuation_prepare_and_proof_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_contract import ContinuationError

    _assert_frozen_read_only_product_bytes()
    config_path, target, runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    monkeypatch.setattr(
        cli_module,
        "_canonical_settled_predecessor_basis",
        lambda _config: {"kind": "SETTLED_PROMOTION"},
    )
    monkeypatch.setattr(cli_module, "_private_fresh_baseline", lambda _config: True)
    prepared: list[object] = []
    monkeypatch.setattr(
        cli_module,
        "_prepare_settled_continuation",
        lambda observed: prepared.append(observed),
    )

    prepared_result = cli_module.bind_ordinary_continuation(config)

    assert prepared == [config]
    assert prepared_result["reason_code"] == "CONTINUATION_ORIGIN_INVALID"

    def _raise_continuation_error(**_kwargs: Any) -> None:
        raise ContinuationError("CONTINUATION_HEAD_MISMATCH")

    monkeypatch.setattr(cli_module, "_private_fresh_baseline", lambda _config: False)
    monkeypatch.setattr(
        cli_module.continuation_proofs,
        "prove_finalized_promotion_tick",
        _raise_continuation_error,
    )

    blocked = cli_module.bind_ordinary_continuation(config)

    assert blocked["ok"] is False
    assert blocked["reason_code"] == "CONTINUATION_HEAD_MISMATCH"
    assert _run_git(target, "status", "--porcelain") == ""
    assert _tree_digest(runtime) == {}
    _assert_frozen_read_only_product_bytes()


def test_bound_coverage_consumed_terminal_probe_can_continue(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    ownership = dict(_LOCK_OWNERSHIP)
    terminal_probes: list[int] = []
    releases: list[dict[str, Any]] = []
    cli_module._RUN_LOCK.clear()
    monkeypatch.setattr(
        cli_module,
        "verify_code_authority",
        lambda _config: {"ok": True, "reason_code": "SOURCE_CHECKED"},
    )
    monkeypatch.setattr(
        cli_module.git_context_module,
        "evaluate_branch_preflight",
        lambda **_kwargs: {"ok": True, "reason_code": "GIT_PREFLIGHT_OK"},
    )
    monkeypatch.setattr(
        cli_module,
        "_qualify_task_branch_resume_preflight",
        lambda _config, preflight: preflight,
    )
    monkeypatch.setattr(
        cli_module,
        "acquire_runner_git_lock",
        lambda **_kwargs: {"ok": True, "ownership": ownership},
    )
    monkeypatch.setattr(
        cli_module,
        "release_runner_git_lock",
        lambda **kwargs: releases.append(kwargs["ownership"]),
    )
    monkeypatch.setattr(cli_module, "_entry_promotion_journal_phase", lambda _config: None)
    reconciled = {
        "ok": True,
        "reason_code": "RECONCILIATION_NO_WORK",
        "main_state": {},
    }
    monkeypatch.setattr(
        cli_module,
        "reconcile_ordinary_promotion",
        lambda _config: reconciled,
    )
    monkeypatch.setattr(
        cli_module,
        "_deferred_successor_provenance_gate",
        lambda _config: {"ok": True, "reason_code": "CONTINUATION_CONSUMED"},
    )
    monkeypatch.setattr(
        cli_module,
        "_load_activation_contract",
        lambda _config: SimpleNamespace(active_task=None),
    )
    monkeypatch.setattr(cli_module, "_persisted_inflight_workers", lambda _config: [])
    monkeypatch.setattr(
        cli_module,
        "_current_authority_inflight_workers",
        lambda _config, _contract, workers, **_kwargs: workers,
    )
    monkeypatch.setattr(
        cli_module,
        "_reprove_deferred_successor_window",
        lambda _config, result, _workers: result,
    )
    monkeypatch.setattr(cli_module, "_loaded_canonical_pending", lambda _config: None)
    monkeypatch.setattr(cli_module, "_decisions_ready", lambda _config: True)
    monkeypatch.setattr(
        cli_module.task_activation_module,
        "_fresh_main_state_bound",
        lambda _state: False,
    )
    monkeypatch.setattr(
        cli_module,
        "_activation_basis",
        lambda *_args, **_kwargs: {
            "kind": "SETTLED_PROMOTION",
            "predecessor": {"settled": True},
        },
    )

    def _terminal_probe(_config: object, _basis: object) -> dict[str, Any] | None:
        terminal_probes.append(len(terminal_probes))
        if len(terminal_probes) == 1:
            return None
        return cli_module._blocked("run", "COVERAGE_TERMINAL_STOP")

    monkeypatch.setattr(cli_module, "_pre_dispatch_terminal_activation", _terminal_probe)
    monkeypatch.setattr(
        cli_module,
        "_authorized_continuation_allows_resume",
        lambda _config, _reconciled: True,
    )

    exit_code = cli_module._command_run(config)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _blocked_stdout(stdout, "COVERAGE_TERMINAL_STOP")
    assert terminal_probes == [0, 1]
    assert releases == [ownership]
    assert cli_module._RUN_LOCK == {}
    _assert_frozen_read_only_product_bytes()


def test_bound_coverage_fresh_tail_fallback_and_snapshot_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, _target, runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    activation = SimpleNamespace(
        identity=SimpleNamespace(
            task_id="expected-task",
            origin_run_id="expected-origin",
            task_fingerprint="expected-fingerprint",
            instruction_sources=(),
        ),
        facts=object(),
    )
    cli_module._RUN_LOCK.clear()

    def _block_basis(_config: object) -> None:
        raise cli_module._SeamBlocked("COVERAGE_BASIS_STOP")

    monkeypatch.setattr(
        cli_module,
        "_canonical_settled_predecessor_basis",
        _block_basis,
    )
    with pytest.raises(cli_module._SeamBlocked, match="COVERAGE_BASIS_STOP"):
        cli_module._fresh_active_tail(config, activation)
    assert cli_module._RUN_LOCK == {}

    monkeypatch.setattr(
        cli_module.task_source_module,
        "load_task_source",
        lambda *_args, **_kwargs: SimpleNamespace(
            contract=SimpleNamespace(
                active_task=SimpleNamespace(task_id="other-task"),
                fingerprint="other-fingerprint",
            ),
            sources=(),
        ),
    )
    with pytest.raises(cli_module._SeamBlocked, match="ACTIVATION_BASIS_INVALID"):
        cli_module._materialize_canonical_run_snapshot(config, activation)

    assert not (runtime / "runs").exists()
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    ("defect", "expected_reason"),
    (
        ("drain_error", "ACTIVATION_PREDECESSOR_UNAVAILABLE"),
        ("drain_identity", "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"),
        ("snapshot_task", "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"),
    ),
)
def test_bound_coverage_canonical_predecessor_rejections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
    expected_reason: str,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_drain_transition import (
        DrainOutcomeError,
        DrainStatus,
    )
    from task_transition_runner.runner_promotion_journal import PromotionJournalPhase

    _assert_frozen_read_only_product_bytes()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "promotion_journal.yaml").write_text("bound: true\n", encoding="utf-8")
    config = SimpleNamespace(
        runtime_root=runtime,
        target_worktree=tmp_path,
        main_ref="refs/heads/main",
    )
    journal = SimpleNamespace(
        phase=PromotionJournalPhase.FINALIZED,
        drain_run_id="drain",
        tock_sha="tock",
        task_id="task",
        origin_run_id="origin",
        promotion_attempt_id="attempt",
        result_sha="result",
        promotion_class="GREEN_PROMOTION",
        task_fingerprint="fingerprint",
        task_snapshot_sha256="snapshot",
        instruction_signature_sha256="instructions",
    )
    proof_identity = {
        "task_id": journal.task_id,
        "origin_run_id": journal.origin_run_id,
        "promotion_attempt_id": journal.promotion_attempt_id,
        "result_sha": journal.result_sha,
        "promotion_class": journal.promotion_class,
    }
    proof = SimpleNamespace(
        to_mapping=lambda: {
            "predecessor_identity": proof_identity,
            "tock_sha": journal.tock_sha,
        }
    )
    drain = SimpleNamespace(
        status=DrainStatus.DRAINED,
        reason_code=(
            "OTHER_REASON"
            if defect == "drain_identity"
            else "PROMOTION_EFFECT_ADMITTED"
        ),
        drain_run_id=journal.drain_run_id,
        observed_main_head_sha=journal.tock_sha,
        task_id=journal.task_id,
        origin_run_id=journal.origin_run_id,
        promotion_attempt_id=journal.promotion_attempt_id,
        result_sha=journal.result_sha,
        promotion_class=journal.promotion_class,
        tock_sha=journal.tock_sha,
    )
    monkeypatch.setattr(cli_module, "load_promotion_journal", lambda **_kwargs: journal)
    monkeypatch.setattr(
        cli_module.continuation_proofs,
        "prove_finalized_promotion_tick",
        lambda **_kwargs: proof,
    )

    if defect == "drain_error":
        def _raise_drain(**_kwargs: Any) -> None:
            raise DrainOutcomeError("DRAIN_OUTCOME_INVALID")

        monkeypatch.setattr(
            cli_module.drain_module,
            "load_persisted_drain_outcome",
            _raise_drain,
        )
    else:
        monkeypatch.setattr(
            cli_module.drain_module,
            "load_persisted_drain_outcome",
            lambda **_kwargs: drain,
        )
    monkeypatch.setattr(
        cli_module.task_source_module,
        "reprove_run_local_snapshot",
        lambda **_kwargs: SimpleNamespace(task_id="other-task"),
    )

    with pytest.raises(cli_module._SeamBlocked, match=expected_reason):
        cli_module._canonical_settled_predecessor_basis(config)

    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize("defect", ("consumption_error", "no_matching_consumption"))
def test_bound_coverage_pre_tock_drain_rejections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_promotion_state import (
        RunnerPromotionStateError,
    )

    _assert_frozen_read_only_product_bytes()
    journal = SimpleNamespace(
        promotion_attempt_id="attempt",
        result_sha="result",
        tock_sha="tock",
    )
    if defect == "consumption_error":
        def _raise_consumption(**_kwargs: Any) -> None:
            raise RunnerPromotionStateError("PROMOTION_CONSUMED_INVALID")

        monkeypatch.setattr(
            cli_module.state_module,
            "load_promotion_consumed",
            _raise_consumption,
        )
    else:
        monkeypatch.setattr(
            cli_module.state_module,
            "load_promotion_consumed",
            lambda **_kwargs: {"consumed_promotions": []},
        )

    allowed = cli_module._reconciled_finalized_identity_allows_pre_tock_drain(
        runtime=tmp_path / "runtime",
        journal=journal,
        proof_identity={},
        proof_tock_sha="tock",
        drain=SimpleNamespace(),
        reconciled_identity={},
    )

    assert allowed is False
    _assert_frozen_read_only_product_bytes()


def test_bound_coverage_fallback_and_terminal_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_promotion_journal import PromotionJournalError

    _assert_frozen_read_only_product_bytes()
    config_path, _target, runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    (runtime / "promotion_journal.yaml").write_text(
        "schema_version: '1.0'\nphase: NOT_A_PHASE\n",
        encoding="utf-8",
    )
    assert cli_module._known_promotion_attempt(config) == config.main_ref

    monkeypatch.setattr(
        cli_module,
        "_canonical_settled_predecessor_basis",
        lambda _config: {
            "kind": "SETTLED_PROMOTION",
            "predecessor": {"settled": True},
        },
    )
    config.task_source.write_text(
        "schema_version: '1.0'\nactive_task: [malformed]\n",
        encoding="utf-8",
    )
    assert cli_module._terminal_without_successor_activation(config) is False

    def _raise_journal(**_kwargs: Any) -> None:
        raise PromotionJournalError("JOURNAL_PHASE_UNKNOWN", "coverage")

    monkeypatch.setattr(cli_module, "load_promotion_journal", _raise_journal)
    drain = SimpleNamespace(outcome=SimpleNamespace())
    assert (
        cli_module._predecessor_drain_allows_successor_activation(config, drain)
        is False
    )

    monkeypatch.setattr(
        cli_module,
        "_settled_predecessor_from_basis",
        lambda _basis: {"settled": True},
    )
    monkeypatch.setattr(
        cli_module,
        "_load_activation_contract",
        lambda _config: SimpleNamespace(active_task=None),
    )
    monkeypatch.setattr(
        cli_module.task_activation_module,
        "evaluate_task_activation",
        lambda *_args, **_kwargs: SimpleNamespace(
            status=ActivationStatus.BLOCKED,
            reason_code="COVERAGE_ACTIVATION_BLOCKED",
        ),
    )
    terminal = cli_module._pre_dispatch_terminal_activation(
        config,
        {"kind": "SETTLED_PROMOTION"},
    )
    assert terminal["reason_code"] == "COVERAGE_ACTIVATION_BLOCKED"
    _assert_frozen_read_only_product_bytes()


@pytest.mark.parametrize(
    "defect",
    ("missing_settled_basis", "invalid_journal", "predecessor_identity"),
)
def test_bound_coverage_deferred_provenance_rejections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_promotion_journal import (
        PromotionJournalError,
        PromotionJournalPhase,
    )

    _assert_frozen_read_only_product_bytes()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "post_tick_continuation.yaml").write_text("bound: true\n", encoding="utf-8")
    (runtime / "promotion_journal.yaml").write_text("bound: true\n", encoding="utf-8")
    config = SimpleNamespace(
        runtime_root=runtime,
        target_worktree=tmp_path,
    )
    continuation = SimpleNamespace(
        status="AUTHORIZED",
        identity=SimpleNamespace(
            task_id="foreign-task",
            origin_run_id="foreign-origin",
            promotion_attempt_id="foreign-attempt",
            result_sha="foreign-result",
            promotion_class="GREEN_PROMOTION",
        ),
        to_mapping=lambda: {"origin": {"evidence": {"tock_sha": "foreign-tock"}}},
    )
    journal = SimpleNamespace(
        phase=PromotionJournalPhase.FINALIZED,
        task_id="task",
        origin_run_id="origin",
        promotion_attempt_id="attempt",
        result_sha="result",
        promotion_class="GREEN_PROMOTION",
        tock_sha="tock",
    )
    monkeypatch.setattr(
        cli_module,
        "load_continuation_artifact",
        lambda _path: continuation,
    )
    monkeypatch.setattr(
        cli_module,
        "_canonical_settled_predecessor_basis",
        lambda _config, **_kwargs: (
            None
            if defect == "missing_settled_basis"
            else {
                "kind": "SETTLED_PROMOTION",
                "predecessor": {
                    "settled": True,
                    "task_id": journal.task_id,
                    "origin_run_id": journal.origin_run_id,
                    "promotion_attempt_id": journal.promotion_attempt_id,
                },
            }
        ),
    )
    if defect == "invalid_journal":
        def _raise_journal(**_kwargs: Any) -> None:
            raise PromotionJournalError("JOURNAL_PHASE_UNKNOWN", "coverage")

        monkeypatch.setattr(cli_module, "load_promotion_journal", _raise_journal)
    else:
        monkeypatch.setattr(
            cli_module,
            "load_promotion_journal",
            lambda **_kwargs: journal,
        )

    result = cli_module._deferred_successor_provenance_gate(config)

    expected_reason = {
        "missing_settled_basis": "ACTIVATION_PREDECESSOR_UNAVAILABLE",
        "invalid_journal": "JOURNAL_PHASE_UNKNOWN",
        "predecessor_identity": "RECONCILIATION_IDENTITY_MISMATCH",
    }[defect]
    assert result["ok"] is False
    assert result["reason_code"] == expected_reason
    _assert_frozen_read_only_product_bytes()


def test_bound_coverage_consumed_reproof_identity_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_promotion_journal import PromotionJournalPhase

    _assert_frozen_read_only_product_bytes()
    config = SimpleNamespace(
        runtime_root=tmp_path / "runtime",
        target_worktree=tmp_path,
        task_source=tmp_path / "tasks.yaml",
    )
    journal = SimpleNamespace(
        phase=PromotionJournalPhase.FINALIZED,
        task_id="task",
        origin_run_id="origin",
        promotion_attempt_id="attempt",
        result_sha="result",
        promotion_class="GREEN_PROMOTION",
        base_main_sha="start",
        tock_sha="tock",
    )
    predecessor_journal = SimpleNamespace(
        phase=PromotionJournalPhase.FINALIZED,
        task_id="predecessor",
        origin_run_id="predecessor-origin",
        promotion_attempt_id="predecessor-attempt",
        result_sha="predecessor-result",
        promotion_class="GREEN_PROMOTION",
        tock_sha="predecessor-tock",
    )
    continuation = SimpleNamespace(
        status="CONSUMED",
        identity=SimpleNamespace(
            task_id=predecessor_journal.task_id,
            origin_run_id=predecessor_journal.origin_run_id,
            promotion_attempt_id=predecessor_journal.promotion_attempt_id,
            result_sha=predecessor_journal.result_sha,
            promotion_class=predecessor_journal.promotion_class,
        ),
        expected_continuation_head_sha=journal.base_main_sha,
        successor_task_id=journal.task_id,
        to_mapping=lambda: {
            "origin": {"evidence": {"tock_sha": predecessor_journal.tock_sha}},
            "successor_promotion_attempt_id": journal.promotion_attempt_id,
            "successor_tock_sha": journal.tock_sha,
        },
    )
    monkeypatch.setattr(
        cli_module.state_module,
        "load_and_validate_main_state",
        lambda **_kwargs: {
            "validated_result_sha": journal.result_sha,
            "last_tock_sha": journal.tock_sha,
            "observed_main_head_sha": journal.tock_sha,
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
        },
    )
    monkeypatch.setattr(
        cli_module.state_module,
        "load_promotion_consumed",
        lambda **_kwargs: {"consumed_promotions": []},
    )
    monkeypatch.setattr(
        cli_module,
        "load_task_contract",
        lambda *_args, **_kwargs: SimpleNamespace(active_task=None),
    )
    monkeypatch.setattr(
        cli_module,
        "load_archived_promotion_journal",
        lambda **_kwargs: predecessor_journal,
    )

    def _git_fact(_repo: Path, *args: str) -> str:
        if args[0] == "log":
            return "WRONG START SUBJECT"
        if args[0] == "rev-list":
            return "start wrong-parent"
        return "tree"

    monkeypatch.setattr(cli_module, "_git_text", _git_fact)
    result = cli_module._consumed_continuation_reproof(
        config,
        settled={
            "settled": True,
            "task_id": journal.task_id,
            "origin_run_id": journal.origin_run_id,
            "promotion_attempt_id": journal.promotion_attempt_id,
        },
        continuation=continuation,
        journal=journal,
    )

    assert result["ok"] is False
    assert result["reason_code"] == "RECONCILIATION_IDENTITY_MISMATCH"
    _assert_frozen_read_only_product_bytes()


_REAL_CLI_PROGRAM = """\
import os
import sys
from pathlib import Path

Path(os.environ["OSS6_CLI_PIDFILE"]).write_text(str(os.getpid()))
from task_transition_runner.cli import main
raise SystemExit(main(sys.argv[1:]))
"""
_POST_TOCK_HOOK = """\
#!/bin/sh
set -u
if [ "$1" != "committed" ]; then
  exit 0
fi
pid=$(cat "$OSS6_CLI_PIDFILE")
while read -r old new ref; do
  if [ "$ref" = "refs/heads/main" ]; then
    subject=$(git log -1 --format=%s "$new" || true)
    if [ "$subject" = "TOCK" ]; then
      kill -9 "$pid"
    fi
  fi
done
exit 0
"""
_ADAPTER_MARKER_INSERT = (
    "    marker = os.environ.get('OSS6_ADAPTER_MARKER')\n"
    "    if marker:\n"
    "        with open(marker, 'a', encoding='utf-8') as handle:\n"
    "            handle.write('invoked\\n')\n"
)


def _candidate_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _clone_clean_runner_source(tmp_path: Path) -> Path:
    source_root = tmp_path / "runner-source"
    subprocess.run(  # noqa: S603
        [
            "git",
            "clone",
            "--quiet",
            "--no-hardlinks",
            str(_candidate_root()),
            str(source_root),
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    assert _run_git(source_root, "rev-parse", "HEAD") == _run_git(
        _candidate_root(),
        "rev-parse",
        "HEAD",
    )
    assert _run_git(source_root, "status", "--porcelain=v1", "-uall") == ""
    return source_root


def _arm_adapter_marker(tmp_path: Path) -> Path:
    executable = tmp_path / "synthetic-cursor-adapter"
    source = executable.read_text(encoding="utf-8")
    needle = "def main() -> None:\n"
    assert needle in source
    executable.write_text(
        source.replace(needle, needle + _ADAPTER_MARKER_INSERT, 1),
        encoding="utf-8",
    )
    return tmp_path / "adapter-invocations"


def _marker_count(marker: Path) -> int:
    if not marker.is_file():
        return 0
    return len([line for line in marker.read_text(encoding="utf-8").splitlines() if line])


_ROLE_PREPARATION = "PREPARATION"
_ROLE_ACCEPTANCE = "ACCEPTANCE"
_B29_PROCESS_PLAN = (
    (_ROLE_ACCEPTANCE, "RESULT_COMMIT"),
    (_ROLE_ACCEPTANCE, "RESULT_REPROOF"),
)
_B30_PROCESS_PLAN = (
    (_ROLE_PREPARATION, "AWAITING_DECISIONS"),
    (_ROLE_PREPARATION, "PENDING_PROMOTION"),
    (_ROLE_ACCEPTANCE, "KILL_AFTER_COMMITTED_TOCK"),
    (_ROLE_ACCEPTANCE, "RESTART"),
)
_B19_PROCESS_PLAN = (
    (_ROLE_PREPARATION, "PREDECESSOR_AWAITING_DECISIONS"),
    (_ROLE_PREPARATION, "PREDECESSOR_PENDING_PROMOTION"),
    (_ROLE_PREPARATION, "PREDECESSOR_DRAINED"),
    (_ROLE_PREPARATION, "SUCCESSOR_AWAITING_DECISIONS"),
    (_ROLE_PREPARATION, "SUCCESSOR_PENDING_PROMOTION"),
    (_ROLE_ACCEPTANCE, "SUCCESSOR_SETTLED_AND_CONTINUATION_CONSUMED"),
    (_ROLE_ACCEPTANCE, "TERMINAL_REST"),
)


def _invoke_real_cli(
    config_path: Path,
    pidfile: Path,
    marker: Path,
    *,
    source_root: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], int]:
    env = os.environ.copy()
    env["OSS6_CLI_PIDFILE"] = str(pidfile)
    env["OSS6_ADAPTER_MARKER"] = str(marker)
    env.pop("CURSOR_API_ENDPOINT", None)
    working_root = _candidate_root() if source_root is None else source_root
    source_python = str(working_root / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        source_python
        if not existing_pythonpath
        else f"{source_python}{os.pathsep}{existing_pythonpath}"
    )
    if pidfile.exists():
        pidfile.unlink()
    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            _REAL_CLI_PROGRAM,
            "run",
            "--config",
            str(config_path),
        ],
        cwd=working_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert pidfile.is_file(), completed.stderr
    return completed, int(pidfile.read_text(encoding="utf-8"))


def _assert_acceptance_plan(plan: tuple[tuple[str, str], ...]) -> None:
    roles = [role for role, _step in plan]
    first_acceptance = roles.index(_ROLE_ACCEPTANCE)
    assert roles[first_acceptance:] == [_ROLE_ACCEPTANCE, _ROLE_ACCEPTANCE]
    assert _ROLE_ACCEPTANCE not in roles[:first_acceptance]
    assert set(roles) <= {_ROLE_PREPARATION, _ROLE_ACCEPTANCE}


def _record_cli_process(
    ledger: list[dict[str, Any]],
    *,
    role: str,
    step: str,
    pid: int,
    completed: subprocess.CompletedProcess[str],
) -> None:
    assert role in {_ROLE_PREPARATION, _ROLE_ACCEPTANCE}
    assert isinstance(pid, int) and pid > 0
    assert all(row["pid"] != pid for row in ledger)
    if any(row["role"] == _ROLE_ACCEPTANCE for row in ledger):
        assert role == _ROLE_ACCEPTANCE
        assert ledger[-1]["role"] == _ROLE_ACCEPTANCE
    ledger.append(
        {
            "role": role,
            "step": step,
            "pid": pid,
            "returncode": completed.returncode,
        }
    )


def _account_real_cli(
    config_path: Path,
    pidfile: Path,
    marker: Path,
    ledger: list[dict[str, Any]],
    *,
    role: str,
    step: str,
    source_root: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], int]:
    completed, pid = _invoke_real_cli(
        config_path,
        pidfile,
        marker,
        source_root=source_root,
    )
    _record_cli_process(ledger, role=role, step=step, pid=pid, completed=completed)
    return completed, pid


def _assert_spawned_ledger(
    ledger: list[dict[str, Any]],
    plan: tuple[tuple[str, str], ...],
) -> None:
    _assert_acceptance_plan(plan)
    actual = [(row["role"], row["step"]) for row in ledger]
    assert actual == list(plan[: len(ledger)])
    assert actual
    assert len({row["pid"] for row in ledger}) == len(ledger)


def _stdout_payload(completed: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    assert completed.stdout, completed.stderr
    return _read_stdout_yaml(completed.stdout)


def _ref_subjects(repo: Path) -> list[str]:
    listing = _run_git(repo, "for-each-ref", "--format=%(contents:subject)")
    return [line for line in listing.splitlines() if line]


def _first_parent_subjects(repo: Path, main_ref: str = "refs/heads/main") -> list[str]:
    revisions = _run_git(repo, "rev-list", "--first-parent", main_ref)
    return [
        _run_git(repo, "log", "-1", "--format=%s", revision)
        for revision in revisions.splitlines()
        if revision
    ]


def _sole_worker(runtime: Path) -> dict[str, Any]:
    paths = sorted((runtime / "runs").glob("*/worker_result.yaml"))
    assert len(paths) == 1, [str(path) for path in paths]
    payload = yaml.safe_load(paths[0].read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _fresh_basis_identity(
    config_path: Path,
    tmp_path: Path,
    main_state: dict[str, Any],
) -> Any:
    from task_transition_runner.config import load_config
    from task_transition_runner.task_activation import (
        evaluate_task_activation,
        select_cli_activation_basis,
    )
    from task_transition_runner.task_source import load_task_source

    basis = select_cli_activation_basis(
        main_state=main_state,
        settled_predecessor=None,
    )
    assert basis["kind"] == "FRESH_INIT_BASELINE"
    assert "predecessor" not in basis
    facts = load_task_source(
        load_config(config_path),
        run_root=tmp_path / "fresh-basis-probe",
    )
    decision = evaluate_task_activation(facts, predecessor=basis)
    return decision.identity


def _bind_external_decisions(runtime: Path, worker: dict[str, Any]) -> None:
    _write_canonical_runtime_decisions(
        runtime,
        SimpleNamespace(
            task_id=worker["task_id"],
            origin_run_id=worker["origin_run_id"],
            promotion_attempt_id=worker["promotion_attempt_id"],
        ),
        str(worker["result_sha"]),
        locations=("run",),
    )


def _git_stdin(repo: Path, *args: str, payload: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        input=payload,
    )
    return completed.stdout.strip()


def _install_post_tock_hook(target: Path, main_ref: str) -> Path:
    hook = target / ".git" / "hooks" / "reference-transaction"
    hook.parent.mkdir(parents=True, exist_ok=True)
    hook.write_text(
        _POST_TOCK_HOOK.replace("refs/heads/main", main_ref, 1),
        encoding="utf-8",
    )
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)
    return hook


def _plant_nonauthoritative_side_context(
    target: Path,
    *,
    main_ref: str,
    tock_sha: str,
) -> dict[str, str]:
    message = _run_git(target, "log", "-1", "--format=%B", tock_sha)
    first_parent = _run_git(target, "rev-parse", f"{tock_sha}^1")
    second_parent = _run_git(target, "rev-parse", f"{tock_sha}^2")
    blob = _git_stdin(
        target,
        "hash-object",
        "-w",
        "--stdin",
        payload="synthetic-side-context\n",
    )
    tree = _git_stdin(
        target,
        "mktree",
        payload=f"100644 blob {blob}\tside-context.txt\n",
    )
    side_tock = _git_stdin(
        target,
        "commit-tree",
        tree,
        "-p",
        first_parent,
        "-p",
        second_parent,
        payload=message,
    )
    carrier = _git_stdin(
        target,
        "commit-tree",
        tree,
        "-p",
        first_parent,
        "-p",
        side_tock,
        payload="synthetic side-parent carrier\n",
    )
    side_ref = "refs/heads/synthetic-side-ref"
    side_parent_ref = "refs/heads/synthetic-side-parent"
    _run_git(target, "update-ref", side_ref, side_tock)
    _run_git(target, "update-ref", side_parent_ref, carrier)
    chain = set(_run_git(target, "rev-list", "--first-parent", main_ref).splitlines())
    assert side_tock != tock_sha
    assert side_tock not in chain
    assert carrier not in chain
    assert _run_git(target, "rev-parse", f"{carrier}^2") == side_tock
    assert _run_git(target, "status", "--porcelain=v1", "-uall") == ""
    return {
        "side_ref": side_ref,
        "side_parent_ref": side_parent_ref,
        "side_tock": side_tock,
        "carrier": carrier,
    }


def _reach_real_pending(
    config_path: Path,
    runtime: Path,
    pidfile: Path,
    marker: Path,
    ledger: list[dict[str, Any]],
    steps: tuple[str, str],
    source_root: Path | None = None,
) -> None:
    first, _first_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_ROLE_PREPARATION,
        step=steps[0],
        source_root=source_root,
    )
    first_stdout = _stdout_payload(first)
    assert first_stdout["outcome"] == "AWAITING_DECISIONS", first.stdout
    _bind_external_decisions(runtime, _sole_worker(runtime))
    second, _second_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_ROLE_PREPARATION,
        step=steps[1],
        source_root=source_root,
    )
    second_stdout = _stdout_payload(second)
    assert second_stdout["outcome"] == "PENDING_PROMOTION", second.stdout
    assert [(row["role"], row["step"]) for row in ledger] == [
        (_ROLE_PREPARATION, steps[0]),
        (_ROLE_PREPARATION, steps[1]),
    ]
    assert ledger[0]["pid"] != ledger[1]["pid"]


def test_real_two_process_pre_authorization_result_reproof(tmp_path: Path) -> None:
    _assert_acceptance_plan(_B29_PROCESS_PLAN)
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    marker = _arm_adapter_marker(tmp_path)
    source_root = _clone_clean_runner_source(tmp_path)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []
    main_state = yaml.safe_load((runtime / "main_state.yaml").read_text(encoding="utf-8"))
    assert isinstance(main_state, dict)
    expected = _fresh_basis_identity(config_path, tmp_path, main_state)

    first, _first_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B29_PROCESS_PLAN[0][0],
        step=_B29_PROCESS_PLAN[0][1],
        source_root=source_root,
    )
    first_stdout = _stdout_payload(first)
    assert sorted((runtime / "runs").glob("*/worker_result.yaml")), (
        first.returncode,
        first.stdout,
        first.stderr,
    )
    worker = _sole_worker(runtime)
    result_commits = [
        line
        for line in _run_git(target, "log", "--all", "--format=%H %s").splitlines()
        if line.partition(" ")[2].startswith("RESULT ")
    ]

    assert first_stdout["outcome"] == "AWAITING_DECISIONS"
    assert first_stdout["reason_code"] == "PROMOTION_DECISION_MISSING"
    assert _marker_count(marker) == 1
    assert len(result_commits) == 1
    assert result_commits[0].partition(" ")[2] == (
        f"RESULT {expected.task_id} {expected.origin_run_id} "
        f"{expected.promotion_attempt_id}"
    )
    assert worker["task_id"] == expected.task_id
    assert worker["origin_run_id"] == expected.origin_run_id
    assert worker["promotion_attempt_id"] == expected.promotion_attempt_id
    assert worker["result_sha"] == _run_git(
        target,
        "rev-parse",
        f"refs/heads/{worker['task_branch']}",
    )
    worker_bytes = (runtime / "runs" / expected.origin_run_id / "worker_result.yaml").read_bytes()
    result_sha = str(worker["result_sha"])
    decisions = runtime / "runs" / expected.origin_run_id / "decisions"
    assert not (decisions / "orchestrator_decision.yaml").exists()
    assert not (decisions / "validator_decision.yaml").exists()
    assert not (runtime / "pending_promotion.yaml").exists()
    _bind_external_decisions(runtime, worker)

    second, _second_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B29_PROCESS_PLAN[1][0],
        step=_B29_PROCESS_PLAN[1][1],
        source_root=source_root,
    )
    second_stdout = _stdout_payload(second)
    reproof = _sole_worker(runtime)
    _assert_spawned_ledger(ledger, _B29_PROCESS_PLAN)

    assert ledger[0]["pid"] != ledger[1]["pid"]
    assert _marker_count(marker) == 1
    assert reproof["task_id"] == expected.task_id
    assert reproof["origin_run_id"] == expected.origin_run_id
    assert reproof["promotion_attempt_id"] == expected.promotion_attempt_id
    assert reproof["result_sha"] == result_sha
    assert (runtime / "runs" / expected.origin_run_id / "worker_result.yaml").read_bytes() == worker_bytes
    assert [
        line
        for line in _run_git(target, "log", "--all", "--format=%H %s").splitlines()
        if line.partition(" ")[2].startswith("RESULT ")
    ] == result_commits
    assert "TOCK" not in _first_parent_subjects(target)
    assert second_stdout["outcome"] == "PENDING_PROMOTION"
    assert second_stdout["reason_code"] == "PROMOTION_INTENT_PERSISTED"
    _assert_frozen_read_only_product_bytes()


def test_real_two_process_post_tock_bookkeeping(tmp_path: Path) -> None:
    from task_transition_runner.config import load_config

    _assert_acceptance_plan(_B30_PROCESS_PLAN)
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    marker = _arm_adapter_marker(tmp_path)
    source_root = _clone_clean_runner_source(tmp_path)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []
    main_ref = load_config(config_path).main_ref
    assert main_ref.startswith("refs/heads/")
    _reach_real_pending(
        config_path,
        runtime,
        pidfile,
        marker,
        ledger,
        (_B30_PROCESS_PLAN[0][1], _B30_PROCESS_PLAN[1][1]),
        source_root=source_root,
    )
    hook = _install_post_tock_hook(target, main_ref)
    killed, killed_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B30_PROCESS_PLAN[2][0],
        step=_B30_PROCESS_PLAN[2][1],
        source_root=source_root,
    )
    hook.unlink()
    pidfile.unlink()

    assert killed.returncode == -9, (killed.returncode, killed.stdout, killed.stderr)
    with pytest.raises(ProcessLookupError):
        os.kill(killed_pid, 0)
    journal = yaml.safe_load((runtime / "promotion_journal.yaml").read_text(encoding="utf-8"))
    assert journal["phase"] == "APPLYING"
    assert "tock_sha" not in journal
    chain = _run_git(target, "rev-list", "--first-parent", main_ref).splitlines()
    subjects = [
        _run_git(target, "log", "-1", "--format=%s", revision) for revision in chain
    ]
    assert subjects.count("TOCK") == 1
    assert subjects[0] == "TOCK"
    parent_line = _run_git(target, "rev-list", "--parents", "-1", main_ref)
    tock_sha, base_sha, result_sha = parent_line.split()
    assert tock_sha == chain[0]
    assert base_sha == journal["base_main_sha"]
    assert result_sha == journal["result_sha"]
    assert _run_git(target, "rev-parse", main_ref) == tock_sha
    pre_main = yaml.safe_load((runtime / "main_state.yaml").read_text(encoding="utf-8"))
    assert pre_main.get("last_tock_sha") != tock_sha
    assert pre_main.get("observed_main_head_sha") != tock_sha
    assert (runtime / "pending_promotion.yaml").is_file()
    assert not (runtime / "promotion_consumed.yaml").is_file()
    side = _plant_nonauthoritative_side_context(
        target,
        main_ref=main_ref,
        tock_sha=tock_sha,
    )
    assert _run_git(target, "rev-list", "--first-parent", main_ref).splitlines() == chain

    recovered, recovered_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B30_PROCESS_PLAN[3][0],
        step=_B30_PROCESS_PLAN[3][1],
        source_root=source_root,
    )
    recovered_stdout = _stdout_payload(recovered)
    finished = yaml.safe_load((runtime / "promotion_journal.yaml").read_text(encoding="utf-8"))
    _assert_spawned_ledger(ledger, _B30_PROCESS_PLAN)

    assert killed_pid != recovered_pid
    assert ledger[-2]["pid"] == killed_pid
    assert ledger[-1]["pid"] == recovered_pid
    assert recovered.returncode == 0, (recovered.returncode, recovered.stdout, recovered.stderr)
    assert recovered_stdout["outcome"] == "RECOVERED", recovered_stdout
    assert recovered_stdout["reason_code"] == "RECONCILIATION_COMPLETED", recovered_stdout
    assert finished["phase"] == "FINALIZED"
    assert finished["tock_sha"] == tock_sha
    assert _run_git(target, "rev-list", "--first-parent", main_ref).splitlines() == chain
    assert _first_parent_subjects(target, main_ref).count("TOCK") == 1
    assert _run_git(target, "rev-parse", main_ref) == tock_sha
    assert _run_git(target, "rev-parse", side["side_ref"]) == side["side_tock"]
    assert _run_git(target, "rev-parse", side["side_parent_ref"]) == side["carrier"]
    assert not (runtime / "pending_promotion.yaml").is_file()
    consumed = yaml.safe_load(
        (runtime / "promotion_consumed.yaml").read_text(encoding="utf-8")
    )
    assert len(consumed["consumed_promotions"]) == 1
    assert consumed["consumed_promotions"][0]["tock_sha"] == tock_sha
    assert (
        consumed["consumed_promotions"][0]["promotion_attempt_id"]
        == finished["promotion_attempt_id"]
    )
    main_state = yaml.safe_load((runtime / "main_state.yaml").read_text(encoding="utf-8"))
    assert main_state["state"] == "GREEN_VALIDATED"
    assert main_state["validated_result_sha"] == journal["result_sha"]
    assert main_state["last_tock_sha"] == tock_sha
    assert main_state["observed_main_head_sha"] == tock_sha
    assert main_state["reason_code"] == "RECONCILIATION_COMPLETED"
    _assert_frozen_read_only_product_bytes()


def test_authenticated_predecessor_basis_is_reused_across_successor_run_tail(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    _assert_acceptance_plan(_B30_PROCESS_PLAN)
    config_path, target, runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=True,
    )
    marker = _arm_adapter_marker(tmp_path)
    source_root = _clone_clean_runner_source(tmp_path)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []
    _reach_real_pending(
        config_path,
        runtime,
        pidfile,
        marker,
        ledger,
        (_B30_PROCESS_PLAN[0][1], _B30_PROCESS_PLAN[1][1]),
        source_root=source_root,
    )
    hook = _install_post_tock_hook(target, "refs/heads/main")
    try:
        killed, killed_pid = _account_real_cli(
            config_path,
            pidfile,
            marker,
            ledger,
            role=_B30_PROCESS_PLAN[2][0],
            step=_B30_PROCESS_PLAN[2][1],
            source_root=source_root,
        )
    finally:
        hook.unlink(missing_ok=True)
        pidfile.unlink(missing_ok=True)

    assert killed.returncode == -9, (killed.returncode, killed.stdout, killed.stderr)
    with pytest.raises(ProcessLookupError):
        os.kill(killed_pid, 0)
    recovered, recovered_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B30_PROCESS_PLAN[3][0],
        step=_B30_PROCESS_PLAN[3][1],
        source_root=source_root,
    )
    recovered_stdout = _stdout_payload(recovered)
    _assert_spawned_ledger(ledger, _B30_PROCESS_PLAN)
    assert recovered_pid != killed_pid
    assert recovered.returncode == 0, (
        recovered.returncode,
        recovered.stdout,
        recovered.stderr,
    )
    assert recovered_stdout["outcome"] == "RECOVERED", recovered_stdout
    assert recovered_stdout["reason_code"] == "RECONCILIATION_COMPLETED"

    journal = yaml.safe_load(
        (runtime / "promotion_journal.yaml").read_text(encoding="utf-8")
    )
    assert journal["phase"] == "FINALIZED"
    assert not (runtime / "pending_promotion.yaml").exists()
    assert not (runtime / "post_tick_continuation.yaml").exists()

    task_path = config_path.parent / "tasks.yaml"
    task_document = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    successor_task_id = "synthetic-task-002"
    task_document["active_task"]["task_id"] = successor_task_id
    task_path.write_text(
        yaml.safe_dump(task_document, sort_keys=False),
        encoding="utf-8",
    )

    canonical_calls: list[dict[str, Any] | None] = []
    activation_bases: list[dict[str, Any]] = []
    consumer_calls: list[tuple[str, bool]] = []
    original_canonical_basis = cli_module._canonical_settled_predecessor_basis
    original_activation_basis = cli_module._activation_basis

    def _record_canonical_basis(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
        if activation_bases:
            raise AssertionError(
                "IDENTITYLESS_PREDECESSOR_REAUTHORIZATION_AFTER_BOUND_BASIS"
            )
        basis = original_canonical_basis(*args, **kwargs)
        canonical_calls.append(basis)
        return basis

    def _record_activation_basis(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
        basis = original_activation_basis(*args, **kwargs)
        assert isinstance(basis, dict)
        activation_bases.append(basis)
        return basis

    def _observe_basis_consumer(label: str, original: Any) -> Any:
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            bound = activation_bases[0] if activation_bases else None
            supplied = bound is not None and any(
                value is bound for value in (*args, *kwargs.values())
            )
            consumer_calls.append((label, supplied))
            return original(*args, **kwargs)

        return _wrapped

    monkeypatch.setattr(
        cli_module,
        "_canonical_settled_predecessor_basis",
        _record_canonical_basis,
    )
    monkeypatch.setattr(cli_module, "_activation_basis", _record_activation_basis)
    for name in (
        "_terminal_without_successor_activation",
        "_pre_dispatch_terminal_activation",
        "_authorized_continuation_allows_resume",
        "_predecessor_drain_allows_successor_activation",
        "bind_ordinary_continuation",
    ):
        monkeypatch.setattr(
            cli_module,
            name,
            _observe_basis_consumer(name, getattr(cli_module, name)),
        )
    monkeypatch.setattr(
        cli_module,
        "verify_code_authority",
        lambda _config: {"ok": True, "reason_code": "SOURCE_CHECKED"},
    )

    exit_code = main(["run", "--config", str(config_path)])
    successor_stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    assert successor_stdout["outcome"] == "AWAITING_DECISIONS", successor_stdout
    assert successor_stdout["reason_code"] == "PROMOTION_DECISION_MISSING"
    assert len(activation_bases) == 1
    authenticated_basis = activation_bases[0]
    assert authenticated_basis["kind"] == "SETTLED_PROMOTION"
    assert authenticated_basis["predecessor"]["settled"] is True
    assert authenticated_basis["predecessor"]["task_id"] == journal["task_id"]
    assert (
        authenticated_basis["predecessor"]["origin_run_id"]
        == journal["origin_run_id"]
    )
    assert (
        authenticated_basis["predecessor"]["promotion_attempt_id"]
        == journal["promotion_attempt_id"]
    )
    assert canonical_calls
    assert canonical_calls[-1] is authenticated_basis
    expected_consumers = {
        "_terminal_without_successor_activation",
        "_pre_dispatch_terminal_activation",
        "_authorized_continuation_allows_resume",
        "_predecessor_drain_allows_successor_activation",
        "bind_ordinary_continuation",
    }
    assert {label for label, _supplied in consumer_calls} == expected_consumers
    assert all(supplied for _label, supplied in consumer_calls), (
        "IDENTITYLESS_PREDECESSOR_REAUTHORIZATION_AFTER_BOUND_BASIS: "
        f"consumer_basis_flow={consumer_calls}"
    )
    continuation = yaml.safe_load(
        (runtime / "post_tick_continuation.yaml").read_text(encoding="utf-8")
    )
    assert continuation["status"] == "AUTHORIZED"
    assert continuation["successor_task_id"] == successor_task_id
    assert not (runtime / _SETTLED_PREDECESSOR_NAME).exists()
    assert cli_module._RUN_LOCK == {}
    _assert_frozen_read_only_product_bytes()


def test_authorized_successor_start_drain_is_reproved_across_process_boundary(
    tmp_path: Path,
) -> None:
    from task_transition_runner.runner_continuation_contract import binding_digest

    config_path, target, runtime = _prepare_fresh_surface(
        tmp_path,
        active_task=True,
    )
    marker = _arm_adapter_marker(tmp_path)
    source_root = _clone_clean_runner_source(tmp_path)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []
    _reach_real_pending(
        config_path,
        runtime,
        pidfile,
        marker,
        ledger,
        ("PREDECESSOR_AWAITING_DECISIONS", "PREDECESSOR_PENDING_PROMOTION"),
        source_root=source_root,
    )
    hook = _install_post_tock_hook(target, "refs/heads/main")
    try:
        killed, killed_pid = _invoke_real_cli(
            config_path,
            pidfile,
            marker,
            source_root=source_root,
        )
    finally:
        hook.unlink(missing_ok=True)
        pidfile.unlink(missing_ok=True)

    assert killed.returncode == -9, (killed.returncode, killed.stdout, killed.stderr)
    with pytest.raises(ProcessLookupError):
        os.kill(killed_pid, 0)
    recovered, recovered_pid = _invoke_real_cli(
        config_path,
        pidfile,
        marker,
        source_root=source_root,
    )
    recovered_stdout = _stdout_payload(recovered)
    assert recovered_pid != killed_pid
    assert recovered.returncode == 0, (
        recovered.returncode,
        recovered.stdout,
        recovered.stderr,
    )
    assert recovered_stdout == {
        "ok": True,
        "command": "run",
        "outcome": "RECOVERED",
        "status": "projected",
        "reason_code": "RECONCILIATION_COMPLETED",
    }

    predecessor_journal = yaml.safe_load(
        (runtime / "promotion_journal.yaml").read_text(encoding="utf-8")
    )
    assert predecessor_journal["phase"] == "FINALIZED"
    predecessor_tock = predecessor_journal["tock_sha"]
    predecessor_consumption = yaml.safe_load(
        (runtime / "promotion_consumed.yaml").read_text(encoding="utf-8")
    )
    matching_consumptions = [
        entry
        for entry in predecessor_consumption["consumed_promotions"]
        if entry["promotion_attempt_id"]
        == predecessor_journal["promotion_attempt_id"]
        and entry["result_sha"] == predecessor_journal["result_sha"]
        and entry["tock_sha"] == predecessor_tock
    ]
    assert len(matching_consumptions) == 1

    task_path = config_path.parent / "tasks.yaml"
    task_document = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    successor_task_id = "synthetic-task-002"
    task_document["active_task"]["task_id"] = successor_task_id
    task_path.write_text(
        yaml.safe_dump(task_document, sort_keys=False),
        encoding="utf-8",
    )
    awaiting, awaiting_pid = _invoke_real_cli(
        config_path,
        pidfile,
        marker,
        source_root=source_root,
    )
    awaiting_stdout = _stdout_payload(awaiting)
    assert awaiting_pid not in {killed_pid, recovered_pid}
    assert awaiting.returncode == 1, (
        awaiting.returncode,
        awaiting.stdout,
        awaiting.stderr,
    )
    assert awaiting_stdout["outcome"] == "AWAITING_DECISIONS", awaiting_stdout
    assert awaiting_stdout["reason_code"] == "PROMOTION_DECISION_MISSING"

    successor_workers = [
        yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in sorted((runtime / "runs").glob("*/worker_result.yaml"))
        if yaml.safe_load(path.read_text(encoding="utf-8"))["task_id"]
        == successor_task_id
    ]
    assert len(successor_workers) == 1
    successor_worker = successor_workers[0]
    start_sha = _run_git(target, "rev-parse", "refs/heads/main")
    assert start_sha != predecessor_tock
    assert _run_git(target, "rev-parse", f"{start_sha}^") == predecessor_tock
    assert _run_git(target, "rev-parse", f"{start_sha}^{{tree}}") == _run_git(
        target,
        "rev-parse",
        f"{predecessor_tock}^{{tree}}",
    )
    assert _run_git(target, "log", "-1", "--format=%s", start_sha) == (
        f"START {successor_task_id} {successor_worker['origin_run_id']} "
        f"{successor_worker['promotion_attempt_id']} {predecessor_tock}"
    )

    continuation_path = runtime / "post_tick_continuation.yaml"
    continuation = yaml.safe_load(continuation_path.read_text(encoding="utf-8"))
    assert continuation["status"] == "AUTHORIZED"
    assert continuation["successor_task_id"] == successor_task_id
    assert continuation["expected_continuation_head_sha"] == start_sha
    assert continuation["original_tick_sha"] == start_sha
    assert continuation["commit_chain"] == [
        {"sha": start_sha, "parent_sha": predecessor_tock}
    ]
    assert continuation["origin"]["evidence"] == {
        "predecessor_identity": {
            "task_id": predecessor_journal["task_id"],
            "origin_run_id": predecessor_journal["origin_run_id"],
            "promotion_attempt_id": predecessor_journal["promotion_attempt_id"],
            "result_sha": predecessor_journal["result_sha"],
            "promotion_class": predecessor_journal["promotion_class"],
        },
        "tock_sha": predecessor_tock,
    }
    main_state = yaml.safe_load(
        (runtime / "main_state.yaml").read_text(encoding="utf-8")
    )
    assert (
        main_state["state"],
        main_state["validated_result_sha"],
        main_state["last_tock_sha"],
        main_state["observed_main_head_sha"],
    ) == (
        "GREEN_VALIDATED",
        predecessor_journal["result_sha"],
        predecessor_tock,
        start_sha,
    )
    drain = yaml.safe_load(
        (runtime / "drain_outcome.yaml").read_text(encoding="utf-8")
    )
    assert drain == {
        "schema_version": "1.0",
        "status": "BLOCKED",
        "drain_run_id": "cli-run",
        "observed_main_head_sha": predecessor_tock,
        "reason_code": "UNRESOLVED_PROMOTION_JOURNAL",
    }
    assert not (runtime / "pending_promotion.yaml").exists()
    assert not (runtime / _SETTLED_PREDECESSOR_NAME).exists()
    _bind_external_decisions(runtime, successor_worker)

    def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
        path.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )

    def _rebind_continuation_digest(payload: dict[str, Any]) -> None:
        payload["continuation_digest_sha256"] = binding_digest(
            {
                "schema_version": payload["schema_version"],
                "origin": {
                    "kind": payload["origin"]["kind"],
                    "evidence": {
                        "predecessor_identity": dict(
                            payload["origin"]["evidence"]["predecessor_identity"]
                        ),
                        "tock_sha": payload["origin"]["evidence"]["tock_sha"],
                    },
                },
                "original_tick_sha": payload["original_tick_sha"],
                "expected_continuation_head_sha": (
                    payload["expected_continuation_head_sha"]
                ),
                "commit_chain": [dict(entry) for entry in payload["commit_chain"]],
                "successor_task_id": payload["successor_task_id"],
            }
        )

    def _clone_resume_world(case_name: str) -> tuple[Path, Path, Path]:
        case_root = tmp_path / "resume-cases" / case_name
        case_config_root = case_root / "config-root"
        case_target = case_root / "target"
        case_runtime = case_root / "runtime"
        shutil.copytree(config_path.parent, case_config_root)
        shutil.copytree(target, case_target)
        shutil.copytree(runtime, case_runtime)
        case_config = case_config_root / config_path.name
        _retarget_public_config(case_config, case_target, case_runtime)
        return case_config, case_target, case_runtime

    def _mutate_case(
        case_name: str,
        case_target: Path,
        case_runtime: Path,
    ) -> None:
        journal_path = case_runtime / "promotion_journal.yaml"
        consumption_path = case_runtime / "promotion_consumed.yaml"
        case_continuation_path = case_runtime / "post_tick_continuation.yaml"
        main_state_path = case_runtime / "main_state.yaml"
        drain_path = case_runtime / "drain_outcome.yaml"
        if case_name == "journal_missing":
            journal_path.unlink()
        elif case_name == "journal_foreign":
            payload = yaml.safe_load(journal_path.read_text(encoding="utf-8"))
            payload["task_id"] = "foreign-predecessor"
            _write_yaml(journal_path, payload)
        elif case_name == "journal_non_finalized":
            payload = yaml.safe_load(journal_path.read_text(encoding="utf-8"))
            payload["phase"] = "RECONCILED"
            _write_yaml(journal_path, payload)
        elif case_name == "consumption_missing":
            consumption_path.unlink()
        elif case_name == "consumption_foreign":
            payload = yaml.safe_load(consumption_path.read_text(encoding="utf-8"))
            payload["consumed_promotions"][0]["result_sha"] = "f" * 40
            _write_yaml(consumption_path, payload)
        elif case_name == "continuation_missing":
            case_continuation_path.unlink()
        elif case_name == "continuation_foreign":
            payload = yaml.safe_load(
                case_continuation_path.read_text(encoding="utf-8")
            )
            payload["origin"]["evidence"]["predecessor_identity"]["task_id"] = (
                "foreign-predecessor"
            )
            _rebind_continuation_digest(payload)
            _write_yaml(case_continuation_path, payload)
        elif case_name == "main_state_missing":
            main_state_path.unlink()
        elif case_name == "main_state_foreign":
            payload = yaml.safe_load(main_state_path.read_text(encoding="utf-8"))
            payload["observed_main_head_sha"] = predecessor_tock
            _write_yaml(main_state_path, payload)
        elif case_name == "start_geometry":
            tree = _run_git(case_target, "rev-parse", f"{start_sha}^{{tree}}")
            foreign_start = _git_stdin(
                case_target,
                "commit-tree",
                tree,
                "-p",
                predecessor_tock,
                payload="START foreign geometry\n",
            )
            _run_git(
                case_target,
                "update-ref",
                "refs/heads/main",
                foreign_start,
            )
            continuation_payload = yaml.safe_load(
                case_continuation_path.read_text(encoding="utf-8")
            )
            continuation_payload["original_tick_sha"] = foreign_start
            continuation_payload["expected_continuation_head_sha"] = foreign_start
            continuation_payload["commit_chain"] = [
                {"sha": foreign_start, "parent_sha": predecessor_tock}
            ]
            _rebind_continuation_digest(continuation_payload)
            _write_yaml(case_continuation_path, continuation_payload)
            state_payload = yaml.safe_load(
                main_state_path.read_text(encoding="utf-8")
            )
            state_payload["observed_main_head_sha"] = foreign_start
            _write_yaml(main_state_path, state_payload)
        elif case_name == "drain_missing":
            drain_path.unlink()
        elif case_name == "drain_foreign":
            payload = yaml.safe_load(drain_path.read_text(encoding="utf-8"))
            payload["drain_run_id"] = "foreign-successor-run"
            _write_yaml(drain_path, payload)
        elif case_name == "drain_geometry":
            payload = yaml.safe_load(drain_path.read_text(encoding="utf-8"))
            payload["observed_main_head_sha"] = start_sha
            _write_yaml(drain_path, payload)
        else:
            raise AssertionError(case_name)

    rejection_cases = (
        "journal_missing",
        "journal_foreign",
        "journal_non_finalized",
        "consumption_missing",
        "consumption_foreign",
        "continuation_missing",
        "continuation_foreign",
        "main_state_missing",
        "main_state_foreign",
        "start_geometry",
        "drain_missing",
        "drain_foreign",
        "drain_geometry",
    )
    rejection_pids: list[int] = []
    for case_name in rejection_cases:
        case_config, case_target, case_runtime = _clone_resume_world(case_name)
        _mutate_case(case_name, case_target, case_runtime)
        case_pidfile = case_runtime / "case.pid"
        blocked, blocked_pid = _invoke_real_cli(
            case_config,
            case_pidfile,
            marker,
            source_root=source_root,
        )
        rejection_pids.append(blocked_pid)
        blocked_stdout = _stdout_payload(blocked)
        assert blocked.returncode == 1, (case_name, blocked.stdout, blocked.stderr)
        assert blocked_stdout["outcome"] == "BLOCKED", (
            case_name,
            blocked_stdout,
        )
        assert not (case_runtime / "pending_promotion.yaml").exists(), case_name

    resumed, resumed_pid = _invoke_real_cli(
        config_path,
        pidfile,
        marker,
        source_root=source_root,
    )
    resumed_stdout = _stdout_payload(resumed)
    assert len(set(rejection_pids)) == len(rejection_pids)
    assert resumed_pid not in {
        killed_pid,
        recovered_pid,
        awaiting_pid,
        *rejection_pids,
    }
    expected_resume = (0, "PENDING_PROMOTION", "PROMOTION_INTENT_PERSISTED")
    observed_resume = (
        resumed.returncode,
        resumed_stdout["outcome"],
        resumed_stdout["reason_code"],
    )
    assert observed_resume == expected_resume, (
        "SUCCESSOR_RESUME_START_DRAIN_NOT_REPROVED: "
        f"expected={expected_resume!r}, observed={observed_resume!r}"
    )
    assert (runtime / "pending_promotion.yaml").is_file()
    assert (
        yaml.safe_load(continuation_path.read_text(encoding="utf-8"))["status"]
        == "AUTHORIZED"
    )
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


def test_real_two_process_successor_continuation_and_terminal_rest(tmp_path: Path) -> None:
    from task_transition_runner.config import load_config
    from task_transition_runner.task_source import load_task_source

    _assert_acceptance_plan(_B19_PROCESS_PLAN)
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    marker = _arm_adapter_marker(tmp_path)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []
    _reach_real_pending(
        config_path,
        runtime,
        pidfile,
        marker,
        ledger,
        (_B19_PROCESS_PLAN[0][1], _B19_PROCESS_PLAN[1][1]),
    )
    drained, _drained_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B19_PROCESS_PLAN[2][0],
        step=_B19_PROCESS_PLAN[2][1],
    )
    drained_stdout = _stdout_payload(drained)
    assert drained_stdout["outcome"] == "DRAINED_AWAITING_TASK"
    assert _first_parent_subjects(target).count("TOCK") == 1
    predecessor_tock = _run_git(target, "rev-parse", "refs/heads/main")
    journal = yaml.safe_load((runtime / "promotion_journal.yaml").read_text(encoding="utf-8"))
    assert journal["tock_sha"] == predecessor_tock
    facts = load_task_source(
        load_config(config_path),
        run_root=tmp_path / "predecessor-facts",
    )
    _write_settled_predecessor(
        runtime,
        task_id=str(journal["task_id"]),
        settled=True,
        origin_run_id=str(journal["origin_run_id"]),
        promotion_attempt_id=str(journal["promotion_attempt_id"]),
        task_fingerprint=facts.contract.fingerprint,
        instruction_sources=[
            {
                "logical_ref": item.logical_ref,
                "sha256": item.sha256,
                "byte_length": item.byte_length,
            }
            for item in facts.sources
        ],
    )
    task_path = config_path.parent / "tasks.yaml"
    task_document = yaml.safe_load(task_path.read_text(encoding="utf-8"))
    successor_task_id = "synthetic-task-002"
    task_document["active_task"]["task_id"] = successor_task_id
    task_path.write_text(yaml.safe_dump(task_document, sort_keys=False), encoding="utf-8")

    awaiting, awaiting_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B19_PROCESS_PLAN[3][0],
        step=_B19_PROCESS_PLAN[3][1],
    )
    _assert_spawned_ledger(ledger, _B19_PROCESS_PLAN)
    awaiting_stdout = _stdout_payload(awaiting)
    assert awaiting_stdout["outcome"] == "AWAITING_DECISIONS", awaiting.stdout
    assert awaiting_stdout["reason_code"] == "PROMOTION_DECISION_MISSING"
    assert _marker_count(marker) == 2
    worker_paths = sorted((runtime / "runs").glob("*/worker_result.yaml"))
    worker_payloads = [
        yaml.safe_load(path.read_text(encoding="utf-8")) for path in worker_paths
    ]
    successor_workers = [
        worker for worker in worker_payloads if worker["task_id"] == successor_task_id
    ]
    assert len(worker_paths) == 2
    assert len(successor_workers) == 1
    successor_worker = successor_workers[0]
    assert successor_worker["result_type"] == "worker_result"
    successor_result_subjects = [
        subject
        for subject in _ref_subjects(target)
        if subject.startswith(f"RESULT {successor_task_id} ")
    ]
    assert successor_result_subjects == [
        "RESULT "
        f"{successor_worker['task_id']} "
        f"{successor_worker['origin_run_id']} "
        f"{successor_worker['promotion_attempt_id']}"
    ]
    authorized_bytes = (runtime / "post_tick_continuation.yaml").read_bytes()
    authorized = yaml.safe_load(authorized_bytes)
    assert authorized["status"] == "AUTHORIZED"
    assert authorized["successor_task_id"] == successor_task_id
    assert "successor_origin_run_id" not in authorized
    assert "successor_promotion_attempt_id" not in authorized
    assert _first_parent_subjects(target).count("TOCK") == 1

    _bind_external_decisions(runtime, successor_worker)
    pending, pending_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B19_PROCESS_PLAN[4][0],
        step=_B19_PROCESS_PLAN[4][1],
    )
    pending_stdout = _stdout_payload(pending)
    assert pending_stdout["outcome"] == "PENDING_PROMOTION", pending.stdout
    assert pending_stdout["reason_code"] == "PROMOTION_INTENT_PERSISTED"
    assert (runtime / "post_tick_continuation.yaml").read_bytes() == authorized_bytes
    assert _marker_count(marker) == 2
    assert _first_parent_subjects(target).count("TOCK") == 1

    settled, settled_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B19_PROCESS_PLAN[5][0],
        step=_B19_PROCESS_PLAN[5][1],
    )
    settled_stdout = _stdout_payload(settled)
    assert settled_stdout["outcome"] == "SETTLED", settled.stdout
    assert settled_stdout["reason_code"] == "CONTINUATION_CONSUMED"
    consumed = yaml.safe_load(
        (runtime / "post_tick_continuation.yaml").read_text(encoding="utf-8")
    )
    assert consumed["status"] == "CONSUMED"
    assert consumed["successor_task_id"] == successor_task_id
    assert "successor_origin_run_id" not in consumed
    assert (
        consumed["successor_promotion_attempt_id"]
        == successor_worker["promotion_attempt_id"]
    )
    assert consumed["origin"] == authorized["origin"]
    consumed_bytes = (runtime / "post_tick_continuation.yaml").read_bytes()
    successor_tock = _run_git(target, "rev-parse", "refs/heads/main")
    successor_journal = yaml.safe_load(
        (runtime / "promotion_journal.yaml").read_text(encoding="utf-8")
    )
    assert successor_tock != predecessor_tock
    assert successor_journal["task_id"] == successor_task_id
    assert successor_journal["tock_sha"] == successor_tock
    assert consumed["successor_tock_sha"] == successor_tock
    assert _first_parent_subjects(target).count("TOCK") == 2
    assert _marker_count(marker) == 2
    assert len(
        [
            path
            for path in (runtime / "runs").glob("*/worker_result.yaml")
            if yaml.safe_load(path.read_text(encoding="utf-8"))["task_id"]
            == successor_task_id
        ]
    ) == 1
    assert [
        subject
        for subject in _ref_subjects(target)
        if subject.startswith(f"RESULT {successor_task_id} ")
    ] == successor_result_subjects

    task_document["active_task"] = None
    task_path.write_text(yaml.safe_dump(task_document, sort_keys=False), encoding="utf-8")
    rested, rested_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_B19_PROCESS_PLAN[6][0],
        step=_B19_PROCESS_PLAN[6][1],
    )
    rested_stdout = _stdout_payload(rested)
    _assert_spawned_ledger(ledger, _B19_PROCESS_PLAN)

    assert len(ledger) == len(_B19_PROCESS_PLAN) == 7
    assert len({row["pid"] for row in ledger}) == 7
    assert awaiting_pid != pending_pid != settled_pid != rested_pid
    assert rested_stdout["outcome"] == "TERMINAL_REST"
    assert _first_parent_subjects(target).count("TOCK") == 2
    assert _marker_count(marker) == 2
    assert (runtime / "post_tick_continuation.yaml").read_bytes() == consumed_bytes
    assert not (runtime / "runner_git.lock").exists()
    _assert_frozen_read_only_product_bytes()


def test_a5_real_git_bridge_derives_basis_from_journal_drain_consumption_main_first_parent_tock_and_run_snapshots_without_settled_predecessor_fixture(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_origin_proofs import (
        prove_finalized_promotion_tick,
    )
    from task_transition_runner.runner_drain_transition import (
        DrainStatus,
        load_persisted_drain_outcome,
    )
    from task_transition_runner.runner_promotion_journal import (
        PromotionJournalPhase,
        load_promotion_journal,
    )
    from task_transition_runner.runner_promotion_state import (
        load_and_validate_main_state,
        load_promotion_consumed,
    )
    from task_transition_runner.task_source import reprove_run_local_snapshot

    test_node_id = (
        "tests/test_cli_run.py::"
        "test_a5_real_git_bridge_derives_basis_from_journal_drain_consumption_"
        "main_first_parent_tock_and_run_snapshots_without_settled_predecessor_fixture"
    )
    monkeypatch.setattr(
        cli_module,
        "verify_code_authority",
        lambda _config: {"ok": True, "reason_code": "SOURCE_CHECKED"},
    )
    world = _qualify_task_end_pending(tmp_path, capsys, monkeypatch)
    drained = _resume_after_real_pending(world, capsys)
    assert drained["outcome"] == "DRAINED_AWAITING_TASK", drained
    runtime = world["runtime"]
    target = world["target"]
    config_path = world["config_path"]
    config = load_config(config_path)
    observed_reads: list[str] = []
    read_roots = (
        ("RUNTIME_ROOT", runtime.resolve()),
        ("INPUT_ROOT", config_path.parent.resolve()),
        ("TARGET_ROOT", target.resolve()),
    )
    original_path_open = Path.open

    def _observe_path_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        mode = args[0] if args else kwargs.get("mode", "r")
        if "r" in str(mode):
            resolved = path.resolve()
            for root_alias, root in read_roots:
                if resolved == root or resolved.is_relative_to(root):
                    relative = resolved.relative_to(root).as_posix()
                    observed_reads.append(f"{root_alias}/{relative}")
                    break
            else:
                raise AssertionError(f"unmapped observed read: {resolved.name}")
        return original_path_open(path, *args, **kwargs)

    basis_calls: list[dict[str, Any]] = []
    observed_activation = cli_module.evaluate_task_activation

    def _capture_basis(
        facts: Any,
        *,
        predecessor: dict[str, Any],
    ) -> Any:
        basis_calls.append(predecessor)
        return observed_activation(facts, predecessor=predecessor)

    monkeypatch.setattr(
        cli_module,
        "evaluate_task_activation",
        _capture_basis,
    )
    with monkeypatch.context() as read_observer:
        read_observer.setattr(Path, "open", _observe_path_open)
        journal = load_promotion_journal(path=runtime / "promotion_journal.yaml")
        assert journal.phase is PromotionJournalPhase.FINALIZED
        drain = load_persisted_drain_outcome(runtime_root=runtime)
        assert drain is not None
        assert drain.status is DrainStatus.DRAINED
        assert drain.reason_code == "PROMOTION_EFFECT_ADMITTED"
        assert (
            drain.task_id,
            drain.origin_run_id,
            drain.promotion_attempt_id,
            drain.result_sha,
            drain.promotion_class,
            drain.tock_sha,
        ) == (
            journal.task_id,
            journal.origin_run_id,
            journal.promotion_attempt_id,
            journal.result_sha,
            journal.promotion_class,
            journal.tock_sha,
        )
        consumption = load_promotion_consumed(
            promotion_consumed_path=runtime / "promotion_consumed.yaml"
        )
        matching_consumption = [
            entry
            for entry in consumption["consumed_promotions"]
            if entry["promotion_attempt_id"] == journal.promotion_attempt_id
        ]
        assert len(matching_consumption) == 1
        assert (
            matching_consumption[0]["result_sha"],
            matching_consumption[0]["tock_sha"],
        ) == (journal.result_sha, journal.tock_sha)
        main_state = load_and_validate_main_state(
            main_state_path=runtime / "main_state.yaml"
        )
        assert (
            main_state["state"],
            main_state["validated_result_sha"],
            main_state["last_tock_sha"],
            main_state["observed_main_head_sha"],
        ) == (
            "GREEN_VALIDATED",
            journal.result_sha,
            journal.tock_sha,
            journal.tock_sha,
        )
        first_parent_chain = _run_git(
            target,
            "rev-list",
            "--first-parent",
            config.main_ref,
        ).splitlines()
        matching_tocks = [
            sha
            for sha in first_parent_chain
            if _run_git(target, "log", "-1", "--format=%s", sha) == "TOCK"
            and (
                f"Promotion-Attempt: {journal.promotion_attempt_id}"
                in _run_git(target, "cat-file", "-p", sha)
            )
        ]
        assert matching_tocks == [journal.tock_sha]
        origin = prove_finalized_promotion_tick(
            repo=target,
            runtime=runtime,
            main_ref=config.main_ref,
        )
        assert origin.parent_sha == journal.tock_sha
        assert origin.to_mapping() == {
            "predecessor_identity": {
                "task_id": journal.task_id,
                "origin_run_id": journal.origin_run_id,
                "promotion_attempt_id": journal.promotion_attempt_id,
                "result_sha": journal.result_sha,
                "promotion_class": journal.promotion_class,
            },
            "tock_sha": journal.tock_sha,
        }
        run_root = runtime / "runs" / journal.origin_run_id
        reproved = reprove_run_local_snapshot(
            run_root=run_root,
            expected_task_fingerprint=journal.task_fingerprint,
            expected_task_snapshot_sha256=journal.task_snapshot_sha256,
            expected_instruction_signature_sha256=(
                journal.instruction_signature_sha256
            ),
        )
        expected_basis = {
            "kind": "SETTLED_PROMOTION",
            "predecessor": {
                "settled": True,
                "task_id": journal.task_id,
                "origin_run_id": journal.origin_run_id,
                "promotion_attempt_id": journal.promotion_attempt_id,
                "task_fingerprint": reproved.task_fingerprint,
                "instruction_sources": [
                    {
                        "logical_ref": source.logical_ref,
                        "sha256": source.sha256,
                        "byte_length": source.byte_length,
                    }
                    for source in reproved.instruction_sources
                ],
            },
        }
        task_path = config_path.parent / "tasks.yaml"
        successor = yaml.safe_load(task_path.read_text(encoding="utf-8"))
        successor["active_task"]["task_id"] = "synthetic-task-002"
        task_path.write_text(
            yaml.safe_dump(successor, sort_keys=False),
            encoding="utf-8",
        )
        before_snapshot_bytes = {
            path.relative_to(runtime).as_posix(): path.read_bytes()
            for path in run_root.rglob("*")
            if path.is_file()
        }

        exit_code = main(["run", "--config", str(config_path)])
        stdout = _read_stdout_yaml(capsys.readouterr().out)

        assert exit_code == 1
        assert stdout["outcome"] == "AWAITING_DECISIONS"
        assert basis_calls == [expected_basis]
        assert not (runtime / _SETTLED_PREDECESSOR_NAME).exists()
        assert {
            path.relative_to(runtime).as_posix(): path.read_bytes()
            for path in run_root.rglob("*")
            if path.is_file()
        } == before_snapshot_bytes
        assert cli_module._RUN_LOCK == {}

    required_reads = {
        "promotion_journal.yaml",
        "drain_outcome.yaml",
        "promotion_consumed.yaml",
        "main_state.yaml",
        "task_snapshot.yaml",
        "instruction_snapshot.yaml",
    }
    assert required_reads <= {Path(path).name for path in observed_reads}
    assert all(Path(path).name != _SETTLED_PREDECESSOR_NAME for path in observed_reads)
    assert all(
        path.startswith(("RUNTIME_ROOT/", "INPUT_ROOT/", "TARGET_ROOT/"))
        for path in observed_reads
    )

    evidence_root_value = os.environ.get("A5_EVIDENCE_ROOT")
    if evidence_root_value is None:
        return
    evidence_root = Path(evidence_root_value).resolve()
    assert not evidence_root.exists()
    canonical_observed_reads = json.dumps(
        observed_reads,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    read_trace = {
        "test_node_id": test_node_id,
        "observed_reads": observed_reads,
        "sha256": hashlib.sha256(canonical_observed_reads).hexdigest(),
    }
    read_trace_bytes = (
        json.dumps(
            read_trace,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")

    candidate_root = Path(os.environ["CANDIDATE_ROOT"]).resolve()
    candidate_commit = os.environ["CANDIDATE_BASE_COMMIT"]
    candidate_tree = os.environ["CANDIDATE_BASE_TREE"]
    assert Path.cwd().resolve() == candidate_root
    assert _run_git(candidate_root, "rev-parse", "HEAD") == candidate_commit
    assert _run_git(candidate_root, "rev-parse", "HEAD^{tree}") == candidate_tree
    validation_root = Path(os.environ["A5_RUNNER_VALIDATION_ROOT"]).resolve()
    validation_relative = Path(os.environ["A5_RUNNER_VALIDATION_LOG"])
    assert not validation_relative.is_absolute()
    assert ".." not in validation_relative.parts
    validation_path = (validation_root / validation_relative).resolve()
    assert validation_path.is_relative_to(validation_root)
    validation_bytes = validation_path.read_bytes()
    validation_digest = hashlib.sha256(validation_bytes).hexdigest()
    assert validation_digest == os.environ["A5_RUNNER_VALIDATION_LOG_SHA256"]
    receipt = {
        "kind": "OSS7_A5_BRIDGE_AUXILIARY_OWNER_RECEIPT",
        "producer_task_id": "PUBLIC_A5_FIXTURE_FREE_BRIDGE_ACCEPTANCE",
        "acceptance_owner_id": (
            "OSS7-ACC-BRIDGE-01_CANONICAL_SETTLED_BASIS_GREEN"
        ),
        "candidate_commit": candidate_commit,
        "candidate_tree": candidate_tree,
        "executed_test_node_ids": [test_node_id],
        "outcome": "PASS",
        "validation_evidence": [
            {
                "command_id": "tests",
                "root_alias": "RUNNER_VALIDATION_ROOT",
                "relative_path": validation_relative.as_posix(),
                "sha256": validation_digest,
                "exit_code": 0,
            }
        ],
        "facts": {
            "fixture_used": False,
            "journal_bound": True,
            "drain_bound": True,
            "consumption_bound": True,
            "main_state_bound": True,
            "first_parent_tock_count": len(matching_tocks),
            "run_snapshots_bound": True,
            "file_read_trace_sha256": hashlib.sha256(
                read_trace_bytes
            ).hexdigest(),
        },
    }
    evidence_root.mkdir()
    read_trace_path = evidence_root / "a5_bridge_file_reads.json"
    read_trace_path.write_bytes(read_trace_bytes)
    receipt_path = evidence_root / "a5_bridge_receipt.json"
    receipt_path.write_text(
        json.dumps(
            receipt,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    assert sorted(path.name for path in evidence_root.iterdir()) == [
        "a5_bridge_file_reads.json",
        "a5_bridge_receipt.json",
    ]


def test_run_has_no_settled_predecessor_file_fallback_or_authority() -> None:
    import task_transition_runner.cli as cli_module

    source = Path(cli_module.__file__).read_text(encoding="utf-8")

    assert _SETTLED_PREDECESSOR_NAME not in source


def test_worker_self_report_never_creates_pending_journal_tock_consumption_or_settlement(
    tmp_path: Path,
) -> None:
    """A successful worker self-report never admits promotion effect."""

    _assert_frozen_read_only_product_bytes()
    source_root = _detached_head_source(tmp_path / "clean-source")
    try:
        for case_name in ("missing", "draft", "invalid", "identity_mismatch"):
            _assert_cli_worker_self_report_has_no_promotion_effect(
                tmp_path / case_name,
                source_root,
                case_name,
            )
    finally:
        _remove_detached_head_source(source_root)
    _assert_frozen_read_only_product_bytes()


def _assert_cli_worker_self_report_has_no_promotion_effect(
    tmp_path: Path,
    source_root: Path,
    case_name: str,
) -> None:
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    main_before = _run_git(target, "rev-parse", "refs/heads/main")
    first = _run_real_cli(source_root, config_path)
    worker_path = _single_persisted_worker_result(runtime)
    worker = yaml.safe_load(worker_path.read_text(encoding="utf-8"))
    assert worker["worker_report"]["reported_status"] == "success"
    assert first["outcome"] == "AWAITING_DECISIONS"
    assert first["reason_code"] == "PROMOTION_DECISION_MISSING"
    assert first["outcome"] != "PENDING_PROMOTION"
    _assert_cli_promotion_effect_absent(target, runtime, baseline_main=main_before)
    if case_name == "missing":
        return
    main_after_worker = _run_git(target, "rev-parse", "refs/heads/main")
    _write_incomplete_cli_decisions(runtime, worker, case_name)
    _set_baseline_timeout(config_path, 1)
    second = _run_real_cli(source_root, config_path)
    replayed = yaml.safe_load(worker_path.read_text(encoding="utf-8"))
    assert replayed["worker_report"]["reported_status"] == "success"
    assert second["ok"] is False
    assert second["outcome"] in {"AWAITING_DECISIONS", "BLOCKED"}
    assert second["outcome"] not in {
        "PENDING_PROMOTION",
        "DRAINED_AWAITING_TASK",
        "SETTLED",
    }
    assert second["reason_code"] == _cli_incomplete_decision_reason(case_name)
    assert _run_git(target, "rev-parse", "refs/heads/main") == main_after_worker
    _assert_cli_promotion_effect_absent(target, runtime, baseline_main=main_before)


def _detached_head_source(source_root: Path) -> Path:
    """Run the real CLI from a clean checkout of this commit.

    Uncommitted test bytes make the editing checkout source-dirty. Source
    authority stays real; it is not stubbed.
    """

    candidate = Path(__file__).resolve().parents[1]
    completed = subprocess.run(  # noqa: S603
        [
            "git",
            "-C",
            str(candidate),
            "worktree",
            "add",
            "--detach",
            str(source_root),
            "HEAD",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert _run_git(source_root, "status", "--porcelain") == ""
    assert _run_git(source_root, "rev-parse", "HEAD") == _run_git(
        candidate,
        "rev-parse",
        "HEAD",
    )
    return source_root


def _remove_detached_head_source(source_root: Path) -> None:
    candidate = Path(__file__).resolve().parents[1]
    subprocess.run(  # noqa: S603
        [
            "git",
            "-C",
            str(candidate),
            "worktree",
            "remove",
            "--force",
            str(source_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _run_real_cli(source_root: Path, config_path: Path) -> dict[str, Any]:
    env = os.environ.copy()
    env.pop("CURSOR_API_ENDPOINT", None)
    previous = env.get("PYTHONPATH", "")
    source_path = str((source_root / "src").resolve())
    env["PYTHONPATH"] = source_path if previous == "" else source_path + os.pathsep + previous
    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            "import sys\n"
            "from task_transition_runner.cli import main\n"
            "raise SystemExit(main(sys.argv[1:]))\n",
            "run",
            "--config",
            str(config_path),
        ],
        cwd=source_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode in {0, 1}, completed.stderr
    assert completed.stderr == "", completed.stderr
    return _read_stdout_yaml(completed.stdout)


def _set_baseline_timeout(config_path: Path, timeout_seconds: int) -> None:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["baseline_validation"]["timeout_seconds"] = timeout_seconds
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _single_persisted_worker_result(runtime: Path) -> Path:
    matches = sorted((runtime / "runs").glob("*/worker_result.yaml"))
    assert len(matches) == 1
    return matches[0]


def _write_incomplete_cli_decisions(
    runtime: Path,
    worker: dict[str, Any],
    case_name: str,
) -> None:
    identity = SimpleNamespace(
        task_id=worker["task_id"],
        origin_run_id=worker["origin_run_id"],
        promotion_attempt_id=worker["promotion_attempt_id"],
    )
    result_sha = worker["result_sha"]
    decisions = runtime / "runs" / identity.origin_run_id / "decisions"
    decisions.mkdir(parents=True, exist_ok=True)
    if case_name == "invalid":
        (decisions / "orchestrator_decision.yaml").write_text(
            "{not-yaml",
            encoding="utf-8",
        )
        validator = _canonical_decision_document(
            identity,
            result_sha,
            decision_type="validator",
        )
        (decisions / "validator_decision.yaml").write_text(
            yaml.safe_dump(validator, sort_keys=False),
            encoding="utf-8",
        )
        return
    orchestrator_sha = result_sha
    if case_name == "identity_mismatch":
        orchestrator_sha = "b" * 40
    orchestrator = _canonical_decision_document(
        identity,
        orchestrator_sha,
        decision_type="orchestrator",
    )
    validator = _canonical_decision_document(
        identity,
        result_sha,
        decision_type="validator",
    )
    if case_name == "draft":
        orchestrator["ready"] = False
        validator["ready"] = False
    (decisions / "orchestrator_decision.yaml").write_text(
        yaml.safe_dump(orchestrator, sort_keys=False),
        encoding="utf-8",
    )
    (decisions / "validator_decision.yaml").write_text(
        yaml.safe_dump(validator, sort_keys=False),
        encoding="utf-8",
    )


def _cli_incomplete_decision_reason(case_name: str) -> str:
    if case_name == "draft":
        return "PROMOTION_DECISION_NOT_READY"
    return "PROMOTION_DECISION_INVALID"


def _assert_cli_promotion_effect_absent(
    target: Path,
    runtime: Path,
    *,
    baseline_main: str,
) -> None:
    main_now = _run_git(target, "rev-parse", "refs/heads/main")
    assert _run_git(target, "rev-parse", "refs/heads/main^") == baseline_main
    assert _run_git(target, "log", "-1", "--format=%s", "refs/heads/main").startswith(
        "START "
    )
    subjects = _run_git(target, "log", "--all", "--format=%s").splitlines()
    assert all(not subject.startswith("TOCK") for subject in subjects)
    assert not (runtime / "pending_promotion.yaml").exists()
    assert not (runtime / "promotion_journal.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    main_state = yaml.safe_load((runtime / "main_state.yaml").read_text(encoding="utf-8"))
    assert main_state["state"] == "GREEN_VALIDATED"
    assert main_state["last_tock_sha"] == baseline_main
    assert main_state["validated_result_sha"] == baseline_main
    assert main_state["observed_main_head_sha"] == main_now
    drain_path = runtime / "drain_outcome.yaml"
    if drain_path.is_file():
        drain = yaml.safe_load(drain_path.read_text(encoding="utf-8"))
        assert drain.get("reason_code") != "PROMOTION_EFFECT_ADMITTED"
    assert not (runtime / "post_tick_continuation.yaml").exists()


def test_bound_predecessor_basis_rejects_unregistered_object(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    registered = {
        "kind": "SETTLED_PROMOTION",
        "predecessor": {
            "settled": True,
            "task_id": "registered-predecessor-task",
        },
    }
    unregistered = {
        "kind": "SETTLED_PROMOTION",
        "predecessor": {
            "settled": True,
            "task_id": "registered-predecessor-task",
        },
    }
    assert unregistered == registered
    assert unregistered is not registered
    cli_module._RUN_LOCK.clear()
    cli_module._RUN_LOCK["predecessor_basis"] = registered
    try:
        blocked = cli_module.bind_ordinary_continuation(
            config,
            predecessor_basis=unregistered,
        )
        assert cli_module._RUN_LOCK.get("predecessor_basis") is registered
    finally:
        cli_module._RUN_LOCK.clear()

    assert blocked == {
        "ok": False,
        "status": "fail-closed",
        "reason_code": "ACTIVATION_PREDECESSOR_UNAVAILABLE",
    }
    _assert_frozen_read_only_product_bytes()


def test_fresh_tail_canonical_basis_fallback_reaches_settled_consumer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    class _CanonicalBasisHandoffSentinel(Exception):
        """Local stop after the canonical basis reaches the settled consumer."""

    class _UntouchedActivation:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"product path touched activation.{name}")

    _assert_frozen_read_only_product_bytes()
    config_path, _target, _runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    canonical_basis = {
        "kind": "SETTLED_PROMOTION",
        "predecessor": {
            "settled": True,
            "task_id": "canonical-fallback-task",
            "origin_run_id": "canonical-fallback-origin",
            "promotion_attempt_id": "canonical-fallback-attempt",
            "task_fingerprint": "canonical-fallback-fingerprint",
            "instruction_sources": [],
        },
    }
    producer_calls: list[object] = []
    handoff: list[tuple[object, object]] = []
    allow_canonical_producer = True
    real_settled_consumer = cli_module._settled_predecessor_from_basis
    lock_basis = {
        "kind": "SETTLED_PROMOTION",
        "predecessor": {
            "settled": True,
            "task_id": "lock-bound-predecessor-task",
        },
    }

    def _produce_canonical_basis(
        observed_config: object,
        *,
        reconciled_identity: object = None,
        predecessor_only_transition: bool = False,
    ) -> dict[str, Any]:
        assert allow_canonical_producer is True
        assert observed_config is config
        assert reconciled_identity is None
        assert predecessor_only_transition is False
        producer_calls.append(observed_config)
        return canonical_basis

    def _observe_settled_consumer(basis: object) -> dict[str, Any] | None:
        settled = real_settled_consumer(basis)
        handoff.append((basis, settled))
        raise _CanonicalBasisHandoffSentinel()

    cli_module._RUN_LOCK.clear()
    assert not isinstance(cli_module._RUN_LOCK.get("predecessor_basis"), dict)
    monkeypatch.setattr(
        cli_module,
        "_canonical_settled_predecessor_basis",
        _produce_canonical_basis,
    )
    monkeypatch.setattr(
        cli_module,
        "_settled_predecessor_from_basis",
        _observe_settled_consumer,
    )
    try:
        with pytest.raises(_CanonicalBasisHandoffSentinel):
            cli_module._fresh_active_tail(config, _UntouchedActivation())
        assert producer_calls == [config]
        assert handoff == [(canonical_basis, canonical_basis["predecessor"])]
        assert handoff[0][0] is canonical_basis

        allow_canonical_producer = False
        cli_module._RUN_LOCK["predecessor_basis"] = lock_basis
        with pytest.raises(_CanonicalBasisHandoffSentinel):
            cli_module._fresh_active_tail(config, _UntouchedActivation())
    finally:
        cli_module._RUN_LOCK.clear()

    assert producer_calls == [config]
    assert handoff == [
        (canonical_basis, canonical_basis["predecessor"]),
        (lock_basis, lock_basis["predecessor"]),
    ]
    assert handoff[0][0] is canonical_basis
    assert handoff[1][0] is lock_basis
    assert cli_module._RUN_LOCK == {}
    _assert_frozen_read_only_product_bytes()


def test_canonical_cli_coverage_reconciliation_completed_entry_phase(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_promotion_journal import (
        PromotionJournal,
        PromotionJournalPhase,
        load_promotion_journal,
    )

    _assert_frozen_read_only_product_bytes()
    module_path = Path(cli_module.__file__).resolve()
    assert module_path.name == "cli.py"
    assert "tmp" not in module_path.parts
    assert "pytest-of-gubel" not in module_path.parts
    config_path, _target, runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    anchor = "a" * 64
    entry_journal = PromotionJournal(
        phase=PromotionJournalPhase.APPLYING,
        task_id="coverage-entry-task",
        origin_run_id="coverage-entry-origin",
        drain_run_id="coverage-entry-drain",
        promotion_attempt_id="coverage-entry-attempt",
        promotion_class="GREEN_PROMOTION",
        base_main_sha=anchor,
        result_sha=anchor,
        validated_result_sha=anchor,
        task_fingerprint=anchor,
        task_snapshot_sha256=anchor,
        instruction_signature_sha256=anchor,
        reason_code="PROMOTION_APPLYING",
        pending_at_utc="2026-10-18T12:00:00Z",
        applying_at_utc="2026-10-18T12:00:01Z",
    )
    journal_path = runtime / "promotion_journal.yaml"
    journal_path.write_text(
        yaml.safe_dump(entry_journal.to_mapping(), sort_keys=False),
        encoding="utf-8",
    )
    loaded_entry = load_promotion_journal(path=journal_path)
    assert loaded_entry.phase is PromotionJournalPhase.APPLYING
    assert loaded_entry.phase in {
        PromotionJournalPhase.APPLYING,
        PromotionJournalPhase.GIT_APPLIED,
        PromotionJournalPhase.RECONCILED,
    }
    assert cli_module._entry_promotion_journal_phase(config) is loaded_entry.phase
    assert not (runtime / "post_tick_continuation.yaml").is_file()
    assert not (runtime / "main_state.yaml").is_file()
    ownership = dict(_LOCK_OWNERSHIP)
    releases: list[dict[str, Any]] = []
    reconcile_reasons: list[str] = []
    cli_module._RUN_LOCK.clear()
    monkeypatch.setattr(
        cli_module,
        "verify_code_authority",
        lambda _config: {"ok": True, "reason_code": "SOURCE_CHECKED"},
    )
    monkeypatch.setattr(
        cli_module.git_context_module,
        "evaluate_branch_preflight",
        lambda **_kwargs: {"ok": True, "reason_code": "GIT_PREFLIGHT_OK"},
    )
    monkeypatch.setattr(
        cli_module,
        "acquire_runner_git_lock",
        lambda **_kwargs: {"ok": True, "ownership": ownership},
    )
    monkeypatch.setattr(
        cli_module,
        "release_runner_git_lock",
        lambda **kwargs: releases.append(kwargs["ownership"]),
    )

    def _completed_reconciliation(_config: object) -> dict[str, Any]:
        reconcile_reasons.append("RECONCILIATION_COMPLETED")
        return {
            "ok": True,
            "reason_code": "RECONCILIATION_COMPLETED",
            "main_state": {},
        }

    monkeypatch.setattr(
        cli_module,
        "reconcile_ordinary_promotion",
        _completed_reconciliation,
    )

    def _forbid_later_seam(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("early RECOVERED return was not taken")

    monkeypatch.setattr(cli_module, "evaluate_drain_before_dispatch", _forbid_later_seam)
    monkeypatch.setattr(cli_module, "evaluate_task_activation", _forbid_later_seam)

    exit_code = cli_module._command_run(config)
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    persisted_entry = load_promotion_journal(path=journal_path)

    assert reconcile_reasons == ["RECONCILIATION_COMPLETED"]
    assert exit_code == 0
    assert stdout == {
        "ok": True,
        "command": "run",
        "outcome": "RECOVERED",
        "status": "projected",
        "reason_code": "RECONCILIATION_COMPLETED",
    }
    assert persisted_entry.phase is PromotionJournalPhase.APPLYING
    assert releases == [ownership]
    assert cli_module._RUN_LOCK == {}
    assert not (runtime / "main_state.yaml").is_file()
    _assert_frozen_read_only_product_bytes()


def test_canonical_cli_coverage_pre_start_drain_predicate() -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_drain_transition import DrainOutcome, DrainStatus
    from task_transition_runner.runner_promotion_journal import (
        PromotionJournal,
        PromotionJournalPhase,
    )

    _assert_frozen_read_only_product_bytes()
    module_path = Path(cli_module.__file__).resolve()
    assert module_path.name == "cli.py"
    assert "tmp" not in module_path.parts
    assert "pytest-of-gubel" not in module_path.parts
    anchor = "ab" * 32
    tock_sha = "cd" * 32
    journal = PromotionJournal(
        phase=PromotionJournalPhase.FINALIZED,
        task_id="coverage-successor-task",
        origin_run_id="coverage-successor-origin",
        drain_run_id="coverage-successor-drain",
        promotion_attempt_id="coverage-successor-attempt",
        promotion_class="GREEN_PROMOTION",
        base_main_sha=anchor,
        result_sha=anchor,
        validated_result_sha=anchor,
        task_fingerprint=anchor,
        task_snapshot_sha256=anchor,
        instruction_signature_sha256=anchor,
        reason_code="RECONCILIATION_COMPLETED",
        pending_at_utc="2026-10-18T12:00:00Z",
        applying_at_utc="2026-10-18T12:00:01Z",
        git_applied_at_utc="2026-10-18T12:00:02Z",
        reconciled_at_utc="2026-10-18T12:00:03Z",
        finalized_at_utc="2026-10-18T12:00:04Z",
        tock_sha=tock_sha,
    )
    matching_drain = DrainOutcome(
        status=DrainStatus.BLOCKED,
        drain_run_id=cli_module._CLI_RUN_ID,
        observed_main_head_sha=tock_sha,
        reason_code="UNRESOLVED_PROMOTION_JOURNAL",
    )
    mismatched_drain = DrainOutcome(
        status=DrainStatus.BLOCKED,
        drain_run_id="not-the-cli-run",
        observed_main_head_sha=tock_sha,
        reason_code="UNRESOLVED_PROMOTION_JOURNAL",
    )
    transition = {"tock_sha": tock_sha}
    predicate = cli_module._authorized_successor_start_allows_pre_start_drain

    assert predicate(journal=journal, drain=matching_drain, transition=None) is False
    assert predicate(journal=journal, drain=None, transition=transition) is False
    assert (
        predicate(journal=journal, drain=mismatched_drain, transition=transition)
        is False
    )
    assert predicate(journal=journal, drain=matching_drain, transition=transition) is True
    _assert_frozen_read_only_product_bytes()


_OSS36C_ALLOWED_PATH = "src/example.py"
_OSS36C_FOREIGN_PATH = "foreign-user.txt"
_OSS36C_SEEDED_BYTES = b"OSS36C-SEEDED-ALLOWED-BYTES\n"
_OSS36C_DEVELOPER_SUBJECT = "Expose the authorized product file."
_OSS36C_PRODUCT_BYTES = {
    "staged": b"OSS36C-STAGED-ALLOWED-BYTES\n",
    "unstaged": b"OSS36C-UNSTAGED-ALLOWED-BYTES\n",
    "untracked": b"OSS36C-UNTRACKED-ALLOWED-BYTES\n",
    "developer_commit": b"OSS36C-DEVELOPER-COMMIT-BYTES\n",
    "mixed_foreign": b"OSS36C-MIXED-ALLOWED-BYTES\n",
}
_OSS36C_FOREIGN_BYTES = b"OSS36C-FOREIGN-PATH-BYTES\n"
_OSS36C_MISSING_SIGNATURE = {
    "staged": "ALLOWED_STAGED_BYTES_MISSING_FROM_RESULT",
    "unstaged": "ALLOWED_UNSTAGED_BYTES_MISSING_FROM_RESULT",
    "untracked": "ALLOWED_UNTRACKED_BYTES_MISSING_FROM_RESULT",
}
_OSS36C_ADAPTER_PROGRAM = r"""
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def _git(workspace, *args):
    completed = subprocess.run(
        ["git", *args],
        cwd=workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(detail)
    return completed.stdout.strip()


def _workspace():
    prefix = "--workspace="
    for argument in sys.argv[1:-1]:
        if argument.startswith(prefix):
            return argument[len(prefix):]
    return ""


def _write(workspace, relative, payload):
    path = Path(workspace) / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _commit_objects(workspace):
    completed = subprocess.run(
        ["git", "cat-file", "--batch-check", "--batch-all-objects"],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
    )
    found = []
    for line in completed.stdout.splitlines():
        oid, kind, _size = line.split()
        if kind == "commit":
            found.append(oid)
    return found


def _mutate(workspace, instruction):
    mode = instruction["mode"]
    allowed_path = instruction["allowed_path"]
    allowed_payload = instruction["allowed_text"].encode("utf-8")
    foreign_path = instruction["foreign_path"]
    foreign_payload = instruction["foreign_text"].encode("utf-8")
    if mode == "capture_frame":
        return
    if mode == "staged":
        _write(workspace, allowed_path, allowed_payload)
        _git(workspace, "add", "--", allowed_path)
        return
    if mode == "unstaged":
        _write(workspace, allowed_path, allowed_payload)
        return
    if mode == "untracked":
        _write(workspace, allowed_path, allowed_payload)
        return
    if mode == "developer_commit":
        _write(workspace, allowed_path, allowed_payload)
        _git(workspace, "add", "--", allowed_path)
        _git(workspace, "commit", "-m", instruction["commit_subject"])
        return
    if mode == "mixed_foreign":
        _write(workspace, allowed_path, allowed_payload)
        _write(workspace, foreign_path, foreign_payload)
        return
    raise RuntimeError("unknown adapter mode")


def _emit(frame, workspace):
    session_id = "synthetic-cli-real-component-session"
    records = (
        {
            "type": "system",
            "subtype": "init",
            "cwd": workspace,
            "session_id": session_id,
        },
        {
            "type": "user",
            "message": {"content": [{"type": "text", "text": frame}]},
            "session_id": session_id,
        },
        {
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "synthetic-cli-assistant"}]
            },
            "session_id": session_id,
        },
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "synthetic-cli-result",
            "session_id": session_id,
        },
    )
    payload = b"".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        + b"\n"
        for record in records
    )
    os.write(1, payload)


def main():
    frame = sys.argv[-1]
    workspace = _workspace()
    instruction = json.loads(Path(os.environ["OSS36C_INSTRUCTION"]).read_text(encoding="utf-8"))
    mutation_error = ""
    try:
        _mutate(workspace, instruction)
    except Exception as exc:
        mutation_error = str(exc)
    index = Path(workspace) / ".git" / "index"
    branch = _git(workspace, "rev-parse", "--abbrev-ref", "HEAD")
    head = _git(workspace, "rev-parse", "HEAD")
    evidence = {
        "mutation_error": mutation_error,
        "prompt_frame": frame,
        "head": head,
        "branch": branch,
        "branch_sha": _git(workspace, "rev-parse", "refs/heads/" + branch),
        "index_sha256": hashlib.sha256(index.read_bytes()).hexdigest(),
        "commit_objects": _commit_objects(workspace),
    }
    Path(instruction["evidence"]).write_text(
        json.dumps(evidence),
        encoding="utf-8",
    )
    _emit(frame, workspace)


if __name__ == "__main__":
    main()
"""


def _oss36c_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _oss36c_binding(payload: bytes) -> tuple[str, int]:
    return _oss36c_sha256(payload), len(payload)


def _oss36c_commit_objects(repo: Path) -> set[str]:
    completed = subprocess.run(  # noqa: S603
        ["git", "cat-file", "--batch-check", "--batch-all-objects"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    found: set[str] = set()
    for line in completed.stdout.splitlines():
        oid, kind, _size = line.split()
        if kind == "commit":
            found.add(oid)
    return found


def _oss36c_subject(repo: Path, sha: str) -> str:
    return _run_git(repo, "log", "-1", "--format=%s", sha)


def _oss36c_parents(repo: Path, sha: str) -> tuple[str, ...]:
    raw = _run_git(repo, "rev-list", "--parents", "-n", "1", sha)
    return tuple(raw.split()[1:])


def _oss36c_index_sha256(repo: Path) -> str:
    return _oss36c_sha256((repo / ".git" / "index").read_bytes())


def _oss36c_blob(repo: Path, sha: str, relative: str) -> bytes | None:
    spec = f"{sha}:{relative}"
    probe = subprocess.run(  # noqa: S603
        ["git", "cat-file", "-e", spec],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    if probe.returncode != 0:
        return None
    completed = subprocess.run(  # noqa: S603
        ["git", "cat-file", "blob", spec],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    return completed.stdout


def _oss36c_commits_with_prefix(repo: Path, prefix: str) -> tuple[str, ...]:
    return tuple(
        sha
        for sha in sorted(_oss36c_commit_objects(repo))
        if _oss36c_subject(repo, sha).startswith(prefix)
    )


def _oss36c_runtime_effects(runtime: Path) -> tuple[str, ...]:
    runs = runtime / "runs"
    if not runs.is_dir():
        return ()
    names = ("worker_result.yaml", "decisions/decision_request.yaml")
    found: list[str] = []
    for path in sorted(runs.glob("*/*")):
        relative = path.relative_to(runs).as_posix()
        if relative.endswith(names) or relative.endswith("decisions/decision_request.yaml"):
            found.append(relative)
    for path in sorted(runs.glob("*/decisions/*")):
        found.append(path.relative_to(runs).as_posix())
    return tuple(found)


def _oss36c_install_adapter(tmp_path: Path) -> None:
    executable = tmp_path / "synthetic-cursor-adapter"
    executable.write_text(
        f"#!{sys.executable}\n{_OSS36C_ADAPTER_PROGRAM}",
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)


def _oss36c_instruction(
    tmp_path: Path,
    *,
    mode: str,
    allowed_payload: bytes,
) -> Path:
    evidence = tmp_path / "oss36c-evidence.json"
    instruction = tmp_path / "oss36c-instruction.json"
    instruction.write_text(
        json.dumps(
            {
                "mode": mode,
                "allowed_path": _OSS36C_ALLOWED_PATH,
                "allowed_text": allowed_payload.decode("utf-8"),
                "foreign_path": _OSS36C_FOREIGN_PATH,
                "foreign_text": _OSS36C_FOREIGN_BYTES.decode("utf-8"),
                "commit_subject": _OSS36C_DEVELOPER_SUBJECT,
                "evidence": str(evidence),
            }
        ),
        encoding="utf-8",
    )
    return instruction


def _oss36c_live_bytes(config_path: Path) -> dict[str, bytes]:
    root = config_path.parent
    return {
        "developer": (root / "prompts" / "developer.md").read_bytes(),
        "markdown_spec": (root / "tasks" / "synthetic-task-001.md").read_bytes(),
        "tasks_yaml": (root / "tasks.yaml").read_bytes(),
    }


def _oss36c_drift_live_sources(config_path: Path) -> None:
    root = config_path.parent
    developer = root / "prompts" / "developer.md"
    markdown_spec = root / "tasks" / "synthetic-task-001.md"
    tasks_yaml = root / "tasks.yaml"
    developer.write_bytes(developer.read_bytes() + b"LIVE-DEVELOPER-DRIFT\n")
    markdown_spec.write_bytes(markdown_spec.read_bytes() + b"LIVE-SPEC-DRIFT\n")
    tasks_yaml.write_bytes(tasks_yaml.read_bytes() + b"# LIVE-TASK-SOURCE-DRIFT\n")


def _oss36c_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
    seed_tracked: bool,
) -> tuple[Path, Path, Path, Path, dict[str, bytes]]:
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    config_path, target, runtime = _prepare_fresh_surface(tmp_path, active_task=True)
    if seed_tracked:
        destination = target / _OSS36C_ALLOWED_PATH
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(_OSS36C_SEEDED_BYTES)
        _run_git(target, "add", "--", _OSS36C_ALLOWED_PATH)
        _run_git(target, "commit", "-m", "seed allowed product baseline")
        _write_fresh_main_state(runtime, target)
    originals = _oss36c_live_bytes(config_path)
    _oss36c_install_adapter(tmp_path)
    allowed_payload = _OSS36C_PRODUCT_BYTES.get(mode, b"")
    instruction = _oss36c_instruction(
        tmp_path,
        mode=mode,
        allowed_payload=allowed_payload,
    )
    monkeypatch.setenv("OSS36C_INSTRUCTION", str(instruction))
    return config_path, target, runtime, tmp_path / "oss36c-evidence.json", originals


def _oss36c_frame_bindings(evidence: dict[str, Any]) -> dict[str, tuple[str, int]]:
    frame = json.loads(evidence["prompt_frame"])
    payloads = frame["payloads"]
    return {
        str(item["role"]): (str(item["sha256"]), int(item["byte_length"]))
        for item in payloads
    }


def _oss36c_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
    *,
    mode: str,
    seed_tracked: bool = False,
    drift_after_snapshot: bool = False,
    drift_after_adapter: bool = False,
    observe_compose: bool = False,
) -> dict[str, Any]:
    import task_transition_runner.cli as cli_module

    config_path, target, runtime, evidence_path, originals = _oss36c_prepare(
        tmp_path,
        monkeypatch,
        mode=mode,
        seed_tracked=seed_tracked,
    )
    snapshots: dict[str, bytes] = {}
    composed: list[dict[str, tuple[str, int]]] = []

    if drift_after_snapshot:
        original_snapshot = cli_module._materialize_canonical_run_snapshot

        def _snapshot_then_drift(config: Any, activation: Any) -> None:
            original_snapshot(config, activation)
            run_root = (
                config.runtime_root / "runs" / activation.identity.origin_run_id
            )
            snapshots["developer"] = (
                run_root / "instructions" / "prompts" / "developer"
            ).read_bytes()
            snapshots["task_spec"] = (run_root / "instructions" / "task_spec").read_bytes()
            _oss36c_drift_live_sources(config_path)

        monkeypatch.setattr(
            cli_module,
            "_materialize_canonical_run_snapshot",
            _snapshot_then_drift,
        )

    if drift_after_adapter:
        original_start = cli_module.start_cursor_adapter

        def _start_then_drift(config: Any) -> dict[str, Any]:
            result = original_start(config)
            identity = cli_module._RUN_LOCK.get("identity")
            origin = getattr(identity, "origin_run_id", "")
            persisted = config.runtime_root / "runs" / origin
            snapshots["developer"] = (
                persisted / "instructions" / "prompts" / "developer"
            ).read_bytes()
            snapshots["task_spec"] = (
                persisted / "instructions" / "task_spec"
            ).read_bytes()
            _oss36c_drift_live_sources(config_path)
            return result

        monkeypatch.setattr(cli_module, "start_cursor_adapter", _start_then_drift)

    if observe_compose:
        original_compose = cli_module.compose_adapter_git_worker_result

        def _observe_compose(**kwargs: Any) -> dict[str, Any]:
            composed.append(
                {
                    "developer": (
                        kwargs["developer"].sha256,
                        kwargs["developer"].byte_length,
                    ),
                    "task_spec": (
                        kwargs["task_spec"].sha256,
                        kwargs["task_spec"].byte_length,
                    ),
                }
            )
            return original_compose(**kwargs)

        monkeypatch.setattr(
            cli_module,
            "compose_adapter_git_worker_result",
            _observe_compose,
        )

    exit_code = main(["run", "--config", str(config_path)])
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    return {
        "exit_code": exit_code,
        "stdout": stdout,
        "target": target,
        "runtime": runtime,
        "config_path": config_path,
        "evidence": evidence,
        "originals": originals,
        "snapshots": snapshots,
        "composed": composed,
        "live": _oss36c_live_bytes(config_path),
    }


def _oss36c_result_effects(observed: dict[str, Any]) -> bool:
    target = observed["target"]
    evidence = observed["evidence"]
    commits_after = _oss36c_commit_objects(target)
    commits_before = set(evidence["commit_objects"])
    head_now = _run_git(target, "rev-parse", "HEAD")
    ref_now = _run_git(target, "rev-parse", f"refs/heads/{evidence['branch']}")
    return bool(
        commits_after - commits_before
        or head_now != evidence["head"]
        or ref_now != evidence["branch_sha"]
        or _oss36c_index_sha256(target) != evidence["index_sha256"]
        or _oss36c_runtime_effects(observed["runtime"])
    )


def _oss36c_require_adapter_stimulus(observed: dict[str, Any]) -> None:
    assert observed["evidence"]["mutation_error"] == ""
    assert observed["evidence"]["prompt_frame"]


def test_run_blocks_developer_commit_before_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    observed = _oss36c_run(
        tmp_path,
        monkeypatch,
        capsys,
        mode="developer_commit",
    )
    _oss36c_require_adapter_stimulus(observed)
    target = observed["target"]
    developer_head = str(observed["evidence"]["head"])
    starts = _oss36c_commits_with_prefix(target, "START ")
    assert len(starts) == 1
    assert _oss36c_parents(target, developer_head) == (starts[0],)
    assert _oss36c_subject(target, developer_head) == _OSS36C_DEVELOPER_SUBJECT
    if _oss36c_result_effects(observed):
        pytest.fail("DEVELOPER_COMMIT_MUST_BLOCK_BEFORE_RESULT")
    assert observed["stdout"]["outcome"] == "BLOCKED"
    assert observed["exit_code"] == 1
    assert _oss36c_runtime_effects(observed["runtime"]) == ()
    assert _oss36c_commits_with_prefix(target, "RESULT ") == ()


@pytest.mark.parametrize("worktree_mode", ["staged", "unstaged", "untracked"])
def test_runner_materializes_allowed_worktree_bytes_in_one_result(
    worktree_mode: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    expected = _OSS36C_PRODUCT_BYTES[worktree_mode]
    observed = _oss36c_run(
        tmp_path,
        monkeypatch,
        capsys,
        mode=worktree_mode,
        seed_tracked=worktree_mode == "unstaged",
    )
    _oss36c_require_adapter_stimulus(observed)
    target = observed["target"]
    starts = _oss36c_commits_with_prefix(target, "START ")
    assert len(starts) == 1
    assert observed["evidence"]["head"] == starts[0]
    assert (target / _OSS36C_ALLOWED_PATH).read_bytes() == expected
    results = _oss36c_commits_with_prefix(target, "RESULT ")
    result_blob = None if len(results) != 1 else _oss36c_blob(
        target,
        results[0],
        _OSS36C_ALLOWED_PATH,
    )
    if result_blob != expected:
        pytest.fail(_OSS36C_MISSING_SIGNATURE[worktree_mode])
    result = results[0]
    assert _oss36c_parents(target, result) == (starts[0],)
    assert _run_git(target, "rev-parse", "HEAD") == result
    assert _run_git(target, "rev-parse", f"refs/heads/{observed['evidence']['branch']}") == result
    assert _run_git(target, "diff", "--name-only", starts[0], result) == _OSS36C_ALLOWED_PATH
    assert _run_git(target, "status", "--porcelain=v1", "-uall") == ""
    assert _oss36c_index_sha256(target) == _oss36c_sha256((target / ".git" / "index").read_bytes())


def test_foreign_path_blocks_before_any_result_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    observed = _oss36c_run(
        tmp_path,
        monkeypatch,
        capsys,
        mode="mixed_foreign",
    )
    _oss36c_require_adapter_stimulus(observed)
    target = observed["target"]
    starts = _oss36c_commits_with_prefix(target, "START ")
    assert len(starts) == 1
    assert observed["evidence"]["head"] == starts[0]
    assert (target / _OSS36C_ALLOWED_PATH).read_bytes() == _OSS36C_PRODUCT_BYTES["mixed_foreign"]
    assert (target / _OSS36C_FOREIGN_PATH).read_bytes() == _OSS36C_FOREIGN_BYTES
    if _oss36c_result_effects(observed):
        pytest.fail("FOREIGN_PATH_MUST_BLOCK_BEFORE_RESULT_EFFECT")
    assert observed["stdout"]["outcome"] == "BLOCKED"
    assert observed["exit_code"] == 1
    assert _oss36c_commits_with_prefix(target, "RESULT ") == ()
    assert _oss36c_runtime_effects(observed["runtime"]) == ()


def test_fresh_adapter_uses_run_local_developer_and_markdown_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    observed = _oss36c_run(
        tmp_path,
        monkeypatch,
        capsys,
        mode="capture_frame",
        drift_after_snapshot=True,
    )
    _oss36c_require_adapter_stimulus(observed)
    originals = observed["originals"]
    snapshots = observed["snapshots"]
    live = observed["live"]
    assert snapshots["developer"] == originals["developer"]
    assert snapshots["task_spec"] == originals["markdown_spec"]
    assert live["developer"] != snapshots["developer"]
    assert live["markdown_spec"] != snapshots["task_spec"]
    assert live["tasks_yaml"] != originals["tasks_yaml"]
    bindings = _oss36c_frame_bindings(observed["evidence"])
    assert tuple(bindings) == ("developer", "task_spec")
    expected = {
        "developer": _oss36c_binding(snapshots["developer"]),
        "task_spec": _oss36c_binding(snapshots["task_spec"]),
    }
    if bindings != expected:
        pytest.fail("FRESH_ADAPTER_USED_LIVE_OR_WRONG_TASK_SPEC")


def test_fresh_worker_result_reuses_adapter_snapshot_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    observed = _oss36c_run(
        tmp_path,
        monkeypatch,
        capsys,
        mode="capture_frame",
        drift_after_adapter=True,
        observe_compose=True,
    )
    _oss36c_require_adapter_stimulus(observed)
    originals = observed["originals"]
    snapshots = observed["snapshots"]
    live = observed["live"]
    assert snapshots["developer"] == originals["developer"]
    assert snapshots["task_spec"] == originals["markdown_spec"]
    assert live["developer"] != originals["developer"]
    assert live["markdown_spec"] != originals["markdown_spec"]
    assert live["tasks_yaml"] != originals["tasks_yaml"]
    assert len(observed["composed"]) == 1
    adapter_binding = _oss36c_frame_bindings(observed["evidence"])
    compose_binding = observed["composed"][0]
    if adapter_binding != compose_binding:
        pytest.fail("WORKER_RESULT_SNAPSHOT_BINDING_DIFFERS_FROM_ADAPTER")
    assert adapter_binding == {
        "developer": _oss36c_binding(snapshots["developer"]),
        "task_spec": _oss36c_binding(snapshots["task_spec"]),
    }


_PREDECISION_CLI_ALPHA = "predecision-alpha"
_PREDECISION_CLI_BETA = "predecision-beta"
_PREDECISION_CLI_UNEXPECTED = "PREDECISION_VALIDATION_UNEXPECTED_NONZERO_REACHED_DECISION"
_PREDECISION_CLI_TIMEOUT = "PREDECISION_VALIDATION_TIMEOUT_REACHED_DECISION"
_PREDECISION_CLI_MUTATION = "PREDECISION_VALIDATION_MUTATION_NOT_FAIL_CLOSED"
_PREDECISION_CLI_REPLAY = "PREDECISION_VALIDATION_CLI_REPLAY_RERAN_DEVELOPER_OR_COMMANDS"
_PREDECISION_CLI_BASELINE = "PREDECISION_BASELINE_OR_FOREIGN_EVIDENCE_IS_NOT_TASK_AUTHORITY"
_PREDECISION_CLI_WITNESS_LINE = re.compile(
    r"^(?P<label>\S+) merge=(?P<merge>\S*) tree=(?P<tree>\S*)$"
)


def _predecision_cli_fail(signature: str) -> None:
    pytest.fail(signature)


def _predecision_cli_script(path: Path, body: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path.resolve())


def _predecision_cli_witness_command(path: Path, witness: Path, label: str) -> str:
    quoted_witness = str(witness).replace("'", "'\\''")
    return _predecision_cli_script(
        path,
        "\n".join(
            (
                "#!/bin/sh",
                "merge=$(git rev-parse -q --verify MERGE_HEAD 2>/dev/null || true)",
                "tree=$(git write-tree 2>/dev/null || true)",
                (
                    f"printf '%s merge=%s tree=%s\\n' '{label}' "
                    f"\"$merge\" \"$tree\" >> '{quoted_witness}'"
                ),
                "exit 0",
                "",
            )
        ),
    )


def _publish_predecision_cli_task(
    config_path: Path,
    commands: tuple[dict[str, Any], ...],
    outcome: dict[str, Any],
) -> None:
    task_source = config_path.parent / "tasks.yaml"
    task_source.write_text(
        yaml.safe_dump(
            {
                "schema_version": _SCHEMA_VERSION,
                "active_task": {
                    "task_id": _CLI_TASK_ID,
                    "spec_ref": "./tasks/synthetic-task-001.md",
                    "allowed_paths": list(_CLI_ALLOWED),
                    "validation": {
                        "commands": [dict(command) for command in commands],
                    },
                    "expected_outcome": dict(outcome),
                    "adapter": {"kind": "cursor"},
                    "metadata": {},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _predecision_cli_rows(witness: Path) -> tuple[dict[str, str], ...]:
    if not witness.is_file():
        return ()
    rows: list[dict[str, str]] = []
    for line in witness.read_text(encoding="utf-8").splitlines():
        matched = _PREDECISION_CLI_WITNESS_LINE.fullmatch(line)
        if matched is None:
            continue
        rows.append(matched.groupdict())
    return tuple(rows)


def _predecision_cli_decisions(runtime: Path) -> tuple[Path, ...]:
    return tuple(sorted((runtime / "runs").glob("*/decisions/decision_request.yaml")))


def _predecision_cli_workers(runtime: Path) -> tuple[dict[str, Any], ...]:
    payloads: list[dict[str, Any]] = []
    for path in sorted((runtime / "runs").glob("*/worker_result.yaml")):
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            payloads.append(loaded)
    return tuple(payloads)


def _predecision_cli_stopped_clean(
    target: Path,
    runtime: Path,
    stdout: Mapping[str, Any],
    *,
    tracked_before: str,
    main_before: str,
) -> bool:
    git_dir = Path(_run_git(target, "rev-parse", "--absolute-git-dir"))
    porcelain = _run_git(target, "status", "--porcelain=v1", "-uall")
    index = _run_git(target, "diff", "--cached", "--name-only")
    tracked = (target / "tracked.txt").read_text(encoding="utf-8")
    main_now = _run_git(target, "rev-parse", "refs/heads/main")
    return (
        stdout.get("outcome") == "BLOCKED"
        and stdout.get("ok") is False
        and _predecision_cli_decisions(runtime) == ()
        and porcelain == ""
        and index == ""
        and not (git_dir / "MERGE_HEAD").is_file()
        and not (git_dir / "hooks" / "pre-commit").is_file()
        and tracked == tracked_before
        and main_now == main_before
    )


@contextlib.contextmanager
def _predecision_clean_cli_source(tmp_path: Path):
    source = _detached_head_source(tmp_path / "clean-source")
    try:
        yield source
    finally:
        _remove_detached_head_source(source)


def _open_predecision_cli(
    tmp_path: Path,
    commands: tuple[dict[str, Any], ...],
    outcome: dict[str, Any],
) -> dict[str, Any]:
    config_path, target, runtime = _prepare_fresh_surface(
        tmp_path / "world",
        active_task=True,
    )
    _publish_predecision_cli_task(config_path, commands, outcome)
    return {
        "config_path": config_path,
        "target": target,
        "runtime": runtime,
        "tracked_before": (target / "tracked.txt").read_text(encoding="utf-8"),
        "main_before": _run_git(target, "rev-parse", "refs/heads/main"),
    }


def test_predecision_unexpected_nonzero_stops_before_decision_request(
    tmp_path: Path,
) -> None:
    command = _predecision_cli_script(
        tmp_path / "commands" / "unexpected.sh",
        "#!/bin/sh\nprintf '%s\\n' 'UNEXPECTED_PREDECISION_FAILURE' >&2\nexit 9\n",
    )
    world = _open_predecision_cli(
        tmp_path,
        ({"id": "predecision-unexpected", "command": command, "timeout_seconds": 30},),
        {"kind": "GREEN"},
    )
    with _predecision_clean_cli_source(tmp_path):
        source = tmp_path / "clean-source"
        stdout = _run_real_cli(source, world["config_path"])
    if not _predecision_cli_stopped_clean(
        world["target"],
        world["runtime"],
        stdout,
        tracked_before=world["tracked_before"],
        main_before=world["main_before"],
    ):
        _predecision_cli_fail(_PREDECISION_CLI_UNEXPECTED)


def test_predecision_timeout_stops_before_decision_request(tmp_path: Path) -> None:
    command = _predecision_cli_script(
        tmp_path / "commands" / "timeout.sh",
        "#!/bin/sh\npython3 -c 'import time; time.sleep(30)'\n",
    )
    world = _open_predecision_cli(
        tmp_path,
        ({"id": "predecision-timeout", "command": command, "timeout_seconds": 1},),
        {"kind": "GREEN"},
    )
    with _predecision_clean_cli_source(tmp_path):
        stdout = _run_real_cli(tmp_path / "clean-source", world["config_path"])
    if not _predecision_cli_stopped_clean(
        world["target"],
        world["runtime"],
        stdout,
        tracked_before=world["tracked_before"],
        main_before=world["main_before"],
    ):
        _predecision_cli_fail(_PREDECISION_CLI_TIMEOUT)


def test_predecision_worktree_index_merge_or_hook_mutation_restores_without_decision(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "mutation-witness.txt"
    quoted_witness = str(witness).replace("'", "'\\''")
    command = _predecision_cli_script(
        tmp_path / "commands" / "mutate.sh",
        "\n".join(
            (
                "#!/bin/sh",
                "printf 'x\\n' >> tracked.txt",
                "git add -- tracked.txt",
                "hook=$(git rev-parse --absolute-git-dir)/hooks/pre-commit",
                "printf '%s\\n' '#!/bin/sh' 'exit 0' > \"$hook\"",
                "chmod +x \"$hook\"",
                "merge=$(git rev-parse -q --verify MERGE_HEAD 2>/dev/null || true)",
                f"printf '%s merge=%s\\n' 'mutate' \"$merge\" >> '{quoted_witness}'",
                "exit 0",
                "",
            )
        ),
    )
    world = _open_predecision_cli(
        tmp_path,
        ({"id": "predecision-mutation", "command": command, "timeout_seconds": 30},),
        {"kind": "GREEN"},
    )
    with _predecision_clean_cli_source(tmp_path):
        stdout = _run_real_cli(tmp_path / "clean-source", world["config_path"])
    if not _predecision_cli_stopped_clean(
        world["target"],
        world["runtime"],
        stdout,
        tracked_before=world["tracked_before"],
        main_before=world["main_before"],
    ):
        _predecision_cli_fail(_PREDECISION_CLI_MUTATION)


def test_predecision_cli_replay_does_not_rerun_developer_or_validation(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "replay-witness.txt"
    commands = (
        {
            "id": _PREDECISION_CLI_ALPHA,
            "command": _predecision_cli_witness_command(
                tmp_path / "commands" / "alpha.sh",
                witness,
                _PREDECISION_CLI_ALPHA,
            ),
            "timeout_seconds": 30,
        },
        {
            "id": _PREDECISION_CLI_BETA,
            "command": _predecision_cli_witness_command(
                tmp_path / "commands" / "beta.sh",
                witness,
                _PREDECISION_CLI_BETA,
            ),
            "timeout_seconds": 30,
        },
    )
    world = _open_predecision_cli(tmp_path, commands, {"kind": "GREEN"})
    marker = _arm_adapter_marker(tmp_path / "world")
    os.environ["OSS6_ADAPTER_MARKER"] = str(marker)
    try:
        with _predecision_clean_cli_source(tmp_path):
            source = tmp_path / "clean-source"
            _set_baseline_timeout(world["config_path"], 1)
            _run_real_cli(source, world["config_path"])
            first_rows = _predecision_cli_rows(witness)
            first_developer = _marker_count(marker)
            first_logs = {
                path.relative_to(world["runtime"]).as_posix(): path.read_bytes()
                for path in sorted((world["runtime"] / "runs").rglob("validation/*"))
                if path.is_file()
            }
            _run_real_cli(source, world["config_path"])
            second_rows = _predecision_cli_rows(witness)
            second_developer = _marker_count(marker)
            second_logs = {
                path.relative_to(world["runtime"]).as_posix(): path.read_bytes()
                for path in sorted((world["runtime"] / "runs").rglob("validation/*"))
                if path.is_file()
            }
    finally:
        os.environ.pop("OSS6_ADAPTER_MARKER", None)
    expected = (
        {"label": _PREDECISION_CLI_ALPHA},
        {"label": _PREDECISION_CLI_BETA},
    )
    labels = tuple({"label": row["label"]} for row in first_rows)
    merges_bound = all(row["merge"] != "" and row["tree"] != "" for row in first_rows)
    if (
        labels != expected
        or not merges_bound
        or second_rows != first_rows
        or first_developer != 1
        or second_developer != 1
        or not first_logs
        or second_logs != first_logs
    ):
        _predecision_cli_fail(_PREDECISION_CLI_REPLAY)


def test_predecision_baseline_harness_manual_and_seed_are_not_task_authority(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "baseline-witness.txt"
    commands = (
        {
            "id": _PREDECISION_CLI_ALPHA,
            "command": _predecision_cli_witness_command(
                tmp_path / "commands" / "alpha.sh",
                witness,
                _PREDECISION_CLI_ALPHA,
            ),
            "timeout_seconds": 30,
        },
        {
            "id": _PREDECISION_CLI_BETA,
            "command": _predecision_cli_witness_command(
                tmp_path / "commands" / "beta.sh",
                witness,
                _PREDECISION_CLI_BETA,
            ),
            "timeout_seconds": 30,
        },
    )
    world = _open_predecision_cli(tmp_path, commands, {"kind": "GREEN"})
    runtime = world["runtime"]
    foreign_log = b"FOREIGN-HARNESS-LOG\n"
    (runtime / "harness_validation.log").write_bytes(foreign_log)
    (runtime / "manual_evidence.yaml").write_text(
        "kind: manual_evidence\nbaseline_validation: true\n",
        encoding="utf-8",
    )
    (runtime / "diagnostic_root_bytes.bin").write_bytes(b"diagnostic-root")
    (runtime / "decision_seed.yaml").write_text(
        "ready: true\npromotion_class: GREEN_PROMOTION\n",
        encoding="utf-8",
    )
    foreign_digest = hashlib.sha256(foreign_log).hexdigest()
    with _predecision_clean_cli_source(tmp_path):
        stdout = _run_real_cli(tmp_path / "clean-source", world["config_path"])
    rows = _predecision_cli_rows(witness)
    workers = _predecision_cli_workers(runtime)
    items: list[Mapping[str, Any]] = []
    if len(workers) == 1 and isinstance(workers[0].get("validation_results"), list):
        items = [
            item
            for item in workers[0]["validation_results"]
            if isinstance(item, Mapping)
        ]
    command_ids = tuple(item.get("command_id") for item in items)
    foreign_used = any(item.get("output_sha256") == foreign_digest for item in items)
    labels = tuple(row["label"] for row in rows)
    merges_bound = all(row["merge"] != "" and row["tree"] != "" for row in rows)
    if (
        stdout.get("outcome") != "AWAITING_DECISIONS"
        or labels != (_PREDECISION_CLI_ALPHA, _PREDECISION_CLI_BETA)
        or not merges_bound
        or command_ids != (_PREDECISION_CLI_ALPHA, _PREDECISION_CLI_BETA)
        or foreign_used
        or "baseline-validation" in command_ids
    ):
        _predecision_cli_fail(_PREDECISION_CLI_BASELINE)


_POSTDECISION_TASK_END_PROMOTION = "src/task_transition_runner/task_end_promotion.py"
_POSTDECISION_TASK_END_PROMOTION_SHA256 = (
    "47ec04529d643f4f33d81a97e228051bec124e19f66c2beb347fdf5f0a89c536"
)
_POSTDECISION_CURRENT_PRODUCT_OVERLAYS = (
    "src/task_transition_runner/cli.py",
    "src/task_transition_runner/runner_git_promotion.py",
)
_POSTDECISION_DIGEST_SIGNATURE = (
    "EFFECT_ADMISSION_PERSISTED_VALIDATION_DIGESTS_NOT_BOUND"
)
_POSTDECISION_REPROOF_SIGNATURE = (
    "POSTDECISION_VALIDATION_REPROOF_AUTHORITY_NOT_BOUND"
)
_POSTDECISION_PRODUCT_ADAPTER = r"""
import json
import os
import sys
from pathlib import Path


def main() -> None:
    marker = os.environ.get("OSS6_ADAPTER_MARKER")
    if marker:
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("invoked\n")
    frame = sys.argv[-1]
    workspace = ""
    prefix = "--workspace="
    for argument in sys.argv[1:-1]:
        if argument.startswith(prefix):
            workspace = argument[len(prefix) :]
    if workspace:
        product = Path(workspace) / "src" / "example.py"
        product.parent.mkdir(parents=True, exist_ok=True)
        product.write_text("postdecision-sealed-product\n", encoding="utf-8")
    session_id = "synthetic-cli-real-component-session"
    records = (
        {
            "type": "system",
            "subtype": "init",
            "cwd": workspace,
            "session_id": session_id,
        },
        {
            "type": "user",
            "message": {"content": [{"type": "text", "text": frame}]},
            "session_id": session_id,
        },
        {
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "synthetic-cli-assistant"}]
            },
            "session_id": session_id,
        },
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "synthetic-cli-result",
            "session_id": session_id,
        },
    )
    payload = b"".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        + b"\n"
        for record in records
    )
    os.write(1, payload)


if __name__ == "__main__":
    main()
"""


def _assert_product_draft_stasis() -> None:
    root = _candidate_root()
    payload = (root / _POSTDECISION_TASK_END_PROMOTION).read_bytes()
    assert (
        hashlib.sha256(payload).hexdigest() == _POSTDECISION_TASK_END_PROMOTION_SHA256
    ), _POSTDECISION_TASK_END_PROMOTION


def _overlay_checkout_bytes(
    source: Path,
    root: Path,
    relative: str,
    *,
    pinned_sha256: str | None = None,
) -> None:
    payload = (root / relative).read_bytes()
    if pinned_sha256 is not None:
        assert hashlib.sha256(payload).hexdigest() == pinned_sha256, relative
    destination = source / relative
    destination.write_bytes(payload)
    assert destination.read_bytes() == payload, relative


def _bind_draft_source(tmp_path: Path) -> Path:
    source = _clone_clean_runner_source(tmp_path)
    root = _candidate_root()
    _overlay_checkout_bytes(
        source,
        root,
        _POSTDECISION_TASK_END_PROMOTION,
        pinned_sha256=_POSTDECISION_TASK_END_PROMOTION_SHA256,
    )
    for relative in _POSTDECISION_CURRENT_PRODUCT_OVERLAYS:
        _overlay_checkout_bytes(source, root, relative)
    relatives = (
        _POSTDECISION_TASK_END_PROMOTION,
        *_POSTDECISION_CURRENT_PRODUCT_OVERLAYS,
    )
    subprocess.run(  # noqa: S603
        ["git", "-C", str(source), "add", "--", *relatives],
        check=True,
        capture_output=True,
        text=True,
    )
    staged = _run_git(
        source,
        "diff",
        "--cached",
        "--name-only",
        "--",
        *relatives,
    )
    candidate_head = _run_git(root, "rev-parse", "HEAD")
    if staged:
        subprocess.run(  # noqa: S603
            [
                "git",
                "-C",
                str(source),
                "-c",
                "user.email=postdecision-reproof@example.invalid",
                "-c",
                "user.name=postdecision-reproof",
                "commit",
                "-m",
                "fixture: bind read-only 36EE5H product draft",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert _run_git(source, "rev-parse", "HEAD") != candidate_head
    else:
        assert _run_git(source, "rev-parse", "HEAD") == candidate_head
    assert _run_git(source, "status", "--porcelain=v1", "-uall") == ""
    return source


def _install_postdecision_product_adapter(world_root: Path) -> Path:
    executable = world_root / "synthetic-cursor-adapter"
    executable.write_text(
        f"#!{sys.executable}\n{_POSTDECISION_PRODUCT_ADAPTER}",
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return world_root / "adapter-invocations"


def _validation_byte_snapshot(runtime: Path, origin_run_id: str) -> tuple[tuple[str, bytes], ...]:
    folder = runtime / "runs" / origin_run_id / "validation"
    files: list[tuple[str, bytes]] = []
    for path in sorted(folder.rglob("*")):
        if path.is_file():
            files.append((path.relative_to(folder).as_posix(), path.read_bytes()))
    return tuple(files)


def _open_postdecision_pending(tmp_path: Path) -> dict[str, Any]:
    from task_transition_runner.runner_code_authority import persist_runtime_source_pin

    _assert_product_draft_stasis()
    world_root = tmp_path / "world"
    config_path, target, runtime = _prepare_fresh_surface(world_root, active_task=True)
    witness = tmp_path / "postdecision-witness.txt"
    commands = (
        {
            "id": _PREDECISION_CLI_ALPHA,
            "command": _predecision_cli_witness_command(
                tmp_path / "commands" / "alpha.sh",
                witness,
                _PREDECISION_CLI_ALPHA,
            ),
            "timeout_seconds": 30,
        },
        {
            "id": _PREDECISION_CLI_BETA,
            "command": _predecision_cli_witness_command(
                tmp_path / "commands" / "beta.sh",
                witness,
                _PREDECISION_CLI_BETA,
            ),
            "timeout_seconds": 30,
        },
    )
    _publish_predecision_cli_task(config_path, commands, {"kind": "GREEN"})
    source_root = _bind_draft_source(tmp_path)
    pin = runtime / "runner_source_pin.yaml"
    pin.unlink()
    persist_runtime_source_pin(
        runtime_root=runtime,
        source_sha=_run_git(source_root, "rev-parse", "HEAD"),
    )
    marker = _install_postdecision_product_adapter(world_root)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []
    first, _first_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_ROLE_PREPARATION,
        step="AWAITING_DECISIONS",
        source_root=source_root,
    )
    if first.returncode not in {0, 1} or first.stderr:
        raise AssertionError((first.returncode, first.stdout, first.stderr))
    first_stdout = _stdout_payload(first)
    if first_stdout.get("outcome") != "AWAITING_DECISIONS":
        raise AssertionError((first_stdout, first.stderr))
    rows = _predecision_cli_rows(witness)
    if tuple(row["label"] for row in rows) != (
        _PREDECISION_CLI_ALPHA,
        _PREDECISION_CLI_BETA,
    ):
        raise AssertionError(rows)
    if not all(row["merge"] and row["tree"] for row in rows):
        raise AssertionError(rows)
    if _marker_count(marker) != 1:
        raise AssertionError(_marker_count(marker))
    worker = _sole_worker(runtime)
    results = worker.get("validation_results")
    if not isinstance(results, list) or len(results) != 2:
        raise AssertionError(worker)
    command_ids = tuple(item.get("command_id") for item in results)
    if command_ids != (_PREDECISION_CLI_ALPHA, _PREDECISION_CLI_BETA):
        raise AssertionError(command_ids)
    for item in results:
        output_ref = item.get("output_ref")
        digest = item.get("output_sha256")
        if not isinstance(output_ref, str) or not isinstance(digest, str):
            raise AssertionError(item)
        log_bytes = (runtime / output_ref).read_bytes()
        if hashlib.sha256(log_bytes).hexdigest() != digest or not log_bytes:
            raise AssertionError(item)
    origin_run_id = str(worker["origin_run_id"])
    manifest_path = runtime / "runs" / origin_run_id / "validation" / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("status") != "passed":
        raise AssertionError(manifest)
    sealed_tree = manifest.get("candidate_tree_oid")
    if not isinstance(sealed_tree, str) or not sealed_tree:
        raise AssertionError(manifest)
    if any(row["tree"] != sealed_tree for row in rows):
        raise AssertionError((rows, sealed_tree))
    base_sha = manifest.get("base_sha")
    if not isinstance(base_sha, str) or len(base_sha) != 40:
        raise AssertionError(manifest)
    base_tree = _run_git(target, "rev-parse", f"{base_sha}^{{tree}}")
    if sealed_tree == base_tree:
        raise AssertionError((sealed_tree, base_tree))
    if manifest.get("result_sha") != worker.get("result_sha"):
        raise AssertionError(manifest)
    if manifest.get("promotion_attempt_id") != worker.get("promotion_attempt_id"):
        raise AssertionError(manifest)
    worker_path = runtime / "runs" / origin_run_id / "worker_result.yaml"
    worker_bytes = worker_path.read_bytes()
    sealed_validation = _validation_byte_snapshot(runtime, origin_run_id)
    _bind_external_decisions(runtime, worker)
    second, _second_pid = _account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=_ROLE_PREPARATION,
        step="PENDING_PROMOTION",
        source_root=source_root,
    )
    if second.returncode not in {0, 1} or second.stderr:
        raise AssertionError((second.returncode, second.stdout, second.stderr))
    second_stdout = _stdout_payload(second)
    if second_stdout.get("outcome") != "PENDING_PROMOTION":
        raise AssertionError((second_stdout, second.stderr))
    if _predecision_cli_rows(witness) != rows:
        raise AssertionError("task commands reran while persisting pending")
    if _marker_count(marker) != 1:
        raise AssertionError("developer reran while persisting pending")
    if worker_path.read_bytes() != worker_bytes:
        raise AssertionError("worker result changed while persisting pending")
    if _validation_byte_snapshot(runtime, origin_run_id) != sealed_validation:
        raise AssertionError("sealed validation changed while persisting pending")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return {
        "config_path": config_path,
        "target": target,
        "runtime": runtime,
        "source_root": source_root,
        "marker": marker,
        "pidfile": pidfile,
        "worker": worker,
        "worker_path": worker_path,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "sealed_tree": sealed_tree,
        "base_sha": str(base_sha),
        "witness": witness,
        "witness_rows": rows,
        "baseline_command": str(config["baseline_validation"]["command"]),
    }


def _admit_sealed_pending(
    world: Mapping[str, Any],
    *,
    validation_command: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    import shlex

    from task_transition_runner.config import load_config
    from task_transition_runner.decision_contracts import (
        DecisionIdentity,
        load_orchestrator_decision,
        load_validator_decision,
    )
    from task_transition_runner.runner_git_context import evaluate_branch_preflight
    from task_transition_runner.runner_git_promotion import (
        admit_pending_promotion_effect,
        load_pending_promotion,
    )
    from task_transition_runner.runner_promotion_state import load_promotion_consumed
    from task_transition_runner.task_contract import load_task_contract
    from task_transition_runner.worker_result import (
        WorkerResultIdentity,
        load_worker_result,
    )

    config = load_config(world["config_path"])
    runtime = world["runtime"]
    target = world["target"]
    worker = world["worker"]
    origin_run_id = str(worker["origin_run_id"])
    digests = {
        str(item["command_id"]): str(item["output_sha256"])
        for item in worker["validation_results"]
    }
    loaded = load_worker_result(
        world["worker_path"],
        expected_identity=WorkerResultIdentity(
            task_id=str(worker["task_id"]),
            origin_run_id=origin_run_id,
            promotion_attempt_id=str(worker["promotion_attempt_id"]),
            result_sha=str(worker["result_sha"]),
        ),
        expected_validation_digests=digests,
    )
    if tuple(item.command_id for item in loaded.validation_results) != tuple(digests):
        raise AssertionError(loaded.validation_results)
    pending = load_pending_promotion(runtime_root=runtime)
    identity = DecisionIdentity(
        task_id=loaded.task_id,
        origin_run_id=loaded.origin_run_id,
        promotion_attempt_id=loaded.promotion_attempt_id,
        result_sha=loaded.result_sha,
    )
    decisions = runtime / "runs" / origin_run_id / "decisions"
    orchestrator = load_orchestrator_decision(
        decisions / "orchestrator_decision.yaml",
        expected_identity=identity,
        artifact_root=runtime,
    )
    validator = load_validator_decision(
        decisions / "validator_decision.yaml",
        expected_identity=identity,
        artifact_root=runtime,
    )
    contract = load_task_contract(
        config.task_source,
        target_worktree=target,
    )
    ownership = {
        "schema_version": _SCHEMA_VERSION,
        "lock_name": "runner_git.lock",
        "run_id": "postdecision-reproof",
        "pid": os.getpid(),
        "started_at_utc": "2026-10-25T22:00:00Z",
    }
    (runtime / "runner_git.lock").write_text(
        json.dumps(ownership) + "\n",
        encoding="utf-8",
    )
    preflight = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=config.target.main_branch,
    )
    if preflight.get("ok") is not True:
        raise AssertionError(preflight)
    command = (
        tuple(shlex.split(config.baseline_validation.command))
        if validation_command is None
        else validation_command
    )
    before = {
        relative for relative, _payload in _validation_byte_snapshot(runtime, origin_run_id)
    }
    result = admit_pending_promotion_effect(
        runtime_root=runtime,
        target_worktree=target,
        validation_command=command,
        validation_timeout_seconds=config.baseline_validation.timeout_seconds,
        command_id="baseline-validation",
        task_snapshot=pending["task_snapshot"],
        instruction_snapshot=pending["instruction_snapshot"],
        worker_result=loaded,
        expected_outcome=contract.active_task.expected_outcome,
        orchestrator_decision=orchestrator,
        validator_decision=validator,
        base_sha=str(pending["base_sha"]),
        task_branch_head_sha=loaded.result_sha,
        allowed_paths=tuple(pending["allowed_paths"]),
        risk_facts={},
        lock_ownership={
            "ok": True,
            "status": "owned",
            "reason_code": "LOCK_OK",
            "owned": True,
            "ownership": ownership,
        },
        git_preflight=preflight,
        consumed_payload=load_promotion_consumed(
            promotion_consumed_path=runtime / "promotion_consumed.yaml",
        ),
        red_lock=None,
        unresolved_foreign_epoch=False,
    )
    after = {
        relative for relative, _payload in _validation_byte_snapshot(runtime, origin_run_id)
    }
    tock_sha = result.get("tock_sha") if isinstance(result, dict) else None
    tock_tree = None
    if isinstance(tock_sha, str) and tock_sha:
        tock_tree = _run_git(target, "rev-parse", f"{tock_sha}^{{tree}}")
    return {
        "result": result,
        "tock_sha": tock_sha if isinstance(tock_sha, str) and tock_sha else None,
        "tock_tree": tock_tree,
        "new_validation_files": tuple(sorted(after - before)),
    }


def _drift_sealed_field(world: Mapping[str, Any], field: str, value: str) -> None:
    manifest_path = world["manifest_path"]
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    runtime = world["runtime"]
    if field == "candidate_tree_oid":
        manifest["candidate_tree_oid"] = value
    elif field in {"base_sha", "result_sha", "promotion_attempt_id"}:
        manifest[field] = value
    for record in manifest["records"]:
        metadata_path = runtime / str(record["metadata_ref"])
        metadata = yaml.safe_load(metadata_path.read_text(encoding="utf-8"))
        metadata[field] = value
        encoded = yaml.safe_dump(
            metadata,
            sort_keys=False,
            allow_unicode=False,
        ).encode("utf-8")
        metadata_path.write_bytes(encoded)
        record[field] = value
        record["metadata_sha256"] = hashlib.sha256(encoded).hexdigest()
    manifest_path.write_bytes(
        yaml.safe_dump(manifest, sort_keys=False, allow_unicode=False).encode("utf-8")
    )


def _drift_sealed_command_text(world: Mapping[str, Any], command_text: str) -> None:
    manifest_path = world["manifest_path"]
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    runtime = world["runtime"]
    record = manifest["records"][0]
    metadata_path = runtime / str(record["metadata_ref"])
    metadata = yaml.safe_load(metadata_path.read_text(encoding="utf-8"))
    metadata["command_text"] = command_text
    encoded = yaml.safe_dump(
        metadata,
        sort_keys=False,
        allow_unicode=False,
    ).encode("utf-8")
    metadata_path.write_bytes(encoded)
    record["command_text"] = command_text
    record["metadata_sha256"] = hashlib.sha256(encoded).hexdigest()
    manifest_path.write_bytes(
        yaml.safe_dump(manifest, sort_keys=False, allow_unicode=False).encode("utf-8")
    )


def _drift_sealed_log_bytes(world: Mapping[str, Any]) -> None:
    record = world["manifest"]["records"][0]
    log_path = world["runtime"] / str(record["output_ref"])
    log_path.write_bytes(log_path.read_bytes() + b"\nSEALED_LOG_DRIFT\n")


def _remove_sealed_log(world: Mapping[str, Any]) -> None:
    record = world["manifest"]["records"][0]
    (world["runtime"] / str(record["output_ref"])).unlink()


def test_effect_admission_resume_binds_persisted_validation_digests(
    tmp_path: Path,
) -> None:
    from task_transition_runner.worker_result import (
        WorkerResultIdentity,
        load_worker_result,
    )

    try:
        world = _open_postdecision_pending(tmp_path)
        worker = world["worker"]
        digests = {
            str(item["command_id"]): str(item["output_sha256"])
            for item in worker["validation_results"]
        }
        loaded = load_worker_result(
            world["worker_path"],
            expected_identity=WorkerResultIdentity(
                task_id=str(worker["task_id"]),
                origin_run_id=str(worker["origin_run_id"]),
                promotion_attempt_id=str(worker["promotion_attempt_id"]),
                result_sha=str(worker["result_sha"]),
            ),
            expected_validation_digests=digests,
        )
        if not loaded.validation_results or not digests:
            raise AssertionError("producer persisted empty validation digests")
        ledger: list[dict[str, Any]] = []
        resumed, _pid = _account_real_cli(
            world["config_path"],
            world["pidfile"],
            world["marker"],
            ledger,
            role=_ROLE_ACCEPTANCE,
            step="RESUME",
            source_root=world["source_root"],
        )
        if _predecision_cli_rows(world["witness"]) != world["witness_rows"]:
            raise AssertionError("task commands reran after decisions")
        if _marker_count(world["marker"]) != 1:
            raise AssertionError("developer reran after decisions")
        observed = f"{resumed.stdout}\n{resumed.stderr}"
        if "RESULT_DIGEST_MISMATCH" in observed:
            pytest.fail(_POSTDECISION_DIGEST_SIGNATURE)
        if resumed.returncode not in {0, 1} or resumed.stderr:
            raise AssertionError((resumed.returncode, resumed.stdout, resumed.stderr))
        stdout = _stdout_payload(resumed)
        if stdout.get("reason_code") == "RESULT_DIGEST_MISMATCH":
            pytest.fail(_POSTDECISION_DIGEST_SIGNATURE)
    finally:
        _assert_product_draft_stasis()


def _assert_imported_canonical_cli(cli_module: Any) -> None:
    module_path = Path(cli_module.__file__).resolve()
    expected = _candidate_root() / "src" / "task_transition_runner" / "cli.py"
    assert module_path == expected
    assert "tmp" not in module_path.parts
    assert "pytest-of-gubel" not in module_path.parts


def test_canonical_fresh_tail_composition_seam_blocked_rolls_main_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module

    _assert_frozen_j_draft_bytes()
    _assert_imported_canonical_cli(cli_module)
    config_path, target, _runtime = _write_public_world(tmp_path)
    from task_transition_runner.config import load_config

    config = load_config(config_path)
    main_before = _run_git(target, "rev-parse", config.main_ref)
    identity = SimpleNamespace(
        task_id="composition-seam-task",
        origin_run_id="composition-seam-origin",
        promotion_attempt_id="composition-seam-attempt",
    )
    activation = SimpleNamespace(identity=identity, facts=object())
    blocked = cli_module._SeamBlocked("COMPOSITION_SEAM_BLOCKED")
    git_calls: list[tuple[str, ...]] = []
    main_observations: list[str] = []
    real_git_text = cli_module._git_text
    prepare_calls: list[str] = []

    def _observe_git(repo_root: Path, *args: str) -> str:
        git_calls.append(args)
        value = real_git_text(repo_root, *args)
        if args == ("rev-parse", config.main_ref):
            main_observations.append(value)
        return value

    def _move_main_during_start(**_kwargs: Any) -> Any:
        _run_git(target, "commit", "--allow-empty", "-m", "start tick moved main")
        moved = _run_git(target, "rev-parse", config.main_ref)
        _run_git(target, "branch", "seam-coverage-task", moved)
        return SimpleNamespace(
            status=cli_module.start_tick_module.StartTickStatus.MATERIALIZED,
            reason_code="START_TICK_MATERIALIZED",
            task_branch="seam-coverage-task",
            tick_sha=moved,
        )

    def _compose(**_kwargs: Any) -> dict[str, Any]:
        raise blocked

    def _prepare_must_stay_closed(**_kwargs: Any) -> dict[str, Any]:
        prepare_calls.append("prepare")
        raise AssertionError("promotion prepare opened after composition seam block")

    cli_module._RUN_LOCK.clear()
    monkeypatch.setattr(cli_module, "_git_text", _observe_git)
    monkeypatch.setattr(cli_module, "_materialize_canonical_run_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cli_module.start_tick_module,
        "reconcile_start_tick",
        _move_main_during_start,
    )
    monkeypatch.setattr(
        cli_module,
        "start_cursor_adapter",
        lambda _config: {"ok": True, "adapter_outcome": object()},
    )
    monkeypatch.setattr(cli_module, "_run_allowed_paths", lambda _config: ())
    monkeypatch.setattr(cli_module, "compose_adapter_git_worker_result", _compose)
    monkeypatch.setattr(
        cli_module,
        "prepare_git_promotion_task_end_artifacts",
        _prepare_must_stay_closed,
    )
    try:
        with pytest.raises(cli_module._SeamBlocked) as raised:
            cli_module._fresh_active_tail(
                config,
                activation,
                predecessor_basis={},
            )
    finally:
        cli_module._RUN_LOCK.clear()

    assert raised.value is blocked
    assert raised.value.reason_code == "COMPOSITION_SEAM_BLOCKED"
    assert raised.value.reason_code != "COMPOSITION_FAILED"
    assert main_observations[0] == main_before
    assert main_observations[1] != main_before
    assert (
        "update-ref",
        config.main_ref,
        main_observations[0],
        main_observations[1],
    ) in git_calls
    assert _run_git(target, "rev-parse", config.main_ref) == main_before
    assert _run_git(target, "rev-parse", "refs/heads/seam-coverage-task") != main_before
    assert prepare_calls == []
    assert _run_git(target, "log", "-1", "--format=%s", config.main_ref) == (
        "public cli baseline"
    )
    _assert_frozen_j_draft_bytes()
    _assert_imported_canonical_cli(cli_module)


def test_canonical_resume_blocks_non_consumed_continuation_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config

    _assert_frozen_j_draft_bytes()
    _assert_imported_canonical_cli(cli_module)
    config_path, target, runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    pending = {
        "task_id": "continuation-lifecycle-task",
        "origin_run_id": "continuation-lifecycle-origin",
        "promotion_attempt_id": "continuation-lifecycle-attempt",
        "result_sha": "a" * 64,
        "promotion_class": "GREEN_PROMOTION",
    }
    identity = SimpleNamespace(
        task_id=pending["task_id"],
        origin_run_id=pending["origin_run_id"],
        promotion_attempt_id=pending["promotion_attempt_id"],
    )
    outcome = SimpleNamespace(
        status=SimpleNamespace(value="DRAINED"),
        reason_code="PROMOTION_EFFECT_ADMITTED",
        tock_sha="b" * 64,
        task_id=pending["task_id"],
        origin_run_id=pending["origin_run_id"],
        promotion_attempt_id=pending["promotion_attempt_id"],
        result_sha=pending["result_sha"],
        promotion_class=pending["promotion_class"],
    )
    decision = SimpleNamespace(
        phase=SimpleNamespace(value="DRAINED_AWAITING_TICK"),
        outcome=outcome,
    )
    continuation_path = runtime / "post_tick_continuation.yaml"
    continuation_path.write_text(
        "status: AUTHORIZED\nsuccessor_task_id: continuation-lifecycle-task\n",
        encoding="utf-8",
    )
    continuation_bytes = continuation_path.read_bytes()
    main_before = _run_git(target, "rev-parse", config.main_ref)
    consumed_statuses: list[object] = []
    admission_calls: list[dict[str, Any]] = []

    def _admit(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        admission_calls.append({"admitted": True})
        return {"admitted": True}

    def _authorize_drain(**kwargs: Any) -> Any:
        assert kwargs["effect_admission"] == {"admitted": True}
        return decision

    def _return_authorized_view(**_kwargs: Any) -> Any:
        view = SimpleNamespace(status="AUTHORIZED")
        consumed_statuses.append(view.status)
        return view

    monkeypatch.setattr(cli_module, "load_pending_promotion", lambda **_kwargs: pending)
    monkeypatch.setattr(cli_module, "_effect_admission", _admit)
    monkeypatch.setattr(cli_module, "evaluate_drain_before_dispatch", _authorize_drain)
    monkeypatch.setattr(
        cli_module.post_tick_module,
        "consume_post_tick_continuation",
        _return_authorized_view,
    )

    with pytest.raises(cli_module._SeamBlocked) as raised:
        cli_module._resume_qualified_pending(config, identity, {"owner": "coverage"})

    assert raised.value.reason_code == "CONTINUATION_LIFECYCLE_INVALID"
    assert raised.value.__cause__ is None
    assert consumed_statuses == ["AUTHORIZED"]
    assert consumed_statuses[0] != "CONSUMED"
    assert admission_calls == [{"admitted": True}]
    assert continuation_path.read_bytes() == continuation_bytes
    assert not (runtime / "promotion_consumed.yaml").exists()
    assert not (runtime / "promotion_journal.yaml").exists()
    assert _run_git(target, "rev-parse", config.main_ref) == main_before
    _assert_frozen_j_draft_bytes()
    _assert_imported_canonical_cli(cli_module)


def test_canonical_effect_admission_blocks_empty_validation_digests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.runner_git_promotion import RunnerGitPromotionError
    from task_transition_runner.config import load_config

    _assert_frozen_j_draft_bytes()
    _assert_imported_canonical_cli(cli_module)
    config_path, target, runtime = _write_public_world(tmp_path)
    config = load_config(config_path)
    identity = SimpleNamespace(
        task_id="empty-digest-task",
        origin_run_id="empty-digest-origin",
        promotion_attempt_id="empty-digest-attempt",
    )
    worker_path = runtime / "runs" / identity.origin_run_id / "worker_result.yaml"
    worker_path.parent.mkdir(parents=True)
    worker_path.write_text(
        "result_sha: " + ("c" * 64) + "\nvalidation_results: []\n",
        encoding="utf-8",
    )
    worker_bytes = worker_path.read_bytes()
    persisted = yaml.safe_load(worker_bytes)
    assert persisted["validation_results"] == []
    main_before = _run_git(target, "rev-parse", config.main_ref)
    worker_loads: list[Path] = []
    effect_opens: list[str] = []
    yaml_reads: list[tuple[Path, object]] = []
    real_read_yaml = cli_module._read_yaml

    def _observe_yaml(path: Path) -> dict[str, Any] | None:
        loaded = real_read_yaml(path)
        yaml_reads.append((path, loaded))
        return loaded

    def _worker_must_stay_unloaded(*_args: Any, **_kwargs: Any) -> Any:
        worker_loads.append(worker_path)
        raise AssertionError("worker loaded without validation digests")

    def _effect_must_stay_closed(**_kwargs: Any) -> Any:
        effect_opens.append("evaluate_drain_before_dispatch")
        raise AssertionError("promotion effect opened without validation digests")

    monkeypatch.setattr(cli_module, "_read_yaml", _observe_yaml)
    monkeypatch.setattr(cli_module, "load_pending_promotion", lambda **_kwargs: {"base_sha": "d" * 64})
    monkeypatch.setattr(cli_module, "load_worker_result", _worker_must_stay_unloaded)
    monkeypatch.setattr(
        cli_module,
        "evaluate_drain_before_dispatch",
        _effect_must_stay_closed,
    )

    with pytest.raises(RunnerGitPromotionError) as direct:
        cli_module._effect_admission(config, identity, {"owner": "coverage"})

    assert direct.value.reason_code == "PROMOTION_EVIDENCE_MISSING"
    assert worker_loads == []
    assert effect_opens == []
    assert any(
        path.resolve() == worker_path.resolve() and loaded == persisted
        for path, loaded in yaml_reads
    )

    with pytest.raises(cli_module._SeamBlocked) as resumed:
        cli_module._resume_qualified_pending(config, identity, {"owner": "coverage"})

    assert resumed.value.reason_code == "PROMOTION_EVIDENCE_MISSING"
    assert isinstance(resumed.value.__cause__, RunnerGitPromotionError)
    assert resumed.value.__cause__.reason_code == "PROMOTION_EVIDENCE_MISSING"
    assert worker_loads == []
    assert effect_opens == []
    assert worker_path.read_bytes() == worker_bytes
    assert not (runtime / "pending_promotion.yaml").exists()
    assert not (runtime / "promotion_journal.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    assert _run_git(target, "rev-parse", config.main_ref) == main_before
    _assert_frozen_j_draft_bytes()
    _assert_imported_canonical_cli(cli_module)
