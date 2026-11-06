"""Synthetic public local lock, ownership, and target-preflight contract.

Keep:
- explicit configured target worktree and runtime root
- local single-writer lock ownership
- concurrent acquire with exactly one winner
- stale reclaim only when the owner pid is dead and old enough
- exact-ownership release CAS
- linked-worktree-correct git-dir / common-dir / target distinction
- fail-closed branch, dirty, and operation-in-progress preflight

The lock is a local single-writer coordination file. It does not
search parent trees for layout defaults.
"""

from __future__ import annotations

import fcntl
import inspect
import json
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

import pytest

import task_transition_runner.runner_git_context as git_context_module
from task_transition_runner.runner_git_context import (
    RunnerGitContextError,
    acquire_runner_git_lock,
    build_runner_git_lock_payload,
    evaluate_branch_preflight,
    evaluate_runner_git_lock,
    release_runner_git_lock,
    resolve_runner_git_context,
    verify_runner_git_lock_ownership,
)

_LOCK_NAME = "runner_git.lock"
_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_OWNER_ID = "lock-owner-synthetic-alpha"
_FOREIGN_OWNER_ID = "lock-owner-synthetic-beta"
_REFERENCE_PID = 4242
_FOREIGN_PID = 5252
_STALE_PID = 31337
_REFERENCE_STARTED_AT = "2026-09-04T17:00:00Z"
_REFERENCE_NOW = "2026-09-04T18:00:00Z"
_STALE_AFTER_SECONDS = 3600
_GIT_AUTHOR_NAME = "Lock Preflight Contract"
_GIT_AUTHOR_EMAIL = "lock-preflight@task-transition-runner.invalid"
_REQUIRED_EXPORTS = (
    "RunnerGitContextError",
    "resolve_runner_git_context",
    "build_runner_git_lock_payload",
    "evaluate_runner_git_lock",
    "acquire_runner_git_lock",
    "release_runner_git_lock",
    "verify_runner_git_lock_ownership",
    "evaluate_branch_preflight",
)
_RESOLVE_REQUIRED_PARAMETERS = (
    "target_worktree",
    "runtime_root",
)
_ACQUIRE_REQUIRED_PARAMETERS = (
    "lock_path",
    "run_id",
    "pid",
    "started_at_utc",
    "now_utc",
    "stale_after_seconds",
    "is_pid_alive",
)
_PREFLIGHT_REQUIRED_PARAMETERS = (
    "target_worktree",
    "main_branch",
)
_LOCK_REQUIRED_FIELDS = (
    "schema_version",
    "lock_name",
    "run_id",
    "pid",
    "started_at_utc",
)
_STALE_STARTED_AT = "2026-09-04T16:59:59Z"
_PUBLIC_FUNCTIONS = (
    resolve_runner_git_context,
    build_runner_git_lock_payload,
    evaluate_runner_git_lock,
    acquire_runner_git_lock,
    release_runner_git_lock,
    verify_runner_git_lock_ownership,
    evaluate_branch_preflight,
)


def _run_git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _init_target_repo(tmp_path: Path, *, name: str = "target-repo") -> Path:
    repo_root = tmp_path / name
    repo_root.mkdir(parents=True)
    subprocess.run(  # noqa: S603
        ["git", "-c", "init.defaultBranch=main", "init"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    _run_git(repo_root, "config", "user.name", _GIT_AUTHOR_NAME)
    _run_git(repo_root, "config", "user.email", _GIT_AUTHOR_EMAIL)
    (repo_root / "tracked.txt").write_text("baseline\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "tracked.txt")
    _run_git(repo_root, "commit", "-m", "baseline")
    return repo_root


def _add_linked_worktree(primary: Path, linked: Path) -> Path:
    linked.parent.mkdir(parents=True, exist_ok=True)
    _run_git(primary, "checkout", "-b", "anchor-hold")
    _run_git(primary, "worktree", "add", str(linked), "main")
    return linked


def _absolute_git_dir(worktree: Path) -> Path:
    return Path(_run_git(worktree, "rev-parse", "--absolute-git-dir")).resolve()


def _absolute_git_common_dir(worktree: Path) -> Path:
    reported = Path(_run_git(worktree, "rev-parse", "--git-common-dir"))
    if reported.is_absolute():
        return reported.resolve()
    return (worktree / reported).resolve()


def _runtime_root(tmp_path: Path) -> Path:
    path = tmp_path / "runner-runtime"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _lock_path(tmp_path: Path) -> Path:
    return _runtime_root(tmp_path) / _LOCK_NAME


def _write_lock(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _read_lock(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _lock_dir_names(lock_path: Path) -> set[str]:
    return {entry.name for entry in lock_path.parent.iterdir()}


def _stale_payload() -> dict[str, Any]:
    return _reference_payload(
        run_id="stale-owner",
        pid=_STALE_PID,
        started_at_utc=_STALE_STARTED_AT,
    )


def _assert_only_named_lock(lock_path: Path, payload: dict[str, Any]) -> None:
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}
    assert _read_lock(lock_path) == payload


def _assert_invalid_lock_preserved(lock_path: Path, original_text: str) -> None:
    evaluated = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda _: False,
    )
    acquired = _acquire(lock_path, is_pid_alive=lambda _: False)
    _assert_blocked(evaluated, "LOCK_INVALID")
    _assert_blocked(acquired, "LOCK_INVALID")
    assert lock_path.read_text(encoding="utf-8") == original_text
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}


def _reference_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "lock_name": _LOCK_NAME,
        "run_id": _REFERENCE_OWNER_ID,
        "pid": _REFERENCE_PID,
        "started_at_utc": _REFERENCE_STARTED_AT,
    }
    payload.update(overrides)
    return payload


def _acquire(
    lock_path: Path,
    *,
    run_id: str = _REFERENCE_OWNER_ID,
    pid: int = _REFERENCE_PID,
    started_at_utc: str = _REFERENCE_STARTED_AT,
    now_utc: str = _REFERENCE_NOW,
    stale_after_seconds: int = _STALE_AFTER_SECONDS,
    is_pid_alive: Callable[[int], bool] | None = None,
) -> dict[str, Any]:
    alive = is_pid_alive if is_pid_alive is not None else (lambda owner_pid: owner_pid == pid)
    result = acquire_runner_git_lock(
        lock_path=lock_path,
        run_id=run_id,
        pid=pid,
        started_at_utc=started_at_utc,
        now_utc=now_utc,
        stale_after_seconds=stale_after_seconds,
        is_pid_alive=alive,
    )
    assert isinstance(result, dict)
    return result


def _assert_blocked(result: Any, reason_code: str) -> dict[str, Any]:
    assert isinstance(result, dict)
    assert result.get("ok") is False
    assert result.get("reason_code") == reason_code
    assert "fail" in str(result.get("status", "fail-closed")).lower()
    return result


def test_public_exports_are_the_named_lock_and_preflight_surface() -> None:
    exported = {name for name in dir(git_context_module) if not name.startswith("_")}

    for name in _REQUIRED_EXPORTS:
        assert name in exported
    assert exported >= set(_REQUIRED_EXPORTS)


def test_public_functions_are_callable() -> None:
    for function in _PUBLIC_FUNCTIONS:
        assert callable(function)


def test_resolve_requires_configured_target_worktree_and_runtime_root() -> None:
    parameters = inspect.signature(resolve_runner_git_context).parameters
    for name in _RESOLVE_REQUIRED_PARAMETERS:
        assert name in parameters
    assert list(parameters) == list(_RESOLVE_REQUIRED_PARAMETERS)


def test_acquire_requires_explicit_lock_identity_and_liveness() -> None:
    parameters = inspect.signature(acquire_runner_git_lock).parameters
    for name in _ACQUIRE_REQUIRED_PARAMETERS:
        assert name in parameters
    assert list(parameters) == list(_ACQUIRE_REQUIRED_PARAMETERS)


def test_preflight_requires_configured_target_worktree_and_branch() -> None:
    parameters = inspect.signature(evaluate_branch_preflight).parameters
    for name in _PREFLIGHT_REQUIRED_PARAMETERS:
        assert name in parameters
    assert list(parameters) == list(_PREFLIGHT_REQUIRED_PARAMETERS)


def test_lock_documents_local_single_writer_bound() -> None:
    documents = [
        git_context_module.__doc__ or "",
        acquire_runner_git_lock.__doc__ or "",
        release_runner_git_lock.__doc__ or "",
        resolve_runner_git_context.__doc__ or "",
    ]
    combined = "\n".join(documents).lower()
    assert "single-writer" in combined
    assert "local" in combined


def test_error_exposes_stable_reason_code() -> None:
    error = RunnerGitContextError("GIT_TARGET_INVALID")
    assert error.reason_code == "GIT_TARGET_INVALID"
    assert "GIT_TARGET_INVALID" in str(error)


def test_lock_payload_uses_stable_public_fields() -> None:
    payload = build_runner_git_lock_payload(
        run_id=_REFERENCE_OWNER_ID,
        pid=_REFERENCE_PID,
        started_at_utc=_REFERENCE_STARTED_AT,
    )

    assert payload == _reference_payload()


def test_evaluate_lock_allows_missing_lock_file(tmp_path: Path) -> None:
    result = evaluate_runner_git_lock(
        lock_path=_lock_path(tmp_path),
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda pid: pid == _REFERENCE_PID,
    )

    assert result == {"ok": True, "reason_code": "LOCK_NONE", "stale": False}


def test_evaluate_lock_blocks_living_owner(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    _write_lock(lock_path, _reference_payload())

    result = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda pid: pid == _REFERENCE_PID,
    )

    blocked = _assert_blocked(result, "LOCK_ACTIVE")
    assert blocked["stale"] is False
    assert blocked["pid_alive"] is True
    assert blocked["lock"] == _reference_payload()


@pytest.mark.parametrize(
    ("started_at_utc", "pid_alive"),
    (
        ("2026-09-04T16:00:00Z", True),
        ("2026-09-04T17:00:00Z", False),
        ("2026-09-04T17:00:01Z", False),
    ),
)
def test_evaluate_lock_rejects_age_or_liveness_alone(
    tmp_path: Path,
    started_at_utc: str,
    pid_alive: bool,
) -> None:
    lock_path = _lock_path(tmp_path)
    _write_lock(lock_path, _reference_payload(started_at_utc=started_at_utc))

    result = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda _: pid_alive,
    )

    blocked = _assert_blocked(result, "LOCK_ACTIVE")
    assert blocked["pid_alive"] is pid_alive
    assert blocked["stale"] is False


def test_evaluate_lock_marks_dead_and_old_owner_reclaimable(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _reference_payload(run_id="stale-owner", pid=_STALE_PID, started_at_utc="2026-09-04T16:59:59Z")
    _write_lock(lock_path, stale)

    result = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda _: False,
    )

    assert result["ok"] is True
    assert result["reason_code"] == "LOCK_STALE_RECLAIMABLE"
    assert result["stale"] is True
    assert result["pid_alive"] is False
    assert result["lock"] == stale


@pytest.mark.parametrize("missing_field", _LOCK_REQUIRED_FIELDS)
def test_closed_lock_json_rejects_missing_required_fields(
    tmp_path: Path,
    missing_field: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    payload = _reference_payload()
    payload.pop(missing_field)
    _write_lock(lock_path, payload)

    _assert_invalid_lock_preserved(lock_path, lock_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "payload",
    (
        {"pid": "4242"},
        {"pid": True},
        {"pid": 0},
        {"pid": -1},
        {"run_id": ""},
        {"run_id": 1},
        {"schema_version": "2.0"},
        {"schema_version": 1},
        {"lock_name": "other.lock"},
        {"started_at_utc": 1},
        {"orphan_field": "not-in-schema"},
    ),
)
def test_closed_lock_json_rejects_wrong_types_and_unknown_fields(
    tmp_path: Path,
    payload: dict[str, Any],
) -> None:
    lock_path = _lock_path(tmp_path)
    invalid = _reference_payload(**payload)
    _write_lock(lock_path, invalid)

    _assert_invalid_lock_preserved(lock_path, lock_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "started_at_utc",
    (
        "",
        "not-a-timestamp",
        "2026-09-04 17:00:00",
        "2026-09-04T17:00:00",
        "2026-09-04T17:00:00+00:00",
    ),
)
def test_evaluate_lock_rejects_invalid_started_at_timestamp(
    tmp_path: Path,
    started_at_utc: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    _write_lock(lock_path, _reference_payload(started_at_utc=started_at_utc))

    _assert_invalid_lock_preserved(lock_path, lock_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "now_utc",
    (
        "",
        "not-a-timestamp",
        "2026-09-04 18:00:00",
        "2026-09-04T18:00:00",
    ),
)
def test_evaluate_lock_rejects_invalid_now_timestamp(
    tmp_path: Path,
    now_utc: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    original = _reference_payload()
    _write_lock(lock_path, original)

    evaluated = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=now_utc,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda _: False,
    )
    acquired = _acquire(
        lock_path,
        now_utc=now_utc,
        is_pid_alive=lambda _: False,
    )
    _assert_blocked(evaluated, "LOCK_INVALID")
    _assert_blocked(acquired, "LOCK_INVALID")
    _assert_only_named_lock(lock_path, original)


def test_evaluate_and_acquire_reject_liveness_probe_errors(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    original = _reference_payload()
    _write_lock(lock_path, original)

    def boom(_pid: int) -> bool:
        raise RuntimeError("liveness probe failed")

    evaluated = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=boom,
    )
    acquired = _acquire(lock_path, is_pid_alive=boom)
    _assert_blocked(evaluated, "LOCK_INVALID")
    _assert_blocked(acquired, "LOCK_INVALID")
    _assert_only_named_lock(lock_path, original)


def test_acquire_missing_lock_writes_explicit_ownership(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)

    acquired = _acquire(lock_path)

    assert acquired["ok"] is True
    assert acquired["reason_code"] == "LOCK_OK"
    assert acquired["acquired"] is True
    assert acquired["reclaimed_stale_lock"] is False
    assert acquired["ownership"] == _reference_payload()
    assert _read_lock(lock_path) == acquired["ownership"]


def test_acquire_blocks_living_foreign_owner_without_lock_loss(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    foreign = _reference_payload(run_id=_FOREIGN_OWNER_ID, pid=_FOREIGN_PID)
    _write_lock(lock_path, foreign)

    blocked = _acquire(
        lock_path,
        run_id=_REFERENCE_OWNER_ID,
        pid=_REFERENCE_PID,
        is_pid_alive=lambda pid: pid == _FOREIGN_PID,
    )

    _assert_blocked(blocked, "LOCK_ACTIVE")
    assert _read_lock(lock_path) == foreign


def test_acquire_reclaims_only_dead_and_old_ownership(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _reference_payload(
        run_id="stale-owner",
        pid=_STALE_PID,
        started_at_utc="2026-09-04T16:59:59Z",
    )
    _write_lock(lock_path, stale)

    acquired = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert acquired["ok"] is True
    assert acquired["reclaimed_stale_lock"] is True
    assert acquired["stale_ownership"] == stale
    assert acquired["ownership"]["run_id"] == _REFERENCE_OWNER_ID
    assert _read_lock(lock_path) == acquired["ownership"]


def test_acquire_reclaims_dead_but_young_ownership_immediately(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    young_dead = _reference_payload(
        run_id="young-dead-owner",
        pid=_STALE_PID,
        started_at_utc="2026-09-04T17:00:00Z",
    )
    _write_lock(lock_path, young_dead)

    acquired = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert acquired["ok"] is True
    assert acquired["reason_code"] == "LOCK_OK"
    assert acquired["acquired"] is True
    assert acquired["reclaimed_stale_lock"] is True
    assert acquired["stale_ownership"] == young_dead
    assert acquired["ownership"]["run_id"] == _REFERENCE_OWNER_ID
    assert _read_lock(lock_path) == acquired["ownership"]


def test_acquire_blocks_invalid_lock_bytes(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.write_text("{not-json", encoding="utf-8")

    blocked = _acquire(lock_path, is_pid_alive=lambda _: False)

    _assert_blocked(blocked, "LOCK_INVALID")
    assert lock_path.read_text(encoding="utf-8") == "{not-json"
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}


@pytest.mark.parametrize(
    "raw",
    (
        "",
        "[]",
        "null",
        "1",
        '"scalar"',
        "true",
        "false",
    ),
)
def test_closed_lock_json_rejects_empty_file_and_non_mappings(
    tmp_path: Path,
    raw: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.write_text(raw, encoding="utf-8")

    _assert_invalid_lock_preserved(lock_path, raw)


def test_lock_path_directory_is_fail_closed_and_unchanged(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.mkdir()
    marker = lock_path / "keep-me.txt"
    marker.write_text("unchanged\n", encoding="utf-8")

    evaluated = evaluate_runner_git_lock(
        lock_path=lock_path,
        now_utc=_REFERENCE_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda _: False,
    )
    acquired = _acquire(lock_path, is_pid_alive=lambda _: False)

    _assert_blocked(evaluated, "LOCK_INVALID")
    _assert_blocked(acquired, "LOCK_INVALID")
    assert lock_path.is_dir()
    assert marker.read_text(encoding="utf-8") == "unchanged\n"
    assert {entry.name for entry in lock_path.iterdir()} == {"keep-me.txt"}


def test_acquire_does_not_reenter_same_living_owner(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    first = _acquire(lock_path)

    second = _acquire(lock_path)

    _assert_blocked(second, "LOCK_ACTIVE")
    _assert_only_named_lock(lock_path, first["ownership"])


def test_stale_reclaim_revalidates_liveness_before_unlink(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    probes: list[int] = []

    def pid_alive(pid: int) -> bool:
        probes.append(pid)
        return len(probes) >= 2

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=pid_alive,
    )

    assert len(probes) >= 2
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_RACE", "LOCK_ACTIVE"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    _assert_only_named_lock(lock_path, stale)


def test_stale_reclaim_revalidates_ownership_before_unlink(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    replacement = _reference_payload(
        run_id=_FOREIGN_OWNER_ID,
        pid=_FOREIGN_PID,
        started_at_utc=_REFERENCE_NOW,
    )
    _write_lock(lock_path, stale)
    probes: list[int] = []

    def pid_alive(pid: int) -> bool:
        probes.append(pid)
        if len(probes) >= 2:
            _write_lock(lock_path, replacement)
        return False

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=pid_alive,
    )

    assert len(probes) >= 2
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_RACE", "LOCK_ACTIVE"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    _assert_only_named_lock(lock_path, replacement)


def test_failed_acquire_against_living_lock_leaves_no_partial_files(
    tmp_path: Path,
) -> None:
    lock_path = _lock_path(tmp_path)
    foreign = _reference_payload(run_id=_FOREIGN_OWNER_ID, pid=_FOREIGN_PID)
    _write_lock(lock_path, foreign)

    blocked = _acquire(
        lock_path,
        is_pid_alive=lambda pid: pid == _FOREIGN_PID,
    )

    _assert_blocked(blocked, "LOCK_ACTIVE")
    _assert_only_named_lock(lock_path, foreign)


def test_failed_reclaim_unlink_leaves_original_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    original_unlink = Path.unlink

    def unlink_lock(self: Path, *args: Any, **kwargs: Any) -> None:
        if self.resolve() == lock_path.resolve():
            raise OSError("busy")
        original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink_lock)

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    _assert_blocked(blocked, "LOCK_RECLAIM_FAILED")
    _assert_only_named_lock(lock_path, stale)


def test_unwritable_runtime_acquire_leaves_no_lock_file(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path)
    lock_path = runtime / _LOCK_NAME
    runtime.chmod(0o555)
    try:
        if os.access(runtime, os.W_OK):
            pytest.skip("process retains write access after clearing write mode bits")
        blocked = _acquire(lock_path)
        assert blocked["ok"] is False
        assert blocked["reason_code"] in {"LOCK_INVALID", "LOCK_ACQUIRE_FAILED"}
        assert "fail" in str(blocked.get("status", "fail-closed")).lower()
        assert lock_path.exists() is False
        assert _lock_dir_names(lock_path) == set()
    finally:
        runtime.chmod(0o755)


def test_exclusive_create_write_failure_removes_partial_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    exclusive_create = {"happened": False}
    original_open = os.open

    def tracked_open(path: str, flags: int, *args: Any, **kwargs: Any) -> int:
        descriptor = original_open(path, flags, *args, **kwargs)
        if Path(path).resolve() == lock_path.resolve() and flags & os.O_EXCL:
            exclusive_create["happened"] = True
        return descriptor

    def fail_dump(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("write failed")

    monkeypatch.setattr(os, "open", tracked_open)
    monkeypatch.setattr(json, "dump", fail_dump)

    blocked = _acquire(lock_path)

    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_ACQUIRE_FAILED", "LOCK_INVALID"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert exclusive_create["happened"] is True
    assert lock_path.exists() is False
    assert _lock_dir_names(lock_path) == set()


def test_concurrent_acquire_allows_exactly_one_winner(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    barrier = threading.Barrier(2)

    def acquire(index: int) -> dict[str, Any]:
        barrier.wait(timeout=5)
        return _acquire(
            lock_path,
            run_id=f"lock-owner-synthetic-{index}",
            pid=4200 + index,
            started_at_utc=f"2026-09-04T18:0{index}:00Z",
            now_utc=_REFERENCE_NOW,
            is_pid_alive=lambda _: True,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(acquire, (1, 2)))

    winners = [result for result in results if result.get("ok") is True]
    losers = [result for result in results if result.get("ok") is not True]
    assert len(winners) == 1
    assert len(losers) == 1
    assert winners[0]["reason_code"] == "LOCK_OK"
    loser = losers[0]
    assert isinstance(loser, dict)
    assert loser.get("ok") is False
    assert loser.get("acquired") is False
    assert loser.get("reason_code") in {"LOCK_ACTIVE", "LOCK_INVALID"}
    assert "fail" in str(loser.get("status", "fail-closed")).lower()
    _assert_only_named_lock(lock_path, winners[0]["ownership"])


def test_missing_lock_exclusive_create_false_is_deterministic_race_loser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    assert lock_path.exists() is False
    before = {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")}
    observed: dict[str, Any] = {"calls": 0}

    def lose_exclusive_create(*, path: Path, ownership: dict[str, Any]) -> bool:
        observed["calls"] += 1
        assert path == lock_path
        assert path.exists() is False
        assert ownership == _reference_payload()
        return False

    monkeypatch.setattr(
        git_context_module,
        "_create_lock_exclusive",
        lose_exclusive_create,
    )

    blocked = _acquire(lock_path)

    assert observed["calls"] == 1
    assert blocked == {
        "ok": False,
        "status": "fail-closed",
        "reason_code": "LOCK_ACTIVE",
        "acquired": False,
    }
    assert lock_path.exists() is False
    after = {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")}
    assert after == before


def test_release_requires_exact_ownership(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)

    released = release_runner_git_lock(
        lock_path=lock_path,
        ownership=acquired["ownership"],
    )

    assert released == {
        "ok": True,
        "status": "released",
        "reason_code": "LOCK_OK",
        "released": True,
    }
    assert lock_path.exists() is False


def test_foreign_release_does_not_remove_living_ownership(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    foreign = build_runner_git_lock_payload(
        run_id=_FOREIGN_OWNER_ID,
        pid=_FOREIGN_PID,
        started_at_utc=_REFERENCE_NOW,
    )

    released = release_runner_git_lock(lock_path=lock_path, ownership=foreign)

    _assert_blocked(released, "LOCK_OWNERSHIP_LOST")
    assert released["released"] is False
    assert _read_lock(lock_path) == acquired["ownership"]


@pytest.mark.parametrize(
    "ownership_change",
    (
        {"run_id": _FOREIGN_OWNER_ID},
        {"pid": _FOREIGN_PID},
        {"started_at_utc": _REFERENCE_NOW},
        {"schema_version": "1.1"},
        {"lock_name": "other.lock"},
    ),
)
def test_release_rejects_partially_matching_ownership(
    tmp_path: Path,
    ownership_change: dict[str, Any],
) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    presented = {**acquired["ownership"], **ownership_change}

    released = release_runner_git_lock(lock_path=lock_path, ownership=presented)

    _assert_blocked(released, "LOCK_OWNERSHIP_LOST")
    assert released["released"] is False
    _assert_only_named_lock(lock_path, acquired["ownership"])


def test_release_rejects_extra_or_missing_ownership_fields(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    extra = {**acquired["ownership"], "orphan_field": "nope"}
    missing = dict(acquired["ownership"])
    missing.pop("started_at_utc")

    extra_release = release_runner_git_lock(lock_path=lock_path, ownership=extra)
    missing_release = release_runner_git_lock(lock_path=lock_path, ownership=missing)

    _assert_blocked(extra_release, "LOCK_OWNERSHIP_LOST")
    _assert_blocked(missing_release, "LOCK_OWNERSHIP_LOST")
    _assert_only_named_lock(lock_path, acquired["ownership"])


def test_idempotent_release_after_success_does_not_invent_authority(
    tmp_path: Path,
) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    first = release_runner_git_lock(
        lock_path=lock_path,
        ownership=acquired["ownership"],
    )
    second = release_runner_git_lock(
        lock_path=lock_path,
        ownership=acquired["ownership"],
    )

    assert first["released"] is True
    _assert_blocked(second, "LOCK_OWNERSHIP_LOST")
    assert second["released"] is False
    assert lock_path.exists() is False


def test_concurrent_release_allows_one_successful_owner_release(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    barrier = threading.Barrier(2)

    def release() -> dict[str, Any]:
        barrier.wait(timeout=5)
        result = release_runner_git_lock(
            lock_path=lock_path,
            ownership=acquired["ownership"],
        )
        assert isinstance(result, dict)
        return result

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result() for future in (
            executor.submit(release),
            executor.submit(release),
        )]

    assert sum(result.get("released") is True for result in results) == 1
    assert sum(result.get("reason_code") == "LOCK_OWNERSHIP_LOST" for result in results) == 1
    assert lock_path.exists() is False


def test_old_owner_never_deletes_replaced_lock(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    replacement = build_runner_git_lock_payload(
        run_id=_FOREIGN_OWNER_ID,
        pid=_FOREIGN_PID,
        started_at_utc=_REFERENCE_NOW,
    )
    _write_lock(lock_path, replacement)

    released = release_runner_git_lock(
        lock_path=lock_path,
        ownership=acquired["ownership"],
    )

    _assert_blocked(released, "LOCK_OWNERSHIP_LOST")
    assert released["released"] is False
    assert _read_lock(lock_path) == replacement


def test_concurrent_stale_reclaimers_never_delete_winner_lock(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _reference_payload(
        run_id="stale-owner",
        pid=_STALE_PID,
        started_at_utc="2026-09-04T16:00:00Z",
    )
    _write_lock(lock_path, stale)
    barrier = threading.Barrier(2)

    def reclaim(index: int) -> dict[str, Any]:
        barrier.wait(timeout=5)
        return _acquire(
            lock_path,
            run_id=f"reclaimer-{index}",
            pid=4200 + index,
            started_at_utc=_REFERENCE_NOW,
            now_utc=_REFERENCE_NOW,
            is_pid_alive=lambda _: False,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reclaim, (1, 2)))

    winners = [result for result in results if result.get("ok") is True]
    losers = [result for result in results if result.get("ok") is not True]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0]["reason_code"] in {"LOCK_ACTIVE", "LOCK_RECLAIM_RACE"}
    persisted = _read_lock(lock_path)
    assert persisted == winners[0]["ownership"]
    assert persisted["run_id"] != losers[0].get("ownership", {}).get("run_id")


def test_verify_ownership_fails_on_missing_and_invalid_file(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    ownership = _reference_payload()

    missing = verify_runner_git_lock_ownership(lock_path=lock_path, ownership=ownership)
    lock_path.write_text("{not-json", encoding="utf-8")
    invalid = verify_runner_git_lock_ownership(lock_path=lock_path, ownership=ownership)

    _assert_blocked(missing, "LOCK_OWNERSHIP_LOST")
    _assert_blocked(invalid, "LOCK_INVALID")


def test_release_reports_unlink_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    ownership = _reference_payload()
    _write_lock(lock_path, ownership)
    monkeypatch.setattr(
        Path,
        "unlink",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("busy")),
    )

    released = release_runner_git_lock(lock_path=lock_path, ownership=ownership)

    _assert_blocked(released, "LOCK_RELEASE_FAILED")
    assert released["released"] is False


def test_resolve_uses_explicit_config_roots_not_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path)
    other_cwd = tmp_path / "unrelated-cwd"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)

    context = resolve_runner_git_context(target_worktree=target, runtime_root=runtime)

    assert Path(context["target_worktree"]).resolve() == target.resolve()
    assert Path(context["git_dir"]).resolve() == _absolute_git_dir(target)
    assert Path(context["git_common_dir"]).resolve() == _absolute_git_common_dir(target)
    assert Path(context["lock_path"]).resolve() == (runtime / _LOCK_NAME).resolve()
    assert Path(context["git_dir"]).resolve() == (target / ".git").resolve()


def test_resolve_rejects_non_git_target(tmp_path: Path) -> None:
    target = tmp_path / "not-a-repository"
    target.mkdir()
    runtime = _runtime_root(tmp_path)

    with pytest.raises(RunnerGitContextError) as exc_info:
        resolve_runner_git_context(target_worktree=target, runtime_root=runtime)

    assert exc_info.value.reason_code == "GIT_TARGET_INVALID"


def test_resolve_rejects_nested_path_that_is_not_worktree_root(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    nested = target / "nested"
    nested.mkdir()
    runtime = _runtime_root(tmp_path)

    with pytest.raises(RunnerGitContextError) as exc_info:
        resolve_runner_git_context(target_worktree=nested, runtime_root=runtime)

    assert exc_info.value.reason_code == "GIT_TARGET_NOT_WORKTREE_ROOT"


def test_resolve_rejects_missing_runtime_root(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    missing_runtime = tmp_path / "missing-runtime"

    with pytest.raises(RunnerGitContextError) as exc_info:
        resolve_runner_git_context(
            target_worktree=target,
            runtime_root=missing_runtime,
        )

    assert exc_info.value.reason_code == "RUNTIME_ROOT_INVALID"


def test_resolve_rejects_runtime_root_file(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime_file = tmp_path / "runtime-as-file"
    runtime_file.write_text("not-a-directory\n", encoding="utf-8")

    with pytest.raises(RunnerGitContextError) as exc_info:
        resolve_runner_git_context(
            target_worktree=target,
            runtime_root=runtime_file,
        )

    assert exc_info.value.reason_code == "RUNTIME_ROOT_INVALID"


def test_linked_worktree_keeps_worktree_common_dir_and_target_distinct(
    tmp_path: Path,
) -> None:
    primary = _init_target_repo(tmp_path, name="primary-repo")
    linked = _add_linked_worktree(primary, tmp_path / "linked-target")
    runtime = _runtime_root(tmp_path)

    context = resolve_runner_git_context(target_worktree=linked, runtime_root=runtime)

    git_dir = Path(context["git_dir"]).resolve()
    git_common_dir = Path(context["git_common_dir"]).resolve()
    target_worktree = Path(context["target_worktree"]).resolve()
    assert target_worktree == linked.resolve()
    assert git_dir == _absolute_git_dir(linked)
    assert git_common_dir == _absolute_git_common_dir(linked)
    assert git_common_dir == _absolute_git_common_dir(primary)
    assert git_dir != git_common_dir
    assert target_worktree != git_common_dir
    assert (linked / ".git").is_file()
    assert git_dir != (linked / ".git").resolve()
    assert Path(context["lock_path"]).resolve() == (runtime / _LOCK_NAME).resolve()


def test_preflight_accepts_clean_configured_main(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    assert result["ok"] is True
    assert result["reason_code"] == "GIT_PREFLIGHT_OK"
    assert result["branch"] == _REFERENCE_MAIN_BRANCH
    assert result["head_sha"] == _run_git(target, "rev-parse", "HEAD")
    assert len(result["head_sha"]) == 40


def test_preflight_does_not_use_process_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _init_target_repo(tmp_path)
    other = _init_target_repo(tmp_path, name="other-repo")
    _run_git(other, "checkout", "-b", "other-main")
    monkeypatch.chdir(other)

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    assert result["ok"] is True
    assert result["branch"] == _REFERENCE_MAIN_BRANCH
    assert result["head_sha"] == _run_git(target, "rev-parse", "HEAD")


@pytest.mark.parametrize(
    "dirty",
    ("unstaged", "staged", "untracked"),
)
def test_preflight_blocks_dirty_target(tmp_path: Path, dirty: str) -> None:
    target = _init_target_repo(tmp_path)
    if dirty == "unstaged":
        (target / "tracked.txt").write_text("unstaged dirty\n", encoding="utf-8")
    elif dirty == "staged":
        (target / "tracked.txt").write_text("staged dirty\n", encoding="utf-8")
        _run_git(target, "add", "--", "tracked.txt")
    else:
        (target / "untracked.txt").write_text("untracked dirty\n", encoding="utf-8")

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    _assert_blocked(result, "GIT_DIRTY")


def test_preflight_treats_ignored_only_paths_as_clean(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    (target / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
    _run_git(target, "add", "--", ".gitignore")
    _run_git(target, "commit", "-m", "ignore ignored.txt")
    (target / "ignored.txt").write_text("ignored only\n", encoding="utf-8")

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    assert result["ok"] is True
    assert result["reason_code"] == "GIT_PREFLIGHT_OK"
    assert result["branch"] == _REFERENCE_MAIN_BRANCH


def test_preflight_blocks_detached_head(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    head_sha = _run_git(target, "rev-parse", "HEAD")
    _run_git(target, "checkout", "--detach", head_sha)

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    _assert_blocked(result, "GIT_DETACHED_HEAD")


def test_preflight_blocks_wrong_named_branch(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    _run_git(target, "checkout", "-b", "feature/synthetic-lock-preflight")

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    blocked = _assert_blocked(result, "GIT_WRONG_BRANCH")
    assert blocked["branch"] == "feature/synthetic-lock-preflight"


@pytest.mark.parametrize(
    ("sentinel_name", "reason_code"),
    (
        ("MERGE_HEAD", "GIT_MERGE_IN_PROGRESS"),
        ("rebase-merge/head-name", "GIT_REBASE_IN_PROGRESS"),
        ("rebase-apply/rebasing", "GIT_REBASE_IN_PROGRESS"),
        ("CHERRY_PICK_HEAD", "GIT_CHERRY_PICK_IN_PROGRESS"),
        ("REVERT_HEAD", "GIT_REVERT_IN_PROGRESS"),
        ("BISECT_LOG", "GIT_BISECT_IN_PROGRESS"),
        ("sequencer/todo", "GIT_SEQUENCER_IN_PROGRESS"),
    ),
)
def test_preflight_blocks_in_progress_git_operations(
    tmp_path: Path,
    sentinel_name: str,
    reason_code: str,
) -> None:
    target = _init_target_repo(tmp_path)
    git_dir = _absolute_git_dir(target)
    sentinel = git_dir / sentinel_name
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("in-progress\n", encoding="utf-8")

    result = evaluate_branch_preflight(
        target_worktree=target,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    _assert_blocked(result, reason_code)


def test_linked_worktree_preflight_accepts_clean_main(tmp_path: Path) -> None:
    primary = _init_target_repo(tmp_path, name="primary-repo")
    linked = _add_linked_worktree(primary, tmp_path / "linked-target")

    result = evaluate_branch_preflight(
        target_worktree=linked,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    assert result["ok"] is True
    assert result["reason_code"] == "GIT_PREFLIGHT_OK"
    assert result["branch"] == _REFERENCE_MAIN_BRANCH
    assert result["head_sha"] == _run_git(linked, "rev-parse", "HEAD")


def test_linked_worktree_preflight_blocks_dirty_linked_target(tmp_path: Path) -> None:
    primary = _init_target_repo(tmp_path, name="primary-repo")
    linked = _add_linked_worktree(primary, tmp_path / "linked-target")
    (linked / "tracked.txt").write_text("linked dirty\n", encoding="utf-8")

    result = evaluate_branch_preflight(
        target_worktree=linked,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    _assert_blocked(result, "GIT_DIRTY")
    clean_primary = evaluate_branch_preflight(
        target_worktree=primary,
        main_branch="anchor-hold",
    )
    assert clean_primary["ok"] is True


def test_linked_worktree_preflight_reads_git_dir_not_dot_git_file(
    tmp_path: Path,
) -> None:
    primary = _init_target_repo(tmp_path, name="primary-repo")
    linked = _add_linked_worktree(primary, tmp_path / "linked-target")
    git_dir = _absolute_git_dir(linked)
    (git_dir / "MERGE_HEAD").write_text("in-progress\n", encoding="utf-8")
    assert (linked / ".git").is_file()
    assert (linked / ".git" / "MERGE_HEAD").exists() is False

    result = evaluate_branch_preflight(
        target_worktree=linked,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )

    _assert_blocked(result, "GIT_MERGE_IN_PROGRESS")


def test_preflight_on_linked_worktree_ignores_primary_merge_sentinel(
    tmp_path: Path,
) -> None:
    primary = _init_target_repo(tmp_path, name="primary-repo")
    linked = _add_linked_worktree(primary, tmp_path / "linked-target")
    primary_git_dir = _absolute_git_dir(primary)
    (primary_git_dir / "MERGE_HEAD").write_text("primary-merge\n", encoding="utf-8")

    linked_result = evaluate_branch_preflight(
        target_worktree=linked,
        main_branch=_REFERENCE_MAIN_BRANCH,
    )
    primary_result = evaluate_branch_preflight(
        target_worktree=primary,
        main_branch="anchor-hold",
    )

    assert linked_result["ok"] is True
    assert linked_result["reason_code"] == "GIT_PREFLIGHT_OK"
    _assert_blocked(primary_result, "GIT_MERGE_IN_PROGRESS")


def test_stale_reclaim_maps_enoent_before_reclaim_open_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    original_open = os.open
    vanished = {"happened": False}

    def vanish_existing_lock_before_reopen(
        path: str | bytes | os.PathLike[str],
        flags: int,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        if Path(os.fspath(path)).resolve() == lock_path.resolve() and not (flags & os.O_CREAT):
            vanished["happened"] = True
            lock_path.unlink(missing_ok=True)
            raise FileNotFoundError(os.fspath(path))
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", vanish_existing_lock_before_reopen)

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert vanished["happened"] is True
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_OWNERSHIP_LOST", "LOCK_RECLAIM_RACE"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert blocked.get("acquired") is False
    assert lock_path.exists() is False
    assert _LOCK_NAME not in _lock_dir_names(lock_path)


@pytest.mark.parametrize("fault", ("open", "flock"))
def test_stale_reclaim_maps_open_or_flock_oserror_and_preserves_stale_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    faulted = {"happened": False}

    if fault == "open":
        original_open = os.open

        def fail_reclaim_open(
            path: str | bytes | os.PathLike[str],
            flags: int,
            *args: Any,
            **kwargs: Any,
        ) -> int:
            if Path(os.fspath(path)).resolve() == lock_path.resolve() and not (
                flags & os.O_CREAT
            ):
                faulted["happened"] = True
                raise PermissionError("reclaim open denied")
            return original_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(os, "open", fail_reclaim_open)
    else:

        def fail_flock(*_args: Any, **_kwargs: Any) -> None:
            faulted["happened"] = True
            raise OSError("flock failed")

        monkeypatch.setattr(fcntl, "flock", fail_flock)

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert faulted["happened"] is True
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_FAILED", "LOCK_ACQUIRE_FAILED"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert blocked.get("acquired") is False
    _assert_only_named_lock(lock_path, stale)


@pytest.mark.parametrize("fault", ("open", "flock"))
def test_release_maps_open_or_flock_oserror_and_preserves_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    ownership = _reference_payload()
    _write_lock(lock_path, ownership)
    original_lock_bytes = lock_path.read_bytes()
    faulted = {"happened": False}

    if fault == "open":
        original_open = os.open

        def fail_release_open(
            path: str | bytes | os.PathLike[str],
            flags: int,
            *args: Any,
            **kwargs: Any,
        ) -> int:
            if Path(os.fspath(path)).resolve() == lock_path.resolve() and not (
                flags & os.O_CREAT
            ):
                faulted["happened"] = True
                raise PermissionError("release open denied")
            return original_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(os, "open", fail_release_open)
    else:

        def fail_flock(*_args: Any, **_kwargs: Any) -> None:
            faulted["happened"] = True
            raise OSError("release flock failed")

        monkeypatch.setattr(fcntl, "flock", fail_flock)

    released = release_runner_git_lock(lock_path=lock_path, ownership=ownership)

    assert faulted["happened"] is True
    _assert_blocked(released, "LOCK_RELEASE_FAILED")
    assert released["released"] is False
    assert lock_path.read_bytes() == original_lock_bytes
    _assert_only_named_lock(lock_path, ownership)


def test_stale_reclaim_preserves_contender_created_after_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    winner = _reference_payload(
        run_id=_FOREIGN_OWNER_ID,
        pid=_FOREIGN_PID,
        started_at_utc=_REFERENCE_NOW,
    )
    _write_lock(lock_path, stale)
    original_unlink = Path.unlink
    planted = {"happened": False}

    def unlink_then_plant_contender(self: Path, *args: Any, **kwargs: Any) -> None:
        original_unlink(self, *args, **kwargs)
        if self.resolve() == lock_path.resolve():
            planted["happened"] = True
            _write_lock(lock_path, winner)

    monkeypatch.setattr(Path, "unlink", unlink_then_plant_contender)

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert planted["happened"] is True
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_RACE", "LOCK_ACTIVE"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert blocked.get("acquired") is False
    _assert_only_named_lock(lock_path, winner)


def test_stale_reclaim_maps_generic_create_oserror_after_unlink_without_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    original_open = os.open
    unlinked = {"happened": False}
    original_unlink = Path.unlink

    def track_stale_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        original_unlink(self, *args, **kwargs)
        if self.resolve() == lock_path.resolve():
            unlinked["happened"] = True

    def fail_exclusive_create(
        path: str | bytes | os.PathLike[str],
        flags: int,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        if Path(os.fspath(path)).resolve() == lock_path.resolve() and flags & os.O_EXCL:
            raise OSError("no space")
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", track_stale_unlink)
    monkeypatch.setattr(os, "open", fail_exclusive_create)

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert unlinked["happened"] is True
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_FAILED", "LOCK_ACQUIRE_FAILED"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert blocked.get("acquired") is False
    assert lock_path.exists() is False
    assert _LOCK_NAME not in _lock_dir_names(lock_path)


@pytest.mark.parametrize("fault", ("dump", "fsync"))
def test_stale_reclaim_write_failure_maps_stable_code_and_removes_partial_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    exclusive_create = {"happened": False}
    original_open = os.open

    def tracked_open(
        path: str | bytes | os.PathLike[str],
        flags: int,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        descriptor = original_open(path, flags, *args, **kwargs)
        if Path(os.fspath(path)).resolve() == lock_path.resolve() and flags & os.O_EXCL:
            exclusive_create["happened"] = True
        return descriptor

    monkeypatch.setattr(os, "open", tracked_open)
    if fault == "dump":
        monkeypatch.setattr(
            json,
            "dump",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("write failed")),
        )
    else:
        monkeypatch.setattr(
            os,
            "fsync",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("fsync failed")),
        )

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert exclusive_create["happened"] is True
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_FAILED", "LOCK_ACQUIRE_FAILED"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert blocked.get("acquired") is False
    assert lock_path.exists() is False
    assert _LOCK_NAME not in _lock_dir_names(lock_path)


def test_stale_reclaim_write_failure_cleanup_unlink_error_is_stable_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)
    exclusive_create = {"happened": False}
    original_open = os.open
    original_unlink = Path.unlink
    unlink_calls = {"count": 0}
    new_ownership = _reference_payload(started_at_utc=_REFERENCE_NOW)

    def tracked_open(
        path: str | bytes | os.PathLike[str],
        flags: int,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        descriptor = original_open(path, flags, *args, **kwargs)
        if Path(os.fspath(path)).resolve() == lock_path.resolve() and flags & os.O_EXCL:
            exclusive_create["happened"] = True
        return descriptor

    def unlink_then_fail_cleanup(self: Path, *args: Any, **kwargs: Any) -> None:
        if self.resolve() != lock_path.resolve():
            original_unlink(self, *args, **kwargs)
            return
        unlink_calls["count"] += 1
        if unlink_calls["count"] == 1:
            original_unlink(self, *args, **kwargs)
            return
        raise OSError("cleanup failed")

    monkeypatch.setattr(os, "open", tracked_open)
    monkeypatch.setattr(Path, "unlink", unlink_then_fail_cleanup)
    monkeypatch.setattr(
        json,
        "dump",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("write failed")),
    )

    blocked = _acquire(
        lock_path,
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    assert exclusive_create["happened"] is True
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {"LOCK_RECLAIM_FAILED", "LOCK_ACQUIRE_FAILED"}
    assert "fail" in str(blocked.get("status", "fail-closed")).lower()
    assert blocked.get("acquired") is not True
    assert blocked.get("reclaimed_stale_lock") is not True
    if lock_path.exists() and lock_path.is_file():
        try:
            persisted = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            persisted = None
        assert persisted != new_ownership
    else:
        assert _LOCK_NAME not in _lock_dir_names(lock_path)


def test_release_does_not_unlink_ownership_replaced_after_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = _lock_path(tmp_path)
    acquired = _acquire(lock_path)
    replacement = build_runner_git_lock_payload(
        run_id=_FOREIGN_OWNER_ID,
        pid=_FOREIGN_PID,
        started_at_utc=_REFERENCE_NOW,
    )
    original_read_text = Path.read_text

    def read_then_replace_ownership(self: Path, *args: Any, **kwargs: Any) -> str:
        text = original_read_text(self, *args, **kwargs)
        if self.resolve() == lock_path.resolve():
            _write_lock(lock_path, replacement)
        return text

    monkeypatch.setattr(Path, "read_text", read_then_replace_ownership)

    released = release_runner_git_lock(
        lock_path=lock_path,
        ownership=acquired["ownership"],
    )

    _assert_blocked(released, "LOCK_OWNERSHIP_LOST")
    assert released["released"] is False
    assert json.loads(original_read_text(lock_path, encoding="utf-8")) == replacement
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}


@pytest.mark.parametrize(
    "invalid_kwargs",
    (
        {"run_id": ""},
        {"run_id": 1},
        {"run_id": " "},
        {"pid": 0},
        {"pid": -1},
        {"pid": "4242"},
        {"pid": True},
        {"started_at_utc": ""},
        {"started_at_utc": "not-a-timestamp"},
        {"started_at_utc": "2026-09-04T17:00:00+00:00"},
    ),
)
def test_acquire_rejects_invalid_new_ownership_without_persisting_lock(
    tmp_path: Path,
    invalid_kwargs: dict[str, Any],
) -> None:
    lock_path = _lock_path(tmp_path)

    blocked = _acquire(lock_path, **invalid_kwargs)

    _assert_blocked(blocked, "LOCK_INVALID")
    assert blocked.get("acquired") is False
    assert lock_path.exists() is False
    assert _LOCK_NAME not in _lock_dir_names(lock_path)


def test_stale_reclaim_rejects_invalid_new_ownership_without_persisting_it(
    tmp_path: Path,
) -> None:
    lock_path = _lock_path(tmp_path)
    stale = _stale_payload()
    _write_lock(lock_path, stale)

    blocked = _acquire(
        lock_path,
        run_id="",
        started_at_utc=_REFERENCE_NOW,
        now_utc=_REFERENCE_NOW,
        is_pid_alive=lambda _: False,
    )

    _assert_blocked(blocked, "LOCK_INVALID")
    assert blocked.get("acquired") is False
    _assert_only_named_lock(lock_path, stale)


@pytest.mark.parametrize(
    "raw",
    (
        "[]",
        "null",
        "1",
        '"scalar"',
        "true",
        "{}",
    ),
)
def test_verify_classifies_non_mapping_or_empty_lock_as_invalid_not_lost(
    tmp_path: Path,
    raw: str,
) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.write_text(raw, encoding="utf-8")

    result = verify_runner_git_lock_ownership(
        lock_path=lock_path,
        ownership=_reference_payload(),
    )

    _assert_blocked(result, "LOCK_INVALID")
    assert lock_path.read_text(encoding="utf-8") == raw
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}


def test_verify_rejects_schema_invalid_document_even_when_bytes_match(
    tmp_path: Path,
) -> None:
    lock_path = _lock_path(tmp_path)
    invalid = _reference_payload(run_id="")
    _write_lock(lock_path, invalid)

    result = verify_runner_git_lock_ownership(
        lock_path=lock_path,
        ownership=invalid,
    )

    _assert_blocked(result, "LOCK_INVALID")
    assert json.loads(lock_path.read_text(encoding="utf-8")) == invalid
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}


def test_verify_rejects_matching_document_with_unknown_field(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    invalid = {**_reference_payload(), "orphan_field": "not-in-schema"}
    _write_lock(lock_path, invalid)

    result = verify_runner_git_lock_ownership(
        lock_path=lock_path,
        ownership=invalid,
    )

    _assert_blocked(result, "LOCK_INVALID")
    assert json.loads(lock_path.read_text(encoding="utf-8")) == invalid
    assert _lock_dir_names(lock_path) == {_LOCK_NAME}


def test_preflight_maps_non_repository_git_errors_fail_closed(tmp_path: Path) -> None:
    target = tmp_path / "not-a-repository"
    target.mkdir()

    try:
        result = evaluate_branch_preflight(
            target_worktree=target,
            main_branch=_REFERENCE_MAIN_BRANCH,
        )
    except RunnerGitContextError as exc:
        assert exc.reason_code == "GIT_TARGET_INVALID"
        return
    _assert_blocked(result, "GIT_TARGET_INVALID")


def test_preflight_maps_non_worktree_root_git_errors_fail_closed(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    nested = target / "nested"
    nested.mkdir()

    try:
        result = evaluate_branch_preflight(
            target_worktree=nested,
            main_branch=_REFERENCE_MAIN_BRANCH,
        )
    except RunnerGitContextError as exc:
        assert exc.reason_code == "GIT_TARGET_NOT_WORKTREE_ROOT"
        return
    _assert_blocked(result, "GIT_TARGET_NOT_WORKTREE_ROOT")
