"""Local single-writer Git lock and target preflight.

The lock is a local coordination file under an explicit runtime root. It
does not search parent trees, claim host isolation, or decide promotion.
"""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

_LOCK_NAME = "runner_git.lock"
_SCHEMA_VERSION = "1.0"
_LOCK_REQUIRED_FIELDS = (
    "schema_version",
    "lock_name",
    "run_id",
    "pid",
    "started_at_utc",
)
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_OPERATION_SENTINELS = (
    ("MERGE_HEAD", "GIT_MERGE_IN_PROGRESS"),
    ("rebase-merge/head-name", "GIT_REBASE_IN_PROGRESS"),
    ("rebase-apply/rebasing", "GIT_REBASE_IN_PROGRESS"),
    ("CHERRY_PICK_HEAD", "GIT_CHERRY_PICK_IN_PROGRESS"),
    ("REVERT_HEAD", "GIT_REVERT_IN_PROGRESS"),
    ("BISECT_LOG", "GIT_BISECT_IN_PROGRESS"),
    ("sequencer/todo", "GIT_SEQUENCER_IN_PROGRESS"),
)


class RunnerGitContextError(Exception):
    """Fail-closed local Git-context or runtime-root violation."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def resolve_runner_git_context(
    *,
    target_worktree: Path | str,
    runtime_root: Path | str,
) -> dict[str, str]:
    """Resolve explicit target worktree and runtime roots for the local lock."""
    target = Path(target_worktree).resolve()
    runtime = Path(runtime_root).resolve()
    if not runtime.is_dir():
        raise RunnerGitContextError("RUNTIME_ROOT_INVALID")
    _require_worktree_root(target)
    git_dir = Path(_run_git(target, "rev-parse", "--absolute-git-dir")).resolve()
    git_common_dir = _resolve_git_common_dir(target)
    return {
        "target_worktree": str(target),
        "git_dir": str(git_dir),
        "git_common_dir": str(git_common_dir),
        "lock_path": str(runtime / _LOCK_NAME),
    }


def build_runner_git_lock_payload(
    *,
    run_id: str,
    pid: int,
    started_at_utc: str,
) -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "lock_name": _LOCK_NAME,
        "run_id": run_id,
        "pid": pid,
        "started_at_utc": started_at_utc,
    }


def evaluate_runner_git_lock(
    *,
    lock_path: Path | str,
    now_utc: str,
    stale_after_seconds: int,
    is_pid_alive: Callable[[int], bool],
) -> dict[str, Any]:
    path = Path(lock_path)
    if not path.exists():
        return {"ok": True, "reason_code": "LOCK_NONE", "stale": False}
    if not path.is_file():
        return _blocked("LOCK_INVALID")
    loaded = _load_lock_mapping(path)
    if loaded is None:
        return _blocked("LOCK_INVALID")
    payload = _validated_lock_payload(loaded)
    if payload is None:
        return _blocked("LOCK_INVALID")
    try:
        started_at = _parse_utc_timestamp(payload["started_at_utc"])
        now = _parse_utc_timestamp(now_utc)
        pid_alive = is_pid_alive(int(payload["pid"]))
    except Exception:
        return _blocked("LOCK_INVALID")
    age_seconds = (now - started_at).total_seconds()
    if (not pid_alive) and age_seconds > stale_after_seconds:
        return {
            "ok": True,
            "reason_code": "LOCK_STALE_RECLAIMABLE",
            "stale": True,
            "pid_alive": False,
            "lock": payload,
        }
    return _blocked(
        "LOCK_ACTIVE",
        stale=False,
        pid_alive=pid_alive,
        lock=payload,
    )


def acquire_runner_git_lock(
    *,
    lock_path: Path | str,
    run_id: str,
    pid: int,
    started_at_utc: str,
    now_utc: str,
    stale_after_seconds: int,
    is_pid_alive: Callable[[int], bool],
) -> dict[str, Any]:
    """Acquire the local single-writer lock. Same-owner acquire is not reentrant."""
    path = Path(lock_path)
    ownership = build_runner_git_lock_payload(
        run_id=run_id,
        pid=pid,
        started_at_utc=started_at_utc,
    )
    if _validated_lock_payload(ownership) is None:
        return _blocked("LOCK_INVALID", acquired=False)
    gate = evaluate_runner_git_lock(
        lock_path=path,
        now_utc=now_utc,
        stale_after_seconds=stale_after_seconds,
        is_pid_alive=is_pid_alive,
    )
    if gate.get("reason_code") == "LOCK_NONE":
        created = _create_lock_exclusive(path=path, ownership=ownership)
        if created is True:
            return _lock_acquired(ownership=ownership, reclaimed_stale_lock=False)
        if created is not False:
            return created
        return _blocked("LOCK_ACTIVE", acquired=False)
    if _acquisition_allows_reclaim(gate):
        return _reclaim_stale_lock(
            lock_path=path,
            new_ownership=ownership,
            stale_ownership=gate["lock"],
            now_utc=now_utc,
            stale_after_seconds=stale_after_seconds,
            is_pid_alive=is_pid_alive,
        )
    blocked = dict(gate)
    blocked["acquired"] = False
    return blocked


def release_runner_git_lock(
    *,
    lock_path: Path | str,
    ownership: dict[str, Any],
) -> dict[str, Any]:
    """Release the local single-writer lock only when ownership matches exactly."""
    path = Path(lock_path)
    try:
        descriptor = os.open(path, os.O_RDWR)
    except FileNotFoundError:
        return _blocked("LOCK_OWNERSHIP_LOST", released=False)
    except OSError:
        return _blocked("LOCK_RELEASE_FAILED", released=False)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError:
            return _blocked("LOCK_RELEASE_FAILED", released=False)
        ownership_gate = verify_runner_git_lock_ownership(
            lock_path=path,
            ownership=ownership,
        )
        if ownership_gate.get("ok") is not True:
            released = dict(ownership_gate)
            released["released"] = False
            return released
        try:
            path.unlink()
        except OSError:
            return _blocked("LOCK_RELEASE_FAILED", released=False)
        return {
            "ok": True,
            "status": "released",
            "reason_code": "LOCK_OK",
            "released": True,
        }
    finally:
        os.close(descriptor)


def verify_runner_git_lock_ownership(
    *,
    lock_path: Path | str,
    ownership: dict[str, Any],
) -> dict[str, Any]:
    path = Path(lock_path)
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        confirmed = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _blocked("LOCK_OWNERSHIP_LOST")
    except (OSError, UnicodeError, json.JSONDecodeError):
        return _blocked("LOCK_INVALID")
    if loaded != confirmed:
        return _blocked("LOCK_OWNERSHIP_LOST")
    if not isinstance(loaded, dict) or _validated_lock_payload(loaded) is None:
        return _blocked("LOCK_INVALID")
    if loaded != ownership:
        return _blocked("LOCK_OWNERSHIP_LOST")
    return {
        "ok": True,
        "status": "owned",
        "reason_code": "LOCK_OK",
        "owned": True,
        "ownership": ownership,
    }


def evaluate_branch_preflight(
    *,
    target_worktree: Path | str,
    main_branch: str,
) -> dict[str, Any]:
    target = Path(target_worktree).resolve()
    try:
        _require_worktree_root(target)
    except RunnerGitContextError as exc:
        return _blocked(exc.reason_code)
    git_dir = Path(_run_git(target, "rev-parse", "--absolute-git-dir")).resolve()
    for relative_sentinel, reason_code in _OPERATION_SENTINELS:
        if (git_dir / relative_sentinel).exists():
            return _blocked(reason_code)
    branch = _run_git(target, "rev-parse", "--abbrev-ref", "HEAD")
    if branch == "HEAD":
        return _blocked("GIT_DETACHED_HEAD")
    if branch != main_branch:
        return _blocked("GIT_WRONG_BRANCH", branch=branch)
    status = _run_git(target, "status", "--porcelain")
    if status:
        return _blocked("GIT_DIRTY")
    head_sha = _run_git(target, "rev-parse", "HEAD")
    return {
        "ok": True,
        "reason_code": "GIT_PREFLIGHT_OK",
        "branch": branch,
        "head_sha": head_sha,
    }


def _reclaim_stale_lock(
    *,
    lock_path: Path,
    new_ownership: dict[str, Any],
    stale_ownership: dict[str, Any],
    now_utc: str,
    stale_after_seconds: int,
    is_pid_alive: Callable[[int], bool],
) -> dict[str, Any]:
    try:
        descriptor = os.open(lock_path, os.O_RDWR)
    except FileNotFoundError:
        return _blocked("LOCK_RECLAIM_RACE", acquired=False)
    except OSError:
        return _blocked("LOCK_RECLAIM_FAILED", acquired=False)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError:
            return _blocked("LOCK_RECLAIM_FAILED", acquired=False)
        gate = evaluate_runner_git_lock(
            lock_path=lock_path,
            now_utc=now_utc,
            stale_after_seconds=stale_after_seconds,
            is_pid_alive=is_pid_alive,
        )
        if not _acquisition_allows_reclaim(gate):
            blocked = dict(gate)
            blocked["ok"] = False
            blocked["status"] = "fail-closed"
            blocked["acquired"] = False
            return blocked
        current = _load_lock_mapping(lock_path)
        if current != stale_ownership:
            return _blocked("LOCK_RECLAIM_RACE", acquired=False)
        try:
            lock_path.unlink()
        except OSError:
            return _blocked("LOCK_RECLAIM_FAILED", acquired=False)
        written = _create_lock_exclusive(path=lock_path, ownership=new_ownership)
        if written is True:
            return _lock_acquired(
                ownership=new_ownership,
                reclaimed_stale_lock=True,
                stale_ownership=stale_ownership,
            )
        if written is False:
            return _blocked("LOCK_RECLAIM_RACE", acquired=False)
        return written
    finally:
        os.close(descriptor)


def _create_lock_exclusive(*, path: Path, ownership: dict[str, Any]) -> bool | dict[str, Any]:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    except OSError:
        return _blocked("LOCK_ACQUIRE_FAILED", acquired=False)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(ownership, handle)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return _blocked("LOCK_ACQUIRE_FAILED", acquired=False)
    return True


def _lock_acquired(
    *,
    ownership: dict[str, Any],
    reclaimed_stale_lock: bool,
    stale_ownership: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "reason_code": "LOCK_OK",
        "acquired": True,
        "ownership": ownership,
        "reclaimed_stale_lock": reclaimed_stale_lock,
        "stale_ownership": stale_ownership,
    }


def _acquisition_allows_reclaim(gate: dict[str, Any]) -> bool:
    return gate.get("reason_code") == "LOCK_STALE_RECLAIMABLE" or (
        gate.get("reason_code") == "LOCK_ACTIVE"
        and gate.get("pid_alive") is False
    )


def _load_lock_mapping(path: Path) -> dict[str, Any] | None:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(loaded, dict):
        return None
    return loaded


def _validated_lock_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    if tuple(sorted(payload)) != tuple(sorted(_LOCK_REQUIRED_FIELDS)):
        return None
    schema_version = payload.get("schema_version")
    lock_name = payload.get("lock_name")
    run_id = payload.get("run_id")
    pid = payload.get("pid")
    started_at_utc = payload.get("started_at_utc")
    if schema_version != _SCHEMA_VERSION:
        return None
    if lock_name != _LOCK_NAME:
        return None
    if type(run_id) is not str or run_id.strip() == "":
        return None
    if type(pid) is not int or pid <= 0:
        return None
    if type(started_at_utc) is not str:
        return None
    try:
        _parse_utc_timestamp(started_at_utc)
    except ValueError:
        return None
    return {
        "schema_version": schema_version,
        "lock_name": lock_name,
        "run_id": run_id,
        "pid": pid,
        "started_at_utc": started_at_utc,
    }


def _parse_utc_timestamp(value: str) -> datetime:
    parsed = datetime.strptime(value, _TIMESTAMP_FORMAT)
    return parsed.replace(tzinfo=timezone.utc)


def _require_worktree_root(target: Path) -> None:
    try:
        toplevel = Path(_run_git(target, "rev-parse", "--show-toplevel")).resolve()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RunnerGitContextError("GIT_TARGET_INVALID") from exc
    if toplevel != target:
        raise RunnerGitContextError("GIT_TARGET_NOT_WORKTREE_ROOT")


def _resolve_git_common_dir(worktree: Path) -> Path:
    reported = _run_git(worktree, "rev-parse", "--git-common-dir")
    return (worktree / reported).resolve()


def _run_git(worktree: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _blocked(reason_code: str, **extra: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "status": "fail-closed",
        "reason_code": reason_code,
    }
    result.update(extra)
    return result
