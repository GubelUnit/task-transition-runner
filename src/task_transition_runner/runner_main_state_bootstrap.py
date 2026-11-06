"""Public fresh-init D-component bootstrap.

Source authority is proved before any runtime byte. A missing or empty
runtime is initialized only when the configured main ref has no canonical
First-Parent START, TOCK, or bound Promotion-Attempt history. Target
preflight, baseline validation, and head plus source reproof follow.
Source pin, main state, and consumption are then persisted per artifact.
Root overlap, non-directory runtime, runtime occupants, preflight
failures, failed baseline validation, post-validation drift, invalid
timestamps, and caller-supplied authority blocks end fail-closed.
"""

from __future__ import annotations

import os as _os
import re as _re
import shlex as _shlex
import subprocess as _subprocess
from pathlib import Path
from typing import Any

import yaml as _yaml

from task_transition_runner.runner_code_authority import (
    RUNTIME_SOURCE_PIN_NAME,
    RunnerCodeAuthorityError,
    SourceAuthorityReport,
    verify_steady_state_code_authority,
)
from task_transition_runner.runner_git_context import evaluate_branch_preflight
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    parse_canonical_tock,
)

__all__ = (
    "allowed_reason_codes",
    "bootstrap_main_state",
)

_ALLOWED_REASON_CODES: tuple[str, ...] = (
    "BASELINE_VALIDATION_FAILED",
    "BOOTSTRAP_AUTHORITY_BLOCK_INVALID",
    "BOOTSTRAP_HEAD_DRIFT",
    "BOOTSTRAP_OK",
    "BOOTSTRAP_ROOT_OVERLAP",
    "BOOTSTRAP_TIMESTAMP_INVALID",
    "BOOTSTRAP_WRITE_FAILED",
    "GIT_BISECT_IN_PROGRESS",
    "GIT_CHERRY_PICK_IN_PROGRESS",
    "GIT_DETACHED_HEAD",
    "GIT_DIRTY",
    "GIT_MERGE_IN_PROGRESS",
    "GIT_REBASE_IN_PROGRESS",
    "GIT_REVERT_IN_PROGRESS",
    "GIT_SEQUENCER_IN_PROGRESS",
    "GIT_TARGET_INVALID",
    "GIT_TARGET_NOT_WORKTREE_ROOT",
    "GIT_WRONG_BRANCH",
    "RUNTIME_NOT_EMPTY",
    "RUNTIME_PERSISTENCE_FAILED",
    "RUNTIME_PROTOCOL_HISTORY_PRESENT",
    "RUNTIME_SOURCE_IDENTITY_INVALID",
    "SOURCE_AUTHORITY_IMPORT_PROVENANCE_INVALID",
    "SOURCE_AUTHORITY_IMPORT_SHADOWED",
    "SOURCE_AUTHORITY_ROOT_OVERLAP",
    "SOURCE_AUTHORITY_RUNTIME_PIN_MISSING",
    "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH",
    "SOURCE_AUTHORITY_SOURCE_DIRTY",
    "SOURCE_AUTHORITY_SOURCE_ROOT_INVALID",
    "SOURCE_AUTHORITY_SOURCE_SHA_INVALID",
    "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH",
)
_SCHEMA_VERSION = "1.0"
_HISTORY_PRESENT = "RUNTIME_PROTOCOL_HISTORY_PRESENT"
_PERSISTENCE_FAILED = "RUNTIME_PERSISTENCE_FAILED"
_PIN_NAME = RUNTIME_SOURCE_PIN_NAME
_START_SHAPE = (
    r"START \S+ origin-[0-9a-f]{64} attempt-[0-9a-f]{64} "
    r"[0-9a-f]{40}\Z"
)
_ATTEMPT_LINE = _re.compile(r"Promotion-Attempt: attempt-[0-9a-f]{64}\Z")


def allowed_reason_codes() -> tuple[str, ...]:
    """Return the tuple of recognized fresh-init bootstrap reason codes."""
    return _ALLOWED_REASON_CODES


def _blocked(reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "fail-closed",
        "reason_code": reason_code,
    }


def _atomic_write_yaml_file(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        _yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    _os.replace(temp_path, path)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_bytes(payload)
    _os.replace(temp_path, path)


def _git_optional(repo: Path, *args: str) -> str | None:
    completed = _subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout


def _git_required(repo: Path, *args: str) -> str:
    completed = _subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _commit_view(repo: Path, sha: str) -> tuple[str, tuple[str, ...], str]:
    raw = _git_required(repo, "cat-file", "-p", sha)
    header, _separator, message = raw.partition("\n\n")
    tree_oid = ""
    parents: list[str] = []
    for line in header.splitlines():
        kind, _separator, value = line.partition(" ")
        if kind == "tree":
            tree_oid = value
        elif kind == "parent":
            parents.append(value)
    return tree_oid, tuple(parents), message


def _is_canonical_tock(
    message: str,
    parents: tuple[str, ...],
    tree_oid: str,
) -> bool:
    try:
        parse_canonical_tock(
            message,
            parents,
            tree_oid,
            validated_tree_oid=tree_oid,
        )
    except RunnerGitPromotionError:
        return False
    return True


def _is_canonical_start(
    repo: Path,
    message: str,
    parents: tuple[str, ...],
    tree_oid: str,
) -> bool:
    if len(parents) != 1:
        return False
    lines = message.splitlines()
    if not lines:
        return False
    pattern = _re.compile(_START_SHAPE.replace("[0-9a-f]{40}", parents[0]))
    if pattern.fullmatch(lines[0]) is None:
        return False
    parent_tree = _git_required(repo, "rev-parse", f"{parents[0]}^{{tree}}").strip()
    return tree_oid == parent_tree


def _is_bound_attempt(message: str, parents: tuple[str, ...]) -> bool:
    if len(parents) != 1:
        return False
    text = message.strip("\n")
    if "\n" in text:
        return False
    return _ATTEMPT_LINE.fullmatch(text) is not None


def _sha_is_protocol(repo: Path, sha: str) -> bool:
    tree_oid, parents, message = _commit_view(repo, sha)
    if _is_canonical_tock(message, parents, tree_oid):
        return True
    if _is_canonical_start(repo, message, parents, tree_oid):
        return True
    return _is_bound_attempt(message, parents)


def _protocol_history_present(repo: Path, main_branch: str) -> bool:
    raw = _git_optional(repo, "rev-list", "--first-parent", main_branch)
    if raw is None:
        return False
    return any(_sha_is_protocol(repo, sha) for sha in raw.splitlines())


def _prove_source_authority(
    *,
    target_root: Path,
    runtime_root: Path,
    expected_source_sha: str | None = None,
) -> SourceAuthorityReport:
    return verify_steady_state_code_authority(
        entry_point=Path(__file__).resolve(),
        target_root=target_root,
        runtime_root=runtime_root,
        expected_source_sha=expected_source_sha,
    )


def _occupied_runtime_block(
    runtime: Path,
    *,
    target_root: Path,
) -> dict[str, Any] | None:
    if not runtime.exists():
        return None
    if not runtime.is_dir():
        return _blocked("RUNTIME_NOT_EMPTY")
    names = {path.name for path in runtime.iterdir()}
    if not names:
        return None
    if names == {_PIN_NAME}:
        try:
            _prove_source_authority(target_root=target_root, runtime_root=runtime)
        except RunnerCodeAuthorityError as exc:
            return _blocked(exc.reason_code)
        return _blocked("RUNTIME_NOT_EMPTY")
    return _blocked("RUNTIME_NOT_EMPTY")


def _pin_bytes(source_sha: str) -> bytes:
    return (
        f"schema_version: '{_SCHEMA_VERSION}'\n"
        f"source_sha: '{source_sha}'\n"
    ).encode("utf-8")


def bootstrap_main_state(
    *,
    target_worktree: Path,
    runtime_root: Path,
    main_branch: str,
    baseline_validation_command: str,
    baseline_validation_timeout_seconds: int,
    updated_at_utc: str,
    authority_block_reason: str | None = None,
) -> dict[str, Any]:
    """Atomically bootstrap canonical main state and an empty consumption ledger."""
    if not updated_at_utc or not updated_at_utc.strip():
        return _blocked("BOOTSTRAP_TIMESTAMP_INVALID")

    if authority_block_reason is not None:
        if not authority_block_reason or not authority_block_reason.strip():
            return _blocked("BOOTSTRAP_AUTHORITY_BLOCK_INVALID")
        return _blocked(authority_block_reason)

    target = Path(target_worktree)
    runtime = Path(runtime_root)
    target_resolved = target.resolve()
    runtime_resolved = runtime.resolve()

    if (
        runtime_resolved.is_relative_to(target_resolved)
        or target_resolved.is_relative_to(runtime_resolved)
    ):
        return _blocked("BOOTSTRAP_ROOT_OVERLAP")

    occupied = _occupied_runtime_block(runtime, target_root=target_resolved)
    if occupied is not None:
        return occupied

    try:
        initial_authority = _prove_source_authority(
            target_root=target_resolved,
            runtime_root=runtime_resolved,
        )
    except RunnerCodeAuthorityError as exc:
        return _blocked(exc.reason_code)

    if _protocol_history_present(target_resolved, main_branch):
        return _blocked(_HISTORY_PRESENT)

    preflight = evaluate_branch_preflight(
        target_worktree=target_resolved,
        main_branch=main_branch,
    )
    if preflight.get("reason_code") != "GIT_PREFLIGHT_OK":
        return _blocked(str(preflight.get("reason_code")))
    head_sha = str(preflight["head_sha"])

    try:
        completed = _subprocess.run(  # noqa: S603
            _shlex.split(baseline_validation_command),
            cwd=target_resolved,
            timeout=baseline_validation_timeout_seconds,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return _blocked("BASELINE_VALIDATION_FAILED")
    except (ValueError, OSError, _subprocess.TimeoutExpired):
        return _blocked("BASELINE_VALIDATION_FAILED")

    reproof = evaluate_branch_preflight(
        target_worktree=target_resolved,
        main_branch=main_branch,
    )
    if reproof.get("reason_code") != "GIT_PREFLIGHT_OK":
        return _blocked(str(reproof.get("reason_code")))

    if reproof.get("head_sha") != head_sha:
        return _blocked("BOOTSTRAP_HEAD_DRIFT")

    try:
        proved_authority = _prove_source_authority(
            target_root=target_resolved,
            runtime_root=runtime_resolved,
            expected_source_sha=initial_authority.source_sha,
        )
    except RunnerCodeAuthorityError as exc:
        return _blocked(exc.reason_code)

    main_state_path = runtime / "main_state.yaml"
    consumed_path = runtime / "promotion_consumed.yaml"
    pin_path = runtime / _PIN_NAME

    main_state_payload = {
        "schema_version": _SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": main_branch,
        "validated_result_sha": head_sha,
        "last_tock_sha": head_sha,
        "observed_main_head_sha": head_sha,
        "reason_code": "BOOTSTRAP_OK",
        "updated_at_utc": updated_at_utc,
    }

    consumed_payload = {
        "schema_version": _SCHEMA_VERSION,
        "consumed_promotions": [],
    }

    try:
        runtime.mkdir(parents=True, exist_ok=True)
        _atomic_write_yaml_file(main_state_path, main_state_payload)
        _atomic_write_yaml_file(consumed_path, consumed_payload)
    except OSError:
        for tmp_file in runtime.glob("*.tmp"):
            tmp_file.unlink(missing_ok=True)
        main_state_path.unlink(missing_ok=True)
        consumed_path.unlink(missing_ok=True)
        return _blocked("BOOTSTRAP_WRITE_FAILED")

    try:
        _atomic_write_bytes(pin_path, _pin_bytes(proved_authority.source_sha))
    except OSError:
        pin_path.with_suffix(pin_path.suffix + ".tmp").unlink(missing_ok=True)
        return _blocked(_PERSISTENCE_FAILED)

    return {
        "ok": True,
        "status": "bootstrapped",
        "reason_code": "BOOTSTRAP_OK",
        "main_state": main_state_payload,
        "artifacts_written": {
            "main_state": main_state_path,
            "promotion_consumed": consumed_path,
        },
    }
