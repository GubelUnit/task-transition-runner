"""Synthetic public fresh-init D-component contract.

Keep:
- existing Git target, expected Main, clean and operation-free preflight
- missing or empty runtime as the only writable starting states
- real baseline validation against the live Main HEAD
- atomic first write of public ``main_state.yaml`` plus an empty
  consumption ledger
- caller-supplied History-/Source-Authority block as fail-closed lock

Drop:
- second-start of an already managed runtime, migration, and private runtime import
- invented SHA fields and optional ledger initialization
- caller-supplied authority text as a substitute for the real gate

The complete fresh-init contract under test is still red against the
preceding Candidate: source authority, then an empty canonical
First-Parent protocol history, then target preflight, baseline
validation, head and authority reproof, then per-artifact persistence
of state, consumption, and the source pin. A baseline commit is not
protocol history. Side refs do not authorize history. The persisted
main state is the closed ``FRESH_INIT_BASELINE`` and carries no
predecessor, origin, or attempt identity.
"""

from __future__ import annotations

import inspect
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

import task_transition_runner.runner_main_state_bootstrap as bootstrap_module
from task_transition_runner.runner_code_authority import (
    RUNTIME_SOURCE_PIN_NAME,
    RunnerCodeAuthorityError,
    derive_source_root,
    verify_steady_state_code_authority,
)
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    parse_canonical_tock,
)
from task_transition_runner.runner_main_state_bootstrap import (
    allowed_reason_codes,
    bootstrap_main_state,
)
from task_transition_runner.runner_promotion_state import (
    load_and_validate_main_state,
    load_promotion_consumed,
)

_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_UPDATED_AT = "2026-09-11T18:00:00Z"
_GIT_AUTHOR_NAME = "Fresh Init Contract"
_GIT_AUTHOR_EMAIL = "fresh-init@task-transition-runner.invalid"
_MAIN_STATE_NAME = "main_state.yaml"
_CONSUMED_NAME = "promotion_consumed.yaml"
_REQUIRED_EXPORTS = (
    "allowed_reason_codes",
    "bootstrap_main_state",
)
_REQUIRED_REASON_CODES = (
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
_FORBIDDEN_COVERAGE_HARNESS_MARKERS = (
    ".coverage",
    "coverage.Coverage",
    "gc.get_objects",
    "_park_coverage_sidecars",
    "_bind_parallel_coverage_paths",
    "_install_parallel_coverage_path_alias",
)
_BOOTSTRAP_REQUIRED_PARAMETERS = (
    "target_worktree",
    "runtime_root",
    "main_branch",
    "baseline_validation_command",
    "baseline_validation_timeout_seconds",
    "updated_at_utc",
)
_BOOTSTRAP_OPTIONAL_PARAMETERS = ("authority_block_reason",)
_REQUIRED_MAIN_STATE_FIELDS = (
    "schema_version",
    "state",
    "main_branch",
    "validated_result_sha",
    "last_tock_sha",
    "observed_main_head_sha",
    "reason_code",
    "updated_at_utc",
)
_CONFLICT_ARTIFACTS = (
    _MAIN_STATE_NAME,
    _CONSUMED_NAME,
    "pending_promotion.yaml",
    "promotion_journal.yaml",
    "drain_outcome.yaml",
    "runner_source_identity.yaml",
    "runner_git.lock",
)
_SYNTHETIC_TASK_ID = "task-synthetic-fresh-init"
_SYNTHETIC_ORIGIN_RUN_ID = "origin-" + ("cd" * 32)
_SYNTHETIC_ATTEMPT_ID = "attempt-" + ("ab" * 32)
_FOREIGN_SOURCE_SHA = "0123456789abcdef0123456789abcdef01234567"
_DIAGNOSTIC_BACKUP_REF = "refs/heads/runner/backup/synthetic-diagnostic"
_FORBIDDEN_FRESH_BASELINE_KEYS = (
    "predecessor",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "settled",
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


def _init_target_repo(
    tmp_path: Path,
    *,
    name: str = "target-repo",
    branch: str = _REFERENCE_MAIN_BRANCH,
) -> Path:
    repo_root = tmp_path / name
    repo_root.mkdir(parents=True)
    subprocess.run(  # noqa: S603
        ["git", "-c", f"init.defaultBranch={branch}", "init"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    _run_git(repo_root, "config", "user.name", _GIT_AUTHOR_NAME)
    _run_git(repo_root, "config", "user.email", _GIT_AUTHOR_EMAIL)
    (repo_root / "tracked.txt").write_text("fresh baseline\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "tracked.txt")
    _run_git(repo_root, "commit", "-m", "fresh baseline")
    return repo_root


def _runtime_root(tmp_path: Path, *, create: bool = True) -> Path:
    path = tmp_path / "runner-runtime"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _write_validation_command(
    tmp_path: Path,
    *,
    exit_code: int,
    probe: Path | None = None,
    name: str = "baseline-validate.sh",
) -> str:
    script = tmp_path / name
    lines = ["#!/bin/sh", "set -eu"]
    if probe is not None:
        lines.append(f"printf 'validated\\n' > '{probe}'")
    lines.append(f"exit {exit_code}")
    script.write_text("\n".join(lines) + "\n", encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _write_argv_validation_command(tmp_path: Path) -> str:
    script = tmp_path / "baseline-validate-argv.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "probe=''\n"
        "while [ $# -gt 0 ]; do\n"
        "  case \"$1\" in\n"
        "    --probe)\n"
        "      probe=$2\n"
        "      shift 2\n"
        "      ;;\n"
        "    *)\n"
        "      shift\n"
        "      ;;\n"
        "  esac\n"
        "done\n"
        "if [ -n \"$probe\" ]; then\n"
        "  printf 'validated\\n' > \"$probe\"\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _write_hanging_validation_command(tmp_path: Path) -> str:
    script = tmp_path / "baseline-hang.sh"
    script.write_text("#!/bin/sh\nexec sleep 30\n", encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _write_post_validation_dirty_command(tmp_path: Path) -> str:
    script = tmp_path / "baseline-dirty.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "printf 'post-validation dirty\\n' > tracked.txt\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _write_post_validation_head_move_command(tmp_path: Path) -> str:
    script = tmp_path / "baseline-head-move.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "git commit --allow-empty -m 'post-validation head move'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _bootstrap(
    *,
    target_worktree: Path,
    runtime_root: Path,
    main_branch: str = _REFERENCE_MAIN_BRANCH,
    baseline_validation_command: str | None = None,
    baseline_validation_timeout_seconds: int = 30,
    updated_at_utc: str = _REFERENCE_UPDATED_AT,
    authority_block_reason: str | None = None,
    command_root: Path | None = None,
) -> dict[str, Any]:
    command = baseline_validation_command
    if command is None:
        root = target_worktree.parent if command_root is None else command_root
        command = _write_validation_command(root, exit_code=0)
    kwargs: dict[str, Any] = {
        "target_worktree": target_worktree,
        "runtime_root": runtime_root,
        "main_branch": main_branch,
        "baseline_validation_command": command,
        "baseline_validation_timeout_seconds": baseline_validation_timeout_seconds,
        "updated_at_utc": updated_at_utc,
    }
    if authority_block_reason is not None:
        kwargs["authority_block_reason"] = authority_block_reason
    result = bootstrap_main_state(**kwargs)
    assert isinstance(result, dict)
    return result


def _assert_blocked(result: dict[str, Any], reason_code: str) -> dict[str, Any]:
    assert result.get("ok") is False
    assert result.get("status") == "fail-closed"
    assert result.get("reason_code") == reason_code
    return result


def _assert_runtime_unwritten(runtime: Path) -> None:
    if not runtime.exists():
        return
    assert list(runtime.iterdir()) == []
    assert not list(runtime.rglob("*.tmp"))


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _expected_fresh_main_state(
    head_sha: str,
    *,
    main_branch: str = _REFERENCE_MAIN_BRANCH,
) -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": main_branch,
        "validated_result_sha": head_sha,
        "last_tock_sha": head_sha,
        "observed_main_head_sha": head_sha,
        "reason_code": "BOOTSTRAP_OK",
        "updated_at_utc": _REFERENCE_UPDATED_AT,
    }


def _empty_consumption_ledger() -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "consumed_promotions": [],
    }


def _plant_merge_sentinel(target: Path) -> None:
    git_dir = Path(_run_git(target, "rev-parse", "--absolute-git-dir"))
    (git_dir / "MERGE_HEAD").write_text("synthetic-merge\n", encoding="utf-8")


@pytest.mark.parametrize(
    "surface",
    ("public-exports", "coverage-harness"),
)
def test_public_exports_are_the_named_fresh_init_surface(surface: str) -> None:
    if surface == "coverage-harness":
        source_text = Path(bootstrap_module.__file__).read_text(encoding="utf-8")
        for marker in _FORBIDDEN_COVERAGE_HARNESS_MARKERS:
            assert marker not in source_text
        return

    exported = {name for name in dir(bootstrap_module) if not name.startswith("_")}

    for name in _REQUIRED_EXPORTS:
        assert name in exported
    assert "main" not in exported
    assert inspect.isfunction(bootstrap_main_state)
    assert inspect.isfunction(allowed_reason_codes)


def test_bootstrap_signature_is_exactly_the_public_keyword_surface() -> None:
    parameters = inspect.signature(bootstrap_main_state).parameters

    assert tuple(parameters) == (
        *_BOOTSTRAP_REQUIRED_PARAMETERS,
        *_BOOTSTRAP_OPTIONAL_PARAMETERS,
    )
    for name in _BOOTSTRAP_REQUIRED_PARAMETERS:
        assert parameters[name].default is inspect.Parameter.empty
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
    for name in _BOOTSTRAP_OPTIONAL_PARAMETERS:
        assert parameters[name].default is None
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY


def test_allowed_reason_codes_are_the_public_preflight_and_runtime_codes() -> None:
    codes = allowed_reason_codes()

    assert isinstance(codes, tuple)
    assert codes == _REQUIRED_REASON_CODES


def test_fresh_init_creates_canonical_state_and_empty_ledger_on_missing_runtime(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)
    head_sha = _run_git(target, "rev-parse", "HEAD")
    main_state_path = runtime / _MAIN_STATE_NAME
    consumed_path = runtime / _CONSUMED_NAME

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    expected_state = _expected_fresh_main_state(head_sha)
    assert result == {
        "ok": True,
        "status": "bootstrapped",
        "reason_code": "BOOTSTRAP_OK",
        "main_state": expected_state,
        "artifacts_written": {
            "main_state": main_state_path,
            "promotion_consumed": consumed_path,
        },
    }
    assert probe.read_text(encoding="utf-8") == "validated\n"
    persisted = load_and_validate_main_state(main_state_path=main_state_path)
    assert persisted == expected_state
    assert tuple(persisted) == _REQUIRED_MAIN_STATE_FIELDS
    assert _read_yaml(main_state_path) == expected_state
    assert load_promotion_consumed(promotion_consumed_path=consumed_path) == (
        _empty_consumption_ledger()
    )
    assert _read_yaml(consumed_path) == _empty_consumption_ledger()
    assert not (runtime / "runner_source_identity.yaml").exists()
    assert _run_git(target, "status", "--porcelain") == ""
    assert _run_git(target, "rev-parse", "HEAD") == head_sha
    assert not list(runtime.rglob("*.tmp"))


def test_fresh_init_accepts_an_empty_runtime_directory(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)
    head_sha = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert result["ok"] is True
    assert result["reason_code"] == "BOOTSTRAP_OK"
    assert result["main_state"] == _expected_fresh_main_state(head_sha)
    assert load_promotion_consumed(
        promotion_consumed_path=runtime / _CONSUMED_NAME
    ) == _empty_consumption_ledger()


def test_fresh_init_binds_the_observed_main_head_not_an_invented_sha(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    (target / "tracked.txt").write_text("second baseline\n", encoding="utf-8")
    _run_git(target, "add", "--", "tracked.txt")
    _run_git(target, "commit", "-m", "observable second baseline")
    runtime = _runtime_root(tmp_path, create=False)
    first_parent = _run_git(target, "rev-parse", "HEAD^")
    head_sha = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert head_sha != first_parent
    assert result["main_state"]["validated_result_sha"] == head_sha
    assert result["main_state"]["last_tock_sha"] == head_sha
    assert result["main_state"]["observed_main_head_sha"] == head_sha
    assert result["main_state"]["validated_result_sha"] != first_parent


@pytest.mark.parametrize(
    ("mutate", "reason_code"),
    (
        (
            lambda target: (target / "tracked.txt").write_text(
                "unstaged dirty\n",
                encoding="utf-8",
            ),
            "GIT_DIRTY",
        ),
        (
            lambda target: _run_git(target, "checkout", "-b", "feature/fresh-init"),
            "GIT_WRONG_BRANCH",
        ),
        (
            lambda target: _run_git(target, "checkout", "--detach", "HEAD"),
            "GIT_DETACHED_HEAD",
        ),
        (_plant_merge_sentinel, "GIT_MERGE_IN_PROGRESS"),
    ),
)
def test_preflight_blocks_before_runtime_mutation(
    tmp_path: Path,
    mutate: Callable[[Path], object],
    reason_code: str,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    mutate(target)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, reason_code)
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)


def test_non_git_target_blocks_without_creating_runtime(tmp_path: Path) -> None:
    target = tmp_path / "not-a-git-target"
    target.mkdir()
    runtime = _runtime_root(tmp_path, create=False)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "GIT_TARGET_INVALID")
    _assert_runtime_unwritten(runtime)


def test_nested_path_inside_a_repo_is_not_the_worktree_root(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    nested = target / "nested"
    nested.mkdir()
    runtime = _runtime_root(tmp_path, create=False)

    result = _bootstrap(target_worktree=nested, runtime_root=runtime)

    _assert_blocked(result, "GIT_TARGET_NOT_WORKTREE_ROOT")
    _assert_runtime_unwritten(runtime)


def test_runtime_inside_the_target_is_root_overlap(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime = target / "runner-runtime"

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "BOOTSTRAP_ROOT_OVERLAP")
    assert not runtime.exists()
    assert _run_git(target, "status", "--porcelain") == ""


@pytest.mark.parametrize("artifact_name", _CONFLICT_ARTIFACTS)
def test_existing_runtime_artifact_blocks_without_overwrite(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)
    artifact = runtime / artifact_name
    original = b"preexisting-runtime-bytes\n"
    artifact.write_bytes(original)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "RUNTIME_NOT_EMPTY")
    assert artifact.read_bytes() == original
    if artifact_name != _MAIN_STATE_NAME:
        assert not (runtime / _MAIN_STATE_NAME).exists()
    if artifact_name != _CONSUMED_NAME:
        assert not (runtime / _CONSUMED_NAME).exists()
    assert not probe.exists()
    assert not list(runtime.rglob("*.tmp"))


def test_empty_main_state_file_is_a_conflict_not_a_fresh_start(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)
    empty_state = runtime / _MAIN_STATE_NAME
    empty_state.write_bytes(b"")

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "RUNTIME_NOT_EMPTY")
    assert empty_state.read_bytes() == b""
    assert not (runtime / _CONSUMED_NAME).exists()


def test_failed_baseline_validation_does_not_invent_or_write_state(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=1, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "BASELINE_VALIDATION_FAILED")
    assert probe.read_text(encoding="utf-8") == "validated\n"
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "status", "--porcelain") == ""


def test_caller_supplied_authority_block_is_fail_closed_without_validation(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
        authority_block_reason="SOURCE_AUTHORITY_MISMATCH",
    )

    _assert_blocked(result, "SOURCE_AUTHORITY_MISMATCH")
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)


def test_caller_supplied_history_block_is_a_lock_not_an_implemented_gate(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        authority_block_reason="HISTORY_PROTOCOL_PRESENT",
    )

    _assert_blocked(result, "HISTORY_PROTOCOL_PRESENT")
    _assert_runtime_unwritten(runtime)


def test_blank_authority_block_reason_is_contract_visible_ambiguity(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        authority_block_reason="",
    )

    _assert_blocked(result, "BOOTSTRAP_AUTHORITY_BLOCK_INVALID")
    _assert_runtime_unwritten(runtime)


def test_successful_fresh_init_does_not_claim_code_authority_or_history_green(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert result["ok"] is True
    assert result["reason_code"] == "BOOTSTRAP_OK"
    assert "authority" not in result
    assert "history" not in result
    assert not (runtime / "runner_source_identity.yaml").exists()


def test_fresh_init_binds_caller_supplied_main_branch_other_than_main(
    tmp_path: Path,
) -> None:
    branch = "trunk"
    target = _init_target_repo(tmp_path, branch=branch)
    runtime = _runtime_root(tmp_path, create=False)
    head_sha = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        main_branch=branch,
    )

    expected_state = _expected_fresh_main_state(head_sha, main_branch=branch)
    assert _run_git(target, "rev-parse", "--abbrev-ref", "HEAD") == branch
    assert result["ok"] is True
    assert result["reason_code"] == "BOOTSTRAP_OK"
    assert result["main_state"] == expected_state
    persisted = load_and_validate_main_state(main_state_path=runtime / _MAIN_STATE_NAME)
    assert persisted["main_branch"] == branch
    assert persisted["observed_main_head_sha"] == head_sha
    assert _run_git(target, "rev-parse", "--abbrev-ref", "HEAD") == branch


def test_target_under_runtime_is_inverse_root_overlap(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path, create=True)
    target = _init_target_repo(runtime, name="target-repo")
    command = _write_validation_command(tmp_path, exit_code=0)
    command_path = Path(command)
    original_command_bytes = command_path.read_bytes()
    assert not command_path.resolve().is_relative_to(runtime.resolve())
    before_runtime = sorted(path.name for path in runtime.iterdir())
    before_head = _run_git(target, "rev-parse", "HEAD")
    before_bytes = {
        path.relative_to(runtime): path.read_bytes()
        for path in runtime.rglob("*")
        if path.is_file()
    }

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "BOOTSTRAP_ROOT_OVERLAP")
    assert command_path.is_file()
    assert command_path.read_bytes() == original_command_bytes
    assert sorted(path.name for path in runtime.iterdir()) == before_runtime
    assert _run_git(target, "rev-parse", "HEAD") == before_head
    assert _run_git(target, "status", "--porcelain") == ""
    assert not (runtime / _MAIN_STATE_NAME).exists()
    assert not (runtime / _CONSUMED_NAME).exists()
    after_bytes = {
        path.relative_to(runtime): path.read_bytes()
        for path in runtime.rglob("*")
        if path.is_file()
    }
    assert after_bytes == before_bytes


def test_malformed_baseline_command_quoting_is_fail_closed(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command="printf 'unterminated",
    )

    _assert_blocked(result, "BASELINE_VALIDATION_FAILED")
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "status", "--porcelain") == ""


def test_missing_baseline_executable_is_fail_closed(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    missing = tmp_path / "missing-baseline-validate"
    assert not missing.exists()

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=str(missing),
    )

    _assert_blocked(result, "BASELINE_VALIDATION_FAILED")
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "status", "--porcelain") == ""


def test_hanging_baseline_command_respects_timeout_without_write(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    command = _write_hanging_validation_command(tmp_path)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
        baseline_validation_timeout_seconds=1,
    )

    _assert_blocked(result, "BASELINE_VALIDATION_FAILED")
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "status", "--porcelain") == ""


def test_baseline_command_string_with_arguments_does_not_use_a_shell(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    pwned = tmp_path / "shell-side-effect.txt"
    script = _write_argv_validation_command(tmp_path)
    command = f"{script} --probe {probe} ; printf pwned > {pwned}"

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    assert result["ok"] is True
    assert result["reason_code"] == "BOOTSTRAP_OK"
    assert probe.read_text(encoding="utf-8") == "validated\n"
    assert not pwned.exists()
    assert load_and_validate_main_state(
        main_state_path=runtime / _MAIN_STATE_NAME
    )["observed_main_head_sha"] == _run_git(target, "rev-parse", "HEAD")


def test_post_validation_dirty_worktree_blocks_before_persist(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    command = _write_post_validation_dirty_command(tmp_path)
    head_sha = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "GIT_DIRTY")
    _assert_runtime_unwritten(runtime)
    assert (target / "tracked.txt").read_text(encoding="utf-8") == (
        "post-validation dirty\n"
    )
    assert _run_git(target, "rev-parse", "HEAD") == head_sha
    assert _run_git(target, "rev-parse", "--abbrev-ref", "HEAD") == (
        _REFERENCE_MAIN_BRANCH
    )


def test_post_validation_clean_head_move_blocks_before_persist(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    command = _write_post_validation_head_move_command(tmp_path)
    head_before = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "BOOTSTRAP_HEAD_DRIFT")
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "rev-parse", "--abbrev-ref", "HEAD") == (
        _REFERENCE_MAIN_BRANCH
    )
    assert _run_git(target, "status", "--porcelain") == ""
    assert _run_git(target, "rev-parse", "HEAD") != head_before


def test_runtime_root_file_stays_byte_identical_and_blocks(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    original = b"not-a-runtime-directory\n"
    runtime.write_bytes(original)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "RUNTIME_NOT_EMPTY")
    assert runtime.is_file()
    assert runtime.read_bytes() == original


def test_unknown_runtime_occupant_is_runtime_not_empty(tmp_path: Path) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)
    occupant = runtime / "unrelated.bin"
    original = b"unknown-occupant\n"
    occupant.write_bytes(original)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "RUNTIME_NOT_EMPTY")
    assert occupant.read_bytes() == original
    assert not (runtime / _MAIN_STATE_NAME).exists()
    assert not (runtime / _CONSUMED_NAME).exists()


@pytest.mark.parametrize("stamp", ("", "   ", "\t\n"))
def test_blank_updated_at_utc_blocks_without_write(
    tmp_path: Path,
    stamp: str,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
        updated_at_utc=stamp,
    )

    _assert_blocked(result, "BOOTSTRAP_TIMESTAMP_INVALID")
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)


def test_atomic_replace_failure_leaves_no_partial_runtime_or_temp_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    real_replace = os.replace
    calls = {"count": 0}

    def fail_second(source: str | os.PathLike[str], dest: str | os.PathLike[str]) -> None:
        calls["count"] += 1
        if calls["count"] >= 2:
            raise OSError("simulated second atomic replace failure")
        real_replace(source, dest)

    monkeypatch.setattr(os, "replace", fail_second)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "BOOTSTRAP_WRITE_FAILED")
    if runtime.exists():
        assert not (runtime / _MAIN_STATE_NAME).exists()
        assert not (runtime / _CONSUMED_NAME).exists()
        assert not list(runtime.rglob("*.tmp"))
    else:
        assert not runtime.exists()


def _commit_tree(
    repo: Path,
    message: str,
    parents: tuple[str, ...],
    tree: str,
) -> str:
    args = ["commit-tree", tree]
    for parent in parents:
        args.extend(["-p", parent])
    args.extend(["-m", message])
    return _run_git(repo, *args)


def _advance_main(repo: Path, sha: str) -> None:
    _run_git(repo, "reset", "--hard", sha)


def _head_tree(repo: Path) -> tuple[str, str]:
    head = _run_git(repo, "rev-parse", "HEAD")
    tree = _run_git(repo, "rev-parse", f"{head}^{{tree}}")
    return head, tree


def _canonical_tock_message(base_sha: str, result_sha: str) -> str:
    return (
        "TOCK\n"
        "\n"
        f"Promotion-Attempt: {_SYNTHETIC_ATTEMPT_ID}\n"
        f"Task-ID: {_SYNTHETIC_TASK_ID}\n"
        f"Origin-Run: {_SYNTHETIC_ORIGIN_RUN_ID}\n"
        f"Result-SHA: {result_sha}\n"
        "Promotion-Class: GREEN_PROMOTION\n"
        f"Expected-Base: {base_sha}\n"
    )


def _plant_canonical_tock(repo: Path) -> str:
    base, tree = _head_tree(repo)
    result = _commit_tree(repo, "synthetic result object\n", (base,), tree)
    tock = _commit_tree(
        repo,
        _canonical_tock_message(base, result),
        (base, result),
        tree,
    )
    _advance_main(repo, tock)
    return tock


def _plant_canonical_start(repo: Path) -> str:
    parent, tree = _head_tree(repo)
    message = (
        f"START {_SYNTHETIC_TASK_ID} {_SYNTHETIC_ORIGIN_RUN_ID} "
        f"{_SYNTHETIC_ATTEMPT_ID} {parent}\n"
    )
    tick = _commit_tree(repo, message, (parent,), tree)
    _advance_main(repo, tick)
    return tick


def _plant_empty_message(repo: Path) -> str:
    parent, tree = _head_tree(repo)
    completed = subprocess.run(  # noqa: S603
        ["git", "commit-tree", tree, "-p", parent],
        cwd=repo,
        input="",
        check=True,
        capture_output=True,
        text=True,
    )
    planted = completed.stdout.strip()
    raw = _run_git(repo, "cat-file", "-p", planted)
    _header, _separator, message = raw.partition("\n\n")
    assert message.splitlines() == []
    _advance_main(repo, planted)
    return planted


def _plant_start_with_foreign_tree(repo: Path) -> str:
    parent, parent_tree = _head_tree(repo)
    blob = subprocess.run(  # noqa: S603
        ["git", "hash-object", "-w", "--stdin"],
        cwd=repo,
        input="foreign start tree\n",
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    foreign_tree = subprocess.run(  # noqa: S603
        ["git", "mktree"],
        cwd=repo,
        input=f"100644 blob {blob}\tforeign.txt\n",
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert foreign_tree != parent_tree
    message = (
        f"START {_SYNTHETIC_TASK_ID} {_SYNTHETIC_ORIGIN_RUN_ID} "
        f"{_SYNTHETIC_ATTEMPT_ID} {parent}\n"
    )
    tick = _commit_tree(repo, message, (parent,), foreign_tree)
    assert _run_git(repo, "rev-parse", f"{tick}^{{tree}}") == foreign_tree
    _advance_main(repo, tick)
    return tick


def _plant_bound_promotion_attempt(repo: Path) -> str:
    parent, tree = _head_tree(repo)
    planted = _commit_tree(
        repo,
        f"Promotion-Attempt: {_SYNTHETIC_ATTEMPT_ID}\n",
        (parent,),
        tree,
    )
    _advance_main(repo, planted)
    return planted


def _plant_unreferenced_tock(repo: Path) -> str:
    base, tree = _head_tree(repo)
    result = _commit_tree(repo, "synthetic result object\n", (base,), tree)
    return _commit_tree(
        repo,
        _canonical_tock_message(base, result),
        (base, result),
        tree,
    )


def _source_pin_text(source_sha: str) -> str:
    return f"schema_version: '{_SCHEMA_VERSION}'\nsource_sha: '{source_sha}'\n"


def _assert_fresh_init_baseline(payload: dict[str, Any], head_sha: str) -> None:
    assert tuple(payload) == _REQUIRED_MAIN_STATE_FIELDS
    assert payload["state"] == "GREEN_VALIDATED"
    assert payload["reason_code"] == "BOOTSTRAP_OK"
    assert payload["validated_result_sha"] == head_sha
    assert payload["last_tock_sha"] == head_sha
    assert payload["observed_main_head_sha"] == head_sha
    for key in _FORBIDDEN_FRESH_BASELINE_KEYS:
        assert key not in payload


def _running_source_root() -> Path:
    return derive_source_root(Path(bootstrap_module.__file__).resolve())


@pytest.mark.parametrize(
    "plant",
    (
        _plant_canonical_tock,
        _plant_canonical_start,
        _plant_bound_promotion_attempt,
    ),
    ids=("canonical-tock", "canonical-start", "bound-promotion-attempt"),
)
def test_first_parent_protocol_history_blocks_before_runtime_write(
    tmp_path: Path,
    plant: Callable[[Path], str],
) -> None:
    target = _init_target_repo(tmp_path)
    plant(target)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)
    head_before = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "RUNTIME_PROTOCOL_HISTORY_PRESENT")
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "rev-parse", "HEAD") == head_before
    assert _run_git(target, "status", "--porcelain") == ""


def test_configured_ref_other_than_main_blocks_canonical_first_parent_history(
    tmp_path: Path,
) -> None:
    branch = "release"
    assert branch != _REFERENCE_MAIN_BRANCH
    target = _init_target_repo(tmp_path, branch=branch)
    planted = _plant_canonical_start(target)
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)
    head_before = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        main_branch=branch,
        baseline_validation_command=command,
    )

    assert _run_git(target, "rev-parse", "--abbrev-ref", "HEAD") == branch
    assert head_before == planted
    _assert_blocked(result, "RUNTIME_PROTOCOL_HISTORY_PRESENT")
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)
    assert _run_git(target, "rev-parse", "HEAD") == head_before
    assert _run_git(target, "status", "--porcelain") == ""


def test_protocol_history_precedes_dirty_preflight_and_failed_validation(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    _plant_canonical_tock(target)
    (target / "tracked.txt").write_text("dirty after history\n", encoding="utf-8")
    runtime = _runtime_root(tmp_path, create=True)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=1, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "RUNTIME_PROTOCOL_HISTORY_PRESENT")
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)
    assert not (runtime / _MAIN_STATE_NAME).exists()
    assert not (runtime / _CONSUMED_NAME).exists()


@pytest.mark.parametrize(
    "non_history",
    (
        "near-miss-subjects",
        "empty-message",
        "start-tree-mismatch",
    ),
)
def test_baseline_and_compact_tock_subject_are_not_protocol_history(
    tmp_path: Path,
    non_history: str,
) -> None:
    if non_history == "empty-message":
        target = _init_target_repo(tmp_path)
        planted = _plant_empty_message(target)
        runtime = _runtime_root(tmp_path, create=False)
        head_sha = _run_git(target, "rev-parse", "HEAD")

        result = _bootstrap(target_worktree=target, runtime_root=runtime)

        assert head_sha == planted
        assert result["ok"] is True
        assert result["reason_code"] == "BOOTSTRAP_OK"
        assert result["reason_code"] != "RUNTIME_PROTOCOL_HISTORY_PRESENT"
        _assert_fresh_init_baseline(result["main_state"], head_sha)
        return
    if non_history == "start-tree-mismatch":
        target = _init_target_repo(tmp_path)
        planted = _plant_start_with_foreign_tree(target)
        parent_tree = _run_git(target, "rev-parse", "HEAD^^{tree}")
        head_tree = _run_git(target, "rev-parse", "HEAD^{tree}")
        runtime = _runtime_root(tmp_path, create=False)
        head_sha = _run_git(target, "rev-parse", "HEAD")

        result = _bootstrap(target_worktree=target, runtime_root=runtime)

        assert head_sha == planted
        assert head_tree != parent_tree
        assert result["ok"] is True
        assert result["reason_code"] == "BOOTSTRAP_OK"
        assert result["reason_code"] != "RUNTIME_PROTOCOL_HISTORY_PRESENT"
        _assert_fresh_init_baseline(result["main_state"], head_sha)
        return

    target = _init_target_repo(tmp_path)
    parent, tree = _head_tree(target)
    compact = _commit_tree(target, "TOCK\n", (parent,), tree)
    _advance_main(target, compact)
    prose_parent, prose_tree = _head_tree(target)
    prose = _commit_tree(
        target,
        "notes about a future TOCK label\n",
        (prose_parent,),
        prose_tree,
    )
    _advance_main(target, prose)
    near_parent, near_tree = _head_tree(target)
    near_message = (
        "TOCK\n"
        "\n"
        f"Promotion-Attempt: {_SYNTHETIC_ATTEMPT_ID}\n"
        f"Promotion-Attempt: {_SYNTHETIC_ATTEMPT_ID}\n"
        f"Origin-Run: {_SYNTHETIC_ORIGIN_RUN_ID}\n"
        f"Result-SHA: {near_parent}\n"
        "Promotion-Class: GREEN_PROMOTION\n"
        f"Expected-Base: {near_parent}\n"
    )
    try:
        parse_canonical_tock(
            near_message,
            (near_parent,),
            near_tree,
            validated_tree_oid=near_tree,
        )
    except RunnerGitPromotionError as exc:
        assert exc.reason_code == "PROMOTION_TOCK_METADATA_INVALID"
    else:
        raise AssertionError("near-canonical tock must stay parser-invalid")
    near = _commit_tree(target, near_message, (near_parent,), near_tree)
    _advance_main(target, near)
    geometry_parent, geometry_tree = _head_tree(target)
    result_object = _commit_tree(
        target,
        "synthetic result object\n",
        (geometry_parent,),
        geometry_tree,
    )
    geometry_message = _canonical_tock_message(geometry_parent, result_object)
    geometry = _commit_tree(
        target,
        geometry_message,
        (geometry_parent,),
        geometry_tree,
    )
    _advance_main(target, geometry)
    observed_parents = tuple(_run_git(target, "rev-parse", f"{geometry}^@").split())
    try:
        parse_canonical_tock(
            geometry_message,
            observed_parents,
            geometry_tree,
            validated_tree_oid=geometry_tree,
        )
    except RunnerGitPromotionError as exc:
        assert exc.reason_code == "PROMOTION_TOCK_PARENT_MISMATCH"
    else:
        raise AssertionError("metadata-valid tock must stay parent-invalid")
    runtime = _runtime_root(tmp_path, create=False)
    head_sha = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert head_sha == geometry
    assert observed_parents == (geometry_parent,)
    assert result["ok"] is True
    assert result["reason_code"] == "BOOTSTRAP_OK"
    assert result["reason_code"] != "RUNTIME_PROTOCOL_HISTORY_PRESENT"
    _assert_fresh_init_baseline(result["main_state"], head_sha)


def test_side_ref_and_side_parent_tock_do_not_authorize_history(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    side_tock = _plant_unreferenced_tock(target)
    _run_git(target, "update-ref", _DIAGNOSTIC_BACKUP_REF, side_tock)
    base, tree = _head_tree(target)
    merge = _commit_tree(
        target,
        "ordinary side-parent note\n",
        (base, side_tock),
        tree,
    )
    _advance_main(target, merge)
    runtime = _runtime_root(tmp_path, create=False)
    head_sha = _run_git(target, "rev-parse", "HEAD")
    first_parent = _run_git(target, "rev-parse", "HEAD^")

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert side_tock != head_sha
    assert first_parent == base
    assert result["ok"] is True
    assert result["reason_code"] == "BOOTSTRAP_OK"
    assert result["reason_code"] != "RUNTIME_PROTOCOL_HISTORY_PRESENT"
    _assert_fresh_init_baseline(result["main_state"], head_sha)
    assert _run_git(target, "rev-parse", _DIAGNOSTIC_BACKUP_REF) == side_tock


def test_fresh_main_state_is_closed_baseline_without_predecessor_identity(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    head_sha = _run_git(target, "rev-parse", "HEAD")

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert result["ok"] is True
    _assert_fresh_init_baseline(result["main_state"], head_sha)
    persisted = load_and_validate_main_state(
        main_state_path=runtime / _MAIN_STATE_NAME,
    )
    _assert_fresh_init_baseline(persisted, head_sha)
    assert "authority" not in result
    assert "history" not in result


def test_source_target_overlap_blocks_before_validation_and_runtime_write(
    tmp_path: Path,
) -> None:
    source = _running_source_root()
    head_before = _run_git(source, "rev-parse", "HEAD")
    status_before = _run_git(source, "status", "--porcelain")
    runtime = _runtime_root(tmp_path, create=False)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)

    result = _bootstrap(
        target_worktree=source,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    assert _run_git(source, "rev-parse", "HEAD") == head_before
    assert _run_git(source, "status", "--porcelain") == status_before
    _assert_blocked(result, "SOURCE_AUTHORITY_ROOT_OVERLAP")
    assert not probe.exists()
    _assert_runtime_unwritten(runtime)


def test_successful_fresh_init_persists_matching_source_pin(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    source = _running_source_root()
    source_sha = _run_git(source, "rev-parse", "HEAD")
    entry = Path(bootstrap_module.__file__).resolve()

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    assert result["ok"] is True
    _assert_fresh_init_baseline(
        result["main_state"],
        _run_git(target, "rev-parse", "HEAD"),
    )
    pin = runtime / RUNTIME_SOURCE_PIN_NAME
    assert pin.is_file()
    assert _read_yaml(pin) == {
        "schema_version": _SCHEMA_VERSION,
        "source_sha": source_sha,
    }
    report = verify_steady_state_code_authority(
        entry_point=entry,
        target_root=target,
        runtime_root=runtime,
        loaded_module_files={
            "task_transition_runner.runner_main_state_bootstrap": entry,
        },
    )
    assert report.source_sha == source_sha
    assert report.runtime_pin_sha == source_sha
    assert not (runtime / "runner_source_identity.yaml").exists()


def test_foreign_source_pin_blocks_before_validation_without_overwrite(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)
    pin = runtime / RUNTIME_SOURCE_PIN_NAME
    original = _source_pin_text(_FOREIGN_SOURCE_SHA).encode("utf-8")
    pin.write_bytes(original)
    probe = tmp_path / "validation-probe.txt"
    command = _write_validation_command(tmp_path, exit_code=0, probe=probe)

    result = _bootstrap(
        target_worktree=target,
        runtime_root=runtime,
        baseline_validation_command=command,
    )

    _assert_blocked(result, "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH")
    assert pin.read_bytes() == original
    assert not probe.exists()
    assert not (runtime / _MAIN_STATE_NAME).exists()
    assert not (runtime / _CONSUMED_NAME).exists()
    assert not list(runtime.rglob("*.tmp"))


def test_matching_source_pin_alone_is_not_a_fresh_runtime(
    tmp_path: Path,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=True)
    source_sha = _run_git(_running_source_root(), "rev-parse", "HEAD")
    pin = runtime / RUNTIME_SOURCE_PIN_NAME
    original = _source_pin_text(source_sha).encode("utf-8")
    pin.write_bytes(original)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    _assert_blocked(result, "RUNTIME_NOT_EMPTY")
    assert pin.read_bytes() == original
    assert not (runtime / _MAIN_STATE_NAME).exists()
    assert not (runtime / _CONSUMED_NAME).exists()


def test_source_pin_replace_failure_is_per_artifact_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    real_replace = os.replace
    replaced_siblings: dict[str, bytes] = {}

    def fail_pin(source: str | os.PathLike[str], dest: str | os.PathLike[str]) -> None:
        if Path(dest).name == RUNTIME_SOURCE_PIN_NAME:
            if runtime.is_dir():
                for sibling in runtime.iterdir():
                    if sibling.is_file() and not sibling.name.endswith(".tmp"):
                        replaced_siblings[sibling.name] = sibling.read_bytes()
            raise OSError("simulated source-pin replace failure")
        real_replace(source, dest)

    monkeypatch.setattr(os, "replace", fail_pin)

    result = _bootstrap(target_worktree=target, runtime_root=runtime)

    for name, blob in replaced_siblings.items():
        sibling = runtime / name
        if not sibling.exists():
            continue
        assert sibling.is_file()
        assert sibling.read_bytes() == blob
    _assert_blocked(result, "RUNTIME_PERSISTENCE_FAILED")
    if runtime.exists():
        assert not (runtime / RUNTIME_SOURCE_PIN_NAME).exists()
        assert not list(runtime.rglob("*.tmp"))
    else:
        assert not runtime.exists()


def test_source_reproof_after_baseline_blocks_cleanliness_drift(
    tmp_path: Path,
) -> None:
    source = _running_source_root()
    marker = source / "init-source-reproof-marker.txt"
    assert not marker.exists()
    assert "init-source-reproof-marker.txt" not in _run_git(
        source,
        "status",
        "--porcelain",
        "--untracked-files=all",
    )
    target = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path, create=False)
    script = tmp_path / "baseline-reproof.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"printf 'drift\\n' > '{marker}'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    head_before = _run_git(source, "rev-parse", "HEAD")
    marker_created = False
    try:
        result = _bootstrap(
            target_worktree=target,
            runtime_root=runtime,
            baseline_validation_command=str(script),
        )
        marker_created = marker.is_file() and marker.read_text(encoding="utf-8") == (
            "drift\n"
        )
    finally:
        marker.unlink(missing_ok=True)
    assert "init-source-reproof-marker.txt" not in _run_git(
        source,
        "status",
        "--porcelain",
        "--untracked-files=all",
    )
    assert _run_git(source, "rev-parse", "HEAD") == head_before
    assert marker_created is True
    _assert_blocked(result, "SOURCE_AUTHORITY_SOURCE_DIRTY")
    _assert_runtime_unwritten(runtime)


def test_source_sha_reproof_after_baseline_blocks_clean_sha_drift(
    tmp_path: Path,
) -> None:
    candidate = _running_source_root()
    candidate_head = _run_git(candidate, "rev-parse", "HEAD")
    candidate_refs = _run_git(candidate, "show-ref")
    candidate_index = _run_git(candidate, "diff", "--cached", "--name-only")
    candidate_porcelain = _run_git(
        candidate,
        "status",
        "--porcelain",
        "--untracked-files=all",
    )
    source = tmp_path / "synthetic-source"
    source.mkdir()
    subprocess.run(
        ["git", "-c", "init.defaultBranch=main", "init"],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    )
    _run_git(source, "config", "user.name", _GIT_AUTHOR_NAME)
    _run_git(source, "config", "user.email", _GIT_AUTHOR_EMAIL)
    package_src = Path(bootstrap_module.__file__).resolve().parent
    shutil.copytree(
        package_src,
        source / "src" / "task_transition_runner",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    _run_git(source, "add", "--", "src")
    _run_git(source, "commit", "-m", "synthetic source baseline")
    source_before = _run_git(source, "rev-parse", "HEAD")
    target = _init_target_repo(tmp_path, name="sha-target")
    runtime = _runtime_root(tmp_path, create=False)
    entry = source / "src" / "task_transition_runner" / "runner_main_state_bootstrap.py"
    loaded_modules = {
        "task_transition_runner.runner_main_state_bootstrap": entry,
    }
    observed = verify_steady_state_code_authority(
        entry_point=entry,
        target_root=target,
        runtime_root=runtime,
        loaded_module_files=loaded_modules,
    )
    assert observed.source_root == source.resolve()
    assert observed.source_sha == source_before
    assert observed.runtime_pin_sha is None
    script = tmp_path / "baseline-source-sha.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"git -C '{source}' commit --allow-empty -m 'synthetic source sha drift'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    child = """
import json
import sys
from pathlib import Path

import task_transition_runner.runner_main_state_bootstrap as module
from task_transition_runner.runner_main_state_bootstrap import bootstrap_main_state

target, runtime, command, source = sys.argv[1:5]
result = bootstrap_main_state(
    target_worktree=Path(target),
    runtime_root=Path(runtime),
    main_branch="main",
    baseline_validation_command=command,
    baseline_validation_timeout_seconds=30,
    updated_at_utc="2026-09-11T18:00:00Z",
)
sys.stdout.write(json.dumps({
    "module_file": str(Path(module.__file__).resolve()),
    "ok": result.get("ok"),
    "reason_code": result.get("reason_code"),
    "status": result.get("status"),
}))
"""
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.pop("COVERAGE_PROCESS_START", None)
    for inherited_name in tuple(env):
        if inherited_name.startswith("COV_CORE_"):
            del env[inherited_name]
    env["PYTHONPATH"] = os.pathsep.join(
        [str(source / "src"), env.get("PYTHONPATH", "")]
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            child,
            str(target),
            str(runtime),
            str(script),
            str(source),
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(completed.stdout)
    assert Path(payload["module_file"]).is_relative_to(source.resolve())
    assert _run_git(source, "rev-parse", "HEAD") != source_before
    assert _run_git(source, "status", "--porcelain") == ""
    assert _run_git(candidate, "rev-parse", "HEAD") == candidate_head
    assert _run_git(candidate, "show-ref") == candidate_refs
    assert _run_git(candidate, "diff", "--cached", "--name-only") == candidate_index
    assert (
        _run_git(
            candidate,
            "status",
            "--porcelain",
            "--untracked-files=all",
        )
        == candidate_porcelain
    )
    _assert_blocked(payload, "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH")
    _assert_runtime_unwritten(runtime)
    with pytest.raises(RunnerCodeAuthorityError) as caught:
        verify_steady_state_code_authority(
            entry_point=entry,
            target_root=target,
            runtime_root=runtime,
            expected_source_sha=source_before,
            loaded_module_files=loaded_modules,
        )
    assert caught.value.reason_code == "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH"
