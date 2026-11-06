"""Neutral public ``runner init --config`` CLI assertion.

Adapted from the freeze-pinned bootstrap-CLI assertion seed. Private
flags, private module names, private helpers, and private repository
layout are not reproduced.

Keep:
- ``init`` plus a config path as the only public init invocation
- machine-readable YAML stdout
- artifact paths serialized as strings
- fail-closed refusal that writes no ``main_state.yaml``
- no default ``SOURCE_AUTHORITY_OK``

Drop:
- ``--repo-root``, ``--runtime-root``, ``--validate-result-sha``
- ``--initialize-promotion-consumed`` as a separate flag
- private ``main()`` helpers and private runtime import

The complete ``init`` composition under test is still red against the
preceding Candidate. It must run the public source-authority and
First-Parent history proofs before any runtime byte, then persist the
source pin with the fresh state and consumption ledger. No cross-file
transaction is claimed.
"""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.cli as cli_module
import task_transition_runner.runner_code_authority as code_authority_module
from task_transition_runner.cli import main
from task_transition_runner.runner_code_authority import (
    RUNTIME_SOURCE_PIN_NAME,
    derive_source_root,
    verify_steady_state_code_authority,
)

_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_GIT_AUTHOR_NAME = "Thin CLI Init Contract"
_GIT_AUTHOR_EMAIL = "thin-cli-init@task-transition-runner.invalid"
_PROMPT_ROLES = ("developer", "orchestrator", "validator")
_FORBIDDEN_OUTCOMES = (
    "RECOVERED",
    "REOPENED",
    "MIGRATED",
    "SOURCE_AUTHORITY_OK",
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


def _init_target_repo(tmp_path: Path, *, branch: str = _REFERENCE_MAIN_BRANCH) -> Path:
    repo_root = tmp_path / "target-repo"
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
    (repo_root / "tracked.txt").write_text("public init baseline\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "tracked.txt")
    _run_git(repo_root, "commit", "-m", "public init baseline")
    return repo_root


def _write_validation_command(tmp_path: Path) -> str:
    script = tmp_path / "baseline-validate.sh"
    script.write_text("#!/bin/sh\nset -eu\nexit 0\n", encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


def _write_public_config(
    tmp_path: Path,
    *,
    target: Path,
    runtime: Path,
    baseline_command: str,
) -> Path:
    config_root = tmp_path / "config-root"
    prompts = config_root / "prompts"
    prompts.mkdir(parents=True)
    runtime.mkdir(parents=True, exist_ok=True)
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
            "command": baseline_command,
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
    return config_path


def _read_stdout_yaml(stdout: str) -> dict[str, Any]:
    payload = yaml.safe_load(stdout)
    assert isinstance(payload, dict)
    return payload


def _invoke_init(config_path: Path) -> int:
    return main(["init", "--config", str(config_path)])


def test_cli_bootstraps_main_state_and_serializes_paths(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )
    head_sha = _run_git(target, "rev-parse", "HEAD")
    main_state_path = runtime / "main_state.yaml"
    consumed_path = runtime / "promotion_consumed.yaml"

    exit_code = _invoke_init(config_path)

    assert exit_code == 0
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    assert stdout["ok"] is True
    assert stdout["command"] == "init"
    assert stdout["outcome"] == "INITIALIZED"
    assert stdout["reason_code"] == "BOOTSTRAP_OK"
    assert stdout["main_state"]["observed_main_head_sha"] == head_sha
    assert stdout["main_state"]["validated_result_sha"] == head_sha
    assert stdout["artifacts_written"] == {
        "main_state": str(main_state_path),
        "promotion_consumed": str(consumed_path),
    }
    assert isinstance(stdout["artifacts_written"]["main_state"], str)
    assert isinstance(stdout["artifacts_written"]["promotion_consumed"], str)
    dumped = yaml.safe_dump(stdout["artifacts_written"])
    assert "!!python/object" not in dumped
    assert main_state_path.is_file()
    assert consumed_path.is_file()
    assert not (runtime / "runner_source_identity.yaml").exists()
    assert stdout.get("outcome") not in _FORBIDDEN_OUTCOMES
    assert "SOURCE_AUTHORITY_OK" not in yaml.safe_dump(stdout)
    assert _run_git(target, "status", "--porcelain") == ""
    assert _run_git(target, "rev-parse", "HEAD") == head_sha
    assert not (runtime / "runner_git.lock").exists()


def test_cli_init_fail_closed_does_not_write_state(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    (target / "tracked.txt").write_text("dirty after baseline\n", encoding="utf-8")
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )

    exit_code = _invoke_init(config_path)

    assert exit_code == 1
    stdout = _read_stdout_yaml(capsys.readouterr().out)
    assert stdout["ok"] is False
    assert stdout["command"] == "init"
    assert stdout["outcome"] == "BLOCKED"
    assert stdout["status"] == "fail-closed"
    assert stdout["reason_code"] == "GIT_DIRTY"
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    assert not (runtime / "runner_source_identity.yaml").exists()
    assert stdout.get("outcome") not in _FORBIDDEN_OUTCOMES


def test_cli_init_does_not_default_source_authority_ok(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 0
    assert stdout["reason_code"] == "BOOTSTRAP_OK"
    assert "authority" not in stdout
    assert stdout.get("source_authority_ok") is not True
    rendered = yaml.safe_dump(stdout)
    assert "SOURCE_AUTHORITY_OK" not in rendered
    identity = runtime / "runner_source_identity.yaml"
    if identity.exists():
        digest = hashlib.sha256(identity.read_bytes()).hexdigest()
        assert "SOURCE_AUTHORITY_OK" not in identity.read_text(encoding="utf-8")
        assert digest != ""


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
_FRESH_BASELINE_FIELDS = (
    "schema_version",
    "state",
    "main_branch",
    "validated_result_sha",
    "last_tock_sha",
    "observed_main_head_sha",
    "reason_code",
    "updated_at_utc",
)


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


def _source_pin_text(source_sha: str) -> str:
    return f"schema_version: '{_SCHEMA_VERSION}'\nsource_sha: '{source_sha}'\n"


def _assert_fresh_init_baseline(payload: dict[str, Any], head_sha: str) -> None:
    assert tuple(payload) == _FRESH_BASELINE_FIELDS
    assert payload["state"] == "GREEN_VALIDATED"
    assert payload["reason_code"] == "BOOTSTRAP_OK"
    assert payload["validated_result_sha"] == head_sha
    assert payload["last_tock_sha"] == head_sha
    assert payload["observed_main_head_sha"] == head_sha
    for key in _FORBIDDEN_FRESH_BASELINE_KEYS:
        assert key not in payload


def _assert_cli_blocked(stdout: dict[str, Any], reason_code: str) -> None:
    assert stdout["ok"] is False
    assert stdout["command"] == "init"
    assert stdout["outcome"] == "BLOCKED"
    assert stdout["status"] == "fail-closed"
    assert stdout["reason_code"] == reason_code
    assert stdout.get("outcome") not in _FORBIDDEN_OUTCOMES


@pytest.mark.parametrize(
    "plant",
    (
        _plant_canonical_tock,
        _plant_canonical_start,
        _plant_bound_promotion_attempt,
    ),
    ids=("canonical-tock", "canonical-start", "bound-promotion-attempt"),
)
def test_cli_init_blocks_first_parent_protocol_history_before_runtime_write(
    tmp_path: Path,
    capsys: Any,
    plant: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    plant(target)
    runtime = tmp_path / "runner-runtime"
    probe = tmp_path / "validation-probe.txt"
    script = tmp_path / "baseline-validate.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"printf 'validated\\n' > '{probe}'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=str(script),
    )
    head_before = _run_git(target, "rev-parse", "HEAD")

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _assert_cli_blocked(stdout, "RUNTIME_PROTOCOL_HISTORY_PRESENT")
    assert not probe.exists()
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    assert not (runtime / RUNTIME_SOURCE_PIN_NAME).exists()
    assert _run_git(target, "rev-parse", "HEAD") == head_before
    assert _run_git(target, "status", "--porcelain") == ""


def test_cli_init_history_precedes_dirty_target_preflight(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    _plant_canonical_start(target)
    (target / "tracked.txt").write_text("dirty after history\n", encoding="utf-8")
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _assert_cli_blocked(stdout, "RUNTIME_PROTOCOL_HISTORY_PRESENT")
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    assert not (runtime / RUNTIME_SOURCE_PIN_NAME).exists()


def test_cli_init_ignores_backup_ref_tock(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    base, tree = _head_tree(target)
    result = _commit_tree(target, "synthetic result object\n", (base,), tree)
    tock = _commit_tree(
        target,
        _canonical_tock_message(base, result),
        (base, result),
        tree,
    )
    _run_git(target, "update-ref", _DIAGNOSTIC_BACKUP_REF, tock)
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )
    head_sha = _run_git(target, "rev-parse", "HEAD")

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 0
    assert stdout["outcome"] == "INITIALIZED"
    assert stdout["reason_code"] == "BOOTSTRAP_OK"
    assert stdout["reason_code"] != "RUNTIME_PROTOCOL_HISTORY_PRESENT"
    _assert_fresh_init_baseline(stdout["main_state"], head_sha)
    assert _run_git(target, "rev-parse", _DIAGNOSTIC_BACKUP_REF) == tock
    assert _run_git(target, "rev-parse", "HEAD") == head_sha


def test_cli_init_persists_closed_baseline_and_matching_source_pin(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )
    source = derive_source_root(Path(cli_module.__file__).resolve())
    source_sha = _run_git(source, "rev-parse", "HEAD")
    head_sha = _run_git(target, "rev-parse", "HEAD")
    entry = Path(cli_module.__file__).resolve()

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 0
    assert stdout["outcome"] == "INITIALIZED"
    assert stdout["reason_code"] == "BOOTSTRAP_OK"
    assert "SOURCE_AUTHORITY_OK" not in yaml.safe_dump(stdout)
    _assert_fresh_init_baseline(stdout["main_state"], head_sha)
    pin = runtime / RUNTIME_SOURCE_PIN_NAME
    assert pin.is_file()
    payload = yaml.safe_load(pin.read_text(encoding="utf-8"))
    assert payload == {
        "schema_version": _SCHEMA_VERSION,
        "source_sha": source_sha,
    }
    report = verify_steady_state_code_authority(
        entry_point=entry,
        target_root=target,
        runtime_root=runtime,
        loaded_module_files={
            "task_transition_runner.cli": entry,
        },
    )
    assert report.source_sha == source_sha
    assert report.runtime_pin_sha == source_sha
    assert not (runtime / "runner_source_identity.yaml").exists()
    assert _run_git(target, "status", "--porcelain") == ""


def test_cli_init_foreign_source_pin_blocks_without_overwrite(
    tmp_path: Path,
    capsys: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )
    pin = runtime / RUNTIME_SOURCE_PIN_NAME
    original = _source_pin_text(_FOREIGN_SOURCE_SHA).encode("utf-8")
    pin.write_bytes(original)

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _assert_cli_blocked(stdout, "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH")
    assert pin.read_bytes() == original
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()


def test_cli_init_source_pin_replace_failure_is_fail_closed(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: Any,
) -> None:
    target = _init_target_repo(tmp_path)
    runtime = tmp_path / "runner-runtime"
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=_write_validation_command(tmp_path),
    )
    real_replace = os.replace

    def fail_pin(source: str | os.PathLike[str], dest: str | os.PathLike[str]) -> None:
        if Path(dest).name == RUNTIME_SOURCE_PIN_NAME:
            raise OSError("simulated source-pin replace failure")
        real_replace(source, dest)

    monkeypatch.setattr("os.replace", fail_pin)

    exit_code = _invoke_init(config_path)
    stdout = _read_stdout_yaml(capsys.readouterr().out)

    assert exit_code == 1
    _assert_cli_blocked(stdout, "RUNTIME_PERSISTENCE_FAILED")
    assert not (runtime / RUNTIME_SOURCE_PIN_NAME).exists()
    assert not list(runtime.rglob("*.tmp"))


@pytest.mark.parametrize(
    "reason_code",
    (
        "SOURCE_AUTHORITY_SOURCE_DIRTY",
        "SOURCE_AUTHORITY_IMPORT_SHADOWED",
    ),
)
def test_cli_init_authority_precedes_history_preflight_and_runtime(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: pytest.MonkeyPatch,
    reason_code: str,
) -> None:
    target = _init_target_repo(tmp_path)
    source = derive_source_root(Path(cli_module.__file__).resolve())
    marker = source / "init-authority-dirty-marker.txt"
    shadow = target / "src" / "task_transition_runner" / "config.py"
    if reason_code == "SOURCE_AUTHORITY_IMPORT_SHADOWED":
        shadow.parent.mkdir(parents=True)
        shadow.write_text("# synthetic shadow\n", encoding="utf-8")
        _run_git(target, "add", "--", "src/task_transition_runner/config.py")
        _run_git(target, "commit", "-m", "synthetic shadow file")
    _plant_canonical_tock(target)
    runtime = tmp_path / "runner-runtime"
    probe = tmp_path / "validation-probe.txt"
    script = tmp_path / "baseline-authority.sh"
    script.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"printf 'validated\\n' > '{probe}'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    config_path = _write_public_config(
        tmp_path,
        target=target,
        runtime=runtime,
        baseline_command=str(script),
    )
    head_before = _run_git(target, "rev-parse", "HEAD")
    try:
        if reason_code == "SOURCE_AUTHORITY_SOURCE_DIRTY":
            assert not marker.exists()
            marker.write_text("dirty\n", encoding="utf-8")
        else:
            assert shadow.is_file()
            monkeypatch.setattr(
                code_authority_module,
                "__file__",
                str(shadow.resolve()),
            )
        exit_code = _invoke_init(config_path)
        stdout = _read_stdout_yaml(capsys.readouterr().out)
    finally:
        marker.unlink(missing_ok=True)
    assert exit_code == 1
    _assert_cli_blocked(stdout, reason_code)
    assert not probe.exists()
    assert not (runtime / "main_state.yaml").exists()
    assert not (runtime / "promotion_consumed.yaml").exists()
    assert not (runtime / RUNTIME_SOURCE_PIN_NAME).exists()
    assert _run_git(target, "rev-parse", "HEAD") == head_before
    assert _run_git(target, "status", "--porcelain") == ""
