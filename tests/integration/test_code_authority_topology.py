"""Real Candidate code-authority topologies for PUBLIC-P-08 staging."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_GIT_AUTHOR_NAME = "Code Authority Topology"
_GIT_AUTHOR_EMAIL = "code-authority-topology@task-transition-runner.invalid"
_FULL_SHA_LENGTH = 40
_SOURCE_ROOT_INVALID = "SOURCE_AUTHORITY_SOURCE_ROOT_INVALID"
_SOURCE_DIRTY = "SOURCE_AUTHORITY_SOURCE_DIRTY"
_SOURCE_SHA_MISMATCH = "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH"
_IMPORT_SHADOWED = "SOURCE_AUTHORITY_IMPORT_SHADOWED"
_PIN_MISSING = "SOURCE_AUTHORITY_RUNTIME_PIN_MISSING"
_PIN_MISMATCH = "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH"
_ROOT_OVERLAP = "SOURCE_AUTHORITY_ROOT_OVERLAP"
_SEMANTIC_STATUS_SLOTS = (
    "target_main_head",
    "main_state",
    "active_task_identity",
    "run_phase",
    "pending",
    "journal",
    "consumption",
    "drain",
    "decisions",
    "drift_warning",
)

_AUTHORITY_PROBE = r"""
import importlib.util
import json
import os
import sys
from pathlib import Path

from task_transition_runner.runner_code_authority import (
    RunnerCodeAuthorityError,
    verify_steady_state_code_authority,
)
from task_transition_runner.status import classify_authority_first_status

entry_point = Path(os.environ["ENTRY_POINT"])
target_root = Path(os.environ["TARGET_ROOT"])
runtime_root = Path(os.environ["RUNTIME_ROOT"])
expected_source_sha = os.environ["EXPECTED_SOURCE_SHA"]
shadow_path = os.environ.get("SHADOW_PATH")
if shadow_path is not None:
    module_name = "task_transition_runner.target_shadow_probe"
    spec = importlib.util.spec_from_file_location(module_name, shadow_path)
    if spec is None or spec.loader is None:
        raise AssertionError("target shadow module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

if os.environ["PROBE_MODE"] == "verify":
    try:
        report = verify_steady_state_code_authority(
            entry_point=entry_point,
            target_root=target_root,
            runtime_root=runtime_root,
            expected_source_sha=expected_source_sha,
        )
    except RunnerCodeAuthorityError as error:
        payload = {"outcome": "BLOCKED", "reason_code": error.reason_code}
    else:
        payload = {
            "outcome": "AUTHORIZED",
            "source_root": str(report.source_root),
            "source_sha": report.source_sha,
            "entry_point": str(report.entry_point),
            "target_root": str(report.target_root),
            "runtime_root": str(report.runtime_root),
            "runtime_pin_sha": report.runtime_pin_sha,
            "loaded_core_modules": list(report.loaded_core_modules),
            "process_cwd": str(Path.cwd().resolve()),
        }
else:
    view = classify_authority_first_status(
        entry_point=entry_point,
        target_worktree=target_root,
        runtime_root=runtime_root,
        expected_source_sha=expected_source_sha,
    )
    payload = {
        "outcome": "STATUS",
        "projection_reason_code": view.projection_reason_code,
        "runner_source_identity": view.runner_source_identity.payload,
        "target_main_head": view.target_main_head.payload,
        "next_external_action": view.next_external_action.payload,
        "reason_code": view.reason_code.payload,
        "semantic_presence": {
            name: getattr(view, name).presence.value
            for name in os.environ["SEMANTIC_STATUS_SLOTS"].split(",")
        },
        "semantic_payload": {
            name: getattr(view, name).payload
            for name in os.environ["SEMANTIC_STATUS_SLOTS"].split(",")
        },
    }

print(json.dumps(payload, sort_keys=True))
"""


@dataclass(frozen=True)
class _Topology:
    source: Path
    target: Path
    runtime: Path
    input_root: Path
    entry_point: Path
    source_sha: str
    target_sha: str


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _fresh_topology(tmp_path: Path, case_name: str) -> _Topology:
    case_root = tmp_path / case_name
    case_root.mkdir()
    candidate_root = Path(__file__).resolve().parents[2]
    source = case_root / "runner-source"
    subprocess.run(  # noqa: S603
        [
            "git",
            "clone",
            "--quiet",
            "--no-local",
            "--no-tags",
            "--single-branch",
            "--branch",
            "main",
            str(candidate_root),
            str(source),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(source, "config", "user.name", _GIT_AUTHOR_NAME)
    _git(source, "config", "user.email", _GIT_AUTHOR_EMAIL)

    target = case_root / "target"
    target.mkdir()
    subprocess.run(  # noqa: S603
        ["git", "-c", "init.defaultBranch=main", "init", "--quiet"],
        cwd=target,
        check=True,
        capture_output=True,
        text=True,
    )
    _git(target, "config", "user.name", _GIT_AUTHOR_NAME)
    _git(target, "config", "user.email", _GIT_AUTHOR_EMAIL)
    (target / "tracked.txt").write_text("target baseline\n", encoding="utf-8")
    _git(target, "add", "--", "tracked.txt")
    _git(target, "commit", "--quiet", "-m", "target baseline")

    runtime = case_root / "runtime"
    runtime.mkdir()
    input_root = case_root / "input"
    input_root.mkdir()
    source_sha = _git(source, "rev-parse", "HEAD")
    assert source_sha == _git(candidate_root, "rev-parse", "HEAD")
    assert len(source_sha) == _FULL_SHA_LENGTH
    assert _git(source, "status", "--porcelain=v1", "--untracked-files=all") == ""
    return _Topology(
        source=source.resolve(),
        target=target.resolve(),
        runtime=runtime.resolve(),
        input_root=input_root.resolve(),
        entry_point=(
            source / "src" / "task_transition_runner" / "cli.py"
        ).resolve(),
        source_sha=source_sha,
        target_sha=_git(target, "rev-parse", "HEAD"),
    )


def _probe(
    topology: _Topology,
    *,
    mode: str = "verify",
    entry_point: Path | None = None,
    target_root: Path | None = None,
    runtime_root: Path | None = None,
    expected_source_sha: str | None = None,
    shadow_path: Path | None = None,
) -> dict[str, Any]:
    environment = os.environ.copy()
    source_path = str(topology.source / "src")
    inherited_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_path
        if not inherited_pythonpath
        else os.pathsep.join((source_path, inherited_pythonpath))
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PROBE_MODE"] = mode
    environment["ENTRY_POINT"] = str(entry_point or topology.entry_point)
    environment["TARGET_ROOT"] = str(target_root or topology.target)
    environment["RUNTIME_ROOT"] = str(runtime_root or topology.runtime)
    environment["EXPECTED_SOURCE_SHA"] = (
        expected_source_sha
        if expected_source_sha is not None
        else topology.source_sha
    )
    environment["SEMANTIC_STATUS_SLOTS"] = ",".join(_SEMANTIC_STATUS_SLOTS)
    if shadow_path is not None:
        environment["SHADOW_PATH"] = str(shadow_path)
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _AUTHORITY_PROBE],
        cwd=topology.input_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )
    return json.loads(completed.stdout)


def _assert_pairwise_disjoint(*roots: Path) -> None:
    resolved = tuple(root.resolve() for root in roots)
    assert len(set(resolved)) == len(resolved)
    for index, left in enumerate(resolved):
        for right in resolved[index + 1 :]:
            assert not left.is_relative_to(right)
            assert not right.is_relative_to(left)


def _assert_blocked(
    topology: _Topology,
    reason_code: str,
    **overrides: Any,
) -> None:
    assert _probe(topology, **overrides) == {
        "outcome": "BLOCKED",
        "reason_code": reason_code,
    }


def _write_runtime_pin(runtime: Path, source_sha: str) -> None:
    (runtime / "runner_source_pin.yaml").write_text(
        "schema_version: '1.0'\n"
        f"source_sha: '{source_sha}'\n",
        encoding="utf-8",
    )


def _different_sha(source_sha: str) -> str:
    candidate = "0" * _FULL_SHA_LENGTH
    return candidate if candidate != source_sha else "f" * _FULL_SHA_LENGTH


def _move_source(topology: _Topology, marker: str) -> str:
    (topology.source / f"{marker}.txt").write_text(
        "committed runner movement\n",
        encoding="utf-8",
    )
    _git(topology.source, "add", "--", f"{marker}.txt")
    _git(topology.source, "commit", "--quiet", "-m", marker)
    moved_sha = _git(topology.source, "rev-parse", "HEAD")
    assert moved_sha != topology.source_sha
    assert _git(
        topology.source,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ) == ""
    return moved_sha


def test_real_source_target_runtime_input_topologies(tmp_path: Path) -> None:
    positive = _fresh_topology(tmp_path, "positive")
    _assert_pairwise_disjoint(
        positive.source,
        positive.target,
        positive.runtime,
        positive.input_root,
    )
    assert list(positive.runtime.iterdir()) == []
    authorized = _probe(positive)
    assert authorized["outcome"] == "AUTHORIZED"
    assert authorized["source_root"] == str(positive.source)
    assert authorized["source_sha"] == positive.source_sha
    assert authorized["entry_point"] == str(positive.entry_point)
    assert authorized["target_root"] == str(positive.target)
    assert authorized["runtime_root"] == str(positive.runtime)
    assert authorized["runtime_pin_sha"] is None
    assert authorized["process_cwd"] == str(positive.input_root)
    assert "task_transition_runner.runner_code_authority" in (
        authorized["loaded_core_modules"]
    )
    assert _git(
        positive.source,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ) == ""
    assert list(positive.runtime.iterdir()) == []

    authorized_status = _probe(positive, mode="status")
    assert authorized_status["projection_reason_code"] == "STATUS_VIEW_PROJECTED"
    assert authorized_status["runner_source_identity"] == {
        "source_sha": positive.source_sha,
        "source_root": str(positive.source),
        "entry_point": str(positive.entry_point),
        "runtime_pin_sha": None,
    }
    assert authorized_status["target_main_head"] == {"sha": positive.target_sha}

    wrong_source = _fresh_topology(tmp_path, "wrong-source")
    wrong_entry = wrong_source.input_root / "cli.py"
    wrong_entry.write_text("# not a Git-controlled entry point\n", encoding="utf-8")
    _assert_blocked(
        wrong_source,
        _SOURCE_ROOT_INVALID,
        entry_point=wrong_entry,
    )

    dirty_source = _fresh_topology(tmp_path, "dirty-source")
    (dirty_source.source / "untracked-drift.txt").write_text(
        "dirty runner source\n",
        encoding="utf-8",
    )
    _assert_blocked(dirty_source, _SOURCE_DIRTY)

    moving_source = _fresh_topology(tmp_path, "moving-source")
    _move_source(moving_source, "runner-source-moved")
    _assert_blocked(
        moving_source,
        _SOURCE_SHA_MISMATCH,
        expected_source_sha=moving_source.source_sha,
    )

    target_shadow = _fresh_topology(tmp_path, "target-import-shadow")
    shadow = (
        target_shadow.target
        / "src"
        / "task_transition_runner"
        / "target_shadow_probe.py"
    )
    shadow.parent.mkdir(parents=True)
    shadow.write_text("SHADOW = True\n", encoding="utf-8")
    _assert_blocked(
        target_shadow,
        _IMPORT_SHADOWED,
        shadow_path=shadow,
    )

    runtime_pin_mismatch = _fresh_topology(tmp_path, "runtime-pin-mismatch")
    _write_runtime_pin(
        runtime_pin_mismatch.runtime,
        _different_sha(runtime_pin_mismatch.source_sha),
    )
    _assert_blocked(runtime_pin_mismatch, _PIN_MISMATCH)

    prior_protocol_history = _fresh_topology(tmp_path, "prior-protocol-history")
    history_marker = prior_protocol_history.runtime / "main_state.yaml"
    history_bytes = b"canonical protocol history\nnot-yaml: [\n"
    history_marker.write_bytes(history_bytes)
    _assert_blocked(prior_protocol_history, _PIN_MISSING)
    assert history_marker.read_bytes() == history_bytes
    assert not (
        prior_protocol_history.runtime / "runner_source_pin.yaml"
    ).exists()

    runner_change = _fresh_topology(tmp_path, "runner-change-without-migration")
    _write_runtime_pin(runner_change.runtime, runner_change.source_sha)
    (runner_change.runtime / "main_state.yaml").write_text(
        "schema_version: '1.0'\nstate: GREEN_VALIDATED\n",
        encoding="utf-8",
    )
    moved_sha = _move_source(runner_change, "new-runner-sha")
    _assert_blocked(
        runner_change,
        _PIN_MISMATCH,
        expected_source_sha=moved_sha,
    )

    overlap_cases = (
        ("target-is-source", "source", "runtime"),
        ("runtime-is-source", "target", "source"),
        ("runtime-is-target", "target", "target"),
    )
    for case_name, target_name, runtime_name in overlap_cases:
        overlap = _fresh_topology(tmp_path, case_name)
        roots = {
            "source": overlap.source,
            "target": overlap.target,
            "runtime": overlap.runtime,
        }
        _assert_blocked(
            overlap,
            _ROOT_OVERLAP,
            target_root=roots[target_name],
            runtime_root=roots[runtime_name],
        )

    authority_first = _fresh_topology(tmp_path, "authority-first-status")
    _write_runtime_pin(authority_first.runtime, authority_first.source_sha)
    misleading_runtime = authority_first.runtime / "main_state.yaml"
    misleading_bytes = b"reason_code: DISK_SAYS_AUTHORIZED\nmalformed: [\n"
    misleading_runtime.write_bytes(misleading_bytes)
    (authority_first.source / "untracked-status-drift.txt").write_text(
        "dirty runner source\n",
        encoding="utf-8",
    )
    blocked_status = _probe(authority_first, mode="status")
    assert blocked_status["outcome"] == "STATUS"
    assert blocked_status["projection_reason_code"] == _SOURCE_DIRTY
    assert blocked_status["runner_source_identity"] == {
        "reason_code": _SOURCE_DIRTY,
        "interpreting_runtime": False,
    }
    assert blocked_status["next_external_action"] == {"value": "BLOCKED"}
    assert blocked_status["reason_code"] == {"value": _SOURCE_DIRTY}
    assert blocked_status["semantic_presence"] == {
        slot: "ABSENT" for slot in _SEMANTIC_STATUS_SLOTS
    }
    assert blocked_status["semantic_payload"] == {
        slot: None for slot in _SEMANTIC_STATUS_SLOTS
    }
    assert misleading_runtime.read_bytes() == misleading_bytes
