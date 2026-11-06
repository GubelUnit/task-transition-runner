"""Synthetic public 12-symbol task-end prepare/finalize contract.

Keep:
- exactly two entry points and the ten bound helpers
- result verification against Git HEAD, branch, dirty paths, and allowed_paths
- WorkerResult and DecisionRequest persistence under the public run layout
- AWAITING_DECISIONS when external decisions are missing, draft, or not ready
- later-call revalidation of ready identity-bound decisions
- step-14 persist of a NON_AUTHORIZING_PROMOTION_INTENT
- clean return-to-main after pending persist, without canonical Main mutation
- Direct GREEN without a prior RED obligation

Drop:
- non-public orchestration and staging concerns
- private ack matrices, role-named approvals, and a thirteenth module symbol
- journal start, consumption, TOCK, merge-candidate validation, and recovery

Typed adapter-result composition, still absent from the product slice:
- one keyword-only composer derives Result-SHA and changed paths only from Git
- the positive publish allowlist is the only adapter projection
- raw stdout, stderr, prompt, session id, and argv never enter WorkerResult
- an identical verified result does not rerun the worker
- an interrupted unverified stream blocks and does not resume
- adapter terminal success is not promotion authority
- WorkerResult and CursorAdapterOutcome keep their current fields
- origin, attempt, and task branch bind the same way as task id and result SHA
- timeout, a false prompt frame, and one broken snapshot binding do not persist
- replay loses to Git HEAD drift and wins over an old stream block
- changed paths cover every commit from the base to HEAD

Fail-closed repair against the frozen composition draft:
- interrupted true together with terminal success stays an unsafe projection
- a non-mapping event_counts stays an unsafe projection and does not raise
- a persisted foreign task, origin, or attempt identity is RESULT_IDENTITY_MISMATCH
- a defective snapshot binding blocks before replay of a git-equal worker result

Committed-object risk authority:
- Task-End reproves ``base_sha -> result_sha`` internally
- caller all-clear facts cannot hide a committed sensitive path
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import inspect
import re
import shutil
import stat
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.adapters.cursor_headless import CursorAdapterOutcome
from task_transition_runner.adapters.cursor_stream import BoundPromptBytes
from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    DecisionIdentity,
    EvidenceReference,
    OrchestratorDecision,
    ValidatorDecision,
    load_decision_request,
    load_orchestrator_decision,
    load_validator_decision,
)
from task_transition_runner.task_contract import DirectGreenOutcome, ExpectedRedOutcome
from task_transition_runner.task_end_promotion import (
    _atomic_write_yaml,
    _copy_yaml_payload,
    _git_stdout,
    _load_yaml_mapping_if_present,
    _non_runtime_dirty_paths,
    _pending_matches_context,
    _promotion_blocked,
    _validate_current_red_lock_closure,
    _validate_ready_promotion_drafts,
    _wait_for_ready_promotion_drafts,
    finalize_git_promotion_task_end_artifacts,
    prepare_git_promotion_task_end_artifacts,
)
from task_transition_runner.worker_result import (
    ValidationResult,
    WorkerReport,
    WorkerResult,
    WorkerResultIdentity,
    load_worker_result,
)

_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_TASK_ID = "synthetic-task-001"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_TASK_BRANCH = "runner/task/synthetic-task-001/attempt-001"
_REFERENCE_ALLOWED_PATHS = ("src/example.py", "tests/test_example.py")
_REFERENCE_COMMAND_ID = "tests"
_REFERENCE_OUTPUT_REF = "runs/origin-run-001/validation/tests.log"
_VALIDATION_LOG_BODY = b"synthetic validation log\n"
_REFERENCE_OUTPUT_SHA256 = hashlib.sha256(_VALIDATION_LOG_BODY).hexdigest()
_REFERENCE_FAILURE_SIGNATURE = "synthetic expected failing assertion text"
_REFERENCE_FINGERPRINT = "1" * 64
_REFERENCE_TASK_DIGEST = "2" * 64
_REFERENCE_INSTRUCTION_DIGEST = "3" * 64
_SPEC_BINDING_SHA256 = "4" * 64
_REFERENCE_UPDATED_AT = "2026-09-08T18:00:00Z"
_REFERENCE_LAST_TOCK_SHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
_PRIOR_TASK_ID = "synthetic-task-000"
_PRIOR_ORIGIN_RUN_ID = "origin-run-000"
_PRIOR_PROMOTION_ATTEMPT_ID = "promotion-attempt-000"
_PRIOR_RESULT_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_CANONICAL_PENDING_NAME = "pending_promotion.yaml"
_WORKER_RESULT_NAME = "worker_result.yaml"
_DECISION_REQUEST_NAME = "decision_request.yaml"
_ORCHESTRATOR_NAME = "orchestrator_decision.yaml"
_VALIDATOR_NAME = "validator_decision.yaml"
_GIT_AUTHOR_NAME = "Task End Contract"
_GIT_AUTHOR_EMAIL = "task-end@task-transition-runner.invalid"
_COMPOSITION_SYMBOL = "compose_adapter_git_worker_result"
_PUBLISHABLE_KEYS = (
    "adapter_status",
    "neutral_reason_code",
    "process_exit_code",
    "timeout",
    "interrupted",
    "event_counts",
    "session_observed",
    "terminal_success",
)
_EVENT_COUNT_KEYS = (
    "assistant",
    "result",
    "system",
    "tool_call",
    "unknown",
    "user",
)
_COMPOSE_PARAMETERS = (
    "repo_root",
    "runtime_root",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "task_branch",
    "developer",
    "task_spec",
    "adapter_outcome",
    "allowed_paths",
    "claimed_identity",
)
_WORKER_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "result_type",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "task_branch",
        "result_sha",
        "changed_paths",
        "validation_results",
        "worker_report",
    }
)
_ADAPTER_OUTCOME_FIELDS = frozenset(
    {
        "publish_projection",
        "prompt_frame_matches",
        "stdout_artifact",
        "stderr_artifact",
    }
)
_PRODUCT_CANARIES = (
    "worker_result.py",
    "task_end_promotion.py",
    "cli.py",
    "adapters/cursor_headless.py",
    "adapters/cursor_stream.py",
)
_STREAM_BLOCK_NAME = "adapter_stream_block.yaml"
_STREAM_BLOCK_KIND = "interrupted_unverified_cursor_session"
_CANARY_PROMPT = "SYNTHETIC-OSS621-CANARY-PROMPT"
_CANARY_SPEC = "SYNTHETIC-OSS621-CANARY-SPEC"
_CANARY_ARGV = "SYNTHETIC-OSS621-CANARY-ARGV"
_CANARY_SESSION = "SYNTHETIC-OSS621-CANARY-SESSION"
_CANARY_STDOUT = "SYNTHETIC-OSS621-CANARY-STDOUT"
_CANARY_STDERR = "SYNTHETIC-OSS621-CANARY-STDERR"
_CANARY_LIVE = "SYNTHETIC-OSS621-CANARY-LIVE-REREAD"
_FORGED_RESULT_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_BOUND_SYMBOLS = (
    "prepare_git_promotion_task_end_artifacts",
    "finalize_git_promotion_task_end_artifacts",
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
_PREPARE_REQUIRED_PARAMETERS = (
    "repo_root",
    "runtime_root",
    "worker_result",
    "allowed_paths",
)
_FINALIZE_REQUIRED_PARAMETERS = (
    "repo_root",
    "runtime_root",
    "origin_run_id",
    "task_snapshot",
    "instruction_snapshot",
    "expected_outcome",
    "allowed_paths",
    "risk_facts",
    "lock_ownership",
    "git_preflight",
    "consumed_payload",
    "red_lock",
    "unresolved_foreign_epoch",
    "timeout_seconds",
    "poll_seconds",
)
_WAIT_REQUIRED_PARAMETERS = (
    "context",
    "timeout_seconds",
    "poll_seconds",
    "artifact_root",
)
_VALIDATE_DRAFTS_REQUIRED_PARAMETERS = (
    "context",
    "orchestrator_state",
    "orchestrator_decision",
    "validator_state",
    "validator_decision",
)
_EFFECT_PATH_NAMES = (
    "promotion_journal.yaml",
    "promotion_consumed.yaml",
    "drain_outcome.yaml",
    "main_state_tock.yaml",
)


def _composed_token(*segments: str) -> str:
    return "".join(segments)


_FORBIDDEN_PARAMETERS = (
    _composed_token("orp", "_", "payload"),
    "tock_sha",
    "journal",
    "merge_candidate",
    "consume",
)


def _module():
    import task_transition_runner.task_end_promotion as task_end_promotion

    return task_end_promotion


def _defined_symbols(module: Any) -> set[str]:
    return {
        name
        for name, value in vars(module).items()
        if not name.startswith("__")
        and inspect.isfunction(value)
        and getattr(value, "__module__", None) == module.__name__
    }


def _validation_evidence() -> EvidenceReference:
    return EvidenceReference(
        kind="validation_log",
        ref=_REFERENCE_OUTPUT_REF,
        sha256=_REFERENCE_OUTPUT_SHA256,
    )


def _git_result_evidence(*, result_sha: str) -> EvidenceReference:
    return EvidenceReference(kind="git_result", ref=result_sha, sha256=None)


def _task_view() -> dict[str, Any]:
    return {
        "task_id": _REFERENCE_TASK_ID,
        "fingerprint": _REFERENCE_FINGERPRINT,
        "digest": _REFERENCE_TASK_DIGEST,
    }


def _reference_task_snapshot() -> dict[str, Any]:
    payload = _task_view()
    return {"current": payload, "bound": copy.deepcopy(payload)}


def _instruction_view() -> dict[str, Any]:
    return {
        "digest": _REFERENCE_INSTRUCTION_DIGEST,
        "bindings": (
            {
                "logical_ref": "./tasks/synthetic-task-001.md",
                "snapshot_path": "instructions/task_spec",
                "sha256": _SPEC_BINDING_SHA256,
                "byte_length": 24,
            },
        ),
    }


def _reference_instruction_snapshot() -> dict[str, Any]:
    payload = _instruction_view()
    return {"current": payload, "bound": copy.deepcopy(payload)}


def _reference_worker_result(*, result_sha: str, task_branch: str) -> WorkerResult:
    return WorkerResult(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        result_type="worker_result",
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        task_branch=task_branch,
        result_sha=result_sha,
        changed_paths=_REFERENCE_ALLOWED_PATHS,
        validation_results=(
            ValidationResult(
                command_id=_REFERENCE_COMMAND_ID,
                exit_code=0,
                output_ref=_REFERENCE_OUTPUT_REF,
                output_sha256=_REFERENCE_OUTPUT_SHA256,
            ),
        ),
        worker_report=WorkerReport(
            reported_status="success",
            summary="synthetic worker summary",
        ),
    )


def _reference_orchestrator(*, result_sha: str) -> OrchestratorDecision:
    return OrchestratorDecision(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        decision_type="orchestrator",
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=result_sha,
        ready=True,
        promotion_class="GREEN_PROMOTION",
        evidence=(_validation_evidence(),),
        rationale=None,
    )


def _reference_validator(*, result_sha: str) -> ValidatorDecision:
    return ValidatorDecision(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        decision_type="validator",
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=result_sha,
        ready=True,
        verdict="GO",
        evidence=(_git_result_evidence(result_sha=result_sha), _validation_evidence()),
        rationale=None,
    )


def _reference_main_state(*, observed_main_head_sha: str, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": observed_main_head_sha,
        "last_tock_sha": _REFERENCE_LAST_TOCK_SHA,
        "observed_main_head_sha": observed_main_head_sha,
        "reason_code": "STATE_READY_SYNTHETIC",
        "updated_at_utc": _REFERENCE_UPDATED_AT,
    }
    payload.update(overrides)
    return payload


def _empty_consumed() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "consumed_promotions": [],
    }


def _owned_lock() -> dict[str, Any]:
    return {
        "ok": True,
        "status": "owned",
        "reason_code": "LOCK_OK",
        "owned": True,
        "ownership": {
            "schema_version": _REFERENCE_SCHEMA_VERSION,
            "lock_name": "runner_git.lock",
            "run_id": _REFERENCE_ORIGIN_RUN_ID,
            "pid": 4242,
            "started_at_utc": "2026-09-08T17:00:00Z",
        },
    }


def _clean_preflight() -> dict[str, Any]:
    return {"ok": True, "reason_code": "GIT_PREFLIGHT_OK"}


def _clear_risk_facts() -> dict[str, Any]:
    return {
        "secret_risk": False,
        "delete_risk": False,
        "scope_risk": False,
    }


def _decision_identity(*, result_sha: str) -> DecisionIdentity:
    return DecisionIdentity(
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=result_sha,
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


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return path


def _runtime_root(tmp_path: Path) -> Path:
    runtime = tmp_path / "runner-runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    return runtime


def _run_directory(runtime: Path) -> Path:
    return runtime / "runs" / _REFERENCE_ORIGIN_RUN_ID


def _decisions_dir(runtime: Path) -> Path:
    return _run_directory(runtime) / "decisions"


def _write_validation_log(runtime: Path) -> Path:
    log_path = runtime / _REFERENCE_OUTPUT_REF
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_bytes(_VALIDATION_LOG_BODY)
    return log_path


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


def _commit_task_result(repo_root: Path) -> tuple[str, str, str]:
    main_sha = _run_git(repo_root, "rev-parse", "HEAD")
    _run_git(repo_root, "checkout", "-b", _REFERENCE_TASK_BRANCH)
    src_path = repo_root / "src" / "example.py"
    test_path = repo_root / "tests" / "test_example.py"
    src_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.parent.mkdir(parents=True, exist_ok=True)
    src_path.write_text("value = 1\n", encoding="utf-8")
    test_path.write_text("def test_example() -> None:\n    assert True\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "src/example.py", "tests/test_example.py")
    _run_git(repo_root, "commit", "-m", "synthetic task result")
    result_sha = _run_git(repo_root, "rev-parse", "HEAD")
    return main_sha, result_sha, _REFERENCE_TASK_BRANCH


def _seed_main_state(runtime: Path, payload: Mapping[str, Any]) -> Path:
    return _write_yaml(runtime / "main_state.yaml", payload)


def _decision_payload(
    *,
    kind: str,
    result_sha: str,
    ready: bool = True,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "decision_type": kind,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": result_sha,
        "ready": ready,
        "evidence": [
            {
                "kind": "validation_log",
                "ref": _REFERENCE_OUTPUT_REF,
                "sha256": _REFERENCE_OUTPUT_SHA256,
            }
        ],
    }
    if kind == "orchestrator":
        payload["promotion_class"] = "GREEN_PROMOTION"
    else:
        payload["verdict"] = "GO"
        payload["evidence"] = [
            {"kind": "git_result", "ref": result_sha},
            payload["evidence"][0],
        ]
    return payload


def _write_ready_decisions(runtime: Path, *, result_sha: str) -> None:
    decisions = _decisions_dir(runtime)
    _write_yaml(
        decisions / _ORCHESTRATOR_NAME,
        _decision_payload(kind="orchestrator", result_sha=result_sha),
    )
    _write_yaml(
        decisions / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha),
    )


def _prepare_kwargs(
    tmp_path: Path,
    *,
    worker_result: WorkerResult | None = None,
) -> dict[str, Any]:
    repo = _init_target_repo(tmp_path)
    main_sha, result_sha, task_branch = _commit_task_result(repo)
    runtime = _runtime_root(tmp_path)
    _write_validation_log(runtime)
    _seed_main_state(runtime, _reference_main_state(observed_main_head_sha=main_sha))
    bound_result = worker_result
    if bound_result is None:
        bound_result = _reference_worker_result(
            result_sha=result_sha,
            task_branch=task_branch,
        )
    return {
        "repo_root": repo,
        "runtime_root": runtime,
        "worker_result": bound_result,
        "allowed_paths": _REFERENCE_ALLOWED_PATHS,
        "main_sha": main_sha,
        "result_sha": result_sha,
        "task_branch": task_branch,
    }


def _finalize_kwargs(
    tmp_path: Path,
    *,
    timeout_seconds: int = 0,
    poll_seconds: int = 1,
    red_lock: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prepared = _prepare_kwargs(tmp_path)
    prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    kwargs = {
        "repo_root": prepared["repo_root"],
        "runtime_root": prepared["runtime_root"],
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "task_snapshot": _reference_task_snapshot(),
        "instruction_snapshot": _reference_instruction_snapshot(),
        "expected_outcome": DirectGreenOutcome(kind="GREEN"),
        "allowed_paths": _REFERENCE_ALLOWED_PATHS,
        "risk_facts": _clear_risk_facts(),
        "lock_ownership": _owned_lock(),
        "git_preflight": _clean_preflight(),
        "consumed_payload": _empty_consumed(),
        "red_lock": red_lock,
        "unresolved_foreign_epoch": False,
        "timeout_seconds": timeout_seconds,
        "poll_seconds": poll_seconds,
        "main_sha": prepared["main_sha"],
        "result_sha": prepared["result_sha"],
        "task_branch": prepared["task_branch"],
    }
    return kwargs


def _assert_no_effect_files(runtime: Path) -> None:
    for name in _EFFECT_PATH_NAMES:
        assert not (runtime / name).exists()


def _assert_no_pending_or_effects(runtime: Path) -> None:
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(runtime)


def _assert_prepare_did_not_persist(runtime: Path) -> None:
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    assert not (_decisions_dir(runtime) / _DECISION_REQUEST_NAME).exists()
    _assert_no_pending_or_effects(runtime)


def _live_red_lock(*, result_sha: str) -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_type": "red_lock",
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": result_sha,
        "expected_main_state_after_promotion": "EXPECTED_RED_OPEN",
        "closure_gate": "GREEN_PROMOTION_REQUIRED",
        "expected_failure_signature": _REFERENCE_FAILURE_SIGNATURE,
        "command_id": _REFERENCE_COMMAND_ID,
    }


def _predecessor_red_lock() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "lock_type": "red_lock",
        "task_id": _PRIOR_TASK_ID,
        "origin_run_id": _PRIOR_ORIGIN_RUN_ID,
        "promotion_attempt_id": _PRIOR_PROMOTION_ATTEMPT_ID,
        "result_sha": _PRIOR_RESULT_SHA,
        "expected_main_state_after_promotion": "EXPECTED_RED_OPEN",
        "closure_gate": "GREEN_PROMOTION_REQUIRED",
        "expected_failure_signature": _REFERENCE_FAILURE_SIGNATURE,
        "command_id": _REFERENCE_COMMAND_ID,
    }


def _open_red_main_state(*, observed_main_head_sha: str) -> dict[str, Any]:
    return _reference_main_state(
        observed_main_head_sha=observed_main_head_sha,
        state="EXPECTED_RED_OPEN",
        validated_result_sha=_PRIOR_RESULT_SHA,
        reason_code="STATE_EXPECTED_RED_OPEN",
    )


def _green_closes_red_context(*, observed_main_head_sha: str) -> dict[str, Any]:
    main_state = _open_red_main_state(observed_main_head_sha=observed_main_head_sha)
    red_lock = _predecessor_red_lock()
    return {
        "promotion_class": "GREEN_PROMOTION",
        "main_state_before_promotion": main_state,
        "red_lock_closure": {
            "red_lock": red_lock,
            "main_state_before_promotion": copy.deepcopy(main_state),
        },
    }


def _foreign_worker_mapping(*, task_branch: str) -> dict[str, Any]:
    payload = dataclasses.asdict(
        _reference_worker_result(
            result_sha="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            task_branch=task_branch,
        )
    )
    payload["worker_report"]["summary"] = "foreign preexisting worker result"
    return payload


def _foreign_decision_request_mapping() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "request_type": "decision_request",
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "required_decisions": [
            {"role": "orchestrator", "path": _ORCHESTRATOR_NAME},
            {"role": "validator", "path": _VALIDATOR_NAME},
        ],
        "evidence": [
            {
                "kind": "validation_log",
                "ref": _REFERENCE_OUTPUT_REF,
                "sha256": _REFERENCE_OUTPUT_SHA256,
            }
        ],
    }


def _finalize_green_closes_red_kwargs(tmp_path: Path) -> dict[str, Any]:
    prepared = _prepare_kwargs(tmp_path)
    runtime = prepared["runtime_root"]
    predecessor = _predecessor_red_lock()
    _seed_main_state(
        runtime,
        _open_red_main_state(observed_main_head_sha=prepared["main_sha"]),
    )
    _write_yaml(runtime / "red_lock.yaml", predecessor)
    prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=runtime,
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    return {
        "repo_root": prepared["repo_root"],
        "runtime_root": runtime,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "task_snapshot": _reference_task_snapshot(),
        "instruction_snapshot": _reference_instruction_snapshot(),
        "expected_outcome": DirectGreenOutcome(kind="GREEN"),
        "allowed_paths": _REFERENCE_ALLOWED_PATHS,
        "risk_facts": _clear_risk_facts(),
        "lock_ownership": _owned_lock(),
        "git_preflight": _clean_preflight(),
        "consumed_payload": _empty_consumed(),
        "red_lock": predecessor,
        "unresolved_foreign_epoch": False,
        "timeout_seconds": 0,
        "poll_seconds": 1,
        "main_sha": prepared["main_sha"],
        "result_sha": prepared["result_sha"],
        "task_branch": prepared["task_branch"],
    }


def _closure_context(
    *,
    result_sha: str,
    observed_main_head_sha: str,
    live_lock: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    red_lock = dict(live_lock) if live_lock is not None else _live_red_lock(
        result_sha=result_sha
    )
    main_state = _reference_main_state(observed_main_head_sha=observed_main_head_sha)
    return {
        "promotion_class": "GREEN_PROMOTION",
        "main_state_before_promotion": main_state,
        "red_lock_closure": {
            "red_lock": red_lock,
            "main_state_before_promotion": copy.deepcopy(main_state),
        },
    }


def _assert_keyword_only(func: Any, required: tuple[str, ...]) -> None:
    signature = inspect.signature(func)
    assert list(signature.parameters) == list(required)
    for parameter in signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
        for forbidden in _FORBIDDEN_PARAMETERS:
            if (
                forbidden == "consume"
                and parameter.name == "consumed_payload"
            ):
                continue
            assert forbidden not in parameter.name


def test_module_surface_is_exactly_twelve_bound_symbols() -> None:
    module = _module()
    defined = _defined_symbols(module) - {_COMPOSITION_SYMBOL}
    assert defined == set(_BOUND_SYMBOLS)
    assert len(defined) == 12
    assert _COMPOSITION_SYMBOL not in _BOUND_SYMBOLS
    for name in _BOUND_SYMBOLS:
        assert hasattr(module, name)
    _assert_keyword_only(
        prepare_git_promotion_task_end_artifacts,
        _PREPARE_REQUIRED_PARAMETERS,
    )
    _assert_keyword_only(
        finalize_git_promotion_task_end_artifacts,
        _FINALIZE_REQUIRED_PARAMETERS,
    )
    _assert_keyword_only(_wait_for_ready_promotion_drafts, _WAIT_REQUIRED_PARAMETERS)
    _assert_keyword_only(
        _validate_ready_promotion_drafts,
        _VALIDATE_DRAFTS_REQUIRED_PARAMETERS,
    )


def test_prepare_verifies_result_and_persists_worker_result_and_decision_request(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    runtime = prepared["runtime_root"]
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    request_path = _decisions_dir(runtime) / _DECISION_REQUEST_NAME
    assert result["ok"] is True
    assert result["outcome"] == "WORKER_RESULT_READY"
    assert result["result_sha"] == prepared["result_sha"]
    assert result["replayed"] is False
    assert Path(result["worker_result_path"]) == worker_path
    assert Path(result["decision_request_path"]) == request_path
    loaded_result = load_worker_result(
        worker_path,
        expected_identity=WorkerResultIdentity(
            task_id=_REFERENCE_TASK_ID,
            origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
            promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
            result_sha=prepared["result_sha"],
        ),
        expected_validation_digests={_REFERENCE_COMMAND_ID: _REFERENCE_OUTPUT_SHA256},
    )
    assert loaded_result.result_sha == prepared["result_sha"]
    loaded_request = load_decision_request(
        request_path,
        expected_identity=_decision_identity(result_sha=prepared["result_sha"]),
        artifact_root=runtime,
    )
    assert loaded_request.request_type == "decision_request"
    roles = {item.role: item.path for item in loaded_request.required_decisions}
    assert roles == {
        "orchestrator": _ORCHESTRATOR_NAME,
        "validator": _VALIDATOR_NAME,
    }
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(runtime)


def test_prepare_replays_identical_verified_result_without_rewriting(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    first = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    worker_path = Path(first["worker_result_path"])
    before = worker_path.read_bytes()
    second = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert second["ok"] is True
    assert second["replayed"] is True
    assert second["outcome"] == "WORKER_RESULT_READY"
    assert worker_path.read_bytes() == before
    assert not (prepared["runtime_root"] / _CANONICAL_PENDING_NAME).exists()


def test_prepare_worker_success_is_not_promotion_authority(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is True
    assert prepared["worker_result"].worker_report.reported_status == "success"
    assert not (prepared["runtime_root"] / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(prepared["runtime_root"])


def test_prepare_blocks_result_sha_mismatch(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    foreign = dataclasses.replace(
        prepared["worker_result"],
        result_sha="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=foreign,
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_RESULT_SHA_MISMATCH"
    assert not (_run_directory(prepared["runtime_root"]) / _WORKER_RESULT_NAME).exists()


def test_prepare_blocks_non_runtime_dirty_paths(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    dirty = prepared["repo_root"] / "stray.txt"
    dirty.write_text("untracked\n", encoding="utf-8")
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["reason_code"] == "PROMOTION_GIT_PREFLIGHT_BLOCKED"
    assert "stray.txt" in _non_runtime_dirty_paths(prepared["repo_root"])
    assert dirty.is_file()


def test_prepare_blocks_changed_path_outside_allowed_paths(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    foreign_result = dataclasses.replace(
        prepared["worker_result"],
        changed_paths=("src/example.py", "secrets.env"),
    )
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=foreign_result,
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["reason_code"] == "PROMOTION_ALLOWED_PATHS_MISMATCH"


def test_missing_decisions_end_awaiting_without_pending(tmp_path: Path) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    assert result["ok"] is False
    assert result["outcome"] == "AWAITING_DECISIONS"
    assert result["reason_code"] in {
        "PROMOTION_DECISION_MISSING",
        "PROMOTION_DECISION_NOT_READY",
    }
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha


def test_draft_decisions_end_awaiting_without_pending(tmp_path: Path) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    result_sha = kwargs["result_sha"]
    _write_yaml(
        _decisions_dir(kwargs["runtime_root"]) / _ORCHESTRATOR_NAME,
        _decision_payload(kind="orchestrator", result_sha=result_sha, ready=False),
    )
    _write_yaml(
        _decisions_dir(kwargs["runtime_root"]) / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha, ready=False),
    )
    kwargs.pop("main_sha")
    kwargs.pop("result_sha")
    kwargs.pop("task_branch")
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "AWAITING_DECISIONS"
    assert result["reason_code"] == "PROMOTION_DECISION_NOT_READY"
    assert not (kwargs["runtime_root"] / _CANONICAL_PENDING_NAME).exists()


def test_ready_decisions_persist_non_authorizing_intent_and_return_to_main(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    _write_ready_decisions(runtime, result_sha=result_sha)
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    pending_path = runtime / _CANONICAL_PENDING_NAME
    assert result["ok"] is True
    assert result["outcome"] == "PENDING_PROMOTION"
    assert result["artifact_class"] == _ARTIFACT_CLASS
    assert result["reason_code"] == "PROMOTION_INTENT_PERSISTED"
    assert Path(result["pending_path"]) == pending_path
    loaded = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    assert loaded["artifact_class"] == _ARTIFACT_CLASS
    assert loaded["result_sha"] == result_sha
    assert loaded["task_id"] == _REFERENCE_TASK_ID
    assert loaded["origin_run_id"] == _REFERENCE_ORIGIN_RUN_ID
    assert loaded["promotion_attempt_id"] == _REFERENCE_PROMOTION_ATTEMPT_ID
    assert loaded["promotion_class"] == "GREEN_PROMOTION"
    assert "tock_sha" not in loaded
    assert "journal" not in loaded
    assert "candidate_tree_oid" not in loaded
    _assert_no_effect_files(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "rev-parse", "HEAD") == main_sha
    assert _run_git(repo, "rev-parse", f"refs/heads/{task_branch}") == result_sha
    assert _run_git(repo, "status", "--porcelain") == ""


def test_later_call_revalidates_ready_decisions_and_replays_matching_pending(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    result_sha = kwargs["result_sha"]
    _write_ready_decisions(kwargs["runtime_root"], result_sha=result_sha)
    call_kwargs = {
        key: value
        for key, value in kwargs.items()
        if key not in {"main_sha", "result_sha", "task_branch"}
    }
    first = finalize_git_promotion_task_end_artifacts(**call_kwargs)
    pending_path = Path(first["pending_path"])
    before = pending_path.read_bytes()
    reloaded_orchestrator = load_orchestrator_decision(
        _decisions_dir(kwargs["runtime_root"]) / _ORCHESTRATOR_NAME,
        expected_identity=_decision_identity(result_sha=result_sha),
        artifact_root=kwargs["runtime_root"],
    )
    reloaded_validator = load_validator_decision(
        _decisions_dir(kwargs["runtime_root"]) / _VALIDATOR_NAME,
        expected_identity=_decision_identity(result_sha=result_sha),
        artifact_root=kwargs["runtime_root"],
    )
    assert reloaded_orchestrator.ready is True
    assert reloaded_validator.verdict == "GO"
    second = finalize_git_promotion_task_end_artifacts(**call_kwargs)
    assert second["ok"] is True
    assert second["replayed"] is True
    assert second["outcome"] == "PENDING_PROMOTION"
    assert pending_path.read_bytes() == before
    _assert_no_effect_files(kwargs["runtime_root"])


def test_identity_mismatch_blocks_without_pending_or_main_mutation(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    foreign = _decision_payload(kind="orchestrator", result_sha=result_sha)
    foreign["result_sha"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    _write_yaml(_decisions_dir(runtime) / _ORCHESTRATOR_NAME, foreign)
    _write_yaml(
        _decisions_dir(runtime) / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha),
    )
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] in {
        "PROMOTION_DECISION_IDENTITY_MISMATCH",
        "PROMOTION_DECISION_INVALID",
    }
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


def test_dirty_return_to_main_is_fail_closed_and_preserves_user_file(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    repo = kwargs["repo_root"]
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    kwargs.pop("main_sha")
    _write_ready_decisions(kwargs["runtime_root"], result_sha=result_sha)
    stray = repo / "user-edit.txt"
    stray.write_text("keep me\n", encoding="utf-8")
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["reason_code"] == "PROMOTION_GIT_PREFLIGHT_BLOCKED"
    assert stray.is_file()
    assert stray.read_text(encoding="utf-8") == "keep me\n"
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert not (kwargs["runtime_root"] / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(kwargs["runtime_root"])


def test_direct_green_does_not_require_prior_red_lock(tmp_path: Path) -> None:
    kwargs = _finalize_kwargs(tmp_path, red_lock=None)
    result_sha = kwargs.pop("result_sha")
    kwargs.pop("main_sha")
    kwargs.pop("task_branch")
    _write_ready_decisions(kwargs["runtime_root"], result_sha=result_sha)
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is True
    assert result["artifact_class"] == _ARTIFACT_CLASS
    loaded = yaml.safe_load(
        (kwargs["runtime_root"] / _CANONICAL_PENDING_NAME).read_text(encoding="utf-8")
    )
    assert loaded["expected_outcome"]["kind"] == "GREEN"
    assert loaded.get("red_lock_closure") in (None, {})


def test_wait_for_ready_promotion_drafts_reports_awaiting_when_missing(
    tmp_path: Path,
) -> None:
    runtime = _runtime_root(tmp_path)
    _write_validation_log(runtime)
    context = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    }
    result = _wait_for_ready_promotion_drafts(
        context=context,
        timeout_seconds=0,
        poll_seconds=1,
        artifact_root=runtime,
    )
    assert result["ok"] is False
    assert result["outcome"] == "AWAITING_DECISIONS"
    assert result["reason_code"] in {
        "PROMOTION_DECISION_MISSING",
        "PROMOTION_DECISION_NOT_READY",
    }


def test_validate_ready_promotion_drafts_accepts_public_decisions() -> None:
    result_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    context = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": result_sha,
    }
    result = _validate_ready_promotion_drafts(
        context=context,
        orchestrator_state=DecisionArtifactState.READY,
        orchestrator_decision=_reference_orchestrator(result_sha=result_sha),
        validator_state=DecisionArtifactState.READY,
        validator_decision=_reference_validator(result_sha=result_sha),
    )
    assert result["ok"] is True
    assert set(result) >= {
        "ok",
        "orchestrator_decision",
        "validator_decision",
    }
    assert result["orchestrator_decision"].promotion_class == "GREEN_PROMOTION"
    assert result["validator_decision"].verdict == "GO"


def test_validate_ready_promotion_drafts_rejects_not_ready_and_mismatch() -> None:
    result_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    context = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": result_sha,
    }
    missing = _validate_ready_promotion_drafts(
        context=context,
        orchestrator_state=DecisionArtifactState.MISSING,
        orchestrator_decision=None,
        validator_state=DecisionArtifactState.MISSING,
        validator_decision=None,
    )
    assert missing["ok"] is False
    assert missing["outcome"] == "AWAITING_DECISIONS"
    mismatched = _validate_ready_promotion_drafts(
        context=context,
        orchestrator_state=DecisionArtifactState.READY,
        orchestrator_decision=_reference_orchestrator(
            result_sha="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ),
        validator_state=DecisionArtifactState.READY,
        validator_decision=_reference_validator(result_sha=result_sha),
    )
    assert mismatched["ok"] is False
    assert mismatched["outcome"] == "BLOCKED"
    assert mismatched["reason_code"] == "PROMOTION_DECISION_IDENTITY_MISMATCH"


def test_pending_matches_context_uses_public_identity_axes() -> None:
    context = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "promotion_class": "GREEN_PROMOTION",
        "task_branch": _REFERENCE_TASK_BRANCH,
        "main_state_before_promotion": {"state": "GREEN_VALIDATED"},
        "red_lock_closure": None,
    }
    matching = copy.deepcopy(context)
    matching["artifact_class"] = _ARTIFACT_CLASS
    assert _pending_matches_context(matching, context) is True
    drifted = copy.deepcopy(matching)
    drifted["result_sha"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    assert _pending_matches_context(drifted, context) is False
    assert "run_id" not in inspect.signature(_pending_matches_context).parameters


def test_red_lock_closure_helper_is_fail_closed(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path)
    green_context = {
        "promotion_class": "GREEN_PROMOTION",
        "main_state_before_promotion": {"state": "GREEN_VALIDATED"},
        "red_lock_closure": None,
    }
    assert (
        _validate_current_red_lock_closure(
            runtime_root=runtime,
            context=green_context,
        )
        is None
    )
    open_red = {
        "promotion_class": "GREEN_PROMOTION",
        "main_state_before_promotion": {"state": "EXPECTED_RED_OPEN"},
        "red_lock_closure": None,
    }
    blocked = _validate_current_red_lock_closure(
        runtime_root=runtime,
        context=open_red,
    )
    assert blocked is not None
    assert blocked["ok"] is False
    assert blocked["reason_code"] in {
        "PROMOTION_RED_LOCK_MISSING",
        "BLOCKED_UNCLEAR_RED_STATE",
    }


def test_yaml_and_git_helpers_are_local_and_atomic(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path)
    runtime = _runtime_root(tmp_path)
    payload = {"schema_version": "1.0", "token": "synthetic"}
    target = runtime / "copied.yaml"
    source = runtime / "source.yaml"
    _atomic_write_yaml(source, payload)
    assert not source.with_suffix(source.suffix + ".tmp").exists()
    loaded = _load_yaml_mapping_if_present(source)
    assert loaded == payload
    assert _load_yaml_mapping_if_present(runtime / "missing.yaml") is None
    _copy_yaml_payload(source, target)
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == payload
    assert _git_stdout(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    (runtime / "outside.txt").write_text("runtime only\n", encoding="utf-8")
    dirty = _non_runtime_dirty_paths(repo)
    assert "dirty.txt" in dirty
    assert "outside.txt" not in dirty
    blocked = _promotion_blocked("PROMOTION_DECISION_MISSING")
    assert blocked == {
        "ok": False,
        "status": "blocked",
        "reason_code": "PROMOTION_DECISION_MISSING",
        "outcome": "BLOCKED",
    }


def test_expected_red_class_without_matching_outcome_does_not_authorise_pending(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    kwargs.pop("main_sha")
    red_payload = _decision_payload(kind="orchestrator", result_sha=result_sha)
    red_payload["promotion_class"] = "RED_TDD_PROMOTION"
    _write_yaml(_decisions_dir(kwargs["runtime_root"]) / _ORCHESTRATOR_NAME, red_payload)
    _write_yaml(
        _decisions_dir(kwargs["runtime_root"]) / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha),
    )
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] in {
        "PROMOTION_EXPECTED_OUTCOME_MISMATCH",
        "PROMOTION_RED_LOCK_MISSING",
    }
    assert not (kwargs["runtime_root"] / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(kwargs["runtime_root"])
    assert (
        _run_git(kwargs["repo_root"], "rev-parse", "--abbrev-ref", "HEAD")
        == task_branch
    )


def test_prepare_blocks_when_head_sha_matches_foreign_branch_name(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = prepared["repo_root"]
    _run_git(repo, "checkout", "-B", "runner/task/synthetic-task-001/other")
    assert _run_git(repo, "rev-parse", "HEAD") == prepared["result_sha"]
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_RESULT_CONTRADICTION"
    _assert_prepare_did_not_persist(prepared["runtime_root"])


def test_prepare_blocks_detached_head_even_when_sha_matches(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = prepared["repo_root"]
    _run_git(repo, "checkout", "--detach", prepared["result_sha"])
    assert _run_git(repo, "rev-parse", "HEAD") == prepared["result_sha"]
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_RESULT_CONTRADICTION"
    _assert_prepare_did_not_persist(prepared["runtime_root"])


@pytest.mark.parametrize(
    ("ready_kind", "other_kind", "other_mode"),
    [
        ("orchestrator", "validator", "missing"),
        ("orchestrator", "validator", "draft"),
        ("validator", "orchestrator", "missing"),
        ("validator", "orchestrator", "draft"),
    ],
)
def test_asymmetric_decisions_remain_awaiting_without_pending_or_main_checkout(
    tmp_path: Path,
    ready_kind: str,
    other_kind: str,
    other_mode: str,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    decisions = _decisions_dir(runtime)
    _write_yaml(
        decisions / (
            _ORCHESTRATOR_NAME if ready_kind == "orchestrator" else _VALIDATOR_NAME
        ),
        _decision_payload(kind=ready_kind, result_sha=result_sha, ready=True),
    )
    if other_mode == "draft":
        _write_yaml(
            decisions
            / (_ORCHESTRATOR_NAME if other_kind == "orchestrator" else _VALIDATOR_NAME),
            _decision_payload(kind=other_kind, result_sha=result_sha, ready=False),
        )
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "AWAITING_DECISIONS"
    assert result["reason_code"] in {
        "PROMOTION_DECISION_MISSING",
        "PROMOTION_DECISION_NOT_READY",
    }
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


@pytest.mark.parametrize("mode", ["malformed", "non_mapping"])
def test_invalid_decision_artifact_blocks_instead_of_awaiting(
    tmp_path: Path,
    mode: str,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    decisions = _decisions_dir(runtime)
    _write_yaml(
        decisions / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha),
    )
    invalid_path = decisions / _ORCHESTRATOR_NAME
    if mode == "malformed":
        invalid_path.write_text("{not-yaml", encoding="utf-8")
    else:
        invalid_path.write_text("- just-a-list\n", encoding="utf-8")
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_DECISION_INVALID"
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


def test_divergent_worker_result_persist_is_fail_closed_and_byte_stable(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    first = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    worker_path = Path(first["worker_result_path"])
    before = worker_path.read_bytes()
    mutated = yaml.safe_load(before.decode("utf-8"))
    mutated["worker_report"]["summary"] = "divergent persisted summary"
    worker_path.write_text(
        yaml.safe_dump(mutated, sort_keys=False),
        encoding="utf-8",
    )
    divergent = worker_path.read_bytes()
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_RESULT_CONTRADICTION"
    assert worker_path.read_bytes() == divergent
    _assert_no_pending_or_effects(prepared["runtime_root"])


def test_divergent_decision_request_persist_is_fail_closed_and_byte_stable(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    first = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    request_path = Path(first["decision_request_path"])
    mutated = yaml.safe_load(request_path.read_text(encoding="utf-8"))
    mutated["evidence"][0]["sha256"] = "b" * 64
    request_path.write_text(
        yaml.safe_dump(mutated, sort_keys=False),
        encoding="utf-8",
    )
    divergent = request_path.read_bytes()
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_DECISION_CONTRADICTION"
    assert request_path.read_bytes() == divergent
    _assert_no_pending_or_effects(prepared["runtime_root"])


def test_finalize_without_prepare_blocks_without_pending_or_main_checkout(
    tmp_path: Path,
) -> None:
    repo = _init_target_repo(tmp_path)
    main_sha = _run_git(repo, "rev-parse", "HEAD")
    runtime = _runtime_root(tmp_path)
    _seed_main_state(runtime, _reference_main_state(observed_main_head_sha=main_sha))
    result = finalize_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=runtime,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        task_snapshot=_reference_task_snapshot(),
        instruction_snapshot=_reference_instruction_snapshot(),
        expected_outcome=DirectGreenOutcome(kind="GREEN"),
        allowed_paths=_REFERENCE_ALLOWED_PATHS,
        risk_facts=_clear_risk_facts(),
        lock_ownership=_owned_lock(),
        git_preflight=_clean_preflight(),
        consumed_payload=_empty_consumed(),
        red_lock=None,
        unresolved_foreign_epoch=False,
        timeout_seconds=0,
        poll_seconds=1,
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_RESULT_SHA_MISSING"
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "rev-parse", "HEAD") == main_sha


def test_live_red_lock_missing_with_bound_closure_blocks_before_pending(
    tmp_path: Path,
) -> None:
    runtime = _runtime_root(tmp_path)
    observed = "ffffffffffffffffffffffffffffffffffffffff"
    context = _closure_context(
        result_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        observed_main_head_sha=observed,
    )
    _seed_main_state(runtime, context["main_state_before_promotion"])
    blocked = _validate_current_red_lock_closure(
        runtime_root=runtime,
        context=context,
    )
    assert blocked is not None
    assert blocked["ok"] is False
    assert blocked["reason_code"] == "PROMOTION_RED_LOCK_MISSING"
    _assert_no_pending_or_effects(runtime)


def test_live_red_lock_identity_drift_blocks_before_pending(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path)
    observed = "ffffffffffffffffffffffffffffffffffffffff"
    bound = _live_red_lock(result_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    live = _live_red_lock(result_sha="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    context = _closure_context(
        result_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        observed_main_head_sha=observed,
        live_lock=bound,
    )
    _seed_main_state(runtime, context["main_state_before_promotion"])
    _write_yaml(runtime / "red_lock.yaml", live)
    blocked = _validate_current_red_lock_closure(
        runtime_root=runtime,
        context=context,
    )
    assert blocked is not None
    assert blocked["ok"] is False
    assert blocked["reason_code"] == "PROMOTION_RED_LOCK_MISMATCH"
    _assert_no_pending_or_effects(runtime)


def test_bound_main_state_closure_drift_blocks_before_pending(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path)
    observed = "ffffffffffffffffffffffffffffffffffffffff"
    context = _closure_context(
        result_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        observed_main_head_sha=observed,
    )
    _write_yaml(
        runtime / "red_lock.yaml",
        context["red_lock_closure"]["red_lock"],
    )
    drifted = _reference_main_state(
        observed_main_head_sha="0123456789abcdef0123456789abcdef01234567"
    )
    _seed_main_state(runtime, drifted)
    blocked = _validate_current_red_lock_closure(
        runtime_root=runtime,
        context=context,
    )
    assert blocked is not None
    assert blocked["ok"] is False
    assert blocked["reason_code"] == "PROMOTION_MAIN_STATE_MISMATCH"
    _assert_no_pending_or_effects(runtime)


def test_load_yaml_mapping_if_present_returns_none_for_malformed_and_non_mapping(
    tmp_path: Path,
) -> None:
    runtime = _runtime_root(tmp_path)
    malformed = runtime / "malformed.yaml"
    malformed.write_text("{not-closed", encoding="utf-8")
    listed = runtime / "listed.yaml"
    listed.write_text("- 1\n- 2\n", encoding="utf-8")
    assert _load_yaml_mapping_if_present(malformed) is None
    assert _load_yaml_mapping_if_present(listed) is None
    _assert_no_pending_or_effects(runtime)


def test_copy_yaml_payload_missing_source_does_not_write_target(tmp_path: Path) -> None:
    runtime = _runtime_root(tmp_path)
    target = runtime / "copied.yaml"
    with pytest.raises(RuntimeError):
        _copy_yaml_payload(runtime / "missing.yaml", target)
    assert not target.exists()
    _assert_no_pending_or_effects(runtime)


def test_git_stdout_non_repo_and_cwd_decoy_fail_without_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decoy = _init_target_repo(tmp_path, name="decoy-repo")
    missing = tmp_path / "not-a-repo"
    missing.mkdir()
    monkeypatch.chdir(decoy)
    with pytest.raises(subprocess.CalledProcessError):
        _git_stdout(missing, "rev-parse", "HEAD")
    _assert_no_pending_or_effects(_runtime_root(tmp_path))


def test_wait_polls_until_timeout_and_stays_awaiting_when_drafts_stay_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime_root(tmp_path)
    _write_validation_log(runtime)
    module = _module()
    clock = {"now": 0.0}
    sleeps: list[float] = []

    def fake_monotonic() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    result = _wait_for_ready_promotion_drafts(
        context={
            "task_id": _REFERENCE_TASK_ID,
            "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
            "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
            "result_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        timeout_seconds=2,
        poll_seconds=1,
        artifact_root=runtime,
    )
    assert result["ok"] is False
    assert result["outcome"] == "AWAITING_DECISIONS"
    assert result["reason_code"] in {
        "PROMOTION_DECISION_MISSING",
        "PROMOTION_DECISION_NOT_READY",
    }
    assert sleeps == [1, 1]
    assert clock["now"] >= 2
    _assert_no_pending_or_effects(runtime)


@pytest.mark.parametrize("mode", ["malformed", "non_mapping"])
def test_invalid_validator_with_ready_orchestrator_blocks_instead_of_awaiting(
    tmp_path: Path,
    mode: str,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    decisions = _decisions_dir(runtime)
    _write_yaml(
        decisions / _ORCHESTRATOR_NAME,
        _decision_payload(kind="orchestrator", result_sha=result_sha),
    )
    invalid_path = decisions / _VALIDATOR_NAME
    if mode == "malformed":
        invalid_path.write_text("{not-yaml", encoding="utf-8")
    else:
        invalid_path.write_text("- just-a-list\n", encoding="utf-8")
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_DECISION_INVALID"
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


@pytest.mark.parametrize(
    ("invalid_kind", "missing_kind"),
    [
        ("orchestrator", "validator"),
        ("validator", "orchestrator"),
    ],
)
def test_invalid_plus_missing_decision_is_invalid_not_awaiting(
    tmp_path: Path,
    invalid_kind: str,
    missing_kind: str,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    decisions = _decisions_dir(runtime)
    invalid_name = (
        _ORCHESTRATOR_NAME if invalid_kind == "orchestrator" else _VALIDATOR_NAME
    )
    decisions.joinpath(invalid_name).write_text("{not-yaml", encoding="utf-8")
    missing_name = (
        _ORCHESTRATOR_NAME if missing_kind == "orchestrator" else _VALIDATOR_NAME
    )
    assert not (decisions / missing_name).exists()
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["outcome"] != "AWAITING_DECISIONS"
    assert result["reason_code"] == "PROMOTION_DECISION_INVALID"
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


def test_foreign_worker_result_before_first_prepare_preserves_bytes(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    runtime = prepared["runtime_root"]
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    request_path = _decisions_dir(runtime) / _DECISION_REQUEST_NAME
    _write_yaml(
        worker_path,
        _foreign_worker_mapping(task_branch=prepared["task_branch"]),
    )
    before = worker_path.read_bytes()
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=runtime,
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_RESULT_CONTRADICTION"
    assert worker_path.read_bytes() == before
    assert not request_path.exists()
    _assert_no_pending_or_effects(runtime)


def test_foreign_decision_request_before_first_prepare_preserves_bytes(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    runtime = prepared["runtime_root"]
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    request_path = _decisions_dir(runtime) / _DECISION_REQUEST_NAME
    _write_yaml(request_path, _foreign_decision_request_mapping())
    before = request_path.read_bytes()
    result = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=runtime,
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_DECISION_CONTRADICTION"
    assert request_path.read_bytes() == before
    assert not worker_path.exists()
    _assert_no_pending_or_effects(runtime)


def test_matching_live_red_lock_and_bound_main_state_closure_succeeds(
    tmp_path: Path,
) -> None:
    runtime = _runtime_root(tmp_path)
    observed = "ffffffffffffffffffffffffffffffffffffffff"
    context = _green_closes_red_context(observed_main_head_sha=observed)
    _seed_main_state(runtime, context["main_state_before_promotion"])
    _write_yaml(runtime / "red_lock.yaml", context["red_lock_closure"]["red_lock"])
    assert (
        _validate_current_red_lock_closure(
            runtime_root=runtime,
            context=context,
        )
        is None
    )
    _assert_no_pending_or_effects(runtime)


def test_finalize_persists_green_closes_predecessor_red_lock_intent(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_green_closes_red_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    _write_ready_decisions(runtime, result_sha=result_sha)
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    pending_path = runtime / _CANONICAL_PENDING_NAME
    assert result["ok"] is True
    assert result["outcome"] == "PENDING_PROMOTION"
    assert result["artifact_class"] == _ARTIFACT_CLASS
    assert result["reason_code"] == "PROMOTION_INTENT_PERSISTED"
    loaded = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    assert loaded["artifact_class"] == _ARTIFACT_CLASS
    assert loaded["promotion_class"] == "GREEN_PROMOTION"
    assert loaded["expected_outcome"]["kind"] == "GREEN"
    assert loaded["main_state_before_promotion"]["state"] == "EXPECTED_RED_OPEN"
    closure = loaded["red_lock_closure"]
    assert closure is not None
    assert closure["closure_type"] == "green_red_lock_closure"
    assert closure["prior_red_result_sha"] == _PRIOR_RESULT_SHA
    assert closure["green_promotion_identity"] == {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": result_sha,
        "promotion_class": "GREEN_PROMOTION",
    }
    _assert_no_effect_files(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "rev-parse", "HEAD") == main_sha
    assert _run_git(repo, "rev-parse", f"refs/heads/{task_branch}") == result_sha


@pytest.mark.parametrize(
    "mode",
    ["missing_lock", "drifted_lock", "drifted_main"],
)
def test_finalize_live_red_lock_composition_blocks_before_pending(
    tmp_path: Path,
    mode: str,
) -> None:
    kwargs = _finalize_green_closes_red_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    live_lock = runtime / "red_lock.yaml"
    if mode == "missing_lock":
        live_lock.unlink()
        expected_code = "PROMOTION_RED_LOCK_MISSING"
    elif mode == "drifted_lock":
        drifted = _predecessor_red_lock()
        drifted["result_sha"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        _write_yaml(live_lock, drifted)
        expected_code = "PROMOTION_RED_LOCK_MISMATCH"
    else:
        _seed_main_state(
            runtime,
            _open_red_main_state(
                observed_main_head_sha="0123456789abcdef0123456789abcdef01234567"
            ),
        )
        expected_code = "PROMOTION_MAIN_STATE_MISMATCH"
    _write_ready_decisions(runtime, result_sha=result_sha)
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == expected_code
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


def test_wait_becomes_ready_after_first_poll_materializes_both_decisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime_root(tmp_path)
    _write_validation_log(runtime)
    result_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    module = _module()
    clock = {"now": 0.0}
    sleeps: list[float] = []

    def fake_monotonic() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds
        _write_ready_decisions(runtime, result_sha=result_sha)

    monkeypatch.setattr(module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    result = _wait_for_ready_promotion_drafts(
        context={
            "task_id": _REFERENCE_TASK_ID,
            "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
            "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
            "result_sha": result_sha,
        },
        timeout_seconds=2,
        poll_seconds=1,
        artifact_root=runtime,
    )
    assert result["ok"] is True
    assert result.get("outcome") != "AWAITING_DECISIONS"
    assert result["orchestrator_decision"].promotion_class == "GREEN_PROMOTION"
    assert result["validator_decision"].verdict == "GO"
    assert sleeps == [1]
    assert clock["now"] == 1
    _assert_no_pending_or_effects(runtime)


@pytest.mark.parametrize(
    "missing_name",
    [_WORKER_RESULT_NAME, _DECISION_REQUEST_NAME],
)
def test_prepare_completes_matching_partial_pair_in_both_directions(
    tmp_path: Path,
    missing_name: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    first = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    runtime = prepared["runtime_root"]
    worker_path = Path(first["worker_result_path"])
    request_path = Path(first["decision_request_path"])
    worker_bytes = worker_path.read_bytes()
    request_bytes = request_path.read_bytes()
    missing_path = (
        worker_path if missing_name == _WORKER_RESULT_NAME else request_path
    )
    kept_path = request_path if missing_path == worker_path else worker_path
    kept_bytes = kept_path.read_bytes()
    missing_path.unlink()
    second = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=runtime,
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    assert second["ok"] is True
    assert second["outcome"] == "WORKER_RESULT_READY"
    assert second["replayed"] is False
    assert kept_path.read_bytes() == kept_bytes
    assert worker_path.read_bytes() == worker_bytes
    assert request_path.read_bytes() == request_bytes
    _assert_no_pending_or_effects(runtime)


def test_finalize_persist_failure_keeps_task_branch_and_does_not_leak_keyerror(
    tmp_path: Path,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    _write_ready_decisions(runtime, result_sha=result_sha)
    (runtime / "pending_promotion.json").write_text(
        '{"artifact_class": "foreign"}\n',
        encoding="utf-8",
    )
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["reason_code"] == "PROMOTION_PENDING_AMBIGUOUS"
    assert result.get("outcome", "BLOCKED") == "BLOCKED"
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()
    _assert_no_effect_files(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha


def test_finalize_persists_expected_red_without_live_red_lock_file(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    red_worker = dataclasses.replace(
        prepared["worker_result"],
        validation_results=(
            ValidationResult(
                command_id=_REFERENCE_COMMAND_ID,
                exit_code=1,
                output_ref=_REFERENCE_OUTPUT_REF,
                output_sha256=_REFERENCE_OUTPUT_SHA256,
            ),
        ),
        worker_report=WorkerReport(
            reported_status="failed",
            summary="synthetic expected red",
        ),
    )
    prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=red_worker,
        allowed_paths=prepared["allowed_paths"],
    )
    runtime = prepared["runtime_root"]
    repo = prepared["repo_root"]
    result_sha = prepared["result_sha"]
    red_payload = _decision_payload(kind="orchestrator", result_sha=result_sha)
    red_payload["promotion_class"] = "RED_TDD_PROMOTION"
    _write_yaml(_decisions_dir(runtime) / _ORCHESTRATOR_NAME, red_payload)
    _write_yaml(
        _decisions_dir(runtime) / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha),
    )
    assert not (runtime / "red_lock.yaml").exists()
    result = finalize_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=runtime,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        task_snapshot=_reference_task_snapshot(),
        instruction_snapshot=_reference_instruction_snapshot(),
        expected_outcome=ExpectedRedOutcome(
            kind="EXPECTED_RED",
            command_id=_REFERENCE_COMMAND_ID,
            failure_signature=_REFERENCE_FAILURE_SIGNATURE,
        ),
        allowed_paths=_REFERENCE_ALLOWED_PATHS,
        risk_facts=_clear_risk_facts(),
        lock_ownership=_owned_lock(),
        git_preflight=_clean_preflight(),
        consumed_payload=_empty_consumed(),
        red_lock=_live_red_lock(result_sha=result_sha),
        unresolved_foreign_epoch=False,
        timeout_seconds=0,
        poll_seconds=1,
    )
    pending_path = runtime / _CANONICAL_PENDING_NAME
    assert result["ok"] is True
    assert result["outcome"] == "PENDING_PROMOTION"
    assert result["artifact_class"] == _ARTIFACT_CLASS
    assert result["reason_code"] == "PROMOTION_INTENT_PERSISTED"
    loaded = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    assert loaded["artifact_class"] == _ARTIFACT_CLASS
    assert loaded["promotion_class"] == "RED_TDD_PROMOTION"
    assert loaded["expected_main_state_after_promotion"] == "EXPECTED_RED_OPEN"
    assert loaded["expected_outcome"]["kind"] == "EXPECTED_RED"
    assert loaded["expected_outcome"]["failure_signature"] == _REFERENCE_FAILURE_SIGNATURE
    assert loaded.get("red_lock_closure") in (None, {})
    assert not (runtime / "red_lock.yaml").exists()
    _assert_no_effect_files(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _run_git(repo, "rev-parse", "HEAD") == prepared["main_sha"]
    assert (
        _run_git(repo, "rev-parse", f"refs/heads/{prepared['task_branch']}")
        == result_sha
    )


def test_wait_blocks_invalid_after_first_poll_instead_of_awaiting_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime_root(tmp_path)
    _write_validation_log(runtime)
    result_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    module = _module()
    clock = {"now": 0.0}
    sleeps: list[float] = []

    def fake_monotonic() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds
        invalid_path = _decisions_dir(runtime) / _ORCHESTRATOR_NAME
        invalid_path.parent.mkdir(parents=True, exist_ok=True)
        invalid_path.write_text("{not-yaml", encoding="utf-8")

    monkeypatch.setattr(module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    result = _wait_for_ready_promotion_drafts(
        context={
            "task_id": _REFERENCE_TASK_ID,
            "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
            "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
            "result_sha": result_sha,
        },
        timeout_seconds=2,
        poll_seconds=1,
        artifact_root=runtime,
    )
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_DECISION_INVALID"
    assert result["outcome"] != "AWAITING_DECISIONS"
    assert sleeps == [1]
    assert clock["now"] == 1
    _assert_no_pending_or_effects(runtime)


def _compose():
    func = getattr(_module(), _COMPOSITION_SYMBOL, None)
    if func is None:
        pytest.fail(f"{_COMPOSITION_SYMBOL} is absent")
    return func


def _bound_bytes(payload: bytes) -> BoundPromptBytes:
    return BoundPromptBytes(
        payload_bytes=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_length=len(payload),
    )


def _event_counts() -> dict[str, int]:
    return {key: 1 for key in _EVENT_COUNT_KEYS}


def _allowlisted_projection(*, completed: bool) -> dict[str, Any]:
    if completed:
        return {
            "adapter_status": "completed",
            "neutral_reason_code": "ADAPTER_COMPLETED",
            "process_exit_code": 0,
            "timeout": False,
            "interrupted": False,
            "event_counts": _event_counts(),
            "session_observed": True,
            "terminal_success": True,
        }
    return {
        "adapter_status": "interrupted",
        "neutral_reason_code": "ADAPTER_INTERRUPTED",
        "process_exit_code": 1,
        "timeout": False,
        "interrupted": True,
        "event_counts": _event_counts(),
        "session_observed": False,
        "terminal_success": False,
    }


def _outcome(
    *,
    completed: bool,
    extra: Mapping[str, Any] | None = None,
) -> CursorAdapterOutcome:
    projection = _allowlisted_projection(completed=completed)
    if extra is not None:
        projection.update(dict(extra))
    return CursorAdapterOutcome(
        publish_projection=projection,
        prompt_frame_matches=True,
        stdout_artifact=_CANARY_STDOUT,
        stderr_artifact=_CANARY_STDERR,
    )


def _git_changed_paths(repo: Path, base_sha: str) -> tuple[str, ...]:
    raw = _run_git(repo, "diff", "--name-only", base_sha, "HEAD")
    if raw == "":
        return ()
    return tuple(raw.splitlines())


def _product_canary() -> dict[str, str]:
    package = Path(inspect.getfile(_module())).resolve().parent
    return {
        relative: hashlib.sha256((package / relative).read_bytes()).hexdigest()
        for relative in _PRODUCT_CANARIES
    }


def _runtime_files(runtime: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    if not runtime.exists():
        return files
    for path in sorted(runtime.rglob("*")):
        if path.is_file():
            files[path.relative_to(runtime).as_posix()] = path.read_bytes()
    return files


def _git_canary(repo: Path) -> tuple[str, str]:
    return (
        _run_git(repo, "rev-parse", "HEAD"),
        _run_git(repo, "status", "--porcelain=v1", "-uall"),
    )


def _binding_pair() -> tuple[BoundPromptBytes, BoundPromptBytes]:
    return (
        _bound_bytes(f"{_CANARY_PROMPT}\n".encode("utf-8")),
        _bound_bytes(f"{_CANARY_SPEC}\n".encode("utf-8")),
    )


def _raw_projection_extras() -> dict[str, Any]:
    return {
        "session_id": _CANARY_SESSION,
        "argv": [_CANARY_ARGV],
        "stdout": _CANARY_STDOUT,
        "stderr": _CANARY_STDERR,
        "prompt": _CANARY_PROMPT,
        "task_spec": _CANARY_SPEC,
        "result_sha": _FORGED_RESULT_SHA,
        "changed_paths": ["secrets.env"],
        "promotion_authorized": True,
        "live_reread": _CANARY_LIVE,
    }


def _compose_kwargs(prepared: Mapping[str, Any], **overrides: Any) -> dict[str, Any]:
    developer, task_spec = _binding_pair()
    payload: dict[str, Any] = {
        "repo_root": prepared["repo_root"],
        "runtime_root": prepared["runtime_root"],
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "task_branch": prepared["task_branch"],
        "developer": developer,
        "task_spec": task_spec,
        "adapter_outcome": _outcome(completed=True, extra=_raw_projection_extras()),
        "allowed_paths": prepared["allowed_paths"],
        "claimed_identity": None,
    }
    payload.update(overrides)
    return payload


def _jsonable(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _jsonable(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _assert_projection_closed(value: object) -> None:
    text = yaml.safe_dump(_jsonable(value), sort_keys=True)
    for canary in (
        _CANARY_PROMPT,
        _CANARY_SPEC,
        _CANARY_ARGV,
        _CANARY_SESSION,
        _CANARY_STDOUT,
        _CANARY_STDERR,
        _CANARY_LIVE,
        _FORGED_RESULT_SHA,
    ):
        assert canary not in text
    assert "session_id" not in text
    assert "prompt_frame_matches" not in text
    assert "promotion_authorized" not in text
    assert "secrets.env" not in text


def _assert_frozen(
    *,
    repo: Path,
    runtime: Path,
    product: Mapping[str, str],
    git: tuple[str, str],
    files: Mapping[str, bytes],
) -> None:
    assert _product_canary() == dict(product)
    assert _git_canary(repo) == git
    assert _runtime_files(runtime) == dict(files)


def _assert_blocked(result: Mapping[str, Any], reason_code: str) -> None:
    assert set(result) == {
        "ok",
        "outcome",
        "reason_code",
        "replayed",
        "worker_rerun",
        "resumed",
        "promotion_authorized",
    }
    assert result["ok"] is False
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == reason_code
    assert result["replayed"] is False
    assert result["worker_rerun"] is False
    assert result["resumed"] is False
    assert result["promotion_authorized"] is False


def _stream_block_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "kind": _STREAM_BLOCK_KIND,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "resumed": False,
    }


def _expected_worker_result(prepared: Mapping[str, Any]) -> WorkerResult:
    return WorkerResult(
        schema_version=_REFERENCE_SCHEMA_VERSION,
        result_type="worker_result",
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        task_branch=str(prepared["task_branch"]),
        result_sha=str(prepared["result_sha"]),
        changed_paths=_git_changed_paths(
            Path(prepared["repo_root"]),
            str(prepared["main_sha"]),
        ),
        validation_results=(),
        worker_report=WorkerReport(
            reported_status="success",
            summary="ADAPTER_COMPLETED",
        ),
    )


def test_worker_result_and_adapter_outcome_fields_stay_closed() -> None:
    assert {
        field.name for field in dataclasses.fields(WorkerResult)
    } == _WORKER_RESULT_FIELDS
    assert {
        field.name for field in dataclasses.fields(WorkerReport)
    } == {"reported_status", "summary"}
    assert {
        field.name for field in dataclasses.fields(CursorAdapterOutcome)
    } == _ADAPTER_OUTCOME_FIELDS


def test_compose_adapter_git_worker_result_is_keyword_only() -> None:
    _assert_keyword_only(_compose(), _COMPOSE_PARAMETERS)
    for parameter in inspect.signature(_compose()).parameters.values():
        assert parameter.default is inspect.Parameter.empty


def test_compose_derives_git_result_and_projects_allowlist_only(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    kwargs = _compose_kwargs(prepared)
    live_prompt = tmp_path / "live-developer.md"
    live_prompt.write_text(_CANARY_LIVE, encoding="utf-8")
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    outcome = kwargs["adapter_outcome"]
    expected_projection = _allowlisted_projection(completed=True)

    result = _compose()(**kwargs)

    assert set(result) == {
        "ok",
        "outcome",
        "reason_code",
        "replayed",
        "worker_rerun",
        "resumed",
        "promotion_authorized",
        "result_sha",
        "changed_paths",
        "worker_result",
        "publish_projection",
        "snapshot_binding",
    }
    assert result["ok"] is True
    assert result["outcome"] == "WORKER_RESULT_READY"
    assert result["reason_code"] == "ADAPTER_COMPLETED"
    assert result["replayed"] is False
    assert result["worker_rerun"] is True
    assert result["resumed"] is False
    assert result["promotion_authorized"] is False
    assert result["result_sha"] == prepared["result_sha"]
    assert tuple(result["changed_paths"]) == _git_changed_paths(repo, prepared["main_sha"])
    assert result["worker_result"] == _expected_worker_result(prepared)
    assert list(result["publish_projection"]) == list(_PUBLISHABLE_KEYS)
    assert result["publish_projection"] == expected_projection
    assert result["publish_projection"] is not outcome.publish_projection
    assert result["snapshot_binding"] == {
        "developer_sha256": kwargs["developer"].sha256,
        "developer_byte_length": kwargs["developer"].byte_length,
        "task_spec_sha256": kwargs["task_spec"].sha256,
        "task_spec_byte_length": kwargs["task_spec"].byte_length,
    }
    outcome.publish_projection["adapter_status"] = _CANARY_LIVE
    outcome.publish_projection["event_counts"]["assistant"] = 99
    assert result["publish_projection"] == expected_projection
    _assert_projection_closed(
        {
            "worker_result": result["worker_result"],
            "publish_projection": result["publish_projection"],
            "snapshot_binding": result["snapshot_binding"],
        }
    )
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()
    probe = tmp_path / "schema-probe"
    probe.mkdir()
    document = {
        "schema_version": result["worker_result"].schema_version,
        "result_type": result["worker_result"].result_type,
        "task_id": result["worker_result"].task_id,
        "origin_run_id": result["worker_result"].origin_run_id,
        "promotion_attempt_id": result["worker_result"].promotion_attempt_id,
        "task_branch": result["worker_result"].task_branch,
        "result_sha": result["worker_result"].result_sha,
        "changed_paths": list(result["worker_result"].changed_paths),
        "validation_results": [],
        "worker_report": {
            "reported_status": result["worker_result"].worker_report.reported_status,
            "summary": result["worker_result"].worker_report.summary,
        },
    }
    probe_path = probe / _WORKER_RESULT_NAME
    probe_path.write_text(
        yaml.safe_dump(document, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    loaded = load_worker_result(
        probe_path,
        expected_identity=WorkerResultIdentity(
            task_id=_REFERENCE_TASK_ID,
            origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
            promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
            result_sha=prepared["result_sha"],
        ),
        expected_validation_digests={},
    )
    assert loaded == result["worker_result"]
    assert live_prompt.read_text(encoding="utf-8") == _CANARY_LIVE


@pytest.mark.parametrize("axis", ["task_id", "origin_run_id", "promotion_attempt_id"])
def test_compose_blocks_identity_mismatch_without_mutation(
    tmp_path: Path,
    axis: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    claimed = WorkerResultIdentity(
        task_id=(
            "synthetic-task-foreign" if axis == "task_id" else _REFERENCE_TASK_ID
        ),
        origin_run_id=(
            "origin-run-foreign"
            if axis == "origin_run_id"
            else _REFERENCE_ORIGIN_RUN_ID
        ),
        promotion_attempt_id=(
            "promotion-attempt-foreign"
            if axis == "promotion_attempt_id"
            else _REFERENCE_PROMOTION_ATTEMPT_ID
        ),
        result_sha=str(prepared["result_sha"]),
    )

    result = _compose()(**_compose_kwargs(prepared, claimed_identity=claimed))

    _assert_blocked(result, "RESULT_IDENTITY_MISMATCH")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


@pytest.mark.parametrize("axis", ["result_sha", "task_branch"])
def test_compose_blocks_git_mismatch_without_mutation(
    tmp_path: Path,
    axis: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    overrides: dict[str, Any] = {}
    if axis == "result_sha":
        overrides["claimed_identity"] = WorkerResultIdentity(
            task_id=_REFERENCE_TASK_ID,
            origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
            promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
            result_sha=_FORGED_RESULT_SHA,
        )
    else:
        overrides["task_branch"] = "runner/task/synthetic-task-001/foreign-branch"

    result = _compose()(**_compose_kwargs(prepared, **overrides))

    _assert_blocked(result, "RESULT_GIT_MISMATCH")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)


def test_compose_blocks_paths_outside_allowed_without_mutation(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    secret = repo / "secrets.env"
    secret.write_text("synthetic-outside-path\n", encoding="utf-8")
    _run_git(repo, "add", "--", "secrets.env")
    _run_git(repo, "commit", "-m", "synthetic outside path")
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    assert "secrets.env" in _git_changed_paths(repo, prepared["main_sha"])

    result = _compose()(**_compose_kwargs(prepared))

    _assert_blocked(result, "RESULT_PATHS_OUTSIDE_ALLOWED")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert secret.is_file()


def test_pre_authorization_restart_reuses_exact_result_and_attempt(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    first_prepare = prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    before_worker = worker_path.read_bytes()
    before_commits = _run_git(repo, "rev-list", "--all").splitlines()
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)

    result = _compose()(**_compose_kwargs(prepared, adapter_outcome=None))
    second_prepare = prepare_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=runtime,
        worker_result=result["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    identity = result["worker_result"]

    assert first_prepare["replayed"] is False
    assert result["ok"] is True
    assert result["outcome"] == "WORKER_RESULT_READY"
    assert result["reason_code"] == "ADAPTER_COMPLETED"
    assert result["replayed"] is True
    assert result["worker_rerun"] is False
    assert result["resumed"] is False
    assert result["promotion_authorized"] is False
    assert result["result_sha"] == prepared["result_sha"]
    assert result["publish_projection"] is None
    assert (
        identity.task_id,
        identity.origin_run_id,
        identity.promotion_attempt_id,
        identity.result_sha,
    ) == (
        _REFERENCE_TASK_ID,
        _REFERENCE_ORIGIN_RUN_ID,
        _REFERENCE_PROMOTION_ATTEMPT_ID,
        prepared["result_sha"],
    )
    assert identity == prepared["worker_result"]
    assert second_prepare["ok"] is True
    assert second_prepare["replayed"] is True
    assert second_prepare["result_sha"] == prepared["result_sha"]
    assert worker_path.read_bytes() == before_worker
    assert _run_git(repo, "rev-list", "--all").splitlines() == before_commits
    _assert_projection_closed(result["worker_result"])
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_decisions_dir(runtime) / _ORCHESTRATOR_NAME).exists()
    assert not (_decisions_dir(runtime) / _VALIDATOR_NAME).exists()
    _assert_no_pending_or_effects(runtime)


def test_compose_blocks_missing_outcome_without_inventing_a_run(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)

    result = _compose()(**_compose_kwargs(prepared, adapter_outcome=None))

    _assert_blocked(result, "ADAPTER_INTERRUPTED")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


def test_compose_blocks_interrupted_stream_and_does_not_resume(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    marker = _run_directory(runtime) / _STREAM_BLOCK_NAME
    before_files = set(_runtime_files(runtime))

    first = _compose()(
        **_compose_kwargs(prepared, adapter_outcome=_outcome(completed=False))
    )

    _assert_blocked(first, "ADAPTER_INTERRUPTED")
    assert set(_runtime_files(runtime)) - before_files == {
        marker.relative_to(runtime).as_posix()
    }
    assert yaml.safe_load(marker.read_text(encoding="utf-8")) == _stream_block_payload()
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    after_block = _runtime_files(runtime)

    second = _compose()(**_compose_kwargs(prepared))

    _assert_blocked(second, "ADAPTER_INTERRUPTED")
    assert yaml.safe_load(marker.read_text(encoding="utf-8")) == _stream_block_payload()
    assert _runtime_files(runtime) == after_block
    assert _product_canary() == product
    assert _git_canary(repo) == git
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()


def test_compose_rejects_unsafe_projection_without_persistence(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    projection = _allowlisted_projection(completed=True)
    del projection["terminal_success"]
    projection["session_id"] = _CANARY_SESSION
    outcome = CursorAdapterOutcome(
        publish_projection=projection,
        prompt_frame_matches=True,
        stdout_artifact=_CANARY_STDOUT,
        stderr_artifact=_CANARY_STDERR,
    )

    result = _compose()(**_compose_kwargs(prepared, adapter_outcome=outcome))

    _assert_blocked(result, "ADAPTER_PROJECTION_UNSAFE")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


def _commit_followup(repo: Path, body: str) -> str:
    path = repo / "src" / "example.py"
    path.write_text(body, encoding="utf-8")
    _run_git(repo, "add", "--", "src/example.py")
    _run_git(repo, "commit", "-m", "synthetic follow-up result")
    return _run_git(repo, "rev-parse", "HEAD")


@pytest.mark.parametrize("fault", ["timeout", "prompt_frame", "snapshot_binding"])
def test_compose_blocks_timeout_frame_and_snapshot_faults_without_persistence(
    tmp_path: Path,
    fault: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    developer, task_spec = _binding_pair()
    outcome = _outcome(completed=True)
    if fault == "timeout":
        projection = _allowlisted_projection(completed=True)
        projection["timeout"] = True
        outcome = CursorAdapterOutcome(
            publish_projection=projection,
            prompt_frame_matches=True,
            stdout_artifact=_CANARY_STDOUT,
            stderr_artifact=_CANARY_STDERR,
        )
    elif fault == "prompt_frame":
        outcome = CursorAdapterOutcome(
            publish_projection=_allowlisted_projection(completed=True),
            prompt_frame_matches=False,
            stdout_artifact=_CANARY_STDOUT,
            stderr_artifact=_CANARY_STDERR,
        )
    else:
        developer = BoundPromptBytes(
            payload_bytes=developer.payload_bytes,
            sha256="0" * 64,
            byte_length=developer.byte_length,
        )
        assert developer.sha256 != hashlib.sha256(developer.payload_bytes).hexdigest()
        assert task_spec.sha256 == hashlib.sha256(task_spec.payload_bytes).hexdigest()

    result = _compose()(
        **_compose_kwargs(
            prepared,
            developer=developer,
            task_spec=task_spec,
            adapter_outcome=outcome,
        )
    )

    _assert_blocked(result, "ADAPTER_PROJECTION_UNSAFE")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


@pytest.mark.parametrize("mode", ["head_drift", "old_stream_block"])
def test_compose_replay_respects_head_drift_and_old_stream_block(
    tmp_path: Path,
    mode: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    marker = _run_directory(runtime) / _STREAM_BLOCK_NAME
    if mode == "head_drift":
        drifted = _commit_followup(repo, "value = 2\n")
        assert drifted != prepared["result_sha"]
    else:
        marker.write_text(
            yaml.safe_dump(_stream_block_payload(), sort_keys=False),
            encoding="utf-8",
        )
    before_worker = worker_path.read_bytes()
    before_marker = marker.read_bytes() if marker.is_file() else None
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)

    result = _compose()(
        **_compose_kwargs(
            prepared,
            adapter_outcome=_outcome(completed=True, extra=_raw_projection_extras()),
        )
    )

    if mode == "head_drift":
        _assert_blocked(result, "RESULT_GIT_MISMATCH")
    else:
        assert result["ok"] is True
        assert result["outcome"] == "WORKER_RESULT_READY"
        assert result["reason_code"] == "ADAPTER_COMPLETED"
        assert result["replayed"] is True
        assert result["worker_rerun"] is False
        assert result["resumed"] is False
        assert result["promotion_authorized"] is False
        assert result["result_sha"] == prepared["result_sha"]
        assert result["publish_projection"] is None
        assert marker.read_bytes() == before_marker
    assert worker_path.read_bytes() == before_worker
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()


def test_compose_changed_paths_cover_every_commit_since_base(tmp_path: Path) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    _commit_followup(repo, "value = 2\n")
    prepared["result_sha"] = _run_git(repo, "rev-parse", "HEAD")
    full = _git_changed_paths(repo, str(prepared["main_sha"]))
    tip = tuple(
        _run_git(
            repo,
            "diff-tree",
            "--no-commit-id",
            "--name-only",
            "-r",
            "HEAD",
        ).splitlines()
    )
    assert full != tip
    assert set(full) == set(_REFERENCE_ALLOWED_PATHS)
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)

    result = _compose()(**_compose_kwargs(prepared))

    assert result["reason_code"] == "ADAPTER_COMPLETED"
    assert result["result_sha"] == prepared["result_sha"]
    assert tuple(result["changed_paths"]) == full
    assert result["worker_result"] == _expected_worker_result(prepared)
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)


@pytest.mark.parametrize(
    "fault",
    ["interrupted_terminal_success", "foreign_event_counts"],
)
def test_compose_blocks_contradictory_projection_without_persistence(
    tmp_path: Path,
    fault: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    projection = _allowlisted_projection(completed=True)
    if fault == "interrupted_terminal_success":
        projection["interrupted"] = True
        assert projection["terminal_success"] is True
    else:
        projection["event_counts"] = "foreign"
        assert not isinstance(projection["event_counts"], Mapping)
    outcome = CursorAdapterOutcome(
        publish_projection=projection,
        prompt_frame_matches=True,
        stdout_artifact=_CANARY_STDOUT,
        stderr_artifact=_CANARY_STDERR,
    )

    result = _compose()(**_compose_kwargs(prepared, adapter_outcome=outcome))

    _assert_blocked(result, "ADAPTER_PROJECTION_UNSAFE")
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


@pytest.mark.parametrize("axis", ["task_id", "origin_run_id", "promotion_attempt_id"])
def test_compose_classifies_persisted_foreign_identity_before_git_mismatch(
    tmp_path: Path,
    axis: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    foreign_identity = {
        "task_id": "synthetic-task-foreign",
        "origin_run_id": "origin-run-foreign",
        "promotion_attempt_id": "promotion-attempt-foreign",
    }
    identity = {
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
    }
    identity[axis] = foreign_identity[axis]
    persisted = dataclasses.replace(prepared["worker_result"], **identity)
    changed_paths = _git_changed_paths(repo, str(prepared["main_sha"]))
    assert persisted.result_sha == prepared["result_sha"]
    assert persisted.task_branch == prepared["task_branch"]
    assert persisted.changed_paths == changed_paths
    assert getattr(persisted, axis) == foreign_identity[axis]
    worker_path = _write_yaml(
        _run_directory(runtime) / _WORKER_RESULT_NAME,
        dataclasses.asdict(persisted),
    )
    reloaded = load_worker_result(
        worker_path,
        expected_identity=WorkerResultIdentity(
            task_id=persisted.task_id,
            origin_run_id=persisted.origin_run_id,
            promotion_attempt_id=persisted.promotion_attempt_id,
            result_sha=persisted.result_sha,
        ),
        expected_validation_digests={
            item.command_id: item.output_sha256
            for item in persisted.validation_results
        },
    )
    assert reloaded == persisted
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)

    result = _compose()(**_compose_kwargs(prepared, claimed_identity=None))

    _assert_blocked(result, "RESULT_IDENTITY_MISMATCH")
    assert worker_path.read_bytes() == files[
        worker_path.relative_to(runtime).as_posix()
    ]
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


def test_compose_blocks_defective_snapshot_binding_before_equal_replay(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    prepare_git_promotion_task_end_artifacts(
        repo_root=prepared["repo_root"],
        runtime_root=prepared["runtime_root"],
        worker_result=prepared["worker_result"],
        allowed_paths=prepared["allowed_paths"],
    )
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    before_worker = worker_path.read_bytes()
    product = _product_canary()
    git = _git_canary(repo)
    files = _runtime_files(runtime)
    developer, task_spec = _binding_pair()
    developer = BoundPromptBytes(
        payload_bytes=developer.payload_bytes,
        sha256="0" * 64,
        byte_length=developer.byte_length,
    )
    assert developer.sha256 != hashlib.sha256(developer.payload_bytes).hexdigest()
    assert task_spec.sha256 == hashlib.sha256(task_spec.payload_bytes).hexdigest()

    result = _compose()(
        **_compose_kwargs(
            prepared,
            developer=developer,
            task_spec=task_spec,
            adapter_outcome=_outcome(completed=True, extra=_raw_projection_extras()),
        )
    )

    _assert_blocked(result, "ADAPTER_PROJECTION_UNSAFE")
    assert result["replayed"] is False
    assert worker_path.read_bytes() == before_worker
    _assert_frozen(repo=repo, runtime=runtime, product=product, git=git, files=files)
    assert not (_run_directory(runtime) / _STREAM_BLOCK_NAME).exists()


def test_task_end_reproves_committed_secret_over_caller_all_clear(tmp_path: Path) -> None:
    repo = _init_target_repo(tmp_path, name="task-end-secret-risk")
    main_sha = _run_git(repo, "rev-parse", "HEAD")
    _run_git(repo, "checkout", "-b", _REFERENCE_TASK_BRANCH)
    source = repo / "src" / "example.py"
    test_path = repo / "tests" / "test_example.py"
    token = repo / "src" / "example.token"
    source.parent.mkdir(parents=True)
    test_path.parent.mkdir(parents=True)
    source.write_text("value = 1\n", encoding="utf-8")
    test_path.write_text("def test_example() -> None:\n    assert True\n", encoding="utf-8")
    token.write_text("synthetic-token\n", encoding="utf-8")
    _run_git(
        repo,
        "add",
        "--",
        "src/example.py",
        "tests/test_example.py",
        "src/example.token",
    )
    _run_git(repo, "commit", "-m", "synthetic task result")
    result_sha = _run_git(repo, "rev-parse", "HEAD")
    allowed_paths = _REFERENCE_ALLOWED_PATHS + ("src/example.token",)
    runtime = _runtime_root(tmp_path)
    _write_validation_log(runtime)
    _seed_main_state(runtime, _reference_main_state(observed_main_head_sha=main_sha))
    prepared = prepare_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=runtime,
        worker_result=_reference_worker_result(
            result_sha=result_sha,
            task_branch=_REFERENCE_TASK_BRANCH,
        ),
        allowed_paths=allowed_paths,
    )
    assert prepared["ok"] is True
    _run_git(repo, "checkout", "main")
    assert _run_git(repo, "status", "--porcelain", "--untracked-files=all") == ""
    assert token.exists() is False
    assert "src/example.token" in _run_git(repo, "diff", "--name-status", main_sha, result_sha)
    _write_ready_decisions(runtime, result_sha=result_sha)
    caller_all_clear = _clear_risk_facts()
    for value in caller_all_clear.values():
        assert type(value) is bool
        assert value is False
    result = finalize_git_promotion_task_end_artifacts(
        repo_root=repo,
        runtime_root=runtime,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        task_snapshot=_reference_task_snapshot(),
        instruction_snapshot=_reference_instruction_snapshot(),
        expected_outcome=DirectGreenOutcome(kind="GREEN"),
        allowed_paths=allowed_paths,
        risk_facts=caller_all_clear,
        lock_ownership=_owned_lock(),
        git_preflight=_clean_preflight(),
        consumed_payload=_empty_consumed(),
        red_lock=None,
        unresolved_foreign_epoch=False,
        timeout_seconds=0,
        poll_seconds=1,
    )
    assert result["ok"] is False
    assert result["status"] == "blocked"
    assert result["outcome"] == "BLOCKED"
    assert result["reason_code"] == "PROMOTION_SECRET_RISK"
    assert not (runtime / _CANONICAL_PENDING_NAME).exists()


def test_public_identity_decision_and_transition_contract_closure(
    tmp_path: Path,
) -> None:
    """Close public Task-End identity, decision, and transition edges."""

    def run(case_name: str, test_case: Any, *args: Any) -> None:
        test_case(tmp_path / case_name, *args)

    test_module_surface_is_exactly_twelve_bound_symbols()
    test_validate_ready_promotion_drafts_accepts_public_decisions()
    test_validate_ready_promotion_drafts_rejects_not_ready_and_mismatch()
    test_pending_matches_context_uses_public_identity_axes()

    run(
        "prepare-positive",
        test_prepare_verifies_result_and_persists_worker_result_and_decision_request,
    )
    run(
        "worker-nonauthority",
        test_prepare_worker_success_is_not_promotion_authority,
    )
    run("result-mismatch", test_prepare_blocks_result_sha_mismatch)
    run(
        "allowed-path-mismatch",
        test_prepare_blocks_changed_path_outside_allowed_paths,
    )
    run(
        "branch-contradiction",
        test_prepare_blocks_when_head_sha_matches_foreign_branch_name,
    )
    run(
        "detached-contradiction",
        test_prepare_blocks_detached_head_even_when_sha_matches,
    )
    run("missing-decisions", test_missing_decisions_end_awaiting_without_pending)
    run("draft-decisions", test_draft_decisions_end_awaiting_without_pending)
    run(
        "ready-decisions",
        test_ready_decisions_persist_non_authorizing_intent_and_return_to_main,
    )
    run(
        "ready-replay",
        test_later_call_revalidates_ready_decisions_and_replays_matching_pending,
    )
    run(
        "decision-identity-mismatch",
        test_identity_mismatch_blocks_without_pending_or_main_mutation,
    )
    for ready_kind, other_kind, other_mode in (
        ("orchestrator", "validator", "missing"),
        ("orchestrator", "validator", "draft"),
        ("validator", "orchestrator", "missing"),
        ("validator", "orchestrator", "draft"),
    ):
        run(
            f"asymmetric-{ready_kind}-{other_mode}",
            test_asymmetric_decisions_remain_awaiting_without_pending_or_main_checkout,
            ready_kind,
            other_kind,
            other_mode,
        )
    for mode in ("malformed", "non_mapping"):
        run(
            f"invalid-orchestrator-{mode}",
            test_invalid_decision_artifact_blocks_instead_of_awaiting,
            mode,
        )
        run(
            f"invalid-validator-{mode}",
            test_invalid_validator_with_ready_orchestrator_blocks_instead_of_awaiting,
            mode,
        )
    for invalid_kind, missing_kind in (
        ("orchestrator", "validator"),
        ("validator", "orchestrator"),
    ):
        run(
            f"invalid-{invalid_kind}-missing-{missing_kind}",
            test_invalid_plus_missing_decision_is_invalid_not_awaiting,
            invalid_kind,
            missing_kind,
        )
    run(
        "worker-result-contradiction",
        test_divergent_worker_result_persist_is_fail_closed_and_byte_stable,
    )
    run(
        "decision-request-contradiction",
        test_divergent_decision_request_persist_is_fail_closed_and_byte_stable,
    )
    run(
        "finalize-without-prepare",
        test_finalize_without_prepare_blocks_without_pending_or_main_checkout,
    )
    run(
        "outcome-class-mismatch",
        test_expected_red_class_without_matching_outcome_does_not_authorise_pending,
    )
    run("direct-green", test_direct_green_does_not_require_prior_red_lock)
    run(
        "expected-red",
        test_finalize_persists_expected_red_without_live_red_lock_file,
    )
    run(
        "green-closure",
        test_finalize_persists_green_closes_predecessor_red_lock_intent,
    )
    for mode in ("missing_lock", "drifted_lock", "drifted_main"):
        run(
            f"green-closure-{mode}",
            test_finalize_live_red_lock_composition_blocks_before_pending,
            mode,
        )


def test_worker_success_without_two_ready_bound_decisions_has_no_effect(
    tmp_path: Path,
) -> None:
    """Worker success is not two ready bound decisions and creates no effect."""

    for case_name in ("missing", "draft", "invalid", "identity_mismatch"):
        _assert_worker_success_without_two_ready_bound_decisions(
            tmp_path / case_name,
            case_name,
        )


def _assert_worker_success_without_two_ready_bound_decisions(
    tmp_path: Path,
    case_name: str,
) -> None:
    kwargs = _finalize_kwargs(tmp_path)
    runtime = kwargs["runtime_root"]
    repo = kwargs["repo_root"]
    main_sha = kwargs.pop("main_sha")
    result_sha = kwargs.pop("result_sha")
    task_branch = kwargs.pop("task_branch")
    loaded = load_worker_result(
        _run_directory(runtime) / _WORKER_RESULT_NAME,
        expected_identity=WorkerResultIdentity(
            task_id=_REFERENCE_TASK_ID,
            origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
            promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
            result_sha=result_sha,
        ),
        expected_validation_digests={
            _REFERENCE_COMMAND_ID: _REFERENCE_OUTPUT_SHA256,
        },
    )
    assert loaded.worker_report.reported_status == "success"
    assert loaded.result_sha == result_sha
    _write_incomplete_bound_decisions(runtime, result_sha=result_sha, case_name=case_name)
    result = finalize_git_promotion_task_end_artifacts(**kwargs)
    assert result["ok"] is False
    assert result["outcome"] != "PENDING_PROMOTION"
    assert result["reason_code"] == _incomplete_decision_reason(case_name)
    assert "pending_path" not in result
    _assert_no_pending_or_effects(runtime)
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == task_branch
    assert _run_git(repo, "rev-parse", "HEAD") == result_sha
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_sha
    assert _run_git(repo, "status", "--porcelain") == ""


def _write_incomplete_bound_decisions(
    runtime: Path,
    *,
    result_sha: str,
    case_name: str,
) -> None:
    if case_name == "missing":
        return
    decisions = _decisions_dir(runtime)
    if case_name == "draft":
        _write_yaml(
            decisions / _ORCHESTRATOR_NAME,
            _decision_payload(kind="orchestrator", result_sha=result_sha, ready=False),
        )
        _write_yaml(
            decisions / _VALIDATOR_NAME,
            _decision_payload(kind="validator", result_sha=result_sha, ready=False),
        )
        return
    if case_name == "invalid":
        _write_yaml(
            decisions / _VALIDATOR_NAME,
            _decision_payload(kind="validator", result_sha=result_sha),
        )
        (decisions / _ORCHESTRATOR_NAME).write_text("{not-yaml", encoding="utf-8")
        return
    foreign = _decision_payload(kind="orchestrator", result_sha=result_sha)
    foreign["result_sha"] = "b" * 40
    _write_yaml(decisions / _ORCHESTRATOR_NAME, foreign)
    _write_yaml(
        decisions / _VALIDATOR_NAME,
        _decision_payload(kind="validator", result_sha=result_sha),
    )


def _incomplete_decision_reason(case_name: str) -> str:
    if case_name == "missing":
        return "PROMOTION_DECISION_MISSING"
    if case_name == "draft":
        return "PROMOTION_DECISION_NOT_READY"
    if case_name == "identity_mismatch":
        return "PROMOTION_DECISION_INVALID"
    return "PROMOTION_DECISION_INVALID"


_PREDECISION_ALPHA = "predecision-alpha"
_PREDECISION_BETA = "predecision-beta"
_PREDECISION_GAMMA = "predecision-gamma"
_PREDECISION_EXPECTED_FAILURE = "PREDECISION_BOUND_EXPECTED_FAILURE"
_PREDECISION_WITNESS_LINE = re.compile(
    r"^(?P<label>\S+) merge=(?P<merge>\S*) tree=(?P<tree>\S*)$"
)
_PREDECISION_ORDER_SIGNATURE = "PREDECISION_VALIDATION_COMMAND_ORDER_NOT_BOUND"
_PREDECISION_GREEN_SIGNATURE = "PREDECISION_VALIDATION_GREEN_EVIDENCE_ABSENT"
_PREDECISION_EXPECTED_RED_SIGNATURE = "PREDECISION_VALIDATION_EXPECTED_RED_NOT_BOUND"
_PREDECISION_REPLAY_SIGNATURE = "PREDECISION_VALIDATION_REPLAY_RERAN_OR_SKIPPED"
_PREDECISION_MISSING_LOG_SIGNATURE = "PREDECISION_VALIDATION_MISSING_LOG_LOSES_AUTHORITY"
_PREDECISION_ALTERED_LOG_SIGNATURE = "PREDECISION_VALIDATION_ALTERED_LOG_LOSES_AUTHORITY"
_PREDECISION_REUSED_LOG_SIGNATURE = "PREDECISION_VALIDATION_REUSED_LOG_LOSES_AUTHORITY"


def _predecision_fail(signature: str) -> None:
    pytest.fail(signature)


def _predecision_script(path: Path, body: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path.resolve())


def _predecision_witness_command(
    path: Path,
    witness: Path,
    label: str,
    *,
    exit_code: int = 0,
    stdout: str = "",
) -> str:
    quoted_witness = str(witness).replace("'", "'\\''")
    quoted_stdout = stdout.replace("'", "'\\''")
    return _predecision_script(
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
                f"printf '%s\\n' '{quoted_stdout}'",
                f"exit {exit_code}",
                "",
            )
        ),
    )


def _predecision_snapshot(
    commands: tuple[dict[str, Any], ...],
    outcome: dict[str, Any],
) -> bytes:
    document = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": {
            "task_id": _REFERENCE_TASK_ID,
            "spec_ref": "./tasks/synthetic-task-001.md",
            "allowed_paths": list(_REFERENCE_ALLOWED_PATHS),
            "validation": {"commands": [dict(command) for command in commands]},
            "expected_outcome": dict(outcome),
            "adapter": {"kind": "cursor"},
            "metadata": {},
        },
    }
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=False).encode("utf-8")


def _predecision_rows(witness: Path) -> tuple[dict[str, str], ...]:
    if not witness.is_file():
        return ()
    rows: list[dict[str, str]] = []
    for line in witness.read_text(encoding="utf-8").splitlines():
        matched = _PREDECISION_WITNESS_LINE.fullmatch(line)
        if matched is None:
            continue
        rows.append(matched.groupdict())
    return tuple(rows)


def _predecision_command_identity(record: Mapping[str, Any], declared: str) -> bool:
    command = record.get("command_text", record.get("command"))
    return command == declared or command == [declared]


def _predecision_mappings(payload: Any) -> tuple[Mapping[str, Any], ...]:
    found: list[Mapping[str, Any]] = []

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            found.append(value)
            for child in value.values():
                walk(child)
            return
        if isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return tuple(found)


def _predecision_records(
    runtime: Path,
    command_ids: tuple[str, ...],
) -> dict[str, Mapping[str, Any]]:
    records: dict[str, Mapping[str, Any]] = {}
    run_dir = _run_directory(runtime)
    if not run_dir.is_dir():
        return records
    for path in sorted(run_dir.rglob("*.yaml")):
        if path.name == "task_snapshot.yaml":
            continue
        try:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            continue
        for mapping in _predecision_mappings(loaded):
            command_id = mapping.get("command_id")
            if command_id not in command_ids or "candidate_tree_oid" not in mapping:
                continue
            records[str(command_id)] = mapping
    return records


def _predecision_log_path(runtime: Path, output_ref: str) -> Path:
    candidate = Path(output_ref)
    if candidate.is_absolute():
        return candidate
    return runtime / output_ref


def _observe_predecision_compose(
    tmp_path: Path,
    commands: tuple[dict[str, Any], ...],
    outcome: dict[str, Any],
    witness: Path,
    *,
    persist: bool,
) -> dict[str, Any]:
    prepared = _prepare_kwargs(tmp_path)
    runtime = Path(prepared["runtime_root"])
    snapshot = _predecision_snapshot(commands, outcome)
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(snapshot)
    composed = _compose()(**_compose_kwargs(prepared))
    persisted = None
    if persist and composed.get("ok") is True and composed.get("worker_result") is not None:
        persisted = prepare_git_promotion_task_end_artifacts(
            repo_root=prepared["repo_root"],
            runtime_root=runtime,
            worker_result=composed["worker_result"],
            allowed_paths=prepared["allowed_paths"],
        )
    worker_payload = _load_yaml_mapping_if_present(
        _run_directory(runtime) / _WORKER_RESULT_NAME
    )
    decision_payload = _load_yaml_mapping_if_present(
        _decisions_dir(runtime) / _DECISION_REQUEST_NAME
    )
    return {
        "prepared_repo": prepared,
        "composed": composed,
        "persisted": persisted,
        "rows": _predecision_rows(witness),
        "records": _predecision_records(
            runtime,
            tuple(str(command["id"]) for command in commands),
        ),
        "worker_payload": worker_payload,
        "decision_payload": decision_payload,
        "runtime": runtime,
    }


def _predecision_validation_items(worker_payload: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    if worker_payload is None:
        return ()
    items = worker_payload.get("validation_results")
    if not isinstance(items, list):
        return ()
    return tuple(item for item in items if isinstance(item, Mapping))


def _predecision_chain_matches(
    observed: Mapping[str, Any],
    commands: tuple[dict[str, Any], ...],
    *,
    exits: tuple[int, ...],
    failure_text: str | None = None,
) -> bool:
    prepared = observed["prepared_repo"]
    rows = tuple(observed["rows"])
    records = observed["records"]
    items = _predecision_validation_items(observed["worker_payload"])
    decision = observed["decision_payload"]
    if len(rows) != len(commands) or len(items) != len(commands):
        return False
    if set(records) != {str(command["id"]) for command in commands}:
        return False
    evidence = ()
    if isinstance(decision, Mapping) and isinstance(decision.get("evidence"), list):
        evidence = tuple(
            item for item in decision["evidence"] if isinstance(item, Mapping)
        )
    if len(evidence) != len(commands):
        return False
    runtime = Path(observed["runtime"])
    result_sha = str(prepared["result_sha"])
    for index, command in enumerate(commands):
        command_id = str(command["id"])
        row = rows[index]
        record = records[command_id]
        item = items[index]
        log_path = _predecision_log_path(runtime, str(item.get("output_ref", "")))
        if not log_path.is_file():
            return False
        log_bytes = log_path.read_bytes()
        digest = hashlib.sha256(log_bytes).hexdigest()
        if row["label"] != command_id or row["merge"] != result_sha or row["tree"] == "":
            return False
        if str(record.get("candidate_tree_oid")) != row["tree"]:
            return False
        if not _predecision_command_identity(record, str(command["command"])):
            return False
        if int(record.get("exit_code", -999)) != exits[index]:
            return False
        if item.get("command_id") != command_id or int(item.get("exit_code", -999)) != exits[index]:
            return False
        if item.get("output_sha256") != digest or str(record.get("output_sha256", digest)) != digest:
            return False
        if evidence[index].get("sha256") != digest:
            return False
        if evidence[index].get("ref") != item.get("output_ref"):
            return False
        if failure_text is not None and command_id == _PREDECISION_BETA:
            if failure_text.encode("utf-8") not in log_bytes or exits[index] == 0:
                return False
    trees = {row["tree"] for row in rows}
    return len(trees) == 1


def _predecision_success_commands(
    directory: Path,
    witness: Path,
    labels: tuple[str, ...],
) -> tuple[dict[str, Any], ...]:
    commands: list[dict[str, Any]] = []
    for label in labels:
        command = _predecision_witness_command(directory / f"{label}.sh", witness, label)
        commands.append({"id": label, "command": command, "timeout_seconds": 30})
    return tuple(commands)


def _predecision_log_files(
    observed: Mapping[str, Any],
    commands: tuple[dict[str, Any], ...],
) -> tuple[Path, ...] | None:
    items = _predecision_validation_items(observed["worker_payload"])
    if len(items) != len(commands):
        return None
    runtime = Path(observed["runtime"])
    paths: list[Path] = []
    for index, command in enumerate(commands):
        item = items[index]
        if item.get("command_id") != command["id"]:
            return None
        log_path = _predecision_log_path(runtime, str(item.get("output_ref", "")))
        if not log_path.is_file():
            return None
        if hashlib.sha256(log_path.read_bytes()).hexdigest() != item.get("output_sha256"):
            return None
        paths.append(log_path)
    return tuple(paths)


def _replay_predecision_compose(observed: Mapping[str, Any]) -> dict[str, Any]:
    prepared = observed["prepared_repo"]
    return _compose()(
        **_compose_kwargs(prepared, adapter_outcome=None),
    )


def test_predecision_commands_run_once_in_declared_order_on_merge_candidate(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "order-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    if not _predecision_chain_matches(observed, commands, exits=(0, 0)):
        _predecision_fail(_PREDECISION_ORDER_SIGNATURE)


def test_predecision_green_requires_every_command_success_before_decision(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "green-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    items = _predecision_validation_items(observed["worker_payload"])
    exits_ok = len(items) == len(commands) and all(
        int(item.get("exit_code", -999)) == 0 for item in items
    )
    if not exits_ok or not _predecision_chain_matches(observed, commands, exits=(0, 0)):
        _predecision_fail(_PREDECISION_GREEN_SIGNATURE)


def test_predecision_expected_red_binds_one_failure_and_green_remainder(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "expected-red-witness.txt"
    directory = tmp_path / "commands"
    commands = (
        {
            "id": _PREDECISION_ALPHA,
            "command": _predecision_witness_command(
                directory / "alpha.sh",
                witness,
                _PREDECISION_ALPHA,
            ),
            "timeout_seconds": 30,
        },
        {
            "id": _PREDECISION_BETA,
            "command": _predecision_witness_command(
                directory / "beta.sh",
                witness,
                _PREDECISION_BETA,
                exit_code=4,
                stdout=_PREDECISION_EXPECTED_FAILURE,
            ),
            "timeout_seconds": 30,
        },
        {
            "id": _PREDECISION_GAMMA,
            "command": _predecision_witness_command(
                directory / "gamma.sh",
                witness,
                _PREDECISION_GAMMA,
            ),
            "timeout_seconds": 30,
        },
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {
            "kind": "EXPECTED_RED",
            "command_id": _PREDECISION_BETA,
            "failure_signature": _PREDECISION_EXPECTED_FAILURE,
        },
        witness,
        persist=True,
    )
    if not _predecision_chain_matches(
        observed,
        commands,
        exits=(0, 4, 0),
        failure_text=_PREDECISION_EXPECTED_FAILURE,
    ):
        _predecision_fail(_PREDECISION_EXPECTED_RED_SIGNATURE)


def test_predecision_replay_does_not_rerun_commands(tmp_path: Path) -> None:
    witness = tmp_path / "replay-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    first_rows = tuple(observed["rows"])
    replayed = _replay_predecision_compose(observed)
    second_rows = _predecision_rows(witness)
    first_bound = _predecision_chain_matches(observed, commands, exits=(0, 0))
    if (
        not first_bound
        or second_rows != first_rows
        or replayed.get("replayed") is not True
        or replayed.get("worker_rerun") is not False
    ):
        _predecision_fail(_PREDECISION_REPLAY_SIGNATURE)


def test_predecision_missing_log_loses_authority(tmp_path: Path) -> None:
    witness = tmp_path / "missing-log-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    logs = _predecision_log_files(observed, commands)
    if logs is None:
        _predecision_fail(_PREDECISION_MISSING_LOG_SIGNATURE)
    for log_path in logs:
        log_path.unlink()
    replayed = _replay_predecision_compose(observed)
    if replayed.get("ok") is True or replayed.get("replayed") is True:
        _predecision_fail(_PREDECISION_MISSING_LOG_SIGNATURE)


def test_predecision_altered_log_loses_authority(tmp_path: Path) -> None:
    witness = tmp_path / "altered-log-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    logs = _predecision_log_files(observed, commands)
    if logs is None:
        _predecision_fail(_PREDECISION_ALTERED_LOG_SIGNATURE)
    altered = logs[0]
    altered.write_bytes(altered.read_bytes() + b"altered-log\n")
    replayed = _replay_predecision_compose(observed)
    if replayed.get("ok") is True or replayed.get("replayed") is True:
        _predecision_fail(_PREDECISION_ALTERED_LOG_SIGNATURE)


def test_predecision_reused_log_loses_authority(tmp_path: Path) -> None:
    witness = tmp_path / "reused-log-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    logs = _predecision_log_files(observed, commands)
    if logs is None or hashlib.sha256(logs[0].read_bytes()).hexdigest() == hashlib.sha256(
        logs[1].read_bytes()
    ).hexdigest():
        _predecision_fail(_PREDECISION_REUSED_LOG_SIGNATURE)
    logs[1].write_bytes(logs[0].read_bytes())
    replayed = _replay_predecision_compose(observed)
    if replayed.get("ok") is True or replayed.get("replayed") is True:
        _predecision_fail(_PREDECISION_REUSED_LOG_SIGNATURE)


def _predecision_operations():
    return _module()._PREDECISION_VALIDATION


def _predecision_manifest_path(runtime: Path) -> Path:
    return _run_directory(runtime) / "validation" / "manifest.yaml"


def _predecision_manifest(runtime: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(_predecision_manifest_path(runtime).read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _assert_task_branch_restored(prepared: Mapping[str, Any]) -> None:
    repo = Path(prepared["repo_root"])
    assert _run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == prepared["task_branch"]
    assert _run_git(repo, "rev-parse", "HEAD") == prepared["result_sha"]
    assert _run_git(repo, "status", "--porcelain=v1", "-uall") == ""
    git_dir = Path(_run_git(repo, "rev-parse", "--absolute-git-dir"))
    assert not (git_dir / "MERGE_HEAD").exists()


def _assert_validation_not_started(runtime: Path, witness: Path) -> None:
    assert not _predecision_manifest_path(runtime).exists()
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    assert _predecision_rows(witness) == ()


def _contract_snapshot(case_name: str) -> object:
    command = {"id": _PREDECISION_ALPHA, "command": "/bin/true", "timeout_seconds": 5}
    document: dict[str, Any] = {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "active_task": {
            "task_id": _REFERENCE_TASK_ID,
            "spec_ref": "./tasks/synthetic-task-001.md",
            "allowed_paths": list(_REFERENCE_ALLOWED_PATHS),
            "validation": {"commands": [dict(command)]},
            "expected_outcome": {"kind": "GREEN"},
            "adapter": {"kind": "cursor"},
            "metadata": {},
        },
    }
    active = document["active_task"]
    if case_name == "snapshot_not_mapping":
        return []
    if case_name == "active_task_identity_mismatch":
        active["task_id"] = "foreign-task"
    elif case_name == "validation_not_mapping":
        active["validation"] = []
    elif case_name == "commands_empty":
        active["validation"] = {"commands": []}
    elif case_name == "command_not_mapping":
        active["validation"] = {"commands": ["not-a-command"]}
    elif case_name == "command_fields_differ":
        active["validation"] = {
            "commands": [{"id": _PREDECISION_ALPHA, "command": "/bin/true"}]
        }
    elif case_name == "duplicate_command_id":
        active["validation"] = {"commands": [dict(command), dict(command)]}
    elif case_name == "outcome_not_mapping":
        active["expected_outcome"] = []
    elif case_name == "green_extra_field":
        active["expected_outcome"] = {"kind": "GREEN", "extra": "forbidden"}
    elif case_name == "expected_red_fields_differ":
        active["expected_outcome"] = {"kind": "EXPECTED_RED"}
    elif case_name == "expected_red_unknown_command":
        active["expected_outcome"] = {
            "kind": "EXPECTED_RED",
            "command_id": "missing-command",
            "failure_signature": _PREDECISION_EXPECTED_FAILURE,
        }
    elif case_name == "outcome_kind_unknown":
        active["expected_outcome"] = {"kind": "BLUE"}
    else:
        raise AssertionError(case_name)
    return document


@pytest.mark.parametrize(
    "case_name",
    (
        "snapshot_not_mapping",
        "active_task_identity_mismatch",
        "validation_not_mapping",
        "commands_empty",
        "command_not_mapping",
        "command_fields_differ",
        "duplicate_command_id",
        "outcome_not_mapping",
        "green_extra_field",
        "expected_red_fields_differ",
        "expected_red_unknown_command",
        "outcome_kind_unknown",
    ),
)
def test_predecision_invalid_snapshot_blocks_before_validation(
    tmp_path: Path,
    case_name: str,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    runtime = Path(prepared["runtime_root"])
    repo = Path(prepared["repo_root"])
    witness = tmp_path / "invalid-snapshot-witness.txt"
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_text(
        yaml.safe_dump(_contract_snapshot(case_name), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    before = _git_canary(repo)
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_CONTRACT_INVALID")
    assert _git_canary(repo) == before
    _assert_validation_not_started(runtime, witness)


def test_predecision_malformed_snapshot_yaml_blocks_before_validation(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    runtime = Path(prepared["runtime_root"])
    repo = Path(prepared["repo_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(b"[\n")
    before = _git_canary(repo)
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_CONTRACT_INVALID")
    assert _git_canary(repo) == before
    assert not _predecision_manifest_path(runtime).exists()


def test_predecision_unreadable_snapshot_blocks_before_validation(
    tmp_path: Path,
) -> None:
    prepared = _prepare_kwargs(tmp_path)
    runtime = Path(prepared["runtime_root"])
    repo = Path(prepared["repo_root"])
    snapshot = _run_directory(runtime) / "task_snapshot.yaml"
    snapshot.write_text("kind: blocked\n", encoding="utf-8")
    snapshot.chmod(0)
    before = _git_canary(repo)
    try:
        composed = _compose()(**_compose_kwargs(prepared))
    finally:
        snapshot.chmod(0o644)
    _assert_blocked(composed, "PREDECISION_VALIDATION_CONTRACT_INVALID")
    assert _git_canary(repo) == before
    assert not _predecision_manifest_path(runtime).exists()


def test_predecision_passed_manifest_replays_worker_without_rerun(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "manifest-replay-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA, _PREDECISION_BETA),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    assert _predecision_chain_matches(observed, commands, exits=(0, 0))
    runtime = Path(observed["runtime"])
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    worker_path.unlink()
    first_rows = tuple(observed["rows"])
    replayed = _replay_predecision_compose(observed)
    assert replayed["ok"] is True
    assert replayed["outcome"] == "WORKER_RESULT_READY"
    assert replayed["reason_code"] == "ADAPTER_COMPLETED"
    assert replayed["replayed"] is True
    assert replayed["worker_rerun"] is False
    assert replayed["promotion_authorized"] is False
    assert tuple(
        item.command_id for item in replayed["worker_result"].validation_results
    ) == (_PREDECISION_ALPHA, _PREDECISION_BETA)
    assert _predecision_rows(witness) == first_rows
    assert not worker_path.exists()


def test_predecision_worker_exit_drift_loses_verified_evidence(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "exit-drift-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA,),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    assert _predecision_chain_matches(observed, commands, exits=(0,))
    runtime = Path(observed["runtime"])
    worker_path = _run_directory(runtime) / _WORKER_RESULT_NAME
    payload = yaml.safe_load(worker_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    payload["validation_results"][0]["exit_code"] = 9
    worker_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    log_bytes = _predecision_log_files(observed, commands)
    assert log_bytes is not None
    original_log = log_bytes[0].read_bytes()
    first_rows = tuple(observed["rows"])
    replayed = _replay_predecision_compose(observed)
    _assert_blocked(replayed, "PREDECISION_VALIDATION_EVIDENCE_INVALID")
    assert _predecision_rows(witness) == first_rows
    assert log_bytes[0].read_bytes() == original_log
    reloaded = yaml.safe_load(worker_path.read_text(encoding="utf-8"))
    assert reloaded["validation_results"][0]["exit_code"] == 9


def _mutate_passed_manifest(observed: Mapping[str, Any], mutate) -> None:
    runtime = Path(observed["runtime"])
    path = _predecision_manifest_path(runtime)
    manifest = _predecision_manifest(runtime)
    mutate(manifest)
    path.write_text(
        yaml.safe_dump(manifest, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    (_run_directory(runtime) / _WORKER_RESULT_NAME).unlink()


@pytest.mark.parametrize(
    ("case_name", "reason_code"),
    (
        ("status_not_passed", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
        ("identity_mismatch", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
        ("records_not_list", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
        ("candidate_tree_missing", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
        ("record_not_mapping", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
        ("metadata_record_drift", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
        ("outcome_no_longer_matches", "PREDECISION_VALIDATION_EVIDENCE_INVALID"),
    ),
)
def test_predecision_manifest_tamper_loses_replay_authority(
    tmp_path: Path,
    case_name: str,
    reason_code: str,
) -> None:
    witness = tmp_path / f"{case_name}-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA,),
    )
    observed = _observe_predecision_compose(
        tmp_path / "repo",
        commands,
        {"kind": "GREEN"},
        witness,
        persist=True,
    )
    assert _predecision_chain_matches(observed, commands, exits=(0,))
    first_rows = tuple(observed["rows"])

    def mutate(manifest: dict[str, Any]) -> None:
        if case_name == "status_not_passed":
            manifest["status"] = "failed"
            return
        if case_name == "identity_mismatch":
            manifest["task_id"] = "foreign-task"
            return
        if case_name == "records_not_list":
            manifest["records"] = "not-a-list"
            return
        if case_name == "candidate_tree_missing":
            manifest["candidate_tree_oid"] = ""
            return
        if case_name == "record_not_mapping":
            manifest["records"][0] = "not-a-record"
            return
        if case_name == "metadata_record_drift":
            manifest["records"][0]["foreign_observer"] = "injected"
            return
        manifest["expected_outcome"] = {
            "kind": "EXPECTED_RED",
            "command_id": _PREDECISION_ALPHA,
            "failure_signature": _PREDECISION_EXPECTED_FAILURE,
        }
        snapshot = _predecision_snapshot(
            commands,
            {
                "kind": "EXPECTED_RED",
                "command_id": _PREDECISION_ALPHA,
                "failure_signature": _PREDECISION_EXPECTED_FAILURE,
            },
        )
        _run_directory(Path(observed["runtime"])).joinpath("task_snapshot.yaml").write_bytes(
            snapshot
        )

    _mutate_passed_manifest(observed, mutate)
    replayed = _replay_predecision_compose(observed)
    _assert_blocked(replayed, reason_code)
    assert _predecision_rows(witness) == first_rows


def test_predecision_existing_validation_file_is_write_once_conflict(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "write-once-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA,),
    )
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    digest = hashlib.sha256(_PREDECISION_ALPHA.encode("utf-8")).hexdigest()[:16]
    preexisting_log = _run_directory(runtime) / "validation" / f"000-{digest}.log"
    preexisting_log.parent.mkdir(parents=True, exist_ok=True)
    marker = b"kind: preexisting-validation\n"
    preexisting_log.write_bytes(marker)
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_WRITE_ONCE_CONFLICT")
    assert preexisting_log.read_bytes() == marker
    assert not _predecision_manifest_path(runtime).exists()
    assert _predecision_rows(witness) == ()
    _assert_task_branch_restored(prepared)


def test_predecision_manifest_temp_collision_is_write_once_conflict(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "manifest-temp-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA,),
    )
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    temporary = _predecision_manifest_path(runtime).with_suffix(".yaml.tmp")
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_bytes(b"temporary-manifest\n")
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_WRITE_ONCE_CONFLICT")
    assert len(_predecision_rows(witness)) == 1
    assert not _predecision_manifest_path(runtime).exists()
    assert temporary.read_bytes() == b"temporary-manifest\n"
    assert not (_run_directory(runtime) / _WORKER_RESULT_NAME).exists()
    _assert_task_branch_restored(prepared)


def test_predecision_log_temp_collision_fails_execution(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "log-temp-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA,),
    )
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    digest = hashlib.sha256(_PREDECISION_ALPHA.encode("utf-8")).hexdigest()[:16]
    temporary = _run_directory(runtime) / "validation" / f"000-{digest}.log.tmp"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_bytes(b"temporary-log\n")
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_EXECUTION_FAILED")
    assert _predecision_manifest(runtime)["reason_code"] == "PREDECISION_VALIDATION_EXECUTION_FAILED"
    assert _predecision_manifest(runtime)["status"] == "failed"
    assert len(_predecision_rows(witness)) == 1
    assert temporary.read_bytes() == b"temporary-log\n"
    _assert_task_branch_restored(prepared)


def test_predecision_merge_conflict_fails_candidate_and_restores(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "conflict-witness.txt"
    commands = _predecision_success_commands(
        tmp_path / "commands",
        witness,
        (_PREDECISION_ALPHA,),
    )
    prepared = _prepare_kwargs(tmp_path / "repo")
    repo = Path(prepared["repo_root"])
    _run_git(repo, "checkout", "main")
    conflict = repo / "src" / "example.py"
    conflict.parent.mkdir(parents=True, exist_ok=True)
    conflict.write_text("value = 2\n", encoding="utf-8")
    _run_git(repo, "add", "--", "src/example.py")
    _run_git(repo, "commit", "-m", "conflicting main edit")
    conflicted_main = _run_git(repo, "rev-parse", "HEAD")
    _run_git(repo, "checkout", str(prepared["task_branch"]))
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_CANDIDATE_FAILED")
    assert _predecision_manifest(runtime)["reason_code"] == "PREDECISION_VALIDATION_CANDIDATE_FAILED"
    assert _predecision_manifest(runtime)["status"] == "failed"
    assert _predecision_rows(witness) == ()
    assert _run_git(repo, "rev-parse", "refs/heads/main") == conflicted_main
    _assert_task_branch_restored(prepared)


def test_predecision_timeout_stops_and_restores_target(tmp_path: Path) -> None:
    witness = tmp_path / "timeout-witness.txt"
    command = _predecision_script(
        tmp_path / "commands" / "sleep.sh",
        "#!/bin/sh\npython3 -c 'import time; time.sleep(30)'\n",
    )
    commands = ({"id": "predecision-timeout", "command": command, "timeout_seconds": 1},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_TIMEOUT")
    manifest = _predecision_manifest(runtime)
    assert manifest["status"] == "failed"
    assert manifest["reason_code"] == "PREDECISION_VALIDATION_TIMEOUT"
    assert manifest["records"][0]["timed_out"] is True
    assert manifest["records"][0]["exit_code"] == -1
    _assert_task_branch_restored(prepared)
    assert not witness.exists()


def test_predecision_target_mutation_restores_worktree_index_and_hook(
    tmp_path: Path,
) -> None:
    witness = tmp_path / "mutation-witness.txt"
    quoted_witness = str(witness).replace("'", "'\\''")
    command = _predecision_script(
        tmp_path / "commands" / "mutate.sh",
        "\n".join(
            (
                "#!/bin/sh",
                "printf 'mutated\\n' >> tracked.txt",
                "git add -- tracked.txt",
                "hook=$(git rev-parse --absolute-git-dir)/hooks/pre-commit",
                "mkdir -p \"$(dirname \"$hook\")\"",
                "printf '%s\\n' '#!/bin/sh' 'exit 0' > \"$hook\"",
                "chmod +x \"$hook\"",
                "printf 'loose\\n' > loose.txt",
                f"printf '%s\\n' 'mutated' >> '{quoted_witness}'",
                "exit 0",
                "",
            )
        ),
    )
    commands = ({"id": "predecision-mutation", "command": command, "timeout_seconds": 30},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    repo = Path(prepared["repo_root"])
    runtime = Path(prepared["runtime_root"])
    main_before = _run_git(repo, "rev-parse", "refs/heads/main")
    tracked_before = (repo / "tracked.txt").read_text(encoding="utf-8")
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_TARGET_MUTATED")
    assert _predecision_manifest(runtime)["reason_code"] == "PREDECISION_VALIDATION_TARGET_MUTATED"
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == tracked_before
    assert not (repo / "loose.txt").exists()
    git_dir = Path(_run_git(repo, "rev-parse", "--absolute-git-dir"))
    assert not (git_dir / "hooks" / "pre-commit").exists()
    assert _run_git(repo, "rev-parse", "refs/heads/main") == main_before
    assert witness.read_text(encoding="utf-8").strip() == "mutated"
    _assert_task_branch_restored(prepared)


def test_predecision_missing_hooks_directory_still_validates_green(
    tmp_path: Path,
) -> None:
    commands = ({"id": _PREDECISION_ALPHA, "command": "/bin/true", "timeout_seconds": 5},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    repo = Path(prepared["repo_root"])
    git_dir = Path(_run_git(repo, "rev-parse", "--absolute-git-dir"))
    shutil.rmtree(git_dir / "hooks")
    assert _predecision_operations().snapshot_git_hooks(repo) == {}
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    assert composed["ok"] is True
    assert composed["replayed"] is False
    assert composed["worker_rerun"] is True
    assert composed["promotion_authorized"] is False
    assert composed["worker_result"].validation_results[0].exit_code == 0
    _assert_task_branch_restored(prepared)
    assert _predecision_operations().snapshot_git_hooks(repo) == {}


def test_predecision_unexpected_exit_is_outcome_mismatch(tmp_path: Path) -> None:
    command = _predecision_script(
        tmp_path / "commands" / "unexpected.sh",
        "#!/bin/sh\nprintf '%s\\n' 'UNEXPECTED_PREDECISION_FAILURE'\nexit 9\n",
    )
    commands = ({"id": "predecision-unexpected", "command": command, "timeout_seconds": 5},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_OUTCOME_MISMATCH")
    manifest = _predecision_manifest(runtime)
    assert manifest["status"] == "failed"
    assert manifest["reason_code"] == "PREDECISION_VALIDATION_OUTCOME_MISMATCH"
    assert manifest["records"][0]["exit_code"] == 9
    _assert_task_branch_restored(prepared)


def test_predecision_expected_red_with_zero_exit_is_outcome_mismatch(
    tmp_path: Path,
) -> None:
    commands = ({"id": _PREDECISION_BETA, "command": "/bin/true", "timeout_seconds": 5},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(
            commands,
            {
                "kind": "EXPECTED_RED",
                "command_id": _PREDECISION_BETA,
                "failure_signature": _PREDECISION_EXPECTED_FAILURE,
            },
        )
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_OUTCOME_MISMATCH")
    assert _predecision_manifest(runtime)["records"][0]["exit_code"] == 0
    _assert_task_branch_restored(prepared)


def test_predecision_expected_red_with_extra_failure_is_outcome_mismatch(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "commands"
    commands = (
        {
            "id": _PREDECISION_ALPHA,
            "command": _predecision_script(
                directory / "alpha.sh",
                "#!/bin/sh\nprintf '%s\\n' 'alpha-failed'\nexit 3\n",
            ),
            "timeout_seconds": 5,
        },
        {
            "id": _PREDECISION_BETA,
            "command": _predecision_script(
                directory / "beta.sh",
                f"#!/bin/sh\nprintf '%s\\n' '{_PREDECISION_EXPECTED_FAILURE}'\nexit 4\n",
            ),
            "timeout_seconds": 5,
        },
    )
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(
            commands,
            {
                "kind": "EXPECTED_RED",
                "command_id": _PREDECISION_BETA,
                "failure_signature": _PREDECISION_EXPECTED_FAILURE,
            },
        )
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_OUTCOME_MISMATCH")
    records = _predecision_manifest(runtime)["records"]
    assert [item["exit_code"] for item in records] == [3, 4]
    _assert_task_branch_restored(prepared)


@pytest.mark.parametrize(
    ("case_name", "command_text", "exit_code"),
    (
        ("missing_executable", "/no/such/predecision-coverage-binary", 127),
        ("permission_denied", "", 126),
        ("unbalanced_quotes", 'echo "unterminated', 127),
    ),
)
def test_predecision_command_start_failures_are_outcome_mismatches(
    tmp_path: Path,
    case_name: str,
    command_text: str,
    exit_code: int,
) -> None:
    if case_name == "permission_denied":
        script = tmp_path / "commands" / "denied.sh"
        script.parent.mkdir(parents=True, exist_ok=True)
        script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        script.chmod(0o644)
        command_text = str(script)
    commands = ({"id": case_name, "command": command_text, "timeout_seconds": 5},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_OUTCOME_MISMATCH")
    record = _predecision_manifest(runtime)["records"][0]
    assert record["exit_code"] == exit_code
    assert record["timed_out"] is False
    log_path = runtime / str(record["output_ref"])
    assert log_path.is_file()
    assert log_path.read_bytes()
    _assert_task_branch_restored(prepared)


def test_predecision_empty_command_returns_without_starting_process(
    tmp_path: Path,
) -> None:
    exit_code, output, timed_out = _predecision_operations().run_validation_command(
        tmp_path,
        "",
        5,
    )
    assert (exit_code, output, timed_out) == (127, b"", False)


def test_predecision_expected_red_records_without_output_do_not_match(
    tmp_path: Path,
) -> None:
    operations = _predecision_operations()
    outcome = {
        "kind": "EXPECTED_RED",
        "command_id": _PREDECISION_BETA,
        "failure_signature": _PREDECISION_EXPECTED_FAILURE,
    }
    missing_record = operations.predecision_outcome_matches(
        [{"command_id": _PREDECISION_ALPHA, "exit_code": 0, "output_ref": "unused.log"}],
        outcome,
        runtime=tmp_path,
    )
    non_string_ref = operations.predecision_outcome_matches(
        [{"command_id": _PREDECISION_BETA, "exit_code": 4, "output_ref": None}],
        outcome,
        runtime=tmp_path,
    )
    missing_file = operations.predecision_outcome_matches(
        [
            {
                "command_id": _PREDECISION_BETA,
                "exit_code": 4,
                "output_ref": "validation/missing.log",
            }
        ],
        outcome,
        runtime=tmp_path,
    )
    assert missing_record is False
    assert non_string_ref is False
    assert missing_file is False
    assert not (tmp_path / "validation" / "missing.log").exists()


def test_predecision_incomplete_command_count_fails_closed(tmp_path: Path) -> None:
    witness = tmp_path / "incomplete-witness.txt"
    command = _predecision_witness_command(
        tmp_path / "commands" / "alpha.sh",
        witness,
        _PREDECISION_ALPHA,
    )

    class _DeclaredLongerThanIteration(tuple):
        def __len__(self) -> int:
            return super().__len__() + 1

    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    result = _predecision_operations().execute_predecision_task_validation(
        repo=Path(prepared["repo_root"]),
        runtime=runtime,
        task_id=_REFERENCE_TASK_ID,
        origin_run_id=_REFERENCE_ORIGIN_RUN_ID,
        promotion_attempt_id=_REFERENCE_PROMOTION_ATTEMPT_ID,
        result_sha=str(prepared["result_sha"]),
        commands=_DeclaredLongerThanIteration(
            ({"id": _PREDECISION_ALPHA, "command": command, "timeout_seconds": 30},)
        ),
        expected_outcome={"kind": "GREEN"},
    )
    assert result == {
        "ok": False,
        "reason_code": "PREDECISION_VALIDATION_INCOMPLETE",
    }
    manifest = _predecision_manifest(runtime)
    assert manifest["status"] == "failed"
    assert manifest["reason_code"] == "PREDECISION_VALIDATION_INCOMPLETE"
    assert len(manifest["records"]) == 1
    assert len(_predecision_rows(witness)) == 1
    assert _predecision_rows(witness)[0]["merge"] == prepared["result_sha"]
    _assert_task_branch_restored(prepared)


def test_predecision_restore_failure_is_reported_after_hook_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands = ({"id": _PREDECISION_ALPHA, "command": "/bin/true", "timeout_seconds": 5},)
    prepared = _prepare_kwargs(tmp_path / "repo")
    runtime = Path(prepared["runtime_root"])
    module = _module()
    operations = module._PREDECISION_VALIDATION
    real_restore_hooks = operations.restore_git_hooks

    def break_git_after_hook_restore(repo: Path, original_hooks: Mapping[str, tuple[bytes, int]]) -> None:
        real_restore_hooks(repo, original_hooks)
        git_dir = Path(module._git_stdout(repo, "rev-parse", "--absolute-git-dir"))
        shutil.rmtree(git_dir)

    monkeypatch.setattr(operations, "restore_git_hooks", break_git_after_hook_restore)
    _run_directory(runtime).joinpath("task_snapshot.yaml").write_bytes(
        _predecision_snapshot(commands, {"kind": "GREEN"})
    )
    composed = _compose()(**_compose_kwargs(prepared))
    _assert_blocked(composed, "PREDECISION_VALIDATION_RESTORE_FAILED")
    manifest = _predecision_manifest(runtime)
    assert manifest["status"] == "failed"
    assert manifest["reason_code"] == "PREDECISION_VALIDATION_RESTORE_FAILED"
    assert not (Path(prepared["repo_root"]) / ".git").exists()


def test_predecision_untracked_cleanup_keeps_tracked_and_outside_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_target_repo(tmp_path)
    outside = tmp_path / "outside-keep"
    outside.mkdir()
    (outside / "secret").write_text("keep\n", encoding="utf-8")
    (repo / "escape").symlink_to(outside, target_is_directory=True)
    (repo / "loose.txt").write_text("loose\n", encoding="utf-8")
    held = repo / "held-dir"
    held.mkdir()
    (held / "keep.txt").write_text("held\n", encoding="utf-8")
    (repo / "tracked.txt").write_text("dirty-tracked\n", encoding="utf-8")
    module = _module()
    real_git_stdout = module._git_stdout

    def status_names_directory_and_outside(repo_root: Path, *args: str) -> str:
        if args[:1] == ("status",):
            observed = real_git_stdout(repo_root, *args)
            return observed + "\n?? held-dir\n?? ../outside-keep"
        return real_git_stdout(repo_root, *args)

    monkeypatch.setattr(module, "_git_stdout", status_names_directory_and_outside)
    module._PREDECISION_VALIDATION.remove_untracked_paths(repo)
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == "dirty-tracked\n"
    assert (outside / "secret").read_text(encoding="utf-8") == "keep\n"
    assert (repo / "escape").is_symlink()
    assert not (repo / "loose.txt").exists()
    assert not held.exists()
