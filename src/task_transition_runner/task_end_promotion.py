"""Public twelve-symbol task-end prepare/finalize slice.

Prepare verifies a WorkerResult against Git HEAD, branch, dirty paths,
and allowed_paths, then persists WorkerResult and DecisionRequest. Finalize
waits for file-backed public decisions, fail-closes INVALID before
AWAITING, qualifies a NON_AUTHORIZING_PROMOTION_INTENT, and returns to
main only after pending persist. Worker success is not promotion
authority. Journal start, consumption, TOCK, and merge-candidate
validation stay outside this slice.
"""

from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import signal
import subprocess
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from task_transition_runner.adapters.cursor_headless import CursorAdapterOutcome
from task_transition_runner.adapters.cursor_stream import BoundPromptBytes
from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    DecisionIdentity,
    OrchestratorDecision,
    ValidatorDecision,
    classify_orchestrator_decision,
    classify_validator_decision,
    load_orchestrator_decision,
    load_validator_decision,
)
from task_transition_runner.runner_git_promotion import (
    _apply_committed_risk_authority,
    evaluate_task_branch_risk,
    persist_pending_promotion,
)
from task_transition_runner.runner_promotion_contracts import (
    qualify_pre_candidate_intent,
)
from task_transition_runner.runner_promotion_state import load_and_validate_main_state
from task_transition_runner.worker_result import (
    ValidationResult,
    WorkerReport,
    WorkerResult,
    WorkerResultIdentity,
    load_worker_result,
)


_SCHEMA_VERSION = "1.0"
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_WORKER_RESULT_NAME = "worker_result.yaml"
_DECISION_REQUEST_NAME = "decision_request.yaml"
_ORCHESTRATOR_NAME = "orchestrator_decision.yaml"
_VALIDATOR_NAME = "validator_decision.yaml"
_CANONICAL_PENDING_NAME = "pending_promotion.yaml"
_VALIDATION_DIRECTORY_NAME = "validation"
_VALIDATION_MANIFEST_NAME = "manifest.yaml"
_PENDING_MATCH_FIELDS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "promotion_class",
    "result_sha",
    "task_branch",
    "main_state_before_promotion",
    "red_lock_closure",
)


def prepare_git_promotion_task_end_artifacts(
    *,
    repo_root: Path | str,
    runtime_root: Path | str,
    worker_result: WorkerResult,
    allowed_paths: tuple[str, ...] | list[str],
) -> dict[str, Any]:
    repo = Path(repo_root)
    runtime = Path(runtime_root)
    if _non_runtime_dirty_paths(repo):
        return _promotion_blocked("PROMOTION_GIT_PREFLIGHT_BLOCKED")
    head_sha = _git_stdout(repo, "rev-parse", "HEAD")
    if head_sha != worker_result.result_sha:
        return _promotion_blocked("PROMOTION_RESULT_SHA_MISMATCH")
    if _git_stdout(repo, "rev-parse", "--abbrev-ref", "HEAD") != worker_result.task_branch:
        return _promotion_blocked("PROMOTION_RESULT_CONTRADICTION")
    allowed = tuple(allowed_paths)
    if any(path not in allowed for path in worker_result.changed_paths):
        return _promotion_blocked("PROMOTION_ALLOWED_PATHS_MISMATCH")

    run_dir = runtime / "runs" / worker_result.origin_run_id
    worker_path = run_dir / _WORKER_RESULT_NAME
    request_path = run_dir / "decisions" / _DECISION_REQUEST_NAME
    worker_payload = {
        "schema_version": worker_result.schema_version,
        "result_type": worker_result.result_type,
        "task_id": worker_result.task_id,
        "origin_run_id": worker_result.origin_run_id,
        "promotion_attempt_id": worker_result.promotion_attempt_id,
        "task_branch": worker_result.task_branch,
        "result_sha": worker_result.result_sha,
        "changed_paths": list(worker_result.changed_paths),
        "validation_results": [
            {
                "command_id": item.command_id,
                "exit_code": item.exit_code,
                "output_ref": item.output_ref,
                "output_sha256": item.output_sha256,
            }
            for item in worker_result.validation_results
        ],
        "worker_report": {
            "reported_status": worker_result.worker_report.reported_status,
            "summary": worker_result.worker_report.summary,
        },
    }
    request_payload = {
        "schema_version": _SCHEMA_VERSION,
        "request_type": "decision_request",
        "task_id": worker_result.task_id,
        "origin_run_id": worker_result.origin_run_id,
        "promotion_attempt_id": worker_result.promotion_attempt_id,
        "result_sha": worker_result.result_sha,
        "required_decisions": [
            {"role": "orchestrator", "path": _ORCHESTRATOR_NAME},
            {"role": "validator", "path": _VALIDATOR_NAME},
        ],
        "evidence": [
            {
                "kind": "validation_log",
                "ref": item.output_ref,
                "sha256": item.output_sha256,
            }
            for item in worker_result.validation_results
        ],
    }
    intended_worker = yaml.safe_dump(
        dict(worker_payload),
        sort_keys=False,
        allow_unicode=False,
    ).encode("utf-8")
    intended_request = yaml.safe_dump(
        dict(request_payload),
        sort_keys=False,
        allow_unicode=False,
    ).encode("utf-8")
    worker_exists = worker_path.is_file()
    request_exists = request_path.is_file()
    if request_exists and request_path.read_bytes() != intended_request:
        return _promotion_blocked("PROMOTION_DECISION_CONTRADICTION")
    if worker_exists and worker_path.read_bytes() != intended_worker:
        return _promotion_blocked("PROMOTION_RESULT_CONTRADICTION")
    if worker_exists and request_exists:
        return {
            "ok": True,
            "outcome": "WORKER_RESULT_READY",
            "result_sha": worker_result.result_sha,
            "replayed": True,
            "worker_result_path": str(worker_path),
            "decision_request_path": str(request_path),
        }
    _atomic_write_yaml(worker_path, worker_payload)
    _atomic_write_yaml(request_path, request_payload)
    return {
        "ok": True,
        "outcome": "WORKER_RESULT_READY",
        "result_sha": worker_result.result_sha,
        "replayed": False,
        "worker_result_path": str(worker_path),
        "decision_request_path": str(request_path),
    }


def finalize_git_promotion_task_end_artifacts(
    *,
    repo_root: Path | str,
    runtime_root: Path | str,
    origin_run_id: str,
    task_snapshot: Mapping[str, Any],
    instruction_snapshot: Mapping[str, Any],
    expected_outcome: Any,
    allowed_paths: tuple[str, ...] | list[str],
    risk_facts: Mapping[str, Any],
    lock_ownership: Mapping[str, Any],
    git_preflight: Mapping[str, Any],
    consumed_payload: Mapping[str, Any],
    red_lock: Mapping[str, Any] | None,
    unresolved_foreign_epoch: bool,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    repo = Path(repo_root)
    runtime = Path(runtime_root)
    worker_path = runtime / "runs" / origin_run_id / _WORKER_RESULT_NAME
    worker_payload = _load_yaml_mapping_if_present(worker_path)
    if worker_payload is None:
        return _promotion_blocked("PROMOTION_RESULT_SHA_MISSING")
    worker_result = load_worker_result(
        worker_path,
        expected_identity=WorkerResultIdentity(
            task_id=str(worker_payload["task_id"]),
            origin_run_id=str(worker_payload["origin_run_id"]),
            promotion_attempt_id=str(worker_payload["promotion_attempt_id"]),
            result_sha=str(worker_payload["result_sha"]),
        ),
        expected_validation_digests={
            item["command_id"]: item["output_sha256"]
            for item in worker_payload["validation_results"]
        },
    )
    wait_context = {
        "task_id": worker_result.task_id,
        "origin_run_id": worker_result.origin_run_id,
        "promotion_attempt_id": worker_result.promotion_attempt_id,
        "result_sha": worker_result.result_sha,
    }
    wait_result = _wait_for_ready_promotion_drafts(
        context=wait_context,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        artifact_root=runtime,
    )
    if wait_result.get("ok") is not True:
        return wait_result
    orchestrator_decision = wait_result["orchestrator_decision"]
    validator_decision = wait_result["validator_decision"]
    live_main = _load_yaml_mapping_if_present(runtime / "main_state.yaml")
    git_main = _git_stdout(repo, "rev-parse", "refs/heads/main")
    closure = None
    if (
        red_lock is not None
        and orchestrator_decision.promotion_class == "GREEN_PROMOTION"
    ):
        bound_main = dict(live_main)
        bound_main["observed_main_head_sha"] = git_main
        closure = {
            "red_lock": dict(red_lock),
            "main_state_before_promotion": bound_main,
        }
    closure_validation = _validate_current_red_lock_closure(
        runtime_root=runtime,
        context={
            "promotion_class": orchestrator_decision.promotion_class,
            "main_state_before_promotion": live_main,
            "red_lock_closure": closure,
        },
    )
    if closure_validation is not None:
        return closure_validation
    if _non_runtime_dirty_paths(repo):
        return _promotion_blocked("PROMOTION_GIT_PREFLIGHT_BLOCKED")
    main_state = load_and_validate_main_state(
        main_state_path=runtime / "main_state.yaml",
    )
    committed_risk = evaluate_task_branch_risk(
        repo_root=repo,
        base_sha=git_main,
        result_sha=worker_result.result_sha,
        allowed_paths=tuple(allowed_paths),
    )
    qualified = _apply_committed_risk_authority(
        risk_facts,
        committed_risk,
        lambda authoritative_risk: qualify_pre_candidate_intent(
            task_snapshot=task_snapshot,
            instruction_snapshot=instruction_snapshot,
            worker_result=worker_result,
            task_branch_head_sha=_git_stdout(
                repo,
                "rev-parse",
                worker_result.task_branch,
            ),
            expected_outcome=expected_outcome,
            allowed_paths=tuple(allowed_paths),
            risk_facts=authoritative_risk,
            orchestrator_state=DecisionArtifactState.READY,
            orchestrator_decision=orchestrator_decision,
            validator_state=DecisionArtifactState.READY,
            validator_decision=validator_decision,
            main_state=main_state,
            consumed_payload=consumed_payload,
            lock_ownership=lock_ownership,
            git_preflight=git_preflight,
            red_lock=red_lock,
            unresolved_foreign_epoch=unresolved_foreign_epoch,
        ),
    )
    if qualified.get("ok") is not True:
        return _promotion_blocked(str(qualified["reason_code"]))
    persisted = persist_pending_promotion(
        runtime_root=runtime,
        target_worktree=repo,
        intent=qualified["intent"],
        task_snapshot=task_snapshot,
        instruction_snapshot=instruction_snapshot,
        worker_result=worker_result,
        expected_outcome=expected_outcome,
        orchestrator_decision=orchestrator_decision,
        validator_decision=validator_decision,
        base_sha=git_main,
    )
    if persisted.get("ok") is not True:
        return _promotion_blocked(str(persisted["reason_code"]))
    _git_stdout(repo, "checkout", "main")
    return {
        "ok": True,
        "outcome": "PENDING_PROMOTION",
        "artifact_class": _ARTIFACT_CLASS,
        "reason_code": "PROMOTION_INTENT_PERSISTED",
        "pending_path": persisted["pending_path"],
        "replayed": persisted["status"] == "already_persisted",
    }


def _wait_for_ready_promotion_drafts(
    *,
    context: dict[str, Any],
    timeout_seconds: int,
    poll_seconds: int,
    artifact_root: Path,
) -> dict[str, Any]:
    identity = DecisionIdentity(
        task_id=str(context["task_id"]),
        origin_run_id=str(context["origin_run_id"]),
        promotion_attempt_id=str(context["promotion_attempt_id"]),
        result_sha=str(context["result_sha"]),
    )
    decisions_dir = (
        Path(artifact_root)
        / "runs"
        / str(context["origin_run_id"])
        / "decisions"
    )
    orchestrator_path = decisions_dir / _ORCHESTRATOR_NAME
    validator_path = decisions_dir / _VALIDATOR_NAME
    start = time.monotonic()
    while True:
        orchestrator_state = classify_orchestrator_decision(
            orchestrator_path,
            expected_identity=identity,
            artifact_root=Path(artifact_root),
        )
        validator_state = classify_validator_decision(
            validator_path,
            expected_identity=identity,
            artifact_root=Path(artifact_root),
        )
        orchestrator_decision = None
        validator_decision = None
        if orchestrator_state == DecisionArtifactState.READY:
            orchestrator_decision = load_orchestrator_decision(
                orchestrator_path,
                expected_identity=identity,
                artifact_root=Path(artifact_root),
            )
        if validator_state == DecisionArtifactState.READY:
            validator_decision = load_validator_decision(
                validator_path,
                expected_identity=identity,
                artifact_root=Path(artifact_root),
            )
        validation = _validate_ready_promotion_drafts(
            context=context,
            orchestrator_state=orchestrator_state,
            orchestrator_decision=orchestrator_decision,
            validator_state=validator_state,
            validator_decision=validator_decision,
        )
        if validation.get("ok") is True:
            return validation
        if validation.get("outcome") == "BLOCKED":
            return validation
        if time.monotonic() - start >= timeout_seconds:
            return validation
        time.sleep(poll_seconds)


def _validate_ready_promotion_drafts(
    *,
    context: dict[str, Any],
    orchestrator_state: DecisionArtifactState,
    orchestrator_decision: OrchestratorDecision | None,
    validator_state: DecisionArtifactState,
    validator_decision: ValidatorDecision | None,
) -> dict[str, Any]:
    if (
        orchestrator_state == DecisionArtifactState.INVALID
        or validator_state == DecisionArtifactState.INVALID
    ):
        return _promotion_blocked("PROMOTION_DECISION_INVALID")
    if (
        orchestrator_state == DecisionArtifactState.MISSING
        or validator_state == DecisionArtifactState.MISSING
    ):
        return {
            "ok": False,
            "outcome": "AWAITING_DECISIONS",
            "reason_code": "PROMOTION_DECISION_MISSING",
        }
    if (
        orchestrator_state == DecisionArtifactState.DRAFT
        or validator_state == DecisionArtifactState.DRAFT
    ):
        return {
            "ok": False,
            "outcome": "AWAITING_DECISIONS",
            "reason_code": "PROMOTION_DECISION_NOT_READY",
        }
    expected_identity = (
        context["task_id"],
        context["origin_run_id"],
        context["promotion_attempt_id"],
        context["result_sha"],
    )
    observed_identities = {
        (
            orchestrator_decision.task_id,
            orchestrator_decision.origin_run_id,
            orchestrator_decision.promotion_attempt_id,
            orchestrator_decision.result_sha,
        ),
        (
            validator_decision.task_id,
            validator_decision.origin_run_id,
            validator_decision.promotion_attempt_id,
            validator_decision.result_sha,
        ),
    }
    if observed_identities != {expected_identity}:
        return _promotion_blocked("PROMOTION_DECISION_IDENTITY_MISMATCH")
    return {
        "ok": True,
        "orchestrator_decision": orchestrator_decision,
        "validator_decision": validator_decision,
    }


def _pending_matches_context(
    pending: Mapping[str, Any],
    context: Mapping[str, Any],
) -> bool:
    return all(
        pending.get(field) == context.get(field) for field in _PENDING_MATCH_FIELDS
    )


def _validate_current_red_lock_closure(
    *,
    runtime_root: Path,
    context: Mapping[str, Any],
) -> dict[str, Any] | None:
    runtime = Path(runtime_root)
    closure = context.get("red_lock_closure")
    if closure is None:
        if (
            context.get("promotion_class") == "GREEN_PROMOTION"
            and context["main_state_before_promotion"].get("state")
            == "EXPECTED_RED_OPEN"
        ):
            return _promotion_blocked("PROMOTION_RED_LOCK_MISSING")
        return None
    red_lock_path = runtime / "red_lock.yaml"
    if not red_lock_path.is_file():
        return _promotion_blocked("PROMOTION_RED_LOCK_MISSING")
    active_red_lock = _load_yaml_mapping_if_present(red_lock_path)
    if active_red_lock != closure.get("red_lock"):
        return _promotion_blocked("PROMOTION_RED_LOCK_MISMATCH")
    active_main_state = _load_yaml_mapping_if_present(runtime / "main_state.yaml")
    if active_main_state != closure.get("main_state_before_promotion"):
        return _promotion_blocked("PROMOTION_MAIN_STATE_MISMATCH")
    return None


def _copy_yaml_payload(source: Path, target: Path) -> None:
    payload = _load_yaml_mapping_if_present(source)
    if payload is None:
        raise RuntimeError(f"YAML mapping missing: {source}")
    _atomic_write_yaml(target, payload)


def _load_yaml_mapping_if_present(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return None
    return payload if isinstance(payload, dict) else None


def _atomic_write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(
        yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    temporary_path.replace(path)


def _non_runtime_dirty_paths(repo_root: Path) -> tuple[str, ...]:
    status = _git_stdout(
        repo_root,
        "status",
        "--porcelain",
        "--untracked-files=all",
    )
    dirty_paths: list[str] = []
    for line in status.splitlines():
        dirty_paths.append(line[3:].split(" -> ", 1)[-1])
    return tuple(dirty_paths)


def _promotion_blocked(reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "blocked",
        "reason_code": reason_code,
        "outcome": "BLOCKED",
    }


def compose_adapter_git_worker_result(
    *,
    repo_root: Path | str,
    runtime_root: Path | str,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    task_branch: str,
    developer: BoundPromptBytes,
    task_spec: BoundPromptBytes,
    adapter_outcome: CursorAdapterOutcome | None,
    allowed_paths: tuple[str, ...] | list[str],
    claimed_identity: WorkerResultIdentity | None,
) -> dict[str, Any]:
    """Derive a WorkerResult from Git plus an allowlisted adapter projection.

    Adapter terminal success is an input fact. It does not authorize
    promotion, and raw stream, prompt, session, or argv bytes are not copied.
    """

    def blocked(reason_code: str) -> dict[str, Any]:
        return {
            "ok": False,
            "outcome": "BLOCKED",
            "reason_code": reason_code,
            "replayed": False,
            "worker_rerun": False,
            "resumed": False,
            "promotion_authorized": False,
        }

    def ready(
        *,
        replayed: bool,
        worker_result: WorkerResult,
        publish_projection: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "outcome": "WORKER_RESULT_READY",
            "reason_code": "ADAPTER_COMPLETED",
            "replayed": replayed,
            "worker_rerun": replayed is False,
            "resumed": False,
            "promotion_authorized": False,
            "result_sha": worker_result.result_sha,
            "changed_paths": worker_result.changed_paths,
            "worker_result": worker_result,
            "publish_projection": publish_projection,
            "snapshot_binding": {
                "developer_sha256": developer.sha256,
                "developer_byte_length": developer.byte_length,
                "task_spec_sha256": task_spec.sha256,
                "task_spec_byte_length": task_spec.byte_length,
            },
        }

    repo = Path(repo_root)
    runtime = Path(runtime_root)
    run_dir = runtime / "runs" / origin_run_id
    head_sha = _git_stdout(repo, "rev-parse", "HEAD")
    actual_branch = _git_stdout(repo, "rev-parse", "--abbrev-ref", "HEAD")
    changed_paths = tuple(
        _git_stdout(repo, "diff", "--name-only", "refs/heads/main", "HEAD").splitlines()
    )
    try:
        bound_validation = _PREDECISION_VALIDATION.load_bound_task_validation(
            run_dir,
            task_id=task_id,
        )
    except (OSError, ValueError, yaml.YAMLError):
        return blocked("PREDECISION_VALIDATION_CONTRACT_INVALID")
    if claimed_identity is not None and (
        claimed_identity.task_id,
        claimed_identity.origin_run_id,
        claimed_identity.promotion_attempt_id,
    ) != (task_id, origin_run_id, promotion_attempt_id):
        return blocked("RESULT_IDENTITY_MISMATCH")
    claimed_sha = head_sha if claimed_identity is None else claimed_identity.result_sha
    if (task_branch, claimed_sha) != (actual_branch, head_sha):
        return blocked("RESULT_GIT_MISMATCH")
    if any(path not in allowed_paths for path in changed_paths):
        return blocked("RESULT_PATHS_OUTSIDE_ALLOWED")
    for bound in (developer, task_spec):
        observed_binding = (
            hashlib.sha256(bound.payload_bytes).hexdigest(),
            len(bound.payload_bytes),
        )
        if observed_binding != (bound.sha256, bound.byte_length):
            return blocked("ADAPTER_PROJECTION_UNSAFE")

    worker_path = run_dir / _WORKER_RESULT_NAME
    existing = _load_yaml_mapping_if_present(worker_path)
    if existing is not None:
        verified_validation = None
        if bound_validation is not None:
            verified_validation = (
                _PREDECISION_VALIDATION.verify_predecision_validation_evidence(
                    repo=repo,
                    runtime=runtime,
                    task_id=task_id,
                    origin_run_id=origin_run_id,
                    promotion_attempt_id=promotion_attempt_id,
                    result_sha=head_sha,
                    commands=bound_validation["commands"],
                    expected_outcome=bound_validation["expected_outcome"],
                )
            )
            if verified_validation is None:
                return blocked("PREDECISION_VALIDATION_EVIDENCE_INVALID")
        loaded = load_worker_result(
            worker_path,
            expected_identity=WorkerResultIdentity(
                task_id=str(existing["task_id"]),
                origin_run_id=str(existing["origin_run_id"]),
                promotion_attempt_id=str(existing["promotion_attempt_id"]),
                result_sha=str(existing["result_sha"]),
            ),
            expected_validation_digests={
                str(item["command_id"]): str(item["output_sha256"])
                for item in existing["validation_results"]
            },
        )
        if (
            loaded.task_id,
            loaded.origin_run_id,
            loaded.promotion_attempt_id,
        ) != (task_id, origin_run_id, promotion_attempt_id):
            return blocked("RESULT_IDENTITY_MISMATCH")
        observed = (
            loaded.task_branch,
            loaded.result_sha,
            loaded.changed_paths,
        )
        expected = (
            actual_branch,
            head_sha,
            changed_paths,
        )
        if observed == expected:
            if (
                verified_validation is not None
                and loaded.validation_results != verified_validation
            ):
                return blocked("PREDECISION_VALIDATION_EVIDENCE_INVALID")
            return ready(
                replayed=True,
                worker_result=loaded,
                publish_projection=None,
            )
        return blocked("RESULT_GIT_MISMATCH")
    if bound_validation is not None:
        replayed_validation = (
            _PREDECISION_VALIDATION.verify_predecision_validation_evidence(
                repo=repo,
                runtime=runtime,
                task_id=task_id,
                origin_run_id=origin_run_id,
                promotion_attempt_id=promotion_attempt_id,
                result_sha=head_sha,
                commands=bound_validation["commands"],
                expected_outcome=bound_validation["expected_outcome"],
            )
        )
        manifest_path = run_dir / _VALIDATION_DIRECTORY_NAME / _VALIDATION_MANIFEST_NAME
        if manifest_path.exists():
            if replayed_validation is None:
                return blocked("PREDECISION_VALIDATION_EVIDENCE_INVALID")
            replayed_worker = WorkerResult(
                schema_version=_SCHEMA_VERSION,
                result_type="worker_result",
                task_id=task_id,
                origin_run_id=origin_run_id,
                promotion_attempt_id=promotion_attempt_id,
                task_branch=task_branch,
                result_sha=head_sha,
                changed_paths=changed_paths,
                validation_results=replayed_validation,
                worker_report=WorkerReport(
                    reported_status="success",
                    summary="ADAPTER_COMPLETED",
                ),
            )
            return ready(
                replayed=True,
                worker_result=replayed_worker,
                publish_projection=None,
            )
    if adapter_outcome is None:
        return blocked("ADAPTER_INTERRUPTED")
    block_path = run_dir / "adapter_stream_block.yaml"
    if _load_yaml_mapping_if_present(block_path) is not None:
        return blocked("ADAPTER_INTERRUPTED")

    publishable_keys = (
        "adapter_status",
        "neutral_reason_code",
        "process_exit_code",
        "timeout",
        "interrupted",
        "event_counts",
        "session_observed",
        "terminal_success",
    )
    source = adapter_outcome.publish_projection
    if any(key not in source for key in publishable_keys):
        return blocked("ADAPTER_PROJECTION_UNSAFE")
    event_counts = source["event_counts"]
    if not isinstance(event_counts, Mapping):
        return blocked("ADAPTER_PROJECTION_UNSAFE")
    publish_projection = {key: source[key] for key in publishable_keys}
    publish_projection["event_counts"] = dict(event_counts)
    if publish_projection["timeout"] is True:
        return blocked("ADAPTER_PROJECTION_UNSAFE")
    if adapter_outcome.prompt_frame_matches is not True:
        return blocked("ADAPTER_PROJECTION_UNSAFE")
    if (
        publish_projection["interrupted"] is True
        and publish_projection["terminal_success"] is True
    ):
        return blocked("ADAPTER_PROJECTION_UNSAFE")
    if publish_projection["terminal_success"] is not True:
        _atomic_write_yaml(
            block_path,
            {
                "schema_version": _SCHEMA_VERSION,
                "kind": "interrupted_unverified_cursor_session",
                "task_id": task_id,
                "origin_run_id": origin_run_id,
                "promotion_attempt_id": promotion_attempt_id,
                "resumed": False,
            },
        )
        return blocked("ADAPTER_INTERRUPTED")
    validation_results: tuple[ValidationResult, ...] = ()
    if bound_validation is not None:
        validation = _PREDECISION_VALIDATION.execute_predecision_task_validation(
            repo=repo,
            runtime=runtime,
            task_id=task_id,
            origin_run_id=origin_run_id,
            promotion_attempt_id=promotion_attempt_id,
            result_sha=head_sha,
            commands=bound_validation["commands"],
            expected_outcome=bound_validation["expected_outcome"],
        )
        if validation["ok"] is not True:
            return blocked(str(validation["reason_code"]))
        validation_results = validation["validation_results"]
    worker_result = WorkerResult(
        schema_version=_SCHEMA_VERSION,
        result_type="worker_result",
        task_id=task_id,
        origin_run_id=origin_run_id,
        promotion_attempt_id=promotion_attempt_id,
        task_branch=task_branch,
        result_sha=head_sha,
        changed_paths=changed_paths,
        validation_results=validation_results,
        worker_report=WorkerReport(
            reported_status="success",
            summary="ADAPTER_COMPLETED",
        ),
    )
    return ready(
        replayed=False,
        worker_result=worker_result,
        publish_projection=publish_projection,
    )


def _git_stdout(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _load_bound_task_validation(
    run_dir: Path,
    *,
    task_id: str,
) -> dict[str, Any] | None:
    snapshot_path = run_dir / "task_snapshot.yaml"
    if not snapshot_path.is_file():
        return None
    payload = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("task snapshot is not a mapping")
    active = payload.get("active_task")
    if not isinstance(active, Mapping) or active.get("task_id") != task_id:
        raise ValueError("task snapshot identity mismatch")
    validation = active.get("validation")
    if not isinstance(validation, Mapping):
        raise ValueError("task validation is not a mapping")
    raw_commands = validation.get("commands")
    if not isinstance(raw_commands, list) or not raw_commands:
        raise ValueError("task validation commands are absent")
    commands: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_command in raw_commands:
        if not isinstance(raw_command, Mapping):
            raise ValueError("task validation command is not a mapping")
        if set(raw_command) != {"id", "command", "timeout_seconds"}:
            raise ValueError("task validation command fields differ")
        command_id = raw_command["id"]
        command_text = raw_command["command"]
        timeout_seconds = raw_command["timeout_seconds"]
        if (
            not isinstance(command_id, str)
            or not command_id.strip()
            or command_id in seen_ids
            or not isinstance(command_text, str)
            or not command_text.strip()
            or not isinstance(timeout_seconds, int)
            or isinstance(timeout_seconds, bool)
            or timeout_seconds <= 0
        ):
            raise ValueError("task validation command is invalid")
        seen_ids.add(command_id)
        commands.append(
            {
                "id": command_id,
                "command": command_text,
                "timeout_seconds": timeout_seconds,
            }
        )
    expected_outcome = active.get("expected_outcome")
    if not isinstance(expected_outcome, Mapping):
        raise ValueError("task expected outcome is not a mapping")
    outcome = dict(expected_outcome)
    if outcome.get("kind") == "GREEN":
        if set(outcome) != {"kind"}:
            raise ValueError("GREEN outcome fields differ")
    elif outcome.get("kind") == "EXPECTED_RED":
        if set(outcome) != {"kind", "command_id", "failure_signature"}:
            raise ValueError("EXPECTED_RED outcome fields differ")
        if (
            outcome.get("command_id") not in seen_ids
            or not isinstance(outcome.get("failure_signature"), str)
            or not str(outcome["failure_signature"]).strip()
        ):
            raise ValueError("EXPECTED_RED outcome is invalid")
    else:
        raise ValueError("task expected outcome is invalid")
    return {"commands": tuple(commands), "expected_outcome": outcome}


def _execute_predecision_task_validation(
    *,
    repo: Path,
    runtime: Path,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    result_sha: str,
    commands: tuple[dict[str, Any], ...],
    expected_outcome: Mapping[str, Any],
) -> dict[str, Any]:
    validation_dir = runtime / "runs" / origin_run_id / _VALIDATION_DIRECTORY_NAME
    manifest_path = validation_dir / _VALIDATION_MANIFEST_NAME
    expected_paths = tuple(
        path
        for index, command in enumerate(commands)
        for path in _PREDECISION_VALIDATION.validation_command_paths(
            validation_dir,
            index,
            str(command["id"]),
        )
    )
    if manifest_path.exists() or any(path.exists() for path in expected_paths):
        return {
            "ok": False,
            "reason_code": "PREDECISION_VALIDATION_WRITE_ONCE_CONFLICT",
        }

    original_branch = _git_stdout(repo, "rev-parse", "--abbrev-ref", "HEAD")
    original_head = _git_stdout(repo, "rev-parse", "HEAD")
    original_status = _git_stdout(repo, "status", "--porcelain=v1", "-uall")
    original_hooks = _PREDECISION_VALIDATION.snapshot_git_hooks(repo)
    base_sha = _git_stdout(repo, "rev-parse", "refs/heads/main")
    records: list[dict[str, Any]] = []
    failure_reason: str | None = None
    candidate_tree_oid = ""
    restored = False
    try:
        _git_stdout(repo, "checkout", "main")
        merged = subprocess.run(  # noqa: S603
            ["git", "merge", "--no-commit", "--no-ff", result_sha],
            cwd=repo,
            check=False,
            capture_output=True,
        )
        if merged.returncode != 0:
            failure_reason = "PREDECISION_VALIDATION_CANDIDATE_FAILED"
        else:
            candidate_tree_oid = _git_stdout(repo, "write-tree")
            candidate_state = _PREDECISION_VALIDATION.snapshot_candidate_state(repo)
            for index, command in enumerate(commands):
                log_path, metadata_path = (
                    _PREDECISION_VALIDATION.validation_command_paths(
                        validation_dir,
                        index,
                        str(command["id"]),
                    )
                )
                exit_code, output_bytes, timed_out = (
                    _PREDECISION_VALIDATION.run_validation_command(
                        repo,
                        str(command["command"]),
                        int(command["timeout_seconds"]),
                    )
                )
                log_bytes = _PREDECISION_VALIDATION.validation_log_bytes(
                    command_id=str(command["id"]),
                    command_text=str(command["command"]),
                    output_bytes=output_bytes,
                )
                output_sha256 = hashlib.sha256(log_bytes).hexdigest()
                output_ref = log_path.relative_to(runtime).as_posix()
                _PREDECISION_VALIDATION.atomic_write_once_bytes(log_path, log_bytes)
                metadata = {
                    "schema_version": _SCHEMA_VERSION,
                    "kind": "predecision_task_validation",
                    "task_id": task_id,
                    "origin_run_id": origin_run_id,
                    "promotion_attempt_id": promotion_attempt_id,
                    "base_sha": base_sha,
                    "result_sha": result_sha,
                    "command_id": str(command["id"]),
                    "command_text": str(command["command"]),
                    "timeout_seconds": int(command["timeout_seconds"]),
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "output_ref": output_ref,
                    "output_sha256": output_sha256,
                    "candidate_tree_oid": candidate_tree_oid,
                }
                metadata_bytes = yaml.safe_dump(
                    metadata,
                    sort_keys=False,
                    allow_unicode=False,
                ).encode("utf-8")
                _PREDECISION_VALIDATION.atomic_write_once_bytes(
                    metadata_path,
                    metadata_bytes,
                )
                records.append(
                    {
                        **metadata,
                        "metadata_ref": metadata_path.relative_to(runtime).as_posix(),
                        "metadata_sha256": hashlib.sha256(metadata_bytes).hexdigest(),
                    }
                )
                if timed_out:
                    failure_reason = "PREDECISION_VALIDATION_TIMEOUT"
                    break
                if (
                    _PREDECISION_VALIDATION.snapshot_candidate_state(repo)
                    != candidate_state
                ):
                    failure_reason = "PREDECISION_VALIDATION_TARGET_MUTATED"
                    break
    except (OSError, subprocess.CalledProcessError, ValueError):
        failure_reason = "PREDECISION_VALIDATION_EXECUTION_FAILED"
    finally:
        restored = _PREDECISION_VALIDATION.restore_predecision_target(
            repo,
            original_branch=original_branch,
            original_head=original_head,
            original_main_sha=base_sha,
            original_status=original_status,
            original_hooks=original_hooks,
        )

    if not restored:
        failure_reason = "PREDECISION_VALIDATION_RESTORE_FAILED"
    if failure_reason is None and len(records) != len(commands):
        failure_reason = "PREDECISION_VALIDATION_INCOMPLETE"
    if failure_reason is None and not (
        _PREDECISION_VALIDATION.predecision_outcome_matches(
            records,
            expected_outcome,
            runtime=runtime,
        )
    ):
        failure_reason = "PREDECISION_VALIDATION_OUTCOME_MISMATCH"
    manifest = {
        "schema_version": _SCHEMA_VERSION,
        "kind": "predecision_task_validation_manifest",
        "task_id": task_id,
        "origin_run_id": origin_run_id,
        "promotion_attempt_id": promotion_attempt_id,
        "base_sha": base_sha,
        "result_sha": result_sha,
        "candidate_tree_oid": candidate_tree_oid,
        "expected_outcome": dict(expected_outcome),
        "status": "passed" if failure_reason is None else "failed",
        "reason_code": failure_reason or "PREDECISION_VALIDATION_PASSED",
        "records": records,
    }
    try:
        _PREDECISION_VALIDATION.atomic_write_once_bytes(
            manifest_path,
            yaml.safe_dump(
                manifest,
                sort_keys=False,
                allow_unicode=False,
            ).encode("utf-8"),
        )
    except OSError:
        return {
            "ok": False,
            "reason_code": "PREDECISION_VALIDATION_WRITE_ONCE_CONFLICT",
        }
    if failure_reason is not None:
        return {"ok": False, "reason_code": failure_reason}
    return {
        "ok": True,
        "validation_results": (
            _PREDECISION_VALIDATION.validation_results_from_records(records)
        ),
    }


def _verify_predecision_validation_evidence(
    *,
    repo: Path,
    runtime: Path,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    result_sha: str,
    commands: tuple[dict[str, Any], ...],
    expected_outcome: Mapping[str, Any],
) -> tuple[ValidationResult, ...] | None:
    manifest_path = (
        runtime
        / "runs"
        / origin_run_id
        / _VALIDATION_DIRECTORY_NAME
        / _VALIDATION_MANIFEST_NAME
    )
    manifest = _load_yaml_mapping_if_present(manifest_path)
    if manifest is None or manifest.get("status") != "passed":
        return None
    base_sha = _git_stdout(repo, "rev-parse", "refs/heads/main")
    identity = (
        manifest.get("task_id"),
        manifest.get("origin_run_id"),
        manifest.get("promotion_attempt_id"),
        manifest.get("base_sha"),
        manifest.get("result_sha"),
        manifest.get("expected_outcome"),
    )
    expected_identity = (
        task_id,
        origin_run_id,
        promotion_attempt_id,
        base_sha,
        result_sha,
        dict(expected_outcome),
    )
    if identity != expected_identity:
        return None
    records = manifest.get("records")
    if not isinstance(records, list) or len(records) != len(commands):
        return None
    seen_refs: set[str] = set()
    seen_digests: set[str] = set()
    candidate_tree_oid = manifest.get("candidate_tree_oid")
    if not isinstance(candidate_tree_oid, str) or not candidate_tree_oid:
        return None
    for index, (record, command) in enumerate(zip(records, commands, strict=True)):
        if not isinstance(record, Mapping):
            return None
        log_path, metadata_path = _PREDECISION_VALIDATION.validation_command_paths(
            manifest_path.parent,
            index,
            str(command["id"]),
        )
        output_ref = log_path.relative_to(runtime).as_posix()
        metadata_ref = metadata_path.relative_to(runtime).as_posix()
        if (
            record.get("command_id") != command["id"]
            or record.get("command_text") != command["command"]
            or record.get("timeout_seconds") != command["timeout_seconds"]
            or record.get("output_ref") != output_ref
            or record.get("metadata_ref") != metadata_ref
            or record.get("candidate_tree_oid") != candidate_tree_oid
            or record.get("timed_out") is not False
            or not log_path.is_file()
            or not metadata_path.is_file()
        ):
            return None
        log_bytes = log_path.read_bytes()
        metadata_bytes = metadata_path.read_bytes()
        output_digest = hashlib.sha256(log_bytes).hexdigest()
        metadata_digest = hashlib.sha256(metadata_bytes).hexdigest()
        if (
            record.get("output_sha256") != output_digest
            or record.get("metadata_sha256") != metadata_digest
            or output_ref in seen_refs
            or output_digest in seen_digests
        ):
            return None
        metadata = _load_yaml_mapping_if_present(metadata_path)
        expected_metadata = {
            key: value
            for key, value in record.items()
            if key not in {"metadata_ref", "metadata_sha256"}
        }
        if metadata != expected_metadata:
            return None
        seen_refs.add(output_ref)
        seen_digests.add(output_digest)
    if not _PREDECISION_VALIDATION.predecision_outcome_matches(
        records,
        expected_outcome,
        runtime=runtime,
    ):
        return None
    return _PREDECISION_VALIDATION.validation_results_from_records(records)


def _validation_command_paths(
    validation_dir: Path,
    index: int,
    command_id: str,
) -> tuple[Path, Path]:
    identity_digest = hashlib.sha256(command_id.encode("utf-8")).hexdigest()[:16]
    stem = f"{index:03d}-{identity_digest}"
    return validation_dir / f"{stem}.log", validation_dir / f"{stem}.yaml"


def _run_validation_command(
    repo: Path,
    command_text: str,
    timeout_seconds: int,
) -> tuple[int, bytes, bool]:
    try:
        argv = shlex.split(command_text)
        if not argv:
            return 127, b"", False
        process = subprocess.Popen(  # noqa: S603
            argv,
            cwd=repo,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except FileNotFoundError as exc:
        return 127, str(exc).encode("utf-8"), False
    except PermissionError as exc:
        return 126, str(exc).encode("utf-8"), False
    except ValueError as exc:
        return 127, str(exc).encode("utf-8"), False
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return process.returncode, stdout + stderr, False
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        return -1, stdout + stderr, True


def _validation_log_bytes(
    *,
    command_id: str,
    command_text: str,
    output_bytes: bytes,
) -> bytes:
    command_digest = hashlib.sha256(command_text.encode("utf-8")).hexdigest()
    return (
        f"command_id: {command_id}\ncommand_sha256: {command_digest}\n"
        "--- output ---\n"
    ).encode("utf-8") + output_bytes


def _predecision_outcome_matches(
    records: list[dict[str, Any]] | list[Mapping[str, Any]],
    expected_outcome: Mapping[str, Any],
    *,
    runtime: Path,
) -> bool:
    exits = {str(record.get("command_id")): record.get("exit_code") for record in records}
    if expected_outcome.get("kind") == "GREEN":
        return all(exit_code == 0 for exit_code in exits.values())
    expected_id = str(expected_outcome.get("command_id"))
    if exits.get(expected_id) == 0:
        return False
    if any(
        exit_code != 0
        for command_id, exit_code in exits.items()
        if command_id != expected_id
    ):
        return False
    expected_record = next(
        (record for record in records if record.get("command_id") == expected_id),
        None,
    )
    if expected_record is None:
        return False
    output_ref = expected_record.get("output_ref")
    if not isinstance(output_ref, str):
        return False
    output_path = runtime / output_ref
    if not output_path.is_file():
        return False
    signature = str(expected_outcome.get("failure_signature"))
    return signature.encode("utf-8") in output_path.read_bytes()


def _validation_results_from_records(
    records: list[dict[str, Any]] | list[Mapping[str, Any]],
) -> tuple[ValidationResult, ...]:
    return tuple(
        ValidationResult(
            command_id=str(record["command_id"]),
            exit_code=int(record["exit_code"]),
            output_ref=str(record["output_ref"]),
            output_sha256=str(record["output_sha256"]),
        )
        for record in records
    )


def _snapshot_candidate_state(repo: Path) -> dict[str, Any]:
    git_dir = Path(_git_stdout(repo, "rev-parse", "--absolute-git-dir"))
    merge_head = git_dir / "MERGE_HEAD"
    return {
        "status": _git_stdout(repo, "status", "--porcelain=v1", "-uall"),
        "tree": _git_stdout(repo, "write-tree"),
        "main": _git_stdout(repo, "rev-parse", "refs/heads/main"),
        "merge_head": merge_head.read_bytes() if merge_head.is_file() else None,
        "hooks": _PREDECISION_VALIDATION.snapshot_git_hooks(repo),
    }


def _snapshot_git_hooks(repo: Path) -> dict[str, tuple[bytes, int]]:
    git_dir = Path(_git_stdout(repo, "rev-parse", "--absolute-git-dir"))
    hooks_dir = git_dir / "hooks"
    if not hooks_dir.is_dir():
        return {}
    return {
        path.relative_to(hooks_dir).as_posix(): (
            path.read_bytes(),
            path.stat().st_mode,
        )
        for path in sorted(hooks_dir.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def _restore_predecision_target(
    repo: Path,
    *,
    original_branch: str,
    original_head: str,
    original_main_sha: str,
    original_status: str,
    original_hooks: Mapping[str, tuple[bytes, int]],
) -> bool:
    subprocess.run(  # noqa: S603
        ["git", "merge", "--abort"],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    subprocess.run(  # noqa: S603
        ["git", "checkout", "-f", original_branch],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    subprocess.run(  # noqa: S603
        ["git", "restore", "--source", original_head, "--staged", "--worktree", "--", ":/"],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    subprocess.run(  # noqa: S603
        ["git", "update-ref", "refs/heads/main", original_main_sha],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    _PREDECISION_VALIDATION.remove_untracked_paths(repo)
    _PREDECISION_VALIDATION.restore_git_hooks(repo, original_hooks)
    try:
        return (
            _git_stdout(repo, "rev-parse", "--abbrev-ref", "HEAD") == original_branch
            and _git_stdout(repo, "rev-parse", "HEAD") == original_head
            and _git_stdout(repo, "rev-parse", "refs/heads/main")
            == original_main_sha
            and _git_stdout(repo, "status", "--porcelain=v1", "-uall")
            == original_status
            and _PREDECISION_VALIDATION.snapshot_git_hooks(repo)
            == dict(original_hooks)
            and not (
                Path(_git_stdout(repo, "rev-parse", "--absolute-git-dir"))
                / "MERGE_HEAD"
            ).exists()
        )
    except (OSError, subprocess.CalledProcessError):
        return False


def _remove_untracked_paths(repo: Path) -> None:
    status = _git_stdout(repo, "status", "--porcelain=v1", "-uall")
    for line in status.splitlines():
        if not line.startswith("?? "):
            continue
        candidate = (repo / line[3:]).resolve()
        if not candidate.is_relative_to(repo.resolve()):
            continue
        if candidate.is_dir() and not candidate.is_symlink():
            shutil.rmtree(candidate)
        else:
            candidate.unlink(missing_ok=True)


def _restore_git_hooks(
    repo: Path,
    original_hooks: Mapping[str, tuple[bytes, int]],
) -> None:
    git_dir = Path(_git_stdout(repo, "rev-parse", "--absolute-git-dir"))
    hooks_dir = git_dir / "hooks"
    hooks_dir.mkdir(parents=True, exist_ok=True)
    current = _PREDECISION_VALIDATION.snapshot_git_hooks(repo)
    for relative in current.keys() - original_hooks.keys():
        (hooks_dir / relative).unlink(missing_ok=True)
    for relative, (payload, mode) in original_hooks.items():
        path = hooks_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(mode)


def _atomic_write_once_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    if path.exists() or temporary_path.exists():
        raise FileExistsError(path)
    temporary_path.write_bytes(payload)
    temporary_path.replace(path)


class _PredecisionValidationOperations:
    """Private namespace preserving the twelve-function public module surface."""

    load_bound_task_validation = staticmethod(_load_bound_task_validation)
    execute_predecision_task_validation = staticmethod(
        _execute_predecision_task_validation
    )
    verify_predecision_validation_evidence = staticmethod(
        _verify_predecision_validation_evidence
    )
    validation_command_paths = staticmethod(_validation_command_paths)
    run_validation_command = staticmethod(_run_validation_command)
    validation_log_bytes = staticmethod(_validation_log_bytes)
    predecision_outcome_matches = staticmethod(_predecision_outcome_matches)
    validation_results_from_records = staticmethod(_validation_results_from_records)
    snapshot_candidate_state = staticmethod(_snapshot_candidate_state)
    snapshot_git_hooks = staticmethod(_snapshot_git_hooks)
    restore_predecision_target = staticmethod(_restore_predecision_target)
    remove_untracked_paths = staticmethod(_remove_untracked_paths)
    restore_git_hooks = staticmethod(_restore_git_hooks)
    atomic_write_once_bytes = staticmethod(_atomic_write_once_bytes)


_PREDECISION_VALIDATION = _PredecisionValidationOperations()
del (
    _load_bound_task_validation,
    _execute_predecision_task_validation,
    _verify_predecision_validation_evidence,
    _validation_command_paths,
    _run_validation_command,
    _validation_log_bytes,
    _predecision_outcome_matches,
    _validation_results_from_records,
    _snapshot_candidate_state,
    _snapshot_git_hooks,
    _restore_predecision_target,
    _remove_untracked_paths,
    _restore_git_hooks,
    _atomic_write_once_bytes,
)
