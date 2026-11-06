"""Thin public CLI composition for exactly ``init``, ``run``, and ``status``.

This module wires the existing components in the closed causal order.
It does not invent a fourth command, a predecessor identity, or a
promotion effect of its own.
"""

from __future__ import annotations

import errno
import hashlib
import inspect
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from task_transition_runner.adapters.cursor_headless import CursorInvocationError
from task_transition_runner.adapters.cursor_stream import BoundPromptBytes
from task_transition_runner.config import ConfigError, RunnerConfig, load_config
from task_transition_runner.decision_contracts import DecisionIdentity
from task_transition_runner.runner_code_authority import RunnerCodeAuthorityError
from task_transition_runner.runner_continuation_contract import (
    ContinuationError,
    ContinuationOrigin,
    load_continuation_artifact,
)
from task_transition_runner.runner_drain_transition import (
    SchedulerPhase,
    evaluate_drain_before_dispatch,
)
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    load_pending_promotion,
)
from task_transition_runner.worker_result import (
    WorkerResultError,
    WorkerResultIdentity,
    load_worker_result,
)
from task_transition_runner.runner_promotion_state import RunnerPromotionStateError
from task_transition_runner.runner_git_context import (
    acquire_runner_git_lock,
    release_runner_git_lock,
)
from task_transition_runner.runner_main_state_bootstrap import bootstrap_main_state
from task_transition_runner.runner_promotion_journal import (
    PromotionJournalError,
    PromotionJournalPhase,
    archive_promotion_journal_epoch,
    load_archived_promotion_journal,
    load_promotion_journal,
    promotion_journal_archive_path_for_identity,
    release_canonical_promotion_journal,
)
from task_transition_runner.status import (
    StatusView,
    classify_authority_first_status,
)
from task_transition_runner.task_activation import ActivationStatus
from task_transition_runner.task_contract import TaskContract, TaskContractError, load_task_contract
from task_transition_runner.task_end_promotion import (
    compose_adapter_git_worker_result,
    finalize_git_promotion_task_end_artifacts,
    prepare_git_promotion_task_end_artifacts,
)
from task_transition_runner.task_source import InstructionSourceBinding, TaskSourceFacts

import task_transition_runner.adapters.cursor_headless as adapter_module
import task_transition_runner.decision_contracts as decision_module
import task_transition_runner.runner_code_authority as authority_module
import task_transition_runner.runner_continuation_lifecycle as continuation_module
import task_transition_runner.runner_continuation_origin_proofs as continuation_proofs
import task_transition_runner.runner_drain_transition as drain_module
import task_transition_runner.runner_git_context as git_context_module
import task_transition_runner.runner_post_tick_continuation as post_tick_module
import task_transition_runner.runner_promotion_reconciliation as reconciliation_module
import task_transition_runner.runner_promotion_state as state_module
import task_transition_runner.runner_start_tick_reconciliation as start_tick_module
import task_transition_runner.task_activation as task_activation_module
import task_transition_runner.task_source as task_source_module

COMMANDS = ("init", "run", "status")
_TRAVERSAL_REASON = "D_COMPOSITION_TRAVERSAL"
_LOCK_NAME = "runner_git.lock"
_CLI_RUN_ID = "cli-run"
_STALE_AFTER_SECONDS = 3600
_CONTINUE_RECONCILIATION = frozenset(
    {"RECONCILIATION_NO_WORK", "RECONCILIATION_COMPLETED"}
)
_DEFERRED_RECONCILIATION_REPROOF = frozenset(
    {
        "RECONCILIATION_MAIN_STATE_DRIFT",
        "RECONCILIATION_IDENTITY_MISMATCH",
    }
)
_RUN_LOCK: dict[str, Any] = {}


class _SeamBlocked(Exception):
    """Internal fail-closed stop for a negative composition seam."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def verify_code_authority(config: object) -> dict[str, Any]:
    if not isinstance(config, RunnerConfig):
        return _blocked_payload("CODE_AUTHORITY_UNAVAILABLE")
    try:
        authority_module.verify_steady_state_code_authority(
            entry_point=Path(__file__).resolve(),
            target_root=config.target_worktree,
            runtime_root=config.runtime_root,
        )
    except RunnerCodeAuthorityError as exc:
        _private_fresh_baseline(config)
        return _blocked_payload(exc.reason_code)
    return {"ok": True, "reason_code": "SOURCE_CHECKED"}


def reconcile_ordinary_promotion(config: object) -> dict[str, Any]:
    if not isinstance(config, RunnerConfig):
        return _blocked_payload("RECONCILIATION_UNAVAILABLE")
    try:
        verdict = reconciliation_module.reconcile_ordinary_promotion(
            repo=config.target_worktree,
            runtime_root=config.runtime_root,
            drain_run_id=_CLI_RUN_ID,
            main_ref=config.main_ref,
            diagnostic_refs=None,
        )
    except KeyError:
        return _blocked_payload("RECONCILIATION_PENDING_INVALID")
    if verdict.reason_code not in _CONTINUE_RECONCILIATION:
        return _blocked_payload(verdict.reason_code)
    reconciliation_module.observe_canonical_first_parent_tock(
        repo=config.target_worktree,
        main_ref=config.main_ref,
        promotion_attempt_id=_known_promotion_attempt(config),
        validated_tree_oid=_git_text(config.target_worktree, "rev-parse", "HEAD^{tree}"),
        diagnostic_refs=None,
    )
    try:
        main_state = state_module.load_and_validate_main_state(
            main_state_path=config.runtime_root / "main_state.yaml",
        )
    except RunnerPromotionStateError:
        main_state = None
    state_module.load_promotion_consumed(
        promotion_consumed_path=config.runtime_root / "promotion_consumed.yaml",
    )
    start_tick_module.reconcile_start_tick(
        decision=SimpleNamespace(
            status=ActivationStatus.TERMINAL_REST,
            reason_code="START_TICK_WINDOW",
            identity=None,
        ),
        target_worktree=config.target_worktree,
        main_state_path=config.runtime_root / "main_state.yaml",
        lock_path=config.runtime_root / _LOCK_NAME,
        ownership=_RUN_LOCK.get("ownership") or {},
        main_branch=config.target.main_branch,
    )
    try:
        drain_module.load_persisted_drain_outcome(runtime_root=config.runtime_root)
    except drain_module.DrainOutcomeError:
        return _blocked_payload("DRAIN_OUTCOME_INVALID")
    return {
        "ok": True,
        "reason_code": verdict.reason_code,
        "main_state": main_state,
        "identity": {
            "task_id": verdict.task_id,
            "origin_run_id": verdict.origin_run_id,
            "promotion_attempt_id": verdict.promotion_attempt_id,
            "result_sha": verdict.result_sha,
            "tock_sha": verdict.tock_sha,
        },
    }


def bind_ordinary_continuation(
    config: object,
    *,
    predecessor_basis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(config, RunnerConfig):
        return _blocked_payload("CONTINUATION_UNAVAILABLE")
    if (
        predecessor_basis is not None
        and predecessor_basis is not _RUN_LOCK.get("predecessor_basis")
    ):
        return _blocked_payload("ACTIVATION_PREDECESSOR_UNAVAILABLE")
    try:
        basis = (
            predecessor_basis
            if predecessor_basis is not None
            else _canonical_settled_predecessor_basis(config)
        )
    except _SeamBlocked as exc:
        return _blocked_payload(exc.reason_code)
    if basis is None:
        continuation_path = config.runtime_root / "post_tick_continuation.yaml"
        if continuation_path.is_file():
            try:
                continuation = load_continuation_artifact(continuation_path)
            except ContinuationError as exc:
                return _blocked_payload(exc.reason_code)
            if continuation.status == "CONSUMED":
                return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
        return {"ok": True, "reason_code": "CONTINUATION_NOT_REQUIRED"}
    reason = "CONTINUATION_ORIGIN_INVALID"
    try:
        if _private_fresh_baseline(config):
            _prepare_settled_continuation(config)
        else:
            continuation_proofs.prove_finalized_promotion_tick(
                repo=config.target_worktree,
                runtime=config.runtime_root,
                main_ref=config.main_ref,
            )
            return {
                "ok": True,
                "reason_code": "FINALIZED_PROMOTION_TICK",
            }
    except ContinuationError as exc:
        reason = exc.reason_code
    return _blocked_payload(reason)


def start_cursor_adapter(config: object) -> dict[str, Any]:
    if not isinstance(config, RunnerConfig):
        return _blocked_payload("CURSOR_ADAPTER_UNAVAILABLE")
    try:
        outcome = _invoke_cursor_adapter(config)
    except CursorInvocationError as exc:
        return _blocked_payload(exc.reason_code)
    return {
        "ok": True,
        "reason_code": "ADAPTER_COMPLETED",
        "adapter_outcome": outcome,
    }


def return_to_main(config: object) -> dict[str, Any]:
    if not isinstance(config, RunnerConfig):
        return _blocked_payload("RETURN_TO_MAIN_UNAVAILABLE")
    repo = config.target_worktree
    main_branch = config.target.main_branch
    branch = _git_text(repo, "rev-parse", "--abbrev-ref", "HEAD")
    if branch != main_branch:
        subprocess.run(  # noqa: S603
            ["git", "checkout", main_branch],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        )
        branch = main_branch
    return {
        "ok": True,
        "reason_code": "RETURNED_TO_MAIN",
        "branch": branch,
    }


def evaluate_task_activation(config: RunnerConfig, *, predecessor: dict[str, Any]) -> Any:
    contract = _load_activation_contract(config)
    if contract.active_task is None or "kind" not in predecessor:
        facts = TaskSourceFacts(
            contract=contract,
            sources=(),
            run_root=config.runtime_root / "terminal-rest",
        )
    else:
        probe = config.runtime_root / "identity-probe"
        run_root = probe if probe.is_dir() else config.runtime_root / "runs" / "cli-activation"
        facts = task_source_module.load_task_source(config, run_root=run_root)
    return task_activation_module.evaluate_task_activation(
        facts,
        predecessor=predecessor,
    )


def main(argv: list[str] | None = None) -> int:
    tokens = list(argv)
    command = tokens[0]
    rest = tokens[1:]
    if command not in COMMANDS:
        return _emit(_blocked(command, "CLI_COMMAND_UNKNOWN"))
    config_path = _option_value(rest, "--config")
    if config_path is None:
        return _emit(_blocked(command, "CLI_CONFIG_REQUIRED"))
    as_json = command == "status" and "--json" in rest
    try:
        config = load_config(Path(config_path))
    except ConfigError as exc:
        return _emit(_blocked(command, exc.reason_code), as_json=as_json)
    if command == "init":
        return _command_init(config)
    if command == "status":
        return _command_status(config, as_json=as_json)
    return _command_run(config)


def _command_init(config: RunnerConfig) -> int:
    result = bootstrap_main_state(
        target_worktree=config.target_worktree,
        runtime_root=config.runtime_root,
        main_branch=config.target.main_branch,
        baseline_validation_command=config.baseline_validation.command,
        baseline_validation_timeout_seconds=config.baseline_validation.timeout_seconds,
        updated_at_utc=_utc_stamp(),
    )
    reason = _blocked_reason(result)
    if reason is not None:
        return _emit(_blocked("init", reason))
    written = result["artifacts_written"]
    return _emit(
        {
            "ok": True,
            "command": "init",
            "outcome": "INITIALIZED",
            "reason_code": "BOOTSTRAP_OK",
            "main_state": result["main_state"],
            "artifacts_written": {
                "main_state": str(written["main_state"]),
                "promotion_consumed": str(written["promotion_consumed"]),
            },
        }
    )


def _command_status(config: RunnerConfig, *, as_json: bool) -> int:
    view = classify_authority_first_status(
        entry_point=Path(__file__).resolve(),
        target_worktree=config.target_worktree,
        runtime_root=config.runtime_root,
    )
    return _emit(_status_command_payload(view), as_json=as_json)


def _command_run(config: RunnerConfig) -> int:
    ownership: dict[str, Any] | None = None
    lock_path = config.runtime_root / _LOCK_NAME
    try:
        authority = _require_ok(verify_code_authority(config))
        preflight = git_context_module.evaluate_branch_preflight(
            target_worktree=config.target_worktree,
            main_branch=config.target.main_branch,
        )
        preflight = _qualify_task_branch_resume_preflight(config, preflight)
        if (
            preflight.get("ok") is not True
            and authority.get("reason_code") != _TRAVERSAL_REASON
        ):
            return _emit(
                _blocked(
                    "run",
                    str(preflight.get("reason_code", "GIT_PREFLIGHT_BLOCKED")),
                )
            )
        acquired = _require_ok(
            acquire_runner_git_lock(
                lock_path=lock_path,
                run_id=_CLI_RUN_ID,
                pid=os.getpid(),
                started_at_utc=_utc_stamp(),
                now_utc=_utc_stamp(),
                stale_after_seconds=_STALE_AFTER_SECONDS,
                is_pid_alive=_is_local_pid_alive,
            )
        )
        ownership = acquired["ownership"]
        _RUN_LOCK["ownership"] = ownership
        entry_journal_phase = _entry_promotion_journal_phase(config)
        deferred_provenance_error = None
        continuation_path = config.runtime_root / "post_tick_continuation.yaml"
        if continuation_path.is_file():
            try:
                continuation = load_continuation_artifact(continuation_path)
            except ContinuationError as exc:
                deferred_provenance_error = _blocked_payload(exc.reason_code)
            else:
                if (
                    continuation.status == "CONSUMED"
                    and (
                        not (config.runtime_root / "promotion_journal.yaml").is_file()
                    )
                ):
                    deferred_provenance_error = _blocked_payload(
                        "RECONCILIATION_IDENTITY_MISMATCH"
                    )
        reconciled = reconcile_ordinary_promotion(config)
        if (
            reconciled.get("reason_code")
            not in _DEFERRED_RECONCILIATION_REPROOF
        ):
            _require_ok(reconciled)
        provenance = (
            None
            if deferred_provenance_error is not None
            else _require_ok(_deferred_successor_provenance_gate(config))
        )
        contract = _load_activation_contract(config)
        inflight = [
            worker
            for worker in _persisted_inflight_workers(config)
            if worker is not None and _canonical_decision_pair_present(config, worker)
        ]
        inflight = _current_authority_inflight_workers(
            config,
            contract,
            inflight,
            reconciled=reconciled,
        )
        reconciled = _reprove_deferred_successor_window(
            config,
            reconciled,
            inflight,
        )
        _require_ok(reconciled)
        if (
            entry_journal_phase
            in {
                PromotionJournalPhase.APPLYING,
                PromotionJournalPhase.GIT_APPLIED,
                PromotionJournalPhase.RECONCILED,
            }
            and reconciled.get("reason_code") == "RECONCILIATION_COMPLETED"
        ):
            return _emit(_public_outcome("RECOVERED", "RECONCILIATION_COMPLETED"))
        if _loaded_canonical_pending(config) is not None and len(inflight) == 1:
            activation = _restore_inflight_activation(config, inflight[0])
            return _later_decision_tail(
                config,
                activation,
                ownership,
                preflight,
            )
        if len(inflight) == 1:
            activation = _restore_inflight_activation(config, inflight[0])
            return _later_decision_tail(
                config,
                activation,
                ownership or {},
                preflight,
            )
        if deferred_provenance_error is not None:
            _require_ok(deferred_provenance_error)
        assert provenance is not None
        if not _decisions_ready(config):
            projected = _projected_run_outcome(config)
            if projected is not None:
                return _emit(projected)
        main_state = reconciled.get("main_state")
        fresh = task_activation_module._fresh_main_state_bound(main_state)
        basis = _activation_basis(
            config,
            main_state,
            reconciled=reconciled,
        )
        _RUN_LOCK["predecessor_basis"] = basis
        if (
            provenance.get("reason_code") == "CONTINUATION_CONSUMED"
            or (
                provenance.get("reason_code") == "CONTINUATION_NOT_REQUIRED"
                and _terminal_without_successor_activation(config, basis)
            )
        ):
            terminal = _pre_dispatch_terminal_activation(config, basis)
            if terminal is not None:
                return _emit(terminal)
        continuation_already_activated = _call_basis_consumer(
            _authorized_continuation_allows_resume,
            config,
            reconciled,
            predecessor_basis=basis,
        )
        if not continuation_already_activated:
            drain = _call_component(
                evaluate_drain_before_dispatch,
                repo_root=config.target_worktree,
                runtime_root=config.runtime_root,
                drain_run_id=_CLI_RUN_ID,
            )
            drain_allows_activation = (
                _predecessor_drain_allows_successor_activation(
                    config,
                    drain,
                    basis,
                )
            )
            if not fresh and not drain_allows_activation:
                _require_ok(_drain_gate(drain))
        terminal = _pre_dispatch_terminal_activation(config, basis)
        if terminal is not None:
            return _emit(terminal)
        if basis is None:
            activation = evaluate_task_activation(config, predecessor={})
        else:
            activation = evaluate_task_activation(config, predecessor=basis)
        status = getattr(activation, "status", None)
        if status is ActivationStatus.TERMINAL_REST:
            return _emit(_rest_payload(activation))
        _RUN_LOCK["identity"] = getattr(activation, "identity", None)
        if fresh or getattr(activation, "facts", None) is not None:
            return _fresh_active_tail(config, activation, basis)
        return _classic_active_tail(config, activation, ownership or {}, basis)
    except (
        _SeamBlocked,
        TaskContractError,
        task_activation_module.TaskActivationError,
    ) as exc:
        return _emit(_blocked("run", exc.reason_code))
    finally:
        held = ownership
        _RUN_LOCK.clear()
        if held is not None:
            release_runner_git_lock(lock_path=lock_path, ownership=held)


def _fresh_active_tail(
    config: RunnerConfig,
    activation: Any,
    predecessor_basis: dict[str, Any] | None = None,
) -> int:
    def _prepare_continuation_before_main_adoption() -> None:
        exact_ref = (
            f"refs/heads/runner/task/{identity.task_id}/{identity.origin_run_id}"
        )
        try:
            _prepare_settled_continuation(
                config,
                expected_continuation_head_sha=_git_text(
                    config.target_worktree,
                    "rev-parse",
                    exact_ref,
                ),
            )
        except ContinuationError as exc:
            raise _SeamBlocked(exc.reason_code) from exc

    basis = predecessor_basis
    if not isinstance(basis, dict):
        basis = _RUN_LOCK.get("predecessor_basis")
        if not isinstance(basis, dict):
            basis = _canonical_settled_predecessor_basis(config)
    settled = _settled_predecessor_from_basis(basis)
    historical_task_ref = None
    if settled is not None:
        historical_task_ref = (
            "refs/heads/runner/task/"
            f"{settled['task_id']}/{settled['origin_run_id']}"
        )
    before_main_adoption = (
        _prepare_continuation_before_main_adoption
        if settled is not None
        else None
    )

    if before_main_adoption is not None:
        _require_ok(
            _call_basis_consumer(
                bind_ordinary_continuation,
                config,
                predecessor_basis=basis,
            )
        )
    identity = activation.identity
    main_before_start = _git_text(
        config.target_worktree,
        "rev-parse",
        config.main_ref,
    )
    _materialize_canonical_run_snapshot(config, activation)
    start = start_tick_module.reconcile_start_tick(
        decision=activation,
        target_worktree=config.target_worktree,
        main_state_path=config.runtime_root / "main_state.yaml",
        lock_path=config.runtime_root / _LOCK_NAME,
        ownership=_RUN_LOCK.get("ownership") or {},
        main_branch=config.target.main_branch,
        before_main_adoption=before_main_adoption,
        historical_task_ref=historical_task_ref,
    )
    if start.status not in {
        start_tick_module.StartTickStatus.MATERIALIZED,
        start_tick_module.StartTickStatus.REUSED,
    }:
        raise _SeamBlocked(str(start.reason_code))
    main_after_start = _git_text(
        config.target_worktree,
        "rev-parse",
        config.main_ref,
    )
    _checkout_task_branch(config, start)
    adapter_payload = start_cursor_adapter(config)
    if adapter_payload.get("ok") is not True:
        raise _SeamBlocked(str(adapter_payload.get("reason_code", "ADAPTER_INTERRUPTED")))
    _materialize_result_boundary(config, identity, start)
    allowed_paths = _run_allowed_paths(config)
    task_branch = _git_text(config.target_worktree, "rev-parse", "--abbrev-ref", "HEAD")
    run_dir = config.runtime_root / "runs" / identity.origin_run_id
    try:
        composed = _require_ok(
            compose_adapter_git_worker_result(
                repo_root=config.target_worktree,
                runtime_root=config.runtime_root,
                task_id=identity.task_id,
                origin_run_id=identity.origin_run_id,
                promotion_attempt_id=identity.promotion_attempt_id,
                task_branch=task_branch,
                developer=_bound_prompt_file(
                    run_dir / "instructions" / "prompts" / "developer"
                ),
                task_spec=_bound_prompt_file(run_dir / "instructions" / "task_spec"),
                adapter_outcome=adapter_payload.get("adapter_outcome"),
                allowed_paths=allowed_paths,
                claimed_identity=None,
            )
        )
    except _SeamBlocked:
        _git_text(
            config.target_worktree,
            "update-ref",
            config.main_ref,
            main_before_start,
            main_after_start,
        )
        raise
    except RuntimeError as exc:
        raise _SeamBlocked("COMPOSITION_FAILED") from exc
    prepared = prepare_git_promotion_task_end_artifacts(
        repo_root=config.target_worktree,
        runtime_root=config.runtime_root,
        worker_result=composed["worker_result"],
        allowed_paths=allowed_paths,
    )
    if prepared.get("ok") is not True:
        raise _SeamBlocked(str(prepared["reason_code"]))
    return _emit(
        {
            "ok": False,
            "command": "run",
            "outcome": "AWAITING_DECISIONS",
            "status": "awaiting",
            "reason_code": "PROMOTION_DECISION_MISSING",
        }
    )


def _materialize_canonical_run_snapshot(
    config: RunnerConfig,
    activation: Any,
) -> None:
    identity = activation.identity
    previous_facts = activation.facts
    facts = task_source_module.load_task_source(
        config,
        run_root=config.runtime_root / "runs" / identity.origin_run_id,
        previous_facts=previous_facts,
    )
    active = facts.contract.active_task
    observed = (
        active.task_id if active is not None else None,
        facts.contract.fingerprint,
        tuple(
            (source.logical_ref, source.sha256, source.byte_length)
            for source in facts.sources
        ),
    )
    required = (
        identity.task_id,
        identity.task_fingerprint,
        tuple(
            (source.logical_ref, source.sha256, source.byte_length)
            for source in identity.instruction_sources
        ),
    )
    if observed != required:
        raise _SeamBlocked("ACTIVATION_BASIS_INVALID")


def _later_decision_tail(
    config: RunnerConfig,
    activation: Any,
    ownership: dict[str, Any],
    preflight: dict[str, Any],
) -> int:
    identity = activation.identity
    worker = _read_yaml(
        config.runtime_root / "runs" / identity.origin_run_id / "worker_result.yaml"
    )
    result_sha = worker["result_sha"]
    expected = DecisionIdentity(
        task_id=identity.task_id,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
        result_sha=result_sha,
    )
    decisions = (
        config.runtime_root / "runs" / identity.origin_run_id / "decisions"
    )
    orchestrator_state = decision_module.classify_orchestrator_decision(
        decisions / "orchestrator_decision.yaml",
        expected_identity=expected,
        artifact_root=config.runtime_root,
    )
    validator_state = decision_module.classify_validator_decision(
        decisions / "validator_decision.yaml",
        expected_identity=expected,
        artifact_root=config.runtime_root,
    )
    if (
        orchestrator_state is decision_module.DecisionArtifactState.INVALID
        or validator_state is decision_module.DecisionArtifactState.INVALID
    ):
        return _emit(_blocked("run", "PROMOTION_DECISION_INVALID"))
    orchestrator = decision_module.load_orchestrator_decision(
        decisions / "orchestrator_decision.yaml",
        expected_identity=expected,
        artifact_root=config.runtime_root,
    )
    validator = decision_module.load_validator_decision(
        decisions / "validator_decision.yaml",
        expected_identity=expected,
        artifact_root=config.runtime_root,
    )
    pending_path = config.runtime_root / "pending_promotion.yaml"
    if pending_path.is_file():
        load_pending_promotion(runtime_root=config.runtime_root)
        return _resume_qualified_pending(config, identity, ownership)
    lock_fact = git_context_module.verify_runner_git_lock_ownership(
        lock_path=config.runtime_root / _LOCK_NAME,
        ownership=ownership,
    )
    finalized = _require_ok(
        finalize_git_promotion_task_end_artifacts(
            repo_root=config.target_worktree,
            runtime_root=config.runtime_root,
            origin_run_id=identity.origin_run_id,
            task_snapshot=_task_snapshot_facts(config, identity.origin_run_id),
            instruction_snapshot=_instruction_snapshot_facts(
                config,
                identity.origin_run_id,
            ),
            expected_outcome=_active_expected_outcome(config),
            allowed_paths=_run_allowed_paths(config),
            risk_facts={},
            lock_ownership=lock_fact,
            git_preflight=preflight,
            consumed_payload=state_module.load_promotion_consumed(
                promotion_consumed_path=config.runtime_root / "promotion_consumed.yaml",
            ),
            red_lock=_read_yaml(config.runtime_root / "red_lock.yaml"),
            unresolved_foreign_epoch=False,
            timeout_seconds=config.baseline_validation.timeout_seconds,
            poll_seconds=1,
        )
    )
    load_pending_promotion(runtime_root=config.runtime_root)
    return _emit(
        {
            "ok": True,
            "command": "run",
            "outcome": "PENDING_PROMOTION",
            "status": "pending",
            "reason_code": str(finalized["reason_code"]),
        }
    )


def _classic_active_tail(
    config: RunnerConfig,
    activation: Any,
    ownership: dict[str, Any],
    predecessor_basis: dict[str, Any] | None = None,
) -> int:
    _require_ok(
        _call_basis_consumer(
            bind_ordinary_continuation,
            config,
            predecessor_basis=predecessor_basis,
        )
    )
    identity = activation.identity
    run_dir = config.runtime_root / "runs" / identity.origin_run_id
    allowed_paths = _run_allowed_paths(config)
    task_branch = _git_text(
        config.target_worktree,
        "rev-parse",
        "--abbrev-ref",
        "HEAD",
    )
    if (run_dir / "worker_result.yaml").is_file():
        adapter_outcome = None
    elif (run_dir / "adapter_stream_block.yaml").is_file():
        raise _SeamBlocked("ADAPTER_INTERRUPTED")
    else:
        adapter_payload = start_cursor_adapter(config)
        if (
            adapter_payload.get("ok") is not True
            and "adapter_outcome" not in adapter_payload
        ):
            raise _SeamBlocked(str(adapter_payload.get("reason_code", "SEAM_BLOCKED")))
        adapter_outcome = adapter_payload.get("adapter_outcome")
    composed = _require_ok(
        compose_adapter_git_worker_result(
            repo_root=config.target_worktree,
            runtime_root=config.runtime_root,
            task_id=identity.task_id,
            origin_run_id=identity.origin_run_id,
            promotion_attempt_id=identity.promotion_attempt_id,
            task_branch=task_branch,
            developer=_bound_prompt_file(run_dir / "instructions" / "prompts" / "developer"),
            task_spec=_bound_prompt_file(run_dir / "instructions" / "task_spec"),
            adapter_outcome=adapter_outcome,
            allowed_paths=allowed_paths,
            claimed_identity=None,
        )
    )
    _require_ok(
        prepare_git_promotion_task_end_artifacts(
            repo_root=config.target_worktree,
            runtime_root=config.runtime_root,
            worker_result=composed["worker_result"],
            allowed_paths=allowed_paths,
        )
    )
    finalized = _require_ok(
        finalize_git_promotion_task_end_artifacts(
            repo_root=config.target_worktree,
            runtime_root=config.runtime_root,
            lock_ownership=ownership,
        )
    )
    _require_ok(return_to_main(config))
    return _emit(
        {
            "ok": True,
            "command": "run",
            "outcome": "PENDING_PROMOTION",
            "status": "pending",
            "reason_code": str(finalized.get("reason_code", "PROMOTION_INTENT_PERSISTED")),
            "pending": {"artifact_class": finalized.get("artifact_class")},
            "journal_start": finalized.get("journal_start", False),
            "tock_sha": finalized.get("tock_sha"),
        }
    )


def _resume_qualified_pending(
    config: RunnerConfig,
    identity: Any,
    ownership: dict[str, Any],
) -> int:
    pending = load_pending_promotion(runtime_root=config.runtime_root)
    _require_ok(_persisted_drain_matches_pending(config, pending))
    returned = return_to_main(config)
    if not _return_authorizes_drain(returned):
        raise _SeamBlocked("RETURN_TO_MAIN_UNAVAILABLE")
    try:
        decision = evaluate_drain_before_dispatch(
            repo_root=config.target_worktree,
            runtime_root=config.runtime_root,
            drain_run_id=_CLI_RUN_ID,
            effect_admission=_effect_admission(config, identity, ownership),
        )
    except RunnerGitPromotionError as exc:
        raise _SeamBlocked(exc.reason_code) from exc
    if not _drain_decision_authorizes_effect(decision, pending):
        outcome = getattr(decision, "outcome", None)
        raise _SeamBlocked(str(getattr(outcome, "reason_code", "DRAIN_BLOCKED")))
    continuation_path = config.runtime_root / "post_tick_continuation.yaml"
    if not continuation_path.is_file():
        return _emit(
            {
                "ok": True,
                "command": "run",
                "outcome": "DRAINED_AWAITING_TASK",
                "status": "drained",
                "reason_code": "PROMOTION_EFFECT_ADMITTED",
            }
        )
    try:
        consumed = post_tick_module.consume_post_tick_continuation(
            repo=config.target_worktree,
            runtime=config.runtime_root,
            main_ref=config.main_ref,
            successor_task_id=identity.task_id,
            successor_promotion_attempt_id=identity.promotion_attempt_id,
            successor_tock_sha=str(getattr(getattr(decision, "outcome", None), "tock_sha", "")),
            consumed_at_utc=_utc_stamp(),
            lock_ownership=ownership,
        )
    except ContinuationError as exc:
        raise _SeamBlocked(exc.reason_code) from exc
    if getattr(consumed, "status", None) != "CONSUMED":
        raise _SeamBlocked("CONTINUATION_LIFECYCLE_INVALID")
    return _emit(
        {
            "ok": True,
            "command": "run",
            "outcome": "SETTLED",
            "status": "settled",
            "reason_code": "CONTINUATION_CONSUMED",
        }
    )


def _effect_admission(
    config: RunnerConfig,
    identity: Any,
    ownership: dict[str, Any],
) -> dict[str, Any]:
    pending = load_pending_promotion(runtime_root=config.runtime_root)
    worker_path = (
        config.runtime_root / "runs" / identity.origin_run_id / "worker_result.yaml"
    )
    worker_payload = _read_yaml(worker_path) or {}
    validation_digests = {
        str(item["command_id"]): str(item["output_sha256"])
        for item in worker_payload.get("validation_results", ())
    }
    if not validation_digests:
        raise RunnerGitPromotionError("PROMOTION_EVIDENCE_MISSING")
    worker_result = load_worker_result(
        worker_path,
        expected_identity=WorkerResultIdentity(
            task_id=identity.task_id,
            origin_run_id=identity.origin_run_id,
            promotion_attempt_id=identity.promotion_attempt_id,
            result_sha=str(worker_payload.get("result_sha", "")),
        ),
        expected_validation_digests=validation_digests,
    )
    expected = DecisionIdentity(
        task_id=identity.task_id,
        origin_run_id=identity.origin_run_id,
        promotion_attempt_id=identity.promotion_attempt_id,
        result_sha=worker_result.result_sha,
    )
    decisions = config.runtime_root / "runs" / identity.origin_run_id / "decisions"
    lock_fact = git_context_module.verify_runner_git_lock_ownership(
        lock_path=config.runtime_root / _LOCK_NAME,
        ownership=ownership,
    )
    preflight = git_context_module.evaluate_branch_preflight(
        target_worktree=config.target_worktree,
        main_branch=config.target.main_branch,
    )
    return {
        "runtime_root": config.runtime_root,
        "target_worktree": config.target_worktree,
        "validation_command": (),
        "validation_timeout_seconds": 0,
        "command_id": "",
        "task_snapshot": pending["task_snapshot"],
        "instruction_snapshot": pending["instruction_snapshot"],
        "worker_result": worker_result,
        "expected_outcome": _active_expected_outcome(config),
        "orchestrator_decision": decision_module.load_orchestrator_decision(
            decisions / "orchestrator_decision.yaml",
            expected_identity=expected,
            artifact_root=config.runtime_root,
        ),
        "validator_decision": decision_module.load_validator_decision(
            decisions / "validator_decision.yaml",
            expected_identity=expected,
            artifact_root=config.runtime_root,
        ),
        "base_sha": str(pending["base_sha"]),
        "task_branch_head_sha": worker_result.result_sha,
        "allowed_paths": tuple(pending["allowed_paths"]),
        "risk_facts": {},
        "lock_ownership": lock_fact,
        "git_preflight": preflight,
        "consumed_payload": state_module.load_promotion_consumed(
            promotion_consumed_path=config.runtime_root / "promotion_consumed.yaml",
        ),
        "red_lock": _read_yaml(config.runtime_root / "red_lock.yaml"),
        "unresolved_foreign_epoch": False,
    }


def _prepare_settled_continuation(
    config: RunnerConfig,
    *,
    expected_continuation_head_sha: str | None = None,
) -> None:
    identity = _RUN_LOCK.get("identity")
    continuation_module.prepare_post_tick_continuation(
        repo=config.target_worktree,
        runtime=config.runtime_root,
        main_ref=config.main_ref,
        continuation_origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        successor_task_id=identity.task_id,
        successor_origin_run_id=identity.origin_run_id,
        successor_promotion_attempt_id=identity.promotion_attempt_id,
        expected_continuation_head_sha=(
            expected_continuation_head_sha
            if expected_continuation_head_sha is not None
            else _git_text(config.target_worktree, "rev-parse", "HEAD")
        ),
        lock_ownership=_RUN_LOCK.get("ownership") or {},
        prepared_at_utc=_utc_stamp(),
    )


def _invoke_cursor_adapter(config: RunnerConfig) -> Any:
    developer, task_spec = _run_local_or_live_prompts(config)
    identity = _RUN_LOCK.get("identity")
    attempt_token = str(
        getattr(identity, "promotion_attempt_id", _CLI_RUN_ID)
    )
    return adapter_module.run_cursor_adapter(
        configured_executable=config.adapter.executable,
        target_root=config.target_worktree,
        runner_source_root=config.runner_source,
        runtime_root=config.runtime_root,
        developer=developer,
        task_spec=task_spec,
        attempt_token=attempt_token,
        timeout_seconds=config.adapter.timeout_seconds,
        model=config.adapter.model,
    )


def _checkout_task_branch(config: RunnerConfig, start: object) -> None:
    task_branch = str(getattr(start, "task_branch"))
    subprocess.run(  # noqa: S603
        ["git", "checkout", task_branch],
        cwd=config.target_worktree,
        check=True,
        capture_output=True,
        text=True,
    )


def _materialize_result_boundary(
    config: RunnerConfig,
    identity: object,
    start: object,
) -> None:
    repo = config.target_worktree
    start_sha = str(getattr(start, "tick_sha", ""))
    head_sha = _git_text(repo, "rev-parse", "HEAD")
    if head_sha != start_sha:
        raise _SeamBlocked("DEVELOPER_COMMIT_BLOCKED")
    dirty = _worktree_change_paths(repo)
    allowed = _run_allowed_paths(config)
    if any(path not in allowed for path in dirty):
        raise _SeamBlocked("FOREIGN_PATH_BLOCKED")
    if dirty:
        _git_text(repo, "add", "--", *dirty)
        tree_oid = _git_text(repo, "write-tree")
    else:
        tree_oid = _git_text(repo, "rev-parse", "HEAD^{tree}")
    result_sha = _git_text(
        repo,
        "commit-tree",
        tree_oid,
        "-p",
        head_sha,
        "-m",
        (
            f"RESULT {identity.task_id} {identity.origin_run_id} "
            f"{identity.promotion_attempt_id}"
        ),
    )
    task_branch = str(getattr(start, "task_branch", ""))
    _git_text(
        repo,
        "update-ref",
        f"refs/heads/{task_branch}",
        result_sha,
        head_sha,
    )


def _worktree_change_paths(repo: Path) -> tuple[str, ...]:
    completed = subprocess.run(  # noqa: S603
        [
            "git",
            "--no-optional-locks",
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return tuple(line[3:] for line in completed.stdout.splitlines())


def _run_local_or_live_prompts(
    config: RunnerConfig,
) -> tuple[BoundPromptBytes, BoundPromptBytes]:
    identity = _RUN_LOCK.get("identity")
    origin = getattr(identity, "origin_run_id", "")
    root = config.runtime_root / "runs" / str(origin) / "instructions"
    developer_path = root / "prompts" / "developer"
    spec_path = root / "task_spec"
    if (developer_path.is_file(), spec_path.is_file()) == (True, True):
        return (
            _bound_prompt_file(developer_path),
            _bound_prompt_file(spec_path),
        )
    return (
        _bound_prompt_file(Path(config.prompts.developer)),
        _bound_prompt_file(config.task_source),
    )


_INSTRUCTION_BINDING_SPECS = (
    ("spec", "instructions/task_spec"),
    ("developer", "instructions/prompts/developer"),
    ("orchestrator", "instructions/prompts/orchestrator"),
    ("validator", "instructions/prompts/validator"),
)


def _canonical_decision_pair_present(
    config: RunnerConfig,
    worker: dict[str, Any],
) -> bool:
    folder = (
        config.runtime_root
        / "runs"
        / str(worker["origin_run_id"])
        / "decisions"
    )
    return (folder / "orchestrator_decision.yaml").is_file() and (
        folder / "validator_decision.yaml"
    ).is_file()


def _persisted_inflight_workers(config: RunnerConfig) -> list[dict[str, Any]]:
    runs = config.runtime_root / "runs"
    if not runs.is_dir():
        return []
    found: list[dict[str, Any]] = []
    for worker_path in sorted(runs.glob("*/worker_result.yaml")):
        found.append(_read_yaml(worker_path))
    return found


def _current_authority_inflight_workers(
    config: RunnerConfig,
    contract: TaskContract,
    workers: list[dict[str, Any]],
    *,
    reconciled: dict[str, Any],
    settled_predecessor: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    settled = settled_predecessor
    if settled is None:
        try:
            settled = _settled_predecessor_from_basis(
                _canonical_settled_predecessor_basis(
                    config,
                    reconciled_identity=_completed_reconciliation_identity(reconciled),
                )
            )
        except _SeamBlocked as exc:
            if (
                exc.reason_code != "ACTIVATION_PREDECESSOR_UNAVAILABLE"
                or _entry_promotion_journal_phase(config)
                is PromotionJournalPhase.FINALIZED
            ):
                raise
            settled = None
    active = contract.active_task
    if settled is not None and active is not None:
        return [worker for worker in workers if worker.get("task_id") == active.task_id]
    return workers


def _restore_inflight_activation(
    config: RunnerConfig,
    worker: dict[str, Any],
) -> task_activation_module.ActivationDecision:
    contract = load_task_contract(
        config.task_source,
        target_worktree=config.target_worktree,
    )
    origin_run_id = str(worker["origin_run_id"])
    sources = _instruction_bindings(config, contract, origin_run_id)
    identity = task_activation_module.ActivationIdentity(
        task_id=str(worker["task_id"]),
        task_fingerprint=contract.fingerprint,
        instruction_sources=sources,
        origin_run_id=origin_run_id,
        promotion_attempt_id=str(worker["promotion_attempt_id"]),
    )
    return task_activation_module.ActivationDecision(
        status=ActivationStatus.ACTIVATED,
        reason_code="FRESH_TASK_ACTIVATED",
        facts=TaskSourceFacts(
            contract=contract,
            sources=sources,
            run_root=config.runtime_root / "runs" / origin_run_id,
        ),
        identity=identity,
    )


def _instruction_bindings(
    config: RunnerConfig,
    contract: TaskContract,
    origin_run_id: str,
) -> tuple[InstructionSourceBinding, ...]:
    meta_path = _find_run_artifact(
        config,
        origin_run_id,
        Path("instructions") / "instruction_snapshot.yaml",
    )
    meta = _read_yaml(meta_path)
    return tuple(_binding_from_mapping(item) for item in meta["sources"])


def _binding_from_mapping(item: dict[str, Any]) -> InstructionSourceBinding:
    return InstructionSourceBinding(
        logical_ref=item["logical_ref"],
        snapshot_path=item.get("snapshot_path", ""),
        sha256=item["sha256"],
        byte_length=item["byte_length"],
    )


def _live_instruction_bindings(
    config: RunnerConfig,
    contract: TaskContract,
) -> tuple[InstructionSourceBinding, ...]:
    active = contract.active_task
    paths = {
        "spec": (active.declared_spec_ref, active.spec_ref),
        "developer": (config.prompts.declared_developer, config.prompts.developer),
        "orchestrator": (
            config.prompts.declared_orchestrator,
            config.prompts.orchestrator,
        ),
        "validator": (config.prompts.declared_validator, config.prompts.validator),
    }
    bindings: list[InstructionSourceBinding] = []
    for role, snapshot_path in _INSTRUCTION_BINDING_SPECS:
        logical_ref, source = paths[role]
        payload = source.read_bytes()
        bindings.append(
            InstructionSourceBinding(
                logical_ref=logical_ref,
                snapshot_path=snapshot_path,
                sha256=hashlib.sha256(payload).hexdigest(),
                byte_length=len(payload),
            )
        )
    return tuple(bindings)


def _find_run_artifact(
    config: RunnerConfig,
    origin_run_id: str,
    relative: Path,
) -> Path | None:
    candidates = (
        config.runtime_root / "runs" / origin_run_id / relative,
        config.runtime_root / "runs" / "cli-activation" / relative,
        config.runtime_root / "identity-probe" / relative,
    )
    return next(path for path in candidates if path.is_file())


def _task_snapshot_facts(config: RunnerConfig, origin_run_id: str) -> dict[str, Any]:
    contract = load_task_contract(
        config.task_source,
        target_worktree=config.target_worktree,
    )
    active = contract.active_task
    current_bytes = config.task_source.read_bytes()
    current = _task_view(
        active.task_id,
        contract.fingerprint,
        hashlib.sha256(current_bytes).hexdigest(),
    )
    snapshot_path = _find_run_artifact(
        config,
        origin_run_id,
        Path("task_snapshot.yaml"),
    )
    bound_bytes = snapshot_path.read_bytes()
    bound = _task_view(
        active.task_id,
        contract.fingerprint,
        hashlib.sha256(bound_bytes).hexdigest(),
    )
    return {"current": current, "bound": bound}


def _task_view(task_id: str, fingerprint: str, digest: str) -> dict[str, str]:
    return {
        "task_id": task_id,
        "fingerprint": fingerprint,
        "digest": digest,
    }


def _instruction_snapshot_facts(
    config: RunnerConfig,
    origin_run_id: str,
) -> dict[str, Any]:
    contract = load_task_contract(
        config.task_source,
        target_worktree=config.target_worktree,
    )
    current = _instruction_view(_live_instruction_bindings(config, contract))
    meta_path = _find_run_artifact(
        config,
        origin_run_id,
        Path("instructions") / "instruction_snapshot.yaml",
    )
    meta = _read_yaml(meta_path)
    return {"current": current, "bound": _instruction_view(meta["sources"])}


def _instruction_view(bindings: list[Any] | tuple[Any, ...]) -> dict[str, Any]:
    normalized = [_public_binding(item) for item in bindings]
    canonical = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return {
        "digest": hashlib.sha256(canonical).hexdigest(),
        "bindings": normalized,
    }


def _public_binding(item: object) -> dict[str, Any]:
    if isinstance(item, InstructionSourceBinding):
        return {
            "byte_length": item.byte_length,
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "snapshot_path": item.snapshot_path,
        }
    return {
        "byte_length": item.get("byte_length"),
        "logical_ref": item.get("logical_ref"),
        "sha256": item.get("sha256"),
        "snapshot_path": item.get("snapshot_path"),
    }


def _active_expected_outcome(config: RunnerConfig) -> Any:
    contract = load_task_contract(
        config.task_source,
        target_worktree=config.target_worktree,
    )
    return contract.active_task.expected_outcome


def _activation_basis(
    config: RunnerConfig,
    main_state: object,
    *,
    reconciled: dict[str, Any],
) -> dict[str, Any] | None:
    settled_basis = _canonical_settled_predecessor_basis(
        config,
        reconciled_identity=_completed_reconciliation_identity(reconciled),
    )
    if settled_basis is not None:
        return settled_basis
    if task_activation_module._fresh_main_state_bound(main_state):
        return task_activation_module.select_cli_activation_basis(
            main_state=main_state,
            settled_predecessor=None,
        )
    return None


def _canonical_settled_predecessor_basis(
    config: RunnerConfig,
    *,
    reconciled_identity: dict[str, Any] | None = None,
    predecessor_only_transition: bool = False,
) -> dict[str, Any] | None:
    runtime = config.runtime_root
    journal_path = runtime / "promotion_journal.yaml"
    if not journal_path.is_file():
        return None
    try:
        journal = load_promotion_journal(path=journal_path)
    except PromotionJournalError as exc:
        raise _SeamBlocked(exc.reason_code) from exc
    if journal.phase is not PromotionJournalPhase.FINALIZED:
        raise _SeamBlocked("ACTIVATION_PREDECESSOR_UNAVAILABLE")
    transition = None
    try:
        proof = continuation_proofs.prove_finalized_promotion_tick(
            repo=config.target_worktree,
            runtime=runtime,
            main_ref=config.main_ref,
        )
    except ContinuationError as exc:
        transition = _authorized_successor_transition(
            config,
            journal,
            predecessor_only=predecessor_only_transition,
        )
        if transition is None:
            reason = (
                "ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH"
                if exc.reason_code == "CONTINUATION_IDENTITY_MISMATCH"
                else exc.reason_code
            )
            raise _SeamBlocked(reason) from exc
        proof_identity = transition["predecessor_identity"]
        proof_tock_sha = transition["tock_sha"]
    else:
        proof_identity = proof.to_mapping()["predecessor_identity"]
        proof_tock_sha = proof.to_mapping()["tock_sha"]
    try:
        drain = drain_module.load_persisted_drain_outcome(runtime_root=runtime)
    except drain_module.DrainOutcomeError as exc:
        raise _SeamBlocked("ACTIVATION_PREDECESSOR_UNAVAILABLE") from exc
    if drain is None:
        raise _SeamBlocked("ACTIVATION_PREDECESSOR_UNAVAILABLE")
    observed = (
        drain.status,
        drain.reason_code,
        drain.drain_run_id,
        drain.observed_main_head_sha,
        drain.task_id,
        drain.origin_run_id,
        drain.promotion_attempt_id,
        drain.result_sha,
        drain.promotion_class,
        drain.tock_sha,
        proof_identity["task_id"],
        proof_identity["origin_run_id"],
        proof_identity["promotion_attempt_id"],
        proof_identity["result_sha"],
        proof_identity["promotion_class"],
        proof_tock_sha,
    )
    required = (
        drain_module.DrainStatus.DRAINED,
        "PROMOTION_EFFECT_ADMITTED",
        journal.drain_run_id,
        journal.tock_sha,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
    )
    if (
        observed != required
        and not _reconciled_finalized_identity_allows_pre_tock_drain(
            runtime=runtime,
            journal=journal,
            proof_identity=proof_identity,
            proof_tock_sha=proof_tock_sha,
            drain=drain,
            reconciled_identity=reconciled_identity,
        )
        and not _authorized_successor_start_allows_pre_start_drain(
            journal=journal,
            drain=drain,
            transition=transition,
        )
    ):
        raise _SeamBlocked("ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH")
    try:
        reproved = task_source_module.reprove_run_local_snapshot(
            run_root=runtime / "runs" / journal.origin_run_id,
            expected_task_fingerprint=journal.task_fingerprint,
            expected_task_snapshot_sha256=journal.task_snapshot_sha256,
            expected_instruction_signature_sha256=(
                journal.instruction_signature_sha256
            ),
        )
    except task_source_module.TaskSourceError as exc:
        raise _SeamBlocked(exc.reason_code) from exc
    if reproved.task_id != journal.task_id:
        raise _SeamBlocked("ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH")
    predecessor = {
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
    }
    return task_activation_module.select_cli_activation_basis(
        main_state=None,
        settled_predecessor=predecessor,
    )


def _completed_reconciliation_identity(
    reconciled: dict[str, Any],
) -> dict[str, Any] | None:
    identity = reconciled.get("identity")
    if (
        reconciled.get("reason_code") != "RECONCILIATION_COMPLETED"
        or not isinstance(identity, dict)
    ):
        return None
    return identity


def _reconciled_finalized_identity_allows_pre_tock_drain(
    *,
    runtime: Path,
    journal: Any,
    proof_identity: dict[str, Any],
    proof_tock_sha: str,
    drain: Any,
    reconciled_identity: dict[str, Any] | None,
) -> bool:
    if reconciled_identity is None or drain is None:
        return False
    try:
        consumed = state_module.load_promotion_consumed(
            promotion_consumed_path=runtime / "promotion_consumed.yaml"
        )
    except RunnerPromotionStateError:
        return False
    matching_consumptions = [
        entry
        for entry in consumed["consumed_promotions"]
        if entry["promotion_attempt_id"] == journal.promotion_attempt_id
        and entry["result_sha"] == journal.result_sha
        and entry["tock_sha"] == journal.tock_sha
    ]
    if len(matching_consumptions) != 1:
        return False
    pre_state = matching_consumptions[0]["main_state_before_promotion"]
    observed = (
        reconciled_identity.get("task_id"),
        reconciled_identity.get("origin_run_id"),
        reconciled_identity.get("promotion_attempt_id"),
        reconciled_identity.get("result_sha"),
        reconciled_identity.get("tock_sha"),
        journal.phase,
        proof_identity["task_id"],
        proof_identity["origin_run_id"],
        proof_identity["promotion_attempt_id"],
        proof_identity["result_sha"],
        proof_identity["promotion_class"],
        proof_tock_sha,
        drain.status,
        drain.reason_code,
        drain.drain_run_id,
        drain.observed_main_head_sha,
        drain.task_id,
        drain.origin_run_id,
        drain.promotion_attempt_id,
        drain.result_sha,
        drain.promotion_class,
        drain.tock_sha,
        matching_consumptions[0]["promotion_attempt_id"],
        matching_consumptions[0]["result_sha"],
        matching_consumptions[0]["tock_sha"],
        drain.observed_main_head_sha,
        pre_state["last_tock_sha"],
        pre_state["observed_main_head_sha"],
    )
    required = (
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.tock_sha,
        PromotionJournalPhase.FINALIZED,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
        drain_module.DrainStatus.NO_PENDING,
        "NO_PENDING_PROMOTION",
        journal.drain_run_id,
        pre_state["last_tock_sha"],
        None,
        None,
        None,
        None,
        None,
        None,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.tock_sha,
        pre_state["last_tock_sha"],
        pre_state["last_tock_sha"],
        journal.base_main_sha,
    )
    return observed == required


def _authorized_successor_start_allows_pre_start_drain(
    *,
    journal: Any,
    drain: Any,
    transition: dict[str, Any] | None,
) -> bool:
    if transition is None or drain is None:
        return False
    observed = (
        journal.phase,
        drain.status,
        drain.reason_code,
        drain.drain_run_id,
        drain.observed_main_head_sha,
        drain.task_id,
        drain.origin_run_id,
        drain.promotion_attempt_id,
        drain.result_sha,
        drain.promotion_class,
        drain.tock_sha,
        transition.get("tock_sha"),
    )
    required = (
        PromotionJournalPhase.FINALIZED,
        drain_module.DrainStatus.BLOCKED,
        "UNRESOLVED_PROMOTION_JOURNAL",
        _CLI_RUN_ID,
        journal.tock_sha,
        None,
        None,
        None,
        None,
        None,
        None,
        journal.tock_sha,
    )
    return observed == required


def _authorized_successor_transition(
    config: RunnerConfig,
    journal: Any,
    *,
    predecessor_only: bool = False,
) -> dict[str, Any] | None:
    continuation_path = config.runtime_root / "post_tick_continuation.yaml"
    if not continuation_path.is_file():
        return None
    try:
        continuation = load_continuation_artifact(continuation_path)
        main_state = state_module.load_and_validate_main_state(
            main_state_path=config.runtime_root / "main_state.yaml"
        )
        consumed = state_module.load_promotion_consumed(
            promotion_consumed_path=(
                config.runtime_root / "promotion_consumed.yaml"
            )
        )
        contract = (
            None
            if predecessor_only
            else load_task_contract(
                config.task_source,
                target_worktree=config.target_worktree,
            )
        )
        live_head = _git_text(
            config.target_worktree,
            "rev-parse",
            config.main_ref,
        )
        payload = continuation.to_mapping()
        chain = payload["commit_chain"]
        start_subject = _git_text(
            config.target_worktree,
            "log",
            "-1",
            "--format=%s",
            live_head,
        )
        subject_parts = start_subject.split(" ")
        start_parents = tuple(
            _git_text(
                config.target_worktree,
                "rev-list",
                "--parents",
                "-n",
                "1",
                live_head,
            ).split()[1:]
        )
        start_tree = _git_text(
            config.target_worktree,
            "rev-parse",
            f"{live_head}^{{tree}}",
        )
        predecessor_tree = _git_text(
            config.target_worktree,
            "rev-parse",
            f"{journal.tock_sha}^{{tree}}",
        )
        first_parent = tuple(
            _git_text(
                config.target_worktree,
                "rev-list",
                "--first-parent",
                "-n",
                "2",
                config.main_ref,
            ).splitlines()
        )
    except (
        ContinuationError,
        KeyError,
        RunnerPromotionStateError,
        subprocess.CalledProcessError,
        TaskContractError,
    ):
        return None
    evidence = payload["origin"]["evidence"]
    predecessor = evidence["predecessor_identity"]
    matching_consumptions = [
        entry
        for entry in consumed["consumed_promotions"]
        if entry["promotion_attempt_id"] == journal.promotion_attempt_id
        and entry["result_sha"] == journal.result_sha
        and entry["tock_sha"] == journal.tock_sha
    ]
    active_task = None if contract is None else contract.active_task
    start_task_id = subject_parts[1] if len(subject_parts) == 5 else None
    successor_task_id = (
        start_task_id if predecessor_only else continuation.successor_task_id
    )
    active_task_id = (
        start_task_id
        if predecessor_only
        else active_task.task_id if active_task is not None else None
    )
    successor_origin = subject_parts[2] if len(subject_parts) == 5 else None
    successor_attempt = subject_parts[3] if len(subject_parts) == 5 else None
    successor_identity_is_canonical = (
        _is_prefixed_sha256(successor_origin, "origin-")
        and _is_prefixed_sha256(successor_attempt, "attempt-")
    )
    expected_subject = (
        f"START {successor_task_id} {successor_origin} "
        f"{successor_attempt} {journal.tock_sha}"
    )
    observed = (
        continuation.status,
        continuation.origin,
        predecessor["task_id"],
        predecessor["origin_run_id"],
        predecessor["promotion_attempt_id"],
        predecessor["result_sha"],
        predecessor["promotion_class"],
        evidence["tock_sha"],
        successor_task_id != journal.task_id,
        live_head != journal.tock_sha,
        live_head,
        main_state["state"],
        main_state["validated_result_sha"],
        main_state["last_tock_sha"],
        main_state["observed_main_head_sha"],
        len(matching_consumptions),
        active_task_id,
        payload["original_tick_sha"],
        payload["expected_continuation_head_sha"],
        chain,
        start_subject,
        start_parents,
        start_tree,
        predecessor_tree,
        first_parent,
        successor_identity_is_canonical,
    )
    required = (
        "AUTHORIZED",
        ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
        True,
        True,
        continuation.expected_continuation_head_sha,
        "GREEN_VALIDATED",
        journal.result_sha,
        journal.tock_sha,
        continuation.expected_continuation_head_sha,
        1,
        successor_task_id,
        live_head,
        live_head,
        [{"sha": live_head, "parent_sha": journal.tock_sha}],
        expected_subject,
        (journal.tock_sha,),
        predecessor_tree,
        predecessor_tree,
        (live_head, journal.tock_sha),
        True,
    )
    if observed != required:
        return None
    return {
        "predecessor_identity": dict(predecessor),
        "tock_sha": str(evidence["tock_sha"]),
        "start_sha": live_head,
    }


def _is_prefixed_sha256(value: object, prefix: str) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    digest = value[len(prefix):]
    return len(digest) == 64 and all(
        character in "0123456789abcdef" for character in digest
    )


def _settled_predecessor_from_basis(
    basis: object,
) -> dict[str, Any] | None:
    if not isinstance(basis, dict) or basis.get("kind") != "SETTLED_PROMOTION":
        return None
    predecessor = basis.get("predecessor")
    return predecessor if isinstance(predecessor, dict) else None


def _known_promotion_attempt(config: RunnerConfig) -> str:
    path = config.runtime_root / "promotion_journal.yaml"
    if path.is_file():
        try:
            return load_promotion_journal(path=path).promotion_attempt_id
        except PromotionJournalError:
            pass
    return config.main_ref


def _terminal_without_successor_activation(
    config: RunnerConfig,
    predecessor_basis: dict[str, Any] | None = None,
) -> bool:
    settled = _settled_predecessor_from_basis(
        predecessor_basis
        if predecessor_basis is not None
        else _canonical_settled_predecessor_basis(config)
    )
    if settled is None:
        return False
    try:
        active = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        ).active_task
    except TaskContractError:
        return False
    return (
        active is None
        or settled.get("settled") is not True
        or active.task_id == settled.get("task_id")
    )


def _authorized_continuation_allows_resume(
    config: RunnerConfig,
    reconciled: dict[str, Any],
    predecessor_basis: dict[str, Any] | None = None,
) -> bool:
    deferred_identity = _deferred_successor_identity(
        config,
        predecessor_basis,
    )
    return (
        deferred_identity is not None
        and reconciled.get("identity") == deferred_identity
    )


def _predecessor_drain_allows_successor_activation(
    config: RunnerConfig,
    drain: object,
    predecessor_basis: dict[str, Any] | None = None,
) -> bool:
    runtime = config.runtime_root
    basis_is_bound = (
        predecessor_basis is not None
        and predecessor_basis is _RUN_LOCK.get("predecessor_basis")
    )
    settled = _settled_predecessor_from_basis(
        predecessor_basis
        if predecessor_basis is not None
        else _canonical_settled_predecessor_basis(config)
    )
    outcome = getattr(drain, "outcome", None)
    if settled is None or outcome is None:
        return False
    try:
        journal = load_promotion_journal(
            path=runtime / "promotion_journal.yaml"
        )
        contract = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
    except (PromotionJournalError, TaskContractError):
        return False
    active = contract.active_task
    pending_names = (
        "pending_promotion.yaml",
        "pending_promotion.yml",
        "pending_promotion.json",
    )
    if any((runtime / name).is_file() for name in pending_names):
        return False
    live_head = _git_text(config.target_worktree, "rev-parse", config.main_ref)
    observed = (
        getattr(getattr(drain, "phase", None), "value", None),
        getattr(getattr(outcome, "status", None), "value", None),
        getattr(outcome, "reason_code", None),
        getattr(outcome, "observed_main_head_sha", None),
        getattr(outcome, "tock_sha", None),
        settled.get("settled"),
        settled.get("task_id"),
        settled.get("origin_run_id"),
        settled.get("promotion_attempt_id"),
        journal.phase,
        getattr(outcome, "task_id", None),
        getattr(outcome, "origin_run_id", None),
        getattr(outcome, "drain_run_id", None),
        getattr(outcome, "promotion_attempt_id", None),
        getattr(outcome, "result_sha", None),
        getattr(outcome, "promotion_class", None),
        getattr(outcome, "tock_sha", None),
        active is None or active.task_id != settled.get("task_id"),
    )
    required = (
        SchedulerPhase.DRAINED_AWAITING_TICK.value,
        drain_module.DrainStatus.DRAINED.value,
        "PROMOTION_EFFECT_ADMITTED",
        live_head,
        live_head,
        True,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        PromotionJournalPhase.FINALIZED,
        journal.task_id,
        journal.origin_run_id,
        journal.drain_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
        True,
    )
    recovered_pre_tock_required = (
        SchedulerPhase.BLOCKED.value,
        drain_module.DrainStatus.BLOCKED.value,
        "UNRESOLVED_PROMOTION_JOURNAL",
        live_head,
        None,
        True,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        PromotionJournalPhase.FINALIZED,
        None,
        None,
        _CLI_RUN_ID,
        None,
        None,
        None,
        None,
        True,
    )
    return observed == required or (
        basis_is_bound and observed == recovered_pre_tock_required
    )


def _pre_dispatch_terminal_activation(
    config: RunnerConfig,
    basis: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if _settled_predecessor_from_basis(basis) is None:
        return None
    contract = _load_activation_contract(config)
    sources = (
        ()
        if contract.active_task is None
        else _live_instruction_bindings(config, contract)
    )
    facts = TaskSourceFacts(
        contract=contract,
        sources=sources,
        run_root=config.runtime_root,
    )
    activation = task_activation_module.evaluate_task_activation(
        facts,
        predecessor={} if basis is None else basis,
    )
    if activation.status is ActivationStatus.TERMINAL_REST:
        return _rest_payload(activation)
    if activation.status is ActivationStatus.BLOCKED:
        return _blocked("run", activation.reason_code)
    return None


def _deferred_successor_provenance_gate(
    config: RunnerConfig,
) -> dict[str, Any]:
    continuation_path = config.runtime_root / "post_tick_continuation.yaml"
    journal_path = config.runtime_root / "promotion_journal.yaml"
    if not continuation_path.is_file():
        return {"ok": True, "reason_code": "CONTINUATION_NOT_REQUIRED"}
    try:
        continuation = load_continuation_artifact(continuation_path)
    except ContinuationError as exc:
        return _blocked_payload(exc.reason_code)
    if not journal_path.is_file():
        if continuation.status == "CONSUMED":
            return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
        return {"ok": True, "reason_code": "CONTINUATION_NOT_REQUIRED"}
    try:
        settled = _settled_predecessor_from_basis(
            _canonical_settled_predecessor_basis(
                config,
                predecessor_only_transition=True,
            )
        )
    except _SeamBlocked as exc:
        return _blocked_payload(exc.reason_code)
    if settled is None:
        return _blocked_payload("ACTIVATION_PREDECESSOR_UNAVAILABLE")
    try:
        journal = load_promotion_journal(path=journal_path)
    except PromotionJournalError as exc:
        return _blocked_payload(exc.reason_code)
    if continuation.status == "CONSUMED":
        return _consumed_continuation_reproof(
            config,
            settled=settled,
            continuation=continuation,
            journal=journal,
        )
    predecessor = continuation.identity
    evidence = continuation.to_mapping()["origin"]["evidence"]
    predecessor_observed = (
        continuation.status,
        journal.phase,
        settled.get("settled"),
        settled.get("task_id"),
        settled.get("origin_run_id"),
        settled.get("promotion_attempt_id"),
        predecessor.task_id,
        predecessor.origin_run_id,
        predecessor.promotion_attempt_id,
        predecessor.result_sha,
        predecessor.promotion_class,
        evidence["tock_sha"],
    )
    predecessor_required = (
        "AUTHORIZED",
        PromotionJournalPhase.FINALIZED,
        True,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
    )
    if predecessor_observed != predecessor_required:
        return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
    try:
        contract = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
    except TaskContractError:
        return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
    workers = [
        worker for worker in _persisted_inflight_workers(config) if worker is not None
    ]
    workers = _current_authority_inflight_workers(
        config,
        contract,
        workers,
        reconciled=_blocked_payload("RECONCILIATION_UNAVAILABLE"),
        settled_predecessor=settled,
    )
    if not workers:
        return {
            True: _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH"),
            False: {"ok": True, "reason_code": "CONTINUATION_DEFERRED"},
        }[(config.runtime_root / "pending_promotion.yaml").is_file()]
    worker = workers[0]
    try:
        loaded_worker = load_worker_result(
            config.runtime_root
            / "runs"
            / str(worker["origin_run_id"])
            / "worker_result.yaml",
            expected_identity=WorkerResultIdentity(
                task_id=str(worker["task_id"]),
                origin_run_id=str(worker["origin_run_id"]),
                promotion_attempt_id=str(worker["promotion_attempt_id"]),
                result_sha=str(worker["result_sha"]),
            ),
            expected_validation_digests={
                str(item["command_id"]): str(item["output_sha256"])
                for item in worker["validation_results"]
            },
        )
        sources = _instruction_bindings(
            config,
            contract,
            loaded_worker.origin_run_id,
        )
        facts = TaskSourceFacts(
            contract=contract,
            sources=sources,
            run_root=(
                config.runtime_root / "runs" / loaded_worker.origin_run_id
            ),
        )
        basis = task_activation_module.select_cli_activation_basis(
            main_state=None,
            settled_predecessor=settled,
        )
        activation = task_activation_module.evaluate_task_activation(
            facts,
            predecessor=basis,
        )
        identity = activation.identity
        observed_worker = (
            len(workers),
            loaded_worker.task_id,
            loaded_worker.origin_run_id,
            loaded_worker.promotion_attempt_id,
            loaded_worker.result_sha,
            continuation.successor_task_id,
        )
        required_worker = (
            1,
            identity.task_id,
            identity.origin_run_id,
            identity.promotion_attempt_id,
            str(worker["result_sha"]),
            identity.task_id,
        )
        if observed_worker != required_worker:
            return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
        _require_deferred_successor_result(config, activation, worker)
    except (
        AttributeError,
        ContinuationError,
        KeyError,
        PromotionJournalError,
        StopIteration,
        TaskContractError,
        WorkerResultError,
        _SeamBlocked,
        task_activation_module.TaskActivationError,
    ):
        return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
    return {"ok": True, "reason_code": "CONTINUATION_DEFERRED_REPROVED"}


def _consumed_continuation_reproof(
    config: RunnerConfig,
    *,
    settled: dict[str, Any],
    continuation: Any,
    journal: Any,
) -> dict[str, Any]:
    payload = continuation.to_mapping()
    try:
        main_state = state_module.load_and_validate_main_state(
            main_state_path=config.runtime_root / "main_state.yaml"
        )
        consumed = state_module.load_promotion_consumed(
            promotion_consumed_path=(
                config.runtime_root / "promotion_consumed.yaml"
            )
        )
        contract = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
        predecessor_journal = load_archived_promotion_journal(
            path=promotion_journal_archive_path_for_identity(
                runtime_root=config.runtime_root,
                origin_run_id=continuation.identity.origin_run_id,
                promotion_attempt_id=continuation.identity.promotion_attempt_id,
            )
        )
        predecessor_tock_sha = predecessor_journal.tock_sha
        start_subject = _git_text(
            config.target_worktree,
            "log",
            "-1",
            "--format=%s",
            journal.base_main_sha,
        )
        start_parents = tuple(
            _git_text(
                config.target_worktree,
                "rev-list",
                "--parents",
                "-n",
                "1",
                journal.base_main_sha,
            ).split()[1:]
        )
        start_tree = _git_text(
            config.target_worktree,
            "rev-parse",
            f"{journal.base_main_sha}^{{tree}}",
        )
        predecessor_tree = _git_text(
            config.target_worktree,
            "rev-parse",
            f"{predecessor_tock_sha}^{{tree}}",
        )
    except (
        PromotionJournalError,
        RunnerPromotionStateError,
        subprocess.CalledProcessError,
        TaskContractError,
    ):
        return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
    matching_consumptions = [
        entry
        for entry in consumed["consumed_promotions"]
        if entry["promotion_attempt_id"] == journal.promotion_attempt_id
        and entry["result_sha"] == journal.result_sha
        and entry["tock_sha"] == journal.tock_sha
    ]
    observed = (
        continuation.status,
        settled.get("settled"),
        settled.get("task_id"),
        settled.get("origin_run_id"),
        settled.get("promotion_attempt_id"),
        journal.phase,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        (
            journal.base_main_sha,
            continuation.expected_continuation_head_sha,
            journal.base_main_sha != predecessor_tock_sha,
            start_subject,
            start_parents,
            start_tree,
            predecessor_tree,
        ),
        journal.tock_sha,
        continuation.identity.task_id,
        continuation.identity.origin_run_id,
        continuation.identity.promotion_attempt_id,
        continuation.identity.result_sha,
        continuation.identity.promotion_class,
        payload["origin"]["evidence"]["tock_sha"],
        predecessor_journal.phase,
        predecessor_journal.task_id,
        predecessor_journal.origin_run_id,
        predecessor_journal.promotion_attempt_id,
        predecessor_journal.result_sha,
        predecessor_journal.promotion_class,
        predecessor_journal.tock_sha,
        predecessor_journal.task_id != journal.task_id,
        continuation.successor_task_id,
        payload.get("successor_promotion_attempt_id"),
        payload.get("successor_tock_sha"),
        main_state["validated_result_sha"],
        main_state["last_tock_sha"],
        main_state["observed_main_head_sha"],
        main_state["reason_code"],
        len(matching_consumptions),
        contract.active_task,
    )
    required = (
        "CONSUMED",
        True,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        PromotionJournalPhase.FINALIZED,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        (
            journal.base_main_sha,
            journal.base_main_sha,
            True,
            (
                f"START {journal.task_id} {journal.origin_run_id} "
                f"{journal.promotion_attempt_id} {predecessor_tock_sha}"
            ),
            (predecessor_tock_sha,),
            predecessor_tree,
            predecessor_tree,
        ),
        journal.tock_sha,
        predecessor_journal.task_id,
        predecessor_journal.origin_run_id,
        predecessor_journal.promotion_attempt_id,
        predecessor_journal.result_sha,
        predecessor_journal.promotion_class,
        predecessor_journal.tock_sha,
        PromotionJournalPhase.FINALIZED,
        predecessor_journal.task_id,
        predecessor_journal.origin_run_id,
        predecessor_journal.promotion_attempt_id,
        predecessor_journal.result_sha,
        predecessor_journal.promotion_class,
        predecessor_journal.tock_sha,
        True,
        journal.task_id,
        journal.promotion_attempt_id,
        journal.tock_sha,
        journal.result_sha,
        journal.tock_sha,
        journal.tock_sha,
        "PROMOTION_EFFECT_ADMITTED",
        1,
        None,
    )
    if observed != required:
        return _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH")
    return {"ok": True, "reason_code": "CONTINUATION_CONSUMED"}


def _reprove_deferred_successor_window(
    config: RunnerConfig,
    reconciled: dict[str, Any],
    inflight: list[dict[str, Any]],
) -> dict[str, Any]:
    reason = reconciled.get("reason_code")
    if reason not in {
        "RECONCILIATION_MAIN_STATE_DRIFT",
        "RECONCILIATION_IDENTITY_MISMATCH",
    }:
        return reconciled
    _require_ok(
        {
            "ok": len(inflight) == 1,
            "reason_code": "RECONCILIATION_IDENTITY_MISMATCH",
        }
    )
    deferred_identity = _deferred_successor_identity(config)
    _require_ok(
        {
            "ok": deferred_identity is not None,
            "reason_code": "RECONCILIATION_IDENTITY_MISMATCH",
        }
    )
    activation = _restore_inflight_activation(config, inflight[0])
    if reason == "RECONCILIATION_IDENTITY_MISMATCH":
        pending = _loaded_canonical_pending(config)
        pending_identity = (
            pending.get("task_id") if pending is not None else None,
            pending.get("origin_run_id") if pending is not None else None,
            pending.get("promotion_attempt_id") if pending is not None else None,
            pending.get("result_sha") if pending is not None else None,
        )
        required_pending_identity = (
            activation.identity.task_id,
            activation.identity.origin_run_id,
            activation.identity.promotion_attempt_id,
            inflight[0].get("result_sha"),
        )
        _require_ok(
            {
                "ok": pending_identity == required_pending_identity,
                "reason_code": "RECONCILIATION_IDENTITY_MISMATCH",
            }
        )
    _require_deferred_successor_result(
        config,
        activation,
        inflight[0],
    )
    if reason == "RECONCILIATION_IDENTITY_MISMATCH":
        predecessor_journal = load_promotion_journal(
            path=config.runtime_root / "promotion_journal.yaml"
        )
        archive_promotion_journal_epoch(
            runtime_root=config.runtime_root,
            journal=predecessor_journal,
        )
        release_canonical_promotion_journal(
            runtime_root=config.runtime_root,
            journal=predecessor_journal,
        )
        return reconcile_ordinary_promotion(config)
    return {
        "ok": True,
        "reason_code": "RECONCILIATION_COMPLETED",
        "main_state": _validated_main_state(config.runtime_root),
        "identity": deferred_identity,
    }


def _deferred_successor_identity(
    config: RunnerConfig,
    predecessor_basis: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    try:
        document = _settled_predecessor_from_basis(
            predecessor_basis
            if predecessor_basis is not None
            else _canonical_settled_predecessor_basis(config)
        )
        continuation = load_continuation_artifact(
            config.runtime_root / "post_tick_continuation.yaml"
        )
        journal = load_promotion_journal(
            path=config.runtime_root / "promotion_journal.yaml"
        )
        contract = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
    except (
        ContinuationError,
        PromotionJournalError,
        TaskContractError,
        _SeamBlocked,
    ):
        return None
    active_task = contract.active_task
    evidence = continuation.to_mapping()["origin"]["evidence"]
    predecessor = evidence["predecessor_identity"]
    observed = (
        document.get("settled") if document is not None else None,
        continuation.status,
        continuation.successor_task_id,
        predecessor["task_id"],
        predecessor["origin_run_id"],
        predecessor["promotion_attempt_id"],
        predecessor["result_sha"],
        predecessor["promotion_class"],
        evidence["tock_sha"],
        document.get("task_id") if document is not None else None,
        document.get("origin_run_id") if document is not None else None,
        document.get("promotion_attempt_id") if document is not None else None,
        journal.phase,
    )
    required = (
        True,
        "AUTHORIZED",
        active_task.task_id if active_task is not None else None,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        journal.result_sha,
        journal.promotion_class,
        journal.tock_sha,
        journal.task_id,
        journal.origin_run_id,
        journal.promotion_attempt_id,
        PromotionJournalPhase.FINALIZED,
    )
    identity = {
        "task_id": journal.task_id,
        "origin_run_id": journal.origin_run_id,
        "promotion_attempt_id": journal.promotion_attempt_id,
        "result_sha": journal.result_sha,
        "tock_sha": journal.tock_sha,
    }
    return {
        True: identity,
        False: None,
    }[observed == required]


def _require_deferred_successor_result(
    config: RunnerConfig,
    activation: object,
    worker: dict[str, Any],
) -> None:
    continuation = load_continuation_artifact(
        config.runtime_root / "post_tick_continuation.yaml"
    )
    state = state_module.load_and_validate_main_state(
        main_state_path=config.runtime_root / "main_state.yaml"
    )
    start_sha = continuation.expected_continuation_head_sha
    result_sha = str(worker["result_sha"])
    start_parents = tuple(
        _git_text(
            config.target_worktree,
            "rev-list",
            "--parents",
            "-n",
            "1",
            start_sha,
        ).split()[1:]
    )
    identity = activation.identity
    predecessor_tock = continuation.to_mapping()["origin"]["evidence"]["tock_sha"]
    task_ref = (
        f"refs/heads/runner/task/{identity.task_id}/{identity.origin_run_id}"
    )
    predecessor_tree = _git_text(
        config.target_worktree,
        "rev-parse",
        f"{predecessor_tock}^{{tree}}",
    )
    observed = (
        _git_text(config.target_worktree, "rev-parse", config.main_ref),
        state["observed_main_head_sha"],
        _git_text(config.target_worktree, "rev-parse", task_ref),
        _git_text(config.target_worktree, "log", "-1", "--format=%s", start_sha),
        start_parents,
        _git_text(
            config.target_worktree,
            "merge-base",
            "--is-ancestor",
            start_sha,
            result_sha,
        ),
        _git_text(config.target_worktree, "rev-parse", f"{start_sha}^{{tree}}"),
    )
    required = (
        start_sha,
        start_sha,
        result_sha,
        (
            f"START {identity.task_id} {identity.origin_run_id} "
            f"{identity.promotion_attempt_id} {predecessor_tock}"
        ),
        (predecessor_tock,),
        "",
        predecessor_tree,
    )
    _require_ok(
        {
            "ok": observed == required,
            "reason_code": "RECONCILIATION_START_TICK_CONFLICT",
        }
    )


def _qualify_task_branch_resume_preflight(
    config: RunnerConfig,
    preflight: dict[str, Any],
) -> dict[str, Any]:
    if preflight.get("reason_code") != "GIT_WRONG_BRANCH":
        return preflight
    workers = [
        worker for worker in _persisted_inflight_workers(config) if worker is not None
    ]
    try:
        contract = _load_activation_contract(config)
    except TaskContractError:
        return preflight
    workers = _current_authority_inflight_workers(
        config,
        contract,
        workers,
        reconciled=_blocked_payload("RECONCILIATION_UNAVAILABLE"),
    )
    if len(workers) != 1:
        return preflight
    worker = workers[0]
    worker_path = (
        config.runtime_root
        / "runs"
        / str(worker.get("origin_run_id", ""))
        / "worker_result.yaml"
    )
    try:
        loaded = load_worker_result(
            worker_path,
            expected_identity=WorkerResultIdentity(
                task_id=str(worker["task_id"]),
                origin_run_id=str(worker["origin_run_id"]),
                promotion_attempt_id=str(worker["promotion_attempt_id"]),
                result_sha=str(worker["result_sha"]),
            ),
            expected_validation_digests={
                str(item["command_id"]): str(item["output_sha256"])
                for item in worker["validation_results"]
            },
        )
        activation = _restore_inflight_activation(config, worker)
    except (WorkerResultError, KeyError, StopIteration, TaskContractError):
        return preflight
    branch = _git_text(config.target_worktree, "rev-parse", "--abbrev-ref", "HEAD")
    head_sha = _git_text(config.target_worktree, "rev-parse", "HEAD")
    status = _git_text(config.target_worktree, "status", "--porcelain")
    observed = (
        status,
        branch,
        head_sha,
        loaded.task_id,
        loaded.origin_run_id,
        loaded.promotion_attempt_id,
        loaded.result_sha,
    )
    required = (
        "",
        loaded.task_branch,
        loaded.result_sha,
        activation.identity.task_id,
        activation.identity.origin_run_id,
        activation.identity.promotion_attempt_id,
        head_sha,
    )
    qualified = {
        "ok": True,
        "reason_code": "GIT_PREFLIGHT_OK",
        "branch": branch,
        "head_sha": head_sha,
    }
    return {
        True: qualified,
        False: preflight,
    }[observed == required]


def _call_component(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    return func(*args, **kwargs)


def _call_basis_consumer(
    func: Any,
    /,
    *args: Any,
    predecessor_basis: dict[str, Any] | None,
) -> Any:
    try:
        inspect.signature(func).bind(
            *args,
            predecessor_basis=predecessor_basis,
        )
    except TypeError:
        return func(*args)
    return func(*args, predecessor_basis=predecessor_basis)


def _decisions_ready(config: RunnerConfig) -> bool:
    runtime = config.runtime_root
    orchestrator = runtime / "decisions" / "orchestrator_decision.yaml"
    validator = runtime / "decisions" / "validator_decision.yaml"
    return orchestrator.is_file() and validator.is_file()


def _projected_run_outcome(config: RunnerConfig) -> dict[str, Any] | None:
    runtime = config.runtime_root
    settled = _settled_projection(runtime)
    if settled is not None:
        return None
    if _public_worker_ready(runtime):
        return _public_outcome("WORKER_RESULT_READY", "WORKER_RESULT_PRESENT")
    if _active_task_present(config):
        return None
    drained = _drained_projection(runtime)
    if drained is not None:
        return drained
    document = _read_yaml(runtime / "main_state.yaml")
    if document is not None and document.get("reason_code") == "RECONCILIATION_COMPLETED":
        state = _validated_main_state(runtime)
        if state is not None and state.get("reason_code") == "RECONCILIATION_COMPLETED":
            return _public_outcome("RECOVERED", "RECONCILIATION_COMPLETED")
    return None


def _entry_promotion_journal_phase(
    config: RunnerConfig,
) -> PromotionJournalPhase | None:
    path = config.runtime_root / "promotion_journal.yaml"
    if not path.is_file():
        return None
    try:
        return load_promotion_journal(path=path).phase
    except PromotionJournalError:
        return None


def _drained_projection(runtime: Path) -> dict[str, Any] | None:
    if not (runtime / "drain_outcome.yaml").is_file():
        return None
    outcome = drain_module.load_persisted_drain_outcome(runtime_root=runtime)
    if outcome is None or outcome.status is not drain_module.DrainStatus.DRAINED:
        return None
    return _public_outcome("DRAINED_AWAITING_TASK", "PROMOTION_EFFECT_ADMITTED")


def _settled_projection(runtime: Path) -> dict[str, Any] | None:
    path = runtime / "post_tick_continuation.yaml"
    if not path.is_file():
        return None
    try:
        view = load_continuation_artifact(path)
    except ContinuationError:
        return None
    if view.status != "CONSUMED":
        return None
    return _public_outcome("SETTLED", "CONTINUATION_CONSUMED")


def _loaded_canonical_pending(config: RunnerConfig) -> dict[str, Any] | None:
    try:
        return load_pending_promotion(runtime_root=config.runtime_root)
    except RunnerGitPromotionError:
        return None


def _public_worker_ready(runtime: Path) -> bool:
    path = runtime / "worker_result.yaml"
    document = _read_yaml(path)
    if document is None:
        return False
    try:
        load_worker_result(
            path,
            expected_identity=WorkerResultIdentity(
                task_id=str(document.get("task_id", "")),
                origin_run_id=str(document.get("origin_run_id", "")),
                promotion_attempt_id=str(document.get("promotion_attempt_id", "")),
                result_sha=str(document.get("result_sha", "")),
            ),
            expected_validation_digests={},
        )
    except WorkerResultError:
        return False
    return True


def _active_task_present(config: RunnerConfig) -> bool:
    try:
        contract = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
    except TaskContractError:
        return False
    return contract.active_task is not None


def _return_authorizes_drain(result: object) -> bool:
    return (
        isinstance(result, dict)
        and result.get("ok") is True
        and result.get("reason_code") == "RETURNED_TO_MAIN"
        and isinstance(result.get("branch"), str)
        and result.get("branch") != ""
    )


def _drain_decision_authorizes_effect(
    decision: object,
    pending: Mapping[str, Any],
) -> bool:
    outcome = getattr(decision, "outcome", None)
    phase = getattr(decision, "phase", None)
    status = getattr(outcome, "status", None)
    identity_fields = (
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "result_sha",
        "promotion_class",
    )
    observed_identity = tuple(
        getattr(outcome, field, None) for field in identity_fields
    )
    required_identity = tuple(pending.get(field) for field in identity_fields)
    return (
        getattr(phase, "value", None) == "DRAINED_AWAITING_TICK"
        and getattr(status, "value", None) == "DRAINED"
        and getattr(outcome, "reason_code", None) == "PROMOTION_EFFECT_ADMITTED"
        and observed_identity == required_identity
    )


def _validated_main_state(runtime: Path) -> dict[str, Any] | None:
    path = runtime / "main_state.yaml"
    try:
        return state_module.load_and_validate_main_state(main_state_path=path)
    except RunnerPromotionStateError:
        return None


def _public_outcome(outcome: str, reason_code: str) -> dict[str, Any]:
    return {
        "ok": True,
        "command": "run",
        "outcome": outcome,
        "status": "projected",
        "reason_code": reason_code,
    }


def _rest_payload(activation: object) -> dict[str, Any]:
    return {
        "ok": True,
        "command": "run",
        "outcome": "TERMINAL_REST",
        "status": "terminal-rest",
        "reason_code": getattr(activation, "reason_code", "TERMINAL_REST"),
    }


def _private_fresh_baseline(config: RunnerConfig) -> bool:
    return task_activation_module._fresh_main_state_bound(
        _read_yaml(config.runtime_root / "main_state.yaml")
    )


def _drain_gate(drain: object) -> dict[str, Any]:
    outcome = getattr(drain, "outcome", None)
    return {
        "ok": getattr(drain, "phase", None) is SchedulerPhase.DISPATCH_READY,
        "reason_code": getattr(outcome, "reason_code", "DRAIN_BLOCKED"),
    }


def _persisted_drain_matches_pending(
    config: RunnerConfig,
    pending: dict[str, Any],
) -> dict[str, Any]:
    prior = drain_module.load_persisted_drain_outcome(
        runtime_root=config.runtime_root,
    )
    if prior is None or prior.status is not drain_module.DrainStatus.DRAINED:
        return {"ok": True, "reason_code": "DRAIN_OUTCOME_NOT_REUSED"}
    observed_identity = (
        prior.task_id,
        prior.origin_run_id,
        prior.promotion_attempt_id,
        prior.result_sha,
        prior.promotion_class,
    )
    required_identity = tuple(
        str(pending[field])
        for field in (
            "task_id",
            "origin_run_id",
            "promotion_attempt_id",
            "result_sha",
            "promotion_class",
        )
    )
    if observed_identity != required_identity:
        return {"ok": True, "reason_code": "DRAIN_OUTCOME_NOT_REUSED"}
    result_tree = _git_text(
        config.target_worktree,
        "rev-parse",
        f"{pending['result_sha']}^{{tree}}",
    )
    observation = reconciliation_module.observe_canonical_first_parent_tock(
        repo=config.target_worktree,
        main_ref=config.main_ref,
        promotion_attempt_id=str(pending["promotion_attempt_id"]),
        validated_tree_oid=result_tree,
        diagnostic_refs=None,
    )
    reproof_mismatch = (
        observation.effect_authorized is not True
        or observation.tock_sha != prior.tock_sha
    )
    return {
        True: _blocked_payload("RECONCILIATION_IDENTITY_MISMATCH"),
        False: {"ok": True, "reason_code": "DRAIN_OUTCOME_REPROVED"},
    }[reproof_mismatch]


def _read_yaml(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(loaded, dict):
        return loaded
    return None


def _require_ok(result: dict[str, Any]) -> dict[str, Any]:
    reason = _blocked_reason(result)
    if reason is not None:
        raise _SeamBlocked(reason)
    return result


def _blocked_reason(result: dict[str, Any]) -> str | None:
    if result.get("ok") is True:
        return None
    return str(result.get("reason_code", "SEAM_BLOCKED"))


def _blocked_payload(reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "fail-closed",
        "reason_code": reason_code,
    }


def _blocked(command: str, reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "command": command,
        "outcome": "BLOCKED",
        "status": "fail-closed",
        "reason_code": reason_code,
    }


def _emit(payload: dict[str, Any], *, as_json: bool = False) -> int:
    if as_json:
        sys.stdout.write(json.dumps(payload, sort_keys=False) + "\n")
    else:
        yaml.safe_dump(payload, sys.stdout, sort_keys=False)
    return 0 if payload.get("ok") is True else 1


def _option_value(tokens: list[str], option: str) -> str | None:
    if option not in tokens:
        return None
    position = tokens.index(option) + 1
    remainder = tokens[position:] + [None]
    return remainder[0]


def _status_command_payload(view: StatusView) -> dict[str, Any]:
    projected = view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    payload: dict[str, Any] = {}
    for slot_name in (
        "runner_source_identity",
        "target_main_head",
        "main_state",
        "active_task_identity",
        "run_phase",
        "pending",
        "journal",
        "consumption",
        "drain",
        "decisions",
        "next_external_action",
        "drift_warning",
    ):
        fact = getattr(view, slot_name)
        payload[slot_name] = {
            "presence": fact.presence.value,
            "payload": fact.payload,
            "reason_code": fact.reason_code,
        }
    payload["ok"] = projected
    payload["command"] = "status"
    payload["outcome"] = view.projection_reason_code if projected else "BLOCKED"
    payload["reason_code"] = view.projection_reason_code
    return payload


def _git_text(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _bound_prompt_file(path: Path) -> BoundPromptBytes:
    payload = path.read_bytes() if path.is_file() else b""
    return BoundPromptBytes(
        payload_bytes=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_length=len(payload),
    )


def _load_activation_contract(config: RunnerConfig) -> TaskContract:
    try:
        return load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
    except TaskContractError as exc:
        source_bytes = config.task_source.read_bytes()
        try:
            loaded = yaml.safe_load(source_bytes)
        except yaml.YAMLError:
            raise exc from None
        if loaded != {"active_task": None}:
            raise
        return TaskContract(
            schema_version="1.0",
            active_task=None,
            source_root=config.task_source.resolve().parent,
            fingerprint=hashlib.sha256(source_bytes).hexdigest(),
            source_bytes=source_bytes,
        )


def _run_allowed_paths(config: RunnerConfig) -> tuple[str, ...]:
    try:
        contract = load_task_contract(
            config.task_source,
            target_worktree=config.target_worktree,
        )
    except TaskContractError:
        return ()
    return contract.active_task.allowed_paths


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_local_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        raise
    return True


_DEFAULT_SEAM_OK = (
    verify_code_authority(None)["ok"],
    reconcile_ordinary_promotion(None)["ok"],
    bind_ordinary_continuation(None)["ok"],
    start_cursor_adapter(None)["ok"],
    return_to_main(None)["ok"],
)
