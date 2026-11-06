"""Fixture-free successor traversal after real post-TOCK recovery."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

_TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(_TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TESTS_ROOT))

import test_cli_run as cli_tests


def test_fresh_successor_after_real_post_tock_recovery_uses_canonical_basis_and_consumes_continuation(
    tmp_path: Path,
) -> None:
    import task_transition_runner.cli as cli_module
    from task_transition_runner.config import load_config
    from task_transition_runner.runner_continuation_origin_proofs import (
        prove_finalized_promotion_tick,
    )
    from task_transition_runner.runner_promotion_reconciliation import (
        reconcile_ordinary_promotion,
    )
    from task_transition_runner.task_source import reprove_run_local_snapshot

    config_path, target, runtime = cli_tests._prepare_fresh_surface(
        tmp_path,
        active_task=True,
    )
    marker = cli_tests._arm_adapter_marker(tmp_path)
    source_root = cli_tests._clone_clean_runner_source(tmp_path)
    pidfile = tmp_path / "cli.pid"
    ledger: list[dict[str, Any]] = []

    cli_tests._reach_real_pending(
        config_path,
        runtime,
        pidfile,
        marker,
        ledger,
        ("PREDECESSOR_AWAITING_DECISIONS", "PREDECESSOR_PENDING_PROMOTION"),
        source_root=source_root,
    )
    hook = cli_tests._install_post_tock_hook(target, "refs/heads/main")
    try:
        killed, killed_pid = cli_tests._account_real_cli(
            config_path,
            pidfile,
            marker,
            ledger,
            role=cli_tests._ROLE_ACCEPTANCE,
            step="PREDECESSOR_KILL_AFTER_COMMITTED_TOCK",
            source_root=source_root,
        )
    finally:
        hook.unlink(missing_ok=True)
        pidfile.unlink(missing_ok=True)

    assert killed.returncode == -9, (killed.returncode, killed.stdout, killed.stderr)
    with pytest.raises(ProcessLookupError):
        os.kill(killed_pid, 0)

    recovered, recovered_pid = cli_tests._account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=cli_tests._ROLE_ACCEPTANCE,
        step="PREDECESSOR_REAL_RECOVERY",
        source_root=source_root,
    )
    recovered_stdout = cli_tests._stdout_payload(recovered)
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

    predecessor_journal = _load_yaml(runtime / "promotion_journal.yaml")
    predecessor_tock = predecessor_journal["tock_sha"]
    assert predecessor_journal["phase"] == "FINALIZED"
    assert cli_tests._run_git(target, "rev-parse", "refs/heads/main") == (
        predecessor_tock
    )
    assert cli_tests._first_parent_subjects(target).count("TOCK") == 1

    predecessor_consumption = _matching_consumptions(
        runtime,
        predecessor_journal,
    )
    assert len(predecessor_consumption) == 1
    predecessor_state = _load_yaml(runtime / "main_state.yaml")
    assert (
        predecessor_state["state"],
        predecessor_state["validated_result_sha"],
        predecessor_state["last_tock_sha"],
        predecessor_state["observed_main_head_sha"],
        predecessor_state["reason_code"],
    ) == (
        "GREEN_VALIDATED",
        predecessor_journal["result_sha"],
        predecessor_tock,
        predecessor_tock,
        "RECONCILIATION_COMPLETED",
    )

    config = load_config(config_path)
    origin = prove_finalized_promotion_tick(
        repo=target,
        runtime=runtime,
        main_ref=config.main_ref,
    )
    assert origin.to_mapping() == {
        "predecessor_identity": {
            "task_id": predecessor_journal["task_id"],
            "origin_run_id": predecessor_journal["origin_run_id"],
            "promotion_attempt_id": predecessor_journal["promotion_attempt_id"],
            "result_sha": predecessor_journal["result_sha"],
            "promotion_class": predecessor_journal["promotion_class"],
        },
        "tock_sha": predecessor_tock,
    }
    reproved_snapshot = reprove_run_local_snapshot(
        run_root=runtime / "runs" / predecessor_journal["origin_run_id"],
        expected_task_fingerprint=predecessor_journal["task_fingerprint"],
        expected_task_snapshot_sha256=predecessor_journal["task_snapshot_sha256"],
        expected_instruction_signature_sha256=(
            predecessor_journal["instruction_signature_sha256"]
        ),
    )
    reconciliation_reproof = reconcile_ordinary_promotion(
        repo=target,
        runtime_root=runtime,
        drain_run_id=predecessor_journal["drain_run_id"],
        main_ref=config.main_ref,
        diagnostic_refs=None,
    )
    assert reconciliation_reproof.reason_code == "RECONCILIATION_COMPLETED"
    assert reconciliation_reproof.git_effect_created is False
    reconciled_identity = {
        "task_id": reconciliation_reproof.task_id,
        "origin_run_id": reconciliation_reproof.origin_run_id,
        "promotion_attempt_id": reconciliation_reproof.promotion_attempt_id,
        "result_sha": reconciliation_reproof.result_sha,
        "tock_sha": reconciliation_reproof.tock_sha,
    }
    canonical_basis = cli_module._canonical_settled_predecessor_basis(
        config,
        reconciled_identity=reconciled_identity,
    )
    assert canonical_basis == {
        "kind": "SETTLED_PROMOTION",
        "predecessor": {
            "settled": True,
            "task_id": predecessor_journal["task_id"],
            "origin_run_id": predecessor_journal["origin_run_id"],
            "promotion_attempt_id": predecessor_journal["promotion_attempt_id"],
            "task_fingerprint": reproved_snapshot.task_fingerprint,
            "instruction_sources": [
                {
                    "logical_ref": source.logical_ref,
                    "sha256": source.sha256,
                    "byte_length": source.byte_length,
                }
                for source in reproved_snapshot.instruction_sources
            ],
        },
    }
    assert not (runtime / cli_tests._SETTLED_PREDECESSOR_NAME).exists()

    task_path = config_path.parent / "tasks.yaml"
    predecessor_task = _load_yaml(task_path)
    successor_task = dict(predecessor_task)
    successor_task["active_task"] = dict(predecessor_task["active_task"])
    successor_task_id = "synthetic-task-002"
    successor_task["active_task"]["task_id"] = successor_task_id
    task_path.write_text(
        yaml.safe_dump(successor_task, sort_keys=False),
        encoding="utf-8",
    )

    awaiting, awaiting_pid = cli_tests._account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=cli_tests._ROLE_ACCEPTANCE,
        step="SUCCESSOR_ORDINARY_START",
        source_root=source_root,
    )
    awaiting_stdout = cli_tests._stdout_payload(awaiting)
    assert awaiting.returncode == 1, (
        awaiting.returncode,
        awaiting.stdout,
        awaiting.stderr,
    )
    assert awaiting_stdout["outcome"] == "AWAITING_DECISIONS"
    assert awaiting_stdout["reason_code"] == "PROMOTION_DECISION_MISSING"

    successor_workers = _workers_for_task(runtime, successor_task_id)
    assert len(successor_workers) == 1
    successor_worker = successor_workers[0]
    predecessor_identity = (
        predecessor_journal["task_id"],
        predecessor_journal["origin_run_id"],
        predecessor_journal["promotion_attempt_id"],
    )
    successor_identity = (
        successor_worker["task_id"],
        successor_worker["origin_run_id"],
        successor_worker["promotion_attempt_id"],
    )
    assert all(
        successor != predecessor
        for successor, predecessor in zip(successor_identity, predecessor_identity)
    )

    start_sha = cli_tests._run_git(target, "rev-parse", "refs/heads/main")
    assert cli_tests._run_git(target, "rev-parse", f"{start_sha}^") == (
        predecessor_tock
    )
    assert cli_tests._run_git(target, "rev-parse", f"{start_sha}^{{tree}}") == (
        cli_tests._run_git(target, "rev-parse", f"{predecessor_tock}^{{tree}}")
    )
    assert cli_tests._run_git(target, "log", "-1", "--format=%s", start_sha) == (
        f"START {successor_task_id} {successor_worker['origin_run_id']} "
        f"{successor_worker['promotion_attempt_id']} {predecessor_tock}"
    )

    continuation_path = runtime / "post_tick_continuation.yaml"
    authorized_bytes = continuation_path.read_bytes()
    authorized = yaml.safe_load(authorized_bytes)
    assert authorized["status"] == "AUTHORIZED"
    assert authorized["successor_task_id"] == successor_task_id
    assert authorized["expected_continuation_head_sha"] == start_sha
    assert authorized["original_tick_sha"] == start_sha
    assert authorized["commit_chain"] == [
        {"sha": start_sha, "parent_sha": predecessor_tock}
    ]
    assert authorized["origin"]["evidence"] == origin.to_mapping()
    assert not (runtime / "pending_promotion.yaml").exists()
    assert not (runtime / cli_tests._SETTLED_PREDECESSOR_NAME).exists()

    cli_tests._bind_external_decisions(runtime, successor_worker)
    pending, pending_pid = cli_tests._account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=cli_tests._ROLE_ACCEPTANCE,
        step="SUCCESSOR_ORDINARY_CONTINUATION",
        source_root=source_root,
    )
    pending_stdout = cli_tests._stdout_payload(pending)
    assert pending.returncode == 0, (
        pending.returncode,
        pending.stdout,
        pending.stderr,
    )
    assert pending_stdout["outcome"] == "PENDING_PROMOTION"
    assert pending_stdout["reason_code"] == "PROMOTION_INTENT_PERSISTED"
    assert continuation_path.read_bytes() == authorized_bytes

    settled, settled_pid = cli_tests._account_real_cli(
        config_path,
        pidfile,
        marker,
        ledger,
        role=cli_tests._ROLE_ACCEPTANCE,
        step="SUCCESSOR_TOCK_AND_CONSUMPTION",
        source_root=source_root,
    )
    settled_stdout = cli_tests._stdout_payload(settled)
    assert settled.returncode == 0, (
        settled.returncode,
        settled.stdout,
        settled.stderr,
    )
    assert settled_stdout["outcome"] == "SETTLED"
    assert settled_stdout["reason_code"] == "CONTINUATION_CONSUMED"

    successor_journal = _load_yaml(runtime / "promotion_journal.yaml")
    successor_tock = successor_journal["tock_sha"]
    assert successor_journal["phase"] == "FINALIZED"
    assert successor_journal["task_id"] == successor_task_id
    assert successor_journal["origin_run_id"] == successor_worker["origin_run_id"]
    assert (
        successor_journal["promotion_attempt_id"]
        == successor_worker["promotion_attempt_id"]
    )
    assert successor_journal["result_sha"] == successor_worker["result_sha"]
    assert successor_tock != predecessor_tock
    assert cli_tests._run_git(
        target,
        "rev-list",
        "--parents",
        "-1",
        successor_tock,
    ).split() == [
        successor_tock,
        start_sha,
        successor_worker["result_sha"],
    ]
    assert cli_tests._first_parent_subjects(target).count("TOCK") == 2
    assert len(_matching_consumptions(runtime, successor_journal)) == 1

    consumed = _load_yaml(continuation_path)
    assert consumed["status"] == "CONSUMED"
    assert consumed["origin"] == authorized["origin"]
    assert consumed["successor_task_id"] == successor_task_id
    assert (
        consumed["successor_promotion_attempt_id"]
        == successor_worker["promotion_attempt_id"]
    )
    assert consumed["successor_tock_sha"] == successor_tock
    assert len(_workers_for_task(runtime, successor_task_id)) == 1
    assert cli_tests._marker_count(marker) == 2
    assert len({row["pid"] for row in ledger}) == len(ledger) == 7
    assert len({killed_pid, recovered_pid, awaiting_pid, pending_pid, settled_pid}) == 5
    assert not (runtime / "runner_git.lock").exists()
    assert not (runtime / cli_tests._SETTLED_PREDECESSOR_NAME).exists()
    cli_tests._assert_frozen_read_only_product_bytes()


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _matching_consumptions(
    runtime: Path,
    journal: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = _load_yaml(runtime / "promotion_consumed.yaml")
    return [
        entry
        for entry in payload["consumed_promotions"]
        if entry["promotion_attempt_id"] == journal["promotion_attempt_id"]
        and entry["result_sha"] == journal["result_sha"]
        and entry["tock_sha"] == journal["tock_sha"]
    ]


def _workers_for_task(runtime: Path, task_id: str) -> list[dict[str, Any]]:
    workers = [
        _load_yaml(path)
        for path in sorted((runtime / "runs").glob("*/worker_result.yaml"))
    ]
    return [worker for worker in workers if worker["task_id"] == task_id]
