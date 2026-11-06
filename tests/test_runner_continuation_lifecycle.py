"""One-origin continuation lifecycle facade.

OSS6-B15 binds one externally supplied fresh successor to one gap-free,
one-parent, non-merge START chain from the proven predecessor TOCK
through the successor START head. OSS6-B16 admits only
``FINALIZED_PROMOTION_TICK`` and only the monotone observation
``PRE_ACK`` to ``CONSUMED``. Prepare, load and dispatch re-prove the
immutable predecessor, TOCK, original START, expected head, ordered
chain and successor task id under the current local lock. The last
gate before authorization, and again on every reload, is a fresh
``git rev-parse`` of the configured main ref; that SHA must still be
the proven predecessor TOCK. Dispatch repeats that lock and main-head
reproof. That reproof also rejects a main head that observed and live
reads agree on when it is not the bound predecessor TOCK, and a stored
binding digest that does not match. Prepare leaves an existing artifact
byte-stable when its binding digest does not match or its declared
chain breaks. A START whose task id matches still fails when only its
origin run or only its promotion attempt differs. A foreign supplied
origin and its foreign task ref do not override a canonical START
subject origin. A different fresh successor origin that is consistent
across input, ref and START subject remains admissible.
An exact task ref and expected head that both name the predecessor
TOCK are an empty START chain: Prepare answers
``CONTINUATION_CHAIN_MISMATCH``, writes no artifact, and does not
raise a raw index error. Load and dispatch re-prove tree drift after
the artifact digest is recomputed for the drifted commit. Origin-run
and promotion-attempt drift leave that prepared artifact byte-identical
and move only the current exact named ref; both answer
``CONTINUATION_SUCCESSOR_MISMATCH``. A historical attempt move that is
reset to the canonical current exact ref is not successor identity
authority: load and dispatch stay successful, leave the artifact bytes
unchanged, and issue no Git command containing ``@{``. A START subject
whose task, origin run and promotion attempt match still fails as
``CONTINUATION_SUCCESSOR_MISMATCH`` when its parent token differs from
the Git parent.
Load and dispatch remain successful after ``git pack-refs --all --prune``
for that unchanged exact successor ref. They also remain successful when
the exact canonical ref is packed and a foreign loose origin ref for the
same task is present; that side ref is not successor identity authority.

This file does not prove successor-TOCK consumption, archive reproof,
historical reopen, or distributed exactly-once behavior.
"""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from task_transition_runner.runner_continuation_contract import (
    ContinuationError,
    ContinuationOrigin,
    LifecyclePhase,
    binding_digest,
)
from task_transition_runner.runner_continuation_origin_proofs import (
    OriginProof,
    prove_finalized_promotion_tick,
)
from task_transition_runner.runner_drain_transition import (
    DrainOutcome,
    DrainStatus,
    persist_drain_outcome,
)
from task_transition_runner.runner_git_context import acquire_runner_git_lock
from task_transition_runner.runner_promotion_journal import (
    PromotionJournalPhase,
    create_promotion_journal,
    persist_promotion_journal,
    transition_promotion_journal,
)
from task_transition_runner.runner_promotion_state import (
    record_promotion_consumed,
    write_main_state,
)
from task_transition_runner.task_contract import (
    canonicalize_persisted_task_snapshot,
)
from task_transition_runner.task_source import reprove_run_local_snapshot
import task_transition_runner.runner_continuation_lifecycle as lifecycle


_MAIN_REF = "refs/heads/main"
_ARTIFACT_NAME = "post_tick_continuation.yaml"
_TASK_ID = "task-synthetic-lifecycle-001"
_SUCCESSOR_TASK_ID = "task-synthetic-lifecycle-successor-001"
_FOREIGN_TASK_ID = "task-synthetic-lifecycle-foreign-001"
_ORIGIN_RUN_ID = "origin-synthetic-lifecycle-001"
_SUCCESSOR_ORIGIN_RUN_ID = "origin-synthetic-lifecycle-successor-001"
_ATTEMPT_ID = "attempt-synthetic-lifecycle-001"
_SUCCESSOR_ATTEMPT_ID = "attempt-synthetic-lifecycle-successor-001"
_FOREIGN_ATTEMPT_ID = "attempt-synthetic-lifecycle-foreign-001"
_DRIFT_ORIGIN_RUN_ID = "origin-synthetic-lifecycle-drift-001"
_SIDE_ORIGIN_RUN_ID = "origin-synthetic-lifecycle-side-001"
_DRIFT_ATTEMPT_ID = "attempt-synthetic-lifecycle-drift-001"
_PROMOTION_CLASS = "GREEN_PROMOTION"
_DRAIN_RUN_ID = "drain-synthetic-lifecycle-001"
_LOCK_RUN_ID = "run-synthetic-lifecycle-001"
_PREPARED_AT = "2026-09-26T12:00:00Z"
_PREPARED_AT_LATER = "2026-09-26T12:05:00Z"
_LOCK_STARTED_AT = "2026-09-26T11:59:00Z"
_LOCK_NOW = "2026-09-26T12:00:00Z"
_STALE_AFTER_SECONDS = 3600
_CHAIN_MISMATCH = "CONTINUATION_CHAIN_MISMATCH"
_HEAD_MISMATCH = "CONTINUATION_HEAD_MISMATCH"
_SUCCESSOR_MISMATCH = "CONTINUATION_SUCCESSOR_MISMATCH"
_ORIGIN_INVALID = "CONTINUATION_ORIGIN_INVALID"
_LIFECYCLE_INVALID = "CONTINUATION_LIFECYCLE_INVALID"
_DIGEST_MISMATCH = "CONTINUATION_DIGEST_MISMATCH"
_AUTHORIZED_FIELDS = frozenset(
    {
        "schema_version",
        "origin",
        "original_tick_sha",
        "expected_continuation_head_sha",
        "commit_chain",
        "successor_task_id",
        "status",
        "prepared_at_utc",
        "continuation_digest_sha256",
    }
)
_PREPARE_PARAMETERS = (
    "repo",
    "runtime",
    "main_ref",
    "continuation_origin",
    "successor_task_id",
    "successor_origin_run_id",
    "successor_promotion_attempt_id",
    "expected_continuation_head_sha",
    "lock_ownership",
    "prepared_at_utc",
)
_LOAD_PARAMETERS = (
    "repo",
    "runtime",
    "main_ref",
    "successor_task_id",
    "observed_main_head_sha",
    "lock_ownership",
)
_DISPATCH_PARAMETERS = (
    "repo",
    "runtime",
    "main_ref",
    "task_id",
    "observed_main_head_sha",
    "lock_ownership",
)
_TRANSITION_PARAMETERS = ("origin", "previous", "current")
_FORBIDDEN_PARAMETER_TOKENS = (
    "reopen",
    "legacy",
    "archive",
    "replay",
    "chronicle",
    "all_refs",
    "consume",
    "queue",
    "planner",
)
_DROPPED_SOURCE_MARKERS = (
    "ReopenOriginLifecycleView",
    "REOPEN_BASELINE_START_TICK",
    "CONTEXT_BOUND_LEGACY",
    "consume_post_tick_continuation",
    "ContinuationConsumptionView",
    "load_archived_promotion_journal",
    "log --all",
    "--all",
)
_REQUIRED_EXPORTS = (
    "ContinuationCommitBinding",
    "FinalizedOriginLifecycleView",
    "ContinuationLifecycleView",
    "prepare_post_tick_continuation",
    "load_authorized_post_tick_continuation",
    "assert_post_tick_continuation_dispatch_task",
    "assert_lifecycle_transition",
)
_STAMPS = {
    "PENDING": "2026-09-26T00:00:00Z",
    "APPLYING": "2026-09-26T00:01:00Z",
    "GIT_APPLIED": "2026-09-26T00:02:00Z",
    "RECONCILED": "2026-09-26T00:03:00Z",
    "FINALIZED": "2026-09-26T00:04:00Z",
}
_PHASES = (
    PromotionJournalPhase.PENDING,
    PromotionJournalPhase.APPLYING,
    PromotionJournalPhase.GIT_APPLIED,
    PromotionJournalPhase.RECONCILED,
    PromotionJournalPhase.FINALIZED,
)
_GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Synthetic Lifecycle",
    "GIT_AUTHOR_EMAIL": "lifecycle@task-transition-runner.invalid",
    "GIT_AUTHOR_DATE": "2026-09-26T00:00:00Z",
    "GIT_COMMITTER_NAME": "Synthetic Lifecycle",
    "GIT_COMMITTER_EMAIL": "lifecycle@task-transition-runner.invalid",
    "GIT_COMMITTER_DATE": "2026-09-26T00:00:00Z",
}
_REAL_SUBPROCESS_RUN = subprocess.run
_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)


def _git(repo: Path, *args: str, input_text: str | None = None) -> str:
    completed = _REAL_SUBPROCESS_RUN(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        input=input_text,
        env=_GIT_ENV,
    )
    return completed.stdout.strip()


def _init_repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-b", "main")
    return path


def _blob(repo: Path, payload: str) -> str:
    return _git(repo, "hash-object", "-w", "--stdin", input_text=payload)


def _tree(repo: Path, blob: str, name: str) -> str:
    return _git(repo, "mktree", input_text=f"100644 blob {blob}\t{name}\n")


def _commit(
    repo: Path,
    *,
    tree: str,
    parents: tuple[str, ...],
    message: str,
) -> str:
    args = ["commit-tree", tree]
    for parent in parents:
        args.extend(["-p", parent])
    return _git(repo, *args, input_text=message)


def _update_ref(repo: Path, ref: str, sha: str) -> None:
    _git(repo, "update-ref", ref, sha)


def _tree_of(repo: Path, sha: str) -> str:
    return _git(repo, "rev-parse", f"{sha}^{{tree}}")


def _parents_of(repo: Path, sha: str) -> tuple[str, ...]:
    raw = _git(repo, "rev-parse", f"{sha}^@")
    if not raw:
        return ()
    return tuple(raw.splitlines())


def _canonical_message(
    *,
    expected_base_sha: str,
    result_sha: str,
) -> str:
    values = {
        "Promotion-Attempt": _ATTEMPT_ID,
        "Task-ID": _TASK_ID,
        "Origin-Run": _ORIGIN_RUN_ID,
        "Result-SHA": result_sha,
        "Promotion-Class": _PROMOTION_CLASS,
        "Expected-Base": expected_base_sha,
    }
    trailers = "\n".join(
        f"{key}: {values[key]}" for key in _CANONICAL_TOCK_TRAILER_KEYS
    )
    return f"TOCK\n\n{trailers}\n"


def _start_message(
    *,
    parent_sha: str,
    task_id: str,
    attempt_id: str,
    origin_run_id: str = _SUCCESSOR_ORIGIN_RUN_ID,
) -> str:
    return f"START {task_id} {origin_run_id} {attempt_id} {parent_sha}\n"


def _task_ref(
    task_id: str,
    origin_run_id: str = _SUCCESSOR_ORIGIN_RUN_ID,
) -> str:
    return f"refs/heads/runner/task/{task_id}/{origin_run_id}"


def _pre_state(base_sha: str) -> dict[str, str]:
    return {
        "schema_version": "1.0",
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": base_sha,
        "last_tock_sha": base_sha,
        "observed_main_head_sha": base_sha,
        "reason_code": "STATE_READY_SYNTHETIC",
        "updated_at_utc": _STAMPS["PENDING"],
    }


def _write_journal_and_its_snapshot_family(
    runtime: Path,
    *,
    base_sha: str,
    result_sha: str,
    tock_sha: str,
) -> None:
    run_root = runtime / "runs" / _ORIGIN_RUN_ID
    task_snapshot = yaml.safe_dump(
        {
            "schema_version": "1.0",
            "active_task": {
                "task_id": _TASK_ID,
                "spec_ref": "./tasks/synthetic-lifecycle.md",
                "allowed_paths": ["synthetic-lifecycle.txt"],
                "validation": {
                    "commands": [
                        {
                            "id": "tests",
                            "command": "python -m pytest -q",
                            "timeout_seconds": 300,
                        }
                    ]
                },
                "expected_outcome": {"kind": "GREEN"},
                "adapter": {"kind": "cursor"},
                "metadata": {},
            },
        },
        sort_keys=False,
    ).encode("utf-8")
    task_snapshot_path = run_root / "task_snapshot.yaml"
    task_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    task_snapshot_path.write_bytes(task_snapshot)
    task_identity = canonicalize_persisted_task_snapshot(task_snapshot)

    source_payloads = (
        (
            "./tasks/synthetic-lifecycle.md",
            "instructions/task_spec",
            b"synthetic lifecycle task spec\n",
        ),
        (
            "./prompts/developer.md",
            "instructions/prompts/developer",
            b"synthetic lifecycle developer prompt\n",
        ),
        (
            "./prompts/orchestrator.md",
            "instructions/prompts/orchestrator",
            b"synthetic lifecycle orchestrator prompt\n",
        ),
        (
            "./prompts/validator.md",
            "instructions/prompts/validator",
            b"synthetic lifecycle validator prompt\n",
        ),
    )
    sources: list[dict[str, Any]] = []
    for logical_ref, snapshot_path, payload in source_payloads:
        destination = run_root / snapshot_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        sources.append(
            {
                "logical_ref": logical_ref,
                "snapshot_path": snapshot_path,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "byte_length": len(payload),
            }
        )
    instruction_snapshot_path = (
        run_root / "instructions" / "instruction_snapshot.yaml"
    )
    instruction_snapshot_path.write_text(
        yaml.safe_dump({"sources": sources}, sort_keys=False),
        encoding="utf-8",
    )
    instruction_signature = hashlib.sha256(
        json.dumps(
            [
                {
                    "byte_length": source["byte_length"],
                    "logical_ref": source["logical_ref"],
                    "sha256": source["sha256"],
                    "snapshot_path": source["snapshot_path"],
                }
                for source in sources
            ],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()

    path = runtime / "promotion_journal.yaml"
    journal = create_promotion_journal(
        task_id=_TASK_ID,
        origin_run_id=_ORIGIN_RUN_ID,
        drain_run_id=_DRAIN_RUN_ID,
        promotion_attempt_id=_ATTEMPT_ID,
        promotion_class=_PROMOTION_CLASS,
        base_main_sha=base_sha,
        result_sha=result_sha,
        validated_result_sha=result_sha,
        task_fingerprint=task_identity.fingerprint,
        task_snapshot_sha256=task_identity.snapshot_sha256,
        instruction_signature_sha256=instruction_signature,
        pending_at_utc=_STAMPS["PENDING"],
        reason_code="RECONCILIATION_COMPLETED",
    )
    persist_promotion_journal(path=path, journal=journal)
    current = journal
    for next_phase in _PHASES[1:]:
        current = transition_promotion_journal(
            current,
            phase=next_phase,
            drain_run_id=_DRAIN_RUN_ID,
            transitioned_at_utc=_STAMPS[next_phase.value],
            reason_code="RECONCILIATION_COMPLETED",
            tock_sha=(
                tock_sha if next_phase is PromotionJournalPhase.GIT_APPLIED else None
            ),
        )
        persist_promotion_journal(path=path, journal=current)
    reproved = reprove_run_local_snapshot(
        run_root=run_root,
        expected_task_fingerprint=current.task_fingerprint,
        expected_task_snapshot_sha256=current.task_snapshot_sha256,
        expected_instruction_signature_sha256=(
            current.instruction_signature_sha256
        ),
    )
    assert reproved.task_id == current.task_id


def _acquire_lock(runtime: Path) -> dict[str, Any]:
    acquired = acquire_runner_git_lock(
        lock_path=runtime / "runner_git.lock",
        run_id=_LOCK_RUN_ID,
        pid=os.getpid(),
        started_at_utc=_LOCK_STARTED_AT,
        now_utc=_LOCK_NOW,
        stale_after_seconds=_STALE_AFTER_SECONDS,
        is_pid_alive=lambda owner_pid: owner_pid == os.getpid(),
    )
    assert acquired.get("ok") is True
    ownership = acquired["ownership"]
    assert isinstance(ownership, dict)
    return ownership


def _linear_starts(
    repo: Path,
    tock_sha: str,
    *,
    hops: int,
    task_id: str,
    attempt_id: str,
    tree_mode: str,
    origin_run_id: str = _SUCCESSOR_ORIGIN_RUN_ID,
) -> tuple[str, ...]:
    parent = tock_sha
    shas: list[str] = []
    for _ in range(hops):
        tree = _tree_of(repo, parent)
        if tree_mode == "drift" and not shas:
            tree = _tree(repo, _blob(repo, "synthetic-lifecycle-drift\n"), "drift.txt")
        sha = _commit(
            repo,
            tree=tree,
            parents=(parent,),
            message=_start_message(
                parent_sha=parent,
                task_id=task_id,
                attempt_id=attempt_id,
                origin_run_id=origin_run_id,
            ),
        )
        shas.append(sha)
        parent = sha
    return tuple(shas)


def _materialize_chain(repo: Path, world: dict[str, Any], chain: str) -> None:
    tock_sha = world["tock_sha"]
    result_sha = world["result_sha"]
    if chain == "none":
        world["original_tick_sha"] = ""
        world["head_sha"] = ""
        return
    if chain == "gap":
        gap_sha = _commit(
            repo,
            tree=_tree_of(repo, tock_sha),
            parents=(tock_sha,),
            message="synthetic lifecycle gap\n",
        )
        head_sha = _commit(
            repo,
            tree=_tree_of(repo, gap_sha),
            parents=(gap_sha,),
            message=_start_message(parent_sha=gap_sha, task_id=_SUCCESSOR_TASK_ID, attempt_id=_SUCCESSOR_ATTEMPT_ID),
        )
        original = head_sha
        ref_task = _SUCCESSOR_TASK_ID
    elif chain == "merge":
        start_sha = _linear_starts(
            repo,
            tock_sha,
            hops=1,
            task_id=_SUCCESSOR_TASK_ID,
            attempt_id=_SUCCESSOR_ATTEMPT_ID,
            tree_mode="same",
        )[0]
        head_sha = _commit(
            repo,
            tree=_tree_of(repo, start_sha),
            parents=(start_sha, result_sha),
            message=_start_message(
                parent_sha=start_sha,
                task_id=_SUCCESSOR_TASK_ID,
                attempt_id=_SUCCESSOR_ATTEMPT_ID,
            ),
        )
        original = start_sha
        ref_task = _SUCCESSOR_TASK_ID
        assert _parents_of(repo, head_sha) == (start_sha, result_sha)
    elif chain == "detached":
        head_sha = _commit(
            repo,
            tree=_tree_of(repo, world["base_sha"]),
            parents=(world["base_sha"],),
            message=_start_message(
                parent_sha=world["base_sha"],
                task_id=_SUCCESSOR_TASK_ID,
                attempt_id=_SUCCESSOR_ATTEMPT_ID,
            ),
        )
        original = head_sha
        ref_task = _SUCCESSOR_TASK_ID
    elif chain == "foreign":
        head_sha = _linear_starts(
            repo,
            tock_sha,
            hops=1,
            task_id=_FOREIGN_TASK_ID,
            attempt_id=_FOREIGN_ATTEMPT_ID,
            tree_mode="same",
        )[0]
        original = head_sha
        ref_task = _SUCCESSOR_TASK_ID
    elif chain == "predecessor":
        head_sha = _linear_starts(
            repo,
            tock_sha,
            hops=1,
            task_id=_TASK_ID,
            attempt_id=_SUCCESSOR_ATTEMPT_ID,
            tree_mode="same",
        )[0]
        original = head_sha
        ref_task = _TASK_ID
    elif chain in {"origin_drift", "attempt_drift"}:
        head_sha = _linear_starts(
            repo,
            tock_sha,
            hops=1,
            task_id=_SUCCESSOR_TASK_ID,
            attempt_id=(
                _DRIFT_ATTEMPT_ID if chain == "attempt_drift" else _SUCCESSOR_ATTEMPT_ID
            ),
            tree_mode="same",
            origin_run_id=(
                _DRIFT_ORIGIN_RUN_ID
                if chain == "origin_drift"
                else _SUCCESSOR_ORIGIN_RUN_ID
            ),
        )[0]
        original = head_sha
        ref_task = _SUCCESSOR_TASK_ID
    else:
        hops = 1 if chain == "one" else 2
        tree_mode = "drift" if chain == "tree" else "same"
        shas = _linear_starts(
            repo,
            tock_sha,
            hops=hops,
            task_id=_SUCCESSOR_TASK_ID,
            attempt_id=_SUCCESSOR_ATTEMPT_ID,
            tree_mode=tree_mode,
        )
        original = shas[0]
        head_sha = shas[-1]
        ref_task = _SUCCESSOR_TASK_ID
    world["original_tick_sha"] = original
    world["head_sha"] = head_sha
    world["ref_task_id"] = ref_task
    if chain != "missing_ref":
        _update_ref(repo, _task_ref(ref_task), head_sha)


def _seed(tmp_path: Path, *, chain: str = "two", lock: bool = True) -> dict[str, Any]:
    repo = _init_repo(tmp_path / "repo")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    base_tree = _tree(repo, _blob(repo, "synthetic-lifecycle-base\n"), "tracked.txt")
    base_sha = _commit(repo, tree=base_tree, parents=(), message="synthetic lifecycle base\n")
    result_tree = _tree(repo, _blob(repo, "synthetic-lifecycle-result\n"), "tracked.txt")
    result_sha = _commit(
        repo,
        tree=result_tree,
        parents=(base_sha,),
        message="synthetic lifecycle result\n",
    )
    tock_sha = _commit(
        repo,
        tree=result_tree,
        parents=(base_sha, result_sha),
        message=_canonical_message(expected_base_sha=base_sha, result_sha=result_sha),
    )
    _update_ref(repo, _MAIN_REF, tock_sha)
    _git(repo, "reset", "--hard", _MAIN_REF)
    _write_journal_and_its_snapshot_family(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        tock_sha=tock_sha,
    )
    record_promotion_consumed(
        promotion_consumed_path=runtime / "promotion_consumed.yaml",
        promotion_attempt_id=_ATTEMPT_ID,
        result_sha=result_sha,
        main_state_before_promotion=_pre_state(base_sha),
        consumed_at_utc=_STAMPS["FINALIZED"],
        tock_sha=tock_sha,
    )
    write_main_state(
        main_state_path=runtime / "main_state.yaml",
        payload={
            "schema_version": "1.0",
            "state": "GREEN_VALIDATED",
            "main_branch": "main",
            "validated_result_sha": result_sha,
            "last_tock_sha": tock_sha,
            "observed_main_head_sha": tock_sha,
            "reason_code": "RECONCILIATION_COMPLETED",
            "updated_at_utc": _STAMPS["FINALIZED"],
        },
    )
    persist_drain_outcome(
        runtime_root=runtime,
        outcome=DrainOutcome(
            status=DrainStatus.DRAINED,
            drain_run_id=_DRAIN_RUN_ID,
            observed_main_head_sha=tock_sha,
            reason_code="PROMOTION_EFFECT_ADMITTED",
            task_id=_TASK_ID,
            origin_run_id=_ORIGIN_RUN_ID,
            promotion_attempt_id=_ATTEMPT_ID,
            result_sha=result_sha,
            promotion_class=_PROMOTION_CLASS,
            tock_sha=tock_sha,
        ),
    )
    world: dict[str, Any] = {
        "repo": repo,
        "runtime": runtime,
        "base_sha": base_sha,
        "result_sha": result_sha,
        "tock_sha": tock_sha,
        "lock_ownership": _acquire_lock(runtime) if lock else {
            "schema_version": "1.0",
            "lock_name": "runner_git.lock",
            "run_id": _LOCK_RUN_ID,
            "pid": os.getpid(),
            "started_at_utc": _LOCK_STARTED_AT,
        },
    }
    _materialize_chain(repo, world, chain)
    return world


def _prepare_kwargs(world: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "repo": world["repo"],
        "runtime": world["runtime"],
        "main_ref": _MAIN_REF,
        "continuation_origin": ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        "successor_task_id": _SUCCESSOR_TASK_ID,
        "successor_origin_run_id": _SUCCESSOR_ORIGIN_RUN_ID,
        "successor_promotion_attempt_id": _SUCCESSOR_ATTEMPT_ID,
        "expected_continuation_head_sha": world["head_sha"],
        "lock_ownership": world["lock_ownership"],
        "prepared_at_utc": _PREPARED_AT,
    }
    payload.update(overrides)
    return payload


def _load_kwargs(world: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "repo": world["repo"],
        "runtime": world["runtime"],
        "main_ref": _MAIN_REF,
        "successor_task_id": _SUCCESSOR_TASK_ID,
        "observed_main_head_sha": world["tock_sha"],
        "lock_ownership": world["lock_ownership"],
    }
    payload.update(overrides)
    return payload


def _dispatch_kwargs(world: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    payload = _load_kwargs(world)
    payload["task_id"] = payload.pop("successor_task_id")
    payload.update(overrides)
    return payload


def _reason(operation: Callable[[], Any]) -> str:
    with pytest.raises(ContinuationError) as caught:
        operation()
    return caught.value.reason_code


def _runtime_bytes(runtime: Path) -> dict[str, bytes]:
    return {
        path.relative_to(runtime).as_posix(): path.read_bytes()
        for path in sorted(runtime.rglob("*"))
        if path.is_file()
    }


def _ref_snapshot(repo: Path) -> str:
    return _git(repo, "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads")


def _object_names(repo: Path) -> tuple[str, ...]:
    output = _git(repo, "cat-file", "--batch-check", "--batch-all-objects")
    return tuple(sorted(line for line in output.splitlines() if line))


def _normalize_git_command(argv: tuple[str, ...]) -> tuple[str, ...]:
    if not argv or argv[0] != "git":
        return ()
    index = 1
    while index < len(argv):
        token = argv[index]
        if token in {"-C", "-c"}:
            index += 2
            continue
        if token.startswith("--git-dir") or token.startswith("--work-tree"):
            index += 1
            continue
        break
    return tuple(argv[index:])


def _is_main_rev_parse(command: tuple[str, ...]) -> bool:
    if not command or command[0] != "rev-parse":
        return False
    if "--abbrev-ref" in command:
        return False
    if any("^{" in token for token in command):
        return False
    return _MAIN_REF in command


def _assert_facade_git(calls: list[tuple[str, ...]]) -> None:
    saw_first_parent = False
    saw_main_rev_parse = False
    for call in calls:
        assert "--all" not in call
        command = _normalize_git_command(call)
        assert command[:1] not in {
            ("for-each-ref",),
            ("show-ref",),
            ("branch",),
            ("update-ref",),
            ("commit-tree",),
        }
        if command[:1] == ("rev-list",):
            assert "--first-parent" in command
            assert _MAIN_REF in command
            saw_first_parent = True
        if _is_main_rev_parse(command):
            saw_main_rev_parse = True
    assert saw_first_parent is True
    assert saw_main_rev_parse is True


def _capture_git(
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[[], Any],
    *,
    drifted_main_sha: str | None = None,
) -> tuple[Any, list[tuple[str, ...]]]:
    calls: list[tuple[str, ...]] = []

    def _spy(args: list[str] | tuple[str, ...], **spawn_kwargs: Any) -> Any:
        if isinstance(args, (list, tuple)) and args and args[0] == "git":
            recorded = tuple(str(part) for part in args)
            calls.append(recorded)
            command = _normalize_git_command(recorded)
            if drifted_main_sha is not None and _is_main_rev_parse(command):
                return subprocess.CompletedProcess(
                    args=list(recorded),
                    returncode=0,
                    stdout=f"{drifted_main_sha}\n",
                    stderr="",
                )
        return _REAL_SUBPROCESS_RUN(args, **spawn_kwargs)

    monkeypatch.setattr(subprocess, "run", _spy)
    try:
        return operation(), calls
    finally:
        monkeypatch.undo()


def _read_artifact(runtime: Path) -> dict[str, Any]:
    loaded = yaml.safe_load((runtime / _ARTIFACT_NAME).read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _binding(payload: dict[str, Any]) -> dict[str, Any]:
    origin = payload["origin"]
    evidence = dict(origin["evidence"])
    evidence["predecessor_identity"] = dict(evidence["predecessor_identity"])
    return {
        "schema_version": payload["schema_version"],
        "origin": {"kind": origin["kind"], "evidence": evidence},
        "original_tick_sha": payload["original_tick_sha"],
        "expected_continuation_head_sha": payload["expected_continuation_head_sha"],
        "commit_chain": [dict(entry) for entry in payload["commit_chain"]],
        "successor_task_id": payload["successor_task_id"],
    }


def _write_artifact(runtime: Path, payload: dict[str, Any]) -> None:
    (runtime / _ARTIFACT_NAME).write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _refresh_digest(payload: dict[str, Any]) -> None:
    payload["continuation_digest_sha256"] = binding_digest(_binding(payload))


def _assert_keyword_only(function: Callable[..., Any], names: tuple[str, ...]) -> None:
    signature = inspect.signature(function)
    assert tuple(signature.parameters) == names
    for name, parameter in signature.parameters.items():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty
        for token in _FORBIDDEN_PARAMETER_TOKENS:
            assert token not in name


def _assert_authorized_view(view: Any, world: dict[str, Any]) -> None:
    assert isinstance(view, lifecycle.ContinuationLifecycleView)
    assert isinstance(view.origin_proof, lifecycle.FinalizedOriginLifecycleView)
    assert view.contract.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert view.contract.status == "AUTHORIZED"
    assert view.observation.phase is LifecyclePhase.PRE_ACK
    assert view.observation.identity == view.contract.identity
    identity = view.contract.identity
    assert identity.task_id == _TASK_ID
    assert identity.origin_run_id == _ORIGIN_RUN_ID
    assert identity.promotion_attempt_id == _ATTEMPT_ID
    assert identity.promotion_class == _PROMOTION_CLASS
    assert view.origin_proof.predecessor_identity == identity
    assert view.origin_proof.tock_sha == world["tock_sha"]
    assert type(view.origin_proof).__name__ == "FinalizedOriginLifecycleView"
    assert view.contract.successor_task_id == _SUCCESSOR_TASK_ID
    assert view.contract.original_tick_sha == world["original_tick_sha"]
    assert view.contract.expected_continuation_head_sha == world["head_sha"]
    assert view.commit_chain[0].parent_sha == world["tock_sha"]
    assert view.commit_chain[0].sha == world["original_tick_sha"]
    assert view.commit_chain[-1].sha == world["head_sha"]
    for previous, current in zip(view.commit_chain, view.commit_chain[1:]):
        assert current.parent_sha == previous.sha
        assert len(_parents_of(world["repo"], current.sha)) == 1
    assert len(_parents_of(world["repo"], view.commit_chain[0].sha)) == 1
    for binding in view.commit_chain:
        assert isinstance(binding, lifecycle.ContinuationCommitBinding)
    payload = _read_artifact(world["runtime"])
    assert payload["schema_version"] == "2.0"
    payload = view.contract.to_mapping()
    assert set(payload) == _AUTHORIZED_FIELDS
    assert payload["status"] == "AUTHORIZED"
    assert payload["prepared_at_utc"] == _PREPARED_AT
    assert payload["continuation_digest_sha256"] == binding_digest(_binding(payload))
    with pytest.raises(dataclasses.FrozenInstanceError):
        view.origin_proof.tock_sha = "drifted-tock"
    with pytest.raises(dataclasses.FrozenInstanceError):
        view.commit_chain[0].sha = "drifted-start"


def test_prepare_binds_fresh_successor_and_gap_free_start_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed(tmp_path)
    before_runtime = _runtime_bytes(world["runtime"])
    before_refs = _ref_snapshot(world["repo"])
    before_objects = _object_names(world["repo"])

    def _prepare() -> Any:
        return lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))

    view, calls = _capture_git(monkeypatch, _prepare)
    _assert_facade_git(calls)
    _assert_authorized_view(view, world)
    assert len(view.commit_chain) == 2
    after_runtime = _runtime_bytes(world["runtime"])
    assert set(after_runtime) - set(before_runtime) == {_ARTIFACT_NAME}
    for name, content in before_runtime.items():
        assert after_runtime[name] == content
    assert _ref_snapshot(world["repo"]) == before_refs
    assert _object_names(world["repo"]) == before_objects
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == world["tock_sha"]


def test_one_start_chain_is_sufficient(tmp_path: Path) -> None:
    world = _seed(tmp_path, chain="one")
    view = lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    _assert_authorized_view(view, world)
    assert len(view.commit_chain) == 1


def test_identical_prepare_reuses_authorized_bytes(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    original = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    again = lifecycle.prepare_post_tick_continuation(
        **_prepare_kwargs(world, prepared_at_utc=_PREPARED_AT_LATER)
    )
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == original
    assert again.contract.to_mapping()["prepared_at_utc"] == _PREPARED_AT
    assert again.contract.continuation_digest_sha256 == (
        _read_artifact(world["runtime"])["continuation_digest_sha256"]
    )


def test_load_and_dispatch_reprove_authorized_binding_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed(tmp_path)
    prepared = lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    before_runtime = _runtime_bytes(world["runtime"])
    before_refs = _ref_snapshot(world["repo"])

    def _load() -> Any:
        return lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world))

    loaded, load_calls = _capture_git(monkeypatch, _load)
    _assert_facade_git(load_calls)
    assert loaded.contract.continuation_digest_sha256 == (
        prepared.contract.continuation_digest_sha256
    )
    assert loaded.commit_chain == prepared.commit_chain
    assert loaded.observation.phase is LifecyclePhase.PRE_ACK

    def _dispatch() -> Any:
        return lifecycle.assert_post_tick_continuation_dispatch_task(
            **_dispatch_kwargs(world)
        )

    dispatched, dispatch_calls = _capture_git(monkeypatch, _dispatch)
    _assert_facade_git(dispatch_calls)
    assert dispatched.contract.successor_task_id == _SUCCESSOR_TASK_ID
    assert _runtime_bytes(world["runtime"]) == before_runtime
    assert _ref_snapshot(world["repo"]) == before_refs


def test_missing_artifact_is_absence_not_an_invented_successor(tmp_path: Path) -> None:
    world = _seed(tmp_path, chain="none")
    before = _runtime_bytes(world["runtime"])
    assert lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world)) is None
    assert (
        lifecycle.assert_post_tick_continuation_dispatch_task(**_dispatch_kwargs(world))
        is None
    )
    assert _runtime_bytes(world["runtime"]) == before


def test_foreign_task_ref_does_not_authorize_or_block(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    foreign_sha = _commit(
        world["repo"],
        tree=_tree_of(world["repo"], world["tock_sha"]),
        parents=(world["tock_sha"],),
        message="synthetic lifecycle foreign ref\n",
    )
    _update_ref(world["repo"], _task_ref(_FOREIGN_TASK_ID), foreign_sha)
    view = lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    _assert_authorized_view(view, world)
    assert _git(world["repo"], "rev-parse", _task_ref(_FOREIGN_TASK_ID)) == foreign_sha


@pytest.mark.parametrize("chain", ["gap", "merge", "tree", "detached"])
def test_chain_geometry_blocks(tmp_path: Path, chain: str) -> None:
    world = _seed(tmp_path, chain=chain)
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    ) == _CHAIN_MISMATCH


@pytest.mark.parametrize("chain", ["missing_ref", "shifted"])
def test_task_ref_head_blocks(tmp_path: Path, chain: str) -> None:
    world = _seed(tmp_path, chain="two" if chain == "shifted" else "missing_ref")
    if chain == "shifted":
        _update_ref(
            world["repo"],
            _task_ref(_SUCCESSOR_TASK_ID),
            world["original_tick_sha"],
        )
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    ) == _HEAD_MISMATCH


def test_fresh_main_rev_parse_drift_blocks_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed(tmp_path)
    drifted = _commit(
        world["repo"],
        tree=_tree_of(world["repo"], world["tock_sha"]),
        parents=(world["tock_sha"],),
        message="synthetic lifecycle main drift\n",
    )
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == world["tock_sha"]

    def _prepare() -> Any:
        return lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))

    with pytest.raises(ContinuationError) as caught:
        _capture_git(monkeypatch, _prepare, drifted_main_sha=drifted)
    assert caught.value.reason_code == _HEAD_MISMATCH
    assert _ARTIFACT_NAME not in _runtime_bytes(world["runtime"])


def test_load_blocks_live_main_and_observed_head_drift(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(
            **_load_kwargs(world, observed_main_head_sha=world["base_sha"])
        )
    ) == _HEAD_MISMATCH
    drifted = _commit(
        world["repo"],
        tree=_tree_of(world["repo"], world["tock_sha"]),
        parents=(world["tock_sha"],),
        message="synthetic lifecycle load drift\n",
    )
    _update_ref(world["repo"], _MAIN_REF, drifted)
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world))
    ) == _HEAD_MISMATCH


@pytest.mark.parametrize(
    ("chain", "successor_task_id"),
    [
        ("two", " "),
        ("two", _TASK_ID),
        ("foreign", _SUCCESSOR_TASK_ID),
        ("predecessor", _TASK_ID),
    ],
)
def test_successor_binding_blocks(
    tmp_path: Path,
    chain: str,
    successor_task_id: str,
) -> None:
    world = _seed(tmp_path, chain=chain)
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(
            **_prepare_kwargs(
                world,
                successor_task_id=successor_task_id,
                successor_origin_run_id=" " if successor_task_id == " " else _SUCCESSOR_ORIGIN_RUN_ID,
            )
        )
    ) == _SUCCESSOR_MISMATCH


def test_load_and_dispatch_reject_other_successor(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(
            **_load_kwargs(world, successor_task_id=_FOREIGN_TASK_ID)
        )
    ) == _SUCCESSOR_MISMATCH
    assert _reason(
        lambda: lifecycle.assert_post_tick_continuation_dispatch_task(
            **_dispatch_kwargs(world, task_id=_FOREIGN_TASK_ID)
        )
    ) == _SUCCESSOR_MISMATCH
    payload = _read_artifact(world["runtime"])
    payload["successor_task_id"] = _FOREIGN_TASK_ID
    _refresh_digest(payload)
    _write_artifact(world["runtime"], payload)
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(
            **_load_kwargs(world, successor_task_id=_FOREIGN_TASK_ID)
        )
    ) == _SUCCESSOR_MISMATCH


def test_missing_journal_and_second_origin_block(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    (world["runtime"] / "promotion_journal.yaml").unlink()
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    ) == _ORIGIN_INVALID
    intact = _seed(tmp_path / "second")
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(
            **_prepare_kwargs(intact, continuation_origin="SECOND_ORIGIN")
        )
    ) == _ORIGIN_INVALID


def test_lock_loss_blocks_before_origin_proof(tmp_path: Path) -> None:
    unlocked = _seed(tmp_path / "unlocked", lock=False)
    (unlocked["runtime"] / "promotion_journal.yaml").unlink()
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(unlocked))
    ) == _LIFECYCLE_INVALID
    world = _seed(tmp_path / "foreign-lock")
    foreign = dict(world["lock_ownership"])
    foreign["pid"] = world["lock_ownership"]["pid"] + 1
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(
            **_prepare_kwargs(world, lock_ownership=foreign)
        )
    ) == _LIFECYCLE_INVALID
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(
            **_load_kwargs(unlocked, lock_ownership=unlocked["lock_ownership"])
        )
    ) == _LIFECYCLE_INVALID


def test_conflicting_or_consumed_artifact_is_not_rebound(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    payload = _read_artifact(world["runtime"])
    payload["successor_task_id"] = _FOREIGN_TASK_ID
    _refresh_digest(payload)
    _write_artifact(world["runtime"], payload)
    conflicted = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    ) == _LIFECYCLE_INVALID
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == conflicted
    payload = _read_artifact(world["runtime"])
    payload["successor_task_id"] = _SUCCESSOR_TASK_ID
    payload["status"] = "CONSUMED"
    payload["successor_promotion_attempt_id"] = _SUCCESSOR_ATTEMPT_ID
    payload["successor_tock_sha"] = "ab" * 20
    payload["consumed_at_utc"] = _PREPARED_AT_LATER
    _refresh_digest(payload)
    _write_artifact(world["runtime"], payload)
    consumed = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    ) == _LIFECYCLE_INVALID
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world))
    ) == _LIFECYCLE_INVALID
    assert _reason(
        lambda: lifecycle.assert_post_tick_continuation_dispatch_task(
            **_dispatch_kwargs(world)
        )
    ) == _LIFECYCLE_INVALID
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == consumed


def test_declared_and_git_chain_drift_block_on_load(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    declared = _read_artifact(world["runtime"])
    declared["commit_chain"][0]["parent_sha"] = world["base_sha"]
    _write_artifact(world["runtime"], declared)
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world))
    ) == _CHAIN_MISMATCH
    shortened = _read_artifact(world["runtime"])
    shortened["commit_chain"] = [
        {"sha": world["head_sha"], "parent_sha": world["tock_sha"]}
    ]
    shortened["original_tick_sha"] = world["head_sha"]
    shortened["expected_continuation_head_sha"] = world["head_sha"]
    _refresh_digest(shortened)
    _write_artifact(world["runtime"], shortened)
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(
            **_load_kwargs(world)
        )
    ) == _CHAIN_MISMATCH


def test_digest_drift_blocks_without_rewriting_git(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    payload = _read_artifact(world["runtime"])
    digest = payload["continuation_digest_sha256"]
    payload["continuation_digest_sha256"] = digest[:-1] + (
        "0" if digest[-1] != "0" else "1"
    )
    _write_artifact(world["runtime"], payload)
    before_refs = _ref_snapshot(world["repo"])
    assert _reason(
        lambda: lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world))
    ) == _DIGEST_MISMATCH
    assert _ref_snapshot(world["repo"]) == before_refs


@pytest.mark.parametrize(
    ("mode", "reason_code"),
    [
        ("missing_lock", _LIFECYCLE_INVALID),
        ("observed_drift", _HEAD_MISMATCH),
        ("live_drift", _HEAD_MISMATCH),
        ("coordinated_drift", _HEAD_MISMATCH),
        ("digest_drift", _DIGEST_MISMATCH),
    ],
)
def test_dispatch_reproof_blocks(
    tmp_path: Path,
    mode: str,
    reason_code: str,
) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    kwargs = _dispatch_kwargs(world)
    if mode == "missing_lock":
        (world["runtime"] / "runner_git.lock").unlink()
    elif mode == "observed_drift":
        kwargs["observed_main_head_sha"] = world["base_sha"]
    elif mode == "live_drift":
        drifted = _commit(
            world["repo"],
            tree=_tree_of(world["repo"], world["tock_sha"]),
            parents=(world["tock_sha"],),
            message="synthetic lifecycle dispatch drift\n",
        )
        _update_ref(world["repo"], _MAIN_REF, drifted)
    elif mode == "coordinated_drift":
        drifted = _commit(
            world["repo"],
            tree=_tree_of(world["repo"], world["tock_sha"]),
            parents=(world["tock_sha"],),
            message="synthetic lifecycle coordinated drift\n",
        )
        _update_ref(world["repo"], _MAIN_REF, drifted)
        kwargs["observed_main_head_sha"] = drifted
    elif mode == "digest_drift":
        payload = _read_artifact(world["runtime"])
        digest = payload["continuation_digest_sha256"]
        payload["continuation_digest_sha256"] = digest[:-1] + (
            "0" if digest[-1] != "0" else "1"
        )
        _write_artifact(world["runtime"], payload)
    else:
        raise AssertionError(mode)
    before = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    assert _reason(
        lambda: lifecycle.assert_post_tick_continuation_dispatch_task(**kwargs)
    ) == reason_code
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before


@pytest.mark.parametrize(
    ("drift", "reason_code"),
    [
        ("binding_digest", _DIGEST_MISMATCH),
        ("declared_chain", _CHAIN_MISMATCH),
    ],
)
def test_prepare_existing_digest_drift_blocks(
    tmp_path: Path,
    drift: str,
    reason_code: str,
) -> None:
    world = _seed(tmp_path)
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    payload = _read_artifact(world["runtime"])
    if drift == "binding_digest":
        digest = payload["continuation_digest_sha256"]
        payload["continuation_digest_sha256"] = digest[:-1] + (
            "0" if digest[-1] != "0" else "1"
        )
    elif drift == "declared_chain":
        payload["commit_chain"][0]["parent_sha"] = world["base_sha"]
        _refresh_digest(payload)
    else:
        raise AssertionError(drift)
    _write_artifact(world["runtime"], payload)
    drifted = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    ) == reason_code
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == drifted


@pytest.mark.parametrize(
    "chain",
    ["origin_drift", "attempt_drift", "foreign_input_ref"],
)
def test_isolated_successor_subject_drift_blocks(tmp_path: Path, chain: str) -> None:
    world = _seed(tmp_path, chain="one" if chain == "foreign_input_ref" else chain)
    subject = _git(world["repo"], "log", "-1", "--format=%s", world["head_sha"])
    task_id, origin_run_id, attempt_id, _parent = subject.removeprefix(
        "START "
    ).split(" ")
    assert task_id == _SUCCESSOR_TASK_ID
    prepare_kwargs = _prepare_kwargs(world)
    if chain == "origin_drift":
        assert origin_run_id == _DRIFT_ORIGIN_RUN_ID
        assert attempt_id == _SUCCESSOR_ATTEMPT_ID
    elif chain == "attempt_drift":
        assert origin_run_id == _SUCCESSOR_ORIGIN_RUN_ID
        assert attempt_id == _DRIFT_ATTEMPT_ID
    else:
        assert origin_run_id == _SUCCESSOR_ORIGIN_RUN_ID
        assert attempt_id == _SUCCESSOR_ATTEMPT_ID
        foreign_ref = _task_ref(_SUCCESSOR_TASK_ID, _DRIFT_ORIGIN_RUN_ID)
        _update_ref(world["repo"], foreign_ref, world["head_sha"])
        _git(world["repo"], "update-ref", "-d", _task_ref(_SUCCESSOR_TASK_ID))
        prepare_kwargs["successor_origin_run_id"] = _DRIFT_ORIGIN_RUN_ID
        assert _git(world["repo"], "rev-parse", foreign_ref) == world["head_sha"]
    if chain != "foreign_input_ref":
        recorded_head = _git(
            world["repo"],
            "rev-parse",
            _task_ref(_SUCCESSOR_TASK_ID),
        )
        assert recorded_head == world["head_sha"]
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(**prepare_kwargs)
    ) == _SUCCESSOR_MISMATCH
    assert _ARTIFACT_NAME not in _runtime_bytes(world["runtime"])


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        (LifecyclePhase.PRE_ACK, LifecyclePhase.PRE_ACK),
        (LifecyclePhase.PRE_ACK, LifecyclePhase.CONSUMED),
        (LifecyclePhase.CONSUMED, LifecyclePhase.CONSUMED),
    ],
)
def test_only_authorized_to_consumed_neighbors_are_forward(
    previous: LifecyclePhase,
    current: LifecyclePhase,
) -> None:
    lifecycle.assert_lifecycle_transition(
        origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        previous=previous,
        current=current,
    )


def test_regression_and_second_origin_transition_block() -> None:
    assert _reason(
        lambda: lifecycle.assert_lifecycle_transition(
            origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
            previous=LifecyclePhase.CONSUMED,
            current=LifecyclePhase.PRE_ACK,
        )
    ) == _LIFECYCLE_INVALID
    assert _reason(
        lambda: lifecycle.assert_lifecycle_transition(
            origin="SECOND_ORIGIN",
            previous=LifecyclePhase.PRE_ACK,
            current=LifecyclePhase.CONSUMED,
        )
    ) == _LIFECYCLE_INVALID


def test_surface_is_keyword_only_and_drops_reopen_and_consume() -> None:
    _assert_keyword_only(lifecycle.prepare_post_tick_continuation, _PREPARE_PARAMETERS)
    _assert_keyword_only(
        lifecycle.load_authorized_post_tick_continuation,
        _LOAD_PARAMETERS,
    )
    _assert_keyword_only(
        lifecycle.assert_post_tick_continuation_dispatch_task,
        _DISPATCH_PARAMETERS,
    )
    _assert_keyword_only(lifecycle.assert_lifecycle_transition, _TRANSITION_PARAMETERS)
    exported = {name for name in dir(lifecycle) if not name.startswith("_")}
    for name in _REQUIRED_EXPORTS:
        assert name in exported
    source = inspect.getsource(lifecycle)
    for marker in _DROPPED_SOURCE_MARKERS:
        assert marker not in source
    assert "ReopenOriginLifecycleView" not in exported
    assert "consume_post_tick_continuation" not in exported
    assert callable(prove_finalized_promotion_tick)
    assert OriginProof.__name__ == "OriginProof"


def _drifted_successor_start(world: dict[str, Any], drift: str) -> str:
    repo = world["repo"]
    parent = world["tock_sha"]
    tree = _tree_of(repo, parent)
    origin_run_id = _SUCCESSOR_ORIGIN_RUN_ID
    attempt_id = _SUCCESSOR_ATTEMPT_ID
    if drift == "tree":
        tree = _tree(
            repo,
            _blob(repo, "synthetic-lifecycle-reproof-drift\n"),
            "reproof-drift.txt",
        )
    elif drift == "origin_run":
        origin_run_id = _DRIFT_ORIGIN_RUN_ID
    elif drift == "promotion_attempt":
        attempt_id = _DRIFT_ATTEMPT_ID
    else:
        raise AssertionError(drift)
    return _commit(
        repo,
        tree=tree,
        parents=(parent,),
        message=_start_message(
            parent_sha=parent,
            task_id=_SUCCESSOR_TASK_ID,
            attempt_id=attempt_id,
            origin_run_id=origin_run_id,
        ),
    )


def test_exact_ref_and_expected_head_on_predecessor_tock_block(
    tmp_path: Path,
) -> None:
    world = _seed(tmp_path, chain="none")
    _update_ref(world["repo"], _task_ref(_SUCCESSOR_TASK_ID), world["tock_sha"])
    exact_ref = _git(world["repo"], "rev-parse", _task_ref(_SUCCESSOR_TASK_ID))
    assert exact_ref == world["tock_sha"]
    assert _git(world["repo"], "log", "-1", "--format=%s", exact_ref) == "TOCK"
    before_runtime = _runtime_bytes(world["runtime"])
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(
            **_prepare_kwargs(world, expected_continuation_head_sha=exact_ref)
        )
    ) == _CHAIN_MISMATCH
    assert _runtime_bytes(world["runtime"]) == before_runtime
    assert _ARTIFACT_NAME not in before_runtime


@pytest.mark.parametrize("surface", ["load", "dispatch"])
@pytest.mark.parametrize(
    ("drift", "reason_code"),
    [
        ("tree", _CHAIN_MISMATCH),
        ("origin_run", _SUCCESSOR_MISMATCH),
        ("promotion_attempt", _SUCCESSOR_MISMATCH),
    ],
)
def test_load_and_dispatch_reprove_tree_origin_and_attempt(
    tmp_path: Path,
    surface: str,
    drift: str,
    reason_code: str,
) -> None:
    world = _seed(tmp_path, chain="one")
    lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    prepared_bytes = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    drifted_sha = _drifted_successor_start(world, drift)
    _update_ref(world["repo"], _task_ref(_SUCCESSOR_TASK_ID), drifted_sha)
    assert _git(
        world["repo"], "rev-parse", _task_ref(_SUCCESSOR_TASK_ID)
    ) == drifted_sha
    subject = _git(world["repo"], "log", "-1", "--format=%s", drifted_sha)
    task_id, origin_run_id, attempt_id, parent_token = subject.removeprefix(
        "START "
    ).split(" ")
    assert task_id == _SUCCESSOR_TASK_ID
    assert parent_token == world["tock_sha"]
    assert _parents_of(world["repo"], drifted_sha) == (world["tock_sha"],)
    if drift == "tree":
        assert _tree_of(world["repo"], drifted_sha) != _tree_of(
            world["repo"], world["tock_sha"]
        )
        assert origin_run_id == _SUCCESSOR_ORIGIN_RUN_ID
        assert attempt_id == _SUCCESSOR_ATTEMPT_ID
    elif drift == "origin_run":
        assert _tree_of(world["repo"], drifted_sha) == _tree_of(
            world["repo"], world["tock_sha"]
        )
        assert origin_run_id == _DRIFT_ORIGIN_RUN_ID
        assert attempt_id == _SUCCESSOR_ATTEMPT_ID
    else:
        assert _tree_of(world["repo"], drifted_sha) == _tree_of(
            world["repo"], world["tock_sha"]
        )
        assert origin_run_id == _SUCCESSOR_ORIGIN_RUN_ID
        assert attempt_id == _DRIFT_ATTEMPT_ID
    if drift == "tree":
        payload = _read_artifact(world["runtime"])
        payload["commit_chain"] = [
            {"sha": drifted_sha, "parent_sha": world["tock_sha"]}
        ]
        payload["original_tick_sha"] = drifted_sha
        payload["expected_continuation_head_sha"] = drifted_sha
        _refresh_digest(payload)
        _write_artifact(world["runtime"], payload)
        rebound = _read_artifact(world["runtime"])
        assert rebound["continuation_digest_sha256"] == binding_digest(
            _binding(rebound)
        )
    else:
        assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == prepared_bytes
    if surface == "load":
        operation: Callable[[], Any] = (
            lambda: lifecycle.load_authorized_post_tick_continuation(
                **_load_kwargs(world)
            )
        )
    elif surface == "dispatch":
        operation = lambda: lifecycle.assert_post_tick_continuation_dispatch_task(
            **_dispatch_kwargs(world)
        )
    else:
        raise AssertionError(surface)
    before_artifact = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    assert _reason(operation) == reason_code
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before_artifact


@pytest.mark.parametrize("surface", ["load", "dispatch"])
def test_load_and_dispatch_ignore_historical_attempt_drift_after_exact_ref_reset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
) -> None:
    world = _seed(tmp_path, chain="one")
    prepared = lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    exact_sha = world["head_sha"]
    exact_ref = _task_ref(_SUCCESSOR_TASK_ID)
    artifact_path = world["runtime"] / _ARTIFACT_NAME
    prepared_bytes = artifact_path.read_bytes()
    _git(world["repo"], "config", "core.logAllRefUpdates", "true")
    drifted_sha = _drifted_successor_start(world, "promotion_attempt")
    _update_ref(world["repo"], exact_ref, drifted_sha)
    _update_ref(world["repo"], exact_ref, exact_sha)
    assert _git(world["repo"], "rev-parse", "--verify", exact_ref) == exact_sha
    assert _git(
        world["repo"], "rev-parse", "--verify", f"{exact_ref}@{{1}}"
    ) == drifted_sha
    historical_subject = _git(
        world["repo"], "log", "-1", "--format=%s", drifted_sha
    )
    assert _DRIFT_ATTEMPT_ID in historical_subject
    assert _SUCCESSOR_ATTEMPT_ID not in historical_subject
    assert artifact_path.read_bytes() == prepared_bytes
    caught: ContinuationError | None = None

    def _operation() -> Any:
        nonlocal caught
        try:
            return _reprove_surface(world, surface)
        except ContinuationError as error:
            caught = error
            return None

    view, calls = _capture_git(monkeypatch, _operation)
    reflog_calls = [
        call for call in calls if any("@{" in part for part in call)
    ]
    assert reflog_calls == []
    assert caught is None
    _assert_authorized_view(view, world)
    assert view.contract.continuation_digest_sha256 == (
        prepared.contract.continuation_digest_sha256
    )
    assert view.commit_chain[-1].sha == exact_sha
    assert artifact_path.read_bytes() == prepared_bytes
    assert _git(world["repo"], "rev-parse", "--verify", exact_ref) == exact_sha


def test_prepare_blocks_start_parent_token_distinct_from_git_parent(
    tmp_path: Path,
) -> None:
    world = _seed(tmp_path, chain="none")
    head_sha = _commit(
        world["repo"],
        tree=_tree_of(world["repo"], world["tock_sha"]),
        parents=(world["tock_sha"],),
        message=_start_message(
            parent_sha=world["base_sha"],
            task_id=_SUCCESSOR_TASK_ID,
            attempt_id=_SUCCESSOR_ATTEMPT_ID,
        ),
    )
    _update_ref(world["repo"], _task_ref(_SUCCESSOR_TASK_ID), head_sha)
    subject = _git(world["repo"], "log", "-1", "--format=%s", head_sha)
    task_id, origin_run_id, attempt_id, parent_token = subject.removeprefix(
        "START "
    ).split(" ")
    assert task_id == _SUCCESSOR_TASK_ID
    assert origin_run_id == _SUCCESSOR_ORIGIN_RUN_ID
    assert attempt_id == _SUCCESSOR_ATTEMPT_ID
    assert parent_token == world["base_sha"]
    assert _parents_of(world["repo"], head_sha) == (world["tock_sha"],)
    assert parent_token != world["tock_sha"]
    before_runtime = _runtime_bytes(world["runtime"])
    assert _reason(
        lambda: lifecycle.prepare_post_tick_continuation(
            **_prepare_kwargs(world, expected_continuation_head_sha=head_sha)
        )
    ) == _SUCCESSOR_MISMATCH
    assert _runtime_bytes(world["runtime"]) == before_runtime
    assert _ARTIFACT_NAME not in before_runtime


def _reprove_surface(world: dict[str, Any], surface: str) -> Any:
    if surface == "load":
        return lifecycle.load_authorized_post_tick_continuation(**_load_kwargs(world))
    if surface == "dispatch":
        return lifecycle.assert_post_tick_continuation_dispatch_task(
            **_dispatch_kwargs(world)
        )
    raise AssertionError(surface)


def _assert_packed_exact_successor_ref(repo: Path, exact_sha: str) -> str:
    exact_ref = _task_ref(_SUCCESSOR_TASK_ID)
    git_dir = Path(_git(repo, "rev-parse", "--absolute-git-dir"))
    packed_lines = (git_dir / "packed-refs").read_text(encoding="utf-8").splitlines()
    assert f"{exact_sha} {exact_ref}" in packed_lines
    assert not (git_dir / exact_ref).exists()
    assert _git(repo, "rev-parse", "--verify", exact_ref) == exact_sha
    return exact_ref


@pytest.mark.parametrize("surface", ["load", "dispatch"])
def test_load_and_dispatch_keep_exact_successor_ref_after_pack_refs(
    tmp_path: Path,
    surface: str,
) -> None:
    world = _seed(tmp_path, chain="one")
    prepared = lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    exact_sha = world["head_sha"]
    before_runtime = _runtime_bytes(world["runtime"])
    before_refs = _ref_snapshot(world["repo"])
    _git(world["repo"], "pack-refs", "--all", "--prune")
    exact_ref = _assert_packed_exact_successor_ref(world["repo"], exact_sha)
    assert _ref_snapshot(world["repo"]) == before_refs
    view = _reprove_surface(world, surface)
    _assert_authorized_view(view, world)
    assert view.contract.continuation_digest_sha256 == (
        prepared.contract.continuation_digest_sha256
    )
    assert view.commit_chain == prepared.commit_chain
    assert _runtime_bytes(world["runtime"]) == before_runtime
    assert _git(world["repo"], "rev-parse", "--verify", exact_ref) == exact_sha
    assert _ref_snapshot(world["repo"]) == before_refs


@pytest.mark.parametrize("surface", ["load", "dispatch"])
def test_load_and_dispatch_ignore_foreign_loose_side_ref_beside_packed_exact_ref(
    tmp_path: Path,
    surface: str,
) -> None:
    world = _seed(tmp_path, chain="one")
    prepared = lifecycle.prepare_post_tick_continuation(**_prepare_kwargs(world))
    exact_sha = world["head_sha"]
    before_runtime = _runtime_bytes(world["runtime"])
    _git(world["repo"], "pack-refs", "--all", "--prune")
    exact_ref = _assert_packed_exact_successor_ref(world["repo"], exact_sha)
    side_ref = _task_ref(_SUCCESSOR_TASK_ID, _SIDE_ORIGIN_RUN_ID)
    side_sha = _commit(
        world["repo"],
        tree=_tree_of(world["repo"], world["tock_sha"]),
        parents=(world["tock_sha"],),
        message="synthetic lifecycle foreign side ref\n",
    )
    _update_ref(world["repo"], side_ref, side_sha)
    git_dir = Path(_git(world["repo"], "rev-parse", "--absolute-git-dir"))
    packed_lines = (git_dir / "packed-refs").read_text(encoding="utf-8").splitlines()
    assert (git_dir / side_ref).is_file()
    assert f"{exact_sha} {exact_ref}" in packed_lines
    assert all(side_ref not in line for line in packed_lines)
    assert _git(world["repo"], "rev-parse", "--verify", exact_ref) == exact_sha
    assert _git(world["repo"], "rev-parse", "--verify", side_ref) == side_sha
    assert side_sha != exact_sha
    before_refs = _ref_snapshot(world["repo"])
    view = _reprove_surface(world, surface)
    _assert_authorized_view(view, world)
    assert view.contract.continuation_digest_sha256 == (
        prepared.contract.continuation_digest_sha256
    )
    assert view.commit_chain[-1].sha == exact_sha
    subject = _git(
        world["repo"],
        "log",
        "-1",
        "--format=%s",
        view.commit_chain[-1].sha,
    )
    assert _SUCCESSOR_ORIGIN_RUN_ID in subject
    assert _SIDE_ORIGIN_RUN_ID not in subject
    assert _runtime_bytes(world["runtime"]) == before_runtime
    assert _git(world["repo"], "rev-parse", "--verify", exact_ref) == exact_sha
    assert _git(world["repo"], "rev-parse", "--verify", side_ref) == side_sha
    assert _ref_snapshot(world["repo"]) == before_refs
