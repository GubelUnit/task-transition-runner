"""Successor-TOCK consumption for one ordinary continuation.

OSS6-B17 binds the first consume of an exact successor task, promotion
attempt and canonical successor TOCK onto the existing authorized
artifact under valid lock ownership. That binding is a local,
identity-bound compare-and-swap and an idempotent reproof of the same
persisted truth. It is not an exactly-once guarantee. An identical retry
refuses a second write with ``CONTINUATION_ALREADY_CONSUMED``, and the
loader shows the same task, attempt, TOCK and timestamp. Compare fails
closed when the supplied SHA is not the first-parent main tip, and also
when that tip's Task-ID, Promotion-Attempt, Expected-Base parents, or
Result tree is foreign to the bound continuation. The canonical
successor TOCK carries ``expected_continuation_head_sha`` as
``Expected-Base`` and parent 0. Parent 1 stays the result commit, and
the predecessor TOCK stays reachable through the bound START chain. A
successor TOCK that instead uses the predecessor TOCK directly as
``Expected-Base`` and parent 0, while that longer START chain is
present, fails closed with ``CONTINUATION_SUCCESSOR_MISMATCH`` and
leaves the authorized artifact byte-identical. The same closed compare
holds for a TOCK-shaped tip that keeps the bound task, promotion
attempt and expected base but has exactly one Git parent. Authorized
bytes stay unchanged. A consumed binding conflicts on a foreign attempt,
a foreign TOCK, or both. A different successor task stays
``CONTINUATION_SUCCESSOR_MISMATCH`` for both ``AUTHORIZED`` and
``CONSUMED`` and does not rewrite bytes. The first consume is observed
as exactly ``git rev-list --first-parent`` on the configured main ref,
whether the process is spawned with ``run`` or ``Popen``. ``--all`` is
not an observation primitive. Lost lock ownership blocks with
``CONTINUATION_LIFECYCLE_INVALID`` before any swap. Distributed or
universal exactly-once remains forbidden. This file does not expect an
archive, reopen or replay helper.
"""

from __future__ import annotations

import importlib
import inspect
import os
import subprocess
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from task_transition_runner.runner_continuation_contract import (
    ContinuationContractView,
    ContinuationError,
    ContinuationOrigin,
    binding_digest,
    load_continuation_artifact,
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
import task_transition_runner.runner_continuation_lifecycle as lifecycle


_MAIN_REF = "refs/heads/main"
_ARTIFACT_NAME = "post_tick_continuation.yaml"
_SIDE_REF = "refs/heads/runner/backup/synthetic-consume"
_TASK_ID = "task-synthetic-consume-001"
_SUCCESSOR_TASK_ID = "task-synthetic-consume-successor-001"
_FOREIGN_TASK_ID = "task-synthetic-consume-foreign-001"
_ORIGIN_RUN_ID = "origin-synthetic-consume-001"
_SUCCESSOR_ORIGIN_RUN_ID = "origin-synthetic-consume-successor-001"
_ATTEMPT_ID = "attempt-synthetic-consume-001"
_SUCCESSOR_ATTEMPT_ID = "attempt-synthetic-consume-successor-001"
_FOREIGN_ATTEMPT_ID = "attempt-synthetic-consume-foreign-001"
_PROMOTION_CLASS = "GREEN_PROMOTION"
_DRAIN_RUN_ID = "drain-synthetic-consume-001"
_LOCK_RUN_ID = "run-synthetic-consume-001"
_PREPARED_AT = "2026-09-27T14:00:00Z"
_CONSUMED_AT = "2026-09-27T15:00:00Z"
_CONSUMED_AT_LATER = "2026-09-27T15:05:00Z"
_LOCK_STARTED_AT = "2026-09-27T13:59:00Z"
_LOCK_NOW = "2026-09-27T14:00:00Z"
_STALE_AFTER_SECONDS = 3600
_ALREADY_CONSUMED = "CONTINUATION_ALREADY_CONSUMED"
_CONSUMPTION_CONFLICT = "CONTINUATION_CONSUMPTION_CONFLICT"
_SUCCESSOR_MISMATCH = "CONTINUATION_SUCCESSOR_MISMATCH"
_LIFECYCLE_INVALID = "CONTINUATION_LIFECYCLE_INVALID"
_CONSUME_PARAMETERS = (
    "repo",
    "runtime",
    "main_ref",
    "successor_task_id",
    "successor_promotion_attempt_id",
    "successor_tock_sha",
    "consumed_at_utc",
    "lock_ownership",
)
_SCHEMA_TASK_FINGERPRINT = "4" * 64
_SCHEMA_TASK_SNAPSHOT_SHA256 = "5" * 64
_SCHEMA_INSTRUCTION_SIGNATURE_SHA256 = "6" * 64
_STAMPS = {
    "PENDING": "2026-09-27T00:00:00Z",
    "APPLYING": "2026-09-27T00:01:00Z",
    "GIT_APPLIED": "2026-09-27T00:02:00Z",
    "RECONCILED": "2026-09-27T00:03:00Z",
    "FINALIZED": "2026-09-27T00:04:00Z",
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
    "GIT_AUTHOR_NAME": "Synthetic Consume",
    "GIT_AUTHOR_EMAIL": "consume@task-transition-runner.invalid",
    "GIT_AUTHOR_DATE": "2026-09-27T00:00:00Z",
    "GIT_COMMITTER_NAME": "Synthetic Consume",
    "GIT_COMMITTER_EMAIL": "consume@task-transition-runner.invalid",
    "GIT_COMMITTER_DATE": "2026-09-27T00:00:00Z",
}
_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)


def _git(repo: Path, *args: str, input_text: str | None = None) -> str:
    completed = subprocess.run(
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


def _canonical_message(
    *,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    expected_base_sha: str,
    result_sha: str,
) -> str:
    values = {
        "Promotion-Attempt": promotion_attempt_id,
        "Task-ID": task_id,
        "Origin-Run": origin_run_id,
        "Result-SHA": result_sha,
        "Promotion-Class": _PROMOTION_CLASS,
        "Expected-Base": expected_base_sha,
    }
    trailers = "\n".join(
        f"{key}: {values[key]}" for key in _CANONICAL_TOCK_TRAILER_KEYS
    )
    return f"TOCK\n\n{trailers}\n"


def _start_message(*, parent_sha: str) -> str:
    return (
        f"START {_SUCCESSOR_TASK_ID} {_SUCCESSOR_ORIGIN_RUN_ID} "
        f"{_SUCCESSOR_ATTEMPT_ID} {parent_sha}\n"
    )


def _task_ref() -> str:
    return f"refs/heads/runner/task/{_SUCCESSOR_TASK_ID}/{_SUCCESSOR_ORIGIN_RUN_ID}"


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


def _write_journal(
    runtime: Path,
    *,
    base_sha: str,
    result_sha: str,
    tock_sha: str,
) -> None:
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
        task_fingerprint=_SCHEMA_TASK_FINGERPRINT,
        task_snapshot_sha256=_SCHEMA_TASK_SNAPSHOT_SHA256,
        instruction_signature_sha256=_SCHEMA_INSTRUCTION_SIGNATURE_SHA256,
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


def _seed(tmp_path: Path) -> dict[str, Any]:
    repo = _init_repo(tmp_path / "repo")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    base_tree = _tree(repo, _blob(repo, "synthetic-consume-base\n"), "base.txt")
    base_sha = _commit(
        repo,
        tree=base_tree,
        parents=(),
        message="synthetic consume base\n",
    )
    result_tree = _tree(repo, _blob(repo, "synthetic-consume-result\n"), "result.txt")
    result_sha = _commit(
        repo,
        tree=result_tree,
        parents=(base_sha,),
        message="synthetic consume result\n",
    )
    tock_sha = _commit(
        repo,
        tree=result_tree,
        parents=(base_sha, result_sha),
        message=_canonical_message(
            task_id=_TASK_ID,
            origin_run_id=_ORIGIN_RUN_ID,
            promotion_attempt_id=_ATTEMPT_ID,
            expected_base_sha=base_sha,
            result_sha=result_sha,
        ),
    )
    _update_ref(repo, _MAIN_REF, tock_sha)
    _write_journal(runtime, base_sha=base_sha, result_sha=result_sha, tock_sha=tock_sha)
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
    start_sha = _commit(
        repo,
        tree=_tree_of(repo, tock_sha),
        parents=(tock_sha,),
        message=_start_message(parent_sha=tock_sha),
    )
    _update_ref(repo, _task_ref(), start_sha)
    return {
        "repo": repo,
        "runtime": runtime,
        "base_sha": base_sha,
        "result_sha": result_sha,
        "tock_sha": tock_sha,
        "head_sha": start_sha,
        "lock_ownership": _acquire_lock(runtime),
    }


def _prepare(world: dict[str, Any]) -> None:
    lifecycle.prepare_post_tick_continuation(
        repo=world["repo"],
        runtime=world["runtime"],
        main_ref=_MAIN_REF,
        continuation_origin=ContinuationOrigin.FINALIZED_PROMOTION_TICK,
        successor_task_id=_SUCCESSOR_TASK_ID,
        successor_origin_run_id=_SUCCESSOR_ORIGIN_RUN_ID,
        successor_promotion_attempt_id=_SUCCESSOR_ATTEMPT_ID,
        expected_continuation_head_sha=world["head_sha"],
        lock_ownership=world["lock_ownership"],
        prepared_at_utc=_PREPARED_AT,
    )


def _successor_tock(
    world: dict[str, Any],
    *,
    expected_base_sha: str | None = None,
) -> str:
    repo = world["repo"]
    bound_head = world["head_sha"]
    base_sha = bound_head if expected_base_sha is None else expected_base_sha
    result_tree = _tree(
        repo,
        _blob(repo, "synthetic-consume-successor-result\n"),
        "successor-result.txt",
    )
    successor_result_sha = _commit(
        repo,
        tree=result_tree,
        parents=(bound_head,),
        message="synthetic consume successor result\n",
    )
    tock_sha = _commit(
        repo,
        tree=result_tree,
        parents=(base_sha, successor_result_sha),
        message=_canonical_message(
            task_id=_SUCCESSOR_TASK_ID,
            origin_run_id=_SUCCESSOR_ORIGIN_RUN_ID,
            promotion_attempt_id=_SUCCESSOR_ATTEMPT_ID,
            expected_base_sha=base_sha,
            result_sha=successor_result_sha,
        ),
    )
    world["successor_result_sha"] = successor_result_sha
    world["successor_tock_sha"] = tock_sha
    world["successor_expected_base_sha"] = base_sha
    return tock_sha


def _noncanonical_successor_tip(
    world: dict[str, Any],
    defect: str,
) -> dict[str, str]:
    repo = world["repo"]
    result_tree = _tree(
        repo,
        _blob(repo, "synthetic-consume-successor-result\n"),
        "successor-result.txt",
    )
    successor_result_sha = _commit(
        repo,
        tree=result_tree,
        parents=(world["head_sha"],),
        message="synthetic consume successor result\n",
    )
    task_id = _SUCCESSOR_TASK_ID
    attempt_id = _SUCCESSOR_ATTEMPT_ID
    expected_base_sha = world["head_sha"]
    parent_1 = world["head_sha"]
    parent_2 = successor_result_sha
    tock_tree = result_tree
    if defect == "task_id":
        task_id = _FOREIGN_TASK_ID
    elif defect == "promotion_attempt":
        attempt_id = _FOREIGN_ATTEMPT_ID
    elif defect == "expected_base_parent":
        expected_base_sha = _commit(
            repo,
            tree=_tree(
                repo,
                _blob(repo, "synthetic-consume-foreign-base\n"),
                "foreign-base.txt",
            ),
            parents=(),
            message="synthetic consume foreign base\n",
        )
        parent_1 = expected_base_sha
    elif defect == "result_tree":
        tock_tree = _tree(
            repo,
            _blob(repo, "synthetic-consume-foreign-tock-tree\n"),
            "foreign-tock.txt",
        )
    elif defect == "parent_cardinality":
        pass
    else:
        raise AssertionError(defect)
    commit_parents: tuple[str, ...] = (
        (parent_1,) if defect == "parent_cardinality" else (parent_1, parent_2)
    )
    tock_sha = _commit(
        repo,
        tree=tock_tree,
        parents=commit_parents,
        message=_canonical_message(
            task_id=task_id,
            origin_run_id=_SUCCESSOR_ORIGIN_RUN_ID,
            promotion_attempt_id=attempt_id,
            expected_base_sha=expected_base_sha,
            result_sha=successor_result_sha,
        ),
    )
    world["successor_result_sha"] = successor_result_sha
    world["successor_tock_sha"] = tock_sha
    built = {
        "tock_sha": tock_sha,
        "result_sha": successor_result_sha,
        "result_tree": result_tree,
        "tock_tree": tock_tree,
        "task_id": task_id,
        "promotion_attempt_id": attempt_id,
        "expected_base_sha": expected_base_sha,
        "parent_1": parent_1,
    }
    if len(commit_parents) == 2:
        built["parent_2"] = parent_2
    return built


def _read_artifact(runtime: Path) -> dict[str, Any]:
    loaded = yaml.safe_load((runtime / _ARTIFACT_NAME).read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _write_artifact(runtime: Path, payload: dict[str, Any]) -> None:
    (runtime / _ARTIFACT_NAME).write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


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


def _mark_consumed(
    runtime: Path,
    *,
    successor_tock_sha: str,
    successor_promotion_attempt_id: str = _SUCCESSOR_ATTEMPT_ID,
    consumed_at_utc: str = _CONSUMED_AT,
) -> bytes:
    payload = _read_artifact(runtime)
    payload["status"] = "CONSUMED"
    payload["successor_promotion_attempt_id"] = successor_promotion_attempt_id
    payload["successor_tock_sha"] = successor_tock_sha
    payload["consumed_at_utc"] = consumed_at_utc
    payload["continuation_digest_sha256"] = binding_digest(_binding(payload))
    _write_artifact(runtime, payload)
    loaded = load_continuation_artifact(runtime / _ARTIFACT_NAME)
    assert loaded.status == "CONSUMED"
    return (runtime / _ARTIFACT_NAME).read_bytes()


def _runtime_files_except_artifact(runtime: Path) -> dict[str, bytes]:
    artifact = runtime / _ARTIFACT_NAME
    return {
        path.relative_to(runtime).as_posix(): path.read_bytes()
        for path in sorted(runtime.rglob("*"))
        if path.is_file() and path != artifact
    }


def _load_consume() -> Callable[..., Any]:
    try:
        module = importlib.import_module(
            "task_transition_runner.runner_post_tick_continuation"
        )
    except ModuleNotFoundError as exc:
        pytest.fail(f"Missing ordinary consume/CAS implementation: {exc}")
    consume = getattr(module, "consume_post_tick_continuation", None)
    if not callable(consume):
        pytest.fail(
            "Missing ordinary consume/CAS implementation: "
            "consume_post_tick_continuation"
        )
    signature = inspect.signature(consume)
    assert tuple(signature.parameters) == _CONSUME_PARAMETERS
    for parameter in signature.parameters.values():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for token in ("archive", "reopen", "replay"):
            assert token not in parameter.name
    return consume


def _consume_kwargs(
    world: dict[str, Any],
    **overrides: Any,
) -> dict[str, Any]:
    payload = {
        "repo": world["repo"],
        "runtime": world["runtime"],
        "main_ref": _MAIN_REF,
        "successor_task_id": _SUCCESSOR_TASK_ID,
        "successor_promotion_attempt_id": _SUCCESSOR_ATTEMPT_ID,
        "successor_tock_sha": world["successor_tock_sha"],
        "consumed_at_utc": _CONSUMED_AT,
        "lock_ownership": world["lock_ownership"],
    }
    payload.update(overrides)
    return payload


def _reason(operation: Callable[[], Any]) -> str:
    with pytest.raises(ContinuationError) as caught:
        operation()
    return caught.value.reason_code


def _assert_no_archive(runtime: Path) -> None:
    assert list(runtime.rglob("post_tick_continuations")) == []


def _assert_git_observation(calls: list[tuple[str, ...]]) -> None:
    assert calls == [("git", "rev-list", "--first-parent", _MAIN_REF)]
    for call in calls:
        assert "--all" not in call


def test_exact_successor_tock_consumes_authorized_artifact_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    authorized = _read_artifact(world["runtime"])
    digest = authorized["continuation_digest_sha256"]
    assert authorized["status"] == "AUTHORIZED"
    assert "successor_tock_sha" not in authorized
    assert authorized["expected_continuation_head_sha"] == world["head_sha"]
    assert world["head_sha"] != world["tock_sha"]
    assert authorized["commit_chain"][-1]["sha"] == world["head_sha"]
    assert authorized["commit_chain"][0]["parent_sha"] == world["tock_sha"]
    successor_tock_sha = _successor_tock(world)
    repo = world["repo"]
    assert world["successor_expected_base_sha"] == world["head_sha"]
    assert _git(repo, "rev-parse", f"{successor_tock_sha}^1") == world["head_sha"]
    assert (
        _git(repo, "rev-parse", f"{successor_tock_sha}^2")
        == world["successor_result_sha"]
    )
    assert _tree_of(repo, successor_tock_sha) == _tree_of(
        repo,
        world["successor_result_sha"],
    )
    message = _git(repo, "log", "-1", "--format=%B", successor_tock_sha)
    assert f"Task-ID: {_SUCCESSOR_TASK_ID}" in message
    assert f"Promotion-Attempt: {_SUCCESSOR_ATTEMPT_ID}" in message
    assert f"Expected-Base: {world['head_sha']}" in message
    assert f"Result-SHA: {world['successor_result_sha']}" in message
    _update_ref(repo, _MAIN_REF, successor_tock_sha)
    other_files = _runtime_files_except_artifact(world["runtime"])
    refs_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    calls: list[tuple[str, ...]] = []
    real_run = subprocess.run
    real_popen = subprocess.Popen

    def _capture(args: list[str] | tuple[str, ...]) -> None:
        if isinstance(args, (list, tuple)) and args and args[0] == "git":
            calls.append(tuple(str(part) for part in args))

    def _spy_run(
        args: list[str] | tuple[str, ...],
        **spawn_kwargs: Any,
    ) -> Any:
        _capture(args)
        return real_run(args, **spawn_kwargs)

    def _spy_popen(
        args: list[str] | tuple[str, ...],
        **spawn_kwargs: Any,
    ) -> Any:
        _capture(args)
        return real_popen(args, **spawn_kwargs)

    monkeypatch.setattr(subprocess, "run", _spy_run)
    monkeypatch.setattr(subprocess, "Popen", _spy_popen)
    consume = _load_consume()
    consumed = consume(**_consume_kwargs(world))
    monkeypatch.undo()
    assert isinstance(consumed, ContinuationContractView)
    assert consumed.status == "CONSUMED"
    mapping = consumed.to_mapping()
    assert mapping["successor_task_id"] == _SUCCESSOR_TASK_ID
    assert mapping["successor_promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
    assert mapping["successor_tock_sha"] == successor_tock_sha
    assert mapping["consumed_at_utc"] == _CONSUMED_AT
    assert mapping["continuation_digest_sha256"] == digest
    persisted = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert persisted.to_mapping() == mapping
    assert _runtime_files_except_artifact(world["runtime"]) == other_files
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == refs_before
    _assert_no_archive(world["runtime"])
    _assert_git_observation(calls)


def test_direct_predecessor_base_does_not_consume_despite_start_chain(
    tmp_path: Path,
) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    authorized = _read_artifact(world["runtime"])
    assert authorized["status"] == "AUTHORIZED"
    assert authorized["expected_continuation_head_sha"] == world["head_sha"]
    assert world["head_sha"] != world["tock_sha"]
    chain = authorized["commit_chain"]
    assert chain[-1]["sha"] == world["head_sha"]
    assert chain[0]["parent_sha"] == world["tock_sha"]
    before = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    successor_tock_sha = _successor_tock(
        world,
        expected_base_sha=world["tock_sha"],
    )
    repo = world["repo"]
    assert _git(repo, "rev-parse", f"{world['successor_result_sha']}^") == world["head_sha"]
    assert _git(repo, "rev-parse", f"{successor_tock_sha}^1") == world["tock_sha"]
    assert (
        _git(repo, "rev-parse", f"{successor_tock_sha}^2")
        == world["successor_result_sha"]
    )
    assert _tree_of(repo, successor_tock_sha) == _tree_of(
        repo,
        world["successor_result_sha"],
    )
    message = _git(repo, "log", "-1", "--format=%B", successor_tock_sha)
    assert f"Task-ID: {_SUCCESSOR_TASK_ID}" in message
    assert f"Promotion-Attempt: {_SUCCESSOR_ATTEMPT_ID}" in message
    assert f"Expected-Base: {world['tock_sha']}" in message
    assert f"Expected-Base: {world['head_sha']}" not in message
    assert f"Result-SHA: {world['successor_result_sha']}" in message
    _update_ref(repo, _MAIN_REF, successor_tock_sha)
    assert _git(repo, "rev-parse", _MAIN_REF) == successor_tock_sha
    consume = _load_consume()
    assert _reason(lambda: consume(**_consume_kwargs(world))) == _SUCCESSOR_MISMATCH
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    persisted = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert persisted.status == "AUTHORIZED"
    assert "successor_tock_sha" not in persisted.to_mapping()
    _assert_no_archive(world["runtime"])


def test_identical_retry_reproves_persisted_bytes_without_rewrite(
    tmp_path: Path,
) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    successor_tock_sha = _successor_tock(world)
    _update_ref(world["repo"], _MAIN_REF, successor_tock_sha)
    before = _mark_consumed(world["runtime"], successor_tock_sha=successor_tock_sha)
    consume = _load_consume()
    assert _reason(
        lambda: consume(
            **_consume_kwargs(world, consumed_at_utc=_CONSUMED_AT_LATER)
        )
    ) == _ALREADY_CONSUMED
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    reproof = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    mapping = reproof.to_mapping()
    assert reproof.status == "CONSUMED"
    assert mapping["successor_task_id"] == _SUCCESSOR_TASK_ID
    assert mapping["successor_promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
    assert mapping["successor_tock_sha"] == successor_tock_sha
    assert mapping["consumed_at_utc"] == _CONSUMED_AT
    _assert_no_archive(world["runtime"])


def test_consume_cas_does_not_swap_without_canonical_successor_tock(
    tmp_path: Path,
) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    successor_tock_sha = _successor_tock(world)
    _update_ref(world["repo"], _SIDE_REF, successor_tock_sha)
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == world["tock_sha"]
    assert _git(world["repo"], "rev-parse", _SIDE_REF) == successor_tock_sha
    before = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    consume = _load_consume()
    assert _reason(
        lambda: consume(
            **_consume_kwargs(world, successor_tock_sha=successor_tock_sha)
        )
    ) == _SUCCESSOR_MISMATCH
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    persisted = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert persisted.status == "AUTHORIZED"
    assert "successor_tock_sha" not in persisted.to_mapping()
    _assert_no_archive(world["runtime"])


@pytest.mark.parametrize(
    "defect",
    (
        "task_id",
        "promotion_attempt",
        "expected_base_parent",
        "result_tree",
        "parent_cardinality",
    ),
)
def test_noncanonical_direct_successor_tock_does_not_consume(
    tmp_path: Path,
    defect: str,
) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    built = _noncanonical_successor_tip(world, defect)
    tip = built["tock_sha"]
    _update_ref(world["repo"], _MAIN_REF, tip)
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == tip
    message = _git(world["repo"], "log", "-1", "--format=%B", tip)
    observed_tree = _tree_of(world["repo"], tip)
    artifact = _read_artifact(world["runtime"])
    bound_tock = artifact["origin"]["evidence"]["tock_sha"]
    bound_head = artifact["expected_continuation_head_sha"]
    assert bound_tock == world["tock_sha"]
    assert bound_head == world["head_sha"]
    assert bound_head != bound_tock
    assert observed_tree == built["tock_tree"]
    assert message.startswith("TOCK\n")
    assert f"Task-ID: {built['task_id']}" in message
    assert f"Promotion-Attempt: {built['promotion_attempt_id']}" in message
    assert f"Expected-Base: {built['expected_base_sha']}" in message
    assert f"Result-SHA: {built['result_sha']}" in message
    if defect == "parent_cardinality":
        observed_parents = (_git(world["repo"], "rev-parse", f"{tip}^1"),)
        with pytest.raises(subprocess.CalledProcessError):
            _git(world["repo"], "rev-parse", f"{tip}^2")
        assert observed_parents == (built["parent_1"],)
        assert "parent_2" not in built
        assert built["parent_1"] == bound_head
        assert built["expected_base_sha"] == bound_head
        assert built["task_id"] == _SUCCESSOR_TASK_ID
        assert built["promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
        assert built["tock_tree"] == built["result_tree"]
        assert built["result_sha"] not in observed_parents
    else:
        observed_parents = (
            _git(world["repo"], "rev-parse", f"{tip}^1"),
            _git(world["repo"], "rev-parse", f"{tip}^2"),
        )
        assert observed_parents == (built["parent_1"], built["parent_2"])
    if defect == "task_id":
        assert built["task_id"] == _FOREIGN_TASK_ID
        assert built["promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
        assert built["expected_base_sha"] == bound_head
        assert observed_parents == (bound_head, built["result_sha"])
        assert built["tock_tree"] == built["result_tree"]
    elif defect == "promotion_attempt":
        assert built["promotion_attempt_id"] == _FOREIGN_ATTEMPT_ID
        assert built["task_id"] == _SUCCESSOR_TASK_ID
        assert built["expected_base_sha"] == bound_head
        assert observed_parents == (bound_head, built["result_sha"])
        assert built["tock_tree"] == built["result_tree"]
    elif defect == "expected_base_parent":
        assert built["expected_base_sha"] != bound_head
        assert built["expected_base_sha"] != bound_tock
        assert observed_parents == (built["expected_base_sha"], built["result_sha"])
        assert observed_parents[0] != bound_head
        assert observed_parents[0] != bound_tock
        assert built["task_id"] == _SUCCESSOR_TASK_ID
        assert built["promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
        assert built["tock_tree"] == built["result_tree"]
    elif defect == "result_tree":
        assert built["tock_tree"] != built["result_tree"]
        assert observed_tree != _tree_of(world["repo"], built["result_sha"])
        assert observed_parents == (bound_head, built["result_sha"])
        assert built["expected_base_sha"] == bound_head
        assert built["task_id"] == _SUCCESSOR_TASK_ID
        assert built["promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
    elif defect == "parent_cardinality":
        assert len(observed_parents) == 1
    else:
        raise AssertionError(defect)
    requested = _consume_kwargs(world, successor_tock_sha=tip)
    assert requested["successor_task_id"] == _SUCCESSOR_TASK_ID
    assert requested["successor_promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
    assert requested["successor_tock_sha"] == tip
    before = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    consume = _load_consume()
    assert _reason(lambda: consume(**requested)) == _SUCCESSOR_MISMATCH
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    persisted = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert persisted.status == "AUTHORIZED"
    assert "successor_tock_sha" not in persisted.to_mapping()
    assert persisted.successor_task_id == _SUCCESSOR_TASK_ID
    _assert_no_archive(world["runtime"])


@pytest.mark.parametrize("axis", ("both", "attempt", "tock"))
def test_conflicting_consumed_reuse_blocks(tmp_path: Path, axis: str) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    successor_tock_sha = _successor_tock(world)
    _update_ref(world["repo"], _MAIN_REF, successor_tock_sha)
    before = _mark_consumed(world["runtime"], successor_tock_sha=successor_tock_sha)
    if axis == "both":
        attempt_id = _FOREIGN_ATTEMPT_ID
        requested_tock = "f" * 40
    elif axis == "attempt":
        attempt_id = _FOREIGN_ATTEMPT_ID
        requested_tock = successor_tock_sha
    elif axis == "tock":
        attempt_id = _SUCCESSOR_ATTEMPT_ID
        requested_tock = "f" * 40
    else:
        raise AssertionError(axis)
    assert (attempt_id != _SUCCESSOR_ATTEMPT_ID) or (requested_tock != successor_tock_sha)
    if axis == "attempt":
        assert requested_tock == successor_tock_sha
    if axis == "tock":
        assert attempt_id == _SUCCESSOR_ATTEMPT_ID
    consume = _load_consume()
    assert _reason(
        lambda: consume(
            **_consume_kwargs(
                world,
                successor_promotion_attempt_id=attempt_id,
                successor_tock_sha=requested_tock,
                consumed_at_utc=_CONSUMED_AT_LATER,
            )
        )
    ) == _CONSUMPTION_CONFLICT
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    reproof = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert reproof.to_mapping()["successor_tock_sha"] == successor_tock_sha
    assert reproof.to_mapping()["successor_promotion_attempt_id"] == _SUCCESSOR_ATTEMPT_ID
    _assert_no_archive(world["runtime"])


def test_lost_lock_ownership_blocks_before_consume_swap(tmp_path: Path) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    successor_tock_sha = _successor_tock(world)
    _update_ref(world["repo"], _MAIN_REF, successor_tock_sha)
    before = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    foreign = dict(world["lock_ownership"])
    foreign["pid"] = world["lock_ownership"]["pid"] + 1
    assert foreign["pid"] != world["lock_ownership"]["pid"]
    consume = _load_consume()
    assert _reason(
        lambda: consume(**_consume_kwargs(world, lock_ownership=foreign))
    ) == _LIFECYCLE_INVALID
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    persisted = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert persisted.status == "AUTHORIZED"
    assert "successor_tock_sha" not in persisted.to_mapping()
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == successor_tock_sha
    _assert_no_archive(world["runtime"])


@pytest.mark.parametrize("artifact_status", ("AUTHORIZED", "CONSUMED"))
def test_successor_task_mismatch_does_not_consume(
    tmp_path: Path,
    artifact_status: str,
) -> None:
    world = _seed(tmp_path)
    _prepare(world)
    successor_tock_sha = _successor_tock(world)
    _update_ref(world["repo"], _MAIN_REF, successor_tock_sha)
    if artifact_status == "CONSUMED":
        _mark_consumed(world["runtime"], successor_tock_sha=successor_tock_sha)
    elif artifact_status != "AUTHORIZED":
        raise AssertionError(artifact_status)
    before = (world["runtime"] / _ARTIFACT_NAME).read_bytes()
    consume = _load_consume()
    assert _reason(
        lambda: consume(
            **_consume_kwargs(world, successor_task_id=_FOREIGN_TASK_ID)
        )
    ) == _SUCCESSOR_MISMATCH
    assert (world["runtime"] / _ARTIFACT_NAME).read_bytes() == before
    persisted = load_continuation_artifact(world["runtime"] / _ARTIFACT_NAME)
    assert persisted.status == artifact_status
    assert persisted.successor_task_id == _SUCCESSOR_TASK_ID
    if artifact_status == "AUTHORIZED":
        assert "successor_tock_sha" not in persisted.to_mapping()
    else:
        assert persisted.to_mapping()["successor_tock_sha"] == successor_tock_sha
        assert (
            persisted.to_mapping()["successor_promotion_attempt_id"]
            == _SUCCESSOR_ATTEMPT_ID
        )
    _assert_no_archive(world["runtime"])
