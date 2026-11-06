"""Ordinary FINALIZED_PROMOTION_TICK origin proof.

OSS6-B14 accepts one predecessor only when a FINALIZED journal, the
matching consumption entry, settled main state, and one canonical
predecessor TOCK agree. The TOCK is the configured main ref's HEAD on
``git rev-list --first-parent``: subject ``TOCK``, ordered parents
``[base, result]``, and the result commit's tree. An older canonical
TOCK of another attempt on that chain remains history. Cross-source
identity drift blocks as ``CONTINUATION_IDENTITY_MISMATCH``, including
journal, main, and consumption result SHAs that drift together away
from the HEAD TOCK result SHA. Malformed sources, duplicate attempt
keys, and non-head geometry block as ``CONTINUATION_ORIGIN_INVALID``.
A disjoint foreign consumption entry does not shadow the matching
entry. Identical ledger duplicates are not a failure axis. Reopen,
archive, replay, and legacy origins are outside this contract.
"""

from __future__ import annotations

import inspect
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.runner_continuation_contract import (
    ContinuationError,
    ContinuationOrigin,
)
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
from task_transition_runner.runner_continuation_origin_proofs import (
    OriginProof,
    prove_finalized_promotion_tick,
)
import task_transition_runner.runner_continuation_origin_proofs as origin_proofs


_MAIN_REF = "refs/heads/main"
_SIDE_REF = "refs/heads/synthetic-side-diagnostic"
_TASK_ID = "task-synthetic-origin-001"
_FOREIGN_TASK_ID = "task-synthetic-origin-foreign"
_ORIGIN_RUN_ID = "origin-synthetic-origin-001"
_FOREIGN_ORIGIN_RUN_ID = "origin-synthetic-origin-foreign"
_ATTEMPT_ID = "attempt-synthetic-origin-001"
_FOREIGN_ATTEMPT_ID = "attempt-synthetic-origin-foreign"
_DISJOINT_ATTEMPT_ID = "attempt-synthetic-origin-disjoint"
_HISTORICAL_ATTEMPT_ID = "attempt-synthetic-origin-historical"
_COORDINATED_DRIFT_RESULT_SHA = "result-synthetic-origin-coordinated-drift"
_DRAIN_RUN_ID = "drain-synthetic-origin-001"
_PROMOTION_CLASS = "GREEN_PROMOTION"
_FOREIGN_PROMOTION_CLASS = "RED_TDD_PROMOTION"
_FOREIGN_RESULT_SHA = "result-synthetic-origin-foreign"
_DISJOINT_RESULT_SHA = "result-synthetic-origin-disjoint"
_FOREIGN_BASE_SHA = "base-synthetic-origin-foreign"
_DISJOINT_TOCK_SHA = "tock-synthetic-origin-disjoint"
_DUPLICATE_TOCK_SHA = "tock-synthetic-origin-duplicate"
_CANONICAL_TOCK_SUBJECT = "TOCK"
_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)
_IDENTITY_KEYS = (
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
)
_ENTRY_PARAMETERS = ("repo", "runtime", "main_ref")
_FORBIDDEN_PARAMETER_TOKENS = (
    "reopen",
    "legacy",
    "archive",
    "replay",
    "chronicle",
    "all_refs",
)
_DROPPED_SOURCE_MARKERS = (
    "prove_reopen_baseline_start_tick",
    "verify_reopen_main_state",
    "REOPEN_BASELINE_START_TICK",
    "CONTEXT_BOUND_LEGACY",
    "ReopenOriginLifecycleView",
    "gaia_chronicle",
    "hydra_post_drain",
    "dispatch_state.yaml",
    "load_archived_promotion_journal",
)
_SCHEMA_TASK_FINGERPRINT = "1" * 64
_SCHEMA_TASK_SNAPSHOT_SHA256 = "2" * 64
_SCHEMA_INSTRUCTION_SIGNATURE_SHA256 = "3" * 64
_STAMPS = {
    "PENDING": "2026-09-25T00:00:00Z",
    "APPLYING": "2026-09-25T00:01:00Z",
    "GIT_APPLIED": "2026-09-25T00:02:00Z",
    "RECONCILED": "2026-09-25T00:03:00Z",
    "FINALIZED": "2026-09-25T00:04:00Z",
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
    "GIT_AUTHOR_NAME": "Synthetic Origin",
    "GIT_AUTHOR_EMAIL": "origin@task-transition-runner.invalid",
    "GIT_AUTHOR_DATE": "2026-09-25T00:00:00Z",
    "GIT_COMMITTER_NAME": "Synthetic Origin",
    "GIT_COMMITTER_EMAIL": "origin@task-transition-runner.invalid",
    "GIT_COMMITTER_DATE": "2026-09-25T00:00:00Z",
}
_REAL_SUBPROCESS_RUN = subprocess.run
_ORIGIN_INVALID = "CONTINUATION_ORIGIN_INVALID"
_IDENTITY_MISMATCH = "CONTINUATION_IDENTITY_MISMATCH"


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


def _canonical_message(
    *,
    promotion_attempt_id: str,
    expected_base_sha: str,
    result_sha: str,
    task_id: str,
    origin_run_id: str,
    promotion_class: str,
) -> str:
    values = {
        "Promotion-Attempt": promotion_attempt_id,
        "Task-ID": task_id,
        "Origin-Run": origin_run_id,
        "Result-SHA": result_sha,
        "Promotion-Class": promotion_class,
        "Expected-Base": expected_base_sha,
    }
    trailers = "\n".join(
        f"{key}: {values[key]}" for key in _CANONICAL_TOCK_TRAILER_KEYS
    )
    return f"{_CANONICAL_TOCK_SUBJECT}\n\n{trailers}\n"


def _prepare_repo(tmp_path: Path) -> tuple[Path, Path, str]:
    repo = _init_repo(tmp_path / "repo")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    tree_oid = _tree(repo, _blob(repo, "synthetic-origin-base\n"), "base.txt")
    base_sha = _commit(
        repo,
        tree=tree_oid,
        parents=(),
        message="synthetic origin base\n",
    )
    _update_ref(repo, _MAIN_REF, base_sha)
    return repo, runtime, base_sha


def _result_commit(
    repo: Path,
    parent: str,
    *,
    payload: str,
    name: str,
) -> tuple[str, str]:
    tree_oid = _tree(repo, _blob(repo, payload), name)
    result_sha = _commit(
        repo,
        tree=tree_oid,
        parents=(parent,),
        message=f"synthetic origin result {name}\n",
    )
    return result_sha, tree_oid


def _tock_parents(
    parent_mode: str,
    *,
    base_sha: str,
    result_sha: str,
) -> tuple[str, ...]:
    if parent_mode == "canonical":
        return (base_sha, result_sha)
    if parent_mode == "swapped":
        return (result_sha, base_sha)
    if parent_mode == "one_parent":
        return (base_sha,)
    raise AssertionError(parent_mode)


def _write_journal(
    runtime: Path,
    *,
    base_sha: str,
    result_sha: str,
    validated_result_sha: str,
    tock_sha: str,
    phase: PromotionJournalPhase,
    task_id: str,
    origin_run_id: str,
    promotion_attempt_id: str,
    promotion_class: str,
) -> None:
    path = runtime / "promotion_journal.yaml"
    journal = create_promotion_journal(
        task_id=task_id,
        origin_run_id=origin_run_id,
        drain_run_id=_DRAIN_RUN_ID,
        promotion_attempt_id=promotion_attempt_id,
        promotion_class=promotion_class,
        base_main_sha=base_sha,
        result_sha=result_sha,
        validated_result_sha=validated_result_sha,
        task_fingerprint=_SCHEMA_TASK_FINGERPRINT,
        task_snapshot_sha256=_SCHEMA_TASK_SNAPSHOT_SHA256,
        instruction_signature_sha256=_SCHEMA_INSTRUCTION_SIGNATURE_SHA256,
        pending_at_utc=_STAMPS["PENDING"],
        reason_code="RECONCILIATION_COMPLETED",
    )
    persist_promotion_journal(path=path, journal=journal)
    current = journal
    target_index = _PHASES.index(phase)
    for next_phase in _PHASES[1 : target_index + 1]:
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


def _write_main(
    runtime: Path,
    *,
    result_sha: str,
    tock_sha: str,
    observed_main_head_sha: str,
    last_tock_sha: str,
    validated_result_sha: str,
    main_branch: str,
    state: str,
) -> None:
    write_main_state(
        main_state_path=runtime / "main_state.yaml",
        payload={
            "schema_version": "1.0",
            "state": state,
            "main_branch": main_branch,
            "validated_result_sha": validated_result_sha,
            "last_tock_sha": last_tock_sha,
            "observed_main_head_sha": observed_main_head_sha,
            "reason_code": "RECONCILIATION_COMPLETED",
            "updated_at_utc": _STAMPS["FINALIZED"],
        },
    )


def _seed_origin(
    tmp_path: Path,
    *,
    journal_task_id: str = _TASK_ID,
    journal_origin_run_id: str = _ORIGIN_RUN_ID,
    journal_attempt_id: str = _ATTEMPT_ID,
    journal_promotion_class: str = _PROMOTION_CLASS,
    journal_result_sha: str | None = None,
    journal_validated_result_sha: str | None = None,
    journal_base_sha: str | None = None,
    journal_tock_sha: str | None = None,
    journal_phase: PromotionJournalPhase = PromotionJournalPhase.FINALIZED,
    write_journal_file: bool = True,
    write_consumption: bool = True,
    consumption_result_sha: str | None = None,
    consumption_tock_sha: str | None = None,
    duplicate_attempt_keys: bool = False,
    disjoint_consumption: bool = False,
    main_observed: str | None = None,
    main_last_tock: str | None = None,
    main_validated: str | None = None,
    main_branch: str = "main",
    main_state_value: str = "GREEN_VALIDATED",
    place_tock_on_main: bool = True,
    parent_mode: str = "canonical",
    tree_mode: str = "result",
    duplicate_attempt: bool = False,
    tock_metadata: str = "canonical",
    main_geometry: str = "head",
    historical_other_attempt: bool = False,
    identical_consumption_duplicate: bool = False,
    coordinated_result_drift: bool = False,
) -> dict[str, Any]:
    repo, runtime, base_sha = _prepare_repo(tmp_path)
    current_base_sha = base_sha
    historical_tock_sha: str | None = None
    if historical_other_attempt:
        historical_result_sha, historical_tree = _result_commit(
            repo,
            base_sha,
            payload="synthetic-origin-historical-result\n",
            name="historical.txt",
        )
        historical_tock_sha = _commit(
            repo,
            tree=historical_tree,
            parents=(base_sha, historical_result_sha),
            message=_canonical_message(
                promotion_attempt_id=_HISTORICAL_ATTEMPT_ID,
                expected_base_sha=base_sha,
                result_sha=historical_result_sha,
                task_id=_TASK_ID,
                origin_run_id=_ORIGIN_RUN_ID,
                promotion_class=_PROMOTION_CLASS,
            ),
        )
        current_base_sha = historical_tock_sha
    result_sha, result_tree = _result_commit(
        repo,
        current_base_sha,
        payload="synthetic-origin-result\n",
        name="result.txt",
    )
    tock_tree = result_tree
    if tree_mode == "foreign":
        tock_tree = _tree(
            repo,
            _blob(repo, "synthetic-origin-foreign-tree\n"),
            "foreign.txt",
        )
    tock_sha = _commit(
        repo,
        tree=tock_tree,
        parents=_tock_parents(
            parent_mode,
            base_sha=current_base_sha,
            result_sha=result_sha,
        ),
        message=(
            "synthetic origin malformed metadata\n"
            if tock_metadata == "malformed"
            else _canonical_message(
                promotion_attempt_id=_ATTEMPT_ID,
                expected_base_sha=current_base_sha,
                result_sha=result_sha,
                task_id=_TASK_ID,
                origin_run_id=_ORIGIN_RUN_ID,
                promotion_class=_PROMOTION_CLASS,
            )
        ),
    )
    bound_result_sha = result_sha
    bound_base_sha = current_base_sha
    bound_tock_sha = tock_sha
    if duplicate_attempt:
        second_result_sha, second_tree = _result_commit(
            repo,
            tock_sha,
            payload="synthetic-origin-second-result\n",
            name="second.txt",
        )
        bound_tock_sha = _commit(
            repo,
            tree=second_tree,
            parents=(tock_sha, second_result_sha),
            message=_canonical_message(
                promotion_attempt_id=_ATTEMPT_ID,
                expected_base_sha=tock_sha,
                result_sha=second_result_sha,
                task_id=_TASK_ID,
                origin_run_id=_ORIGIN_RUN_ID,
                promotion_class=_PROMOTION_CLASS,
            ),
        )
        bound_result_sha = second_result_sha
        bound_base_sha = tock_sha
    if main_geometry == "older_ancestor":
        later_tree = _tree(
            repo,
            _blob(repo, "synthetic-origin-later\n"),
            "later.txt",
        )
        later_sha = _commit(
            repo,
            tree=later_tree,
            parents=(bound_tock_sha,),
            message="synthetic origin later main\n",
        )
        _update_ref(repo, _MAIN_REF, later_sha)
    elif main_geometry == "side_parent":
        tip_tree = _tree(
            repo,
            _blob(repo, "synthetic-origin-side-parent\n"),
            "side-parent.txt",
        )
        tip_sha = _commit(
            repo,
            tree=tip_tree,
            parents=(base_sha, bound_tock_sha),
            message="synthetic origin side parent tip\n",
        )
        _update_ref(repo, _MAIN_REF, tip_sha)
    elif place_tock_on_main:
        _update_ref(repo, _MAIN_REF, bound_tock_sha)
    else:
        _update_ref(repo, _SIDE_REF, bound_tock_sha)
    recorded_result = (
        journal_result_sha if journal_result_sha is not None else bound_result_sha
    )
    recorded_validated = (
        journal_validated_result_sha
        if journal_validated_result_sha is not None
        else recorded_result
    )
    recorded_base = journal_base_sha if journal_base_sha is not None else bound_base_sha
    recorded_tock = journal_tock_sha if journal_tock_sha is not None else bound_tock_sha
    consumed_result = (
        consumption_result_sha
        if consumption_result_sha is not None
        else bound_result_sha
    )
    settled_validated = (
        main_validated if main_validated is not None else bound_result_sha
    )
    if coordinated_result_drift:
        recorded_result = _COORDINATED_DRIFT_RESULT_SHA
        recorded_validated = bound_result_sha
        consumed_result = _COORDINATED_DRIFT_RESULT_SHA
        settled_validated = _COORDINATED_DRIFT_RESULT_SHA
    if write_journal_file:
        _write_journal(
            runtime,
            base_sha=recorded_base,
            result_sha=recorded_result,
            validated_result_sha=recorded_validated,
            tock_sha=recorded_tock,
            phase=journal_phase,
            task_id=journal_task_id,
            origin_run_id=journal_origin_run_id,
            promotion_attempt_id=journal_attempt_id,
            promotion_class=journal_promotion_class,
        )
    if write_consumption:
        record_promotion_consumed(
            promotion_consumed_path=runtime / "promotion_consumed.yaml",
            promotion_attempt_id=_ATTEMPT_ID,
            result_sha=consumed_result,
            main_state_before_promotion=_pre_state(base_sha),
            consumed_at_utc=_STAMPS["FINALIZED"],
            tock_sha=(
                consumption_tock_sha
                if consumption_tock_sha is not None
                else bound_tock_sha
            ),
        )
        if duplicate_attempt_keys:
            _append_consumption_entry(
                runtime,
                promotion_attempt_id=_ATTEMPT_ID,
                result_sha=bound_result_sha,
                tock_sha=_DUPLICATE_TOCK_SHA,
            )
        if disjoint_consumption:
            record_promotion_consumed(
                promotion_consumed_path=runtime / "promotion_consumed.yaml",
                promotion_attempt_id=_DISJOINT_ATTEMPT_ID,
                result_sha=_DISJOINT_RESULT_SHA,
                main_state_before_promotion=_pre_state(base_sha),
                consumed_at_utc=_STAMPS["FINALIZED"],
                tock_sha=_DISJOINT_TOCK_SHA,
            )
        if identical_consumption_duplicate:
            _append_identical_consumption_entry(runtime)
    _write_main(
        runtime,
        result_sha=bound_result_sha,
        tock_sha=bound_tock_sha,
        observed_main_head_sha=(
            main_observed if main_observed is not None else bound_tock_sha
        ),
        last_tock_sha=main_last_tock if main_last_tock is not None else bound_tock_sha,
        validated_result_sha=settled_validated,
        main_branch=main_branch,
        state=main_state_value,
    )
    return {
        "repo": repo,
        "runtime": runtime,
        "base_sha": base_sha,
        "result_sha": bound_result_sha,
        "tock_sha": bound_tock_sha,
        "historical_tock_sha": historical_tock_sha,
    }


def _append_consumption_entry(
    runtime: Path,
    *,
    promotion_attempt_id: str,
    result_sha: str,
    tock_sha: str,
) -> None:
    path = runtime / "promotion_consumed.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["consumed_promotions"].append(
        {
            "promotion_attempt_id": promotion_attempt_id,
            "result_sha": result_sha,
            "main_state_before_promotion": _pre_state("base-synthetic-origin-ledger"),
            "consumed_at_utc": _STAMPS["RECONCILED"],
            "tock_sha": tock_sha,
        }
    )
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _append_identical_consumption_entry(runtime: Path) -> None:
    path = runtime / "promotion_consumed.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    entries = payload["consumed_promotions"]
    assert len(entries) == 1
    entries.append(yaml.safe_load(yaml.safe_dump(entries[0])))
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _runtime_bytes(runtime: Path) -> dict[str, bytes]:
    return {
        path.relative_to(runtime).as_posix(): path.read_bytes()
        for path in sorted(runtime.rglob("*"))
        if path.is_file()
    }


def _ref_snapshot(repo: Path) -> str:
    return _git(
        repo,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
        "refs/heads",
    )


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


def _assert_origin_git(
    calls: list[tuple[str, ...]],
    *,
    main_ref: str,
    require_first_parent: bool,
) -> None:
    saw_first_parent = False
    for call in calls:
        assert "--all" not in call
        command = _normalize_git_command(call)
        assert command[:1] != ("for-each-ref",)
        assert command[:1] != ("show-ref",)
        assert command[:1] != ("log",)
        if command[:1] == ("rev-list",):
            assert "--first-parent" in command
            assert main_ref in command
            saw_first_parent = True
    if require_first_parent:
        assert saw_first_parent is True


def _assert_entry_signature() -> None:
    signature = inspect.signature(prove_finalized_promotion_tick)
    assert tuple(signature.parameters) == _ENTRY_PARAMETERS
    for name, parameter in signature.parameters.items():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty
        for token in _FORBIDDEN_PARAMETER_TOKENS:
            assert token not in name


def _call_origin(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo: Path,
    runtime: Path,
    require_first_parent: bool,
) -> OriginProof:
    _assert_entry_signature()
    before_files = _runtime_bytes(runtime)
    before_refs = _ref_snapshot(repo)
    before_objects = _object_names(repo)
    calls: list[tuple[str, ...]] = []

    def _spy(args: list[str] | tuple[str, ...], **spawn_kwargs: Any) -> Any:
        if isinstance(args, (list, tuple)) and args and args[0] == "git":
            calls.append(tuple(args))
        return _REAL_SUBPROCESS_RUN(args, **spawn_kwargs)

    failure: ContinuationError | None = None
    proof: OriginProof | None = None
    monkeypatch.setattr(subprocess, "run", _spy)
    try:
        candidate = prove_finalized_promotion_tick(
            repo=repo,
            runtime=runtime,
            main_ref=_MAIN_REF,
        )
        assert isinstance(candidate, OriginProof)
        proof = candidate
    except ContinuationError as exc:
        failure = exc
    finally:
        monkeypatch.undo()
    assert _runtime_bytes(runtime) == before_files
    assert _ref_snapshot(repo) == before_refs
    assert _object_names(repo) == before_objects
    _assert_origin_git(
        calls,
        main_ref=_MAIN_REF,
        require_first_parent=require_first_parent,
    )
    if failure is not None:
        raise failure
    assert proof is not None
    return proof


def _assert_blocked(
    monkeypatch: pytest.MonkeyPatch,
    world: dict[str, Any],
    reason_code: str,
    *,
    require_first_parent: bool,
) -> None:
    with pytest.raises(ContinuationError) as caught:
        _call_origin(
            monkeypatch,
            repo=world["repo"],
            runtime=world["runtime"],
            require_first_parent=require_first_parent,
        )
    assert caught.value.reason_code == reason_code


def test_matching_finalized_origin_proves_canonical_predecessor_tock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path)
    proof = _call_origin(
        monkeypatch,
        repo=world["repo"],
        runtime=world["runtime"],
        require_first_parent=True,
    )

    assert proof.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert proof.parent_sha == world["tock_sha"]
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == world["tock_sha"]
    assert set(proof.evidence) == {"predecessor_identity", "tock_sha"}
    identity = proof.evidence["predecessor_identity"]
    assert set(identity) == set(_IDENTITY_KEYS)
    assert dict(identity) == {
        "task_id": _TASK_ID,
        "origin_run_id": _ORIGIN_RUN_ID,
        "promotion_attempt_id": _ATTEMPT_ID,
        "result_sha": world["result_sha"],
        "promotion_class": _PROMOTION_CLASS,
    }
    assert proof.evidence["tock_sha"] == world["tock_sha"]
    thawed = proof.to_mapping()
    assert thawed == {
        "predecessor_identity": dict(identity),
        "tock_sha": world["tock_sha"],
    }
    thawed["predecessor_identity"]["task_id"] = _FOREIGN_TASK_ID
    assert proof.evidence["predecessor_identity"]["task_id"] == _TASK_ID
    with pytest.raises(TypeError):
        proof.evidence["tock_sha"] = _FOREIGN_RESULT_SHA  # type: ignore[index]
    with pytest.raises(TypeError):
        proof.evidence["predecessor_identity"]["task_id"] = _FOREIGN_TASK_ID  # type: ignore[index]


def test_missing_journal_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, write_journal_file=False)
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


def test_malformed_journal_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path)
    (world["runtime"] / "promotion_journal.yaml").write_text("[\n", encoding="utf-8")
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


def test_non_finalized_journal_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(
        tmp_path,
        journal_phase=PromotionJournalPhase.RECONCILED,
    )
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


def test_missing_consumption_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, write_consumption=False)
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


@pytest.mark.parametrize(
    ("kwargs",),
    [
        ({"journal_task_id": _FOREIGN_TASK_ID},),
        ({"journal_result_sha": _FOREIGN_RESULT_SHA},),
        ({"consumption_result_sha": _FOREIGN_RESULT_SHA},),
        ({"main_validated": _FOREIGN_RESULT_SHA},),
        ({"journal_tock_sha": "tock-synthetic-origin-foreign"},),
    ],
)
def test_identity_deviation_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, str],
) -> None:
    world = _seed_origin(tmp_path, **kwargs)
    _assert_blocked(
        monkeypatch,
        world,
        _IDENTITY_MISMATCH,
        require_first_parent=False,
    )


@pytest.mark.parametrize("parent_mode", ["swapped", "one_parent"])
def test_tock_parent_deviation_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    parent_mode: str,
) -> None:
    world = _seed_origin(tmp_path, parent_mode=parent_mode)
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


def test_tock_tree_deviation_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, tree_mode="foreign")
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


@pytest.mark.parametrize(
    ("kwargs",),
    [
        ({"main_observed": "head-synthetic-origin-foreign"},),
        ({"main_last_tock": "tock-synthetic-origin-foreign"},),
        ({"main_branch": "topic"},),
        ({"main_state_value": "EXPECTED_RED_OPEN"},),
    ],
)
def test_main_deviation_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, str],
) -> None:
    world = _seed_origin(tmp_path, **kwargs)
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


def test_side_ref_tock_does_not_prove_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, place_tock_on_main=False)
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == world["base_sha"]
    assert _git(world["repo"], "rev-parse", _SIDE_REF) == world["tock_sha"]
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=False,
    )


@pytest.mark.parametrize(
    ("kwargs",),
    [
        ({"journal_origin_run_id": _FOREIGN_ORIGIN_RUN_ID},),
        ({"journal_attempt_id": _FOREIGN_ATTEMPT_ID},),
        ({"journal_promotion_class": _FOREIGN_PROMOTION_CLASS},),
        ({"journal_base_sha": _FOREIGN_BASE_SHA},),
        ({"journal_validated_result_sha": _FOREIGN_RESULT_SHA},),
        ({"consumption_tock_sha": _DUPLICATE_TOCK_SHA},),
    ],
)
def test_cross_source_identity_deviation_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, str],
) -> None:
    world = _seed_origin(tmp_path, **kwargs)
    _assert_blocked(
        monkeypatch,
        world,
        _IDENTITY_MISMATCH,
        require_first_parent=True,
    )


@pytest.mark.parametrize(
    ("kwargs", "prepare"),
    [
        ({}, "malformed_consumption"),
        ({"duplicate_attempt_keys": True}, "ledger"),
        ({}, "malformed_main"),
        ({"tock_metadata": "malformed"}, "git"),
        ({"main_geometry": "side_parent"}, "git"),
        ({"main_geometry": "older_ancestor"}, "git"),
    ],
)
def test_malformed_or_geometric_origin_deviation_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, Any],
    prepare: str,
) -> None:
    world = _seed_origin(tmp_path, **kwargs)
    if prepare == "malformed_consumption":
        (world["runtime"] / "promotion_consumed.yaml").write_text(
            "[\n",
            encoding="utf-8",
        )
    elif prepare == "malformed_main":
        (world["runtime"] / "main_state.yaml").write_text("[\n", encoding="utf-8")
    elif prepare == "git":
        chain = _git(world["repo"], "rev-list", "--first-parent", _MAIN_REF).splitlines()
        assert world["tock_sha"] != chain[0] or kwargs.get("tock_metadata") == "malformed"
        if kwargs.get("main_geometry") == "side_parent":
            assert world["tock_sha"] not in chain
            parents = _git(
                world["repo"],
                "rev-list",
                "--parents",
                "-n",
                "1",
                _MAIN_REF,
            ).split()
            assert parents[2] == world["tock_sha"]
        if kwargs.get("main_geometry") == "older_ancestor":
            assert chain[0] != world["tock_sha"]
            assert world["tock_sha"] in chain
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=prepare == "git",
    )


def test_disjoint_consumption_history_keeps_matching_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, disjoint_consumption=True)
    proof = _call_origin(
        monkeypatch,
        repo=world["repo"],
        runtime=world["runtime"],
        require_first_parent=True,
    )
    assert proof.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert proof.parent_sha == world["tock_sha"]
    identity = proof.evidence["predecessor_identity"]
    assert identity["promotion_attempt_id"] == _ATTEMPT_ID
    assert identity["result_sha"] == world["result_sha"]
    assert proof.evidence["tock_sha"] == world["tock_sha"]


def test_duplicate_first_parent_attempt_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, duplicate_attempt=True)
    _assert_blocked(
        monkeypatch,
        world,
        _ORIGIN_INVALID,
        require_first_parent=True,
    )


def test_older_other_attempt_canonical_tock_keeps_head_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, historical_other_attempt=True)
    historical_tock_sha = world["historical_tock_sha"]
    assert isinstance(historical_tock_sha, str)
    chain = _git(world["repo"], "rev-list", "--first-parent", _MAIN_REF).splitlines()
    assert chain == [world["tock_sha"], historical_tock_sha, world["base_sha"]]
    historical_message = _git(world["repo"], "cat-file", "-p", historical_tock_sha)
    assert f"Promotion-Attempt: {_HISTORICAL_ATTEMPT_ID}" in historical_message
    assert _HISTORICAL_ATTEMPT_ID != _ATTEMPT_ID
    proof = _call_origin(
        monkeypatch,
        repo=world["repo"],
        runtime=world["runtime"],
        require_first_parent=True,
    )
    assert proof.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert proof.parent_sha == world["tock_sha"]
    identity = proof.evidence["predecessor_identity"]
    assert identity["promotion_attempt_id"] == _ATTEMPT_ID
    assert identity["result_sha"] == world["result_sha"]
    assert proof.evidence["tock_sha"] == world["tock_sha"]


def test_identical_consumption_duplicates_keep_matching_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, identical_consumption_duplicate=True)
    payload = yaml.safe_load(
        (world["runtime"] / "promotion_consumed.yaml").read_text(encoding="utf-8")
    )
    entries = payload["consumed_promotions"]
    assert len(entries) == 2
    assert entries[0] == entries[1]
    assert entries[0]["promotion_attempt_id"] == _ATTEMPT_ID
    assert entries[0]["result_sha"] == world["result_sha"]
    assert entries[0]["tock_sha"] == world["tock_sha"]
    proof = _call_origin(
        monkeypatch,
        repo=world["repo"],
        runtime=world["runtime"],
        require_first_parent=True,
    )
    assert proof.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert proof.parent_sha == world["tock_sha"]
    identity = proof.evidence["predecessor_identity"]
    assert identity["promotion_attempt_id"] == _ATTEMPT_ID
    assert identity["result_sha"] == world["result_sha"]
    assert proof.evidence["tock_sha"] == world["tock_sha"]


@pytest.mark.parametrize(
    ("deviation_field", "expected_reason"),
    [
        pytest.param("consumed_at_utc", None, id="ledger-time-is-not-identity"),
        pytest.param(
            "main_state_before_promotion",
            _ORIGIN_INVALID,
            id="pre-state-remains-bound-truth",
        ),
        pytest.param(
            "result_sha",
            _ORIGIN_INVALID,
            id="result-sha-remains-bound-truth",
        ),
    ],
)
def test_consumption_duplicate_identity_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    deviation_field: str,
    expected_reason: str | None,
) -> None:
    world = _seed_origin(tmp_path)
    path = world["runtime"] / "promotion_consumed.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    entries = payload["consumed_promotions"]
    assert len(entries) == 1
    original = entries[0]
    duplicate = yaml.safe_load(yaml.safe_dump(original))
    if deviation_field == "consumed_at_utc":
        duplicate["consumed_at_utc"] = _STAMPS["RECONCILED"]
    elif deviation_field == "main_state_before_promotion":
        duplicate["main_state_before_promotion"] = {
            **duplicate["main_state_before_promotion"],
            "state": "EXPECTED_RED_OPEN",
        }
    elif deviation_field == "result_sha":
        duplicate["result_sha"] = _FOREIGN_RESULT_SHA
    else:
        raise AssertionError(deviation_field)
    entries.append(duplicate)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )

    for field in (
        "promotion_attempt_id",
        "result_sha",
        "tock_sha",
        "main_state_before_promotion",
    ):
        if field == deviation_field:
            continue
        assert duplicate[field] == original[field]
    assert duplicate[deviation_field] != original[deviation_field]

    if expected_reason is not None:
        _assert_blocked(
            monkeypatch,
            world,
            expected_reason,
            require_first_parent=True,
        )
        return

    proof = _call_origin(
        monkeypatch,
        repo=world["repo"],
        runtime=world["runtime"],
        require_first_parent=True,
    )
    assert proof.origin is ContinuationOrigin.FINALIZED_PROMOTION_TICK
    assert proof.parent_sha == world["tock_sha"]
    assert proof.evidence["tock_sha"] == world["tock_sha"]


def test_coordinated_runtime_result_drift_blocks_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _seed_origin(tmp_path, coordinated_result_drift=True)
    journal = yaml.safe_load(
        (world["runtime"] / "promotion_journal.yaml").read_text(encoding="utf-8")
    )
    main_state = yaml.safe_load(
        (world["runtime"] / "main_state.yaml").read_text(encoding="utf-8")
    )
    consumed = yaml.safe_load(
        (world["runtime"] / "promotion_consumed.yaml").read_text(encoding="utf-8")
    )
    assert journal["result_sha"] == _COORDINATED_DRIFT_RESULT_SHA
    assert journal["validated_result_sha"] == world["result_sha"]
    assert main_state["validated_result_sha"] == _COORDINATED_DRIFT_RESULT_SHA
    assert (
        consumed["consumed_promotions"][0]["result_sha"]
        == _COORDINATED_DRIFT_RESULT_SHA
    )
    assert _COORDINATED_DRIFT_RESULT_SHA != world["result_sha"]
    _assert_blocked(
        monkeypatch,
        world,
        _IDENTITY_MISMATCH,
        require_first_parent=True,
    )


def test_origin_module_exports_no_dropped_private_symbols() -> None:
    source = Path(origin_proofs.__file__).read_text(encoding="utf-8")
    for marker in _DROPPED_SOURCE_MARKERS:
        assert marker not in source
    for name in dir(origin_proofs):
        if name.startswith("_"):
            continue
        lowered = name.lower()
        for token in _FORBIDDEN_PARAMETER_TOKENS:
            assert token not in lowered
