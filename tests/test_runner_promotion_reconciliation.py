"""First-parent observation contract for canonical TOCK authority.

OSS6-B02, OSS6-B03 and OSS6-B04 bind one observation entry point. Reachability
outside the configured ``main_ref`` is diagnosis. Effect authority exists only
for a single parseable TOCK on ``git rev-list --first-parent <main_ref>``.

The shared C0 parser ``parse_canonical_tock`` is the only metadata reader.
Duplicate Promotion-Attempt identity is counted before a commit is accepted
against the caller-supplied validated tree. Backup, recovery, task, worktree
and side-parent matches stay noncanonical. ``--all`` is not an observation
primitive.

The ordinary state matrix binds one convergence entry point for the supported
pre-TOCK and post-TOCK windows. It reads the existing pending, journal,
main-state and consumption contracts. It does not claim a cross-file
transaction, a reopen path, or a second Result or TOCK.

OSS6-B17 binds one successor TOCK to the existing continuation artifact.
An exact successor task, attempt and canonical TOCK binds that artifact
through a local, identity-bound compare-and-swap. An identical consumed
binding is an idempotent reproof of the same persisted truth. That binding
is not an exactly-once guarantee. A conflicting reuse or a different
successor blocks. Distributed or universal exactly-once remains forbidden.
No archive, reopen or replay helper is part of this contract.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.decision_contracts import (
    DecisionArtifactState,
    EvidenceReference,
    OrchestratorDecision,
    ValidatorDecision,
)
from task_transition_runner.runner_continuation_contract import (
    ContinuationError,
    binding_digest,
    load_continuation_artifact,
)
from task_transition_runner.runner_git_context import acquire_runner_git_lock
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    load_pending_promotion,
    parse_canonical_tock,
    persist_pending_promotion,
)
from task_transition_runner.runner_promotion_contracts import (
    QualifiedPromotionIntent,
    qualify_pre_candidate_intent,
)
from task_transition_runner.runner_promotion_journal import (
    PromotionJournal,
    PromotionJournalPhase,
    create_promotion_journal,
    load_promotion_journal,
    persist_promotion_journal,
    transition_promotion_journal,
)
from task_transition_runner.runner_promotion_state import (
    load_and_validate_main_state,
    load_promotion_consumed,
    record_promotion_consumed,
    write_main_state,
)
from task_transition_runner.task_contract import DirectGreenOutcome
from task_transition_runner.worker_result import (
    ValidationResult,
    WorkerReport,
    WorkerResult,
)
from task_transition_runner.runner_promotion_reconciliation import (
    FirstParentObservationStatus,
    FirstParentTockObservation,
    observe_canonical_first_parent_tock,
)

_MAIN_REF = "refs/heads/main"
_ATTEMPT_ID = "attempt-synthetic-first-parent-001"
_OTHER_ATTEMPT_ID = "attempt-synthetic-first-parent-002"
_TASK_ID = "task-synthetic-first-parent-001"
_OTHER_TASK_ID = "task-synthetic-first-parent-002"
_ORIGIN_RUN_ID = "origin-synthetic-first-parent-001"
_OTHER_ORIGIN_RUN_ID = "origin-synthetic-first-parent-002"
_SHARED_TREE_NAME = "shared.txt"
_SHARED_TREE_PAYLOAD = "synthetic-shared-validated-tree\n"
_PROMOTION_CLASS = "GREEN_PROMOTION"
_CANONICAL_TOCK_SUBJECT = "TOCK"
_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)
_DIAGNOSTIC_REF_KEYS = ("backup", "recovery", "task", "worktree")
_DIAGNOSTIC_REFS = {
    "backup": "refs/heads/synthetic-backup-diagnostic",
    "recovery": "refs/heads/synthetic-recovery-diagnostic",
    "task": "refs/heads/synthetic-task-diagnostic",
    "worktree": "refs/heads/synthetic-worktree-diagnostic",
}
_UNLISTED_REF = "refs/heads/synthetic-unlisted-lane"
_GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Synthetic Observation",
    "GIT_AUTHOR_EMAIL": "observation@task-transition-runner.invalid",
    "GIT_AUTHOR_DATE": "2026-09-22T00:00:00Z",
    "GIT_COMMITTER_NAME": "Synthetic Observation",
    "GIT_COMMITTER_EMAIL": "observation@task-transition-runner.invalid",
    "GIT_COMMITTER_DATE": "2026-09-22T00:00:00Z",
}
_REAL_SUBPROCESS_RUN = subprocess.run


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


def _first_parent_shas(repo: Path, ref: str) -> tuple[str, ...]:
    output = _git(repo, "rev-list", "--first-parent", ref)
    return tuple(line for line in output.splitlines() if line)


def _canonical_message(
    *,
    promotion_attempt_id: str,
    expected_base_sha: str,
    result_sha: str,
    task_id: str = _TASK_ID,
    origin_run_id: str = _ORIGIN_RUN_ID,
    promotion_class: str = _PROMOTION_CLASS,
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


def _materialize_tock(
    repo: Path,
    *,
    promotion_attempt_id: str,
    first_parent: str,
    tree_name: str,
    blob_payload: str,
    task_id: str = _TASK_ID,
    origin_run_id: str = _ORIGIN_RUN_ID,
    promotion_class: str = _PROMOTION_CLASS,
) -> dict[str, Any]:
    tree_oid = _tree(repo, _blob(repo, blob_payload), tree_name)
    result_sha = _commit(
        repo,
        tree=tree_oid,
        parents=(first_parent,),
        message=f"synthetic result {tree_name}\n",
    )
    message = _canonical_message(
        promotion_attempt_id=promotion_attempt_id,
        expected_base_sha=first_parent,
        result_sha=result_sha,
        task_id=task_id,
        origin_run_id=origin_run_id,
        promotion_class=promotion_class,
    )
    parents = (first_parent, result_sha)
    tock_sha = _commit(
        repo,
        tree=tree_oid,
        parents=parents,
        message=message,
    )
    return {
        "tock_sha": tock_sha,
        "result_sha": result_sha,
        "tree_oid": tree_oid,
        "parents": parents,
        "message": message,
        "promotion_attempt_id": promotion_attempt_id,
    }


def _root_commit(repo: Path) -> str:
    tree_oid = _tree(repo, _blob(repo, "synthetic-base\n"), "base.txt")
    return _commit(
        repo,
        tree=tree_oid,
        parents=(),
        message="synthetic base\n",
    )


def _ordinary_tip(repo: Path, parent: str) -> str:
    tree_oid = _tree(repo, _blob(repo, "synthetic-tip\n"), "tip.txt")
    return _commit(
        repo,
        tree=tree_oid,
        parents=(parent,),
        message="synthetic main tip\n",
    )


def _ref_snapshot(repo: Path) -> str:
    return _git(
        repo,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
        "refs/heads",
    )


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


def _assert_observation_primitive(calls: list[tuple[str, ...]], main_ref: str) -> None:
    normalized = [_normalize_git_command(call) for call in calls]
    rev_lists = [parts for parts in normalized if parts[:1] == ("rev-list",)]
    assert any(
        "--first-parent" in parts and main_ref in parts for parts in rev_lists
    )
    for parts in rev_lists:
        assert parts[0] == "rev-list"
        assert "--first-parent" in parts
        assert main_ref in parts
    for call in calls:
        assert "--all" not in call
        command = _normalize_git_command(call)
        assert command[:1] != ("for-each-ref",)
        assert command[:1] != ("show-ref",)


def _observe(
    monkeypatch: pytest.MonkeyPatch,
    **kwargs: Any,
) -> FirstParentTockObservation:
    calls: list[tuple[str, ...]] = []

    def _spy(args: list[str] | tuple[str, ...], **spawn_kwargs: Any) -> Any:
        if isinstance(args, (list, tuple)) and args and args[0] == "git":
            calls.append(tuple(args))
        return _REAL_SUBPROCESS_RUN(args, **spawn_kwargs)

    repo = Path(kwargs["repo"])
    main_ref = str(kwargs["main_ref"])
    before_refs = _ref_snapshot(repo)
    before_status = _git(repo, "status", "--porcelain")
    monkeypatch.setattr(subprocess, "run", _spy)
    observed = observe_canonical_first_parent_tock(**kwargs)
    monkeypatch.undo()
    assert _ref_snapshot(repo) == before_refs
    assert _git(repo, "status", "--porcelain") == before_status
    assert isinstance(observed, FirstParentTockObservation)
    assert observed.main_ref == main_ref
    _assert_observation_primitive(calls, main_ref)
    return observed


def _assert_blocked(
    observed: FirstParentTockObservation,
    reason_code: str,
) -> None:
    assert observed.status == FirstParentObservationStatus.BLOCKED
    assert observed.effect_authorized is False
    assert observed.reason_code == reason_code
    assert observed.tock_sha is None


def test_module_source_binds_shared_parser_and_first_parent_primitive() -> None:
    source_path = Path(observe_canonical_first_parent_tock.__code__.co_filename)
    source = source_path.read_text(encoding="utf-8")
    assert "parse_canonical_tock" in source
    assert "rev-list" in source
    assert "--first-parent" in source
    assert "--all" not in source


def test_one_first_parent_tock_is_the_only_effect_authority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path / "authorized")
    base_sha = _root_commit(repo)
    tock = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="result.txt",
        blob_payload="synthetic-result\n",
    )
    tip_sha = _ordinary_tip(repo, tock["tock_sha"])
    _update_ref(repo, _MAIN_REF, tip_sha)
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert tock["tock_sha"] in chain
    assert tock["result_sha"] not in chain
    parsed = parse_canonical_tock(
        tock["message"],
        tock["parents"],
        tock["tree_oid"],
        validated_tree_oid=tock["tree_oid"],
    )
    assert parsed["promotion_attempt_id"] == _ATTEMPT_ID
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=tock["tree_oid"],
    )
    assert observed.status == FirstParentObservationStatus.AUTHORIZED
    assert observed.effect_authorized is True
    assert observed.reason_code is None
    assert observed.tock_sha == tock["tock_sha"]
    assert observed.tock_sha != tip_sha


def test_absent_first_parent_tock_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path / "missing")
    base_sha = _root_commit(repo)
    tip_sha = _ordinary_tip(repo, base_sha)
    _update_ref(repo, _MAIN_REF, tip_sha)
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=_tree(repo, _blob(repo, "unused\n"), "unused.txt"),
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_MISSING")


def test_compact_subject_on_first_parent_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path / "compact")
    base_sha = _root_commit(repo)
    tree_oid = _tree(repo, _blob(repo, "compact\n"), "compact.txt")
    compact_sha = _commit(
        repo,
        tree=tree_oid,
        parents=(base_sha,),
        message=f"TOCK {_TASK_ID} {_ATTEMPT_ID} {base_sha}\n",
    )
    _update_ref(repo, _MAIN_REF, compact_sha)
    with pytest.raises(RunnerGitPromotionError) as exc_info:
        parse_canonical_tock(
            f"TOCK {_TASK_ID} {_ATTEMPT_ID} {base_sha}\n",
            (base_sha,),
            tree_oid,
            validated_tree_oid=tree_oid,
        )
    assert exc_info.value.reason_code == "PROMOTION_TOCK_METADATA_INVALID"
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=tree_oid,
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_MISSING")


@pytest.mark.parametrize("diagnostic_key", _DIAGNOSTIC_REF_KEYS)
def test_named_diagnostic_ref_match_is_noncanonical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    diagnostic_key: str,
) -> None:
    repo = _init_repo(tmp_path / diagnostic_key)
    base_sha = _root_commit(repo)
    tock = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="diagnostic.txt",
        blob_payload=f"synthetic-{diagnostic_key}\n",
    )
    _update_ref(repo, _MAIN_REF, base_sha)
    _update_ref(repo, _DIAGNOSTIC_REFS[diagnostic_key], tock["tock_sha"])
    assert tock["tock_sha"] not in _first_parent_shas(repo, _MAIN_REF)
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=tock["tree_oid"],
        diagnostic_refs={diagnostic_key: _DIAGNOSTIC_REFS[diagnostic_key]},
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_NONCANONICAL")


def test_side_parent_only_tock_is_noncanonical(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path / "side-parent")
    base_sha = _root_commit(repo)
    tock = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="side.txt",
        blob_payload="synthetic-side-parent\n",
    )
    carrier_tree = _tree(repo, _blob(repo, "carrier\n"), "carrier.txt")
    carrier_sha = _commit(
        repo,
        tree=carrier_tree,
        parents=(base_sha, tock["tock_sha"]),
        message="synthetic side-parent carrier\n",
    )
    _update_ref(repo, _MAIN_REF, carrier_sha)
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert chain[0] == carrier_sha
    assert tock["tock_sha"] not in chain
    assert _git(repo, "rev-parse", f"{carrier_sha}^2") == tock["tock_sha"]
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=tock["tree_oid"],
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_NONCANONICAL")


def test_unlisted_ref_match_stays_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path / "unlisted")
    base_sha = _root_commit(repo)
    tock = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="unlisted.txt",
        blob_payload="synthetic-unlisted\n",
    )
    _update_ref(repo, _MAIN_REF, base_sha)
    _update_ref(repo, _UNLISTED_REF, tock["tock_sha"])
    assert tock["tock_sha"] not in _first_parent_shas(repo, _MAIN_REF)
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=tock["tree_oid"],
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_MISSING")


def test_duplicate_attempt_blocks_before_tree_acceptance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path / "duplicate")
    base_sha = _root_commit(repo)
    first = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="first.txt",
        blob_payload="synthetic-duplicate-first\n",
    )
    second = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=first["tock_sha"],
        tree_name="second.txt",
        blob_payload="synthetic-duplicate-second\n",
    )
    _update_ref(repo, _MAIN_REF, second["tock_sha"])
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert first["tock_sha"] in chain
    assert second["tock_sha"] in chain
    assert first["parents"] != second["parents"]
    assert first["tree_oid"] != second["tree_oid"]
    parsed_first = parse_canonical_tock(
        first["message"],
        first["parents"],
        first["tree_oid"],
        validated_tree_oid=first["tree_oid"],
    )
    parsed_second_own_tree = parse_canonical_tock(
        second["message"],
        second["parents"],
        second["tree_oid"],
        validated_tree_oid=second["tree_oid"],
    )
    assert parsed_first["promotion_attempt_id"] == _ATTEMPT_ID
    assert parsed_second_own_tree["promotion_attempt_id"] == _ATTEMPT_ID
    with pytest.raises(RunnerGitPromotionError) as exc_info:
        parse_canonical_tock(
            second["message"],
            second["parents"],
            second["tree_oid"],
            validated_tree_oid=first["tree_oid"],
        )
    assert exc_info.value.reason_code == "PROMOTION_TOCK_TREE_MISMATCH"
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=first["tree_oid"],
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_DUPLICATE")
    assert observed.reason_code != "PROMOTION_TOCK_TREE_MISMATCH"
    assert observed.reason_code != "PROMOTION_TOCK_PARENT_MISMATCH"
    assert observed.reason_code != "RECONCILIATION_TOCK_TREE_MISMATCH"
    assert observed.reason_code != "RECONCILIATION_TOCK_PARENT_MISMATCH"


def test_two_tree_matched_envelopes_of_one_attempt_are_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path / "duplicate-matched-tree")
    base_sha = _root_commit(repo)
    first = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name=_SHARED_TREE_NAME,
        blob_payload=_SHARED_TREE_PAYLOAD,
    )
    second = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=first["tock_sha"],
        tree_name=_SHARED_TREE_NAME,
        blob_payload=_SHARED_TREE_PAYLOAD,
    )
    _update_ref(repo, _MAIN_REF, second["tock_sha"])
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert first["tree_oid"] == second["tree_oid"]
    assert first["tock_sha"] in chain
    assert second["tock_sha"] in chain
    assert first["parents"] != second["parents"]
    for envelope in (first, second):
        parsed = parse_canonical_tock(
            envelope["message"],
            envelope["parents"],
            envelope["tree_oid"],
            validated_tree_oid=first["tree_oid"],
        )
        assert parsed["promotion_attempt_id"] == _ATTEMPT_ID
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=first["tree_oid"],
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_DUPLICATE")


@pytest.mark.parametrize(
    ("task_id", "origin_run_id", "promotion_class"),
    (
        (_OTHER_TASK_ID, _ORIGIN_RUN_ID, _PROMOTION_CLASS),
        (_TASK_ID, _OTHER_ORIGIN_RUN_ID, _PROMOTION_CLASS),
        (_TASK_ID, _ORIGIN_RUN_ID, "RED_TDD_PROMOTION"),
    ),
)
def test_divergent_task_or_origin_keeps_the_same_attempt_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    task_id: str,
    origin_run_id: str,
    promotion_class: str,
) -> None:
    repo = _init_repo(tmp_path / f"duplicate-{task_id}-{origin_run_id}-{promotion_class}")
    base_sha = _root_commit(repo)
    first = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="identity-first.txt",
        blob_payload="synthetic-identity-first\n",
    )
    second = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=first["tock_sha"],
        tree_name="identity-second.txt",
        blob_payload="synthetic-identity-second\n",
        task_id=task_id,
        origin_run_id=origin_run_id,
        promotion_class=promotion_class,
    )
    _update_ref(repo, _MAIN_REF, second["tock_sha"])
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert first["tock_sha"] in chain
    assert second["tock_sha"] in chain
    parsed_first = parse_canonical_tock(
        first["message"],
        first["parents"],
        first["tree_oid"],
        validated_tree_oid=first["tree_oid"],
    )
    parsed_second = parse_canonical_tock(
        second["message"],
        second["parents"],
        second["tree_oid"],
        validated_tree_oid=second["tree_oid"],
    )
    assert parsed_first["promotion_attempt_id"] == _ATTEMPT_ID
    assert parsed_second["promotion_attempt_id"] == _ATTEMPT_ID
    assert parsed_second["task_id"] == task_id
    assert parsed_second["origin_run_id"] == origin_run_id
    assert parsed_second["promotion_class"] == promotion_class
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=first["tree_oid"],
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_DUPLICATE")


def test_other_attempt_on_the_same_chain_leaves_the_sought_attempt_authorized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path / "distinct-attempts")
    base_sha = _root_commit(repo)
    sought = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name="sought.txt",
        blob_payload="synthetic-sought-attempt\n",
    )
    other = _materialize_tock(
        repo,
        promotion_attempt_id=_OTHER_ATTEMPT_ID,
        first_parent=sought["tock_sha"],
        tree_name="other.txt",
        blob_payload="synthetic-other-attempt\n",
    )
    _update_ref(repo, _MAIN_REF, other["tock_sha"])
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert sought["tock_sha"] in chain
    assert other["tock_sha"] in chain
    parsed_sought = parse_canonical_tock(
        sought["message"],
        sought["parents"],
        sought["tree_oid"],
        validated_tree_oid=sought["tree_oid"],
    )
    parsed_other = parse_canonical_tock(
        other["message"],
        other["parents"],
        other["tree_oid"],
        validated_tree_oid=other["tree_oid"],
    )
    assert parsed_sought["promotion_attempt_id"] == _ATTEMPT_ID
    assert parsed_other["promotion_attempt_id"] == _OTHER_ATTEMPT_ID
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=sought["tree_oid"],
    )
    assert observed.status == FirstParentObservationStatus.AUTHORIZED
    assert observed.effect_authorized is True
    assert observed.reason_code is None
    assert observed.tock_sha == sought["tock_sha"]


def test_named_side_ref_keeps_the_first_parent_match_authorized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path / "side-plus-main")
    base_sha = _root_commit(repo)
    canonical = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name=_SHARED_TREE_NAME,
        blob_payload=_SHARED_TREE_PAYLOAD,
    )
    side = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name=_SHARED_TREE_NAME,
        blob_payload=_SHARED_TREE_PAYLOAD,
        task_id=_OTHER_TASK_ID,
    )
    _update_ref(repo, _MAIN_REF, canonical["tock_sha"])
    _update_ref(repo, _DIAGNOSTIC_REFS["backup"], side["tock_sha"])
    assert canonical["tree_oid"] == side["tree_oid"]
    assert canonical["tock_sha"] != side["tock_sha"]
    assert canonical["tock_sha"] in _first_parent_shas(repo, _MAIN_REF)
    assert side["tock_sha"] not in _first_parent_shas(repo, _MAIN_REF)
    parsed = parse_canonical_tock(
        canonical["message"],
        canonical["parents"],
        canonical["tree_oid"],
        validated_tree_oid=canonical["tree_oid"],
    )
    parsed_side = parse_canonical_tock(
        side["message"],
        side["parents"],
        side["tree_oid"],
        validated_tree_oid=canonical["tree_oid"],
    )
    assert parsed["promotion_attempt_id"] == _ATTEMPT_ID
    assert parsed_side["promotion_attempt_id"] == _ATTEMPT_ID
    assert parsed_side["task_id"] == _OTHER_TASK_ID
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=canonical["tree_oid"],
        diagnostic_refs={"backup": _DIAGNOSTIC_REFS["backup"]},
    )
    assert observed.status == FirstParentObservationStatus.AUTHORIZED
    assert observed.effect_authorized is True
    assert observed.reason_code is None
    assert observed.tock_sha == canonical["tock_sha"]


def test_multiple_side_only_matches_stay_noncanonical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path / "side-only-pair")
    base_sha = _root_commit(repo)
    backup = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name=_SHARED_TREE_NAME,
        blob_payload=_SHARED_TREE_PAYLOAD,
    )
    recovery = _materialize_tock(
        repo,
        promotion_attempt_id=_ATTEMPT_ID,
        first_parent=base_sha,
        tree_name=_SHARED_TREE_NAME,
        blob_payload=_SHARED_TREE_PAYLOAD,
        task_id=_OTHER_TASK_ID,
    )
    _update_ref(repo, _MAIN_REF, base_sha)
    _update_ref(repo, _DIAGNOSTIC_REFS["backup"], backup["tock_sha"])
    _update_ref(repo, _DIAGNOSTIC_REFS["recovery"], recovery["tock_sha"])
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert backup["tree_oid"] == recovery["tree_oid"]
    assert backup["tock_sha"] != recovery["tock_sha"]
    assert backup["tock_sha"] not in chain
    assert recovery["tock_sha"] not in chain
    for envelope in (backup, recovery):
        parsed = parse_canonical_tock(
            envelope["message"],
            envelope["parents"],
            envelope["tree_oid"],
            validated_tree_oid=backup["tree_oid"],
        )
        assert parsed["promotion_attempt_id"] == _ATTEMPT_ID
    observed = _observe(
        monkeypatch,
        repo=repo,
        main_ref=_MAIN_REF,
        promotion_attempt_id=_ATTEMPT_ID,
        validated_tree_oid=backup["tree_oid"],
        diagnostic_refs={
            "backup": _DIAGNOSTIC_REFS["backup"],
            "recovery": _DIAGNOSTIC_REFS["recovery"],
        },
    )
    _assert_blocked(observed, "RECONCILIATION_TOCK_NONCANONICAL")


_ORDINARY_ABSENT = (
    "ordinary reconciliation behavior is absent: reconcile_ordinary_promotion"
)
_ORDINARY_TASK_ID = "task-synthetic-ordinary-001"
_ORDINARY_ORIGIN_ID = "origin-synthetic-ordinary-001"
_ORDINARY_ATTEMPT_ID = "attempt-synthetic-ordinary-001"
_ORDINARY_FOREIGN_TASK_ID = "task-synthetic-ordinary-foreign"
_ORDINARY_FOREIGN_ORIGIN_ID = "origin-synthetic-ordinary-foreign"
_ORDINARY_FOREIGN_ATTEMPT_ID = "attempt-synthetic-ordinary-foreign"
_ORDINARY_FOREIGN_RESULT_SHA = "1234567890abcdef1234567890abcdef12345678"
_ORDINARY_PROVENANCE_AXES = (
    ("task_id", _ORDINARY_FOREIGN_TASK_ID),
    ("origin_run_id", _ORDINARY_FOREIGN_ORIGIN_ID),
    ("promotion_attempt_id", _ORDINARY_FOREIGN_ATTEMPT_ID),
    ("result_sha", _ORDINARY_FOREIGN_RESULT_SHA),
)
_ORDINARY_THIRD_HEAD_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_ORDINARY_PRIOR_DRAIN_ID = "drain-synthetic-ordinary-prior"
_ORDINARY_RECOVERY_DRAIN_ID = "drain-synthetic-ordinary-recovery"
_ORDINARY_PROMOTION_CLASS = "GREEN_PROMOTION"
_ORDINARY_ALLOWED_PATHS = ("src/example.py",)
_ORDINARY_COMMAND_ID = "ordinary-validation"
_ORDINARY_OUTPUT_REF = "runs/origin-synthetic-ordinary-001/validation/tests.log"
_ORDINARY_VALIDATION_LOG = b"synthetic ordinary validation\n"
_ORDINARY_OUTPUT_SHA256 = hashlib.sha256(_ORDINARY_VALIDATION_LOG).hexdigest()
_ORDINARY_FINGERPRINT = "a" * 64
_ORDINARY_TASK_DIGEST = "b" * 64
_ORDINARY_INSTRUCTION_DIGEST = "c" * 64
_ORDINARY_BINDING_SHA256 = "d" * 64
_ORDINARY_PENDING_NAME = "pending_promotion.yaml"
_ORDINARY_JOURNAL_NAME = "promotion_journal.yaml"
_ORDINARY_MAIN_STATE_NAME = "main_state.yaml"
_ORDINARY_CONSUMPTION_NAME = "promotion_consumed.yaml"
_ORDINARY_TASK_BRANCH = (
    "runner/task/task-synthetic-ordinary-001/origin-synthetic-ordinary-001"
)
_ORDINARY_STAMPS = {
    "PENDING": "2026-09-22T00:00:00Z",
    "APPLYING": "2026-09-22T00:01:00Z",
    "GIT_APPLIED": "2026-09-22T00:02:00Z",
    "RECONCILED": "2026-09-22T00:03:00Z",
    "FINALIZED": "2026-09-22T00:04:00Z",
}
_ORDINARY_PHASES = (
    PromotionJournalPhase.PENDING,
    PromotionJournalPhase.APPLYING,
    PromotionJournalPhase.GIT_APPLIED,
    PromotionJournalPhase.RECONCILED,
    PromotionJournalPhase.FINALIZED,
)
_ORDINARY_ENTRY_PARAMETERS = (
    "repo",
    "runtime_root",
    "drain_run_id",
    "main_ref",
    "diagnostic_refs",
)
_ORDINARY_FORBIDDEN_PARAMETER_TOKENS = (
    "reopen",
    "legacy",
    "archive",
    "all_refs",
    "chronicle",
)


def _ordinary_pre_state(base_sha: str) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": base_sha,
        "last_tock_sha": base_sha,
        "observed_main_head_sha": base_sha,
        "reason_code": "STATE_READY_SYNTHETIC",
        "updated_at_utc": _ORDINARY_STAMPS["PENDING"],
    }


def _ordinary_post_state(result_sha: str, tock_sha: str) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "state": "GREEN_VALIDATED",
        "main_branch": "main",
        "validated_result_sha": result_sha,
        "last_tock_sha": tock_sha,
        "observed_main_head_sha": tock_sha,
        "reason_code": "RECONCILIATION_COMPLETED",
        "updated_at_utc": _ORDINARY_STAMPS["FINALIZED"],
    }


def _ordinary_objects(repo: Path) -> tuple[str, ...]:
    output = _git(repo, "cat-file", "--batch-check", "--batch-all-objects")
    return tuple(sorted(line for line in output.splitlines() if line))


def _ordinary_file_bytes(runtime: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for path in sorted(runtime.rglob("*")):
        relative = str(path.relative_to(runtime))
        if path.is_file():
            files[relative] = path.read_bytes()
        elif path.is_dir():
            files[relative + "/"] = b"<dir>"
    return files


def _assert_reconcile_git(calls: list[tuple[str, ...]], main_ref: str) -> None:
    for call in calls:
        assert "--all" not in call
        command = _normalize_git_command(call)
        assert command[:1] != ("for-each-ref",)
        assert command[:1] != ("show-ref",)
        assert command[:1] != ("log",)
        if command[:1] == ("rev-list",):
            assert "--first-parent" in command
            assert main_ref in command


def _call_ordinary(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo: Path,
    runtime: Path,
    drain_run_id: str = _ORDINARY_RECOVERY_DRAIN_ID,
    diagnostic_refs: dict[str, str] | None = None,
) -> Any:
    before_objects = _ordinary_objects(repo)
    calls: list[tuple[str, ...]] = []

    def _spy(args: list[str] | tuple[str, ...], **spawn_kwargs: Any) -> Any:
        if isinstance(args, (list, tuple)) and args and args[0] == "git":
            calls.append(tuple(args))
        return _REAL_SUBPROCESS_RUN(args, **spawn_kwargs)

    monkeypatch.setattr(subprocess, "run", _spy)
    module = importlib.import_module(
        "task_transition_runner.runner_promotion_reconciliation"
    )
    reconcile = getattr(module, "reconcile_ordinary_promotion", None)
    verdict_type = getattr(module, "OrdinaryReconciliationVerdict", None)
    if not callable(reconcile) or verdict_type is None:
        pytest.fail(_ORDINARY_ABSENT)
    signature = inspect.signature(reconcile)
    assert tuple(signature.parameters) == _ORDINARY_ENTRY_PARAMETERS
    for parameter in signature.parameters.values():
        assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
        for token in _ORDINARY_FORBIDDEN_PARAMETER_TOKENS:
            assert token not in parameter.name
    verdict = reconcile(
        repo=repo,
        runtime_root=runtime,
        drain_run_id=drain_run_id,
        main_ref=_MAIN_REF,
        diagnostic_refs=diagnostic_refs,
    )
    monkeypatch.undo()
    _assert_reconcile_git(calls, _MAIN_REF)
    assert _ordinary_objects(repo) == before_objects
    assert isinstance(verdict, verdict_type)
    assert verdict.git_effect_created is False
    return verdict


def _assert_ordinary_verdict(
    verdict: Any,
    *,
    reason_code: str,
    phase: str | None,
    tock_sha: str | None,
    identity: tuple[str | None, str | None, str | None, str | None],
    drain_run_id: str | None,
) -> None:
    assert verdict.reason_code == reason_code
    assert verdict.journal_phase == phase
    assert verdict.tock_sha == tock_sha
    assert verdict.task_id == identity[0]
    assert verdict.origin_run_id == identity[1]
    assert verdict.promotion_attempt_id == identity[2]
    assert verdict.result_sha == identity[3]
    assert verdict.drain_run_id == drain_run_id


def _assert_blocked_ordinary(verdict: Any, reason_code: str) -> None:
    _assert_ordinary_verdict(
        verdict,
        reason_code=reason_code,
        phase=None,
        tock_sha=None,
        identity=(None, None, None, None),
        drain_run_id=None,
    )


def _materialize_result(repo: Path, base_sha: str) -> dict[str, str]:
    tree_oid = _tree(repo, _blob(repo, "synthetic-ordinary-result\n"), "result.txt")
    result_sha = _commit(
        repo,
        tree=tree_oid,
        parents=(base_sha,),
        message="synthetic ordinary result\n",
    )
    return {"result_sha": result_sha, "tree_oid": tree_oid}


def _materialize_matching_tock(
    repo: Path,
    *,
    base_sha: str,
    result_sha: str,
    tree_oid: str,
    task_id: str = _ORDINARY_TASK_ID,
    parents: tuple[str, ...] | None = None,
    tock_tree_oid: str | None = None,
) -> dict[str, Any]:
    message = _canonical_message(
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        expected_base_sha=base_sha,
        result_sha=result_sha,
        task_id=task_id,
        origin_run_id=_ORDINARY_ORIGIN_ID,
        promotion_class=_ORDINARY_PROMOTION_CLASS,
    )
    bound_parents = (base_sha, result_sha) if parents is None else parents
    bound_tree = tree_oid if tock_tree_oid is None else tock_tree_oid
    tock_sha = _commit(
        repo,
        tree=bound_tree,
        parents=bound_parents,
        message=message,
    )
    return {
        "tock_sha": tock_sha,
        "message": message,
        "parents": bound_parents,
        "tree_oid": bound_tree,
        "result_sha": result_sha,
    }


def _persist_ordinary_pending(
    runtime: Path,
    repo: Path,
    *,
    base_sha: str,
    result_sha: str,
    pre_state: dict[str, Any],
) -> dict[str, Any]:
    task_view = {
        "task_id": _ORDINARY_TASK_ID,
        "fingerprint": _ORDINARY_FINGERPRINT,
        "digest": _ORDINARY_TASK_DIGEST,
    }
    instruction_view = {
        "digest": _ORDINARY_INSTRUCTION_DIGEST,
        "bindings": (
            {
                "logical_ref": "./tasks/task-synthetic-ordinary-001.md",
                "snapshot_path": "instructions/task_spec",
                "sha256": _ORDINARY_BINDING_SHA256,
                "byte_length": 24,
            },
        ),
    }
    worker_result = WorkerResult(
        schema_version="1.0",
        result_type="worker_result",
        task_id=_ORDINARY_TASK_ID,
        origin_run_id=_ORDINARY_ORIGIN_ID,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        task_branch=_ORDINARY_TASK_BRANCH,
        result_sha=result_sha,
        changed_paths=_ORDINARY_ALLOWED_PATHS,
        validation_results=(
            ValidationResult(
                command_id=_ORDINARY_COMMAND_ID,
                exit_code=0,
                output_ref=_ORDINARY_OUTPUT_REF,
                output_sha256=_ORDINARY_OUTPUT_SHA256,
            ),
        ),
        worker_report=WorkerReport(
            reported_status="success",
            summary="synthetic ordinary result",
        ),
    )
    evidence = (
        EvidenceReference(kind="validation_log", ref=_ORDINARY_OUTPUT_REF),
    )
    orchestrator = OrchestratorDecision(
        schema_version="1.0",
        decision_type="orchestrator",
        task_id=_ORDINARY_TASK_ID,
        origin_run_id=_ORDINARY_ORIGIN_ID,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=result_sha,
        ready=True,
        promotion_class=_ORDINARY_PROMOTION_CLASS,
        evidence=evidence,
        rationale=None,
    )
    validator = ValidatorDecision(
        schema_version="1.0",
        decision_type="validator",
        task_id=_ORDINARY_TASK_ID,
        origin_run_id=_ORDINARY_ORIGIN_ID,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=result_sha,
        ready=True,
        verdict="GO",
        evidence=(
            EvidenceReference(kind="git_result", ref=result_sha, sha256=None),
            *evidence,
        ),
        rationale=None,
    )
    qualified = qualify_pre_candidate_intent(
        task_snapshot={"current": task_view, "bound": dict(task_view)},
        instruction_snapshot={
            "current": instruction_view,
            "bound": {
                "digest": instruction_view["digest"],
                "bindings": instruction_view["bindings"],
            },
        },
        worker_result=worker_result,
        task_branch_head_sha=result_sha,
        expected_outcome=DirectGreenOutcome(kind="GREEN"),
        allowed_paths=_ORDINARY_ALLOWED_PATHS,
        risk_facts={
            "secret_risk": False,
            "delete_risk": False,
            "scope_risk": False,
        },
        orchestrator_state=DecisionArtifactState.READY,
        orchestrator_decision=orchestrator,
        validator_state=DecisionArtifactState.READY,
        validator_decision=validator,
        main_state=pre_state,
        consumed_payload={"schema_version": "1.0", "consumed_promotions": []},
        lock_ownership={
            "ok": True,
            "status": "owned",
            "reason_code": "LOCK_OK",
            "owned": True,
            "ownership": {
                "schema_version": "1.0",
                "lock_name": "runner_git.lock",
                "run_id": _ORDINARY_ORIGIN_ID,
                "pid": 4242,
                "started_at_utc": _ORDINARY_STAMPS["PENDING"],
            },
        },
        git_preflight={"ok": True, "reason_code": "GIT_PREFLIGHT_OK"},
        red_lock=None,
        unresolved_foreign_epoch=False,
    )
    intent = qualified["intent"]
    assert isinstance(intent, QualifiedPromotionIntent)
    persisted = persist_pending_promotion(
        runtime_root=runtime,
        target_worktree=repo,
        intent=intent,
        task_snapshot={"current": task_view, "bound": dict(task_view)},
        instruction_snapshot={
            "current": instruction_view,
            "bound": {
                "digest": instruction_view["digest"],
                "bindings": instruction_view["bindings"],
            },
        },
        worker_result=worker_result,
        expected_outcome=DirectGreenOutcome(kind="GREEN"),
        orchestrator_decision=orchestrator,
        validator_decision=validator,
        base_sha=base_sha,
    )
    assert persisted["ok"] is True, persisted
    loaded = load_pending_promotion(runtime_root=runtime)
    assert loaded["task_id"] == _ORDINARY_TASK_ID
    assert loaded["origin_run_id"] == _ORDINARY_ORIGIN_ID
    assert loaded["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert loaded["result_sha"] == result_sha
    assert loaded["base_sha"] == base_sha
    assert loaded["promotion_class"] == _ORDINARY_PROMOTION_CLASS
    return loaded


class _AtomicReplaceFailure:
    """Fails only the atomic replace after the caller has read the target."""

    def __init__(self, real_os: Any) -> None:
        self._real_os = real_os

    def replace(self, source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        raise OSError("synthetic atomic replace failed")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real_os, name)


def _arm_atomic_replace_failure(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
) -> None:
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, "os", _AtomicReplaceFailure(module.os))


def _journal_provenance_kwargs(axis: str, foreign_value: str) -> dict[str, str]:
    if axis == "result_sha":
        return {"result_override": foreign_value}
    return {axis: foreign_value}


def _mutate_runtime_yaml(path: Path, mutator: Any) -> bytes:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutator(payload)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path.read_bytes()


def _drop_bound_fingerprint(payload: dict[str, Any]) -> None:
    del payload["task_snapshot"]["bound"]["fingerprint"]


def _drop_bound_task_digest(payload: dict[str, Any]) -> None:
    del payload["task_snapshot"]["bound"]["digest"]


def _drop_bound_instruction_digest(payload: dict[str, Any]) -> None:
    del payload["instruction_snapshot"]["bound"]["digest"]


def _malform_bound_fingerprint(payload: dict[str, Any]) -> None:
    payload["task_snapshot"]["bound"]["fingerprint"] = "z" * 64


def _replace_task_bound_with_text(payload: dict[str, Any]) -> None:
    payload["task_snapshot"]["bound"] = "not-a-mapping"


def _drop_journal_fingerprint(payload: dict[str, Any]) -> None:
    del payload["task_fingerprint"]


def _malform_journal_task_digest(payload: dict[str, Any]) -> None:
    payload["task_snapshot_sha256"] = "Z" * 64


def _blank_journal_instruction_signature(payload: dict[str, Any]) -> None:
    payload["instruction_signature_sha256"] = ""


def _write_journal_at_phase(
    runtime: Path,
    *,
    base_sha: str,
    result_sha: str,
    phase: PromotionJournalPhase,
    tock_sha: str | None,
    task_id: str = _ORDINARY_TASK_ID,
    origin_run_id: str = _ORDINARY_ORIGIN_ID,
    promotion_attempt_id: str = _ORDINARY_ATTEMPT_ID,
    result_override: str | None = None,
    task_fingerprint: str = _ORDINARY_FINGERPRINT,
    task_snapshot_sha256: str = _ORDINARY_TASK_DIGEST,
    instruction_signature_sha256: str = _ORDINARY_INSTRUCTION_DIGEST,
) -> PromotionJournal:
    path = runtime / _ORDINARY_JOURNAL_NAME
    bound_result = result_sha if result_override is None else result_override
    journal = create_promotion_journal(
        task_id=task_id,
        origin_run_id=origin_run_id,
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
        promotion_attempt_id=promotion_attempt_id,
        promotion_class=_ORDINARY_PROMOTION_CLASS,
        base_main_sha=base_sha,
        result_sha=bound_result,
        validated_result_sha=bound_result,
        task_fingerprint=task_fingerprint,
        task_snapshot_sha256=task_snapshot_sha256,
        instruction_signature_sha256=instruction_signature_sha256,
        pending_at_utc=_ORDINARY_STAMPS["PENDING"],
        reason_code="RECONCILIATION_COMPLETED",
    )
    persist_promotion_journal(path=path, journal=journal)
    current = journal
    target_index = _ORDINARY_PHASES.index(phase)
    for next_phase in _ORDINARY_PHASES[1 : target_index + 1]:
        current = transition_promotion_journal(
            current,
            phase=next_phase,
            drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
            transitioned_at_utc=_ORDINARY_STAMPS[next_phase.value],
            reason_code="RECONCILIATION_COMPLETED",
            tock_sha=(
                tock_sha if next_phase is PromotionJournalPhase.GIT_APPLIED else None
            ),
        )
        persist_promotion_journal(path=path, journal=current)
    loaded = load_promotion_journal(path=path)
    assert loaded.phase is phase
    assert loaded.task_id == task_id
    assert loaded.origin_run_id == origin_run_id
    assert loaded.promotion_attempt_id == promotion_attempt_id
    assert loaded.result_sha == bound_result
    assert loaded.validated_result_sha == bound_result
    return loaded


def _prepare_repo(tmp_path: Path) -> tuple[Path, Path, str]:
    repo = _init_repo(tmp_path / "repo")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    base_sha = _root_commit(repo)
    _update_ref(repo, _MAIN_REF, base_sha)
    return repo, runtime, base_sha


def _prepare_result(tmp_path: Path) -> tuple[Path, Path, str, str, str]:
    repo, runtime, base_sha = _prepare_repo(tmp_path)
    result = _materialize_result(repo, base_sha)
    return repo, runtime, base_sha, result["result_sha"], result["tree_oid"]


def _seed_pre_state(runtime: Path, base_sha: str) -> dict[str, Any]:
    return write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=_ordinary_pre_state(base_sha),
    )


def test_ordinary_empty_runtime_is_mutation_free_no_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, _base_sha = _prepare_repo(tmp_path)
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_NO_WORK")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_unbound_tock_without_runtime_is_no_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_NO_WORK")
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


@pytest.mark.parametrize(
    ("payload", "reason_code"),
    [
        ("[\n", "RECONCILIATION_PENDING_INVALID"),
        ("not: [\n", "RECONCILIATION_JOURNAL_INVALID"),
    ],
)
def test_ordinary_malformed_artifact_blocks_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: str,
    reason_code: str,
) -> None:
    repo, runtime, _base_sha = _prepare_repo(tmp_path)
    name = (
        _ORDINARY_PENDING_NAME
        if reason_code == "RECONCILIATION_PENDING_INVALID"
        else _ORDINARY_JOURNAL_NAME
    )
    target = runtime / name
    target.write_text(payload, encoding="utf-8")
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, reason_code)
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_ambiguous_pending_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    (runtime / "pending_promotion.yml").write_text("ambiguous\n", encoding="utf-8")
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_PENDING_INVALID")
    assert _ordinary_file_bytes(runtime) == before_files
    assert (runtime / _ORDINARY_JOURNAL_NAME).exists() is False


def test_ordinary_nonfinal_journal_without_pending_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_PENDING_INVALID")
    assert _ordinary_file_bytes(runtime) == before_files
    kept = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert kept.task_fingerprint == _ORDINARY_FINGERPRINT
    assert kept.task_snapshot_sha256 == _ORDINARY_TASK_DIGEST
    assert kept.instruction_signature_sha256 == _ORDINARY_INSTRUCTION_DIGEST


def test_ordinary_invalid_journal_is_not_reconstructed_from_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    (runtime / _ORDINARY_JOURNAL_NAME).write_text("[\n", encoding="utf-8")
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_JOURNAL_INVALID")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_pending_without_journal_reconstructs_only_pending_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    pending_bytes = (runtime / _ORDINARY_PENDING_NAME).read_bytes()
    state_bytes = (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes()
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="PENDING",
        tock_sha=None,
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    journal = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert journal.phase is PromotionJournalPhase.PENDING
    assert journal.tock_sha is None
    assert journal.task_id == pending["task_id"]
    assert journal.origin_run_id == pending["origin_run_id"]
    assert journal.promotion_attempt_id == pending["promotion_attempt_id"]
    assert journal.result_sha == pending["result_sha"]
    assert journal.validated_result_sha == pending["result_sha"]
    assert journal.base_main_sha == pending["base_sha"]
    assert journal.promotion_class == pending["promotion_class"]
    assert journal.drain_run_id == _ORDINARY_RECOVERY_DRAIN_ID
    assert journal.applying_at_utc is None
    assert journal.task_fingerprint == pending["task_snapshot"]["bound"]["fingerprint"]
    assert journal.task_snapshot_sha256 == pending["task_snapshot"]["bound"]["digest"]
    assert (
        journal.instruction_signature_sha256
        == pending["instruction_snapshot"]["bound"]["digest"]
    )
    assert journal.task_fingerprint == _ORDINARY_FINGERPRINT
    assert journal.task_snapshot_sha256 == _ORDINARY_TASK_DIGEST
    assert journal.instruction_signature_sha256 == _ORDINARY_INSTRUCTION_DIGEST
    snapshot_bytes = yaml.safe_dump(pending["task_snapshot"]).encode("utf-8")
    instruction_bytes = yaml.safe_dump(pending["instruction_snapshot"]).encode("utf-8")
    assert journal.task_fingerprint != hashlib.sha256(snapshot_bytes).hexdigest()
    assert journal.task_snapshot_sha256 != hashlib.sha256(snapshot_bytes).hexdigest()
    assert journal.instruction_signature_sha256 != hashlib.sha256(
        instruction_bytes
    ).hexdigest()
    journal_bytes = (runtime / _ORDINARY_JOURNAL_NAME).read_bytes()
    assert journal.task_fingerprint.encode("ascii") in journal_bytes
    assert journal.task_snapshot_sha256.encode("ascii") in journal_bytes
    assert journal.instruction_signature_sha256.encode("ascii") in journal_bytes
    assert (runtime / _ORDINARY_PENDING_NAME).read_bytes() == pending_bytes
    assert (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes() == state_bytes
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == base_sha


@pytest.mark.parametrize(
    "mutator",
    [
        _drop_bound_fingerprint,
        _drop_bound_task_digest,
        _drop_bound_instruction_digest,
        _malform_bound_fingerprint,
        _replace_task_bound_with_text,
    ],
)
def test_pending_without_journal_incomplete_anchor_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutator: Any,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    pending_path = runtime / _ORDINARY_PENDING_NAME
    pending_bytes = _mutate_runtime_yaml(pending_path, mutator)
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "JOURNAL_FIELDS_INVALID")
    assert (runtime / _ORDINARY_JOURNAL_NAME).exists() is False
    assert pending_path.read_bytes() == pending_bytes
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == base_sha


@pytest.mark.parametrize(
    "mutator",
    [
        _drop_journal_fingerprint,
        _malform_journal_task_digest,
        _blank_journal_instruction_signature,
    ],
)
def test_journal_without_pending_incomplete_anchor_is_not_completed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutator: Any,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    journal_path = runtime / _ORDINARY_JOURNAL_NAME
    journal_bytes = _mutate_runtime_yaml(journal_path, mutator)
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "JOURNAL_FIELDS_INVALID")
    assert journal_path.read_bytes() == journal_bytes
    assert _ordinary_file_bytes(runtime) == before_files
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == base_sha


@pytest.mark.parametrize(
    "override",
    [
        {"task_fingerprint": "e" * 64},
        {"task_snapshot_sha256": "e" * 64},
        {"instruction_signature_sha256": "e" * 64},
    ],
)
@pytest.mark.parametrize(
    "phase",
    (
        PromotionJournalPhase.PENDING,
        PromotionJournalPhase.APPLYING,
    ),
)
def test_a5_disagreed_pending_anchor_blocks_across_pre_effect_phases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    override: dict[str, str],
    phase: PromotionJournalPhase,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=phase,
        tock_sha=None,
        **override,
    )
    journal_path = runtime / _ORDINARY_JOURNAL_NAME
    journal_bytes = journal_path.read_bytes()
    pending_bytes = (runtime / _ORDINARY_PENDING_NAME).read_bytes()
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "JOURNAL_FIELD_DRIFT")
    assert journal_path.read_bytes() == journal_bytes
    assert (runtime / _ORDINARY_PENDING_NAME).read_bytes() == pending_bytes
    assert _ordinary_file_bytes(runtime) == before_files
    kept = load_promotion_journal(path=journal_path)
    field_name = next(iter(override))
    assert getattr(kept, field_name) == override[field_name]
    assert getattr(kept, field_name) != {
        "task_fingerprint": pending["task_snapshot"]["bound"]["fingerprint"],
        "task_snapshot_sha256": pending["task_snapshot"]["bound"]["digest"],
        "instruction_signature_sha256": pending["instruction_snapshot"]["bound"][
            "digest"
        ],
    }[field_name]
    assert kept.phase is phase


@pytest.mark.parametrize(
    "mutator",
    [
        _drop_bound_fingerprint,
        _drop_bound_task_digest,
        _drop_bound_instruction_digest,
    ],
)
def test_missing_pending_anchor_does_not_heal_existing_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutator: Any,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.APPLYING,
        tock_sha=None,
    )
    journal_path = runtime / _ORDINARY_JOURNAL_NAME
    journal_bytes = journal_path.read_bytes()
    pending_bytes = _mutate_runtime_yaml(runtime / _ORDINARY_PENDING_NAME, mutator)
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "JOURNAL_FIELDS_INVALID")
    assert journal_path.read_bytes() == journal_bytes
    assert (runtime / _ORDINARY_PENDING_NAME).read_bytes() == pending_bytes
    assert _ordinary_file_bytes(runtime) == before_files
    kept = load_promotion_journal(path=journal_path)
    assert kept.task_fingerprint == _ORDINARY_FINGERPRINT
    assert kept.task_snapshot_sha256 == _ORDINARY_TASK_DIGEST
    assert kept.instruction_signature_sha256 == _ORDINARY_INSTRUCTION_DIGEST
    assert kept.phase is PromotionJournalPhase.APPLYING


def test_malformed_pending_anchor_does_not_rewrite_existing_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    journal_path = runtime / _ORDINARY_JOURNAL_NAME
    journal_bytes = journal_path.read_bytes()
    pending_bytes = _mutate_runtime_yaml(
        runtime / _ORDINARY_PENDING_NAME,
        _malform_bound_fingerprint,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "JOURNAL_FIELD_DRIFT")
    assert journal_path.read_bytes() == journal_bytes
    assert (runtime / _ORDINARY_PENDING_NAME).read_bytes() == pending_bytes
    assert _ordinary_file_bytes(runtime) == before_files
    kept = load_promotion_journal(path=journal_path)
    assert kept.task_fingerprint == _ORDINARY_FINGERPRINT


@pytest.mark.parametrize(("axis", "foreign_value"), _ORDINARY_PROVENANCE_AXES)
def test_ordinary_pending_identity_mismatch_does_not_write_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    axis: str,
    foreign_value: str,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    pending_path = runtime / _ORDINARY_PENDING_NAME
    document = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
    document[axis] = foreign_value
    pending_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    loaded = load_pending_promotion(runtime_root=runtime)
    assert loaded[axis] == foreign_value
    assert loaded["orchestrator_decision"][axis] != foreign_value
    assert loaded["validator_decision"][axis] != foreign_value
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_IDENTITY_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_journal_write_failure_leaves_pending_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    pending = load_pending_promotion(runtime_root=runtime)
    state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert pending["task_id"] == _ORDINARY_TASK_ID
    assert pending["result_sha"] == result_sha
    assert state["observed_main_head_sha"] == base_sha
    assert (runtime / _ORDINARY_JOURNAL_NAME).exists() is False
    before_files = _ordinary_file_bytes(runtime)
    _arm_atomic_replace_failure(
        monkeypatch,
        "task_transition_runner.runner_promotion_journal",
    )
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_WRITE_FAILED")
    assert _ordinary_file_bytes(runtime) == before_files
    assert (runtime / _ORDINARY_JOURNAL_NAME).exists() is False


@pytest.mark.parametrize("phase_name", ["PENDING", "APPLYING"])
def test_ordinary_pre_tock_resume_keeps_the_phase_and_creates_no_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_name: str,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    phase = PromotionJournalPhase(phase_name)
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=phase,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase=phase_name,
        tock_sha=None,
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == base_sha


def test_ordinary_pre_tock_main_state_drift_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    drifted = dict(pre_state)
    drifted["observed_main_head_sha"] = _ORDINARY_THIRD_HEAD_SHA
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=drifted,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_named_side_tock_is_noncanonical_during_pre_tock_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.APPLYING,
        tock_sha=None,
    )
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, base_sha)
    _update_ref(repo, _DIAGNOSTIC_REFS["backup"], tock["tock_sha"])
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(
        monkeypatch,
        repo=repo,
        runtime=runtime,
        diagnostic_refs={"backup": _DIAGNOSTIC_REFS["backup"]},
    )
    _assert_blocked_ordinary(verdict, "RECONCILIATION_TOCK_NONCANONICAL")
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == base_sha


def test_ordinary_unlisted_side_tock_does_not_block_pre_tock_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, base_sha)
    _update_ref(repo, _UNLISTED_REF, tock["tock_sha"])
    _update_ref(repo, _DIAGNOSTIC_REFS["backup"], base_sha)
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(
        monkeypatch,
        repo=repo,
        runtime=runtime,
        diagnostic_refs={"backup": _DIAGNOSTIC_REFS["backup"]},
    )
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="PENDING",
        tock_sha=None,
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == base_sha


@pytest.mark.parametrize(
    "phase_name",
    ["PENDING", "APPLYING", "GIT_APPLIED", "RECONCILED"],
)
def test_ordinary_canonical_tock_finishes_bookkeeping_without_a_second_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_name: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    phase = PromotionJournalPhase(phase_name)
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=phase,
        tock_sha=(
            tock["tock_sha"] if phase_name in {"GIT_APPLIED", "RECONCILED"} else None
        ),
    )
    if phase_name == "RECONCILED":
        record_promotion_consumed(
            promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
            promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
            result_sha=result_sha,
            main_state_before_promotion=pre_state,
            consumed_at_utc=_ORDINARY_STAMPS["RECONCILED"],
            tock_sha=tock["tock_sha"],
        )
        write_main_state(
            main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
            payload=_ordinary_post_state(result_sha, tock["tock_sha"]),
        )
    consumption_before = (
        (runtime / _ORDINARY_CONSUMPTION_NAME).read_bytes()
        if (runtime / _ORDINARY_CONSUMPTION_NAME).is_file()
        else None
    )
    state_at_entry = (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes()
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=tock["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    finished = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert finished.phase is PromotionJournalPhase.FINALIZED
    assert finished.tock_sha == tock["tock_sha"]
    assert finished.task_id == journal.task_id == pending["task_id"]
    assert finished.origin_run_id == journal.origin_run_id
    assert finished.promotion_attempt_id == journal.promotion_attempt_id
    assert finished.result_sha == journal.result_sha == result_sha
    assert finished.base_main_sha == base_sha
    assert finished.pending_at_utc == journal.pending_at_utc
    assert finished.drain_run_id == _ORDINARY_RECOVERY_DRAIN_ID
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]
    if phase_name == "RECONCILED":
        assert (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes() == state_at_entry
        assert (runtime / _ORDINARY_CONSUMPTION_NAME).read_bytes() == consumption_before
    else:
        post_state = load_and_validate_main_state(
            main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
        )
        assert post_state["state"] == "GREEN_VALIDATED"
        assert post_state["validated_result_sha"] == result_sha
        assert post_state["last_tock_sha"] == tock["tock_sha"]
        assert post_state["observed_main_head_sha"] == tock["tock_sha"]
        assert post_state["reason_code"] == "RECONCILIATION_COMPLETED"
        assert state_at_entry != (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes()
    consumed = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    assert len(consumed["consumed_promotions"]) == 1
    entry = consumed["consumed_promotions"][0]
    assert entry["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert entry["result_sha"] == result_sha
    assert entry["tock_sha"] == tock["tock_sha"]
    assert entry["main_state_before_promotion"] == pre_state


def test_existing_first_parent_tock_converges_without_second_tock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    canonical = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    side = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
        task_id=_ORDINARY_FOREIGN_TASK_ID,
    )
    carrier_tree = _tree(
        repo,
        _blob(repo, "synthetic-side-parent\n"),
        "side-parent.txt",
    )
    carrier_sha = _commit(
        repo,
        tree=carrier_tree,
        parents=(base_sha, side["tock_sha"]),
        message="synthetic side-parent carrier\n",
    )
    side_ref = _DIAGNOSTIC_REFS["backup"]
    side_parent_ref = _UNLISTED_REF
    _update_ref(repo, _MAIN_REF, canonical["tock_sha"])
    _update_ref(repo, side_ref, side["tock_sha"])
    _update_ref(repo, side_parent_ref, carrier_sha)
    chain_before = _git(repo, "rev-list", "--first-parent", _MAIN_REF).splitlines()
    assert chain_before[0] == canonical["tock_sha"]
    assert side["tock_sha"] not in chain_before
    assert carrier_sha not in chain_before
    assert canonical["tock_sha"] != side["tock_sha"]
    assert _git(repo, "rev-parse", f"{carrier_sha}^2") == side["tock_sha"]
    parsed_side = parse_canonical_tock(
        side["message"],
        side["parents"],
        side["tree_oid"],
        validated_tree_oid=side["tree_oid"],
    )
    assert parsed_side["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert parsed_side["task_id"] == _ORDINARY_FOREIGN_TASK_ID
    subjects_before = [
        _git(repo, "log", "-1", "--format=%s", sha) for sha in chain_before
    ]
    assert subjects_before.count("TOCK") == 1
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.APPLYING,
        tock_sha=None,
    )
    assert journal.phase is PromotionJournalPhase.APPLYING
    assert journal.tock_sha is None
    assert pending["task_id"] == _ORDINARY_TASK_ID
    verdict = _call_ordinary(
        monkeypatch,
        repo=repo,
        runtime=runtime,
        diagnostic_refs={"backup": side_ref},
    )
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=canonical["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    assert verdict.git_effect_created is False
    assert verdict.tock_sha != side["tock_sha"]
    finished = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert finished.phase is PromotionJournalPhase.FINALIZED
    assert finished.tock_sha == canonical["tock_sha"]
    assert finished.promotion_attempt_id == journal.promotion_attempt_id
    chain_after = _git(repo, "rev-list", "--first-parent", _MAIN_REF).splitlines()
    assert chain_after == chain_before
    assert [
        _git(repo, "log", "-1", "--format=%s", sha) for sha in chain_after
    ].count("TOCK") == 1
    assert _git(repo, "rev-parse", _MAIN_REF) == canonical["tock_sha"]
    assert _git(repo, "rev-parse", side_ref) == side["tock_sha"]
    assert _git(repo, "rev-parse", side_parent_ref) == carrier_sha
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    post_state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert post_state["state"] == "GREEN_VALIDATED"
    assert post_state["validated_result_sha"] == result_sha
    assert post_state["last_tock_sha"] == canonical["tock_sha"]
    assert post_state["observed_main_head_sha"] == canonical["tock_sha"]
    assert post_state["reason_code"] == "RECONCILIATION_COMPLETED"
    consumed = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    assert len(consumed["consumed_promotions"]) == 1
    entry = consumed["consumed_promotions"][0]
    assert entry["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert entry["result_sha"] == result_sha
    assert entry["tock_sha"] == canonical["tock_sha"]
    assert entry["main_state_before_promotion"] == pre_state


def test_ordinary_canonical_tock_without_pending_finishes_from_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.APPLYING,
        tock_sha=None,
    )
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=tock["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    finished = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert finished.phase is PromotionJournalPhase.FINALIZED
    assert finished.tock_sha == tock["tock_sha"]
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    post_state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert post_state["observed_main_head_sha"] == tock["tock_sha"]
    assert post_state["validated_result_sha"] == result_sha
    consumed = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    assert consumed["consumed_promotions"][0]["main_state_before_promotion"] == pre_state


def test_ordinary_tock_metadata_mismatch_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
        task_id=_ORDINARY_FOREIGN_TASK_ID,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    parsed = parse_canonical_tock(
        tock["message"],
        tock["parents"],
        tock["tree_oid"],
        validated_tree_oid=tree_oid,
    )
    assert parsed["task_id"] == _ORDINARY_FOREIGN_TASK_ID
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_TOCK_METADATA_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_tock_parent_mismatch_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
        parents=(base_sha,),
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    with pytest.raises(RunnerGitPromotionError) as raised:
        parse_canonical_tock(
            tock["message"],
            tock["parents"],
            tock["tree_oid"],
            validated_tree_oid=tree_oid,
        )
    assert raised.value.reason_code == "PROMOTION_TOCK_PARENT_MISMATCH"
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_TOCK_PARENT_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_tock_tree_mismatch_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    foreign_tree = _tree(
        repo,
        _blob(repo, "synthetic-ordinary-foreign-tree\n"),
        "other.txt",
    )
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
        tock_tree_oid=foreign_tree,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    with pytest.raises(RunnerGitPromotionError) as raised:
        parse_canonical_tock(
            tock["message"],
            tock["parents"],
            tock["tree_oid"],
            validated_tree_oid=tree_oid,
        )
    assert raised.value.reason_code == "PROMOTION_TOCK_TREE_MISMATCH"
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_TOCK_TREE_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_post_tock_main_state_drift_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    drifted = dict(pre_state)
    drifted["observed_main_head_sha"] = _ORDINARY_THIRD_HEAD_SHA
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=drifted,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_consumption_conflict_blocks_without_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=_ORDINARY_FOREIGN_RESULT_SHA,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_ORDINARY_STAMPS["GIT_APPLIED"],
        tock_sha=tock["tock_sha"],
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_CONSUMPTION_CONFLICT")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_main_state_write_failure_does_not_advance_past_git_applied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    parsed = parse_canonical_tock(
        tock["message"],
        tock["parents"],
        tock["tree_oid"],
        validated_tree_oid=tree_oid,
    )
    state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert parsed["task_id"] == _ORDINARY_TASK_ID
    assert parsed["result_sha"] == result_sha
    assert journal.phase is PromotionJournalPhase.GIT_APPLIED
    assert journal.tock_sha == tock["tock_sha"]
    assert state["observed_main_head_sha"] == base_sha
    before_files = _ordinary_file_bytes(runtime)
    _arm_atomic_replace_failure(
        monkeypatch,
        "task_transition_runner.runner_promotion_state",
    )
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_WRITE_FAILED")
    assert _ordinary_file_bytes(runtime) == before_files
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


@pytest.mark.parametrize(("axis", "foreign_value"), _ORDINARY_PROVENANCE_AXES)
def test_ordinary_journal_provenance_mismatch_blocks_before_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    axis: str,
    foreign_value: str,
) -> None:
    repo, runtime, base_sha, result_sha, _tree_oid = _prepare_result(tmp_path)
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
        **_journal_provenance_kwargs(axis, foreign_value),
    )
    assert getattr(journal, axis) == foreign_value
    assert pending[axis] != foreign_value
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_IDENTITY_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files


def _finalized_pending_world(
    tmp_path: Path,
    *,
    with_consumed_continuation: bool,
) -> dict[str, Any]:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.FINALIZED,
        tock_sha=tock["tock_sha"],
    )
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=result_sha,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
        tock_sha=tock["tock_sha"],
    )
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=_ordinary_post_state(result_sha, tock["tock_sha"]),
    )
    if with_consumed_continuation:
        _write_continuation(
            runtime,
            _continuation_payload(
                successor_task_id=_ORDINARY_TASK_ID,
                status="CONSUMED",
                successor_promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
                successor_tock_sha=tock["tock_sha"],
            ),
        )
    return {
        "repo": repo,
        "runtime": runtime,
        "base_sha": base_sha,
        "result_sha": result_sha,
        "tock_sha": tock["tock_sha"],
        "pre_state": pre_state,
        "pending": pending,
        "journal": journal,
    }


def test_ordinary_finalized_reproof_is_byte_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _finalized_pending_world(
        tmp_path,
        with_consumed_continuation=True,
    )
    repo = world["repo"]
    runtime = world["runtime"]
    pending_path = runtime / _ORDINARY_PENDING_NAME
    assert pending_path.is_file()
    before_files = _ordinary_file_bytes(runtime)
    after_cleanup = dict(before_files)
    removed_pending = after_cleanup.pop(_ORDINARY_PENDING_NAME)
    assert removed_pending == pending_path.read_bytes()
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=world["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            world["result_sha"],
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    assert pending_path.exists() is False
    assert _ordinary_file_bytes(runtime) == after_cleanup
    assert _git(repo, "rev-parse", _MAIN_REF) == world["tock_sha"]

    stable_files = _ordinary_file_bytes(runtime)
    reproof = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        reproof,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=world["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            world["result_sha"],
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    assert _ordinary_file_bytes(runtime) == stable_files
    assert pending_path.exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == world["tock_sha"]


@pytest.mark.parametrize(
    ("pending_kind", "reason_code"),
    [
        ("invalid", "RECONCILIATION_PENDING_INVALID"),
        ("foreign", "RECONCILIATION_IDENTITY_MISMATCH"),
    ],
)
def test_ordinary_finalized_nonidentical_pending_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pending_kind: str,
    reason_code: str,
) -> None:
    world = _finalized_pending_world(
        tmp_path,
        with_consumed_continuation=True,
    )
    runtime = world["runtime"]
    pending_path = runtime / _ORDINARY_PENDING_NAME
    if pending_kind == "invalid":
        pending_path.write_text("[\n", encoding="utf-8")
    else:
        document = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
        for identity in (
            document,
            document["orchestrator_decision"],
            document["validator_decision"],
        ):
            identity["task_id"] = _ORDINARY_FOREIGN_TASK_ID
        pending_path.write_text(
            yaml.safe_dump(document, sort_keys=False),
            encoding="utf-8",
        )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_blocked_ordinary(verdict, reason_code)
    assert _ordinary_file_bytes(runtime) == before_files
    assert pending_path.is_file()


def test_ordinary_pending_drifted_after_entry_load_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _finalized_pending_world(
        tmp_path,
        with_consumed_continuation=True,
    )
    runtime = world["runtime"]
    pending_path = runtime / _ORDINARY_PENDING_NAME
    entry_bytes = pending_path.read_bytes()
    before_other = _ordinary_file_bytes(runtime)
    before_other.pop(_ORDINARY_PENDING_NAME)
    reconciliation = importlib.import_module(
        "task_transition_runner.runner_promotion_reconciliation"
    )
    real_load_pending = reconciliation.load_pending_promotion
    drifted = {"happened": False}

    def load_then_drift(*, runtime_root: Path | str) -> dict[str, Any]:
        loaded = real_load_pending(runtime_root=runtime_root)
        if not drifted["happened"]:
            document = yaml.safe_load(pending_path.read_text(encoding="utf-8"))
            document["task_branch"] = str(document["task_branch"]) + "-drifted"
            pending_path.write_text(
                yaml.safe_dump(document, sort_keys=False),
                encoding="utf-8",
            )
            drifted["happened"] = True
        return loaded

    monkeypatch.setattr(reconciliation, "load_pending_promotion", load_then_drift)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_blocked_ordinary(verdict, "RECONCILIATION_IDENTITY_MISMATCH")
    after_files = _ordinary_file_bytes(runtime)
    drifted_bytes = after_files.pop(_ORDINARY_PENDING_NAME)
    assert drifted["happened"] is True
    assert drifted_bytes != entry_bytes
    assert after_files == before_other
    assert pending_path.is_file()


def test_ordinary_finalized_pending_unlink_failure_is_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _finalized_pending_world(
        tmp_path,
        with_consumed_continuation=True,
    )
    runtime = world["runtime"]
    pending_path = runtime / _ORDINARY_PENDING_NAME
    before_files = _ordinary_file_bytes(runtime)
    original_unlink = Path.unlink

    def fail_pending_unlink(
        self: Path,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if self.resolve() == pending_path.resolve():
            raise OSError("synthetic pending unlink failure")
        original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_pending_unlink)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_blocked_ordinary(verdict, "RECONCILIATION_WRITE_FAILED")
    assert _ordinary_file_bytes(runtime) == before_files
    assert pending_path.is_file()
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == world["tock_sha"]


@pytest.mark.parametrize(("axis", "foreign_value"), _ORDINARY_PROVENANCE_AXES)
def test_ordinary_finalized_provenance_mismatch_does_not_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    axis: str,
    foreign_value: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.FINALIZED,
        tock_sha=tock["tock_sha"],
        **_journal_provenance_kwargs(axis, foreign_value),
    )
    parsed = parse_canonical_tock(
        tock["message"],
        tock["parents"],
        tock["tree_oid"],
        validated_tree_oid=tree_oid,
    )
    assert getattr(journal, axis) == foreign_value
    assert parsed[axis] != foreign_value
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=result_sha,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
        tock_sha=tock["tock_sha"],
    )
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=_ordinary_post_state(result_sha, tock["tock_sha"]),
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_IDENTITY_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files

def _write_consumption_entries(
    runtime: Path,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "consumed_promotions": entries,
    }
    (runtime / _ORDINARY_CONSUMPTION_NAME).write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )


def _consumption_entry(
    *,
    pre_state: dict[str, Any],
    result_sha: str,
    tock_sha: str,
    promotion_attempt_id: str = _ORDINARY_ATTEMPT_ID,
    consumed_at_utc: str = _ORDINARY_STAMPS["GIT_APPLIED"],
) -> dict[str, Any]:
    return {
        "promotion_attempt_id": promotion_attempt_id,
        "result_sha": result_sha,
        "main_state_before_promotion": pre_state,
        "consumed_at_utc": consumed_at_utc,
        "tock_sha": tock_sha,
    }


def test_ordinary_duplicate_attempt_blocks_before_base_parent_or_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    first = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    second_tree = _tree(
        repo,
        _blob(repo, "synthetic-ordinary-duplicate-envelope\n"),
        "duplicate.txt",
    )
    second_message = _canonical_message(
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        expected_base_sha=first["tock_sha"],
        result_sha=result_sha,
        task_id=_ORDINARY_TASK_ID,
        origin_run_id=_ORDINARY_ORIGIN_ID,
        promotion_class=_ORDINARY_PROMOTION_CLASS,
    )
    second_parents = (first["tock_sha"], result_sha)
    second_sha = _commit(
        repo,
        tree=second_tree,
        parents=second_parents,
        message=second_message,
    )
    _update_ref(repo, _MAIN_REF, second_sha)
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert first["tock_sha"] in chain
    assert second_sha in chain
    assert first["tock_sha"] != second_sha
    parsed_first = parse_canonical_tock(
        first["message"],
        first["parents"],
        first["tree_oid"],
        validated_tree_oid=first["tree_oid"],
    )
    parsed_second = parse_canonical_tock(
        second_message,
        second_parents,
        second_tree,
        validated_tree_oid=second_tree,
    )
    assert parsed_first["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert parsed_second["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert parsed_second["expected_base_sha"] == first["tock_sha"]
    with pytest.raises(RunnerGitPromotionError) as raised:
        parse_canonical_tock(
            second_message,
            second_parents,
            second_tree,
            validated_tree_oid=tree_oid,
        )
    assert raised.value.reason_code == "PROMOTION_TOCK_TREE_MISMATCH"
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_TOCK_DUPLICATE")
    assert verdict.reason_code != "RECONCILIATION_TOCK_PARENT_MISMATCH"
    assert verdict.reason_code != "RECONCILIATION_TOCK_TREE_MISMATCH"
    assert verdict.reason_code != "RECONCILIATION_TOCK_METADATA_MISMATCH"
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == second_sha


def test_ordinary_foreign_newer_tock_does_not_mask_older_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    older = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    foreign_tree = _tree(
        repo,
        _blob(repo, "synthetic-ordinary-foreign-attempt\n"),
        "foreign.txt",
    )
    foreign_result = _commit(
        repo,
        tree=foreign_tree,
        parents=(older["tock_sha"],),
        message="synthetic foreign result\n",
    )
    foreign_message = _canonical_message(
        promotion_attempt_id=_ORDINARY_FOREIGN_ATTEMPT_ID,
        expected_base_sha=older["tock_sha"],
        result_sha=foreign_result,
        task_id=_ORDINARY_FOREIGN_TASK_ID,
        origin_run_id=_ORDINARY_FOREIGN_ORIGIN_ID,
        promotion_class=_ORDINARY_PROMOTION_CLASS,
    )
    foreign_parents = (older["tock_sha"], foreign_result)
    foreign_sha = _commit(
        repo,
        tree=foreign_tree,
        parents=foreign_parents,
        message=foreign_message,
    )
    _update_ref(repo, _MAIN_REF, foreign_sha)
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert chain[0] == foreign_sha
    assert older["tock_sha"] in chain
    parsed_older = parse_canonical_tock(
        older["message"],
        older["parents"],
        older["tree_oid"],
        validated_tree_oid=tree_oid,
    )
    parsed_foreign = parse_canonical_tock(
        foreign_message,
        foreign_parents,
        foreign_tree,
        validated_tree_oid=foreign_tree,
    )
    assert parsed_older["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert parsed_foreign["promotion_attempt_id"] == _ORDINARY_FOREIGN_ATTEMPT_ID
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert verdict.reason_code != "RECONCILIATION_TOCK_METADATA_MISMATCH"
    assert verdict.tock_sha != foreign_sha
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == foreign_sha


def test_ordinary_invalid_newer_tock_does_not_mask_older_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    older = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    invalid_message = _canonical_message(
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        expected_base_sha=base_sha,
        result_sha=result_sha,
        task_id=_ORDINARY_TASK_ID,
        origin_run_id=_ORDINARY_ORIGIN_ID,
        promotion_class=_ORDINARY_PROMOTION_CLASS,
    )
    invalid_parents = (older["tock_sha"],)
    invalid_sha = _commit(
        repo,
        tree=tree_oid,
        parents=invalid_parents,
        message=invalid_message,
    )
    _update_ref(repo, _MAIN_REF, invalid_sha)
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert chain[0] == invalid_sha
    assert older["tock_sha"] in chain
    with pytest.raises(RunnerGitPromotionError) as raised:
        parse_canonical_tock(
            invalid_message,
            invalid_parents,
            tree_oid,
            validated_tree_oid=tree_oid,
        )
    assert raised.value.reason_code == "PROMOTION_TOCK_PARENT_MISMATCH"
    parsed_older = parse_canonical_tock(
        older["message"],
        older["parents"],
        older["tree_oid"],
        validated_tree_oid=tree_oid,
    )
    assert parsed_older["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert verdict.reason_code != "RECONCILIATION_TOCK_PARENT_MISMATCH"
    assert verdict.reason_code != "RECONCILIATION_TOCK_TREE_MISMATCH"
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == invalid_sha


def test_ordinary_matching_tock_behind_newer_main_commit_is_not_current_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    successor_tree = _tree(
        repo,
        _blob(repo, "synthetic-ordinary-later-main\n"),
        "later.txt",
    )
    successor_sha = _commit(
        repo,
        tree=successor_tree,
        parents=(tock["tock_sha"],),
        message="synthetic ordinary later main\n",
    )
    _update_ref(repo, _MAIN_REF, successor_sha)
    chain = _first_parent_shas(repo, _MAIN_REF)
    assert chain[0] == successor_sha
    assert tock["tock_sha"] in chain
    assert successor_sha != tock["tock_sha"]
    with pytest.raises(RunnerGitPromotionError) as raised:
        parse_canonical_tock(
            "synthetic ordinary later main\n",
            (tock["tock_sha"],),
            successor_tree,
            validated_tree_oid=successor_tree,
        )
    assert raised.value.reason_code == "PROMOTION_TOCK_METADATA_INVALID"
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.PENDING,
        tock_sha=None,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert verdict.reason_code != "RECONCILIATION_COMPLETED"
    assert _ordinary_file_bytes(runtime) == before_files
    state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert state["observed_main_head_sha"] == base_sha
    assert state["observed_main_head_sha"] != tock["tock_sha"]
    assert _git(repo, "rev-parse", _MAIN_REF) == successor_sha


def test_ordinary_journal_tock_sha_must_match_observed_tock_from_git_applied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=base_sha,
    )
    assert journal.phase is PromotionJournalPhase.GIT_APPLIED
    assert journal.tock_sha == base_sha
    assert journal.tock_sha != tock["tock_sha"]
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_IDENTITY_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files
    persisted = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert persisted.phase is PromotionJournalPhase.GIT_APPLIED
    assert persisted.tock_sha == base_sha
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


def test_ordinary_consumption_checks_every_ledger_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    loaded = _write_consumption_entries(
        runtime,
        [
            _consumption_entry(
                pre_state=pre_state,
                result_sha=result_sha,
                tock_sha=tock["tock_sha"],
            ),
            _consumption_entry(
                pre_state=pre_state,
                result_sha=_ORDINARY_FOREIGN_RESULT_SHA,
                tock_sha=tock["tock_sha"],
                consumed_at_utc=_ORDINARY_STAMPS["RECONCILED"],
            ),
        ],
    )
    assert loaded["consumed_promotions"][0]["result_sha"] == result_sha
    assert (
        loaded["consumed_promotions"][1]["result_sha"]
        == _ORDINARY_FOREIGN_RESULT_SHA
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_CONSUMPTION_CONFLICT")
    assert _ordinary_file_bytes(runtime) == before_files


def test_ordinary_consumption_checks_bound_main_state_before_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    divergent_pre_state = dict(pre_state)
    divergent_pre_state["reason_code"] = "STATE_DIVERGENT_SYNTHETIC"
    loaded = _write_consumption_entries(
        runtime,
        [
            _consumption_entry(
                pre_state=divergent_pre_state,
                result_sha=result_sha,
                tock_sha=tock["tock_sha"],
            ),
        ],
    )
    entry = loaded["consumed_promotions"][0]
    assert entry["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert entry["result_sha"] == result_sha
    assert entry["tock_sha"] == tock["tock_sha"]
    assert (
        entry["main_state_before_promotion"]
        != pending["main_state_before_promotion"]
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_CONSUMPTION_CONFLICT")
    assert _ordinary_file_bytes(runtime) == before_files


@pytest.mark.parametrize(
    "field_name",
    ["validated_result_sha", "last_tock_sha"],
)
def test_ordinary_head_on_tock_still_requires_result_and_last_tock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    partial = _ordinary_post_state(result_sha, tock["tock_sha"])
    partial[field_name] = base_sha
    assert partial["observed_main_head_sha"] == tock["tock_sha"]
    required = result_sha if field_name == "validated_result_sha" else tock["tock_sha"]
    assert partial[field_name] != required
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=partial,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


@pytest.mark.parametrize(
    ("field_name", "foreign_value"),
    [
        ("state", "EXPECTED_RED_OPEN"),
        ("main_branch", "foreign-main"),
    ],
)
def test_ordinary_matching_sha_truth_still_requires_semantic_main_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    foreign_value: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    partial = _ordinary_post_state(result_sha, tock["tock_sha"])
    partial[field_name] = foreign_value
    assert partial["observed_main_head_sha"] == tock["tock_sha"]
    assert partial["validated_result_sha"] == result_sha
    assert partial["last_tock_sha"] == tock["tock_sha"]
    assert partial[field_name] != _ordinary_post_state(result_sha, tock["tock_sha"])[
        field_name
    ]
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=partial,
    )
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


@pytest.mark.parametrize("phase_name", ["RECONCILED", "FINALIZED"])
@pytest.mark.parametrize(
    ("field_name", "foreign_value"),
    [
        ("state", "EXPECTED_RED_OPEN"),
        ("main_branch", "foreign-main"),
    ],
)
def test_ordinary_late_phase_matching_sha_truth_still_requires_semantic_main_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_name: str,
    field_name: str,
    foreign_value: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    phase = PromotionJournalPhase(phase_name)
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=phase,
        tock_sha=tock["tock_sha"],
    )
    partial = _ordinary_post_state(result_sha, tock["tock_sha"])
    partial[field_name] = foreign_value
    assert partial["observed_main_head_sha"] == tock["tock_sha"]
    assert partial["validated_result_sha"] == result_sha
    assert partial["last_tock_sha"] == tock["tock_sha"]
    assert partial[field_name] != _ordinary_post_state(result_sha, tock["tock_sha"])[
        field_name
    ]
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=partial,
    )
    loaded = _write_consumption_entries(
        runtime,
        [
            _consumption_entry(
                pre_state=pre_state,
                result_sha=result_sha,
                tock_sha=tock["tock_sha"],
                consumed_at_utc=_ORDINARY_STAMPS[phase_name],
            ),
        ],
    )
    entry = loaded["consumed_promotions"][0]
    assert journal.phase is phase
    assert journal.tock_sha == tock["tock_sha"]
    assert entry["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert entry["result_sha"] == result_sha
    assert entry["tock_sha"] == tock["tock_sha"]
    assert entry["main_state_before_promotion"] == pre_state
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_MAIN_STATE_DRIFT")
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


@pytest.mark.parametrize("phase_name", ["PENDING", "APPLYING"])
def test_ordinary_early_journal_write_failure_stops_before_state_and_consumption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_name: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    phase = PromotionJournalPhase(phase_name)
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=phase,
        tock_sha=None,
    )
    state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert journal.phase is phase
    assert journal.tock_sha is None
    assert journal.base_main_sha == base_sha
    assert state["observed_main_head_sha"] == base_sha
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]
    before_files = _ordinary_file_bytes(runtime)
    _arm_atomic_replace_failure(
        monkeypatch,
        "task_transition_runner.runner_promotion_journal",
    )
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_WRITE_FAILED")
    assert _ordinary_file_bytes(runtime) == before_files
    persisted = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert persisted.phase is phase
    assert persisted.tock_sha is None
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


def test_ordinary_journal_follow_up_persist_failure_is_write_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=_ordinary_post_state(result_sha, tock["tock_sha"]),
    )
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=result_sha,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_ORDINARY_STAMPS["GIT_APPLIED"],
        tock_sha=tock["tock_sha"],
    )
    state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert journal.tock_sha == tock["tock_sha"]
    assert state["observed_main_head_sha"] == tock["tock_sha"]
    assert state["validated_result_sha"] == result_sha
    assert state["last_tock_sha"] == tock["tock_sha"]
    before_files = _ordinary_file_bytes(runtime)
    _arm_atomic_replace_failure(
        monkeypatch,
        "task_transition_runner.runner_promotion_journal",
    )
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_WRITE_FAILED")
    assert _ordinary_file_bytes(runtime) == before_files
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


def test_ordinary_consumption_replace_failure_is_write_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=_ordinary_post_state(result_sha, tock["tock_sha"]),
    )
    state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert journal.phase is PromotionJournalPhase.GIT_APPLIED
    assert journal.tock_sha == tock["tock_sha"]
    assert state["observed_main_head_sha"] == tock["tock_sha"]
    assert state["validated_result_sha"] == result_sha
    assert state["last_tock_sha"] == tock["tock_sha"]
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    before_files = _ordinary_file_bytes(runtime)
    _arm_atomic_replace_failure(
        monkeypatch,
        "task_transition_runner.runner_promotion_state",
    )
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_WRITE_FAILED")
    assert _ordinary_file_bytes(runtime) == before_files
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


@pytest.mark.parametrize("phase_name", ["RECONCILED", "FINALIZED"])
def test_ordinary_later_journal_tock_sha_drift_blocks_without_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_name: str,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    phase = PromotionJournalPhase(phase_name)
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=phase,
        tock_sha=base_sha,
    )
    write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=_ordinary_post_state(result_sha, tock["tock_sha"]),
    )
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=result_sha,
        main_state_before_promotion=pre_state,
        consumed_at_utc=_ORDINARY_STAMPS[phase_name],
        tock_sha=tock["tock_sha"],
    )
    assert journal.phase is phase
    assert journal.tock_sha == base_sha
    assert journal.tock_sha != tock["tock_sha"]
    before_files = _ordinary_file_bytes(runtime)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "RECONCILIATION_IDENTITY_MISMATCH")
    assert _ordinary_file_bytes(runtime) == before_files
    persisted = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert persisted.phase is phase
    assert persisted.tock_sha == base_sha
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]


def test_ordinary_disjoint_consumption_entry_is_kept_and_current_promotion_finalizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    pending = _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    foreign_pre_state = dict(pre_state)
    foreign_pre_state["validated_result_sha"] = _ORDINARY_FOREIGN_RESULT_SHA
    foreign_pre_state["last_tock_sha"] = _ORDINARY_THIRD_HEAD_SHA
    foreign_pre_state["observed_main_head_sha"] = _ORDINARY_THIRD_HEAD_SHA
    foreign_pre_state["reason_code"] = "STATE_FOREIGN_SYNTHETIC"
    foreign_entry = _consumption_entry(
        pre_state=foreign_pre_state,
        result_sha=_ORDINARY_FOREIGN_RESULT_SHA,
        tock_sha=_ORDINARY_THIRD_HEAD_SHA,
        promotion_attempt_id=_ORDINARY_FOREIGN_ATTEMPT_ID,
        consumed_at_utc=_ORDINARY_STAMPS["PENDING"],
    )
    assert foreign_entry["promotion_attempt_id"] != _ORDINARY_ATTEMPT_ID
    assert foreign_entry["result_sha"] != result_sha
    assert foreign_entry["tock_sha"] != tock["tock_sha"]
    loaded = _write_consumption_entries(runtime, [foreign_entry])
    assert loaded["consumed_promotions"] == [foreign_entry]
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=tock["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    finished = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert finished.phase is PromotionJournalPhase.FINALIZED
    assert finished.tock_sha == tock["tock_sha"]
    consumed = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    assert len(consumed["consumed_promotions"]) == 2
    assert consumed["consumed_promotions"][0] == foreign_entry
    current_entry = consumed["consumed_promotions"][1]
    assert current_entry["promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert current_entry["result_sha"] == result_sha
    assert current_entry["tock_sha"] == tock["tock_sha"]
    assert (
        current_entry["main_state_before_promotion"]
        == pending["main_state_before_promotion"]
    )
    post_state = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert post_state["observed_main_head_sha"] == tock["tock_sha"]
    assert post_state["validated_result_sha"] == result_sha
    assert post_state["last_tock_sha"] == tock["tock_sha"]
    assert _git(repo, "rev-parse", _MAIN_REF) == tock["tock_sha"]
    after_first = _ordinary_file_bytes(runtime)
    consumption_bytes = (runtime / _ORDINARY_CONSUMPTION_NAME).read_bytes()
    reproof = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        reproof,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=tock["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    assert _ordinary_file_bytes(runtime) == after_first
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).read_bytes() == consumption_bytes
    reproof_ledger = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    assert len(reproof_ledger["consumed_promotions"]) == 2
    assert reproof_ledger["consumed_promotions"][0] == foreign_entry
    assert reproof_ledger["consumed_promotions"][1] == current_entry


_CONTINUATION_NAME = "post_tick_continuation.yaml"
_CONTINUATION_PREDECESSOR_TASK_ID = "task-synthetic-predecessor-001"
_CONTINUATION_PREDECESSOR_ORIGIN_ID = "origin-synthetic-predecessor-001"
_CONTINUATION_PREDECESSOR_ATTEMPT_ID = "attempt-synthetic-predecessor-001"
_CONTINUATION_PREDECESSOR_RESULT_SHA = "a" * 40
_CONTINUATION_PREDECESSOR_TOCK = "b" * 40
_CONTINUATION_TICK = "c" * 40
_CONTINUATION_PREPARED_AT = "2026-09-27T14:00:00Z"
_CONTINUATION_CONSUMED_AT = "2026-09-27T15:00:00Z"
_CONTINUATION_CONSUMED_AT_LATER = "2026-09-27T15:05:00Z"
_CONTINUATION_LOCK_RUN_ID = "run-synthetic-consume-001"
_CONTINUATION_LOCK_STARTED_AT = "2026-09-27T13:59:00Z"
_CONTINUATION_LOCK_NOW = "2026-09-27T14:00:00Z"
_CONTINUATION_ALREADY_CONSUMED = "CONTINUATION_ALREADY_CONSUMED"
_CONTINUATION_CONSUMPTION_CONFLICT = "CONTINUATION_CONSUMPTION_CONFLICT"
_CONTINUATION_SUCCESSOR_MISMATCH = "CONTINUATION_SUCCESSOR_MISMATCH"
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


def _continuation_binding(payload: dict[str, Any]) -> dict[str, Any]:
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


def _continuation_payload(
    *,
    successor_task_id: str,
    status: str = "AUTHORIZED",
    successor_promotion_attempt_id: str | None = None,
    successor_tock_sha: str | None = None,
    consumed_at_utc: str = _CONTINUATION_CONSUMED_AT,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "2.0",
        "origin": {
            "kind": "FINALIZED_PROMOTION_TICK",
            "evidence": {
                "predecessor_identity": {
                    "task_id": _CONTINUATION_PREDECESSOR_TASK_ID,
                    "origin_run_id": _CONTINUATION_PREDECESSOR_ORIGIN_ID,
                    "promotion_attempt_id": _CONTINUATION_PREDECESSOR_ATTEMPT_ID,
                    "result_sha": _CONTINUATION_PREDECESSOR_RESULT_SHA,
                    "promotion_class": _ORDINARY_PROMOTION_CLASS,
                },
                "tock_sha": _CONTINUATION_PREDECESSOR_TOCK,
            },
        },
        "original_tick_sha": _CONTINUATION_TICK,
        "expected_continuation_head_sha": _CONTINUATION_TICK,
        "commit_chain": [
            {
                "sha": _CONTINUATION_TICK,
                "parent_sha": _CONTINUATION_PREDECESSOR_TOCK,
            }
        ],
        "successor_task_id": successor_task_id,
        "status": status,
        "prepared_at_utc": _CONTINUATION_PREPARED_AT,
    }
    if status == "CONSUMED":
        payload["successor_promotion_attempt_id"] = successor_promotion_attempt_id
        payload["successor_tock_sha"] = successor_tock_sha
        payload["consumed_at_utc"] = consumed_at_utc
    payload["continuation_digest_sha256"] = binding_digest(
        _continuation_binding(payload)
    )
    return payload


def _write_continuation(runtime: Path, payload: dict[str, Any]) -> bytes:
    path = runtime / _CONTINUATION_NAME
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    loaded = load_continuation_artifact(path)
    assert loaded.status == payload["status"]
    assert loaded.successor_task_id == payload["successor_task_id"]
    return path.read_bytes()


def _open_successor_tock_window(
    tmp_path: Path,
) -> tuple[Path, Path, str, str, dict[str, Any]]:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _seed_pre_state(runtime, base_sha)
    _persist_ordinary_pending(
        runtime,
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        pre_state=pre_state,
    )
    _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.GIT_APPLIED,
        tock_sha=tock["tock_sha"],
    )
    return repo, runtime, result_sha, tock["tock_sha"], pre_state


def _acquire_consume_lock(runtime: Path) -> dict[str, Any]:
    acquired = acquire_runner_git_lock(
        lock_path=runtime / "runner_git.lock",
        run_id=_CONTINUATION_LOCK_RUN_ID,
        pid=os.getpid(),
        started_at_utc=_CONTINUATION_LOCK_STARTED_AT,
        now_utc=_CONTINUATION_LOCK_NOW,
        stale_after_seconds=3600,
        is_pid_alive=lambda owner_pid: owner_pid == os.getpid(),
    )
    assert acquired.get("ok") is True
    ownership = acquired["ownership"]
    assert isinstance(ownership, dict)
    return ownership


def _load_consume() -> Any:
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
    repo: Path,
    runtime: Path,
    *,
    successor_task_id: str,
    successor_promotion_attempt_id: str,
    successor_tock_sha: str,
    consumed_at_utc: str,
    lock_ownership: dict[str, Any],
) -> dict[str, Any]:
    return {
        "repo": repo,
        "runtime": runtime,
        "main_ref": _MAIN_REF,
        "successor_task_id": successor_task_id,
        "successor_promotion_attempt_id": successor_promotion_attempt_id,
        "successor_tock_sha": successor_tock_sha,
        "consumed_at_utc": consumed_at_utc,
        "lock_ownership": lock_ownership,
    }


def test_ordinary_exact_successor_tock_consumes_authorized_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, result_sha, tock_sha, _pre_state = _open_successor_tock_window(
        tmp_path
    )
    payload = _continuation_payload(successor_task_id=_ORDINARY_TASK_ID)
    digest = payload["continuation_digest_sha256"]
    _write_continuation(runtime, payload)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=tock_sha,
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    consumed = load_continuation_artifact(runtime / _CONTINUATION_NAME)
    mapping = consumed.to_mapping()
    assert consumed.status == "CONSUMED"
    assert mapping["successor_task_id"] == _ORDINARY_TASK_ID
    assert mapping["successor_promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert mapping["successor_tock_sha"] == tock_sha
    assert mapping["consumed_at_utc"]
    assert mapping["continuation_digest_sha256"] == digest
    assert list(runtime.rglob("post_tick_continuations")) == []
    assert _git(repo, "rev-parse", _MAIN_REF) == tock_sha


def test_ordinary_conflicting_continuation_reuse_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, _result_sha, tock_sha, _pre_state = _open_successor_tock_window(
        tmp_path
    )
    payload = _continuation_payload(
        successor_task_id=_ORDINARY_TASK_ID,
        status="CONSUMED",
        successor_promotion_attempt_id=_ORDINARY_FOREIGN_ATTEMPT_ID,
        successor_tock_sha="f" * 40,
    )
    assert payload["successor_tock_sha"] != tock_sha
    before = _write_continuation(runtime, payload)
    pending_before = (runtime / _ORDINARY_PENDING_NAME).read_bytes()
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, _CONTINUATION_CONSUMPTION_CONFLICT)
    assert (runtime / _CONTINUATION_NAME).read_bytes() == before
    assert (runtime / _ORDINARY_PENDING_NAME).read_bytes() == pending_before
    assert list(runtime.rglob("post_tick_continuations")) == []


def test_ordinary_continuation_error_preserves_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, _result_sha, _tock_sha, _pre_state = (
        _open_successor_tock_window(tmp_path)
    )
    pending_before = (runtime / _ORDINARY_PENDING_NAME).read_bytes()
    continuation_path = runtime / _CONTINUATION_NAME
    continuation_path.write_text("[\n", encoding="utf-8")
    continuation_before = continuation_path.read_bytes()
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, "CONTINUATION_ARTIFACT_INVALID")
    assert continuation_path.read_bytes() == continuation_before
    assert (runtime / _ORDINARY_PENDING_NAME).read_bytes() == pending_before


def test_ordinary_successor_mismatch_does_not_consume_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, _result_sha, _tock_sha, _pre_state = _open_successor_tock_window(
        tmp_path
    )
    payload = _continuation_payload(successor_task_id=_ORDINARY_FOREIGN_TASK_ID)
    assert payload["successor_task_id"] != _ORDINARY_TASK_ID
    before = _write_continuation(runtime, payload)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_blocked_ordinary(verdict, _CONTINUATION_SUCCESSOR_MISMATCH)
    assert (runtime / _CONTINUATION_NAME).read_bytes() == before
    persisted = load_continuation_artifact(runtime / _CONTINUATION_NAME)
    assert persisted.status == "AUTHORIZED"
    assert list(runtime.rglob("post_tick_continuations")) == []


def test_ordinary_identical_consumed_continuation_is_read_only_reproof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, runtime, result_sha, tock_sha, _pre_state = _open_successor_tock_window(
        tmp_path
    )
    payload = _continuation_payload(
        successor_task_id=_ORDINARY_TASK_ID,
        status="CONSUMED",
        successor_promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        successor_tock_sha=tock_sha,
    )
    before = _write_continuation(runtime, payload)
    verdict = _call_ordinary(monkeypatch, repo=repo, runtime=runtime)
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=tock_sha,
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            result_sha,
        ),
        drain_run_id=_ORDINARY_RECOVERY_DRAIN_ID,
    )
    assert (runtime / _CONTINUATION_NAME).read_bytes() == before
    lock_ownership = _acquire_consume_lock(runtime)
    consume = _load_consume()
    with pytest.raises(ContinuationError) as caught:
        consume(
            **_consume_kwargs(
                repo,
                runtime,
                successor_task_id=_ORDINARY_TASK_ID,
                successor_promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
                successor_tock_sha=tock_sha,
                consumed_at_utc=_CONTINUATION_CONSUMED_AT_LATER,
                lock_ownership=lock_ownership,
            )
        )
    assert getattr(caught.value, "reason_code", None) == _CONTINUATION_ALREADY_CONSUMED
    assert (runtime / _CONTINUATION_NAME).read_bytes() == before
    reproof = load_continuation_artifact(runtime / _CONTINUATION_NAME)
    mapping = reproof.to_mapping()
    assert reproof.status == "CONSUMED"
    assert mapping["successor_task_id"] == _ORDINARY_TASK_ID
    assert mapping["successor_promotion_attempt_id"] == _ORDINARY_ATTEMPT_ID
    assert mapping["successor_tock_sha"] == tock_sha
    assert mapping["consumed_at_utc"] == _CONTINUATION_CONSUMED_AT
    assert list(runtime.rglob("post_tick_continuations")) == []


def _pending_free_finalized_world(tmp_path: Path) -> dict[str, Any]:
    repo, runtime, base_sha, result_sha, tree_oid = _prepare_result(tmp_path)
    tock = _materialize_matching_tock(
        repo,
        base_sha=base_sha,
        result_sha=result_sha,
        tree_oid=tree_oid,
    )
    _update_ref(repo, _MAIN_REF, tock["tock_sha"])
    pre_state = _ordinary_pre_state(base_sha)
    post_state = _ordinary_post_state(result_sha, tock["tock_sha"])
    assert pre_state != post_state
    assert pre_state["observed_main_head_sha"] == base_sha
    assert post_state["observed_main_head_sha"] == tock["tock_sha"]
    journal = _write_journal_at_phase(
        runtime,
        base_sha=base_sha,
        result_sha=result_sha,
        phase=PromotionJournalPhase.FINALIZED,
        tock_sha=tock["tock_sha"],
    )
    live = write_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME,
        payload=post_state,
    )
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    assert journal.phase is PromotionJournalPhase.FINALIZED
    assert journal.tock_sha == tock["tock_sha"]
    assert journal.task_fingerprint == _ORDINARY_FINGERPRINT
    assert journal.task_snapshot_sha256 == _ORDINARY_TASK_DIGEST
    assert journal.instruction_signature_sha256 == _ORDINARY_INSTRUCTION_DIGEST
    assert live["observed_main_head_sha"] == tock["tock_sha"]
    assert live != pre_state
    return {
        "repo": repo,
        "runtime": runtime,
        "base_sha": base_sha,
        "result_sha": result_sha,
        "tock_sha": tock["tock_sha"],
        "pre_state": pre_state,
        "post_state": post_state,
        "journal": journal,
    }


def _assert_pending_free_bytes_and_head(
    world: dict[str, Any],
    before_files: dict[str, bytes],
    head_before: str,
) -> None:
    assert _ordinary_file_bytes(world["runtime"]) == before_files
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == head_before
    assert head_before == world["tock_sha"]
    assert (world["runtime"] / _ORDINARY_PENDING_NAME).exists() is False


def test_pending_free_finalized_reproof_uses_consumption_pre_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _pending_free_finalized_world(tmp_path)
    runtime = world["runtime"]
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=world["result_sha"],
        main_state_before_promotion=world["pre_state"],
        consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
        tock_sha=world["tock_sha"],
    )
    consumed = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    entry = consumed["consumed_promotions"][0]
    assert entry["promotion_attempt_id"] == world["journal"].promotion_attempt_id
    assert entry["result_sha"] == world["journal"].result_sha
    assert entry["tock_sha"] == world["journal"].tock_sha
    assert entry["main_state_before_promotion"] == world["pre_state"]
    assert entry["main_state_before_promotion"]["observed_main_head_sha"] == (
        world["journal"].base_main_sha
    )
    live = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert live == world["post_state"]
    assert live != entry["main_state_before_promotion"]
    before_files = _ordinary_file_bytes(runtime)
    head_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=world["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            world["result_sha"],
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    _assert_pending_free_bytes_and_head(world, before_files, head_before)
    reproved = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )["consumed_promotions"][0]
    assert reproved["main_state_before_promotion"] == world["pre_state"]


def test_pending_free_finalized_missing_consumption_blocks_without_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _pending_free_finalized_world(tmp_path)
    runtime = world["runtime"]
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False
    before_files = _ordinary_file_bytes(runtime)
    head_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_blocked_ordinary(verdict, "RECONCILIATION_CONSUMPTION_CONFLICT")
    _assert_pending_free_bytes_and_head(world, before_files, head_before)
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).exists() is False


def test_pending_free_finalized_ambiguous_consumption_blocks_without_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _pending_free_finalized_world(tmp_path)
    runtime = world["runtime"]
    _write_consumption_entries(
        runtime,
        [
            _consumption_entry(
                pre_state=world["post_state"],
                result_sha=world["result_sha"],
                tock_sha=world["tock_sha"],
                consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
            ),
            _consumption_entry(
                pre_state=world["post_state"],
                result_sha=world["result_sha"],
                tock_sha=world["tock_sha"],
                consumed_at_utc="2026-09-22T00:05:00Z",
            ),
        ],
    )
    consumed = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )
    assert len(consumed["consumed_promotions"]) == 2
    assert {
        (
            entry["promotion_attempt_id"],
            entry["result_sha"],
            entry["tock_sha"],
        )
        for entry in consumed["consumed_promotions"]
    } == {
        (_ORDINARY_ATTEMPT_ID, world["result_sha"], world["tock_sha"]),
    }
    before_files = _ordinary_file_bytes(runtime)
    head_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_blocked_ordinary(verdict, "RECONCILIATION_CONSUMPTION_CONFLICT")
    _assert_pending_free_bytes_and_head(world, before_files, head_before)


def test_pending_free_finalized_contradictory_consumption_blocks_without_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _pending_free_finalized_world(tmp_path)
    runtime = world["runtime"]
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=_ORDINARY_ATTEMPT_ID,
        result_sha=world["result_sha"],
        main_state_before_promotion=world["post_state"],
        consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
        tock_sha=world["tock_sha"],
    )
    entry = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )["consumed_promotions"][0]
    assert entry["promotion_attempt_id"] == world["journal"].promotion_attempt_id
    assert entry["result_sha"] == world["journal"].result_sha
    assert entry["tock_sha"] == world["journal"].tock_sha
    assert entry["main_state_before_promotion"]["observed_main_head_sha"] != (
        world["journal"].base_main_sha
    )
    before_files = _ordinary_file_bytes(runtime)
    head_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_blocked_ordinary(verdict, "RECONCILIATION_CONSUMPTION_CONFLICT")
    _assert_pending_free_bytes_and_head(world, before_files, head_before)


def test_pending_free_finalized_identical_consumption_ignores_fully_foreign_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _pending_free_finalized_world(tmp_path)
    runtime = world["runtime"]
    identical = _consumption_entry(
        pre_state=world["pre_state"],
        result_sha=world["result_sha"],
        tock_sha=world["tock_sha"],
        consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
    )
    foreign_pre_state = dict(world["pre_state"])
    foreign_pre_state["validated_result_sha"] = _ORDINARY_FOREIGN_RESULT_SHA
    foreign_pre_state["last_tock_sha"] = _ORDINARY_THIRD_HEAD_SHA
    foreign_pre_state["observed_main_head_sha"] = _ORDINARY_THIRD_HEAD_SHA
    foreign_pre_state["reason_code"] = "STATE_FOREIGN_SYNTHETIC"
    historical = _consumption_entry(
        pre_state=foreign_pre_state,
        result_sha=_ORDINARY_FOREIGN_RESULT_SHA,
        tock_sha=_ORDINARY_THIRD_HEAD_SHA,
        promotion_attempt_id=_ORDINARY_FOREIGN_ATTEMPT_ID,
        consumed_at_utc=_ORDINARY_STAMPS["PENDING"],
    )
    identical_axes = (
        identical["promotion_attempt_id"],
        identical["result_sha"],
        identical["tock_sha"],
    )
    historical_axes = (
        historical["promotion_attempt_id"],
        historical["result_sha"],
        historical["tock_sha"],
    )
    assert historical_axes != identical_axes
    assert historical["promotion_attempt_id"] != identical["promotion_attempt_id"]
    assert historical["result_sha"] != identical["result_sha"]
    assert historical["tock_sha"] != identical["tock_sha"]
    assert identical_axes == (
        world["journal"].promotion_attempt_id,
        world["journal"].result_sha,
        world["journal"].tock_sha,
    )
    assert identical["main_state_before_promotion"] == world["pre_state"]
    assert identical["main_state_before_promotion"]["observed_main_head_sha"] == (
        world["journal"].base_main_sha
    )
    assert historical["main_state_before_promotion"] != world["pre_state"]
    loaded = _write_consumption_entries(runtime, [historical, identical])
    assert loaded["consumed_promotions"] == [historical, identical]
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    before_files = _ordinary_file_bytes(runtime)
    head_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=world["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            world["result_sha"],
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    _assert_pending_free_bytes_and_head(world, before_files, head_before)
    reproved = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )["consumed_promotions"]
    assert reproved == [historical, identical]
    assert reproved[1]["main_state_before_promotion"] == world["pre_state"]
    assert reproved[0]["main_state_before_promotion"]["observed_main_head_sha"] == (
        _ORDINARY_THIRD_HEAD_SHA
    )


def test_pending_free_finalized_reproof_defers_predecessor_bound_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _pending_free_finalized_world(tmp_path)
    runtime = world["runtime"]
    journal = world["journal"]
    record_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME,
        promotion_attempt_id=journal.promotion_attempt_id,
        result_sha=journal.result_sha,
        main_state_before_promotion=world["pre_state"],
        consumed_at_utc=_ORDINARY_STAMPS["FINALIZED"],
        tock_sha=journal.tock_sha,
    )
    entry = load_promotion_consumed(
        promotion_consumed_path=runtime / _ORDINARY_CONSUMPTION_NAME
    )["consumed_promotions"][0]
    assert entry["promotion_attempt_id"] == journal.promotion_attempt_id
    assert entry["result_sha"] == journal.result_sha
    assert entry["tock_sha"] == journal.tock_sha
    assert entry["main_state_before_promotion"] == world["pre_state"]
    assert entry["main_state_before_promotion"]["observed_main_head_sha"] == (
        journal.base_main_sha
    )
    live = load_and_validate_main_state(
        main_state_path=runtime / _ORDINARY_MAIN_STATE_NAME
    )
    assert live == world["post_state"]
    assert journal.phase is PromotionJournalPhase.FINALIZED
    assert journal.task_id == _ORDINARY_TASK_ID
    assert journal.origin_run_id == _ORDINARY_ORIGIN_ID
    assert journal.promotion_attempt_id == _ORDINARY_ATTEMPT_ID
    assert journal.result_sha == world["result_sha"]
    assert journal.promotion_class == _ORDINARY_PROMOTION_CLASS
    assert journal.tock_sha == world["tock_sha"]
    payload = {
        "schema_version": "2.0",
        "origin": {
            "kind": "FINALIZED_PROMOTION_TICK",
            "evidence": {
                "predecessor_identity": {
                    "task_id": journal.task_id,
                    "origin_run_id": journal.origin_run_id,
                    "promotion_attempt_id": journal.promotion_attempt_id,
                    "result_sha": journal.result_sha,
                    "promotion_class": journal.promotion_class,
                },
                "tock_sha": journal.tock_sha,
            },
        },
        "original_tick_sha": _CONTINUATION_TICK,
        "expected_continuation_head_sha": _CONTINUATION_TICK,
        "commit_chain": [
            {
                "sha": _CONTINUATION_TICK,
                "parent_sha": journal.tock_sha,
            }
        ],
        "successor_task_id": _ORDINARY_FOREIGN_TASK_ID,
        "status": "AUTHORIZED",
        "prepared_at_utc": _CONTINUATION_PREPARED_AT,
    }
    payload["continuation_digest_sha256"] = binding_digest(
        _continuation_binding(payload)
    )
    identity = payload["origin"]["evidence"]["predecessor_identity"]
    assert identity["task_id"] != _CONTINUATION_PREDECESSOR_TASK_ID
    assert identity["origin_run_id"] != _CONTINUATION_PREDECESSOR_ORIGIN_ID
    assert identity["promotion_attempt_id"] != _CONTINUATION_PREDECESSOR_ATTEMPT_ID
    assert identity["result_sha"] != _CONTINUATION_PREDECESSOR_RESULT_SHA
    assert payload["origin"]["evidence"]["tock_sha"] != _CONTINUATION_PREDECESSOR_TOCK
    assert payload["successor_task_id"] != journal.task_id
    before_continuation = _write_continuation(runtime, payload)
    loaded = load_continuation_artifact(runtime / _CONTINUATION_NAME)
    loaded_identity = loaded.to_mapping()["origin"]["evidence"]["predecessor_identity"]
    assert loaded.status == "AUTHORIZED"
    assert loaded.successor_task_id == _ORDINARY_FOREIGN_TASK_ID
    assert loaded_identity == identity
    assert loaded.to_mapping()["origin"]["evidence"]["tock_sha"] == journal.tock_sha
    journal_bytes = (runtime / _ORDINARY_JOURNAL_NAME).read_bytes()
    consumption_bytes = (runtime / _ORDINARY_CONSUMPTION_NAME).read_bytes()
    main_state_bytes = (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes()
    before_files = _ordinary_file_bytes(runtime)
    head_before = _git(world["repo"], "rev-parse", _MAIN_REF)
    verdict = _call_ordinary(
        monkeypatch,
        repo=world["repo"],
        runtime=runtime,
    )
    _assert_ordinary_verdict(
        verdict,
        reason_code="RECONCILIATION_COMPLETED",
        phase="FINALIZED",
        tock_sha=world["tock_sha"],
        identity=(
            _ORDINARY_TASK_ID,
            _ORDINARY_ORIGIN_ID,
            _ORDINARY_ATTEMPT_ID,
            world["result_sha"],
        ),
        drain_run_id=_ORDINARY_PRIOR_DRAIN_ID,
    )
    _assert_pending_free_bytes_and_head(world, before_files, head_before)
    assert (runtime / _ORDINARY_JOURNAL_NAME).read_bytes() == journal_bytes
    assert (runtime / _ORDINARY_CONSUMPTION_NAME).read_bytes() == consumption_bytes
    assert (runtime / _ORDINARY_MAIN_STATE_NAME).read_bytes() == main_state_bytes
    assert (runtime / _CONTINUATION_NAME).read_bytes() == before_continuation
    persisted = load_continuation_artifact(runtime / _CONTINUATION_NAME)
    assert persisted.status == "AUTHORIZED"
    assert persisted.successor_task_id == _ORDINARY_FOREIGN_TASK_ID
    assert persisted.to_mapping()["origin"]["evidence"]["predecessor_identity"] == (
        identity
    )
    reproved = load_promotion_journal(path=runtime / _ORDINARY_JOURNAL_NAME)
    assert reproved.phase is PromotionJournalPhase.FINALIZED
    assert reproved.task_id == journal.task_id
    assert reproved.origin_run_id == journal.origin_run_id
    assert reproved.promotion_attempt_id == journal.promotion_attempt_id
    assert reproved.result_sha == journal.result_sha
    assert reproved.promotion_class == journal.promotion_class
    assert reproved.tock_sha == journal.tock_sha
    assert (runtime / _ORDINARY_PENDING_NAME).exists() is False
    assert _git(world["repo"], "rev-parse", _MAIN_REF) == head_before