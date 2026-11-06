"""Synthetic public git-lifecycle proof for effect-admission tree binding.

Keep:
- persist a non-authorizing pending intent first
- admit the effect only after persisted candidate validation
- validation runs while MERGE_HEAD equals result_sha and Main is still
  the bound original SHA
- native TOCK tree OID equals the frozen validated candidate-tree OID
- canonical TOCK subject is exactly ``TOCK``
- the six identity trailers appear exactly once
- ordered parents are exactly ``[base, result]``
- validated merge-candidate content and TOCK tree differ from the old Main tree
- Direct GREEN, EXPECTED_RED, and GREEN-closure admits share that tree proof
- EXPECTED_RED output bytes contain the failure signature and exact digest
- EXPECTED_RED with the signature present and exit_code 0 stays fail-closed
- live task-branch ref drift after pending is a result/branch block with no
  journal, consumption, TOCK, or Main mutation
- no-commit merge conflict restores Main, index, working tree, and MERGE_HEAD
  without leaking CalledProcessError and without effects
- identical-tree live task-branch drift is an exact result-sha mismatch
- a deleted live task-branch ref is a stable result/branch block without a
  raw git exception

Drop:
- private role fixtures, parser envelopes, and approval paths
- recovery of an already existing TOCK
- PUBLIC-P, sandbox, or exactly-once claims
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(_TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TESTS_ROOT))

import test_runner_git_promotion as pending_tests


def test_tock_tree_equals_validated_candidate_tree(tmp_path: Path) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    result = pending_tests._call_admit(world)
    pending_tests._assert_successful_tock(world, result, exit_code=0)


def test_expected_red_tock_tree_equals_validated_candidate_tree(tmp_path: Path) -> None:
    world = pending_tests._prepare_effect_world(tmp_path, flavor="expected_red")
    result = pending_tests._call_admit(world)
    pending_tests._assert_successful_tock(world, result, exit_code=1)


def test_green_closure_tock_tree_equals_validated_candidate_tree(tmp_path: Path) -> None:
    world = pending_tests._prepare_effect_world(tmp_path, flavor="green_closure")
    result = pending_tests._call_admit(world)
    pending_tests._assert_successful_tock(world, result, exit_code=0)


def test_live_task_branch_ref_drift_after_pending_is_fail_closed(
    tmp_path: Path,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    pending_tests._drift_live_task_branch_after_pending(world)
    result = pending_tests._call_admit_fail_closed(world)
    pending_tests._assert_effect_blocked(
        result,
        pending_tests._RESULT_BRANCH_BLOCK_CODES,
        world,
    )


def test_no_commit_merge_conflict_restores_git_state_without_effects(
    tmp_path: Path,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path, conflict_on_main=True)
    result = pending_tests._call_admit_fail_closed(world)
    pending_tests._assert_effect_blocked(
        result,
        pending_tests._MERGE_CONFLICT_BLOCK_CODES,
        world,
    )
    pending_tests._assert_main_restored(world)


def test_identical_tree_live_task_branch_drift_is_fail_closed(
    tmp_path: Path,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    pending_tests._drift_live_task_branch_identical_tree(world)
    result = pending_tests._call_admit_fail_closed(world)
    pending_tests._assert_effect_blocked(
        result,
        ("PROMOTION_RESULT_SHA_MISMATCH",),
        world,
    )


def test_missing_live_task_branch_ref_is_fail_closed(tmp_path: Path) -> None:
    world = pending_tests._prepare_effect_world(tmp_path)
    pending_tests._delete_live_task_branch_ref(world)
    result = pending_tests._call_admit_fail_closed(world)
    pending_tests._assert_effect_blocked(
        result,
        pending_tests._RESULT_BRANCH_BLOCK_CODES,
        world,
    )


_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)


def _admitted_tock_geometry(tmp_path: Path, flavor: str) -> dict[str, object]:
    world = pending_tests._prepare_effect_world(tmp_path, flavor=flavor)
    result = pending_tests._call_admit(world)
    assert result["ok"] is True
    repo = world["repo"]
    tock_sha = result["tock_sha"]
    message = pending_tests._run_git(repo, "log", "-1", "--pretty=%B", tock_sha)
    listed = pending_tests._run_git(
        repo,
        "rev-list",
        "--parents",
        "-n",
        "1",
        tock_sha,
    ).split()
    tree = pending_tests._run_git(repo, "rev-parse", f"{tock_sha}^{{tree}}")
    return {
        "world": world,
        "result": result,
        "tock_sha": tock_sha,
        "message": message,
        "listed_parents": listed,
        "tree": tree,
    }


@pytest.mark.parametrize("flavor", ("green", "expected_red"))
def test_canonical_tock_subject_is_exact_tock(tmp_path: Path, flavor: str) -> None:
    observed = _admitted_tock_geometry(tmp_path, flavor)
    message = str(observed["message"])
    subject = message.splitlines()[0]
    assert subject == "TOCK", (
        "C0 compact one-parent TOCK subject is not canonical; "
        f"observed {subject!r}"
    )


@pytest.mark.parametrize("flavor", ("green", "expected_red"))
def test_canonical_tock_trailers_each_exactly_once(
    tmp_path: Path,
    flavor: str,
) -> None:
    observed = _admitted_tock_geometry(tmp_path, flavor)
    world = observed["world"]
    message = str(observed["message"])
    intent = world["intent"]
    expected = {
        "Promotion-Attempt": intent.promotion_attempt_id,
        "Task-ID": intent.task_id,
        "Origin-Run": intent.origin_run_id,
        "Result-SHA": world["result_sha"],
        "Promotion-Class": intent.promotion_class,
        "Expected-Base": world["main_sha"],
    }
    observed_keys = [
        line.split(": ", 1)[0]
        for line in message.splitlines()
        if ": " in line
    ]
    assert observed_keys == list(_CANONICAL_TOCK_TRAILER_KEYS), (
        "C0 requires six identity trailers exactly once; "
        f"observed {observed_keys!r}"
    )
    for key, value in expected.items():
        assert message.count(f"{key}: {value}") == 1


@pytest.mark.parametrize("flavor", ("green", "expected_red"))
def test_canonical_tock_parents_are_base_then_result(
    tmp_path: Path,
    flavor: str,
) -> None:
    observed = _admitted_tock_geometry(tmp_path, flavor)
    world = observed["world"]
    assert observed["listed_parents"] == [
        observed["tock_sha"],
        world["main_sha"],
        world["result_sha"],
    ], (
        "C0 parent order must be [base, result]; "
        "one-parent TOCK geometry is not canonical"
    )


@pytest.mark.parametrize("flavor", ("green", "expected_red"))
def test_canonical_tock_tree_is_frozen_validated_candidate_tree(
    tmp_path: Path,
    flavor: str,
) -> None:
    observed = _admitted_tock_geometry(tmp_path, flavor)
    result = observed["result"]
    assert observed["tree"] == result["validated_candidate_tree_oid"]
    assert observed["tree"] == result["tree_oid"]


def test_public_contract_closure_real_git_integration(tmp_path: Path) -> None:
    """Reprove positive and fail-closed public closure with real Git objects."""
    test_tock_tree_equals_validated_candidate_tree(tmp_path / "direct-green")
    test_expected_red_tock_tree_equals_validated_candidate_tree(
        tmp_path / "expected-red",
    )
    test_green_closure_tock_tree_equals_validated_candidate_tree(
        tmp_path / "green-closure",
    )

    test_live_task_branch_ref_drift_after_pending_is_fail_closed(
        tmp_path / "live-result-ref-drift",
    )
    test_no_commit_merge_conflict_restores_git_state_without_effects(
        tmp_path / "merge-conflict",
    )
    test_identical_tree_live_task_branch_drift_is_fail_closed(
        tmp_path / "identical-tree-result-ref-drift",
    )
    test_missing_live_task_branch_ref_is_fail_closed(
        tmp_path / "missing-result-ref",
    )

    for flavor in ("green", "expected_red"):
        test_canonical_tock_subject_is_exact_tock(
            tmp_path / f"{flavor}-subject",
            flavor,
        )
        test_canonical_tock_trailers_each_exactly_once(
            tmp_path / f"{flavor}-trailers",
            flavor,
        )
        test_canonical_tock_parents_are_base_then_result(
            tmp_path / f"{flavor}-parents",
            flavor,
        )
        test_canonical_tock_tree_is_frozen_validated_candidate_tree(
            tmp_path / f"{flavor}-tree",
            flavor,
        )
