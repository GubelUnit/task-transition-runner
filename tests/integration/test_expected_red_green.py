"""Real-git expected-RED obligation open and fresh-GREEN close.

Keep:
- the designated command opens one obligation only when it exits non-zero
  and its UTF-8 output contains the declared failure signature
- that opening result SHA, command id, and signature are the obligation
- a later GREEN admission uses a new task, origin, attempt, and result
- that GREEN admission closes the same obligation and leaves main
  GREEN_VALIDATED
- an unrelated failure does not open the obligation
- an additional unexplained failure without the declared signature does
  not open the obligation
- a zero exit does not open the obligation, even when the output contains
  the declared signature and further failure text

Drop:
- owner receipts, runner logs, and any pass claim beyond these two nodes
- product, dependency, or lockfile changes
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

import yaml

_TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(_TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TESTS_ROOT))

import test_runner_git_promotion as pending_tests

_FRESH_TASK_ID = pending_tests._FOREIGN_TASK_ID
_FRESH_ORIGIN_RUN_ID = pending_tests._FOREIGN_ORIGIN_RUN_ID
_FRESH_PROMOTION_ATTEMPT_ID = pending_tests._FOREIGN_PROMOTION_ATTEMPT_ID
_FRESH_TASK_BRANCH = "runner/task/synthetic-task-002/attempt-002"
_DECLARED_SIGNATURE = pending_tests._REFERENCE_FAILURE_SIGNATURE
_UNRELATED_FAILURE = "synthetic unrelated failure\n"
_ADDITIONAL_FAILURE = "additional unexplained failure\n"


def test_exact_declared_red_opens_and_fresh_green_closes_same_obligation(
    tmp_path: Path,
) -> None:
    opened = pending_tests._prepare_effect_world(
        tmp_path / "open",
        flavor="expected_red",
    )
    red_admit = pending_tests._call_admit(opened)
    pending_tests._assert_successful_tock(opened, red_admit, exit_code=1)

    red_lock = dict(opened["admit_kwargs"]["red_lock"])
    opened_result_sha = opened["result_sha"]
    main_after_red = _load_yaml(opened["runtime"] / "main_state.yaml")
    validation = pending_tests._assert_persisted_candidate_validation(
        opened,
        exit_code=1,
    )
    validation_output = pending_tests._validation_output_bytes(
        opened,
        validation,
    ).decode("utf-8")

    assert opened["intent"].red_lock_closure is None
    assert red_admit["ok"] is True
    assert main_after_red["state"] == "EXPECTED_RED_OPEN"
    assert main_after_red["validated_result_sha"] == opened_result_sha
    assert main_after_red["last_tock_sha"] == red_admit["tock_sha"]
    assert red_lock["result_sha"] == opened_result_sha
    assert red_lock["task_id"] == pending_tests._REFERENCE_TASK_ID
    assert red_lock["origin_run_id"] == pending_tests._REFERENCE_ORIGIN_RUN_ID
    assert (
        red_lock["promotion_attempt_id"]
        == pending_tests._REFERENCE_PROMOTION_ATTEMPT_ID
    )
    assert red_lock["command_id"] == pending_tests._REFERENCE_COMMAND_ID
    assert red_lock["expected_failure_signature"] == _DECLARED_SIGNATURE
    assert red_lock["closure_gate"] == "GREEN_PROMOTION_REQUIRED"
    assert validation["exit_code"] != 0
    assert _DECLARED_SIGNATURE in validation_output

    closed = _admit_fresh_green_closure(
        tmp_path / "close",
        opened_repo=opened["repo"],
        red_lock=red_lock,
        main_after_red=main_after_red,
        red_tock_sha=red_admit["tock_sha"],
    )
    closure = closed["intent"].red_lock_closure
    green_identity = closure["green_promotion_identity"]
    main_after_green = _load_yaml(closed["runtime"] / "main_state.yaml")

    assert closure["closure_type"] == "green_red_lock_closure"
    assert closure["prior_red_result_sha"] == opened_result_sha
    assert closure["prior_red_result_sha"] == red_lock["result_sha"]
    assert green_identity["task_id"] == _FRESH_TASK_ID
    assert green_identity["origin_run_id"] == _FRESH_ORIGIN_RUN_ID
    assert green_identity["promotion_attempt_id"] == _FRESH_PROMOTION_ATTEMPT_ID
    assert green_identity["result_sha"] == closed["result_sha"]
    assert green_identity["promotion_class"] == "GREEN_PROMOTION"
    assert green_identity["task_id"] != red_lock["task_id"]
    assert green_identity["origin_run_id"] != red_lock["origin_run_id"]
    assert (
        green_identity["promotion_attempt_id"]
        != red_lock["promotion_attempt_id"]
    )
    assert green_identity["result_sha"] != red_lock["result_sha"]
    assert main_after_green["state"] == "GREEN_VALIDATED"
    assert main_after_green["validated_result_sha"] == closed["result_sha"]
    assert main_after_green["validated_result_sha"] != opened_result_sha
    assert main_after_green["last_tock_sha"] == closed["admit"]["tock_sha"]
    assert closed["parents"] == [
        closed["admit"]["tock_sha"],
        red_admit["tock_sha"],
        closed["result_sha"],
    ]


def test_unrelated_or_additional_failure_never_satisfies_expected_red(
    tmp_path: Path,
) -> None:
    _assert_expected_red_stays_closed(
        tmp_path / "unrelated",
        stdout=_UNRELATED_FAILURE,
        exit_code=1,
        signature_present=False,
    )
    _assert_expected_red_stays_closed(
        tmp_path / "additional",
        stdout=_ADDITIONAL_FAILURE,
        exit_code=1,
        signature_present=False,
    )
    _assert_expected_red_stays_closed(
        tmp_path / "signature-without-failure",
        stdout=_DECLARED_SIGNATURE + "\n" + _ADDITIONAL_FAILURE,
        exit_code=0,
        signature_present=True,
    )


def _assert_expected_red_stays_closed(
    tmp_path: Path,
    *,
    stdout: str,
    exit_code: int,
    signature_present: bool,
) -> None:
    world = pending_tests._prepare_effect_world(tmp_path, flavor="expected_red")
    result = pending_tests._call_admit_fail_closed(
        world,
        validation_command=_validation_command(stdout, exit_code),
    )
    pending_tests._assert_effect_blocked(
        result,
        ("PROMOTION_VALIDATION_OUTCOME_MISMATCH",),
        world,
        require_validation=True,
    )
    payload = pending_tests._assert_persisted_candidate_validation(
        world,
        exit_code=exit_code,
    )
    output = pending_tests._validation_output_bytes(world, payload).decode("utf-8")
    main_state = _load_yaml(world["main_state_path"])
    assert (_DECLARED_SIGNATURE in output) is signature_present
    assert result["ok"] is False
    assert "tock_sha" not in result
    assert main_state["state"] != "EXPECTED_RED_OPEN"
    assert main_state["validated_result_sha"] != world["result_sha"]
    assert (world["runtime"] / "promotion_journal.yaml").exists() is False
    assert (world["runtime"] / "promotion_consumed.yaml").exists() is False


def _validation_command(stdout: str, exit_code: int) -> tuple[str, ...]:
    source = (
        "import sys\n"
        f"sys.stdout.write({stdout!r})\n"
        f"raise SystemExit({exit_code})\n"
    )
    return pending_tests._python_repo_command(source)


def _admit_fresh_green_closure(
    tmp_path: Path,
    *,
    opened_repo: Path,
    red_lock: dict[str, Any],
    main_after_red: dict[str, Any],
    red_tock_sha: str,
) -> dict[str, Any]:
    repo = opened_repo
    pending_tests._run_git(repo, "checkout", "-b", _FRESH_TASK_BRANCH)
    (repo / "src" / "example.py").write_text(
        "candidate example green closure\n",
        encoding="utf-8",
    )
    pending_tests._run_git(repo, "add", "--", "src/example.py")
    pending_tests._run_git(repo, "commit", "-m", "fresh green closure")
    green_result_sha = pending_tests._run_git(repo, "rev-parse", "HEAD")
    pending_tests._run_git(repo, "checkout", "main")
    green_base_sha = pending_tests._run_git(repo, "rev-parse", "refs/heads/main")
    assert green_base_sha == red_tock_sha

    worker = pending_tests._reference_worker_result(
        task_id=_FRESH_TASK_ID,
        origin_run_id=_FRESH_ORIGIN_RUN_ID,
        promotion_attempt_id=_FRESH_PROMOTION_ATTEMPT_ID,
        task_branch=_FRESH_TASK_BRANCH,
        result_sha=green_result_sha,
        changed_paths=("src/example.py",),
    )
    orchestrator = pending_tests._reference_orchestrator(
        promotion_class="GREEN_PROMOTION",
        task_id=_FRESH_TASK_ID,
        origin_run_id=_FRESH_ORIGIN_RUN_ID,
        promotion_attempt_id=_FRESH_PROMOTION_ATTEMPT_ID,
        result_sha=green_result_sha,
    )
    validator = pending_tests._reference_validator(
        task_id=_FRESH_TASK_ID,
        origin_run_id=_FRESH_ORIGIN_RUN_ID,
        promotion_attempt_id=_FRESH_PROMOTION_ATTEMPT_ID,
        result_sha=green_result_sha,
    )
    task_view = pending_tests._task_view(task_id=_FRESH_TASK_ID)
    task_snapshot = pending_tests._reference_task_snapshot(
        current=task_view,
        bound=copy.deepcopy(task_view),
    )
    qualified = pending_tests.qualify_pre_candidate_intent(
        **pending_tests._green_closure_kwargs(
            worker_result=worker,
            task_branch_head_sha=green_result_sha,
            main_state=main_after_red,
            orchestrator_decision=orchestrator,
            validator_decision=validator,
            red_lock=red_lock,
            task_snapshot=task_snapshot,
            allowed_paths=("src/example.py",),
        )
    )
    assert qualified["ok"] is True
    assert qualified["reason_code"] == "PROMOTION_INTENT_QUALIFIED"
    intent = qualified["intent"]
    assert intent.red_lock_closure["prior_red_result_sha"] == red_lock["result_sha"]

    persist_kwargs = pending_tests._persist_kwargs(
        tmp_path,
        intent=intent,
        qualify="green_closure",
        target_worktree=repo,
        worker_result=worker,
        orchestrator_decision=orchestrator,
        validator_decision=validator,
        task_snapshot=task_snapshot,
        base_sha=green_base_sha,
    )
    persisted = pending_tests.persist_pending_promotion(**persist_kwargs)
    assert persisted["ok"] is True
    runtime = persist_kwargs["runtime_root"]
    lock_ownership = pending_tests._write_owned_process_lock(runtime)
    admit_kwargs = {
        "runtime_root": runtime,
        "target_worktree": repo,
        "validation_command": pending_tests._noop_validation_command(),
        "validation_timeout_seconds": 30,
        "command_id": pending_tests._REFERENCE_COMMAND_ID,
        "task_snapshot": task_snapshot,
        "instruction_snapshot": persist_kwargs["instruction_snapshot"],
        "worker_result": worker,
        "expected_outcome": persist_kwargs["expected_outcome"],
        "orchestrator_decision": orchestrator,
        "validator_decision": validator,
        "base_sha": green_base_sha,
        "task_branch_head_sha": green_result_sha,
        "allowed_paths": ("src/example.py",),
        "risk_facts": pending_tests._clear_risk_facts(),
        "lock_ownership": lock_ownership,
        "git_preflight": pending_tests._clean_preflight(),
        "consumed_payload": pending_tests._empty_consumed(),
        "red_lock": red_lock,
        "unresolved_foreign_epoch": False,
    }
    red_tock_tree = pending_tests._run_git(
        repo,
        "rev-parse",
        f"{red_tock_sha}^{{tree}}",
    )
    world = {
        "repo": repo,
        "runtime": runtime,
        "flavor": "green_closure",
        "main_tree": red_tock_tree,
        "admit_kwargs": admit_kwargs,
    }
    admit = pending_tests._call_admit(world)
    _assert_green_tock_replaces_red_tree(world, admit, red_tock_tree)
    parents = pending_tests._run_git(
        repo,
        "rev-list",
        "--parents",
        "-n",
        "1",
        admit["tock_sha"],
    ).split()
    return {
        "admit": admit,
        "intent": intent,
        "runtime": runtime,
        "result_sha": green_result_sha,
        "parents": parents,
    }


def _assert_green_tock_replaces_red_tree(
    world: dict[str, Any],
    admit: dict[str, Any],
    red_tock_tree: str,
) -> None:
    assert admit["ok"] is True
    assert admit["status"] == "tocked"
    validated_tree = admit["validated_candidate_tree_oid"]
    tock_sha = admit["tock_sha"]
    repo = world["repo"]
    assert admit["tree_oid"] == validated_tree
    assert pending_tests._run_git(repo, "rev-parse", f"{tock_sha}^{{tree}}") == (
        validated_tree
    )
    assert pending_tests._run_git(repo, "rev-parse", "refs/heads/main^{tree}") == (
        validated_tree
    )
    assert validated_tree != red_tock_tree
    assert (repo / "src" / "example.py").read_text(encoding="utf-8") == (
        "candidate example green closure\n"
    )
    record = pending_tests._assert_persisted_candidate_validation(
        world,
        exit_code=0,
    )
    assert record["candidate_tree_oid"] == validated_tree
    assert (world["runtime"] / "promotion_journal.yaml").is_file()
    assert (world["runtime"] / "promotion_consumed.yaml").is_file()
    assert pending_tests._run_git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert pending_tests._run_git(repo, "status", "--porcelain") == ""
    assert pending_tests._run_git(repo, "rev-parse", "HEAD") == tock_sha


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
