"""Synthetic public read-only status contract.

Keep:
- a narrow projection of already classified State, Journal, Pending,
  Consumption, Drain, Decision, next-action, and reason-code facts
- ABSENT versus BLOCKED versus CLASSIFIED as the only presence states
- byte-identical Git refs, HEAD, index, worktree, runtime, and lock
  artifacts before and after every status call
- pending projected only as ``NON_AUTHORIZING_PROMOTION_INTENT``
- authority-first classification through ``classify_authority_first_status``
  before any semantic runtime reading

Mismatch, and only mismatch, returns the ``SOURCE_AUTHORITY_*`` reason
with a minimal non-interpreting diagnosis. Semantic runtime facts stay
ABSENT. After a real authority proof, every present public fact is
classified and projected as ``STATUS_VIEW_PROJECTED``. Malformed public
facts fail closed with ``STATUS_FACTS_INVALID`` or
``STATUS_PRESENCE_PAYLOAD_CONFLICT``.

Drop:
- ``SOURCE_AUTHORITY_OK`` or ``RECOVERED`` inferred from files
- semantic runtime interpretation before Source Authority
- mutation of Git, runtime, or the lock
- lock acquisition, init, drain, promotion, activation, or migration
"""

from __future__ import annotations

import hashlib
import inspect
import json
import stat
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

import task_transition_runner.cli as cli_module
import task_transition_runner.status as status_module
from task_transition_runner.runner_code_authority import RUNTIME_SOURCE_PIN_NAME
from task_transition_runner.status import (
    ClassifiedFact,
    ClassifiedStatusFacts,
    FactPresence,
    StatusView,
    StatusViewError,
    allowed_reason_codes,
    project_readonly_status,
)

_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_TASK_ID = "synthetic-status-task-001"
_FOREIGN_TASK_ID = "synthetic-status-task-002"
_REFERENCE_ORIGIN_RUN_ID = "origin-run-001"
_REFERENCE_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_REFERENCE_DRAIN_RUN_ID = "drain-run-001"
_REFERENCE_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_FOREIGN_RESULT_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_REFERENCE_TOCK_SHA = "cccccccccccccccccccccccccccccccccccccccc"
_REFERENCE_HEAD_PLACEHOLDER = "dddddddddddddddddddddddddddddddddddddddd"
_REFERENCE_UPDATED_AT = "2026-09-11T21:00:00Z"
_GIT_AUTHOR_NAME = "Read Only Status Contract"
_GIT_AUTHOR_EMAIL = "read-only-status@task-transition-runner.invalid"
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_REQUIRED_EXPORTS = (
    "ClassifiedFact",
    "ClassifiedStatusFacts",
    "FactPresence",
    "StatusView",
    "StatusViewError",
    "allowed_reason_codes",
    "project_readonly_status",
)
_FORBIDDEN_EXPORTS = (
    "acquire_lock",
    "admit_pending_promotion_effect",
    "bootstrap_main_state",
    "check_code_authority",
    "consume_continuation",
    "drain_before_dispatch",
    "evaluate_task_activation",
    "load_and_validate_main_state",
    "reconcile",
    "recover",
    "release_lock",
    "verify_source_authority",
)
_FORBIDDEN_PARAMETER_NAMES = {
    "authority_ok",
    "default_source_authority",
    "recover",
    "reconcile",
    "source_root",
}
_FACT_SLOTS = (
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
    "reason_code",
    "drift_warning",
)
_PROJECT_REQUIRED_PARAMETERS = (
    "classified_facts",
    "target_worktree",
    "runtime_root",
)
_REQUIRED_REASON_CODES = (
    "STATUS_FACTS_INVALID",
    "STATUS_FORBIDDEN_OUTCOME",
    "STATUS_PRESENCE_PAYLOAD_CONFLICT",
    "STATUS_RUNTIME_ROOT_INVALID",
    "STATUS_TARGET_INVALID",
    "STATUS_VIEW_PROJECTED",
)
_ALLOWED_NEXT_ACTIONS = (
    "INITIALIZED",
    "TERMINAL_REST",
    "WORKER_RESULT_READY",
    "AWAITING_DECISIONS",
    "PENDING_PROMOTION",
    "DRAINED_AWAITING_TASK",
    "SETTLED",
    "BLOCKED",
)
_FORBIDDEN_NEXT_ACTIONS = (
    "RECOVERED",
    "REOPENED",
    "MIGRATED",
    "SOURCE_AUTHORITY_OK",
)
_RUNTIME_ARTIFACT_NAMES = (
    "main_state.yaml",
    "promotion_consumed.yaml",
    "pending_promotion.yaml",
    "promotion_journal.yaml",
    "drain_outcome.yaml",
    "decision_request.yaml",
    "orchestrator_decision.yaml",
    "validator_decision.yaml",
    "runner_source_identity.yaml",
    "runner_git.lock",
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _run_git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _init_target_repo(
    tmp_path: Path,
    *,
    name: str = "target-repo",
    branch: str = _REFERENCE_MAIN_BRANCH,
) -> Path:
    repo_root = tmp_path / name
    repo_root.mkdir(parents=True)
    subprocess.run(  # noqa: S603
        ["git", "-c", f"init.defaultBranch={branch}", "init"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    _run_git(repo_root, "config", "user.name", _GIT_AUTHOR_NAME)
    _run_git(repo_root, "config", "user.email", _GIT_AUTHOR_EMAIL)
    (repo_root / "tracked.txt").write_text("status baseline\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "tracked.txt")
    _run_git(repo_root, "commit", "-m", "status baseline")
    return repo_root


def _runtime_root(tmp_path: Path, *, create: bool = True) -> Path:
    path = tmp_path / "runner-runtime"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _write_yaml(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _absent() -> ClassifiedFact:
    return ClassifiedFact(presence=FactPresence.ABSENT, payload=None, reason_code=None)


def _classified(payload: dict[str, Any]) -> ClassifiedFact:
    return ClassifiedFact(
        presence=FactPresence.CLASSIFIED,
        payload=payload,
        reason_code=None,
    )


def _blocked(
    reason_code: str,
    payload: dict[str, Any] | None = None,
) -> ClassifiedFact:
    return ClassifiedFact(
        presence=FactPresence.BLOCKED,
        payload={} if payload is None else payload,
        reason_code=reason_code,
    )


def _facts(**overrides: ClassifiedFact) -> ClassifiedStatusFacts:
    payload = {slot: _absent() for slot in _FACT_SLOTS}
    payload.update(overrides)
    return ClassifiedStatusFacts(**payload)


def _main_state_payload(
    *,
    state: str = "GREEN_VALIDATED",
    observed_main_head_sha: str = _REFERENCE_HEAD_PLACEHOLDER,
    reason_code: str = "MAIN_STATE_GREEN",
) -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "state": state,
        "main_branch": _REFERENCE_MAIN_BRANCH,
        "validated_result_sha": _REFERENCE_RESULT_SHA,
        "last_tock_sha": _REFERENCE_TOCK_SHA,
        "observed_main_head_sha": observed_main_head_sha,
        "reason_code": reason_code,
        "updated_at_utc": _REFERENCE_UPDATED_AT,
    }


def _pending_payload() -> dict[str, Any]:
    return {
        "artifact_class": _ARTIFACT_CLASS,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "promotion_class": "GREEN_PROMOTION",
    }


def _journal_payload(*, phase: str = "PENDING") -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "phase": phase,
        "task_id": _REFERENCE_TASK_ID,
        "origin_run_id": _REFERENCE_ORIGIN_RUN_ID,
        "drain_run_id": _REFERENCE_DRAIN_RUN_ID,
        "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
        "result_sha": _REFERENCE_RESULT_SHA,
        "reason_code": "JOURNAL_PENDING",
    }


def _consumption_payload() -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "consumed_promotions": [
            {
                "promotion_attempt_id": _REFERENCE_PROMOTION_ATTEMPT_ID,
                "result_sha": _REFERENCE_RESULT_SHA,
                "tock_sha": _REFERENCE_TOCK_SHA,
            }
        ],
    }


def _drain_payload(*, status: str = "NO_PENDING") -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "status": status,
        "drain_run_id": _REFERENCE_DRAIN_RUN_ID,
        "observed_main_head_sha": _REFERENCE_HEAD_PLACEHOLDER,
        "reason_code": "NO_PENDING_PROMOTION",
    }


def _decision_payload(
    *,
    request_state: str = "ready",
    orchestrator_state: str = "draft",
    validator_state: str = "missing",
) -> dict[str, Any]:
    return {
        "request": {"path": "decision_request.yaml", "state": request_state},
        "orchestrator": {
            "path": "orchestrator_decision.yaml",
            "state": orchestrator_state,
        },
        "validator": {"path": "validator_decision.yaml", "state": validator_state},
    }


def _positive_facts(observed_head: str) -> ClassifiedStatusFacts:
    return _facts(
        runner_source_identity=_absent(),
        target_main_head=_classified({"sha": observed_head}),
        main_state=_classified(_main_state_payload(observed_main_head_sha=observed_head)),
        active_task_identity=_classified({"task_id": _REFERENCE_TASK_ID}),
        run_phase=_classified({"value": "AWAITING_DECISIONS"}),
        pending=_classified(_pending_payload()),
        journal=_classified(_journal_payload()),
        consumption=_classified(_consumption_payload()),
        drain=_classified(_drain_payload()),
        decisions=_classified(_decision_payload()),
        next_external_action=_classified({"value": "AWAITING_DECISIONS"}),
        reason_code=_classified({"value": "DECISIONS_INCOMPLETE"}),
        drift_warning=_classified({"warning": None}),
    )


def _seed_contradictory_runtime(runtime_root: Path, *, observed_head: str) -> None:
    _write_yaml(
        runtime_root / "main_state.yaml",
        _main_state_payload(
            state="BLOCKED",
            observed_main_head_sha=observed_head,
            reason_code="DISK_SAYS_BLOCKED",
        ),
    )
    _write_yaml(
        runtime_root / "pending_promotion.yaml",
        {
            **_pending_payload(),
            "task_id": _FOREIGN_TASK_ID,
            "result_sha": _FOREIGN_RESULT_SHA,
        },
    )
    _write_yaml(
        runtime_root / "promotion_journal.yaml",
        _journal_payload(phase="FINALIZED"),
    )
    _write_yaml(
        runtime_root / "promotion_consumed.yaml",
        {
            "schema_version": _SCHEMA_VERSION,
            "consumed_promotions": [],
        },
    )
    _write_yaml(
        runtime_root / "drain_outcome.yaml",
        _drain_payload(status="DRAINED"),
    )
    _write_yaml(
        runtime_root / "decision_request.yaml",
        {"schema_version": _SCHEMA_VERSION, "request_type": "decision_request"},
    )
    _write_yaml(
        runtime_root / "orchestrator_decision.yaml",
        {"schema_version": _SCHEMA_VERSION, "ready": True},
    )
    _write_yaml(
        runtime_root / "validator_decision.yaml",
        {"schema_version": _SCHEMA_VERSION, "ready": True, "verdict": "GO"},
    )
    _write_yaml(
        runtime_root / "runner_source_identity.yaml",
        {
            "schema_version": _SCHEMA_VERSION,
            "status": "SOURCE_AUTHORITY_OK",
            "source_sha": _FOREIGN_RESULT_SHA,
        },
    )
    _write_bytes(runtime_root / "runner_git.lock", b"synthetic-lock-owner\n")


def _dirtify_worktree(repo_root: Path) -> None:
    (repo_root / "unstaged.txt").write_text("dirty worktree\n", encoding="utf-8")
    (repo_root / "staged.txt").write_text("staged but uncommitted\n", encoding="utf-8")
    _run_git(repo_root, "add", "--", "staged.txt")


def _file_fingerprint(path: Path) -> str:
    mode = stat.S_IMODE(path.stat().st_mode)
    return f"{mode:o}:{_sha256(path.read_bytes())}"


def _tree_digest(root: Path) -> dict[str, str]:
    digest: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if ".git" in path.parts:
            continue
        digest[path.relative_to(root).as_posix()] = _file_fingerprint(path)
    return digest


def _git_identity(repo_root: Path) -> dict[str, str]:
    git_dir = repo_root / ".git"
    index_path = git_dir / "index"
    return {
        "head_sha": _run_git(repo_root, "rev-parse", "HEAD").strip(),
        "head_ref": (git_dir / "HEAD").read_text(encoding="utf-8"),
        "head_bytes": _sha256((git_dir / "HEAD").read_bytes()),
        "index_bytes": _sha256(index_path.read_bytes()) if index_path.exists() else "",
        "stage": _run_git(repo_root, "ls-files", "--stage"),
        "refs": _run_git(
            repo_root,
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ),
        "porcelain": _run_git(repo_root, "status", "--porcelain=v1", "-uall"),
        "diff_cached": _run_git(repo_root, "diff", "--cached"),
        "diff_worktree": _run_git(repo_root, "diff"),
        "count_objects": _run_git(repo_root, "count-objects", "-v"),
    }


def _runtime_identity(runtime_root: Path) -> dict[str, str]:
    identity: dict[str, str] = {}
    for name in _RUNTIME_ARTIFACT_NAMES:
        path = runtime_root / name
        identity[f"exists:{name}"] = str(path.exists())
        if path.exists():
            identity[f"bytes:{name}"] = _file_fingerprint(path)
    identity["tree"] = yaml.safe_dump(_tree_digest(runtime_root), sort_keys=True)
    return identity


def _snapshot(repo_root: Path, runtime_root: Path) -> dict[str, Any]:
    return {
        "git": _git_identity(repo_root),
        "worktree": _tree_digest(repo_root),
        "runtime": _runtime_identity(runtime_root),
        "runtime_exists": runtime_root.exists(),
    }


def _project(
    facts: ClassifiedStatusFacts,
    *,
    repo_root: Path,
    runtime_root: Path,
) -> StatusView:
    return project_readonly_status(
        classified_facts=facts,
        target_worktree=repo_root,
        runtime_root=runtime_root,
    )


def _expect_status_error(
    reason_code: str,
    *,
    facts: object,
    repo_root: Path,
    runtime_root: Path,
) -> StatusViewError:
    with pytest.raises(StatusViewError, match=reason_code) as exc_info:
        project_readonly_status(
            classified_facts=facts,  # type: ignore[arg-type]
            target_worktree=repo_root,
            runtime_root=runtime_root,
        )
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _project_without_mutation(
    facts: ClassifiedStatusFacts,
    *,
    repo_root: Path,
    runtime_root: Path,
) -> StatusView:
    before = _snapshot(repo_root, runtime_root)
    view = _project(facts, repo_root=repo_root, runtime_root=runtime_root)
    assert _snapshot(repo_root, runtime_root) == before
    return view


def test_public_exports_are_the_named_readonly_status_surface() -> None:
    exported = {name for name in dir(status_module) if not name.startswith("_")}
    for name in _REQUIRED_EXPORTS:
        assert name in exported
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported
    assert "cli" not in exported
    assert "run" not in exported
    assert "init" not in exported


def test_fact_presence_is_absent_classified_or_blocked() -> None:
    values = {presence.value for presence in FactPresence}
    assert values == {"ABSENT", "CLASSIFIED", "BLOCKED"}
    assert len(list(FactPresence)) == 3


def test_allowed_reason_codes_are_view_local_and_exclude_authority_pass() -> None:
    codes = allowed_reason_codes()
    assert codes == tuple(sorted(codes))
    for code in _REQUIRED_REASON_CODES:
        assert code in codes
    assert "SOURCE_AUTHORITY_OK" not in codes
    assert "RECOVERED" not in codes
    assert "SETTLED" not in codes


def test_project_readonly_status_requires_classified_facts_and_observation_roots() -> None:
    signature = inspect.signature(project_readonly_status)
    assert list(signature.parameters) == list(_PROJECT_REQUIRED_PARAMETERS)
    for name in _PROJECT_REQUIRED_PARAMETERS:
        parameter = signature.parameters[name]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty
    for name in _FORBIDDEN_PARAMETER_NAMES:
        assert name not in signature.parameters


def test_classified_status_facts_require_every_public_slot() -> None:
    signature = inspect.signature(ClassifiedStatusFacts)
    assert list(signature.parameters) == list(_FACT_SLOTS)
    for name in _FACT_SLOTS:
        parameter = signature.parameters[name]
        assert parameter.default is inspect.Parameter.empty


def test_classified_facts_are_projected_without_rereading_persistence(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    facts = _positive_facts(observed_head)

    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )

    assert isinstance(view, StatusView)
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    assert view.runner_source_identity.presence is FactPresence.ABSENT
    assert view.target_main_head.payload == {"sha": observed_head}
    assert view.main_state.payload == _main_state_payload(
        observed_main_head_sha=observed_head
    )
    assert view.active_task_identity.payload == {"task_id": _REFERENCE_TASK_ID}
    assert view.run_phase.payload == {"value": "AWAITING_DECISIONS"}
    assert view.pending.payload == _pending_payload()
    assert view.pending.payload["artifact_class"] == _ARTIFACT_CLASS
    assert view.journal.payload == _journal_payload()
    assert view.consumption.payload == _consumption_payload()
    assert view.drain.payload == _drain_payload()
    assert view.decisions.payload == _decision_payload()
    assert view.next_external_action.payload == {"value": "AWAITING_DECISIONS"}
    assert view.reason_code.payload == {"value": "DECISIONS_INCOMPLETE"}
    assert view.drift_warning.payload == {"warning": None}
    assert view.projection_reason_code != "SOURCE_AUTHORITY_OK"
    assert view.projection_reason_code != "SETTLED"


def test_pending_projection_keeps_non_authorizing_intent_class(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    facts = _facts(
        pending=_classified(_pending_payload()),
        next_external_action=_classified({"value": "PENDING_PROMOTION"}),
        reason_code=_classified({"value": "PENDING_INTENT_ONLY"}),
    )
    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.pending.presence is FactPresence.CLASSIFIED
    assert view.pending.payload["artifact_class"] == _ARTIFACT_CLASS
    assert view.next_external_action.payload == {"value": "PENDING_PROMOTION"}
    assert "tock_sha" not in view.pending.payload
    assert "journal_start" not in view.pending.payload


def test_decision_states_missing_draft_ready_invalid_are_projected(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    facts = _facts(
        decisions=_classified(
            _decision_payload(
                request_state="ready",
                orchestrator_state="invalid",
                validator_state="missing",
            )
        ),
        next_external_action=_classified({"value": "AWAITING_DECISIONS"}),
        reason_code=_classified({"value": "VALIDATOR_MISSING"}),
    )
    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.decisions.payload["request"]["state"] == "ready"
    assert view.decisions.payload["orchestrator"]["state"] == "invalid"
    assert view.decisions.payload["validator"]["state"] == "missing"
    assert view.next_external_action.payload == {"value": "AWAITING_DECISIONS"}


def test_absent_facts_are_not_invented_from_runtime_files(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)

    view = _project_without_mutation(
        _facts(),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )

    for slot in _FACT_SLOTS:
        fact = getattr(view, slot)
        assert fact.presence is FactPresence.ABSENT
        assert fact.payload is None
        assert fact.reason_code is None
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    assert view.next_external_action.payload is None
    assert view.main_state.payload is None
    assert view.runner_source_identity.payload is None


def test_classified_null_active_task_does_not_infer_terminal_rest(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    facts = _facts(active_task_identity=_classified({"task_id": None}))
    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.active_task_identity.payload == {"task_id": None}
    assert view.next_external_action.presence is FactPresence.ABSENT
    assert view.run_phase.presence is FactPresence.ABSENT


def test_blocked_facts_are_not_reinterpreted_as_recovery_or_run_success(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    facts = _facts(
        main_state=_blocked(
            "PROMOTION_CONSUMPTION_CONFLICT",
            _main_state_payload(state="BLOCKED", observed_main_head_sha=observed_head),
        ),
        pending=_blocked("PENDING_PROMOTION_INVALID", _pending_payload()),
        next_external_action=_classified({"value": "BLOCKED"}),
        reason_code=_classified({"value": "PROMOTION_CONSUMPTION_CONFLICT"}),
    )
    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.main_state.presence is FactPresence.BLOCKED
    assert view.main_state.reason_code == "PROMOTION_CONSUMPTION_CONFLICT"
    assert view.pending.presence is FactPresence.BLOCKED
    assert view.next_external_action.payload == {"value": "BLOCKED"}
    assert view.projection_reason_code not in {"RECOVERED", "SETTLED", "SOURCE_AUTHORITY_OK"}
    assert view.run_phase.presence is FactPresence.ABSENT


def test_disk_payloads_contradicting_classified_facts_are_ignored(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    facts = _positive_facts(observed_head)
    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    disk_main = yaml.safe_load((runtime_root / "main_state.yaml").read_text(encoding="utf-8"))
    disk_pending = yaml.safe_load(
        (runtime_root / "pending_promotion.yaml").read_text(encoding="utf-8")
    )
    disk_source = yaml.safe_load(
        (runtime_root / "runner_source_identity.yaml").read_text(encoding="utf-8")
    )
    assert disk_main["state"] == "BLOCKED"
    assert view.main_state.payload["state"] == "GREEN_VALIDATED"
    assert disk_pending["task_id"] == _FOREIGN_TASK_ID
    assert view.pending.payload["task_id"] == _REFERENCE_TASK_ID
    assert disk_source["status"] == "SOURCE_AUTHORITY_OK"
    assert view.runner_source_identity.presence is FactPresence.ABSENT


def test_absent_source_identity_is_not_source_authority_ok(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    view = _project_without_mutation(
        _facts(runner_source_identity=_absent()),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.runner_source_identity.presence is FactPresence.ABSENT
    assert view.runner_source_identity.payload is None
    rendered = yaml.safe_dump(view.runner_source_identity.payload)
    assert "SOURCE_AUTHORITY_OK" not in rendered
    assert "SOURCE_AUTHORITY_OK" not in view.projection_reason_code


def test_source_identity_file_is_not_interpreted_as_authority(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    view = _project_without_mutation(
        _facts(),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert (runtime_root / "runner_source_identity.yaml").is_file()
    assert view.runner_source_identity.presence is FactPresence.ABSENT
    assert view.main_state.presence is FactPresence.ABSENT


def test_classified_source_mismatch_is_projected_without_aggregating_disk(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    facts = _facts(
        runner_source_identity=_classified(
            {
                "diagnostic": "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH",
                "interpreting_runtime": False,
            }
        ),
        main_state=_absent(),
        next_external_action=_classified({"value": "BLOCKED"}),
        reason_code=_classified({"value": "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH"}),
    )
    view = _project_without_mutation(
        facts,
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.runner_source_identity.payload["interpreting_runtime"] is False
    assert view.main_state.presence is FactPresence.ABSENT
    assert view.main_state.payload is None
    assert view.next_external_action.payload == {"value": "BLOCKED"}


def test_status_does_not_mutate_git_refs_head_index_or_worktree(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    _dirtify_worktree(repo_root)
    before = _git_identity(repo_root)
    assert "unstaged.txt" in before["porcelain"]
    assert "staged.txt" in before["stage"]

    _project(
        _positive_facts(observed_head),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )

    after = _git_identity(repo_root)
    assert after == before
    assert after["head_sha"] == observed_head
    assert after["refs"] == before["refs"]
    assert after["index_bytes"] == before["index_bytes"]
    assert after["porcelain"] == before["porcelain"]
    assert (repo_root / "unstaged.txt").read_text(encoding="utf-8") == "dirty worktree\n"


def test_status_does_not_mutate_runtime_artifact_bytes_or_create_lock(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    before = _runtime_identity(runtime_root)

    _project(
        _positive_facts(observed_head),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )

    after = _runtime_identity(runtime_root)
    assert after == before
    assert after["exists:runner_git.lock"] == "True"
    assert after["bytes:runner_git.lock"] == before["bytes:runner_git.lock"]


def test_status_does_not_create_runtime_or_lock_when_runtime_is_empty(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    before = _snapshot(repo_root, runtime_root)
    view = _project(_facts(), repo_root=repo_root, runtime_root=runtime_root)
    assert _snapshot(repo_root, runtime_root) == before
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    for name in _RUNTIME_ARTIFACT_NAMES:
        assert not (runtime_root / name).exists()


def test_repeated_status_call_is_idempotent_and_still_read_only(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    _dirtify_worktree(repo_root)
    facts = _positive_facts(observed_head)
    before = _snapshot(repo_root, runtime_root)
    first = _project(facts, repo_root=repo_root, runtime_root=runtime_root)
    second = _project(facts, repo_root=repo_root, runtime_root=runtime_root)
    assert _snapshot(repo_root, runtime_root) == before
    assert first.projection_reason_code == second.projection_reason_code
    assert first.main_state.payload == second.main_state.payload
    assert first.pending.payload == second.pending.payload
    assert first.decisions.payload == second.decisions.payload


def test_next_external_action_is_projected_not_inferred(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    for action in _ALLOWED_NEXT_ACTIONS:
        view = _project_without_mutation(
            _facts(next_external_action=_classified({"value": action})),
            repo_root=repo_root,
            runtime_root=runtime_root,
        )
        assert view.next_external_action.payload == {"value": action}


def test_reason_codes_are_projected_from_classified_facts(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    view = _project_without_mutation(
        _facts(reason_code=_classified({"value": "ALREADY_CLASSIFIED_BLOCK"})),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert view.reason_code.payload == {"value": "ALREADY_CLASSIFIED_BLOCK"}
    assert view.reason_code.presence is FactPresence.CLASSIFIED


def test_recovered_or_reopen_outcomes_are_rejected_without_mutation(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    _dirtify_worktree(repo_root)
    before = _snapshot(repo_root, runtime_root)
    for action in _FORBIDDEN_NEXT_ACTIONS:
        error = _expect_status_error(
            "STATUS_FORBIDDEN_OUTCOME",
            facts=_facts(next_external_action=_classified({"value": action})),
            repo_root=repo_root,
            runtime_root=runtime_root,
        )
        assert error.reason_code == "STATUS_FORBIDDEN_OUTCOME"
        assert _snapshot(repo_root, runtime_root) == before


def test_invalid_classified_facts_fail_closed_without_mutation(tmp_path: Path) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    before = _snapshot(repo_root, runtime_root)

    _expect_status_error(
        "STATUS_FACTS_INVALID",
        facts="not-classified-facts",
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    _expect_status_error(
        "STATUS_PRESENCE_PAYLOAD_CONFLICT",
        facts=_facts(
            main_state=ClassifiedFact(
                presence=FactPresence.ABSENT,
                payload=_main_state_payload(),
                reason_code=None,
            )
        ),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    _expect_status_error(
        "STATUS_PRESENCE_PAYLOAD_CONFLICT",
        facts=_facts(
            main_state=ClassifiedFact(
                presence=FactPresence.CLASSIFIED,
                payload=None,
                reason_code=None,
            )
        ),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    _expect_status_error(
        "STATUS_PRESENCE_PAYLOAD_CONFLICT",
        facts=_facts(
            main_state=ClassifiedFact(
                presence=FactPresence.BLOCKED,
                payload=_main_state_payload(),
                reason_code=None,
            )
        ),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert _snapshot(repo_root, runtime_root) == before


def test_invalid_target_or_runtime_fails_closed_without_creating_state(
    tmp_path: Path,
) -> None:
    missing_target = tmp_path / "missing-target"
    existing_file = tmp_path / "not-a-repo"
    existing_file.write_text("not git\n", encoding="utf-8")
    runtime_root = _runtime_root(tmp_path)
    repo_root = _init_target_repo(tmp_path)
    missing_runtime = tmp_path / "missing-runtime"

    _expect_status_error(
        "STATUS_TARGET_INVALID",
        facts=_facts(),
        repo_root=missing_target,
        runtime_root=runtime_root,
    )
    _expect_status_error(
        "STATUS_TARGET_INVALID",
        facts=_facts(),
        repo_root=existing_file,
        runtime_root=runtime_root,
    )
    _expect_status_error(
        "STATUS_RUNTIME_ROOT_INVALID",
        facts=_facts(),
        repo_root=repo_root,
        runtime_root=missing_runtime,
    )
    assert not missing_target.exists()
    assert not missing_runtime.exists()
    assert existing_file.read_text(encoding="utf-8") == "not git\n"
    for name in _RUNTIME_ARTIFACT_NAMES:
        assert not (runtime_root / name).exists()


def test_status_module_does_not_claim_code_authority_or_recovery() -> None:
    exported = {name for name in dir(status_module) if not name.startswith("_")}
    for name in _FORBIDDEN_EXPORTS:
        assert name not in exported


@pytest.mark.parametrize(
    "malformed_payload",
    [{}, "AWAITING_DECISIONS"],
    ids=["mapping_without_value", "non_mapping"],
)
def test_malformed_classified_next_action_is_fail_closed_without_mutation(
    tmp_path: Path,
    malformed_payload: object,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    _dirtify_worktree(repo_root)
    before = _snapshot(repo_root, runtime_root)
    if isinstance(malformed_payload, dict):
        next_action = _classified(malformed_payload)
    else:
        next_action = ClassifiedFact(
            presence=FactPresence.CLASSIFIED,
            payload=malformed_payload,  # type: ignore[arg-type]
            reason_code=None,
        )

    error = _expect_status_error(
        "STATUS_PRESENCE_PAYLOAD_CONFLICT",
        facts=_facts(next_external_action=next_action),
        repo_root=repo_root,
        runtime_root=runtime_root,
    )
    assert error.reason_code == "STATUS_PRESENCE_PAYLOAD_CONFLICT"
    assert _snapshot(repo_root, runtime_root) == before


def test_projected_nested_payloads_are_isolated_from_caller_owned_input(
    tmp_path: Path,
) -> None:
    repo_root = _init_target_repo(tmp_path)
    runtime_root = _runtime_root(tmp_path)
    observed_head = _run_git(repo_root, "rev-parse", "HEAD").strip()
    _seed_contradictory_runtime(runtime_root, observed_head=observed_head)
    _dirtify_worktree(repo_root)
    caller_decisions = _decision_payload()
    caller_consumption = _consumption_payload()
    facts = _facts(
        decisions=_classified(caller_decisions),
        consumption=_classified(caller_consumption),
        next_external_action=_classified({"value": "AWAITING_DECISIONS"}),
    )
    before = _snapshot(repo_root, runtime_root)
    view = _project(facts, repo_root=repo_root, runtime_root=runtime_root)
    assert _snapshot(repo_root, runtime_root) == before

    assert view.decisions.payload is not caller_decisions
    assert view.decisions.payload is not facts.decisions.payload
    assert view.decisions.payload["request"] is not caller_decisions["request"]
    assert view.decisions.payload["orchestrator"] is not caller_decisions["orchestrator"]
    assert view.consumption.payload is not caller_consumption
    assert view.consumption.payload is not facts.consumption.payload
    assert (
        view.consumption.payload["consumed_promotions"]
        is not caller_consumption["consumed_promotions"]
    )
    assert (
        view.consumption.payload["consumed_promotions"][0]
        is not caller_consumption["consumed_promotions"][0]
    )
    assert view.decisions.payload["request"]["state"] == "ready"
    assert view.consumption.payload["consumed_promotions"][0]["result_sha"] == (
        _REFERENCE_RESULT_SHA
    )

    caller_decisions["request"]["state"] = "mutated-by-caller"
    caller_decisions["orchestrator"]["path"] = "mutated-by-caller.yaml"
    caller_consumption["consumed_promotions"][0]["result_sha"] = _FOREIGN_RESULT_SHA
    caller_consumption["consumed_promotions"][0]["tock_sha"] = _FOREIGN_RESULT_SHA
    assert view.decisions.payload["request"]["state"] == "ready"
    assert view.decisions.payload["orchestrator"]["path"] == "orchestrator_decision.yaml"
    assert view.consumption.payload["consumed_promotions"][0]["result_sha"] == (
        _REFERENCE_RESULT_SHA
    )
    assert view.consumption.payload["consumed_promotions"][0]["tock_sha"] == (
        _REFERENCE_TOCK_SHA
    )

    view.decisions.payload["request"]["state"] = "mutated-by-view"
    view.decisions.payload["validator"]["state"] = "mutated-by-view"
    view.consumption.payload["consumed_promotions"][0]["result_sha"] = (
        _REFERENCE_TOCK_SHA
    )
    view.consumption.payload["consumed_promotions"][0]["promotion_attempt_id"] = (
        "mutated-by-view"
    )
    assert caller_decisions["request"]["state"] == "mutated-by-caller"
    assert caller_decisions["orchestrator"]["path"] == "mutated-by-caller.yaml"
    assert caller_decisions["validator"]["state"] == "missing"
    assert caller_consumption["consumed_promotions"][0]["result_sha"] == (
        _FOREIGN_RESULT_SHA
    )
    assert caller_consumption["consumed_promotions"][0]["promotion_attempt_id"] == (
        _REFERENCE_PROMOTION_ATTEMPT_ID
    )
    assert facts.decisions.payload["request"]["state"] == "mutated-by-caller"
    assert facts.consumption.payload["consumed_promotions"][0]["result_sha"] == (
        _FOREIGN_RESULT_SHA
    )
    assert _snapshot(repo_root, runtime_root) == before


_CLASSIFY_AUTHORITY_FIRST_STATUS = "classify_authority_first_status"
_CLASSIFY_PARAMETERS = (
    "entry_point",
    "target_worktree",
    "runtime_root",
    "expected_source_sha",
    "loaded_module_files",
)
_SEMANTIC_RUNTIME_SLOTS = (
    "target_main_head",
    "main_state",
    "active_task_identity",
    "run_phase",
    "pending",
    "journal",
    "consumption",
    "drain",
    "decisions",
    "drift_warning",
)
_DECOY_MAIN_REASON = "DISK_SAYS_BLOCKED"
_CLASSIFIED_SOURCE_MARKER = "authority-classified-source-sha"


def _source_modules(topology: Mapping[str, Any]) -> dict[str, Path]:
    return {
        "task_transition_runner.cli": topology["entry"],
        "task_transition_runner.config": topology["core"],
    }


def _init_authority_topology(tmp_path: Path) -> dict[str, Any]:
    source = _init_target_repo(tmp_path, name="source-repo")
    package = source / "src" / "task_transition_runner"
    package.mkdir(parents=True)
    entry = package / "cli.py"
    core = package / "config.py"
    entry.write_text("# synthetic status entry\n", encoding="utf-8")
    core.write_text("# synthetic status core\n", encoding="utf-8")
    _run_git(source, "add", "--", "src/task_transition_runner/cli.py")
    _run_git(source, "add", "--", "src/task_transition_runner/config.py")
    _run_git(source, "commit", "-m", "synthetic status entry")
    target = _init_target_repo(tmp_path, name="target-repo")
    runtime = _runtime_root(tmp_path)
    return {
        "source": source,
        "entry": entry,
        "core": core,
        "target": target,
        "runtime": runtime,
        "source_sha": _run_git(source, "rev-parse", "HEAD").strip(),
        "target_sha": _run_git(target, "rev-parse", "HEAD").strip(),
    }


def _write_runtime_pin(runtime_root: Path, source_sha: str) -> None:
    _write_bytes(
        runtime_root / RUNTIME_SOURCE_PIN_NAME,
        (
            f"schema_version: '{_SCHEMA_VERSION}'\n"
            f"source_sha: '{source_sha}'\n"
        ).encode("utf-8"),
    )


def _render_status_view(view: StatusView) -> str:
    payload: dict[str, Any] = {"projection_reason_code": view.projection_reason_code}
    for slot in _FACT_SLOTS:
        fact = getattr(view, slot)
        payload[slot] = {
            "presence": fact.presence.value,
            "payload": fact.payload,
            "reason_code": fact.reason_code,
        }
    return yaml.safe_dump(payload, sort_keys=True)


def _assert_bytes_unchanged(
    *,
    source: Path,
    target: Path,
    runtime_root: Path,
    before_source: dict[str, str],
    before_target: dict[str, Any],
) -> None:
    assert _git_identity(source) == before_source
    assert _snapshot(target, runtime_root) == before_target


def _classify_authority_first_status(
    *,
    entry_point: Path,
    target_worktree: Path,
    runtime_root: Path,
    expected_source_sha: str | None = None,
    loaded_module_files: Mapping[str, Path | str] | None = None,
) -> StatusView:
    """Call the missing classifier, otherwise project absent placeholders."""

    classify = getattr(status_module, _CLASSIFY_AUTHORITY_FIRST_STATUS, None)
    if classify is None:
        return project_readonly_status(
            classified_facts=_facts(),
            target_worktree=target_worktree,
            runtime_root=runtime_root,
        )
    signature = inspect.signature(classify)
    assert tuple(signature.parameters) == _CLASSIFY_PARAMETERS
    for name in ("entry_point", "target_worktree", "runtime_root"):
        parameter = signature.parameters[name]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty
    for name in ("expected_source_sha", "loaded_module_files"):
        parameter = signature.parameters[name]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None
    return classify(
        entry_point=entry_point,
        target_worktree=target_worktree,
        runtime_root=runtime_root,
        expected_source_sha=expected_source_sha,
        loaded_module_files=loaded_module_files,
    )


def _assert_minimal_authority_diagnosis(view: StatusView, reason_code: str) -> None:
    rendered = _render_status_view(view)
    assert _DECOY_MAIN_REASON not in rendered
    assert _FOREIGN_TASK_ID not in rendered
    assert "SOURCE_AUTHORITY_OK" not in rendered
    assert "RECOVERED" not in rendered
    assert view.projection_reason_code == reason_code
    assert view.runner_source_identity.presence is FactPresence.CLASSIFIED
    assert view.runner_source_identity.payload == {
        "reason_code": reason_code,
        "interpreting_runtime": False,
    }
    assert view.reason_code.presence is FactPresence.CLASSIFIED
    assert view.reason_code.payload == {"value": reason_code}
    assert view.next_external_action.presence is FactPresence.CLASSIFIED
    assert view.next_external_action.payload == {"value": "BLOCKED"}
    for slot in _SEMANTIC_RUNTIME_SLOTS:
        fact = getattr(view, slot)
        assert fact.presence is FactPresence.ABSENT
        assert fact.payload is None
        assert fact.reason_code is None


def _prepare_mismatch(
    tmp_path: Path,
    case_id: str,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    _seed_contradictory_runtime(runtime, observed_head=topology["target_sha"])
    _write_runtime_pin(runtime, _FOREIGN_RESULT_SHA)
    modules = _source_modules(topology)
    expected_source_sha: str | None = topology["source_sha"]
    if case_id == "source_sha_mismatch":
        expected_source_sha = _FOREIGN_RESULT_SHA
        (topology["source"] / "untracked-status.txt").write_text(
            "dirty\n",
            encoding="utf-8",
        )
        reason_code = "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH"
    elif case_id == "source_dirty":
        (topology["source"] / "untracked-status.txt").write_text(
            "dirty\n",
            encoding="utf-8",
        )
        shadow = topology["target"] / "src" / "task_transition_runner" / "config.py"
        shadow.parent.mkdir(parents=True)
        shadow.write_text("# synthetic shadow\n", encoding="utf-8")
        modules["task_transition_runner.config"] = shadow
        reason_code = "SOURCE_AUTHORITY_SOURCE_DIRTY"
    elif case_id == "import_provenance_invalid":
        outside = tmp_path / "outside-core.py"
        outside.write_text("# outside source\n", encoding="utf-8")
        modules["task_transition_runner.config"] = outside
        reason_code = "SOURCE_AUTHORITY_IMPORT_PROVENANCE_INVALID"
    elif case_id == "import_shadowed":
        shadow = topology["target"] / "src" / "task_transition_runner" / "config.py"
        shadow.parent.mkdir(parents=True)
        shadow.write_text("# synthetic shadow\n", encoding="utf-8")
        modules["task_transition_runner.config"] = shadow
        reason_code = "SOURCE_AUTHORITY_IMPORT_SHADOWED"
    elif case_id == "runtime_pin_mismatch":
        reason_code = "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH"
    else:
        raise AssertionError(case_id)
    return (
        topology,
        reason_code,
        {
            "entry_point": topology["entry"],
            "target_worktree": topology["target"],
            "runtime_root": runtime,
            "expected_source_sha": expected_source_sha,
            "loaded_module_files": modules,
        },
    )


@pytest.mark.parametrize(
    "case_id",
    (
        "source_sha_mismatch",
        "source_dirty",
        "import_provenance_invalid",
        "import_shadowed",
        "runtime_pin_mismatch",
    ),
)
def test_authority_mismatch_is_minimal_noninterpreting_diagnosis(
    tmp_path: Path,
    case_id: str,
) -> None:
    topology, reason_code, arguments = _prepare_mismatch(tmp_path, case_id)
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(topology["target"], topology["runtime"])

    view = _classify_authority_first_status(**arguments)

    _assert_bytes_unchanged(
        source=topology["source"],
        target=topology["target"],
        runtime_root=topology["runtime"],
        before_source=before_source,
        before_target=before_target,
    )
    _assert_minimal_authority_diagnosis(view, reason_code)
    assert view.projection_reason_code != "STATUS_FACTS_INVALID"
    assert view.projection_reason_code != "STATUS_VIEW_PROJECTED"


def test_authorized_status_classifies_every_public_fact(tmp_path: Path) -> None:
    topology = _init_authority_topology(tmp_path)
    target = topology["target"]
    runtime = topology["runtime"]
    target_sha = topology["target_sha"]
    _write_yaml(runtime / "main_state.yaml", _main_state_payload(observed_main_head_sha=target_sha))
    _write_yaml(runtime / "pending_promotion.yaml", _pending_payload())
    _write_yaml(runtime / "promotion_journal.yaml", _journal_payload())
    _write_yaml(runtime / "promotion_consumed.yaml", _consumption_payload())
    _write_yaml(runtime / "drain_outcome.yaml", _drain_payload())
    _write_yaml(runtime / "decision_request.yaml", {"state": "ready"})
    _write_yaml(runtime / "orchestrator_decision.yaml", {"state": "draft"})
    _write_yaml(
        runtime / "runner_source_identity.yaml",
        {
            "schema_version": _SCHEMA_VERSION,
            "status": "SOURCE_AUTHORITY_OK",
            "source_sha": _FOREIGN_RESULT_SHA,
        },
    )
    _write_runtime_pin(runtime, topology["source_sha"])
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(target, runtime)

    view = _classify_authority_first_status(
        entry_point=topology["entry"],
        target_worktree=target,
        runtime_root=runtime,
        expected_source_sha=topology["source_sha"],
        loaded_module_files=_source_modules(topology),
    )

    _assert_bytes_unchanged(
        source=topology["source"],
        target=target,
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    assert view.runner_source_identity.payload == {
        "source_sha": topology["source_sha"],
        "source_root": str(topology["source"].resolve()),
        "entry_point": str(topology["entry"].resolve()),
        "runtime_pin_sha": topology["source_sha"],
    }
    assert "SOURCE_AUTHORITY_OK" not in _render_status_view(view)
    assert view.target_main_head.payload == {"sha": target_sha}
    assert view.main_state.payload == _main_state_payload(observed_main_head_sha=target_sha)
    assert view.active_task_identity.payload == {"task_id": _REFERENCE_TASK_ID}
    assert view.run_phase.payload == {"value": "AWAITING_DECISIONS"}
    assert view.pending.payload == _pending_payload()
    assert view.pending.payload["artifact_class"] == _ARTIFACT_CLASS
    assert view.journal.payload == _journal_payload()
    assert view.consumption.payload == _consumption_payload()
    assert view.drain.payload == _drain_payload()
    assert view.decisions.payload == _decision_payload(
        request_state="ready",
        orchestrator_state="draft",
        validator_state="missing",
    )
    assert view.next_external_action.payload == {"value": "AWAITING_DECISIONS"}
    assert view.reason_code.payload == {"value": "DECISIONS_INCOMPLETE"}
    assert view.drift_warning.payload == {"warning": None}
    assert view.next_external_action.payload["value"] != "RECOVERED"
    assert view.projection_reason_code != "RECOVERED"


def test_pending_only_unmanaged_runtime_is_classified_without_pin(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    _write_yaml(runtime / "pending_promotion.yaml", _pending_payload())
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(topology["target"], runtime)

    view = _classify_authority_first_status(
        entry_point=topology["entry"],
        target_worktree=topology["target"],
        runtime_root=runtime,
        loaded_module_files=_source_modules(topology),
    )

    _assert_bytes_unchanged(
        source=topology["source"],
        target=topology["target"],
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    assert view.runner_source_identity.presence is FactPresence.CLASSIFIED
    assert view.runner_source_identity.payload == {
        "source_sha": topology["source_sha"],
        "source_root": str(topology["source"].resolve()),
        "entry_point": str(topology["entry"].resolve()),
        "runtime_pin_sha": None,
    }
    assert view.target_main_head.presence is FactPresence.CLASSIFIED
    assert view.target_main_head.payload == {"sha": topology["target_sha"]}
    assert view.pending.presence is FactPresence.CLASSIFIED
    assert view.pending.payload == _pending_payload()
    assert view.active_task_identity.payload == {"task_id": _REFERENCE_TASK_ID}
    assert view.main_state.presence is FactPresence.ABSENT
    assert view.journal.presence is FactPresence.ABSENT
    assert view.consumption.presence is FactPresence.ABSENT
    assert view.drain.presence is FactPresence.ABSENT
    assert view.decisions.presence is FactPresence.ABSENT
    assert view.drift_warning.presence is FactPresence.ABSENT
    assert view.run_phase.payload == {"value": "PENDING_PROMOTION"}
    assert view.next_external_action.payload == {"value": "PENDING_PROMOTION"}
    assert view.reason_code.payload == {"value": "PENDING_INTENT_ONLY"}
    assert not (runtime / RUNTIME_SOURCE_PIN_NAME).exists()


def test_non_mapping_main_state_is_facts_invalid_after_authority(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    _write_runtime_pin(runtime, topology["source_sha"])
    _write_bytes(runtime / "main_state.yaml", b"[]\n")
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(topology["target"], runtime)
    raised: StatusViewError | None = None

    try:
        _classify_authority_first_status(
            entry_point=topology["entry"],
            target_worktree=topology["target"],
            runtime_root=runtime,
            loaded_module_files=_source_modules(topology),
        )
    except StatusViewError as exc:
        raised = exc

    _assert_bytes_unchanged(
        source=topology["source"],
        target=topology["target"],
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    assert raised is not None, (
        "Current status projects absent placeholders without "
        "authority-first classification"
    )
    assert raised.reason_code == "STATUS_FACTS_INVALID"


def test_null_pending_document_is_presence_payload_conflict(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    target_sha = topology["target_sha"]
    _write_runtime_pin(runtime, topology["source_sha"])
    _write_yaml(
        runtime / "main_state.yaml",
        _main_state_payload(observed_main_head_sha=target_sha),
    )
    _write_bytes(runtime / "pending_promotion.yaml", b"null\n")
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(topology["target"], runtime)
    raised: StatusViewError | None = None

    try:
        _classify_authority_first_status(
            entry_point=topology["entry"],
            target_worktree=topology["target"],
            runtime_root=runtime,
            loaded_module_files=_source_modules(topology),
        )
    except StatusViewError as exc:
        raised = exc

    _assert_bytes_unchanged(
        source=topology["source"],
        target=topology["target"],
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    assert raised is not None, (
        "Current status projects absent placeholders without "
        "authority-first classification"
    )
    assert raised.reason_code == "STATUS_PRESENCE_PAYLOAD_CONFLICT"


_MAIN_HEAD_DRIFT = "MAIN_HEAD_DRIFT"


def _project_authorized(topology: Mapping[str, Any]) -> StatusView:
    runtime = topology["runtime"]
    _write_runtime_pin(runtime, topology["source_sha"])
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(topology["target"], runtime)
    view = _classify_authority_first_status(
        entry_point=topology["entry"],
        target_worktree=topology["target"],
        runtime_root=runtime,
        expected_source_sha=topology["source_sha"],
        loaded_module_files=_source_modules(topology),
    )
    _assert_bytes_unchanged(
        source=topology["source"],
        target=topology["target"],
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    return view


def test_observed_head_drift_is_classified_without_rewriting_state(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    observed = _FOREIGN_RESULT_SHA
    _write_yaml(
        runtime / "main_state.yaml",
        _main_state_payload(observed_main_head_sha=observed),
    )
    _write_yaml(runtime / "pending_promotion.yaml", _pending_payload())
    _write_yaml(runtime / "promotion_journal.yaml", _journal_payload())
    _write_yaml(runtime / "promotion_consumed.yaml", _consumption_payload())
    _write_yaml(runtime / "drain_outcome.yaml", _drain_payload())
    _write_yaml(runtime / "decision_request.yaml", {"state": "ready"})
    _write_yaml(runtime / "orchestrator_decision.yaml", {"state": "draft"})

    view = _project_authorized(topology)

    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    assert view.drift_warning.presence is FactPresence.CLASSIFIED
    assert view.drift_warning.payload == {"warning": _MAIN_HEAD_DRIFT}
    assert view.target_main_head.payload == {"sha": topology["target_sha"]}
    assert view.main_state.payload["observed_main_head_sha"] == observed
    assert view.main_state.payload["observed_main_head_sha"] != topology["target_sha"]
    assert view.decisions.payload == _decision_payload(
        request_state="ready",
        orchestrator_state="draft",
        validator_state="missing",
    )
    assert view.next_external_action.payload == {"value": "AWAITING_DECISIONS"}
    assert view.run_phase.payload == {"value": "AWAITING_DECISIONS"}
    assert view.reason_code.payload == {"value": "DECISIONS_INCOMPLETE"}
    assert view.next_external_action.payload["value"] != "RECOVERED"
    assert view.next_external_action.payload["value"] != "BLOCKED"


def test_present_decision_without_valid_state_is_classified_invalid(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    _write_yaml(
        runtime / "main_state.yaml",
        _main_state_payload(observed_main_head_sha=topology["target_sha"]),
    )
    _write_yaml(runtime / "decision_request.yaml", {"schema_version": _SCHEMA_VERSION})
    _write_yaml(runtime / "orchestrator_decision.yaml", {"state": "ready"})
    _write_yaml(runtime / "validator_decision.yaml", {"state": "ready"})

    view = _project_authorized(topology)

    assert view.decisions.presence is FactPresence.CLASSIFIED
    assert view.decisions.payload == _decision_payload(
        request_state="invalid",
        orchestrator_state="ready",
        validator_state="ready",
    )
    assert view.next_external_action.payload == {"value": "AWAITING_DECISIONS"}
    assert view.run_phase.payload == {"value": "AWAITING_DECISIONS"}
    assert view.reason_code.payload == {"value": "DECISIONS_INCOMPLETE"}
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"


def test_three_ready_decisions_select_pending_promotion(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    _write_yaml(
        runtime / "main_state.yaml",
        _main_state_payload(observed_main_head_sha=topology["target_sha"]),
    )
    _write_yaml(runtime / "pending_promotion.yaml", _pending_payload())
    _write_yaml(runtime / "decision_request.yaml", {"state": "ready"})
    _write_yaml(runtime / "orchestrator_decision.yaml", {"state": "ready"})
    _write_yaml(runtime / "validator_decision.yaml", {"state": "ready"})

    view = _project_authorized(topology)

    assert view.decisions.presence is FactPresence.CLASSIFIED
    assert view.decisions.payload == _decision_payload(
        request_state="ready",
        orchestrator_state="ready",
        validator_state="ready",
    )
    assert view.next_external_action.presence is FactPresence.CLASSIFIED
    assert view.next_external_action.payload == {"value": "PENDING_PROMOTION"}
    assert view.run_phase.payload == {"value": "PENDING_PROMOTION"}
    assert view.reason_code.payload == {"value": "PENDING_INTENT_ONLY"}
    assert view.drift_warning.payload == {"warning": None}
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"


def test_persisted_recovered_fact_is_visible_without_recovery_authority(
    tmp_path: Path,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    _write_yaml(
        runtime / "main_state.yaml",
        _main_state_payload(observed_main_head_sha=topology["target_sha"]),
    )
    _write_yaml(runtime / "decision_request.yaml", {"state": "ready"})
    _write_yaml(runtime / "orchestrator_decision.yaml", {"state": "draft"})
    _write_yaml(runtime / "drain_outcome.yaml", _drain_payload(status="RECOVERED"))

    view = _project_authorized(topology)

    assert view.drain.presence is FactPresence.CLASSIFIED
    assert view.drain.payload == _drain_payload(status="RECOVERED")
    assert view.drain.payload["status"] == "RECOVERED"
    assert view.next_external_action.payload == {"value": "AWAITING_DECISIONS"}
    assert view.run_phase.payload == {"value": "AWAITING_DECISIONS"}
    assert view.reason_code.payload == {"value": "DECISIONS_INCOMPLETE"}
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    assert view.runner_source_identity.payload["source_sha"] == topology["source_sha"]
    assert view.next_external_action.payload["value"] != "RECOVERED"
    assert view.reason_code.payload["value"] != "RECOVERED"
    assert view.projection_reason_code != "RECOVERED"
    assert "SOURCE_AUTHORITY_OK" not in _render_status_view(view)


def _write_status_cli_world(tmp_path: Path) -> tuple[Path, Path, Path]:
    target = _init_target_repo(tmp_path, name="cli-target")
    runtime = tmp_path / "runner-runtime"
    runtime.mkdir()
    config_root = tmp_path / "config-root"
    prompts = config_root / "prompts"
    prompts.mkdir(parents=True)
    task_source = config_root / "tasks.yaml"
    task_source.write_text("active_task: null\n", encoding="utf-8")
    for role in ("developer", "orchestrator", "validator"):
        (prompts / f"{role}.md").write_text(f"synthetic {role} prompt\n", encoding="utf-8")
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "target": {
            "repo_root": str(target.resolve()),
            "main_branch": _REFERENCE_MAIN_BRANCH,
        },
        "runtime_root": str(runtime.resolve()),
        "task_source": str(task_source.resolve()),
        "baseline_validation": {"command": "true", "timeout_seconds": 30},
        "adapter": {
            "kind": "cursor",
            "executable": "cursor-agent",
            "model": None,
            "timeout_seconds": 1800,
        },
        "prompts": {
            "developer": str((prompts / "developer.md").resolve()),
            "orchestrator": str((prompts / "orchestrator.md").resolve()),
            "validator": str((prompts / "validator.md").resolve()),
        },
    }
    config_path = config_root / "runner.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path, target, runtime


def test_status_cli_mismatch_uses_classifier_without_runtime_semantics(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, target, runtime = _write_status_cli_world(tmp_path)
    _seed_contradictory_runtime(
        runtime,
        observed_head=_run_git(target, "rev-parse", "HEAD").strip(),
    )
    _write_runtime_pin(runtime, _FOREIGN_RESULT_SHA)
    calls: list[dict[str, Any]] = []

    def _fake_classify(**kwargs: Any) -> StatusView:
        calls.append(kwargs)
        slots = {slot: _absent() for slot in _FACT_SLOTS}
        slots["runner_source_identity"] = _classified(
            {
                "reason_code": "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH",
                "interpreting_runtime": False,
            }
        )
        slots["reason_code"] = _classified(
            {"value": "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH"}
        )
        slots["next_external_action"] = _classified({"value": "BLOCKED"})
        return StatusView(
            projection_reason_code="SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH",
            **slots,
        )

    monkeypatch.setattr(
        status_module,
        _CLASSIFY_AUTHORITY_FIRST_STATUS,
        _fake_classify,
        raising=False,
    )
    monkeypatch.setattr(
        cli_module,
        _CLASSIFY_AUTHORITY_FIRST_STATUS,
        _fake_classify,
        raising=False,
    )
    before = _snapshot(target, runtime)

    exit_code = cli_module.main(["status", "--config", str(config_path), "--json"])
    stdout = capsys.readouterr().out

    assert _snapshot(target, runtime) == before
    assert calls, (
        "Current status projects absent placeholders without "
        "authority-first classification"
    )
    assert exit_code == 1
    assert Path(calls[0]["entry_point"]).resolve() == Path(cli_module.__file__).resolve()
    assert Path(calls[0]["target_worktree"]).resolve() == target.resolve()
    assert Path(calls[0]["runtime_root"]).resolve() == runtime.resolve()
    payload = json.loads(stdout)
    assert payload["ok"] is False
    assert payload["command"] == "status"
    assert payload["outcome"] == "BLOCKED"
    assert payload["reason_code"] == "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH"
    assert _DECOY_MAIN_REASON not in stdout
    assert "SOURCE_AUTHORITY_OK" not in stdout
    assert "RECOVERED" not in stdout


def test_status_cli_projects_classified_facts_after_authority(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, target, runtime = _write_status_cli_world(tmp_path)
    _write_yaml(
        runtime / "main_state.yaml",
        {
            "schema_version": _SCHEMA_VERSION,
            "state": "BLOCKED",
            "reason_code": _DECOY_MAIN_REASON,
        },
    )
    calls: list[dict[str, Any]] = []

    def _fake_classify(**kwargs: Any) -> StatusView:
        calls.append(kwargs)
        return project_readonly_status(
            classified_facts=_facts(
                runner_source_identity=_classified(
                    {"source_sha": _CLASSIFIED_SOURCE_MARKER}
                ),
                main_state=_classified({"reason_code": _CLASSIFIED_SOURCE_MARKER}),
                next_external_action=_classified({"value": "TERMINAL_REST"}),
            ),
            target_worktree=target,
            runtime_root=runtime,
        )

    monkeypatch.setattr(
        status_module,
        _CLASSIFY_AUTHORITY_FIRST_STATUS,
        _fake_classify,
        raising=False,
    )
    monkeypatch.setattr(
        cli_module,
        _CLASSIFY_AUTHORITY_FIRST_STATUS,
        _fake_classify,
        raising=False,
    )
    before = _snapshot(target, runtime)

    exit_code = cli_module.main(["status", "--config", str(config_path)])
    stdout = capsys.readouterr().out

    assert _snapshot(target, runtime) == before
    assert calls, (
        "Current status projects absent placeholders without "
        "authority-first classification"
    )
    assert exit_code == 0
    payload = yaml.safe_load(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "status"
    assert payload["outcome"] == "STATUS_VIEW_PROJECTED"
    assert payload["reason_code"] == "STATUS_VIEW_PROJECTED"
    assert _CLASSIFIED_SOURCE_MARKER in stdout
    assert _DECOY_MAIN_REASON not in stdout
    assert "SOURCE_AUTHORITY_OK" not in stdout
    assert "RECOVERED" not in stdout


_PROMOTION_WITHOUT_PENDING = ("PENDING_PROMOTION", "PENDING_INTENT_ONLY")


@pytest.mark.parametrize(
    "case_id",
    ("no_decisions", "three_ready_decisions"),
)
def test_absent_pending_does_not_invent_promotion(
    tmp_path: Path,
    case_id: str,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    if case_id == "three_ready_decisions":
        _write_yaml(runtime / "decision_request.yaml", {"state": "ready"})
        _write_yaml(runtime / "orchestrator_decision.yaml", {"state": "ready"})
        _write_yaml(runtime / "validator_decision.yaml", {"state": "ready"})
    elif case_id != "no_decisions":
        raise AssertionError(case_id)

    view = _project_authorized(topology)

    assert view.pending.presence is FactPresence.ABSENT
    assert view.pending.payload is None
    assert view.projection_reason_code == "STATUS_VIEW_PROJECTED"
    if case_id == "no_decisions":
        assert view.decisions.presence is FactPresence.ABSENT
        assert view.decisions.payload is None
    else:
        assert view.decisions.presence is FactPresence.CLASSIFIED
        assert view.decisions.payload == _decision_payload(
            request_state="ready",
            orchestrator_state="ready",
            validator_state="ready",
        )
    for slot_name in ("next_external_action", "reason_code", "run_phase"):
        payload = getattr(view, slot_name).payload
        assert isinstance(payload, dict)
        assert payload.get("value") not in _PROMOTION_WITHOUT_PENDING


@pytest.mark.parametrize(
    "case_id",
    ("pending_task_id", "main_state_observed_main_head_sha"),
)
def test_missing_required_mapping_key_is_facts_invalid(
    tmp_path: Path,
    case_id: str,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    if case_id == "pending_task_id":
        pending = _pending_payload()
        del pending["task_id"]
        _write_yaml(runtime / "pending_promotion.yaml", pending)
    elif case_id == "main_state_observed_main_head_sha":
        main_state = _main_state_payload(
            observed_main_head_sha=topology["target_sha"],
        )
        del main_state["observed_main_head_sha"]
        _write_yaml(runtime / "main_state.yaml", main_state)
    else:
        raise AssertionError(case_id)
    _write_runtime_pin(runtime, topology["source_sha"])
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(topology["target"], runtime)
    raised: Exception | None = None

    try:
        _classify_authority_first_status(
            entry_point=topology["entry"],
            target_worktree=topology["target"],
            runtime_root=runtime,
            expected_source_sha=topology["source_sha"],
            loaded_module_files=_source_modules(topology),
        )
    except Exception as exc:
        raised = exc

    _assert_bytes_unchanged(
        source=topology["source"],
        target=topology["target"],
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    assert isinstance(raised, StatusViewError), (
        f"{type(raised).__name__} escaped the status reason grammar"
    )
    assert raised.reason_code == "STATUS_FACTS_INVALID"


def test_unreadable_target_head_after_authority_is_target_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topology = _init_authority_topology(tmp_path)
    runtime = topology["runtime"]
    target = topology["target"]
    _write_runtime_pin(runtime, topology["source_sha"])
    _write_bytes(runtime / "runner_git.lock", b"synthetic-lock-owner\n")
    before_source = _git_identity(topology["source"])
    before_target = _snapshot(target, runtime)
    observed = {
        "authority_completed": False,
        "target_head_after_authority": False,
    }
    real_verify = status_module.verify_steady_state_code_authority
    real_run = subprocess.run

    def _verify_after_authority(**kwargs: Any) -> Any:
        report = real_verify(**kwargs)
        observed["authority_completed"] = True
        return report

    def _fail_target_head_read(args: list[str] | tuple[str, ...], **kwargs: Any) -> Any:
        command = list(args)
        cwd = kwargs.get("cwd")
        if (
            command[:3] == ["git", "rev-parse", "HEAD"]
            and cwd is not None
            and Path(cwd).resolve() == target.resolve()
        ):
            observed["target_head_after_authority"] = observed["authority_completed"]
            raise subprocess.CalledProcessError(
                128,
                command,
                output="",
                stderr="fatal: unable to read target HEAD",
            )
        return real_run(args, **kwargs)

    class _StatusProcess:
        run = staticmethod(_fail_target_head_read)

    monkeypatch.setattr(
        status_module,
        "verify_steady_state_code_authority",
        _verify_after_authority,
    )
    monkeypatch.setattr(status_module, "subprocess", _StatusProcess())
    raised: Exception | None = None

    try:
        _classify_authority_first_status(
            entry_point=topology["entry"],
            target_worktree=target,
            runtime_root=runtime,
            expected_source_sha=topology["source_sha"],
            loaded_module_files=_source_modules(topology),
        )
    except Exception as exc:
        raised = exc

    _assert_bytes_unchanged(
        source=topology["source"],
        target=target,
        runtime_root=runtime,
        before_source=before_source,
        before_target=before_target,
    )
    assert observed["authority_completed"] is True
    assert observed["target_head_after_authority"] is True
    assert isinstance(raised, StatusViewError), (
        f"{type(raised).__name__} escaped the status reason grammar"
    )
    assert raised.reason_code == "STATUS_TARGET_INVALID"
