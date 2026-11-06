"""Public pending-intent persistence and intent-to-effect admission.

A qualified intent is stored as ``pending_promotion.yaml``. The document
binds task, result, attempt, base, main, snapshot, decision, evidence, and
expected-outcome facts. ``result_sha`` is later validation input, not a
candidate-tree proof. Identical persist is byte-stable. Conflict and
corruption preserve existing bytes.

``admit_pending_promotion_effect`` is the only intent-to-effect entry.
Pending persistence does not start a journal, consume a promotion,
materialize a TOCK, or mutate Main. Effect admission materializes a
no-commit merge-candidate, persists runner validation against that view,
and allows journal start, TOCK, and consumption only after the frozen
native tree identity is reproofed. Journal start copies the already
admitted pending ``task_fingerprint``, ``task_snapshot_sha256``, and
``instruction_signature_sha256`` before ``commit-tree``. That TOCK uses
subject ``TOCK``, the
six identity trailers exactly once, parents ``[base, result]``, and the
frozen candidate tree. ``parse_canonical_tock`` is the only public parser
for that envelope.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from task_transition_runner.decision_contracts import DecisionArtifactState
from task_transition_runner.runner_promotion_contracts import (
    QualifiedPromotionIntent,
    qualify_pre_candidate_intent,
)
from task_transition_runner import runner_git_context as _runner_git_context
from task_transition_runner import runner_promotion_journal as _promotion_journal
from task_transition_runner import runner_promotion_state as _promotion_state


_SCHEMA_VERSION = "1.0"
_ARTIFACT_CLASS = "NON_AUTHORIZING_PROMOTION_INTENT"
_CANONICAL_PENDING_NAME = "pending_promotion.yaml"
_MAIN_STATE_NAME = "main_state.yaml"
_DOCUMENT_FIELDS = (
    "schema_version",
    "artifact_class",
    "task_id",
    "origin_run_id",
    "promotion_attempt_id",
    "result_sha",
    "promotion_class",
    "expected_outcome",
    "expected_main_state_after_promotion",
    "base_sha",
    "task_branch",
    "main_state_before_promotion",
    "task_snapshot",
    "instruction_snapshot",
    "orchestrator_decision",
    "validator_decision",
    "allowed_paths",
    "red_lock_closure",
)
_DOCUMENT_FIELD_SET = frozenset(_DOCUMENT_FIELDS)
_OUTCOME_TO_CLASS = {
    "GREEN": "GREEN_PROMOTION",
    "EXPECTED_RED": "RED_TDD_PROMOTION",
}
_SNAPSHOT_SIDES = frozenset({"current", "bound"})
_TASK_VIEW_FIELDS = frozenset({"task_id", "fingerprint", "digest"})
_INSTRUCTION_VIEW_FIELDS = frozenset({"digest", "bindings"})
_CANONICAL_TOCK_SUBJECT = "TOCK"
_CANONICAL_TOCK_TRAILER_KEYS = (
    "Promotion-Attempt",
    "Task-ID",
    "Origin-Run",
    "Result-SHA",
    "Promotion-Class",
    "Expected-Base",
)
_PREDECISION_MANIFEST_NAME = "manifest.yaml"


class RunnerGitPromotionError(Exception):
    """Fail-closed violation of pending-intent persistence."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class _DuplicateKeyError(yaml.YAMLError):
    """A mapping node declared the same key more than once."""


class _StrictLoader(yaml.SafeLoader):
    """Safe loader that rejects duplicate mapping keys at every nesting level."""


def _construct_mapping_rejecting_duplicates(
    loader: yaml.Loader,
    node: yaml.nodes.MappingNode,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise _DuplicateKeyError("duplicate mapping key")
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_rejecting_duplicates,
)


def persist_pending_promotion(
    *,
    runtime_root: Path | str,
    target_worktree: Path | str,
    intent: QualifiedPromotionIntent,
    task_snapshot: Any,
    instruction_snapshot: Any,
    worker_result: Any,
    expected_outcome: Any,
    orchestrator_decision: Any,
    validator_decision: Any,
    base_sha: str,
) -> dict[str, Any]:
    runtime = Path(runtime_root)
    if _has_alternate_pending(runtime):
        return _blocked("PROMOTION_PENDING_AMBIGUOUS")
    if not Path(target_worktree).is_dir():
        return _blocked("PROMOTION_TARGET_MISSING")
    relation = _fact_relation_block(
        intent=intent,
        task_snapshot=task_snapshot,
        instruction_snapshot=instruction_snapshot,
        worker_result=worker_result,
        expected_outcome=expected_outcome,
        orchestrator_decision=orchestrator_decision,
        validator_decision=validator_decision,
        base_sha=base_sha,
        runtime=runtime,
    )
    if relation is not None:
        return relation
    document = _pending_document(
        intent=intent,
        task_snapshot=task_snapshot,
        instruction_snapshot=instruction_snapshot,
        worker_result=worker_result,
        expected_outcome=expected_outcome,
        orchestrator_decision=orchestrator_decision,
        validator_decision=validator_decision,
        base_sha=base_sha,
    )
    pending_path = runtime / _CANONICAL_PENDING_NAME
    if pending_path.is_file():
        try:
            existing = _load_pending_document(pending_path)
        except RunnerGitPromotionError:
            return _blocked("PROMOTION_PENDING_INVALID")
        if existing == document:
            return _persisted_result(
                document=existing,
                pending_path=pending_path,
                already_persisted=True,
            )
        return _blocked("PROMOTION_PENDING_CONFLICT")
    try:
        _atomic_write_yaml(pending_path, document)
    except RunnerGitPromotionError:
        return _blocked("PROMOTION_PENDING_INVALID")
    return _persisted_result(
        document=document,
        pending_path=pending_path,
        already_persisted=False,
    )


def load_pending_promotion(*, runtime_root: Path | str) -> dict[str, Any]:
    runtime = Path(runtime_root)
    if _has_alternate_pending(runtime):
        raise RunnerGitPromotionError("PROMOTION_PENDING_AMBIGUOUS")
    pending_path = runtime / _CANONICAL_PENDING_NAME
    if not pending_path.is_file():
        raise RunnerGitPromotionError("PROMOTION_PENDING_MISSING")
    return _load_pending_document(pending_path)


def _fact_relation_block(
    *,
    intent: QualifiedPromotionIntent,
    task_snapshot: Any,
    instruction_snapshot: Any,
    worker_result: Any,
    expected_outcome: Any,
    orchestrator_decision: Any,
    validator_decision: Any,
    base_sha: str,
    runtime: Path,
) -> dict[str, Any] | None:
    if intent.artifact_class != _ARTIFACT_CLASS:
        return _blocked("PROMOTION_PENDING_INVALID")
    if _identity_axes(worker_result) != _identity_axes(intent):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    if _identity_axes(orchestrator_decision) != _identity_axes(intent):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    if _identity_axes(validator_decision) != _identity_axes(intent):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    task_views = _closed_snapshot_views(task_snapshot, _TASK_VIEW_FIELDS)
    if not isinstance(task_views, tuple):
        return task_views
    task_current, task_bound = task_views
    if (task_current["task_id"], task_bound["task_id"]) != (
        intent.task_id,
        intent.task_id,
    ):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    instruction_views = _closed_snapshot_views(
        instruction_snapshot,
        _INSTRUCTION_VIEW_FIELDS,
    )
    if not isinstance(instruction_views, tuple):
        return instruction_views
    if _OUTCOME_TO_CLASS.get(expected_outcome.kind) != intent.promotion_class:
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    if base_sha == "":
        return _blocked("PROMOTION_PENDING_INVALID")
    if not orchestrator_decision.evidence:
        return _blocked("PROMOTION_PENDING_INVALID")
    if not validator_decision.evidence:
        return _blocked("PROMOTION_PENDING_INVALID")
    main_state_path = runtime / _MAIN_STATE_NAME
    persisted_main: Any = None
    if main_state_path.is_file():
        try:
            persisted_main = _read_strict_yaml(main_state_path)
        except RunnerGitPromotionError:
            return _blocked("PROMOTION_PENDING_INVALID")
    if _canon(persisted_main) != _canon(intent.main_state_before_promotion):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    return None


def _pending_document(
    *,
    intent: QualifiedPromotionIntent,
    task_snapshot: Any,
    instruction_snapshot: Any,
    worker_result: Any,
    expected_outcome: Any,
    orchestrator_decision: Any,
    validator_decision: Any,
    base_sha: str,
) -> dict[str, Any]:
    return _canon(
        {
            "schema_version": _SCHEMA_VERSION,
            "artifact_class": _ARTIFACT_CLASS,
            "task_id": intent.task_id,
            "origin_run_id": intent.origin_run_id,
            "promotion_attempt_id": intent.promotion_attempt_id,
            "result_sha": intent.result_sha,
            "promotion_class": intent.promotion_class,
            "expected_outcome": asdict(expected_outcome),
            "expected_main_state_after_promotion": (
                intent.expected_main_state_after_promotion
            ),
            "base_sha": base_sha,
            "task_branch": worker_result.task_branch,
            "main_state_before_promotion": intent.main_state_before_promotion,
            "task_snapshot": task_snapshot,
            "instruction_snapshot": instruction_snapshot,
            "orchestrator_decision": asdict(orchestrator_decision),
            "validator_decision": asdict(validator_decision),
            "allowed_paths": list(intent.allowed_paths),
            "red_lock_closure": intent.red_lock_closure,
        }
    )


def _closed_snapshot_views(
    snapshot: Any,
    required_fields: frozenset[str],
) -> tuple[Mapping[str, Any], Mapping[str, Any]] | dict[str, Any]:
    if _SNAPSHOT_SIDES - set(snapshot):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    current = snapshot["current"]
    bound = snapshot["bound"]
    if not isinstance(current, Mapping) or not isinstance(bound, Mapping):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    if required_fields - set(current) or required_fields - set(bound):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    if _canon(current) != _canon(bound):
        return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
    return current, bound


def _read_strict_yaml(path: Path) -> Any:
    try:
        return yaml.load(path.read_text(encoding="utf-8"), Loader=_StrictLoader)
    except (OSError, yaml.YAMLError) as exc:
        raise RunnerGitPromotionError("PROMOTION_PENDING_INVALID") from exc


def _load_pending_document(path: Path) -> dict[str, Any]:
    loaded = _read_strict_yaml(path)
    if (
        not isinstance(loaded, dict)
        or set(loaded) != _DOCUMENT_FIELD_SET
        or loaded.get("artifact_class") != _ARTIFACT_CLASS
        or loaded.get("schema_version") != _SCHEMA_VERSION
    ):
        raise RunnerGitPromotionError("PROMOTION_PENDING_INVALID")
    return _canon(loaded)


def _has_alternate_pending(runtime: Path) -> bool:
    yml_exists = (runtime / "pending_promotion.yml").is_file()
    json_exists = (runtime / "pending_promotion.json").is_file()
    return yml_exists or json_exists


def _identity_axes(value: Any) -> tuple[str, str, str, str]:
    return (
        value.task_id,
        value.origin_run_id,
        value.promotion_attempt_id,
        value.result_sha,
    )


def _canon(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_canon(item) for item in value]
    if isinstance(value, list):
        return [_canon(item) for item in value]
    if isinstance(value, dict):
        return {key: _canon(item) for key, item in value.items()}
    return value


def _atomic_write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(path.name + ".tmp")
    try:
        temporary_path.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        raise RunnerGitPromotionError("PROMOTION_PENDING_INVALID") from exc


def _persisted_result(
    *,
    document: dict[str, Any],
    pending_path: Path,
    already_persisted: bool,
) -> dict[str, Any]:
    if already_persisted:
        reason_code = "PROMOTION_INTENT_ALREADY_PERSISTED"
        status = "already_persisted"
    else:
        reason_code = "PROMOTION_INTENT_PERSISTED"
        status = "persisted"
    return {
        "ok": True,
        "status": status,
        "reason_code": reason_code,
        "artifact_class": _ARTIFACT_CLASS,
        "pending_path": str(pending_path),
        "intent": document,
    }


def _blocked(reason_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "blocked",
        "reason_code": reason_code,
    }


def _live_consumption_block(
    payload: Mapping[str, Any],
    worker_result: Any,
) -> dict[str, Any] | None:
    entries = payload["consumed_promotions"]
    already = [
        entry
        for entry in entries
        if entry["promotion_attempt_id"] == worker_result.promotion_attempt_id
        and entry["result_sha"] == worker_result.result_sha
    ]
    conflict = [
        entry
        for entry in entries
        if (
            entry["promotion_attempt_id"] == worker_result.promotion_attempt_id
        )
        != (entry["result_sha"] == worker_result.result_sha)
    ]
    if already:
        return _blocked("PROMOTION_ALREADY_CONSUMED")
    if conflict:
        return _blocked("PROMOTION_CONSUMPTION_CONFLICT")
    return None


def _canonical_tock_message(
    *,
    promotion_attempt_id: str,
    task_id: str,
    origin_run_id: str,
    result_sha: str,
    promotion_class: str,
    expected_base_sha: str,
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


def _canonical_tock_fields(message: str) -> dict[str, str] | None:
    prefix = f"{_CANONICAL_TOCK_SUBJECT}\n\n"
    if not message.startswith(prefix):
        return None
    lines = message[len(prefix):].splitlines()
    if len(lines) != len(_CANONICAL_TOCK_TRAILER_KEYS):
        return None
    fields: dict[str, str] = {}
    for line in lines:
        key, _separator, value = line.partition(": ")
        if value == "":
            return None
        fields[key] = value
    if set(fields) != set(_CANONICAL_TOCK_TRAILER_KEYS):
        return None
    return fields


def parse_canonical_tock(
    message: str,
    parent_oids: tuple[str, ...],
    tree_oid: str,
    *,
    validated_tree_oid: str,
) -> dict[str, Any]:
    """Parse one canonical TOCK envelope.

    Metadata is judged before parents, and parents before the tree. Every
    failure uses the single promotion reason for that stage.
    """
    fields = _canonical_tock_fields(message)
    if fields is None:
        raise RunnerGitPromotionError("PROMOTION_TOCK_METADATA_INVALID")
    observed_parents = tuple(parent_oids)
    expected_parents = (fields["Expected-Base"], fields["Result-SHA"])
    if observed_parents != expected_parents:
        raise RunnerGitPromotionError("PROMOTION_TOCK_PARENT_MISMATCH")
    if tree_oid != validated_tree_oid:
        raise RunnerGitPromotionError("PROMOTION_TOCK_TREE_MISMATCH")
    return {
        "subject": _CANONICAL_TOCK_SUBJECT,
        "promotion_attempt_id": fields["Promotion-Attempt"],
        "task_id": fields["Task-ID"],
        "origin_run_id": fields["Origin-Run"],
        "result_sha": fields["Result-SHA"],
        "promotion_class": fields["Promotion-Class"],
        "expected_base_sha": fields["Expected-Base"],
        "parent_oids": observed_parents,
        "tree_oid": tree_oid,
    }


_COMMITTED_RISK_KEYS = ("secret_risk", "delete_risk", "scope_risk")
_SENSITIVE_PATH_MARKERS = (".env", "secret", "token", "credential")
_CLOSED_PATH_GLOB_MARKERS = ("*", "?", "[", "]")
_PROTECTED_SCOPE_PREFIX = ".github/"


def evaluate_task_branch_risk(
    *,
    repo_root: Path | str,
    base_sha: str,
    result_sha: str,
    allowed_paths: tuple[str, ...] | list[str],
) -> dict[str, Any]:
    """Derive the closed risk triple from committed objects only.

    A clean worktree is not authority. Valid geometry yields exactly
    ``secret_risk``, ``delete_risk``, and ``scope_risk`` as real bools.
    Unresolvable objects, a reversed base/result relation, and globbed
    allowed paths fail closed instead of clearing risk.
    """
    repo = Path(repo_root)
    if not _object_is_commit(repo, base_sha):
        return _apply_committed_risk_authority({}, _risk_source_invalid())
    if not _object_is_commit(repo, result_sha):
        return _apply_committed_risk_authority({}, _risk_source_invalid())
    if _result_is_ancestor_of_base(repo, base_sha=base_sha, result_sha=result_sha):
        return _apply_committed_risk_authority({}, _risk_source_invalid())
    if not _allowed_paths_are_closed(allowed_paths):
        return _apply_committed_risk_authority({}, _risk_source_invalid())
    secret_risk = False
    delete_risk = False
    scope_risk = False
    allowed = tuple(allowed_paths)
    diff = _git_stdout(repo, "diff", "--name-status", base_sha, result_sha)
    for line in diff.splitlines():
        status, path = line.split("\t", 1)
        normalized = Path(path).as_posix()
        if "D" in status:
            delete_risk = True
        if _contains_any(normalized.lower(), _SENSITIVE_PATH_MARKERS):
            secret_risk = True
        if normalized.startswith(_PROTECTED_SCOPE_PREFIX) or normalized not in allowed:
            scope_risk = True
    return {
        "secret_risk": secret_risk,
        "delete_risk": delete_risk,
        "scope_risk": scope_risk,
    }


def _overlay_caller_risk(
    caller_risk_facts: Mapping[str, Any],
    committed_risk: Mapping[str, Any],
) -> dict[str, bool]:
    overlaid: dict[str, bool] = {}
    for key in _COMMITTED_RISK_KEYS:
        caller_asserts_risk = caller_risk_facts.get(key) is True
        committed_asserts_risk = committed_risk[key] is True
        overlaid[key] = caller_asserts_risk or committed_asserts_risk
    return overlaid


def _apply_committed_risk_authority(
    caller_risk_facts: Mapping[str, Any],
    committed_risk: Mapping[str, Any],
    qualify: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Stop an unresolved object source before overlay or qualification."""
    if committed_risk.get("reason_code") == "PROMOTION_RISK_SOURCE_INVALID":
        return _risk_source_invalid()
    return qualify(_overlay_caller_risk(caller_risk_facts, committed_risk))


def _admitted_journal_anchors(pending: Mapping[str, Any]) -> dict[str, str]:
    """Copy the three anchors already stored on the admitted pending document."""
    task_bound = pending["task_snapshot"]["bound"]
    instruction_bound = pending["instruction_snapshot"]["bound"]
    return {
        "task_fingerprint": task_bound["fingerprint"],
        "task_snapshot_sha256": task_bound["digest"],
        "instruction_signature_sha256": instruction_bound["digest"],
    }


def _sealed_task_validation(
    *,
    repo: Path,
    runtime: Path,
    pending: Mapping[str, Any],
    worker_result: Any,
    expected_outcome: Any,
) -> str | None:
    """Reprove write-once validation evidence without executing task commands."""
    from task_transition_runner import task_end_promotion as producer

    run_dir = runtime / "runs" / str(worker_result.origin_run_id)
    snapshot_path = run_dir / "task_snapshot.yaml"
    validation_dir = run_dir / "validation"
    manifest_path = validation_dir / _PREDECISION_MANIFEST_NAME
    if not manifest_path.exists():
        return None
    snapshot_bytes = snapshot_path.read_bytes()
    bound_validation = producer._PREDECISION_VALIDATION.load_bound_task_validation(
        run_dir,
        task_id=worker_result.task_id,
    )
    outcome = asdict(expected_outcome)
    verified = producer._PREDECISION_VALIDATION.verify_predecision_validation_evidence(
        repo=repo,
        runtime=runtime,
        task_id=worker_result.task_id,
        origin_run_id=worker_result.origin_run_id,
        promotion_attempt_id=worker_result.promotion_attempt_id,
        result_sha=worker_result.result_sha,
        commands=bound_validation["commands"],
        expected_outcome=bound_validation["expected_outcome"],
    )
    manifest = _read_strict_yaml(manifest_path)
    sealed_tree = manifest.get("candidate_tree_oid")
    if (
        hashlib.sha256(snapshot_bytes).hexdigest()
        != pending["task_snapshot"]["bound"]["digest"]
        or bound_validation["expected_outcome"] != outcome
        or pending["expected_outcome"] != outcome
        or verified is None
        or verified != worker_result.validation_results
        or not isinstance(manifest, Mapping)
        or not isinstance(sealed_tree, str)
        or not sealed_tree
    ):
        raise RunnerGitPromotionError("PROMOTION_EVIDENCE_MISSING")
    return sealed_tree


def admit_pending_promotion_effect(
    *,
    runtime_root: Path | str,
    target_worktree: Path | str,
    validation_command: Any,
    validation_timeout_seconds: int,
    command_id: str,
    task_snapshot: Any,
    instruction_snapshot: Any,
    worker_result: Any,
    expected_outcome: Any,
    orchestrator_decision: Any,
    validator_decision: Any,
    base_sha: str,
    task_branch_head_sha: str,
    allowed_paths: Any,
    risk_facts: Any,
    lock_ownership: Any,
    git_preflight: Any,
    consumed_payload: Any,
    red_lock: Any,
    unresolved_foreign_epoch: bool,
) -> dict[str, Any]:
    runtime = Path(runtime_root)
    repo = Path(target_worktree)
    pending = load_pending_promotion(runtime_root=runtime)
    bound_main = str(pending["base_sha"])
    try:
        admitted_anchors = _admitted_journal_anchors(pending)
    except (KeyError, TypeError):
        return _blocked("JOURNAL_FIELDS_INVALID")
    journal_path = runtime / "promotion_journal.yaml"
    if journal_path.is_file():
        existing_journal = _promotion_journal.load_promotion_journal(path=journal_path)
        if (
            existing_journal.task_id,
            existing_journal.origin_run_id,
            existing_journal.promotion_attempt_id,
            existing_journal.result_sha,
            existing_journal.promotion_class,
            existing_journal.base_main_sha,
            existing_journal.validated_result_sha,
        ) != (
            pending["task_id"],
            pending["origin_run_id"],
            pending["promotion_attempt_id"],
            pending["result_sha"],
            pending["promotion_class"],
            pending["base_sha"],
            pending["result_sha"],
        ):
            return _blocked("PROMOTION_PENDING_IDENTITY_MISMATCH")
        if (
            existing_journal.task_fingerprint,
            existing_journal.task_snapshot_sha256,
            existing_journal.instruction_signature_sha256,
        ) != (
            admitted_anchors["task_fingerprint"],
            admitted_anchors["task_snapshot_sha256"],
            admitted_anchors["instruction_signature_sha256"],
        ):
            return _blocked("JOURNAL_FIELD_DRIFT")
        if existing_journal.phase not in (
            _promotion_journal.PromotionJournalPhase.PENDING,
            _promotion_journal.PromotionJournalPhase.APPLYING,
        ):
            return _blocked("UNRESOLVED_PROMOTION_JOURNAL")

    def fail(reason_code: str) -> dict[str, Any]:
        _restore_bound_main(repo, bound_main)
        return _blocked(reason_code)

    if base_sha != bound_main:
        return fail("PROMOTION_BASE_SHA_MISMATCH")
    if _git_stdout(repo, "rev-parse", "refs/heads/main") != bound_main:
        return fail("PROMOTION_BASE_SHA_MISMATCH")
    if git_preflight.get("ok") is not True:
        return fail("PROMOTION_GIT_PREFLIGHT_BLOCKED")
    if unresolved_foreign_epoch:
        return fail("PROMOTION_UNRESOLVED_FOREIGN_EPOCH")
    ownership = lock_ownership.get("ownership", {})
    if ownership.get("pid") != os.getpid():
        return fail("PROMOTION_LOCK_MISMATCH")
    lock_path = runtime / "runner_git.lock"
    verified = _runner_git_context.verify_runner_git_lock_ownership(
        lock_path=lock_path,
        ownership=ownership,
    )
    if verified.get("ok") is not True:
        if (
            verified.get("reason_code") == "LOCK_OWNERSHIP_LOST"
            and not lock_path.is_file()
        ):
            return fail("PROMOTION_LOCK_MISSING")
        return fail("PROMOTION_LOCK_MISMATCH")
    if _git_stdout(repo, "status", "--porcelain") != "":
        return fail("PROMOTION_GIT_PREFLIGHT_BLOCKED")
    try:
        live_consumed = _promotion_state.load_promotion_consumed(
            promotion_consumed_path=runtime / "promotion_consumed.yaml",
        )
    except _promotion_state.RunnerPromotionStateError as exc:
        return fail(exc.reason_code)
    live_consumption = _live_consumption_block(live_consumed, worker_result)
    if live_consumption is not None:
        return fail(live_consumption["reason_code"])
    if _git(repo, "cat-file", "-e", f"{task_branch_head_sha}^{{commit}}", check=False).returncode != 0:
        return fail("PROMOTION_RESULT_SHA_MISSING")
    if task_branch_head_sha != worker_result.result_sha:
        return fail("PROMOTION_RESULT_SHA_MISMATCH")
    live_branch = _git(
        repo,
        "rev-parse",
        "--verify",
        "--quiet",
        f"refs/heads/{pending['task_branch']}",
        check=False,
    )
    if live_branch.returncode != 0:
        return fail("PROMOTION_RESULT_SHA_MISSING")
    if live_branch.stdout.strip() != worker_result.result_sha:
        return fail("PROMOTION_RESULT_SHA_MISMATCH")
    if orchestrator_decision.promotion_class != pending["promotion_class"]:
        return fail("PROMOTION_DECISION_CONTRADICTION")
    if red_lock is not None and any(
        field not in red_lock
        for field in (
            "schema_version",
            "expected_main_state_after_promotion",
            "closure_gate",
            "task_id",
            "origin_run_id",
            "promotion_attempt_id",
            "result_sha",
            "expected_failure_signature",
            "command_id",
        )
    ):
        return fail("PROMOTION_RED_LOCK_UNKNOWN")
    main_state_path = runtime / _MAIN_STATE_NAME
    persisted_main = _read_strict_yaml(main_state_path)
    if _canon(persisted_main) != _canon(pending["main_state_before_promotion"]):
        return fail("PROMOTION_MAIN_STATE_MISMATCH")
    committed_risk = evaluate_task_branch_risk(
        repo_root=repo,
        base_sha=base_sha,
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
            task_branch_head_sha=task_branch_head_sha,
            expected_outcome=expected_outcome,
            allowed_paths=tuple(allowed_paths),
            risk_facts=authoritative_risk,
            orchestrator_state=(
                DecisionArtifactState.READY
                if orchestrator_decision.ready
                else DecisionArtifactState.DRAFT
            ),
            orchestrator_decision=orchestrator_decision,
            validator_state=(
                DecisionArtifactState.READY
                if validator_decision.ready
                else DecisionArtifactState.DRAFT
            ),
            validator_decision=validator_decision,
            main_state=persisted_main,
            consumed_payload=consumed_payload,
            lock_ownership=lock_ownership,
            git_preflight=git_preflight,
            red_lock=red_lock,
            unresolved_foreign_epoch=unresolved_foreign_epoch,
        ),
    )
    if qualified.get("ok") is not True:
        return fail(str(qualified["reason_code"]))
    intent = qualified["intent"]
    try:
        sealed_tree = _sealed_task_validation(
            repo=repo,
            runtime=runtime,
            pending=pending,
            worker_result=worker_result,
            expected_outcome=expected_outcome,
        )
    except (
        KeyError,
        OSError,
        RunnerGitPromotionError,
        TypeError,
        ValueError,
        yaml.YAMLError,
    ):
        return fail("PROMOTION_EVIDENCE_MISSING")
    merged = _git(
        repo,
        "merge",
        "--no-commit",
        "--no-ff",
        str(pending["task_branch"]),
        check=False,
    )
    if merged.returncode != 0:
        return fail("PROMOTION_CANDIDATE_TREE_DRIFT")
    frozen_tree = _git_stdout(repo, "write-tree")
    if sealed_tree is not None:
        if (
            frozen_tree != sealed_tree
            or _git_stdout(repo, "rev-parse", "HEAD") != bound_main
            or _git_stdout(repo, "rev-parse", "refs/heads/main") != bound_main
            or _git_stdout(repo, "rev-parse", "MERGE_HEAD")
            != worker_result.result_sha
        ):
            return fail("PROMOTION_CANDIDATE_TREE_DRIFT")
    else:
        started_at_utc = _utc_now()
        try:
            completed = subprocess.run(  # noqa: S603
                list(validation_command),
                cwd=repo,
                capture_output=True,
                timeout=validation_timeout_seconds,
            )
            output_bytes = completed.stdout + completed.stderr
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            output_bytes = b""
            exit_code = -1
        except FileNotFoundError:
            output_bytes = b""
            exit_code = 127
        except PermissionError:
            output_bytes = b""
            exit_code = 126
        _persist_candidate_validation(
            runtime=runtime,
            origin_run_id=str(worker_result.origin_run_id),
            command_id=command_id,
            command=list(validation_command),
            cwd=str(repo),
            started_at_utc=started_at_utc,
            timeout_seconds=validation_timeout_seconds,
            exit_code=exit_code,
            output_bytes=output_bytes,
            candidate_tree_oid=frozen_tree,
        )
        hook_path = (
            Path(_git_stdout(repo, "rev-parse", "--absolute-git-dir"))
            / "hooks"
            / "pre-commit"
        )
        if hook_path.is_file():
            subprocess.run(  # noqa: S603
                [str(hook_path)],
                cwd=repo,
                check=False,
                capture_output=True,
            )
        if _git_stdout(repo, "diff", "--name-only") != "":
            return fail("PROMOTION_CANDIDATE_TREE_DRIFT")
        if _git_stdout(repo, "write-tree") != frozen_tree:
            return fail("PROMOTION_CANDIDATE_TREE_DRIFT")
        if expected_outcome.kind == "EXPECTED_RED":
            try:
                output_text = output_bytes.decode("utf-8")
            except UnicodeDecodeError:
                outcome_matched = False
            else:
                outcome_matched = (
                    exit_code != 0
                    and expected_outcome.failure_signature in output_text
                )
        else:
            outcome_matched = exit_code == 0
        if not outcome_matched:
            return fail("PROMOTION_VALIDATION_OUTCOME_MISMATCH")

    tock_message = _canonical_tock_message(
        promotion_attempt_id=intent.promotion_attempt_id,
        task_id=intent.task_id,
        origin_run_id=intent.origin_run_id,
        result_sha=intent.result_sha,
        promotion_class=intent.promotion_class,
        expected_base_sha=bound_main,
    )
    parsed_tock = parse_canonical_tock(
        tock_message,
        (bound_main, intent.result_sha),
        frozen_tree,
        validated_tree_oid=frozen_tree,
    )
    parent_oids = parsed_tock["parent_oids"]
    journal_path = runtime / "promotion_journal.yaml"
    now = _utc_now()
    if journal_path.is_file():
        journal = _promotion_journal.load_promotion_journal(path=journal_path)
        effect_drain_run_id = journal.drain_run_id
    else:
        effect_drain_run_id = intent.origin_run_id
        try:
            journal = _promotion_journal.create_promotion_journal(
                task_id=intent.task_id,
                origin_run_id=intent.origin_run_id,
                drain_run_id=effect_drain_run_id,
                promotion_attempt_id=intent.promotion_attempt_id,
                promotion_class=intent.promotion_class,
                base_main_sha=bound_main,
                result_sha=intent.result_sha,
                validated_result_sha=intent.result_sha,
                task_fingerprint=admitted_anchors["task_fingerprint"],
                task_snapshot_sha256=admitted_anchors["task_snapshot_sha256"],
                instruction_signature_sha256=admitted_anchors[
                    "instruction_signature_sha256"
                ],
                pending_at_utc=now,
                reason_code="PROMOTION_EFFECT_ADMITTED",
            )
        except _promotion_journal.PromotionJournalError as exc:
            return fail(exc.reason_code)
        _promotion_journal.persist_promotion_journal(path=journal_path, journal=journal)
    tock_sha = _git_stdout(
        repo,
        "commit-tree",
        parsed_tock["tree_oid"],
        "-p",
        parent_oids[0],
        "-p",
        parent_oids[1],
        "-m",
        tock_message,
    )
    if journal.phase is _promotion_journal.PromotionJournalPhase.PENDING:
        journal = _promotion_journal.transition_promotion_journal(
            journal,
            phase=_promotion_journal.PromotionJournalPhase.APPLYING,
            drain_run_id=effect_drain_run_id,
            transitioned_at_utc=now,
            reason_code="PROMOTION_EFFECT_ADMITTED",
        )
        _promotion_journal.persist_promotion_journal(
            path=journal_path,
            journal=journal,
        )
    _restore_merge_state(repo)
    _git(repo, "update-ref", "refs/heads/main", tock_sha)
    _git(repo, "symbolic-ref", "HEAD", "refs/heads/main")
    _git(repo, "reset", "--hard", tock_sha)
    journal = _promotion_journal.transition_promotion_journal(
        journal,
        phase=_promotion_journal.PromotionJournalPhase.GIT_APPLIED,
        drain_run_id=effect_drain_run_id,
        transitioned_at_utc=now,
        reason_code="PROMOTION_EFFECT_ADMITTED",
        tock_sha=tock_sha,
    )
    _promotion_journal.persist_promotion_journal(path=journal_path, journal=journal)
    _promotion_state.record_promotion_consumed(
        promotion_consumed_path=runtime / "promotion_consumed.yaml",
        promotion_attempt_id=intent.promotion_attempt_id,
        result_sha=intent.result_sha,
        main_state_before_promotion=pending["main_state_before_promotion"],
        consumed_at_utc=now,
        tock_sha=tock_sha,
    )
    _promotion_state.write_main_state(
        main_state_path=main_state_path,
        payload={
            "schema_version": pending["main_state_before_promotion"]["schema_version"],
            "state": intent.expected_main_state_after_promotion,
            "main_branch": pending["main_state_before_promotion"]["main_branch"],
            "validated_result_sha": intent.result_sha,
            "last_tock_sha": tock_sha,
            "observed_main_head_sha": tock_sha,
            "reason_code": "PROMOTION_EFFECT_ADMITTED",
            "updated_at_utc": now,
        },
    )
    journal = _promotion_journal.transition_promotion_journal(
        journal,
        phase=_promotion_journal.PromotionJournalPhase.RECONCILED,
        drain_run_id=effect_drain_run_id,
        transitioned_at_utc=now,
        reason_code="PROMOTION_EFFECT_ADMITTED",
    )
    _promotion_journal.persist_promotion_journal(path=journal_path, journal=journal)
    journal = _promotion_journal.transition_promotion_journal(
        journal,
        phase=_promotion_journal.PromotionJournalPhase.FINALIZED,
        drain_run_id=effect_drain_run_id,
        transitioned_at_utc=now,
        reason_code="PROMOTION_EFFECT_ADMITTED",
    )
    _promotion_journal.persist_promotion_journal(path=journal_path, journal=journal)
    return {
        "ok": True,
        "status": "tocked",
        "reason_code": "PROMOTION_EFFECT_ADMITTED",
        "validated_candidate_tree_oid": frozen_tree,
        "tree_oid": frozen_tree,
        "tock_sha": tock_sha,
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _git(
    repo: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=check,
        capture_output=True,
        text=True,
    )


def _git_stdout(repo: Path, *args: str) -> str:
    return _git(repo, *args).stdout.strip()


def _risk_source_invalid() -> dict[str, Any]:
    return {
        "ok": False,
        "status": "blocked",
        "reason_code": "PROMOTION_RISK_SOURCE_INVALID",
    }


def _object_is_commit(repo: Path, sha: str) -> bool:
    completed = _git(repo, "cat-file", "-t", "--", sha, check=False)
    return completed.returncode == 0 and completed.stdout.strip() == "commit"


def _result_is_ancestor_of_base(
    repo: Path,
    *,
    base_sha: str,
    result_sha: str,
) -> bool:
    if base_sha == result_sha:
        return False
    completed = _git(
        repo,
        "merge-base",
        "--is-ancestor",
        result_sha,
        base_sha,
        check=False,
    )
    return completed.returncode == 0


def _allowed_paths_are_closed(allowed_paths: tuple[str, ...] | list[str]) -> bool:
    joined = "\n".join(str(path) for path in allowed_paths)
    return not _contains_any(joined, _CLOSED_PATH_GLOB_MARKERS)


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    found = False
    for marker in markers:
        if marker in text:
            found = True
    return found


def _restore_merge_state(repo: Path) -> None:
    git_dir = Path(_git_stdout(repo, "rev-parse", "--absolute-git-dir"))
    if (git_dir / "MERGE_HEAD").is_file():
        _git(repo, "merge", "--abort", check=False)


def _restore_bound_main(repo: Path, bound_sha: str) -> None:
    _restore_merge_state(repo)
    _git(repo, "symbolic-ref", "HEAD", "refs/heads/main")
    _git(repo, "update-ref", "refs/heads/main", bound_sha)
    _git(repo, "reset", "--hard", bound_sha)


def _persist_candidate_validation(
    *,
    runtime: Path,
    origin_run_id: str,
    command_id: str,
    command: list[str],
    cwd: str,
    started_at_utc: str,
    timeout_seconds: int,
    exit_code: int,
    output_bytes: bytes,
    candidate_tree_oid: str,
) -> None:
    relative_dir = Path("runs") / origin_run_id / "validation"
    output_ref = str(relative_dir / f"{command_id}.log")
    validation_dir = runtime / relative_dir
    validation_dir.mkdir(parents=True, exist_ok=True)
    (runtime / output_ref).write_bytes(output_bytes)
    payload = {
        "command": command,
        "cwd": cwd,
        "started_at_utc": started_at_utc,
        "timeout_seconds": timeout_seconds,
        "exit_code": exit_code,
        "output_ref": output_ref,
        "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
        "candidate_tree_oid": candidate_tree_oid,
    }
    _atomic_write_yaml(validation_dir / f"{command_id}.yaml", payload)
