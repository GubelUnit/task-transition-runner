"""Local CAS from AUTHORIZED to CONSUMED for one ordinary continuation.

The successor task, promotion attempt and canonical main-ref TOCK must
match together. Task-ID, Promotion-Attempt and the Result tree must
prove that TOCK against the bound continuation. Expected-Base and
parent 0 bind to ``expected_continuation_head_sha``. The predecessor
TOCK stays reachable through the already bound START chain. An identical
consumed artifact is reproved without a second write. Any conflicting
axis blocks. This is not an exactly-once guarantee.
"""

from __future__ import annotations

import subprocess
import zlib
from pathlib import Path
from typing import Any

import yaml

from task_transition_runner.runner_continuation_contract import (
    ContinuationContractView,
    ContinuationError,
    LifecyclePhase,
    assert_lifecycle_transition,
    load_continuation_artifact,
    observe_lifecycle,
)
from task_transition_runner.runner_git_context import verify_runner_git_lock_ownership
from task_transition_runner.runner_git_promotion import (
    RunnerGitPromotionError,
    parse_canonical_tock,
)


_ARTIFACT_NAME = "post_tick_continuation.yaml"
_LOCK_NAME = "runner_git.lock"
_STATUS_CONSUMED = "CONSUMED"
_ALREADY_CONSUMED = "CONTINUATION_ALREADY_CONSUMED"
_CONSUMPTION_CONFLICT = "CONTINUATION_CONSUMPTION_CONFLICT"
_SUCCESSOR_MISMATCH = "CONTINUATION_SUCCESSOR_MISMATCH"
_LIFECYCLE_INVALID = "CONTINUATION_LIFECYCLE_INVALID"


def consume_post_tick_continuation(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
    successor_task_id: str,
    successor_promotion_attempt_id: str,
    successor_tock_sha: str,
    consumed_at_utc: str,
    lock_ownership: dict[str, Any],
) -> ContinuationContractView:
    """Consume one authorized artifact, or reprove consumed bytes in place."""

    _require_lock(runtime, lock_ownership)
    view = load_continuation_artifact(runtime / _ARTIFACT_NAME)
    if (
        view.status != _STATUS_CONSUMED
        and view.successor_task_id != successor_task_id
    ):
        raise ContinuationError(_SUCCESSOR_MISMATCH)
    if view.status != _STATUS_CONSUMED:
        _require_canonical_successor_tock(
            repo=repo,
            view=view,
            successor_promotion_attempt_id=successor_promotion_attempt_id,
            successor_tock_sha=successor_tock_sha,
        )
    return apply_successor_continuation_consumption(
        repo=repo,
        runtime=runtime,
        main_ref=main_ref,
        successor_task_id=successor_task_id,
        successor_promotion_attempt_id=successor_promotion_attempt_id,
        successor_tock_sha=successor_tock_sha,
        consumed_at_utc=consumed_at_utc,
    )


def apply_successor_continuation_consumption(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
    successor_task_id: str,
    successor_promotion_attempt_id: str,
    successor_tock_sha: str,
    consumed_at_utc: str,
) -> ContinuationContractView:
    """Apply the identity CAS. The caller owns the lock boundary."""

    view = load_continuation_artifact(runtime / _ARTIFACT_NAME)
    if view.successor_task_id != successor_task_id:
        raise ContinuationError(_SUCCESSOR_MISMATCH)
    if view.status == _STATUS_CONSUMED:
        return _reprove_consumed(
            view,
            successor_task_id=successor_task_id,
            successor_promotion_attempt_id=successor_promotion_attempt_id,
            successor_tock_sha=successor_tock_sha,
        )
    if _first_parent_head(repo, main_ref) != successor_tock_sha:
        raise ContinuationError(_SUCCESSOR_MISMATCH)
    observation = observe_lifecycle(view)
    assert_lifecycle_transition(
        origin=view.origin,
        previous=observation.phase,
        current=LifecyclePhase.CONSUMED,
    )
    payload = view.to_mapping()
    payload["status"] = _STATUS_CONSUMED
    payload["successor_promotion_attempt_id"] = successor_promotion_attempt_id
    payload["successor_tock_sha"] = successor_tock_sha
    payload["consumed_at_utc"] = consumed_at_utc
    artifact = runtime / _ARTIFACT_NAME
    artifact.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return load_continuation_artifact(artifact)


def _reprove_consumed(
    view: ContinuationContractView,
    *,
    successor_task_id: str,
    successor_promotion_attempt_id: str,
    successor_tock_sha: str,
) -> ContinuationContractView:
    mapping = view.to_mapping()
    persisted = (
        view.successor_task_id,
        mapping["successor_promotion_attempt_id"],
        mapping["successor_tock_sha"],
    )
    requested = (
        successor_task_id,
        successor_promotion_attempt_id,
        successor_tock_sha,
    )
    if persisted == requested:
        raise ContinuationError(_ALREADY_CONSUMED)
    raise ContinuationError(_CONSUMPTION_CONFLICT)


def _require_canonical_successor_tock(
    *,
    repo: Path,
    view: ContinuationContractView,
    successor_promotion_attempt_id: str,
    successor_tock_sha: str,
) -> None:
    bound_head = view.expected_continuation_head_sha
    tock_tree, parents, message = _read_loose_commit(repo, successor_tock_sha)
    if len(parents) < 2:
        raise ContinuationError(_SUCCESSOR_MISMATCH)
    result_tree = _read_loose_commit(repo, parents[1])[0]
    try:
        parsed = parse_canonical_tock(
            message,
            parents,
            tock_tree,
            validated_tree_oid=result_tree,
        )
    except RunnerGitPromotionError:
        raise ContinuationError(_SUCCESSOR_MISMATCH) from None
    observed_base = (parsed["expected_base_sha"], parsed["parent_oids"][0])
    if (
        parsed["task_id"] != view.successor_task_id
        or parsed["promotion_attempt_id"] != successor_promotion_attempt_id
        or observed_base != (bound_head, bound_head)
    ):
        raise ContinuationError(_SUCCESSOR_MISMATCH)


def _read_loose_commit(
    repo: Path,
    sha: str,
) -> tuple[str, tuple[str, ...], str]:
    object_path = repo / ".git" / "objects" / sha[:2] / sha[2:]
    payload = zlib.decompress(object_path.read_bytes())
    _kind, _separator, body = payload.partition(b"\0")
    header, _separator, message = body.partition(b"\n\n")
    tree_oid = ""
    parents: list[str] = []
    for line in header.splitlines():
        key, _separator, value = line.partition(b" ")
        if key == b"tree":
            tree_oid = value.decode("ascii")
        elif key == b"parent":
            parents.append(value.decode("ascii"))
    return tree_oid, tuple(parents), message.decode("utf-8")


def _require_lock(runtime: Path, ownership: dict[str, Any]) -> None:
    gate = verify_runner_git_lock_ownership(
        lock_path=runtime / _LOCK_NAME,
        ownership=ownership,
    )
    if gate.get("ok") is not True:
        raise ContinuationError(_LIFECYCLE_INVALID)


def _first_parent_head(repo: Path, main_ref: str) -> str:
    output = _git_text(repo, "rev-list", "--first-parent", main_ref)
    return output.splitlines()[0]


def _git_text(repo: Path, *args: str) -> str:
    process = subprocess.Popen(  # noqa: S603
        ["git", *args],
        cwd=repo,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, _stderr = process.communicate()
    return stdout.strip()
