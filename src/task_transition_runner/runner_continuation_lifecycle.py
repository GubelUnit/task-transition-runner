"""Ordinary one-origin continuation lifecycle facade.

Prepare binds one external fresh successor to one gap-free, one-parent
START chain from a proven FINALIZED_PROMOTION_TICK through the successor
START head. Load and dispatch re-prove that immutable binding under the
current local lock. The last gate is a fresh main-ref rev-parse: the live
SHA must still be the predecessor TOCK. AUTHORIZED stays PRE_ACK here;
this module does not consume a successor TOCK.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from task_transition_runner.runner_continuation_contract import (
    ContinuationContractView,
    ContinuationError,
    ContinuationIdentity,
    ContinuationOrigin,
    LifecycleObservation,
    LifecyclePhase,
    assert_lifecycle_transition as _assert_contract_transition,
    binding_digest,
    load_continuation_artifact,
    observe_lifecycle,
)
from task_transition_runner.runner_continuation_origin_proofs import (
    OriginProof,
    prove_finalized_promotion_tick,
)
from task_transition_runner.runner_git_context import verify_runner_git_lock_ownership


_SCHEMA_VERSION = "2.0"
_STATUS_AUTHORIZED = "AUTHORIZED"
_ARTIFACT_NAME = "post_tick_continuation.yaml"
_LOCK_NAME = "runner_git.lock"
_CHAIN_MISMATCH = "CONTINUATION_CHAIN_MISMATCH"
_HEAD_MISMATCH = "CONTINUATION_HEAD_MISMATCH"
_SUCCESSOR_MISMATCH = "CONTINUATION_SUCCESSOR_MISMATCH"
_ORIGIN_INVALID = "CONTINUATION_ORIGIN_INVALID"
_LIFECYCLE_INVALID = "CONTINUATION_LIFECYCLE_INVALID"


@dataclass(frozen=True)
class ContinuationCommitBinding:
    """One one-parent link in the successor START chain."""

    sha: str
    parent_sha: str


@dataclass(frozen=True)
class FinalizedOriginLifecycleView:
    """Predecessor identity and TOCK proven for this lifecycle."""

    predecessor_identity: ContinuationIdentity
    tock_sha: str


@dataclass(frozen=True)
class ContinuationLifecycleView:
    """Typed facade over one authorized continuation artifact."""

    contract: ContinuationContractView
    origin_proof: FinalizedOriginLifecycleView
    observation: LifecycleObservation
    commit_chain: tuple[ContinuationCommitBinding, ...]


@dataclass(frozen=True)
class _CommitView:
    tree_oid: str
    parents: tuple[str, ...]
    message: str


def prepare_post_tick_continuation(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
    continuation_origin: ContinuationOrigin,
    successor_task_id: str,
    successor_origin_run_id: str,
    successor_promotion_attempt_id: str,
    expected_continuation_head_sha: str,
    lock_ownership: dict[str, Any],
    prepared_at_utc: str,
) -> ContinuationLifecycleView:
    """Bind one fresh successor START chain to the proven predecessor."""

    _require_lock(runtime, lock_ownership)
    _require_finalized_origin(continuation_origin)
    proof = prove_finalized_promotion_tick(
        repo=repo,
        runtime=runtime,
        main_ref=main_ref,
    )
    tock_sha = _tock_sha(proof)
    _require_fresh_successor(successor_task_id, tock_task_id=_predecessor_task_id(proof))
    ref_sha = _task_ref_sha(repo, successor_task_id, successor_origin_run_id)
    if ref_sha is None or ref_sha != expected_continuation_head_sha:
        raise ContinuationError(_HEAD_MISMATCH)
    chain = _walk_start_chain(
        repo,
        head_sha=ref_sha,
        tock_sha=tock_sha,
        successor_task_id=successor_task_id,
        successor_origin_run_id=successor_origin_run_id,
        successor_promotion_attempt_id=successor_promotion_attempt_id,
    )
    _require_live_main(repo, main_ref, tock_sha)
    payload = _artifact_payload(
        proof,
        chain,
        successor_task_id=successor_task_id,
        prepared_at_utc=prepared_at_utc,
    )
    artifact = runtime / _ARTIFACT_NAME
    if artifact.is_file():
        existing = load_continuation_artifact(artifact)
        if (
            existing.status != _STATUS_AUTHORIZED
            or existing.continuation_digest_sha256 != payload["continuation_digest_sha256"]
        ):
            raise ContinuationError(_LIFECYCLE_INVALID)
        return _lifecycle_view(existing, proof, chain)
    _write_artifact(artifact, payload)
    return _lifecycle_view(load_continuation_artifact(artifact), proof, chain)


def load_authorized_post_tick_continuation(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
    successor_task_id: str,
    observed_main_head_sha: str,
    lock_ownership: dict[str, Any],
) -> ContinuationLifecycleView | None:
    """Re-prove an authorized artifact, or return None when it is absent."""

    return _reprove_authorized(
        repo=repo,
        runtime=runtime,
        main_ref=main_ref,
        successor_task_id=successor_task_id,
        observed_main_head_sha=observed_main_head_sha,
        lock_ownership=lock_ownership,
    )


def assert_post_tick_continuation_dispatch_task(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
    task_id: str,
    observed_main_head_sha: str,
    lock_ownership: dict[str, Any],
) -> ContinuationLifecycleView | None:
    """Repeat the authorized reproof for one dispatch task id."""

    return _reprove_authorized(
        repo=repo,
        runtime=runtime,
        main_ref=main_ref,
        successor_task_id=task_id,
        observed_main_head_sha=observed_main_head_sha,
        lock_ownership=lock_ownership,
    )


def assert_lifecycle_transition(
    *,
    origin: ContinuationOrigin,
    previous: LifecyclePhase,
    current: LifecyclePhase,
) -> None:
    """Admit only FINALIZED_PROMOTION_TICK from PRE_ACK toward CONSUMED."""

    if origin != ContinuationOrigin.FINALIZED_PROMOTION_TICK:
        raise ContinuationError(_LIFECYCLE_INVALID)
    _assert_contract_transition(origin=origin, previous=previous, current=current)


def _reprove_authorized(
    *,
    repo: Path,
    runtime: Path,
    main_ref: str,
    successor_task_id: str,
    observed_main_head_sha: str,
    lock_ownership: dict[str, Any],
) -> ContinuationLifecycleView | None:
    _require_lock(runtime, lock_ownership)
    artifact = runtime / _ARTIFACT_NAME
    if not artifact.is_file():
        return None
    contract = load_continuation_artifact(artifact)
    if contract.status != _STATUS_AUTHORIZED:
        raise ContinuationError(_LIFECYCLE_INVALID)
    if contract.successor_task_id != successor_task_id:
        raise ContinuationError(_SUCCESSOR_MISMATCH)
    bound_tock = _bound_tock_sha(contract)
    if observed_main_head_sha != bound_tock:
        raise ContinuationError(_HEAD_MISMATCH)
    _require_live_main(repo, main_ref, bound_tock)
    proof = prove_finalized_promotion_tick(
        repo=repo,
        runtime=runtime,
        main_ref=main_ref,
    )
    chain = _bindings_from_contract(contract)
    _require_declared_chain(repo, contract, successor_task_id)
    return _lifecycle_view(contract, proof, chain)


def _require_lock(runtime: Path, ownership: dict[str, Any]) -> None:
    gate = verify_runner_git_lock_ownership(
        lock_path=runtime / _LOCK_NAME,
        ownership=ownership,
    )
    if gate.get("ok") is not True:
        raise ContinuationError(_LIFECYCLE_INVALID)


def _require_finalized_origin(origin: object) -> None:
    if origin != ContinuationOrigin.FINALIZED_PROMOTION_TICK:
        raise ContinuationError(_ORIGIN_INVALID)


def _require_fresh_successor(successor_task_id: str, *, tock_task_id: str) -> None:
    if not successor_task_id.strip() or successor_task_id == tock_task_id:
        raise ContinuationError(_SUCCESSOR_MISMATCH)


def _require_live_main(repo: Path, main_ref: str, tock_sha: str) -> None:
    if _git_text(repo, "rev-parse", main_ref) != tock_sha:
        raise ContinuationError(_HEAD_MISMATCH)


def _task_ref_sha(repo: Path, task_id: str, origin_run_id: str) -> str | None:
    ref = f"refs/heads/runner/task/{task_id}/{origin_run_id}"
    completed = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", ref],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _walk_start_chain(
    repo: Path,
    *,
    head_sha: str,
    tock_sha: str,
    successor_task_id: str,
    successor_origin_run_id: str,
    successor_promotion_attempt_id: str,
) -> tuple[ContinuationCommitBinding, ...]:
    bindings: list[ContinuationCommitBinding] = []
    current = head_sha
    while current != tock_sha:
        commit = _read_commit(repo, current)
        if len(commit.parents) != 1:
            raise ContinuationError(_CHAIN_MISMATCH)
        parent = commit.parents[0]
        if commit.tree_oid != _read_commit(repo, parent).tree_oid:
            raise ContinuationError(_CHAIN_MISMATCH)
        expected = (
            f"START {successor_task_id} {successor_origin_run_id} "
            f"{successor_promotion_attempt_id} {parent}"
        )
        line = commit.message.splitlines()[0]
        if line != expected:
            if line.startswith("START "):
                raise ContinuationError(_SUCCESSOR_MISMATCH)
            raise ContinuationError(_CHAIN_MISMATCH)
        bindings.append(ContinuationCommitBinding(sha=current, parent_sha=parent))
        current = parent
    if not bindings:
        raise ContinuationError(_CHAIN_MISMATCH)
    bindings.reverse()
    return tuple(bindings)


def _require_declared_chain(
    repo: Path,
    contract: ContinuationContractView,
    successor_task_id: str,
) -> None:
    entries = contract.to_mapping()["commit_chain"]
    head_sha = str(entries[-1]["sha"])
    bound_origin: str | None = None
    bound_attempt: str | None = None
    for entry in entries:
        commit = _read_commit(repo, str(entry["sha"]))
        if commit.parents != (str(entry["parent_sha"]),):
            raise ContinuationError(_CHAIN_MISMATCH)
        parent = commit.parents[0]
        if commit.tree_oid != _read_commit(repo, parent).tree_oid:
            raise ContinuationError(_CHAIN_MISMATCH)
        line = commit.message.splitlines()[0]
        parts = line.split(" ")
        if bound_origin is None:
            bound_origin = parts[2]
            bound_attempt = parts[3]
        expected = (
            f"START {successor_task_id} {bound_origin} "
            f"{bound_attempt} {parent}"
        )
        if (
            line != expected
            or _task_ref_sha(repo, successor_task_id, bound_origin) != head_sha
        ):
            raise ContinuationError(_SUCCESSOR_MISMATCH)


def _lifecycle_view(
    contract: ContinuationContractView,
    proof: OriginProof,
    chain: tuple[ContinuationCommitBinding, ...],
) -> ContinuationLifecycleView:
    return ContinuationLifecycleView(
        contract=contract,
        origin_proof=FinalizedOriginLifecycleView(
            predecessor_identity=_predecessor_identity(proof),
            tock_sha=_tock_sha(proof),
        ),
        observation=observe_lifecycle(contract),
        commit_chain=chain,
    )


def _artifact_payload(
    proof: OriginProof,
    chain: tuple[ContinuationCommitBinding, ...],
    *,
    successor_task_id: str,
    prepared_at_utc: str,
) -> dict[str, Any]:
    identity = _predecessor_identity(proof)
    binding = {
        "schema_version": _SCHEMA_VERSION,
        "origin": {
            "kind": ContinuationOrigin.FINALIZED_PROMOTION_TICK.value,
            "evidence": {
                "predecessor_identity": identity.to_mapping(),
                "tock_sha": _tock_sha(proof),
            },
        },
        "original_tick_sha": chain[0].sha,
        "expected_continuation_head_sha": chain[-1].sha,
        "commit_chain": [
            {"sha": item.sha, "parent_sha": item.parent_sha} for item in chain
        ],
        "successor_task_id": successor_task_id,
    }
    return {
        **binding,
        "status": _STATUS_AUTHORIZED,
        "prepared_at_utc": prepared_at_utc,
        "continuation_digest_sha256": binding_digest(binding),
    }


def _bindings_from_contract(
    contract: ContinuationContractView,
) -> tuple[ContinuationCommitBinding, ...]:
    return tuple(
        ContinuationCommitBinding(
            sha=str(entry["sha"]),
            parent_sha=str(entry["parent_sha"]),
        )
        for entry in contract.to_mapping()["commit_chain"]
    )


def _predecessor_identity(proof: OriginProof) -> ContinuationIdentity:
    raw = proof.evidence["predecessor_identity"]
    return ContinuationIdentity(
        task_id=str(raw["task_id"]),
        origin_run_id=str(raw["origin_run_id"]),
        promotion_attempt_id=str(raw["promotion_attempt_id"]),
        result_sha=str(raw["result_sha"]),
        promotion_class=str(raw["promotion_class"]),
    )


def _predecessor_task_id(proof: OriginProof) -> str:
    return _predecessor_identity(proof).task_id


def _tock_sha(proof: OriginProof) -> str:
    return str(proof.evidence["tock_sha"])


def _bound_tock_sha(contract: ContinuationContractView) -> str:
    evidence = contract.to_mapping()["origin"]["evidence"]
    return str(evidence["tock_sha"])


def _write_artifact(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _read_commit(repo: Path, sha: str) -> _CommitView:
    raw = _git_text(repo, "cat-file", "-p", sha)
    header, _, message = raw.partition("\n\n")
    tree_oid = ""
    parents: list[str] = []
    for line in header.splitlines():
        kind, _, value = line.partition(" ")
        if kind == "tree":
            tree_oid = value
        elif kind == "parent":
            parents.append(value)
    return _CommitView(tree_oid=tree_oid, parents=tuple(parents), message=message)


def _git_text(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()
