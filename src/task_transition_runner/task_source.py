"""Public file-backed task source and run-local instruction snapshot.

The loader consumes a loaded ``RunnerConfig`` and the existing task
contract. ``active_task: null`` is terminal rest. Exactly one active task
is snapshotted as-is: each used spec and configured prompt is read once,
persisted under an explicit run root, and bound by logical reference,
relative snapshot path, SHA-256, and byte length. Digests prove
provenance only. ``previous_facts`` is caller-supplied.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from task_transition_runner.config import RunnerConfig
from task_transition_runner.task_contract import (
    TaskContract,
    TaskContractError,
    canonicalize_persisted_task_snapshot,
    load_task_contract,
)


_TASK_SNAPSHOT_RELATIVE = "task_snapshot.yaml"
_INSTRUCTION_META_RELATIVE = "instructions/instruction_snapshot.yaml"
_SPEC_SNAPSHOT_RELATIVE = "instructions/task_spec"
_DEVELOPER_SNAPSHOT_RELATIVE = "instructions/prompts/developer"
_ORCHESTRATOR_SNAPSHOT_RELATIVE = "instructions/prompts/orchestrator"
_VALIDATOR_SNAPSHOT_RELATIVE = "instructions/prompts/validator"
_INSTRUCTION_META_FIELDS = frozenset({"sources"})
_INSTRUCTION_SOURCE_FIELDS = frozenset(
    {"logical_ref", "snapshot_path", "sha256", "byte_length"}
)


class TaskSourceError(Exception):
    """Fail-closed violation of the public task-source contract."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class _DuplicateSnapshotKeyError(yaml.YAMLError):
    """A persisted instruction snapshot repeated a mapping key."""


class _StrictSnapshotLoader(yaml.SafeLoader):
    """Safe instruction-snapshot loader with closed mapping keys."""


def _construct_snapshot_mapping(
    loader: yaml.Loader,
    node: yaml.nodes.MappingNode,
) -> dict[object, object]:
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise _DuplicateSnapshotKeyError("duplicate mapping key")
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictSnapshotLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_snapshot_mapping,
)


@dataclass(frozen=True)
class InstructionSourceBinding:
    logical_ref: str
    snapshot_path: str
    sha256: str
    byte_length: int


@dataclass(frozen=True)
class TaskSourceFacts:
    contract: TaskContract
    sources: tuple[InstructionSourceBinding, ...]
    run_root: Path


@dataclass(frozen=True)
class ReprovedInstructionSource:
    """Claim-facing instruction identity without run-local persistence path."""

    logical_ref: str
    sha256: str
    byte_length: int


@dataclass(frozen=True)
class RunLocalSnapshotReproof:
    """Identity reproduced from one persisted run-local snapshot family."""

    task_id: str
    task_fingerprint: str
    task_snapshot_sha256: str
    instruction_signature_sha256: str
    instruction_sources: tuple[ReprovedInstructionSource, ...]


def load_task_source(
    config: RunnerConfig,
    *,
    run_root: Path,
    previous_facts: TaskSourceFacts | None = None,
) -> TaskSourceFacts:
    previous_snapshot = None if previous_facts is None else previous_facts.contract
    contract = load_task_contract(
        config.task_source,
        target_worktree=config.target_worktree,
        previous_snapshot=previous_snapshot,
    )
    resolved_run_root = Path(run_root).resolve()
    active = contract.active_task
    if active is None:
        sources: tuple[InstructionSourceBinding, ...] = ()
        payloads: tuple[bytes, ...] = ()
    else:
        spec_bytes = _read_spec_bytes(active.spec_ref)
        developer_bytes = _read_prompt_bytes(
            config.prompts.developer,
            root=config.config_root,
        )
        orchestrator_bytes = _read_prompt_bytes(
            config.prompts.orchestrator,
            root=config.config_root,
        )
        validator_bytes = _read_prompt_bytes(
            config.prompts.validator,
            root=config.config_root,
        )
        sources = (
            _binding(active.declared_spec_ref, _SPEC_SNAPSHOT_RELATIVE, spec_bytes),
            _binding(
                config.prompts.declared_developer,
                _DEVELOPER_SNAPSHOT_RELATIVE,
                developer_bytes,
            ),
            _binding(
                config.prompts.declared_orchestrator,
                _ORCHESTRATOR_SNAPSHOT_RELATIVE,
                orchestrator_bytes,
            ),
            _binding(
                config.prompts.declared_validator,
                _VALIDATOR_SNAPSHOT_RELATIVE,
                validator_bytes,
            ),
        )
        payloads = (
            spec_bytes,
            developer_bytes,
            orchestrator_bytes,
            validator_bytes,
        )
        _reject_same_id_instruction_drift(previous_facts, active.task_id, sources)

    _persist_run_local(resolved_run_root, contract.source_bytes, sources, payloads)
    return TaskSourceFacts(
        contract=contract,
        sources=sources,
        run_root=resolved_run_root,
    )


def reprove_run_local_snapshot(
    *,
    run_root: Path,
    expected_task_fingerprint: str,
    expected_task_snapshot_sha256: str,
    expected_instruction_signature_sha256: str,
    target_worktree: Path | None = None,
) -> RunLocalSnapshotReproof:
    """Reprove exact persisted bytes against immutable external anchors."""

    _ = target_worktree
    root = Path(run_root).resolve()
    task_bytes = _read_reproof_bytes(root / _TASK_SNAPSHOT_RELATIVE, root=root)
    task_digest = _sha256(task_bytes)
    if task_digest != expected_task_snapshot_sha256:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    try:
        task_identity = canonicalize_persisted_task_snapshot(task_bytes)
    except TaskContractError:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID") from None
    if task_identity.task_id is None:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    if task_identity.fingerprint != expected_task_fingerprint:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH")

    meta_bytes = _read_reproof_bytes(root / _INSTRUCTION_META_RELATIVE, root=root)
    sources = _load_persisted_instruction_bindings(meta_bytes)
    for binding in sources:
        payload = _read_reproof_bytes(root / binding.snapshot_path, root=root)
        if len(payload) != binding.byte_length or _sha256(payload) != binding.sha256:
            raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    instruction_signature = _instruction_signature(sources)
    if instruction_signature != expected_instruction_signature_sha256:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_IDENTITY_MISMATCH")
    return RunLocalSnapshotReproof(
        task_id=task_identity.task_id,
        task_fingerprint=task_identity.fingerprint,
        task_snapshot_sha256=task_identity.snapshot_sha256,
        instruction_signature_sha256=instruction_signature,
        instruction_sources=tuple(
            ReprovedInstructionSource(
                logical_ref=item.logical_ref,
                sha256=item.sha256,
                byte_length=item.byte_length,
            )
            for item in sources
        ),
    )


def read_instruction_bytes(facts: TaskSourceFacts, logical_ref: str) -> bytes:
    binding = _binding_for(facts, logical_ref)
    root = facts.run_root
    resolved = (root / binding.snapshot_path).resolve()
    if not resolved.is_relative_to(root):
        raise TaskSourceError("SOURCE_PATH_ESCAPE")
    if not resolved.is_file():
        raise TaskSourceError("SOURCE_MISSING")
    try:
        payload = resolved.read_bytes()
    except PermissionError as exc:
        raise TaskSourceError("SOURCE_UNREADABLE") from exc
    if len(payload) != binding.byte_length:
        raise TaskSourceError("SOURCE_VALUE_CONFLICT")
    if _sha256(payload) != binding.sha256:
        raise TaskSourceError("SOURCE_VALUE_CONFLICT")
    return payload


def _read_spec_bytes(spec_ref: Path) -> bytes:
    try:
        return spec_ref.read_bytes()
    except PermissionError as exc:
        raise TaskSourceError("SOURCE_UNREADABLE") from exc


def _read_prompt_bytes(path: Path, *, root: Path) -> bytes:
    if path.is_symlink():
        target = path.readlink()
        if any(part == ".." for part in target.parts):
            raise TaskSourceError("SOURCE_PATH_ESCAPE")
        resolved = path.resolve()
        if not resolved.is_relative_to(root):
            raise TaskSourceError("SOURCE_PATH_ESCAPE")
    if not path.is_file():
        raise TaskSourceError("SOURCE_MISSING")
    try:
        return path.read_bytes()
    except PermissionError as exc:
        raise TaskSourceError("SOURCE_UNREADABLE") from exc


def _reject_same_id_instruction_drift(
    previous_facts: TaskSourceFacts | None,
    task_id: str,
    sources: tuple[InstructionSourceBinding, ...],
) -> None:
    if previous_facts is None:
        return
    previous_active = previous_facts.contract.active_task
    if previous_active is None:
        return
    if previous_active.task_id != task_id:
        return
    if _source_signature(sources) != _source_signature(previous_facts.sources):
        raise TaskSourceError("SOURCE_VALUE_CONFLICT")


def _source_signature(
    sources: tuple[InstructionSourceBinding, ...],
) -> tuple[tuple[str, str, int], ...]:
    return tuple(
        (item.logical_ref, item.sha256, item.byte_length) for item in sources
    )


def _binding(
    logical_ref: str,
    snapshot_path: str,
    payload: bytes,
) -> InstructionSourceBinding:
    return InstructionSourceBinding(
        logical_ref=logical_ref,
        snapshot_path=snapshot_path,
        sha256=_sha256(payload),
        byte_length=len(payload),
    )


def _binding_for(facts: TaskSourceFacts, logical_ref: str) -> InstructionSourceBinding:
    for item in facts.sources:
        if item.logical_ref == logical_ref:
            return item
    raise TaskSourceError("SOURCE_VALUE_CONFLICT")


def _read_reproof_bytes(path: Path, *, root: Path) -> bytes:
    resolved = path.resolve()
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    try:
        return resolved.read_bytes()
    except OSError:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID") from None


def _load_persisted_instruction_bindings(
    meta_bytes: bytes,
) -> tuple[InstructionSourceBinding, ...]:
    try:
        document = yaml.load(meta_bytes, Loader=_StrictSnapshotLoader)
    except (UnicodeDecodeError, yaml.YAMLError):
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID") from None
    if not isinstance(document, dict) or frozenset(document) != _INSTRUCTION_META_FIELDS:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    raw_sources = document["sources"]
    if not isinstance(raw_sources, list) or not raw_sources:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    return tuple(_load_persisted_instruction_binding(item) for item in raw_sources)


def _load_persisted_instruction_binding(item: object) -> InstructionSourceBinding:
    if not isinstance(item, dict) or frozenset(item) != _INSTRUCTION_SOURCE_FIELDS:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    logical_ref = item["logical_ref"]
    snapshot_path = item["snapshot_path"]
    digest = item["sha256"]
    byte_length = item["byte_length"]
    if not isinstance(logical_ref, str) or logical_ref.strip() == "":
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    if not _is_safe_snapshot_path(snapshot_path):
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    if not _is_lowercase_sha256(digest):
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    if type(byte_length) is not int or byte_length < 0:
        raise TaskSourceError("ACTIVATION_PREDECESSOR_SNAPSHOT_INVALID")
    return InstructionSourceBinding(
        logical_ref=logical_ref,
        snapshot_path=snapshot_path,
        sha256=digest,
        byte_length=byte_length,
    )


def _is_safe_snapshot_path(value: object) -> bool:
    if not isinstance(value, str) or value == "":
        return False
    path = Path(value)
    return (
        not path.is_absolute()
        and path.as_posix() == value
        and all(part not in {".", ".."} for part in path.parts)
    )


def _is_lowercase_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _instruction_signature(
    sources: tuple[InstructionSourceBinding, ...],
) -> str:
    canonical = [
        {
            "byte_length": item.byte_length,
            "logical_ref": item.logical_ref,
            "sha256": item.sha256,
            "snapshot_path": item.snapshot_path,
        }
        for item in sources
    ]
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return _sha256(encoded)


def _persist_run_local(
    run_root: Path,
    source_bytes: bytes,
    sources: tuple[InstructionSourceBinding, ...],
    payloads: tuple[bytes, ...],
) -> None:
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / _TASK_SNAPSHOT_RELATIVE).write_bytes(source_bytes)
    if sources == ():
        return
    listed = []
    for item, payload in zip(sources, payloads, strict=True):
        target = run_root / item.snapshot_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        listed.append(
            {
                "logical_ref": item.logical_ref,
                "snapshot_path": item.snapshot_path,
                "sha256": item.sha256,
                "byte_length": item.byte_length,
            }
        )
    meta_path = run_root / _INSTRUCTION_META_RELATIVE
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(
        yaml.safe_dump({"sources": listed}, sort_keys=False),
        encoding="utf-8",
    )


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
