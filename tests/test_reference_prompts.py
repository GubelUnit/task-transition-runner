"""Public reference prompt contracts for the three role files.

The candidate witnesses read only ``prompts/developer.md``,
``prompts/orchestrator.md``, and ``prompts/validator.md``.
While any of those files is absent, that witness fails on the absence alone.
Synthetic prompt bytes and synthetic decision outputs bind declared inputs,
naked role outputs, mutation limits, and privacy limits without creating
prompt files.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.decision_contracts import (
    DecisionContractError,
    DecisionIdentity,
    load_orchestrator_decision,
    load_validator_decision,
)

_MISSING_PROMPTS_SIGNATURE = "EXPECTED_RED_THREE_REFERENCE_PROMPTS_MISSING"
_PROMPT_CONTRACT_INVALID = "PROMPT_CONTRACT_INVALID"
_DECISION_OUTPUT_MALFORMED = "DECISION_OUTPUT_MALFORMED"
_DEVELOPER_PATH = "prompts/developer.md"
_ORCHESTRATOR_PATH = "prompts/orchestrator.md"
_VALIDATOR_PATH = "prompts/validator.md"
_RUNNER_CONFIG_RELATIVE = "examples/reference/runner.yaml"
_RUNNER_CONFIG_SHA256 = (
    "f64132db3a4ea577cee1ec38dec65e3e08adb0e5b7e7bd5237f3a170d21aff5c"
)
_SCHEMA_VERSION = "1.0"
_TASK_ID = "synthetic-task-001"
_ORIGIN_RUN_ID = "origin-run-001"
_PROMOTION_ATTEMPT_ID = "promotion-attempt-001"
_RESULT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_LOG_REF = "runs/origin-run-001/validation/tests.log"
_LOG_BODY = b"synthetic validation log\n"
_LOG_SHA256 = hashlib.sha256(_LOG_BODY).hexdigest()
_ORCHESTRATOR_DECISION_NAME = "orchestrator_decision.yaml"
_VALIDATOR_DECISION_NAME = "validator_decision.yaml"
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_PROMPT_DECLARATIONS = {
    "developer": _DEVELOPER_PATH,
    "orchestrator": _ORCHESTRATOR_PATH,
    "validator": _VALIDATOR_PATH,
}
_COMMON_LINES = (
    "markdown_wrapper: forbidden",
    "chain_of_thought: not_required",
    "state_mutation: forbidden",
    "private_context: forbidden",
    "bytes_and_digests: required",
    "operator_transport: COPY_RETURNED_BYTES_UNCHANGED_TO_DECLARED_PATH",
    "malformed_output: fail_closed",
)
_DEVELOPER_LINES = (
    "role: developer",
    "input: run-local task snapshot",
    "input: run-local task spec snapshot",
    "input: run-local developer prompt snapshot",
    "input: exact allowed paths and validation commands",
    "input: Target working directory",
    "output: TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER",
    "output_grammar: naked",
    "read_scope: current_run_task_and_instruction_snapshots_only",
    "change_scope: allowed_paths_only",
    "validation_scope: declared_commands_only",
    "report: result_and_uncertainties",
    "runtime_mutation: forbidden",
    "decision_mutation: forbidden",
    "promotion_claim: forbidden",
    "settlement_claim: forbidden",
    *_COMMON_LINES,
)
_ORCHESTRATOR_LINES = (
    "role: orchestrator",
    "input: decision request",
    "input: full bound identity",
    "input: task and instruction snapshots",
    "input: WorkerResult and Result SHA",
    "input: diff and changed paths",
    "input: persisted validation",
    "input: committed risk facts",
    "output: EXACT_ORCHESTRATOR_DECISION_YAML_ONLY",
    "output_grammar: naked_yaml",
    "target_path: REQUEST_DECLARED_PATH_ONLY",
    "authority: CHOOSES_PROMOTION_CLASS_BUT_DOES_NOT_EFFECT_PROMOTION",
    "promotion_class_values: GREEN_PROMOTION,RED_TDD_PROMOTION,BLOCKED",
    "review_scope: provided_snapshot_result_and_evidence",
    "identity_binding: task_id,origin_run_id,promotion_attempt_id,result_sha",
    "evidence_reference: required",
    "direct_git_mutation: forbidden",
    "direct_runner_mutation: forbidden",
    "admission_authority: absent",
    *_COMMON_LINES,
)
_VALIDATOR_LINES = (
    "role: validator",
    "input: all bound product and evidence inputs needed for review",
    "input: Orchestrator Decision",
    "output: EXACT_VALIDATOR_DECISION_YAML_ONLY",
    "output_grammar: naked_yaml",
    "target_path: REQUEST_DECLARED_PATH_ONLY",
    "authority: GO_OR_NO_GO_WITHOUT_PROMOTION_CLASS",
    "verdict_values: GO,NO_GO",
    "promotion_class_choice: forbidden",
    "review_scope: independent_snapshot_result_orchestrator_decision_and_evidence",
    "identity_binding: task_id,origin_run_id,promotion_attempt_id,result_sha",
    "evidence_reference: required",
    "direct_git_mutation: forbidden",
    "direct_runner_mutation: forbidden",
    *_COMMON_LINES,
)
_ROLE_LINES = {
    "developer": _DEVELOPER_LINES,
    "orchestrator": _ORCHESTRATOR_LINES,
    "validator": _VALIDATOR_LINES,
}
_CONTRACT_KEYS = frozenset(
    line.split(":", 1)[0] for lines in _ROLE_LINES.values() for line in lines
)
_CLAIM_TOKENS = (
    "DRAINED_AWAITING_TASK",
    "PUBLIC-P-",
    "OSS7-ACC",
    "SETTLED",
    "TERMINAL_REST",
)
_ROLE_FORBIDDEN = {
    "developer": (
        "BLOCKED",
        "CHOOSES_PROMOTION_CLASS",
        "EXACT_ORCHESTRATOR_DECISION_YAML_ONLY",
        "EXACT_VALIDATOR_DECISION_YAML_ONLY",
        "GO_OR_NO_GO",
        "GREEN_PROMOTION",
        "RED_TDD_PROMOTION",
        "decision_type:",
        "promotion_class:",
        "schema_version:",
        "verdict:",
        *_CLAIM_TOKENS,
    ),
    "orchestrator": (
        "EXACT_VALIDATOR_DECISION_YAML_ONLY",
        "GO_OR_NO_GO_WITHOUT_PROMOTION_CLASS",
        "TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER",
        "schema_version:",
        "verdict:",
        "verdict_values:",
        *_CLAIM_TOKENS,
    ),
    "validator": (
        "BLOCKED",
        "CHOOSES_PROMOTION_CLASS",
        "EXACT_ORCHESTRATOR_DECISION_YAML_ONLY",
        "GREEN_PROMOTION",
        "RED_TDD_PROMOTION",
        "TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER",
        "promotion_class:",
        "schema_version:",
        *_CLAIM_TOKENS,
    ),
}
_PRIVATE_MARKERS = (
    "/home/",
    "aethelgard",
    "chronicle",
    "execution_ledger.md",
    "gaia",
    "gubel",
    "hostname",
    "hydra",
    "myfistchat",
    "orp",
    "persona",
    "private_prompt",
    "runenschmied",
    "session_id",
    "settled_predecessor",
    "username",
)
_COT_MARKERS = (
    "chain of thought",
    "chain-of-thought",
    "hidden reasoning",
    "show your reasoning",
    "think step by step",
)
_MUTATION_MARKERS = (
    "git commit",
    "git push",
    "pending_promotion.yaml",
    "promotion_journal.yaml",
    "write runtime/",
)
_PROMOTION_CLASSES = ("GREEN_PROMOTION", "RED_TDD_PROMOTION", "BLOCKED")
_VERDICTS = ("GO", "NO_GO")


class ReferencePromptContractError(Exception):
    """Reference-prompt rule that is stricter than the public decision loader."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _candidate_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _asset_path(relative: str) -> Path:
    return _candidate_root() / relative


def _closed_relative(value: object) -> bool:
    if not isinstance(value, str) or value == "":
        return False
    declared = Path(value)
    if declared.is_absolute() or declared.as_posix() != value:
        return False
    return all(part not in {".", ".."} for part in declared.parts)


def _prompt_text(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID) from exc


def _assert_privacy(payload: bytes) -> None:
    text = payload.decode("utf-8").casefold()
    for marker in _PRIVATE_MARKERS:
        if marker.casefold() in text:
            raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)


def _contract_lines(text: str) -> tuple[str, ...]:
    observed: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if line == "":
            continue
        key, separator, _value = line.partition(":")
        if separator and key in _CONTRACT_KEYS:
            observed.append(line)
    return tuple(observed)


def _prompt_bytes(
    role: str,
    *extra: str,
    omit: frozenset[str] = frozenset(),
) -> bytes:
    lines = [line for line in _ROLE_LINES[role] if line not in omit]
    lines.extend(extra)
    return ("\n".join(lines) + "\n").encode("utf-8")


def assert_prompt_path(relative: str, role: str) -> None:
    """Require the single public path bound to one prompt role."""

    if role not in _PROMPT_DECLARATIONS:
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    if relative != _PROMPT_DECLARATIONS[role] or not _closed_relative(relative):
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    if relative.startswith("examples/"):
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)


def assert_prompt_contract(role: str, payload: bytes) -> None:
    """Require one role's declared inputs, naked output, and closed limits."""

    if role not in _ROLE_LINES:
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    text = _prompt_text(payload)
    if text.strip() == "" or "\x00" in text or "```" in text:
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    _assert_privacy(payload)
    folded = text.casefold()
    for marker in _COT_MARKERS:
        if marker in folded:
            raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    for marker in _MUTATION_MARKERS:
        if marker in folded:
            raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    if _contract_lines(text) != _ROLE_LINES[role]:
        raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)
    for forbidden in _ROLE_FORBIDDEN[role]:
        if forbidden in text:
            raise ReferencePromptContractError(_PROMPT_CONTRACT_INVALID)


def _identity() -> DecisionIdentity:
    return DecisionIdentity(
        task_id=_TASK_ID,
        origin_run_id=_ORIGIN_RUN_ID,
        promotion_attempt_id=_PROMOTION_ATTEMPT_ID,
        result_sha=_RESULT_SHA,
    )


def _evidence() -> dict[str, str]:
    return {
        "kind": "validation_log",
        "ref": _LOG_REF,
        "sha256": _LOG_SHA256,
    }


def _orchestrator_document(promotion_class: str = "GREEN_PROMOTION") -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "decision_type": "orchestrator",
        "task_id": _TASK_ID,
        "origin_run_id": _ORIGIN_RUN_ID,
        "promotion_attempt_id": _PROMOTION_ATTEMPT_ID,
        "result_sha": _RESULT_SHA,
        "ready": True,
        "promotion_class": promotion_class,
        "evidence": [_evidence()],
    }


def _validator_document(verdict: str = "GO") -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "decision_type": "validator",
        "task_id": _TASK_ID,
        "origin_run_id": _ORIGIN_RUN_ID,
        "promotion_attempt_id": _PROMOTION_ATTEMPT_ID,
        "result_sha": _RESULT_SHA,
        "ready": True,
        "verdict": verdict,
        "evidence": [_evidence()],
    }


def _dump(document: dict[str, Any]) -> bytes:
    return yaml.safe_dump(document, sort_keys=False).encode("utf-8")


def _artifact_root(tmp_path: Path) -> Path:
    root = tmp_path / "artifact-root"
    log_path = root / _LOG_REF
    log_path.parent.mkdir(parents=True)
    log_path.write_bytes(_LOG_BODY)
    return root


def assert_naked_decision_envelope(payload: bytes) -> None:
    """Reject markdown, prose, and non-UTF-8 around a decision document."""

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReferencePromptContractError(_DECISION_OUTPUT_MALFORMED) from exc
    if (
        text == ""
        or not text.endswith("\n")
        or text != text.lstrip()
        or "```" in text
        or "\n---" in text
        or text.startswith("---")
        or "\x00" in text
    ):
        raise ReferencePromptContractError(_DECISION_OUTPUT_MALFORMED)
    first = text.splitlines()[0]
    if not first.startswith("schema_version:") or first.startswith("#"):
        raise ReferencePromptContractError(_DECISION_OUTPUT_MALFORMED)


def load_naked_role_output(
    role: str,
    payload: bytes,
    root: Path,
    *,
    identity: DecisionIdentity | None = None,
) -> object:
    """Load one naked decision document through the public role loader."""

    assert_naked_decision_envelope(payload)
    bound_identity = _identity() if identity is None else identity
    if role == "orchestrator":
        path = root / _ORCHESTRATOR_DECISION_NAME
        path.write_bytes(payload)
        return load_orchestrator_decision(
            path,
            expected_identity=bound_identity,
            artifact_root=root,
        )
    if role == "validator":
        path = root / _VALIDATOR_DECISION_NAME
        path.write_bytes(payload)
        return load_validator_decision(
            path,
            expected_identity=bound_identity,
            artifact_root=root,
        )
    raise ReferencePromptContractError(_DECISION_OUTPUT_MALFORMED)


def test_reference_config_declares_three_prompt_paths() -> None:
    payload = _asset_path(_RUNNER_CONFIG_RELATIVE).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == _RUNNER_CONFIG_SHA256
    document = yaml.safe_load(payload.decode("utf-8"))
    assert document["prompts"] == _PROMPT_DECLARATIONS


def test_reference_prompt_candidate_bytes_bind_role_contracts() -> None:
    missing = [
        relative
        for relative in _PROMPT_DECLARATIONS.values()
        if not _asset_path(relative).is_file()
    ]
    if missing:
        pytest.fail(
            f"{_MISSING_PROMPTS_SIGNATURE}: missing {', '.join(missing)}"
        )
    for role, relative in _PROMPT_DECLARATIONS.items():
        payload = _asset_path(relative).read_bytes()
        assert_prompt_path(relative, role)
        assert_prompt_contract(role, payload)
        assert _FINGERPRINT.fullmatch(hashlib.sha256(payload).hexdigest())


def test_prompt_paths_stay_outside_seed_and_reference_examples() -> None:
    for role, relative in _PROMPT_DECLARATIONS.items():
        assert_prompt_path(relative, role)
        declared = Path(relative)
        assert not declared.is_absolute()
        assert not relative.startswith("examples/")

    foreign = (
        ("developer", "examples/fixture/prompts/developer.md"),
        ("developer", "examples/reference/prompts/developer.md"),
        ("developer", "../prompts/developer.md"),
        ("developer", "/tmp/developer.md"),
        ("developer", _ORCHESTRATOR_PATH),
        ("orchestrator", _DEVELOPER_PATH),
        ("validator", "prompts/extra.md"),
        ("reviewer", _VALIDATOR_PATH),
    )
    for role, relative in foreign:
        with pytest.raises(
            ReferencePromptContractError,
            match=_PROMPT_CONTRACT_INVALID,
        ) as exc_info:
            assert_prompt_path(relative, role)

        assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_positive_bytes_bind_declared_inputs_and_naked_output(role: str) -> None:
    payload = _prompt_bytes(role)
    assert_prompt_path(_PROMPT_DECLARATIONS[role], role)
    assert_prompt_contract(role, payload)
    text = payload.decode("utf-8")

    assert text.startswith(f"role: {role}\n")
    assert "markdown_wrapper: forbidden" in text
    assert "chain_of_thought: not_required" in text
    assert "state_mutation: forbidden" in text
    assert "private_context: forbidden" in text
    assert "bytes_and_digests: required" in text
    assert "```" not in text
    assert _FINGERPRINT.fullmatch(hashlib.sha256(payload).hexdigest())


def test_developer_positive_bytes_bind_terminal_adapter_result() -> None:
    text = _prompt_bytes("developer").decode("utf-8")

    assert "output: TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER" in text
    assert "output_grammar: naked" in text
    assert "change_scope: allowed_paths_only" in text
    assert "runtime_mutation: forbidden" in text
    assert "decision_mutation: forbidden" in text
    assert "promotion_claim: forbidden" in text
    assert "settlement_claim: forbidden" in text
    assert "EXACT_ORCHESTRATOR_DECISION_YAML_ONLY" not in text
    assert "EXACT_VALIDATOR_DECISION_YAML_ONLY" not in text


def test_orchestrator_positive_bytes_bind_decision_yaml_without_effect() -> None:
    text = _prompt_bytes("orchestrator").decode("utf-8")

    assert "output: EXACT_ORCHESTRATOR_DECISION_YAML_ONLY" in text
    assert "output_grammar: naked_yaml" in text
    assert "target_path: REQUEST_DECLARED_PATH_ONLY" in text
    assert (
        "authority: CHOOSES_PROMOTION_CLASS_BUT_DOES_NOT_EFFECT_PROMOTION" in text
    )
    assert "promotion_class_values: GREEN_PROMOTION,RED_TDD_PROMOTION,BLOCKED" in text
    assert "admission_authority: absent" in text
    assert "direct_git_mutation: forbidden" in text
    assert "TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER" not in text
    assert "verdict:" not in text


def test_validator_positive_bytes_bind_go_no_go_without_promotion_class() -> None:
    text = _prompt_bytes("validator").decode("utf-8")

    assert "output: EXACT_VALIDATOR_DECISION_YAML_ONLY" in text
    assert "output_grammar: naked_yaml" in text
    assert "target_path: REQUEST_DECLARED_PATH_ONLY" in text
    assert "authority: GO_OR_NO_GO_WITHOUT_PROMOTION_CLASS" in text
    assert "verdict_values: GO,NO_GO" in text
    assert "promotion_class_choice: forbidden" in text
    assert "direct_runner_mutation: forbidden" in text
    assert "GREEN_PROMOTION" not in text
    assert "promotion_class:" not in text


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_positive_bytes_allow_boundary_prose(role: str) -> None:
    payload = _prompt_bytes(role, "No other authority is granted.")
    assert_prompt_contract(role, payload)


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_missing_obligation_is_rejected(role: str) -> None:
    for omitted in _ROLE_LINES[role]:
        payload = _prompt_bytes(role, omit=frozenset({omitted}))
        with pytest.raises(
            ReferencePromptContractError,
            match=_PROMPT_CONTRACT_INVALID,
        ) as exc_info:
            assert_prompt_contract(role, payload)

        assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_markdown_wrapper_is_rejected(role: str) -> None:
    payload = b"```markdown\n" + _prompt_bytes(role) + b"```\n"

    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, payload)

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_chain_of_thought_is_rejected(role: str) -> None:
    payload = _prompt_bytes(role, "think step by step")

    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, payload)

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize(
    ("role", "command"),
    (
        ("developer", "git commit"),
        ("orchestrator", "git push"),
        ("validator", "write runtime/pending_promotion.yaml"),
    ),
)
def test_prompt_contract_negative_state_mutation_is_rejected(
    role: str,
    command: str,
) -> None:
    payload = _prompt_bytes(role, command)

    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, payload)

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_private_marker_is_rejected(role: str) -> None:
    payload = _prompt_bytes(role) + b"\npersona\n"

    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, payload)

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_foreign_output_is_rejected(role: str) -> None:
    foreign = {
        "developer": "output: EXACT_ORCHESTRATOR_DECISION_YAML_ONLY",
        "orchestrator": "output: EXACT_VALIDATOR_DECISION_YAML_ONLY",
        "validator": "output: TERMINAL_CURSOR_ADAPTER_RESULT_CONSUMED_BY_RUNNER",
    }
    payload = _prompt_bytes(role, foreign[role])

    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, payload)

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_empty_is_rejected(role: str) -> None:
    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, b"")

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("role", ("developer", "orchestrator", "validator"))
def test_prompt_contract_negative_non_utf8_is_rejected(role: str) -> None:
    with pytest.raises(
        ReferencePromptContractError,
        match=_PROMPT_CONTRACT_INVALID,
    ) as exc_info:
        assert_prompt_contract(role, b"\xff")

    assert exc_info.value.reason_code == _PROMPT_CONTRACT_INVALID


@pytest.mark.parametrize("promotion_class", _PROMOTION_CLASSES)
def test_orchestrator_output_positive_bytes_are_exact_naked_yaml(
    promotion_class: str,
    tmp_path: Path,
) -> None:
    payload = _dump(_orchestrator_document(promotion_class))
    loaded = load_naked_role_output("orchestrator", payload, _artifact_root(tmp_path))

    assert loaded.decision_type == "orchestrator"
    assert loaded.promotion_class == promotion_class
    assert not hasattr(loaded, "verdict")
    assert "```" not in payload.decode("utf-8")
    assert payload.startswith(b"schema_version:")


@pytest.mark.parametrize("verdict", _VERDICTS)
def test_validator_output_positive_bytes_are_exact_naked_yaml(
    verdict: str,
    tmp_path: Path,
) -> None:
    payload = _dump(_validator_document(verdict))
    loaded = load_naked_role_output("validator", payload, _artifact_root(tmp_path))

    assert loaded.decision_type == "validator"
    assert loaded.verdict == verdict
    assert not hasattr(loaded, "promotion_class")
    assert "```" not in payload.decode("utf-8")
    assert payload.startswith(b"schema_version:")


@pytest.mark.parametrize("role", ("orchestrator", "validator"))
def test_decision_output_negative_markdown_wrapper_is_rejected(
    role: str,
    tmp_path: Path,
) -> None:
    naked = _dump(
        _orchestrator_document() if role == "orchestrator" else _validator_document()
    )
    payload = b"```yaml\n" + naked + b"```\n"

    with pytest.raises(
        ReferencePromptContractError,
        match=_DECISION_OUTPUT_MALFORMED,
    ) as exc_info:
        load_naked_role_output(role, payload, _artifact_root(tmp_path))

    assert exc_info.value.reason_code == _DECISION_OUTPUT_MALFORMED


@pytest.mark.parametrize("role", ("orchestrator", "validator"))
def test_decision_output_negative_leading_prose_is_rejected(
    role: str,
    tmp_path: Path,
) -> None:
    naked = _dump(
        _orchestrator_document() if role == "orchestrator" else _validator_document()
    )
    payload = b"Decision follows.\n" + naked

    with pytest.raises(
        ReferencePromptContractError,
        match=_DECISION_OUTPUT_MALFORMED,
    ) as exc_info:
        load_naked_role_output(role, payload, _artifact_root(tmp_path))

    assert exc_info.value.reason_code == _DECISION_OUTPUT_MALFORMED


def test_decision_output_negative_role_swap_is_rejected(tmp_path: Path) -> None:
    payload = _dump(_validator_document())

    with pytest.raises(DecisionContractError, match="DECISION_VALUE_CONFLICT") as exc_info:
        load_naked_role_output("orchestrator", payload, _artifact_root(tmp_path))

    assert exc_info.value.reason_code == "DECISION_VALUE_CONFLICT"


def test_decision_output_negative_identity_mismatch_is_rejected(tmp_path: Path) -> None:
    payload = _dump(_orchestrator_document())
    foreign = DecisionIdentity(
        task_id="synthetic-task-002",
        origin_run_id=_ORIGIN_RUN_ID,
        promotion_attempt_id=_PROMOTION_ATTEMPT_ID,
        result_sha=_RESULT_SHA,
    )

    with pytest.raises(
        DecisionContractError,
        match="DECISION_IDENTITY_MISMATCH",
    ) as exc_info:
        load_naked_role_output(
            "orchestrator",
            payload,
            _artifact_root(tmp_path),
            identity=foreign,
        )

    assert exc_info.value.reason_code == "DECISION_IDENTITY_MISMATCH"


def test_decision_output_negative_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    document = _orchestrator_document()
    document["evidence"] = [
        {
            "kind": "validation_log",
            "ref": _LOG_REF,
            "sha256": "0" * 64,
        }
    ]

    with pytest.raises(DecisionContractError, match="DECISION_DIGEST_MISMATCH") as exc_info:
        load_naked_role_output(
            "orchestrator",
            _dump(document),
            _artifact_root(tmp_path),
        )

    assert exc_info.value.reason_code == "DECISION_DIGEST_MISMATCH"


def test_decision_output_negative_validator_promotion_class_is_rejected(
    tmp_path: Path,
) -> None:
    document = _validator_document()
    document["promotion_class"] = "GREEN_PROMOTION"

    with pytest.raises(DecisionContractError, match="DECISION_FIELD_UNKNOWN") as exc_info:
        load_naked_role_output("validator", _dump(document), _artifact_root(tmp_path))

    assert exc_info.value.reason_code == "DECISION_FIELD_UNKNOWN"


def test_decision_output_negative_orchestrator_verdict_is_rejected(
    tmp_path: Path,
) -> None:
    document = _orchestrator_document()
    document["verdict"] = "GO"

    with pytest.raises(DecisionContractError, match="DECISION_FIELD_UNKNOWN") as exc_info:
        load_naked_role_output(
            "orchestrator",
            _dump(document),
            _artifact_root(tmp_path),
        )

    assert exc_info.value.reason_code == "DECISION_FIELD_UNKNOWN"


def test_decision_output_negative_unknown_promotion_class_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(DecisionContractError, match="DECISION_VALUE_CONFLICT") as exc_info:
        load_naked_role_output(
            "orchestrator",
            _dump(_orchestrator_document("SETTLED")),
            _artifact_root(tmp_path),
        )

    assert exc_info.value.reason_code == "DECISION_VALUE_CONFLICT"


def test_developer_output_role_rejects_decision_document() -> None:
    with pytest.raises(
        ReferencePromptContractError,
        match=_DECISION_OUTPUT_MALFORMED,
    ) as exc_info:
        load_naked_role_output("developer", _dump(_orchestrator_document()), Path("."))

    assert exc_info.value.reason_code == _DECISION_OUTPUT_MALFORMED


_DEVELOPER_INPUT_LINES = tuple(
    line for line in _DEVELOPER_LINES if line.startswith("input:")
)
_TASK_SNAPSHOT_RUNNER_AUTHORITY_LINE = (
    "runner_authority: run-local task snapshot is not an additional developer payload"
)


def _task_snapshot_authority_is_unambiguous(text: str) -> bool:
    """Bind the existing task-snapshot line without adding a third payload.

    ``input: run-local task snapshot`` stays a declared contract line so the
    closed prompt contract does not gain another ``input:`` payload. Runner
    authority has to be a separate non-payload line. The serialized prompt
    frame therefore remains the developer prompt plus the markdown task spec.
    """

    stripped = [line.strip() for line in text.splitlines() if line.strip()]
    inputs = tuple(line for line in stripped if line.startswith("input:"))
    authority = tuple(line for line in stripped if line.startswith("runner_authority:"))
    return (
        inputs == _DEVELOPER_INPUT_LINES
        and authority == (_TASK_SNAPSHOT_RUNNER_AUTHORITY_LINE,)
    )


def test_developer_prompt_marks_task_snapshot_as_runner_authority() -> None:
    payload = _asset_path(_DEVELOPER_PATH).read_bytes()
    text = _prompt_text(payload)
    if not _task_snapshot_authority_is_unambiguous(text):
        pytest.fail("TASK_SNAPSHOT_PAYLOAD_AUTHORITY_AMBIGUOUS")
    assert_prompt_contract("developer", payload)
