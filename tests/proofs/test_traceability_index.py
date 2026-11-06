"""Regenerierbare, nicht normative Nachweislandkarte der 24 OSS.7-Verpflichtungen.

Jede Zeile entsteht nur aus den hier explizit gebundenen öffentlichen
Autoritäten und aus der live gelesenen Candidate-Commit- und Tree-Identität.
Die Sicht vergibt keine Produkt- oder Claim-Autorität.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import pytest
import unittest
from pathlib import Path
from typing import Mapping

_CANDIDATE_ROOT = Path(__file__).resolve().parents[2]
_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_TOKEN = re.compile(r"^[A-Z0-9_]+$")
_REQUIREMENT_ID = re.compile(r"^OSS7-B(?:0[1-9]|1[0-9]|2[0-4])$")
_PUBLIC_PROOF_ID = re.compile(r"^PUBLIC-P-(?:0[1-9]|10)$")
_OWNER_ID = re.compile(r"^OSS7-ACC-[A-Z0-9_-]+$")
_SUB_EPIC = "OSS.7"
_ROW_STATUS = "BOUND"
_AUTHORITY = "NON_NORMATIVE_DERIVED_VIEW"
_CONFLICT_RULE = "AUTHORITATIVE_CONTRACT_AND_REAL_EVIDENCE_WIN"
_FORM = "ONE_MACHINE_READABLE_REGENERABLE_VIEW"
_LEDGER_EXPORT = "FORBIDDEN"
_EVIDENCE_ABSENT = "NONE"
_QUALIFIED_JOIN_IDS = frozenset(
    {
        "ALL_PUBLIC_P_IDS",
        "PUBLIC-P-01_TO_PUBLIC-P-10_SUPPORT",
        "OSS7_MAXIMUM_VERDICT",
    }
)
_SOURCE_AUTHORITIES = frozenset(
    {
        "CANDIDATE_TEST_AND_COVERAGE_BOUNDARY",
        "CLAIM_CEILING_AND_KNOWN_LIMIT_AUTHORITY",
        "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "PUBLIC_PRODUCT_AND_CLI_CONTRACT",
        "VALIDATED_TREE_EQUALS_EFFECTED_TREE_AUTHORITY",
    }
)
_MINIMUM_ROW_KEYS = frozenset(
    {
        "acceptance_owner",
        "candidate_binding",
        "negative_evidence",
        "positive_evidence",
        "qualified_public_proof_id",
        "recovery_evidence",
        "requirement_id",
        "source_authority",
        "status",
        "sub_epic",
    }
)
_ROW_KEYS = _MINIMUM_ROW_KEYS | {"contract"}
_DOCUMENT_KEYS = frozenset(
    {
        "authority",
        "candidate_binding",
        "conflict_rule",
        "form",
        "private_execution_ledger_export",
        "public_acceptance_owners",
        "public_owner_count",
        "row_count",
        "rows",
        "sub_epic",
    }
)
_BINDING_KEYS = frozenset({"commit", "tree"})
_PUBLIC_OWNER_KEYS = frozenset({"acceptance_owner", "public_id"})
_FORBIDDEN_PASS_STATUSES = frozenset({"PASS", "GREEN", "CLOSED", "PROVEN"})
_FORBIDDEN_PAYLOAD_NEEDLES = (
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
    "username",
)
_FORBIDDEN_EXACT_KEYS = frozenset(
    {
        "chronicle",
        "execution_ledger",
        "hostname",
        "ledger",
        "persona",
        "private_prompt",
        "session_id",
        "task_history",
        "username",
    }
)

_PUBLIC_PROOF_OWNERS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "PUBLIC-P-01",
        "OSS7-ACC-P01_CONTRACT_PROOF_CLOSURE",
        ("OSS7-B06", "OSS7-B07"),
    ),
    (
        "PUBLIC-P-02",
        "OSS7-ACC-P02_DIRECT_GREEN_INTEGRATION",
        ("OSS7-B03", "OSS7-B15", "OSS7-B16"),
    ),
    (
        "PUBLIC-P-03",
        "OSS7-ACC-P03_EXPECTED_RED_CLOSURE",
        ("OSS7-B08",),
    ),
    (
        "PUBLIC-P-04",
        "OSS7-ACC-P04_WORKER_SELF_REPORT_NEGATIVE",
        ("OSS7-B09",),
    ),
    (
        "PUBLIC-P-05",
        "OSS7-ACC-P05_PRE_AUTHORIZATION_RESTART",
        ("OSS7-B10",),
    ),
    (
        "PUBLIC-P-06",
        "OSS7-ACC-P06_POST_EFFECT_RECONCILIATION",
        ("OSS7-B11",),
    ),
    (
        "PUBLIC-P-07",
        "OSS7-ACC-P07_SUCCESSOR_AFTER_RECOVERY",
        ("OSS7-B12",),
    ),
    (
        "PUBLIC-P-08",
        "OSS7-ACC-P08_CODE_AUTHORITY_TOPOLOGY",
        ("OSS7-B13", "OSS7-B14"),
    ),
    (
        "PUBLIC-P-09",
        "OSS7-ACC-P09_FINAL_CLEAN_REPRODUCIBILITY",
        ("OSS7-B04", "OSS7-B05", "OSS7-B17", "OSS7-B18", "OSS7-B19", "OSS7-B20"),
    ),
    (
        "PUBLIC-P-10",
        "OSS7-ACC-P10_KNOWN_LIMITS_CLAIM_JOIN",
        ("OSS7-B22",),
    ),
)

_AUXILIARY_OWNERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "OSS7-ACC-BRIDGE-01_CANONICAL_SETTLED_BASIS_GREEN",
        ("OSS7-B01", "OSS7-B02"),
    ),
    (
        "OSS7-ACC-TRACEABILITY-01_REGENERABLE_INDEX",
        ("OSS7-B21",),
    ),
    (
        "OSS7-ACC-CLOSURE-01_CANDIDATE_READ_ONLY_CLOSURE",
        ("OSS7-B23", "OSS7-B24"),
    ),
)

_ROW_FACTS: dict[str, dict[str, str]] = {
    "OSS7-B01": {
        "contract": "CANONICAL_SETTLED_PROMOTION_BASIS",
        "source_authority": "PUBLIC_PRODUCT_AND_CLI_CONTRACT",
        "qualified_public_proof_id": "PUBLIC-P-01",
        "positive_evidence": "UNIT_AND_REAL_GIT_INTEGRATION_WITHOUT_SETTLED_PREDECESSOR_FIXTURE",
        "negative_evidence": "MISSING_MALFORMED_DRIFTED_SELF_AUTHENTICATING_OR_CROSS_ATTEMPT_BLOCK",
        "recovery_evidence": "JOURNAL_ANCHOR_RECONSTRUCTION_FROM_ADMITTED_PENDING_ONLY",
    },
    "OSS7-B02": {
        "contract": "PREDECESSOR_SNAPSHOT_REPROOF",
        "source_authority": "PUBLIC_PRODUCT_AND_CLI_CONTRACT",
        "qualified_public_proof_id": "PUBLIC-P-01",
        "positive_evidence": "RECOMPUTED_SNAPSHOT_IDENTITY_MATCHES_JOURNAL_ANCHORS",
        "negative_evidence": "BYTE_TAMPER_FAMILY_REPLACEMENT_AND_CROSS_RUN_IDENTITY_BLOCK",
        "recovery_evidence": "JOURNAL_TRANSITION_AND_RECOVERY_ANCHOR_PRESERVATION",
    },
    "OSS7-B03": {
        "contract": "TASK_A_FIRST_EPOCH",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-02",
        "positive_evidence": "DIRECT_GREEN_TASK_A_DRAINED_AWAITING_TASK_WITH_DERIVED_BASIS",
        "negative_evidence": "FALSE_SETTLED_OR_INJECTED_PREDECESSOR_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B04": {
        "contract": "TASK_B_ORDINARY_SUCCESSOR",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-09",
        "positive_evidence": "FINAL_CLEAN_TASK_B_SETTLED_CONTINUATION_CONSUMED",
        "negative_evidence": "BYPASS_WRONG_PREDECESSOR_OR_UNCONSUMED_CONTINUATION_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B05": {
        "contract": "SEPARATE_TERMINAL_REST",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-09",
        "positive_evidence": "SEPARATE_INVOCATION_MUTATION_FREE_TERMINAL_REST",
        "negative_evidence": "NEW_TASK_COMMIT_RUN_OR_CONTINUATION_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B06": {
        "contract": "COMPLETE_PUBLIC_CONTRACT_CLOSURE",
        "source_authority": "PUBLIC_PRODUCT_AND_CLI_CONTRACT",
        "qualified_public_proof_id": "PUBLIC-P-01",
        "positive_evidence": "PUBLIC_IDENTITY_STATE_DECISION_AND_TRANSITION_CONTRACTS",
        "negative_evidence": "MISSING_UNKNOWN_MISMATCH_CONTRADICTION_OR_CONSUMED_CONFLICT_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B07": {
        "contract": "VALIDATED_OBJECT_EQUALS_EFFECTED_OBJECT",
        "source_authority": "VALIDATED_TREE_EQUALS_EFFECTED_TREE_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-01",
        "positive_evidence": "TOCK_TREE_EQUALS_VALIDATED_CANDIDATE_TREE",
        "negative_evidence": "STAGED_UNSTAGED_INDEX_OR_HOOK_MUTATION_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B08": {
        "contract": "EXPECTED_RED_EXACT_CLOSURE",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-03",
        "positive_evidence": "DECLARED_FAILURE_OPENS_AND_LATER_GREEN_CLOSES_SAME_OBLIGATION",
        "negative_evidence": "UNRELATED_OR_UNEXPLAINED_FAILURE_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B09": {
        "contract": "WORKER_SUCCESS_NONAUTHORITY",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-04",
        "positive_evidence": "OBSERVED_ABSENCE_OF_PENDING_JOURNAL_TOCK_CONSUMPTION_AND_SETTLEMENT",
        "negative_evidence": "MISSING_DRAFT_INVALID_OR_MISMATCHED_DECISIONS_STOP_BEFORE_PENDING",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B10": {
        "contract": "PRE_AUTHORIZATION_TWO_PROCESS_RESTART",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-05",
        "positive_evidence": "SECOND_PROCESS_RESUMES_THE_SAME_RESULT_AND_ATTEMPT",
        "negative_evidence": "IDENTITY_DRIFT_OR_CONFLICTING_DUPLICATE_BLOCK",
        "recovery_evidence": "TWO_REAL_PROCESSES_ONE_RESULT_COMMIT_NO_SECOND_WORKER",
    },
    "OSS7-B11": {
        "contract": "POST_EFFECT_TWO_PROCESS_RECONCILIATION",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-06",
        "positive_evidence": "BOOKKEEPING_CONVERGES_WITHOUT_A_SECOND_TOCK",
        "negative_evidence": "SIDE_REF_SIDE_PARENT_DUPLICATE_OR_GEOMETRY_MISMATCH_BLOCK",
        "recovery_evidence": "SECOND_PROCESS_OBSERVES_ONE_CANONICAL_FIRST_PARENT_TOCK",
    },
    "OSS7-B12": {
        "contract": "FRESH_SUCCESSOR_AFTER_RECOVERY",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-07",
        "positive_evidence": "FRESH_TASK_ORDINARY_CONTINUATION_AFTER_RECOVERY_SETTLEMENT",
        "negative_evidence": "ORDINARY_NON_RECOVERY_TASK_B_CANNOT_SUBSTITUTE",
        "recovery_evidence": "PRIOR_RECOVERY_SETTLEMENT_BEFORE_FRESH_TASK_IDENTITY",
    },
    "OSS7-B13": {
        "contract": "CODE_AUTHORITY_POSITIVE_TOPOLOGY",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-08",
        "positive_evidence": "CLEAN_EXACT_SOURCE_WITH_DISTINCT_TARGET_AND_RUNTIME",
        "negative_evidence": "ROOT_OVERLAP_OR_UNRESOLVED_IDENTITY_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B14": {
        "contract": "CODE_AUTHORITY_NEGATIVE_TOPOLOGY",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-08",
        "positive_evidence": _EVIDENCE_ABSENT,
        "negative_evidence": "DIRTY_MOVING_SHADOWED_WRONG_SOURCE_OR_HISTORY_WITHOUT_RUNTIME_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B15": {
        "contract": "HISTORY_FREE_REFERENCE_SEED",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-02",
        "positive_evidence": "NEW_REPOSITORY_AND_ONE_NEUTRAL_BASELINE_FROM_BYTE_SEED",
        "negative_evidence": "SEED_CONTAINING_GIT_RESULT_RUNTIME_PROTOCOL_OR_IMPLEMENTATION_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B16": {
        "contract": "IMMUTABLE_EXTERNAL_REFERENCE_TESTS",
        "source_authority": "CANDIDATE_TEST_AND_COVERAGE_BOUNDARY",
        "qualified_public_proof_id": "PUBLIC-P-02",
        "positive_evidence": "TASK_CONTRACTS_OUTSIDE_ALLOWED_PATHS_AND_TASK_B_RERUNS_BOTH",
        "negative_evidence": "TEST_MUTATION_IS_PROMOTION_BLOCKING",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B17": {
        "contract": "PROMPT_ROLE_AND_HANDOFF_BOUNDARY",
        "source_authority": "PUBLIC_PRODUCT_AND_CLI_CONTRACT",
        "qualified_public_proof_id": "PUBLIC-P-09",
        "positive_evidence": "THREE_PROMPTS_EMIT_EXACT_ROLE_OUTPUTS_FROM_DECLARED_ARTIFACTS",
        "negative_evidence": "MARKDOWN_WRAPPER_MALFORMED_YAML_MISMATCH_OR_DIRECT_MUTATION_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B18": {
        "contract": "FROZEN_REFERENCE_BUNDLE",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-09",
        "positive_evidence": "DIGEST_BOUND_REFERENCE_BUNDLE_BYTES",
        "negative_evidence": "POST_FREEZE_BYTE_DRIFT_INVALIDATES_PROOF",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B19": {
        "contract": "FRESH_FINAL_ROOTS",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-09",
        "positive_evidence": "PAIRWISE_DISTINCT_FRESH_RUNNER_INPUT_TARGET_RUNTIME_AND_EVIDENCE_ROOTS",
        "negative_evidence": "REHEARSAL_OR_PRIVATE_ROOT_REUSE_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B20": {
        "contract": "DETERMINISTIC_CONTRACT_SUITE_REEXECUTION",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-09",
        "positive_evidence": "FROZEN_DETERMINISTIC_CONTRACTS_RERUN_GREEN",
        "negative_evidence": "MODEL_OUTPUT_EQUALITY_IS_NOT_REQUIRED",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B21": {
        "contract": "REGENERABLE_TRACEABILITY_INDEX",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "ALL_PUBLIC_P_IDS",
        "positive_evidence": "BYTE_IDENTICAL_REGENERATION",
        "negative_evidence": "STALE_AMBIGUOUS_MISSING_OR_SELF_AUTHORIZING_ROW_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B22": {
        "contract": "KNOWN_LIMITS_EVIDENCE_JOIN",
        "source_authority": "CLAIM_CEILING_AND_KNOWN_LIMIT_AUTHORITY",
        "qualified_public_proof_id": "PUBLIC-P-10",
        "positive_evidence": "SUPPORTED_AND_UNSUPPORTED_BOUNDARIES_JOINED_TO_LAW_OR_EVIDENCE",
        "negative_evidence": "INVENTED_CAPABILITY_OR_ERASED_LIMIT_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B23": {
        "contract": "CANDIDATE_VALIDATION_COVERAGE_AND_PRIVACY",
        "source_authority": "CANDIDATE_TEST_AND_COVERAGE_BOUNDARY",
        "qualified_public_proof_id": "PUBLIC-P-01_TO_PUBLIC-P-10_SUPPORT",
        "positive_evidence": "CANDIDATE_VALIDATION_SCOPED_COVERAGE_DEPENDENCY_STASIS_AND_PRIVACY_SCAN",
        "negative_evidence": "COVERAGE_IS_NOT_CORRECTNESS_AND_PRIVATE_FULL_REPO_COUNTS_FORBIDDEN",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
    "OSS7-B24": {
        "contract": "FINAL_CLEAN_RUN_CAUSAL_CLOSURE",
        "source_authority": "PUBLIC_CLAIM_TO_PROOF_AUTHORITY",
        "qualified_public_proof_id": "OSS7_MAXIMUM_VERDICT",
        "positive_evidence": "ASSERTED_SUCCESS_CAUSED_BY_AUTHORIZED_REAL_WITNESS",
        "negative_evidence": "SUCCESS_DOUBLE_SWALLOWED_FAILURE_IN_PLACE_REPAIR_OR_CONTRADICTORY_FIXTURE_BLOCK",
        "recovery_evidence": _EVIDENCE_ABSENT,
    },
}


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError("Candidate-Git-Identität ist nicht lesbar")
    return completed.stdout.strip()


def _read_candidate_binding(repo: Path) -> dict[str, str]:
    refs = _git(repo, "for-each-ref", "--format=%(refname)", "refs/heads/")
    if refs.splitlines() != ["refs/heads/main"]:
        raise AssertionError("Candidate-Refs sind nicht ausschließlich main")
    commit = _git(repo, "rev-parse", "HEAD")
    main = _git(repo, "rev-parse", "refs/heads/main")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    if commit != main:
        raise AssertionError("HEAD ist nicht refs/heads/main")
    if _HEX40.fullmatch(commit) is None or _HEX40.fullmatch(tree) is None:
        raise AssertionError("Candidate-Bindung ist kein 40-stelliges Kleinhex")
    return {"commit": commit, "tree": tree}


def _owner_by_requirement() -> dict[str, str]:
    owners: dict[str, str] = {}
    for _public_id, owner, requirement_ids in _PUBLIC_PROOF_OWNERS:
        for requirement_id in requirement_ids:
            if requirement_id in owners:
                raise AssertionError("Anforderung hat mehr als einen Acceptance Owner")
            owners[requirement_id] = owner
    for owner, requirement_ids in _AUXILIARY_OWNERS:
        for requirement_id in requirement_ids:
            if requirement_id in owners:
                raise AssertionError("Anforderung hat mehr als einen Acceptance Owner")
            owners[requirement_id] = owner
    return owners


def _expected_requirement_ids() -> tuple[str, ...]:
    return tuple(f"OSS7-B{index:02d}" for index in range(1, 25))


def _binding_copy(binding: Mapping[str, str]) -> dict[str, str]:
    return {"commit": binding["commit"], "tree": binding["tree"]}


def _render_document(binding: Mapping[str, str]) -> dict[str, object]:
    owners = _owner_by_requirement()
    expected_ids = _expected_requirement_ids()
    if set(owners) != set(expected_ids) or set(_ROW_FACTS) != set(expected_ids):
        raise AssertionError("Verpflichtungsmenge ist nicht exakt OSS7-B01 bis OSS7-B24")
    if len(_PUBLIC_PROOF_OWNERS) != 10:
        raise AssertionError("PUBLIC-P-Owner-Register hat nicht genau zehn Einträge")
    public_ids = [item[0] for item in _PUBLIC_PROOF_OWNERS]
    public_owners = [item[1] for item in _PUBLIC_PROOF_OWNERS]
    if public_ids != [f"PUBLIC-P-{index:02d}" for index in range(1, 11)]:
        raise AssertionError("PUBLIC-P-IDs sind nicht genau PUBLIC-P-01 bis PUBLIC-P-10")
    if len(set(public_owners)) != 10:
        raise AssertionError("PUBLIC-P-Acceptance-Owner sind nicht eindeutig")
    rows: list[dict[str, object]] = []
    copied_binding = _binding_copy(binding)
    for requirement_id in expected_ids:
        facts = _ROW_FACTS[requirement_id]
        rows.append(
            {
                "acceptance_owner": owners[requirement_id],
                "candidate_binding": _binding_copy(copied_binding),
                "contract": facts["contract"],
                "negative_evidence": facts["negative_evidence"],
                "positive_evidence": facts["positive_evidence"],
                "qualified_public_proof_id": facts["qualified_public_proof_id"],
                "recovery_evidence": facts["recovery_evidence"],
                "requirement_id": requirement_id,
                "source_authority": facts["source_authority"],
                "status": _ROW_STATUS,
                "sub_epic": _SUB_EPIC,
            }
        )
    public_acceptance_owners = [
        {"acceptance_owner": owner, "public_id": public_id}
        for public_id, owner, _requirement_ids in _PUBLIC_PROOF_OWNERS
    ]
    return {
        "authority": _AUTHORITY,
        "candidate_binding": copied_binding,
        "conflict_rule": _CONFLICT_RULE,
        "form": _FORM,
        "private_execution_ledger_export": _LEDGER_EXPORT,
        "public_acceptance_owners": public_acceptance_owners,
        "public_owner_count": 10,
        "row_count": 24,
        "rows": rows,
        "sub_epic": _SUB_EPIC,
    }


def _canonical_bytes(document: Mapping[str, object]) -> bytes:
    payload = json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (payload + "\n").encode("ascii")


def _walk_keys(value: object) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            found.append(str(key))
            found.extend(_walk_keys(nested))
    elif isinstance(value, list):
        for nested in value:
            found.extend(_walk_keys(nested))
    return found


def _assert_no_private_payload(raw: bytes) -> None:
    if any(byte > 127 for byte in raw):
        raise AssertionError("Nachweislandkarte enthält nicht-ASCII-Bytes")
    text = raw.decode("ascii").casefold()
    for needle in _FORBIDDEN_PAYLOAD_NEEDLES:
        if needle in text:
            raise AssertionError("Nachweislandkarte enthält private Ledger- oder Identitätsdaten")
    if re.search(r"(^|[^A-Z0-9_])/[A-Z0-9_./-]+", text, flags=re.IGNORECASE):
        raise AssertionError("Nachweislandkarte enthält einen Pfad")


def _assert_token(value: object, *, allow_none: bool = False) -> str:
    if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
        raise AssertionError("Nachweisklasse ist kein geschlossenes Token")
    if value == _EVIDENCE_ABSENT and not allow_none:
        raise AssertionError("Pflichttoken fehlt")
    return value


def _assert_document(document: object) -> None:
    if not isinstance(document, dict) or set(document) != _DOCUMENT_KEYS:
        raise AssertionError("Nachweislandkarte hat nicht den geschlossenen Schlüsselumfang")
    if document["authority"] != _AUTHORITY:
        raise AssertionError("Autorität der Sicht ist nicht nicht-normativ")
    if document["conflict_rule"] != _CONFLICT_RULE:
        raise AssertionError("Konfliktregel fehlt")
    if document["form"] != _FORM:
        raise AssertionError("Form der Sicht ist nicht die eine regenerierbare View")
    if document["private_execution_ledger_export"] != _LEDGER_EXPORT:
        raise AssertionError("privater Ledger-Export ist nicht verboten")
    if document["sub_epic"] != _SUB_EPIC:
        raise AssertionError("Sub-Epic ist nicht OSS.7")
    binding = document["candidate_binding"]
    if not isinstance(binding, dict) or set(binding) != _BINDING_KEYS:
        raise AssertionError("Candidate-Bindung ist unvollständig")
    if _HEX40.fullmatch(str(binding["commit"])) is None or _HEX40.fullmatch(str(binding["tree"])) is None:
        raise AssertionError("Candidate-Bindung ist kein 40-stelliges Kleinhex")
    rows = document["rows"]
    public_owners = document["public_acceptance_owners"]
    if not isinstance(rows, list) or not isinstance(public_owners, list):
        raise AssertionError("Zeilen oder Owner-Register sind keine Listen")
    if document["row_count"] != 24 or len(rows) != 24:
        raise AssertionError("nicht genau 24 OSS7-B-Zeilen")
    if document["public_owner_count"] != 10 or len(public_owners) != 10:
        raise AssertionError("nicht genau zehn PUBLIC-P-Owner")
    expected_ids = list(_expected_requirement_ids())
    seen_ids: list[str] = []
    rows_by_owner: dict[str, list[str]] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != _ROW_KEYS:
            raise AssertionError("Zeilenschlüssel weichen vom geschlossenen Vertrag ab")
        if not _MINIMUM_ROW_KEYS <= set(row):
            raise AssertionError("Pflichtschlüssel einer Zeile fehlen")
        requirement_id = row["requirement_id"]
        if not isinstance(requirement_id, str) or _REQUIREMENT_ID.fullmatch(requirement_id) is None:
            raise AssertionError("requirement_id liegt außerhalb OSS7-B01 bis OSS7-B24")
        seen_ids.append(requirement_id)
        owner = row["acceptance_owner"]
        if not isinstance(owner, str) or _OWNER_ID.fullmatch(owner) is None:
            raise AssertionError("Acceptance Owner fehlt oder ist mehrdeutig")
        if owner == row["source_authority"] or "TRACEABILITY_INDEX" in str(row["source_authority"]):
            raise AssertionError("Zeile autorisiert sich selbst")
        source_authority = _assert_token(row["source_authority"])
        if source_authority not in _SOURCE_AUTHORITIES:
            raise AssertionError("source_authority liegt außerhalb der öffentlichen Rollen")
        qualified = row["qualified_public_proof_id"]
        if not isinstance(qualified, str) or re.fullmatch(r"^[A-Z0-9_-]+$", qualified) is None:
            raise AssertionError("qualified_public_proof_id ist kein geschlossenes Token")
        if _PUBLIC_PROOF_ID.fullmatch(qualified) is None and qualified not in _QUALIFIED_JOIN_IDS:
            raise AssertionError("qualified_public_proof_id ist nicht gebunden")
        for evidence_key in ("positive_evidence", "negative_evidence", "recovery_evidence"):
            _assert_token(row[evidence_key], allow_none=True)
        if row["status"] != _ROW_STATUS or row["status"] in _FORBIDDEN_PASS_STATUSES:
            raise AssertionError("Zeilenstatus behauptet einen nicht belegten Proof-Pass")
        if row["sub_epic"] != _SUB_EPIC:
            raise AssertionError("Zeilen-Sub-Epic ist nicht OSS.7")
        row_binding = row["candidate_binding"]
        if row_binding != binding:
            raise AssertionError("Zeilenbindung weicht von der Candidate-Bindung ab")
        _assert_token(row["contract"])
        rows_by_owner.setdefault(owner, []).append(requirement_id)
    if seen_ids != expected_ids or len(set(seen_ids)) != 24:
        raise AssertionError("Anforderungen sind nicht eindeutig und vollständig")
    seen_public_ids: list[str] = []
    seen_public_owners: list[str] = []
    for entry in public_owners:
        if not isinstance(entry, dict) or set(entry) != _PUBLIC_OWNER_KEYS:
            raise AssertionError("PUBLIC-P-Owner-Eintrag ist nicht geschlossen")
        public_id = entry["public_id"]
        owner = entry["acceptance_owner"]
        if not isinstance(public_id, str) or _PUBLIC_PROOF_ID.fullmatch(public_id) is None:
            raise AssertionError("PUBLIC-P-ID ist nicht qualifiziert")
        if not isinstance(owner, str) or _OWNER_ID.fullmatch(owner) is None:
            raise AssertionError("PUBLIC-P-Owner fehlt")
        seen_public_ids.append(public_id)
        seen_public_owners.append(owner)
        owned_rows = tuple(rows_by_owner.get(owner, ()))
        expected_rows = dict(
            (item[0], item[2]) for item in _PUBLIC_PROOF_OWNERS
        ).get(public_id)
        if expected_rows is None or owned_rows != expected_rows:
            raise AssertionError("PUBLIC-P-Owner ist nicht eindeutig seinen Anforderungen zugeordnet")
        for requirement_id in owned_rows:
            matched = next(row for row in rows if row["requirement_id"] == requirement_id)
            if matched["qualified_public_proof_id"] != public_id:
                raise AssertionError("Owner-Zeile zeigt auf eine andere PUBLIC-P-ID")
            if matched["acceptance_owner"] != owner:
                raise AssertionError("Anforderung hat mehr als einen Acceptance Owner")
    if seen_public_ids != [f"PUBLIC-P-{index:02d}" for index in range(1, 11)]:
        raise AssertionError("PUBLIC-P-Register ist nicht genau PUBLIC-P-01 bis PUBLIC-P-10")
    if len(set(seen_public_ids)) != 10 or len(set(seen_public_owners)) != 10:
        raise AssertionError("PUBLIC-P-Owner sind nicht eineindeutig")
    auxiliary_ids: list[str] = []
    for owner, requirement_ids in _AUXILIARY_OWNERS:
        if owner in seen_public_owners:
            raise AssertionError("Hilfsowner ist als zusätzlicher PUBLIC-P-Owner eingetragen")
        if tuple(rows_by_owner.get(owner, ())) != requirement_ids:
            raise AssertionError("Hilfsowner-Zuordnung ist mehrdeutig oder unvollständig")
        auxiliary_ids.extend(requirement_ids)
    assigned = [requirement_id for requirement_ids in rows_by_owner.values() for requirement_id in requirement_ids]
    if sorted(assigned) != expected_ids:
        raise AssertionError("nicht jede Anforderung hat genau einen Owner")
    if _FORBIDDEN_EXACT_KEYS.intersection(_walk_keys(document)):
        raise AssertionError("Nachweislandkarte exportiert private Ledger-Schlüssel")


def regenerate_traceability_index(repo: Path | None = None) -> bytes:
    """Erzeuge eine frische kanonische Sicht aus Git-Identität und öffentlichen Bindungen."""
    binding = _read_candidate_binding(repo or _CANDIDATE_ROOT)
    document = _render_document(binding)
    _assert_document(document)
    raw = _canonical_bytes(document)
    _assert_no_private_payload(raw)
    parsed = json.loads(raw)
    _assert_document(parsed)
    if _canonical_bytes(parsed) != raw:
        raise AssertionError("kanonische Bytes sind nicht regenerationsstabil")
    return raw


def _require_traceability_witness_roots() -> tuple[Path, Path]:
    if os.environ.get("OSS7_TRACEABILITY_WITNESS") != "1":
        pytest.skip("OSS7_TRACEABILITY_WITNESS ist erforderlich")
    evidence = os.environ.get("TRACEABILITY_EVIDENCE_ROOT")
    work = os.environ.get("TRACEABILITY_WORK_ROOT")
    if not evidence or not work:
        pytest.skip("TRACEABILITY_EVIDENCE_ROOT und TRACEABILITY_WORK_ROOT sind erforderlich")
    evidence_root = Path(evidence).expanduser().resolve()
    work_root = Path(work).expanduser().resolve()
    if evidence_root.exists() or work_root.exists():
        raise AssertionError("bestehender oder fremder Publish-Root ist nicht leer")
    if not work_root.is_relative_to(evidence_root):
        raise AssertionError("Work-Root liegt ausserhalb des Evidence-Roots")
    return evidence_root, work_root


def _publish(raw: bytes, evidence_root: Path) -> Path:
    target = evidence_root / "traceability_index.json"
    temporary = evidence_root / ".traceability_index.json.tmp"
    try:
        temporary.write_bytes(raw)
        temporary.replace(target)
        return target
    finally:
        temporary.unlink(missing_ok=True)


def _remove_traceability_index(evidence_root: Path) -> None:
    target = evidence_root / "traceability_index.json"
    if target.is_file():
        target.unlink()


def _seal_traceability_witness_log(evidence_root: Path, status: str) -> None:
    log_path = evidence_root / "readiness/generated/traceability_witness.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.exists():
        log_path.write_text(status + "\n", encoding="utf-8")
    os.chmod(log_path, 0o444)


class TraceabilityIndexRegenerationTests(unittest.TestCase):
    def test_two_fresh_regenerations_are_byte_identical(self) -> None:
        repo = _CANDIDATE_ROOT
        first = regenerate_traceability_index(repo)
        second = regenerate_traceability_index(repo)
        self.assertEqual(first, second)
        reread = json.loads(first)
        _assert_document(reread)
        _assert_no_private_payload(first)

    def test_index_binds_the_live_candidate_commit_and_tree(self) -> None:
        binding = _read_candidate_binding(_CANDIDATE_ROOT)
        document = json.loads(regenerate_traceability_index(_CANDIDATE_ROOT))
        self.assertEqual(document["candidate_binding"], binding)
        self.assertEqual(document["row_count"], 24)
        self.assertEqual(document["public_owner_count"], 10)
        self.assertEqual(
            [row["requirement_id"] for row in document["rows"]],
            list(_expected_requirement_ids()),
        )

    def test_each_requirement_has_one_owner_and_separated_evidence(self) -> None:
        document = json.loads(regenerate_traceability_index(_CANDIDATE_ROOT))
        evidence_keys = ("positive_evidence", "negative_evidence", "recovery_evidence")
        owners_by_requirement: dict[str, str] = {}
        for row in document["rows"]:
            self.assertIsInstance(row["acceptance_owner"], str)
            self.assertTrue(all(row[key] for key in evidence_keys))
            self.assertEqual(len(evidence_keys), 3)
            owners_by_requirement[row["requirement_id"]] = row["acceptance_owner"]
        self.assertEqual(len(owners_by_requirement), 24)

    def test_duplicate_public_owner_is_rejected(self) -> None:
        document = json.loads(regenerate_traceability_index(_CANDIDATE_ROOT))
        document["public_acceptance_owners"].append(
            {
                "acceptance_owner": "OSS7-ACC-P01_CONTRACT_PROOF_CLOSURE",
                "public_id": "PUBLIC-P-01",
            }
        )
        document["public_owner_count"] = len(document["public_acceptance_owners"])
        with self.assertRaises(AssertionError):
            _assert_document(document)

    def test_shared_public_owner_is_rejected(self) -> None:
        document = json.loads(regenerate_traceability_index(_CANDIDATE_ROOT))
        document["public_acceptance_owners"][1]["acceptance_owner"] = document[
            "public_acceptance_owners"
        ][0]["acceptance_owner"]
        with self.assertRaises(AssertionError):
            _assert_document(document)

    def test_private_ledger_payload_is_rejected(self) -> None:
        document = json.loads(regenerate_traceability_index(_CANDIDATE_ROOT))
        document["rows"][0]["positive_evidence"] = "/home/private/execution_ledger.md"
        raw = _canonical_bytes(document)
        with self.assertRaises(AssertionError):
            _assert_no_private_payload(raw)

    def test_self_authorizing_row_is_rejected(self) -> None:
        document = json.loads(regenerate_traceability_index(_CANDIDATE_ROOT))
        document["rows"][20]["source_authority"] = "TRACEABILITY_INDEX"
        with self.assertRaises(AssertionError):
            _assert_document(document)

    @pytest.mark.external_witness
    def test_publish_traceability_index_from_explicit_witness_roots(self) -> None:
        evidence_root, work_root = _require_traceability_witness_roots()
        try:
            first = regenerate_traceability_index(_CANDIDATE_ROOT)
            second = regenerate_traceability_index(_CANDIDATE_ROOT)
            if first != second:
                raise AssertionError("zwei frische Regenerationen sind nicht bytegleich")
            evidence_root.mkdir(parents=True)
            work_root.mkdir(parents=True)
            first_path = work_root / "traceability_index_regeneration_1.json"
            second_path = work_root / "traceability_index_regeneration_2.json"
            first_path.write_bytes(first)
            second_path.write_bytes(second)
            if first_path.read_bytes() != second_path.read_bytes():
                raise AssertionError("geschriebene Regenerationen sind nicht bytegleich")
            published = _publish(first, evidence_root)
            if published.read_bytes() != first:
                raise AssertionError("veröffentlichter Index weicht von der Regeneration ab")
            reread = json.loads(published.read_bytes())
            _assert_document(reread)
            _assert_no_private_payload(published.read_bytes())
            _seal_traceability_witness_log(evidence_root, "PUBLISHED")
        except Exception:
            _remove_traceability_index(evidence_root)
            _seal_traceability_witness_log(evidence_root, "FAILED")
            raise


if __name__ == "__main__":
    unittest.main()
