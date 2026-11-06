"""Vertragsgrenze zwischen hermetischer Default-Suite und externen Witnesses.

Die Datei führt keinen Witness aus, schreibt keine Evidence und startet
keinen verschachtelten pytest-Prozess. Sie liest nur Candidate-Quellen
und prüft Marker-, Opt-in-, Root-, A5-, Traceability-, historische
P02-Pin- und lokale Residuum-Verträge statisch.
"""

from __future__ import annotations

import atexit
import ast
import shlex
import tomllib
from pathlib import Path

import pytest


_CANDIDATE_ROOT = Path(__file__).resolve().parents[1]
_P02_PATH = _CANDIDATE_ROOT / "tests/integration/test_direct_green.py"
_TRACEABILITY_PATH = _CANDIDATE_ROOT / "tests/proofs/test_traceability_index.py"
_A5_PATH = _CANDIDATE_ROOT / "tests/test_cli_run.py"
_P02_WITNESS = "test_public_p02_direct_green_a_to_b"
_P02_OPT_IN = "OSS7_PUBLIC_P02_WITNESS"
_P02_EVIDENCE_ROOT = "PUBLIC_P02_EVIDENCE_ROOT"
_P02_WORK_ROOT = "PUBLIC_P02_WORK_ROOT"
_P02_PINS = {
    "_BASE_COMMIT": "3f836c0cb47c6af925d14e5258d0488ab374317a",
    "_BASE_TREE": "fb0124da699dd448fce9cf5146b49df2f9938f9f",
    "_MANIFEST_SHA256": "3bb1bb7e0896287fd84c46d3ebe5d31195da3062f3de8453b8a1d14811000eec",
}
_TRACEABILITY_PUBLISHER = "test_publish_traceability_index_from_explicit_witness_roots"
_TRACEABILITY_OPT_IN = "OSS7_TRACEABILITY_WITNESS"
_TRACEABILITY_EVIDENCE_ROOT = "TRACEABILITY_EVIDENCE_ROOT"
_TRACEABILITY_WORK_ROOT = "TRACEABILITY_WORK_ROOT"
_TRACEABILITY_HERMETIC_NODES = (
    "test_two_fresh_regenerations_are_byte_identical",
    "test_index_binds_the_live_candidate_commit_and_tree",
    "test_each_requirement_has_one_owner_and_separated_evidence",
    "test_duplicate_public_owner_is_rejected",
    "test_shared_public_owner_is_rejected",
    "test_private_ledger_payload_is_rejected",
    "test_self_authorizing_row_is_rejected",
)
_A5_NODE = (
    "test_a5_real_git_bridge_derives_basis_from_journal_drain_consumption_"
    "main_first_parent_tock_and_run_snapshots_without_settled_predecessor_fixture"
)
_A5_REQUIRED_ENV = frozenset(
    {
        "A5_EVIDENCE_ROOT",
        "A5_RUNNER_VALIDATION_ROOT",
        "A5_RUNNER_VALIDATION_LOG",
        "A5_RUNNER_VALIDATION_LOG_SHA256",
    }
)
_A5_FORBIDDEN_ENV = frozenset(
    {
        "EVIDENCE_ROOT",
        "RUNNER_VALIDATION_ROOT",
        "RUNNER_VALIDATION_LOG",
        "RUNNER_VALIDATION_LOG_SHA256",
    }
)
_GENERIC_ROOT_ENV = frozenset({"EVIDENCE_ROOT", "WORK_ROOT"})
_RESIDUE_DIRECTORY_NAMES = frozenset({"__pycache__", ".pytest_cache"})
_RESIDUE_FILE_NAMES = frozenset({".coverage"})
_OWN_BYTECODE_PREFIX = "test_default_suite_hermetic_boundary"
_SKIP_CALLS = frozenset({"skip", "skipTest"})
_FAIL_CALLS = frozenset({"fail", "_stop"})


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def residue_path_is_cleanup_eligible(candidate: Path, path: Path) -> bool:
    """Nur ignoriertes Candidate-Residuum darf die gebundene Reinigung sehen."""
    resolved = path.resolve()
    candidate_resolved = candidate.resolve()
    if resolved == candidate_resolved or not _is_within(resolved, candidate_resolved):
        return False
    if resolved.name in _RESIDUE_FILE_NAMES:
        return True
    current = resolved
    while current != candidate_resolved:
        if current.name in _RESIDUE_DIRECTORY_NAMES:
            return True
        current = current.parent
    return False


def _residue_files(candidate: Path) -> list[Path]:
    files: list[Path] = []
    for path in candidate.rglob("*"):
        if path.is_file() and residue_path_is_cleanup_eligible(candidate, path):
            files.append(path)
    return files


def _snapshot_residue(candidate: Path) -> dict[Path, bytes]:
    return {path.resolve(): path.read_bytes() for path in _residue_files(candidate)}


_RESIDUE_SNAPSHOT = _snapshot_residue(_CANDIDATE_ROOT)


def _remove_file(path: Path) -> None:
    if path.is_file() and residue_path_is_cleanup_eligible(_CANDIDATE_ROOT, path):
        path.unlink()


def restore_or_remove_candidate_ignored_runtime_residue() -> None:
    """Stellt vorgefundenes Residuum wieder her und entfernt nur neues."""
    current = {path.resolve(): path for path in _residue_files(_CANDIDATE_ROOT)}
    for resolved, payload in _RESIDUE_SNAPSHOT.items():
        if resolved.name.startswith(_OWN_BYTECODE_PREFIX):
            _remove_file(resolved)
            continue
        existing = current.get(resolved)
        if existing is None:
            if residue_path_is_cleanup_eligible(_CANDIDATE_ROOT, resolved):
                resolved.parent.mkdir(parents=True, exist_ok=True)
                resolved.write_bytes(payload)
            continue
        if existing.read_bytes() != payload:
            existing.write_bytes(payload)
    for resolved in current:
        if resolved in _RESIDUE_SNAPSHOT:
            continue
        _remove_file(resolved)
    for path in _residue_files(_CANDIDATE_ROOT):
        if path.name.startswith(_OWN_BYTECODE_PREFIX) and path.parent.name == "__pycache__":
            _remove_file(path)


atexit.register(restore_or_remove_candidate_ignored_runtime_residue)
_CLEANUP_CONTRACT_BOUND = True


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parsed(path: Path) -> ast.Module:
    return ast.parse(_read_text(path), filename=str(path))


def _pytest_ini_options() -> dict[str, object]:
    document = tomllib.loads(_read_text(_CANDIDATE_ROOT / "pyproject.toml"))
    tool = document.get("tool")
    if not isinstance(tool, dict):
        return {}
    pytest_table = tool.get("pytest")
    if not isinstance(pytest_table, dict):
        return {}
    ini_options = pytest_table.get("ini_options")
    if not isinstance(ini_options, dict):
        return {}
    return ini_options


def _marker_names(markers: object) -> set[str]:
    raw_items: list[str] = []
    if isinstance(markers, str):
        raw_items = markers.splitlines()
    elif isinstance(markers, list):
        raw_items = [item for item in markers if isinstance(item, str)]
    names: set[str] = set()
    for item in raw_items:
        name = item.split(":", 1)[0].strip()
        if name:
            names.add(name)
    return names


def _addopts_tokens(addopts: object) -> list[str]:
    if isinstance(addopts, str):
        return shlex.split(addopts)
    if isinstance(addopts, list) and all(isinstance(item, str) for item in addopts):
        return list(addopts)
    return []


def _marker_expressions(tokens: list[str]) -> list[str]:
    expressions: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "-m" and index + 1 < len(tokens):
            expressions.append(" ".join(tokens[index + 1].split()))
            index += 2
            continue
        if token.startswith("-m") and token != "-m":
            expressions.append(" ".join(token[2:].split()))
        index += 1
    return expressions


def _external_witness_default_deselected(ini_options: dict[str, object]) -> bool:
    if "external_witness" not in _marker_names(ini_options.get("markers")):
        return False
    expressions = _marker_expressions(_addopts_tokens(ini_options.get("addopts")))
    return expressions == ["not external_witness"]


def _call_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _is_os_environ(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def _environ_key(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == "get" and _is_os_environ(node.func.value):
            if node.args and isinstance(node.args[0], ast.Constant):
                value = node.args[0].value
                if isinstance(value, str):
                    return value
    if isinstance(node, ast.Subscript) and _is_os_environ(node.value):
        slice_node = node.slice
        if isinstance(slice_node, ast.Constant) and isinstance(slice_node.value, str):
            return slice_node.value
    return None


def _environ_keys(tree: ast.AST) -> set[str]:
    keys: set[str] = set()
    for node in ast.walk(tree):
        key = _environ_key(node)
        if key is not None:
            keys.add(key)
    return keys


def _expr_is_external_witness(node: ast.expr) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    return (
        isinstance(target, ast.Attribute)
        and target.attr == "external_witness"
        and isinstance(target.value, ast.Attribute)
        and target.value.attr == "mark"
        and isinstance(target.value.value, ast.Name)
        and target.value.value.id == "pytest"
    )


def _assigned_mark(node: ast.AST) -> bool:
    if not isinstance(node, ast.Assign):
        return False
    if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets):
        return False
    values = node.value.elts if isinstance(node.value, (ast.List, ast.Tuple)) else [node.value]
    return any(isinstance(value, ast.expr) and _expr_is_external_witness(value) for value in values)


def _function_marked(function: ast.FunctionDef) -> bool:
    return any(_expr_is_external_witness(decorator) for decorator in function.decorator_list)


def _class_marked(class_node: ast.ClassDef) -> bool:
    if any(_expr_is_external_witness(decorator) for decorator in class_node.decorator_list):
        return True
    return any(_assigned_mark(statement) for statement in class_node.body)


def _module_functions(tree: ast.Module) -> list[ast.FunctionDef]:
    return [node for node in tree.body if isinstance(node, ast.FunctionDef)]


def _find_function(tree: ast.AST, name: str) -> ast.FunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def _find_class(tree: ast.Module, name: str) -> ast.ClassDef | None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def _class_methods(class_node: ast.ClassDef) -> list[ast.FunctionDef]:
    return [node for node in class_node.body if isinstance(node, ast.FunctionDef)]


def _substantive_body(function: ast.FunctionDef) -> list[ast.stmt]:
    body = list(function.body)
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[1:]
    return body


def _statement_calls(statement: ast.stmt) -> list[ast.Call]:
    calls: list[ast.Call] = []
    for node in ast.walk(statement):
        if isinstance(node, ast.Call):
            calls.append(node)
    return calls


def _first_call_name(function: ast.FunctionDef) -> str | None:
    for statement in _substantive_body(function):
        calls = _statement_calls(statement)
        if calls:
            return _call_name(calls[0])
    return None


def _body_skips_or_returns(body: list[ast.stmt]) -> bool:
    if len(body) != 1:
        return False
    statement = body[0]
    if isinstance(statement, ast.Return):
        return True
    if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
        return _call_name(statement.value) in _SKIP_CALLS
    return False


def _body_fails(body: list[ast.stmt]) -> bool:
    for statement in body:
        if isinstance(statement, ast.Raise):
            return True
        for call in _statement_calls(statement):
            if _call_name(call) in _FAIL_CALLS:
                return True
    return False


def _if_mentions_env(node: ast.If, env_name: str) -> bool:
    return any(_environ_key(child) == env_name for child in ast.walk(node.test))


def _leading_opt_in_skips(function: ast.FunctionDef, env_name: str) -> bool:
    body = _substantive_body(function)
    if not body or not isinstance(body[0], ast.If):
        return False
    gate = body[0]
    return _if_mentions_env(gate, env_name) and _body_skips_or_returns(gate.body)


def _opt_in_gate(tree: ast.AST, function: ast.FunctionDef, env_name: str) -> bool:
    if _leading_opt_in_skips(function, env_name):
        return True
    helper_name = _first_call_name(function)
    if helper_name is None:
        return False
    helper = _find_function(tree, helper_name)
    if helper is None:
        return False
    return _leading_opt_in_skips(helper, env_name)


def _assigned_environ_names(function: ast.FunctionDef) -> dict[str, str]:
    assigned: dict[str, str] = {}
    for statement in function.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name):
            continue
        key = _environ_key(statement.value)
        if key is not None:
            assigned[target.id] = key
    return assigned


def _if_references_names(test: ast.expr, names: set[str]) -> bool:
    return any(isinstance(node, ast.Name) and node.id in names for node in ast.walk(test))


def _missing_named_roots_skip(function: ast.FunctionDef, root_names: set[str]) -> bool:
    assigned = _assigned_environ_names(function)
    local_names = {name for name, key in assigned.items() if key in root_names}
    if {assigned[name] for name in local_names} != root_names:
        return False
    saw_skip = False
    for node in ast.walk(function):
        if not isinstance(node, ast.If) or not _if_references_names(node.test, local_names):
            continue
        if _body_fails(node.body):
            return False
        if _body_skips_or_returns(node.body):
            saw_skip = True
    return saw_skip


def _missing_roots_skip(tree: ast.AST, function: ast.FunctionDef, root_names: set[str]) -> bool:
    if _missing_named_roots_skip(function, root_names):
        return True
    helper_name = _first_call_name(function)
    if helper_name is None:
        return False
    helper = _find_function(tree, helper_name)
    if helper is None:
        return False
    return _missing_named_roots_skip(helper, root_names)


def _subscript_key(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Subscript):
        return None
    if not isinstance(node.value, ast.Name) or node.value.id != "env":
        return None
    if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
        return node.slice.value
    return None


def _strips_nested_pytest_controls(function: ast.FunctionDef) -> bool:
    cleared_addopts = False
    popped_coverage = False
    strips_cov_core = False
    for node in ast.walk(function):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    _subscript_key(target) == "PYTEST_ADDOPTS"
                    and isinstance(node.value, ast.Constant)
                    and node.value.value == ""
                ):
                    cleared_addopts = True
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "pop" and node.args:
                key = node.args[0]
                if isinstance(key, ast.Constant) and key.value == "PYTEST_ADDOPTS":
                    cleared_addopts = True
                if isinstance(key, ast.Constant) and key.value == "COVERAGE_PROCESS_START":
                    popped_coverage = True
            if (
                node.func.attr == "startswith"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "COV_CORE_"
            ):
                strips_cov_core = True
    return cleared_addopts and popped_coverage and strips_cov_core


def _calls_function(function: ast.FunctionDef, name: str) -> bool:
    return any(isinstance(node, ast.Call) and _call_name(node) == name for node in ast.walk(function))


def _test_calls_attr(test: ast.expr, name: str) -> bool:
    return any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == name
        for node in ast.walk(test)
    )


def _rejects_preexisting_root(functions: list[ast.FunctionDef]) -> bool:
    for function in functions:
        for node in ast.walk(function):
            if isinstance(node, ast.If) and _test_calls_attr(node.test, "exists") and _body_fails(node.body):
                return True
    return False


def _module_cleans_tmp_residue(tree: ast.AST) -> bool:
    has_tmp_name = False
    has_finally_unlink = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.endswith(".tmp"):
            has_tmp_name = True
        if isinstance(node, ast.Try):
            for statement in node.finalbody:
                if any(_call_name(call) in {"unlink", "remove"} for call in _statement_calls(statement)):
                    has_finally_unlink = True
    return has_tmp_name and has_finally_unlink


def _module_seals_failed_log(tree: ast.AST) -> bool:
    has_log_name = False
    has_readonly_mode = False
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and "traceability_witness.log" in node.value
        ):
            has_log_name = True
        if isinstance(node, ast.Constant) and node.value == 0o444:
            has_readonly_mode = True
    return has_log_name and has_readonly_mode


def _none_return_gate_key(function: ast.FunctionDef) -> str | None:
    assigned = _assigned_environ_names(function)
    for node in function.body:
        if not isinstance(node, ast.If) or not _body_skips_or_returns(node.body):
            continue
        test = node.test
        if not isinstance(test, ast.Compare) or len(test.ops) != 1 or not isinstance(test.ops[0], ast.Is):
            continue
        if len(test.comparators) != 1 or not isinstance(test.comparators[0], ast.Constant):
            continue
        if test.comparators[0].value is not None or not isinstance(test.left, ast.Name):
            continue
        key = assigned.get(test.left.id)
        if key is not None:
            return key
    return None


def _related_functions(tree: ast.AST, function: ast.FunctionDef) -> list[ast.FunctionDef]:
    related = [function]
    helper_name = _first_call_name(function)
    if helper_name is None:
        return related
    helper = _find_function(tree, helper_name)
    if helper is not None and helper is not function:
        related.append(helper)
    return related


def _p02_gaps(tree: ast.Module) -> list[str]:
    gaps: list[str] = []
    functions = _module_functions(tree)
    tests = [function for function in functions if function.name.startswith("test_")]
    witness = _find_function(tree, _P02_WITNESS)
    if witness is None or not _function_marked(witness):
        gaps.append("P02_WITNESS_NOT_MARKED_EXTERNAL_WITNESS")
    if any(function.name != _P02_WITNESS and _function_marked(function) for function in tests):
        gaps.append("P02_UNEXPECTED_EXTERNAL_WITNESS_MARK")
    if witness is None or not _opt_in_gate(tree, witness, _P02_OPT_IN):
        gaps.append("P02_OPT_IN_DOES_NOT_SKIP_WITHOUT_OUTER_EVIDENCE")
    environ_keys = _environ_keys(tree)
    if {_P02_EVIDENCE_ROOT, _P02_WORK_ROOT} - environ_keys:
        gaps.append("P02_EXPLICIT_ROOTS_MISSING")
    if _GENERIC_ROOT_ENV & environ_keys:
        gaps.append("P02_GENERIC_ROOTS_STILL_ACTIVE")
    if witness is None or not _missing_roots_skip(
        tree,
        witness,
        {_P02_EVIDENCE_ROOT, _P02_WORK_ROOT},
    ):
        gaps.append("P02_MISSING_ROOTS_FAIL_INSTEAD_OF_SKIP")
    nested = _find_function(tree, "_run_declared_command")
    if nested is None or not _strips_nested_pytest_controls(nested):
        gaps.append("P02_NESTED_PYTEST_INHERITS_OUTER_CONTROLS")
    return gaps


def _traceability_gaps(tree: ast.Module) -> list[str]:
    gaps: list[str] = []
    class_node = _find_class(tree, "TraceabilityIndexRegenerationTests")
    methods = _class_methods(class_node) if class_node is not None else []
    by_name = {method.name: method for method in methods}
    publisher = by_name.get(_TRACEABILITY_PUBLISHER)
    class_is_marked = class_node is not None and _class_marked(class_node)
    if publisher is None or class_is_marked or not _function_marked(publisher):
        gaps.append("TRACEABILITY_PUBLISHER_MISSING")
    hermetic_methods = [by_name.get(name) for name in _TRACEABILITY_HERMETIC_NODES]
    if any(method is None for method in hermetic_methods):
        gaps.append("TRACEABILITY_HERMETIC_NODE_MISSING")
    if class_is_marked or any(
        method is not None and _function_marked(method) for method in hermetic_methods
    ):
        gaps.append("TRACEABILITY_HERMETIC_NODE_MARKED_EXTERNAL_WITNESS")
    if any(
        method is not None and (_calls_function(method, "_publish") or _calls_function(method, "_work_directory"))
        for method in hermetic_methods
    ):
        gaps.append("TRACEABILITY_HERMETIC_NODE_PUBLISHES")
    if any(isinstance(node, ast.Call) and _call_name(node) == "mkdtemp" for node in ast.walk(tree)):
        gaps.append("GENERIC_SYSTEM_TEMP_OSS7_TRACEABILITY_FALLBACK")
    environ_keys = _environ_keys(tree)
    required_roots = {_TRACEABILITY_OPT_IN, _TRACEABILITY_EVIDENCE_ROOT, _TRACEABILITY_WORK_ROOT}
    if required_roots - environ_keys:
        gaps.append("TRACEABILITY_EXPLICIT_ROOTS_MISSING")
    if _GENERIC_ROOT_ENV & environ_keys:
        gaps.append("TRACEABILITY_GENERIC_ROOTS_STILL_ACTIVE")
    if publisher is not None and not class_is_marked and _function_marked(publisher):
        if not _opt_in_gate(tree, publisher, _TRACEABILITY_OPT_IN):
            gaps.append("TRACEABILITY_OPT_IN_DOES_NOT_SKIP_WITHOUT_OUTER_EVIDENCE")
        if not _missing_roots_skip(
            tree,
            publisher,
            {_TRACEABILITY_EVIDENCE_ROOT, _TRACEABILITY_WORK_ROOT},
        ):
            gaps.append("TRACEABILITY_MISSING_ROOTS_FAIL_INSTEAD_OF_SKIP")
        if not _rejects_preexisting_root(_related_functions(tree, publisher)):
            gaps.append("TRACEABILITY_PREEXISTING_ROOT_NOT_REJECTED")
    if not _module_cleans_tmp_residue(tree):
        gaps.append("TRACEABILITY_TMP_RESIDUE_CLEANUP_MISSING")
    if not _module_seals_failed_log(tree):
        gaps.append("TRACEABILITY_FAILED_LOG_SEAL_MISSING")
    return gaps


def _a5_gaps(tree: ast.Module) -> list[str]:
    gaps: list[str] = []
    function = _find_function(tree, _A5_NODE)
    if function is None:
        return ["A5_PRODUCT_PROOF_MISSING"]
    if _function_marked(function):
        gaps.append("A5_PRODUCT_PROOF_MARKED_EXTERNAL_WITNESS")
    environ_keys = _environ_keys(function)
    if _none_return_gate_key(function) != "A5_EVIDENCE_ROOT" or not _A5_REQUIRED_ENV <= environ_keys:
        gaps.append("A5_RECEIPT_TAIL_USES_GENERIC_READINESS_ROOTS")
    if _A5_FORBIDDEN_ENV & environ_keys:
        gaps.append("A5_GENERIC_READINESS_VARIABLES_STILL_ACTIVE")
    return gaps


def _external_witness_contract_gaps() -> list[str]:
    return [
        *_p02_gaps(_parsed(_P02_PATH)),
        *_traceability_gaps(_parsed(_TRACEABILITY_PATH)),
        *_a5_gaps(_parsed(_A5_PATH)),
    ]


def _assert_historical_p02_pins() -> None:
    found: dict[str, str] = {}
    for node in _parsed(_P02_PATH).body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id not in _P02_PINS:
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            found[target.id] = node.value.value
    if found != _P02_PINS:
        pytest.fail("P02_HISTORICAL_PIN_DRIFT")


def _assert_cleanup_contract_bounds() -> None:
    assert _CLEANUP_CONTRACT_BOUND is True
    assert _RESIDUE_DIRECTORY_NAMES == frozenset({"__pycache__", ".pytest_cache"})
    assert _RESIDUE_FILE_NAMES == frozenset({".coverage"})
    assert residue_path_is_cleanup_eligible(_CANDIDATE_ROOT, _CANDIDATE_ROOT / ".coverage")
    assert residue_path_is_cleanup_eligible(
        _CANDIDATE_ROOT,
        _CANDIDATE_ROOT / ".pytest_cache" / "CACHEDIR.TAG",
    )
    assert residue_path_is_cleanup_eligible(
        _CANDIDATE_ROOT,
        _CANDIDATE_ROOT / "tests" / "__pycache__" / "x.pyc",
    )
    assert not residue_path_is_cleanup_eligible(_CANDIDATE_ROOT, _CANDIDATE_ROOT / "pyproject.toml")
    assert not residue_path_is_cleanup_eligible(
        _CANDIDATE_ROOT,
        _CANDIDATE_ROOT / "src" / "task_transition_runner" / "cli.py",
    )
    assert not residue_path_is_cleanup_eligible(_CANDIDATE_ROOT, Path("/tmp/.coverage"))


def test_default_suite_requires_structural_external_witness_boundary() -> None:
    ini_options = _pytest_ini_options()
    if ini_options.get("testpaths") != ["tests"]:
        pytest.fail("PYTEST_TESTPATHS_DRIFT")
    if not _external_witness_default_deselected(ini_options):
        pytest.fail("MISSING_EXTERNAL_WITNESS_DEFAULT_DESELECTION")


def test_external_witnesses_require_explicit_task_roots_and_isolated_child_environment() -> None:
    _assert_historical_p02_pins()
    _assert_cleanup_contract_bounds()
    gaps = _external_witness_contract_gaps()
    if gaps:
        pytest.fail("EXTERNAL_WITNESS_EXPLICIT_ROOT_CONTRACT_OPEN: " + ",".join(gaps))


def _boundary_text(*parts: str) -> str:
    return "".join(parts)


def _boundary_has_task_evidence_token(text: str) -> bool:
    marker = _boundary_text("task")
    start = 0
    while True:
        found = text.find(marker, start)
        if found < 0:
            return False
        cursor = found + len(marker)
        if cursor < len(text) and text[cursor].isdigit():
            while cursor < len(text) and text[cursor].isdigit():
                cursor += 1
            if cursor < len(text) and text[cursor] == "-":
                return True
        start = found + len(marker)


def _boundary_has_platform_drive_or_unc(text: str) -> bool:
    windows_slash = chr(92)
    separators = "/" + windows_slash
    for index, char in enumerate(text):
        if not char.isalpha() or index + 2 >= len(text):
            continue
        if text[index + 1] != ":" or text[index + 2] not in separators:
            continue
        if index > 0 and (text[index - 1].isalnum() or text[index - 1] == "_"):
            continue
        return True
    return windows_slash + windows_slash in text


def _boundary_private_literal_coupling(text: str) -> bool:
    posix_homes = (
        _boundary_text("/", "home", "/"),
        _boundary_text("/", "Users", "/"),
        _boundary_text("/", "root", "/"),
    )
    if any(prefix in text for prefix in posix_homes):
        return True
    if _boundary_has_platform_drive_or_unc(text):
        return True
    if _boundary_text("Cursor", "_Projects") in text:
        return True
    if _boundary_text(".", "oss7", "-evidence") in text:
        return True
    if _boundary_text("readiness", "-") in text:
        return True
    return _boundary_has_task_evidence_token(text)


def _boundary_constant_text(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _boundary_path_call(node: ast.AST) -> ast.Call | None:
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Path":
        return node
    return None


def _boundary_external_path_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for statement in tree.body:
        value: ast.expr | None = None
        targets: list[ast.expr] = []
        if isinstance(statement, ast.Assign):
            value = statement.value
            targets = list(statement.targets)
        elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
            value = statement.value
            targets = [statement.target]
        call = _boundary_path_call(value) if value is not None else None
        if call is None or not call.args:
            continue
        literal = _boundary_constant_text(call.args[0])
        if literal is None or not _boundary_private_literal_coupling(literal):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                names.add(target.id)
    return names


def _boundary_read_attributes() -> frozenset[str]:
    return frozenset(
        {
            _boundary_text("read", "_bytes"),
            _boundary_text("read", "_text"),
            _boundary_text("write", "_bytes"),
            _boundary_text("write", "_text"),
            _boundary_text("is", "_file"),
            _boundary_text("stat"),
            _boundary_text("resolve"),
            _boundary_text("open"),
        }
    )


def _boundary_reads_external_receipt(tree: ast.Module) -> bool:
    names = _boundary_external_path_names(tree)
    read_attributes = _boundary_read_attributes()
    open_name = _boundary_text("open")
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == open_name and node.args:
            literal = _boundary_constant_text(node.args[0])
            if literal is not None and _boundary_private_literal_coupling(literal):
                return True
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr not in read_attributes:
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Name) and receiver.id in names:
            return True
        call = _boundary_path_call(receiver)
        if call is None or not call.args:
            continue
        literal = _boundary_constant_text(call.args[0])
        if literal is not None and _boundary_private_literal_coupling(literal):
            return True
    return False


def _boundary_governance_tokens() -> frozenset[str]:
    return frozenset(
        {
            _boundary_text("g", "it"),
            _boundary_text("rev", "-parse"),
            _boundary_text("--abbrev", "-ref"),
            _boundary_text("HE", "AD"),
            _boundary_text("for-each", "-ref"),
            _boundary_text("show", "-ref"),
            _boundary_text("refs", "/", "heads"),
            _boundary_text("work", "tree"),
            _boundary_text("--", "porcelain"),
        }
    )


def _boundary_constant_is_live_governance(value: str) -> bool:
    for token in _boundary_governance_tokens():
        if value == token or value.startswith(token):
            return True
    return False


def _boundary_names_process_module(tree: ast.AST) -> bool:
    module_name = _boundary_text("sub", "process")
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(alias.name == module_name for alias in node.names):
            return True
        if isinstance(node, ast.ImportFrom) and node.module == module_name:
            return True
        if isinstance(node, ast.Name) and node.id == module_name:
            return True
    return False


def _boundary_enforces_live_governance(tree: ast.AST) -> bool:
    if _boundary_names_process_module(tree):
        return True
    for node in ast.walk(tree):
        literal = _boundary_constant_text(node)
        if literal is not None and _boundary_constant_is_live_governance(literal):
            return True
    return False


def _public_boundary_is_coupled(source: str, tree: ast.Module) -> bool:
    """Koppelt nur Host-, Evidence- und Live-Steuerung, nicht candidate-lokale Pin-Konstanten."""
    return (
        _boundary_private_literal_coupling(source)
        or _boundary_reads_external_receipt(tree)
        or _boundary_enforces_live_governance(tree)
    )


def test_public_boundary_contains_no_private_host_or_governance_evidence_coupling() -> None:
    """Liest nur diese Quelldatei und lehnt private Host-, Evidence- und Live-Steuerung ab."""
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    if _public_boundary_is_coupled(source, tree):
        pytest.fail("PUBLIC_BOUNDARY_CONTAINS_PRIVATE_OR_GOVERNANCE_COUPLING")
