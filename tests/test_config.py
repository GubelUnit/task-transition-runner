"""Synthetic public runner-configuration contract.

The public surface is intentionally small and explicit:

- ``load_config`` reads one YAML file and fail-closed validates it.
- ``RunnerConfig`` is the loaded canonical value object.
- ``ConfigError.reason_code`` is the stable ``CONFIG_*`` failure token.
- ``runner_source`` is the resolved directory of this implementation module.
  Target worktree and runtime root must stay outside it. The loader does
  not search parent trees and does not inject host path defaults.
"""

from __future__ import annotations

import copy
import inspect
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

from task_transition_runner.config import ConfigError, RunnerConfig, load_config

_PROMPT_ROLES = ("developer", "orchestrator", "validator")
_REFERENCE_SCHEMA_VERSION = "1.0"
_REFERENCE_MAIN_BRANCH = "main"
_REFERENCE_MAIN_REF = "refs/heads/main"


def _reference_payload() -> dict[str, Any]:
    return {
        "schema_version": _REFERENCE_SCHEMA_VERSION,
        "target": {
            "repo_root": "./fixture-repo",
            "main_branch": _REFERENCE_MAIN_BRANCH,
        },
        "runtime_root": "./runner-runtime",
        "task_source": "./tasks.yaml",
        "baseline_validation": {
            "command": "python -m pytest -q",
            "timeout_seconds": 300,
        },
        "adapter": {
            "kind": "cursor",
            "executable": "cursor-agent",
            "model": None,
            "timeout_seconds": 1800,
        },
        "prompts": {
            "developer": "./prompts/developer.md",
            "orchestrator": "./prompts/orchestrator.md",
            "validator": "./prompts/validator.md",
        },
    }


def _write_reference_tree(
    root: Path,
    payload: dict[str, Any] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "fixture-repo").mkdir(parents=True, exist_ok=True)
    (root / "runner-runtime").mkdir(parents=True, exist_ok=True)
    (root / "prompts").mkdir(parents=True, exist_ok=True)
    (root / "tasks.yaml").write_text("synthetic: true\n", encoding="utf-8")
    for role in _PROMPT_ROLES:
        (root / "prompts" / f"{role}.md").write_text(
            f"synthetic {role} prompt\n",
            encoding="utf-8",
        )
    document = _reference_payload() if payload is None else copy.deepcopy(payload)
    config_path = root / "runner.yaml"
    config_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    return config_path


def _overwrite_config(config_path: Path, text: str) -> Path:
    config_path.write_text(text, encoding="utf-8")
    return config_path


def _drop_field(payload: dict[str, Any], dotted: str) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    keys = dotted.split(".")
    cursor: Any = clone
    for key in keys[:-1]:
        cursor = cursor[key]
    del cursor[keys[-1]]
    return clone


def _set_field(payload: dict[str, Any], dotted: str, value: Any) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    keys = dotted.split(".")
    cursor: Any = clone
    for key in keys[:-1]:
        cursor = cursor[key]
    cursor[keys[-1]] = value
    return clone


def _expect_reason(config_path: Path, reason_code: str) -> ConfigError:
    with pytest.raises(ConfigError, match=reason_code) as exc_info:
        load_config(config_path)
    assert exc_info.value.reason_code == reason_code
    return exc_info.value


def _runner_source() -> Path:
    return Path(inspect.getfile(load_config)).resolve().parent


def _composed_absolute(*segments: str) -> str:
    return "/" + "/".join(segments)


def _make_unreadable(path: Path) -> None:
    path.chmod(stat.S_IWUSR)
    if os.access(path, os.R_OK):
        pytest.skip(
            "process retains read access after clearing read mode bits"
        )


def test_reference_configuration_exposes_canonical_roots_and_main_ref(
    tmp_path: Path,
) -> None:
    config_path = _write_reference_tree(tmp_path)
    loaded = load_config(config_path)

    config_root = config_path.resolve().parent
    task_source = (config_root / "tasks.yaml").resolve()
    target_worktree = (config_root / "fixture-repo").resolve()
    runtime_root = (config_root / "runner-runtime").resolve()

    assert isinstance(loaded, RunnerConfig)
    assert loaded.schema_version == _REFERENCE_SCHEMA_VERSION
    assert loaded.config_root == config_root
    assert loaded.task_source == task_source
    assert loaded.task_source_root == task_source.parent
    assert loaded.target.repo_root == target_worktree
    assert loaded.target_worktree == target_worktree
    assert loaded.target.main_branch == _REFERENCE_MAIN_BRANCH
    assert loaded.main_ref == _REFERENCE_MAIN_REF
    assert loaded.main_ref != loaded.target.main_branch
    assert loaded.runtime_root == runtime_root
    assert loaded.runner_source == _runner_source()
    assert loaded.baseline_validation.command == "python -m pytest -q"
    assert loaded.baseline_validation.timeout_seconds == 300
    assert loaded.adapter.kind == "cursor"
    assert loaded.adapter.executable == "cursor-agent"
    assert loaded.adapter.model is None
    assert loaded.adapter.timeout_seconds == 1800
    assert loaded.prompts.developer == (config_root / "prompts" / "developer.md").resolve()
    assert loaded.prompts.orchestrator == (
        config_root / "prompts" / "orchestrator.md"
    ).resolve()
    assert loaded.prompts.validator == (config_root / "prompts" / "validator.md").resolve()
    assert loaded.prompts.developer.is_relative_to(loaded.config_root)
    assert loaded.prompts.orchestrator.is_relative_to(loaded.config_root)
    assert loaded.prompts.validator.is_relative_to(loaded.config_root)
    assert loaded.target_worktree != loaded.runner_source
    assert not loaded.target_worktree.is_relative_to(loaded.runner_source)
    assert loaded.runtime_root != loaded.runner_source
    assert not loaded.runtime_root.is_relative_to(loaded.runner_source)
    assert not loaded.runtime_root.is_relative_to(loaded.target_worktree)


def test_config_root_follows_symlink_resolved_config_file(tmp_path: Path) -> None:
    real_root = tmp_path / "real-config"
    real_config = _write_reference_tree(real_root)
    link_dir = tmp_path / "link-dir"
    link_dir.mkdir()
    linked_config = link_dir / "runner.yaml"
    linked_config.symlink_to(real_config)

    loaded = load_config(linked_config)

    assert loaded.config_root == real_root.resolve()
    assert loaded.target_worktree == (real_root / "fixture-repo").resolve()
    assert loaded.runtime_root == (real_root / "runner-runtime").resolve()


def test_main_ref_is_derived_from_the_configured_branch_name(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "target.main_branch", "release")
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.target.main_branch == "release"
    assert loaded.main_ref == "refs/heads/release"
    assert loaded.main_ref != loaded.target.main_branch


def test_main_ref_preserves_a_slash_in_the_branch_name(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "target.main_branch", "feature/foo")
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.target.main_branch == "feature/foo"
    assert loaded.main_ref == "refs/heads/feature/foo"


def test_prompt_symlink_inside_config_root_is_accepted(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    developer = tmp_path / "prompts" / "developer.md"
    canonical = tmp_path / "prompts" / "canonical-developer.md"
    canonical.write_text("canonical developer prompt\n", encoding="utf-8")
    developer.unlink()
    developer.symlink_to(canonical)

    loaded = load_config(config_path)

    assert loaded.prompts.developer == canonical.resolve()
    assert loaded.prompts.developer.is_relative_to(loaded.config_root)


def test_declared_prompt_refs_are_unnormalized_yaml_scalars(tmp_path: Path) -> None:
    payload = _reference_payload()
    payload["prompts"]["developer"] = "prompts/developer.md"
    payload["prompts"]["orchestrator"] = "./prompts/orchestrator.md"
    payload["prompts"]["validator"] = "prompts/validator.md"
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.prompts.declared_developer == "prompts/developer.md"
    assert loaded.prompts.declared_orchestrator == "./prompts/orchestrator.md"
    assert loaded.prompts.declared_validator == "prompts/validator.md"
    assert isinstance(loaded.prompts.declared_developer, str)
    assert loaded.prompts.developer == (tmp_path / "prompts" / "developer.md").resolve()
    assert loaded.prompts.declared_developer != str(loaded.prompts.developer)


def test_declared_prompt_refs_stay_distinct_from_symlink_resolved_paths(
    tmp_path: Path,
) -> None:
    config_path = _write_reference_tree(tmp_path)
    developer = tmp_path / "prompts" / "developer.md"
    canonical = tmp_path / "prompts" / "canonical-developer.md"
    canonical.write_text("canonical developer prompt\n", encoding="utf-8")
    developer.unlink()
    developer.symlink_to(canonical)

    loaded = load_config(config_path)

    assert loaded.prompts.declared_developer == "./prompts/developer.md"
    assert loaded.prompts.developer == canonical.resolve()
    assert loaded.prompts.declared_developer != str(loaded.prompts.developer)


def test_foreign_yaml_and_config_suffix_do_not_change_declared_prompt_refs(
    tmp_path: Path,
) -> None:
    yaml_config = _write_reference_tree(tmp_path)
    (tmp_path / "notes.yaml").write_text(
        "prompts:\n"
        "  developer: ./prompts/validator.md\n"
        "  orchestrator: ./prompts/developer.md\n"
        "  validator: ./prompts/orchestrator.md\n",
        encoding="utf-8",
    )
    (tmp_path / "extra.yml").write_text(
        "prompts:\n  developer: ./prompts/orchestrator.md\n",
        encoding="utf-8",
    )
    unsuffixed = tmp_path / "runner"
    unsuffixed.write_bytes(yaml_config.read_bytes())

    loaded = load_config(unsuffixed)

    assert loaded.prompts.declared_developer == "./prompts/developer.md"
    assert loaded.prompts.declared_orchestrator == "./prompts/orchestrator.md"
    assert loaded.prompts.declared_validator == "./prompts/validator.md"
    assert loaded.prompts.developer == (tmp_path / "prompts" / "developer.md").resolve()


def test_nested_task_source_root_is_the_resolved_file_directory(tmp_path: Path) -> None:
    nested = tmp_path / "tasks" / "source.yaml"
    nested.parent.mkdir()
    nested.write_text("synthetic: true\n", encoding="utf-8")
    payload = _set_field(_reference_payload(), "task_source", "./tasks/source.yaml")
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.task_source == nested.resolve()
    assert loaded.task_source_root == nested.resolve().parent
    assert loaded.task_source_root != loaded.config_root


def test_task_source_root_follows_symlink_resolved_file(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    real_dir = tmp_path / "real-source"
    real_dir.mkdir()
    real_file = real_dir / "source.yaml"
    real_file.write_text("synthetic: true\n", encoding="utf-8")
    declared = tmp_path / "tasks.yaml"
    declared.unlink()
    declared.symlink_to(real_file)

    loaded = load_config(config_path)

    assert loaded.task_source == real_file.resolve()
    assert loaded.task_source_root == real_dir.resolve()


def test_omitted_adapter_model_normalizes_to_none(tmp_path: Path) -> None:
    payload = _drop_field(_reference_payload(), "adapter.model")
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.adapter.model is None


def test_configured_adapter_model_string_is_accepted(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "adapter.model", "synthetic-model")
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.adapter.model == "synthetic-model"


def test_path_basename_executable_is_stored_without_existence_check(
    tmp_path: Path,
) -> None:
    payload = _set_field(_reference_payload(), "adapter.executable", "cursor-agent")
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.adapter.executable == "cursor-agent"
    assert "/" not in loaded.adapter.executable


def test_explicit_absolute_executable_is_stored_without_existence_check(
    tmp_path: Path,
) -> None:
    explicit = str((tmp_path / "missing-cursor-agent").resolve())
    payload = _set_field(_reference_payload(), "adapter.executable", explicit)
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.adapter.executable == explicit
    assert not Path(explicit).exists()


def test_absolute_target_and_runtime_paths_are_accepted(tmp_path: Path) -> None:
    target = tmp_path / "abs-target"
    runtime = tmp_path / "abs-runtime"
    target.mkdir()
    runtime.mkdir()
    payload = _reference_payload()
    payload["target"]["repo_root"] = str(target)
    payload["runtime_root"] = str(runtime)
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.target_worktree == target.resolve()
    assert loaded.runtime_root == runtime.resolve()
    assert not loaded.runtime_root.is_relative_to(loaded.target_worktree)
    assert not loaded.target_worktree.is_relative_to(loaded.runner_source)


def test_composed_home_class_absolute_target_is_accepted(tmp_path: Path) -> None:
    target = _composed_absolute("home", "synthetic-user", "fixture-repo")
    runtime = _composed_absolute("home", "synthetic-user", "runner-runtime")
    payload = _reference_payload()
    payload["target"]["repo_root"] = target
    payload["runtime_root"] = runtime
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.target_worktree == Path(target).resolve()
    assert loaded.runtime_root == Path(runtime).resolve()


def test_absolute_prompt_inside_config_root_is_accepted(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    prompt = (tmp_path / "prompts" / "developer.md").resolve()
    payload = _set_field(_reference_payload(), "prompts.developer", str(prompt))
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.prompts.developer == prompt
    assert loaded.prompts.developer.is_relative_to(loaded.config_root)


def test_absolute_prompt_declaration_is_preserved_as_yaml_scalar(
    tmp_path: Path,
) -> None:
    prompt = (tmp_path / "prompts" / "developer.md").resolve()
    declared = str(prompt)
    payload = _set_field(_reference_payload(), "prompts.developer", declared)
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.prompts.declared_developer == declared
    assert isinstance(loaded.prompts.declared_developer, str)
    assert loaded.prompts.developer == prompt


def test_absolute_task_source_file_is_accepted(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    task_source = (tmp_path / "tasks.yaml").resolve()
    payload = _set_field(_reference_payload(), "task_source", str(task_source))
    config_path = _write_reference_tree(tmp_path, payload)

    loaded = load_config(config_path)

    assert loaded.task_source == task_source
    assert loaded.task_source_root == tmp_path.resolve()


@pytest.mark.parametrize(
    "dotted_field",
    (
        "schema_version",
        "target",
        "target.repo_root",
        "target.main_branch",
        "runtime_root",
        "task_source",
        "baseline_validation",
        "baseline_validation.command",
        "baseline_validation.timeout_seconds",
        "adapter",
        "adapter.kind",
        "adapter.executable",
        "adapter.timeout_seconds",
        "prompts",
        "prompts.developer",
        "prompts.orchestrator",
        "prompts.validator",
    ),
)
def test_missing_required_field_is_blocked(tmp_path: Path, dotted_field: str) -> None:
    config_path = _write_reference_tree(tmp_path, _drop_field(_reference_payload(), dotted_field))
    _expect_reason(config_path, "CONFIG_FIELD_MISSING")


def test_empty_mapping_document_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path, {})
    _expect_reason(config_path, "CONFIG_FIELD_MISSING")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("unexpected", True),
        ("ui_bridge", {"enabled": True}),
        ("main_ref", _REFERENCE_MAIN_REF),
        ("target.extra", "ignored"),
        ("baseline_validation.cwd", "."),
        ("adapter.provider", "generic"),
        ("prompts.reviewer", "./prompts/reviewer.md"),
    ),
)
def test_unknown_field_is_blocked(tmp_path: Path, dotted_field: str, value: Any) -> None:
    config_path = _write_reference_tree(
        tmp_path,
        _set_field(_reference_payload(), dotted_field, value),
    )
    _expect_reason(config_path, "CONFIG_FIELD_UNKNOWN")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("schema_version", 1.0),
        ("schema_version", 1),
        ("target", "./fixture-repo"),
        ("target.repo_root", ["./fixture-repo"]),
        ("target.main_branch", ["main"]),
        ("runtime_root", {"path": "./runner-runtime"}),
        ("task_source", ["./tasks.yaml"]),
        ("baseline_validation.command", ["python", "-m", "pytest"]),
        ("baseline_validation.timeout_seconds", "300"),
        ("baseline_validation.timeout_seconds", True),
        ("baseline_validation.timeout_seconds", False),
        ("adapter.kind", 1),
        ("adapter.executable", ["cursor-agent"]),
        ("adapter.model", 12),
        ("adapter.timeout_seconds", "1800"),
        ("adapter.timeout_seconds", True),
        ("prompts", ["./prompts/developer.md"]),
        ("prompts.developer", {"path": "./prompts/developer.md"}),
    ),
)
def test_wrong_field_type_is_blocked(tmp_path: Path, dotted_field: str, value: Any) -> None:
    config_path = _write_reference_tree(
        tmp_path,
        _set_field(_reference_payload(), dotted_field, value),
    )
    _expect_reason(config_path, "CONFIG_FIELD_TYPE")


@pytest.mark.parametrize(
    "document",
    (
        "",
        "\n",
        "- just a list\n",
        "not a mapping\n",
        "schema_version: [unterminated\n",
    ),
)
def test_empty_non_mapping_and_syntax_errors_are_blocked(
    tmp_path: Path,
    document: str,
) -> None:
    config_path = _write_reference_tree(tmp_path)
    _overwrite_config(config_path, document)
    _expect_reason(config_path, "CONFIG_FIELD_TYPE")


def test_duplicate_yaml_keys_are_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    text = config_path.read_text(encoding="utf-8")
    _overwrite_config(config_path, 'schema_version: "1.0"\n' + text)
    _expect_reason(config_path, "CONFIG_VALUE_CONFLICT")


@pytest.mark.parametrize(
    ("dotted_field", "value"),
    (
        ("schema_version", "2.0"),
        ("schema_version", "1.0.0"),
        ("target.main_branch", "refs/heads/main"),
        ("target.main_branch", ""),
        ("target.main_branch", "HEAD"),
        ("target.main_branch", "main branch"),
        ("target.main_branch", ".."),
        ("target.main_branch", "main~1"),
        ("target.main_branch", "*"),
        ("target.main_branch", "-main"),
        ("target.main_branch", "feature//foo"),
        ("target.main_branch", "foo..bar"),
        ("target.main_branch", "foo/.hidden"),
        ("target.main_branch", "foo.lock"),
        ("target.repo_root", ""),
        ("runtime_root", ""),
        ("task_source", ""),
        ("adapter.kind", "other"),
        ("adapter.executable", ""),
        ("adapter.executable", "./cursor-agent"),
        ("adapter.executable", "bin/cursor-agent"),
        ("prompts.developer", ""),
        ("baseline_validation.timeout_seconds", 0),
        ("baseline_validation.timeout_seconds", -1),
        ("adapter.timeout_seconds", 0),
        ("adapter.timeout_seconds", -1),
    ),
)
def test_conflicting_values_are_blocked(tmp_path: Path, dotted_field: str, value: Any) -> None:
    config_path = _write_reference_tree(
        tmp_path,
        _set_field(_reference_payload(), dotted_field, value),
    )
    _expect_reason(config_path, "CONFIG_VALUE_CONFLICT")


@pytest.mark.parametrize(
    "dotted_field",
    (
        "target.repo_root",
        "runtime_root",
        "task_source",
        "prompts.developer",
        "prompts.orchestrator",
        "prompts.validator",
    ),
)
def test_parent_traversal_is_blocked(tmp_path: Path, dotted_field: str) -> None:
    config_path = _write_reference_tree(
        tmp_path,
        _set_field(_reference_payload(), dotted_field, "../outside"),
    )
    _expect_reason(config_path, "CONFIG_PATH_TRAVERSAL")


def test_normalized_parent_traversal_inside_config_root_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(
        tmp_path,
        _set_field(
            _reference_payload(),
            "prompts.developer",
            "./prompts/../prompts/developer.md",
        ),
    )
    _expect_reason(config_path, "CONFIG_PATH_TRAVERSAL")


def test_prompt_symlink_escape_is_blocked(tmp_path: Path) -> None:
    enclosed = tmp_path / "enclosed"
    config_path = _write_reference_tree(enclosed)
    leaked = tmp_path / "outside-prompts" / "leaked.md"
    leaked.parent.mkdir()
    leaked.write_text("escaped prompt\n", encoding="utf-8")
    prompt = enclosed / "prompts" / "developer.md"
    prompt.unlink()
    prompt.symlink_to(leaked)
    _expect_reason(config_path, "CONFIG_PATH_ESCAPE")


def test_absolute_prompt_outside_config_root_is_blocked(tmp_path: Path) -> None:
    enclosed = tmp_path / "enclosed"
    leaked = tmp_path / "outside-prompts" / "leaked.md"
    leaked.parent.mkdir()
    leaked.write_text("escaped prompt\n", encoding="utf-8")
    payload = _set_field(_reference_payload(), "prompts.developer", str(leaked.resolve()))
    config_path = _write_reference_tree(enclosed, payload)
    _expect_reason(config_path, "CONFIG_PATH_ESCAPE")


def test_runtime_inside_target_worktree_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "runtime_root",
        "./fixture-repo/nested-runtime",
    )
    config_path = _write_reference_tree(tmp_path, payload)
    (tmp_path / "fixture-repo" / "nested-runtime").mkdir()
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_target_inside_runtime_root_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "target.repo_root",
        "./runner-runtime/nested-target",
    )
    config_path = _write_reference_tree(tmp_path, payload)
    (tmp_path / "runner-runtime" / "nested-target").mkdir()
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_runtime_equal_to_target_worktree_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "runtime_root", "./fixture-repo")
    config_path = _write_reference_tree(tmp_path, payload)
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_filesystem_root_runtime_overlaps_target(tmp_path: Path) -> None:
    payload = _set_field(_reference_payload(), "runtime_root", os.sep)
    config_path = _write_reference_tree(tmp_path, payload)
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_target_worktree_overlapping_runner_source_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    target = tmp_path / "fixture-repo"
    target.rmdir()
    target.symlink_to(_runner_source())
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_runtime_overlapping_runner_source_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    runtime = tmp_path / "runner-runtime"
    runtime.rmdir()
    runtime.symlink_to(_runner_source())
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_target_as_parent_of_runner_source_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "target.repo_root",
        str(_runner_source().parent),
    )
    config_path = _write_reference_tree(tmp_path, payload)
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


def test_runtime_as_parent_of_runner_source_is_blocked(tmp_path: Path) -> None:
    payload = _set_field(
        _reference_payload(),
        "runtime_root",
        str(_runner_source().parent),
    )
    config_path = _write_reference_tree(tmp_path, payload)
    _expect_reason(config_path, "CONFIG_ROOT_OVERLAP")


@pytest.mark.parametrize("role", _PROMPT_ROLES)
def test_missing_prompt_source_is_blocked(tmp_path: Path, role: str) -> None:
    config_path = _write_reference_tree(tmp_path)
    (tmp_path / "prompts" / f"{role}.md").unlink()
    _expect_reason(config_path, "CONFIG_SOURCE_MISSING")


def test_prompt_source_directory_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    prompt = tmp_path / "prompts" / "developer.md"
    prompt.unlink()
    prompt.mkdir()
    _expect_reason(config_path, "CONFIG_SOURCE_MISSING")


def test_unreadable_prompt_source_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    prompt = tmp_path / "prompts" / "validator.md"
    original_mode = prompt.stat().st_mode
    try:
        _make_unreadable(prompt)
        _expect_reason(config_path, "CONFIG_SOURCE_UNREADABLE")
    finally:
        prompt.chmod(original_mode)


def test_missing_task_source_file_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    (tmp_path / "tasks.yaml").unlink()
    _expect_reason(config_path, "CONFIG_SOURCE_MISSING")


def test_task_source_directory_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    task_source = tmp_path / "tasks.yaml"
    task_source.unlink()
    task_source.mkdir()
    _expect_reason(config_path, "CONFIG_SOURCE_MISSING")


def test_unreadable_task_source_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    task_source = tmp_path / "tasks.yaml"
    original_mode = task_source.stat().st_mode
    try:
        _make_unreadable(task_source)
        _expect_reason(config_path, "CONFIG_SOURCE_UNREADABLE")
    finally:
        task_source.chmod(original_mode)


def test_broken_task_source_symlink_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    task_source = tmp_path / "tasks.yaml"
    task_source.unlink()
    task_source.symlink_to(tmp_path / "missing-task-source.yaml")
    _expect_reason(config_path, "CONFIG_SOURCE_MISSING")


def test_missing_config_file_is_blocked(tmp_path: Path) -> None:
    _expect_reason(tmp_path / "runner.yaml", "CONFIG_SOURCE_MISSING")


def test_config_path_directory_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    config_path.unlink()
    config_path.mkdir()
    _expect_reason(config_path, "CONFIG_SOURCE_MISSING")


def test_unreadable_config_file_is_blocked(tmp_path: Path) -> None:
    config_path = _write_reference_tree(tmp_path)
    original_mode = config_path.stat().st_mode
    try:
        _make_unreadable(config_path)
        _expect_reason(config_path, "CONFIG_SOURCE_UNREADABLE")
    finally:
        if config_path.is_file():
            config_path.chmod(original_mode)
