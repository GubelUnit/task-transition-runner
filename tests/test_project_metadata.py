"""Synthetic project-metadata and dependency-boundary contract for the candidate skeleton."""

from __future__ import annotations

import tomllib
import unittest
from pathlib import Path
from typing import Any


_CANDIDATE_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT_PATH = _CANDIDATE_ROOT / "pyproject.toml"
_NEUTRAL_PROJECT_NAMES = frozenset(
    {"task-transition-runner", "task_transition_runner"}
)
_REQUIRED_DEV_DEPENDENCIES = ("pytest", "pytest-cov")
_REQUIRED_RUNTIME_DEPENDENCIES = ("pyyaml",)
_FORBIDDEN_DEPENDENCIES = (
    "yaml",
    "fastapi",
    "uvicorn",
    "starlette",
    "pydantic",
    "typer",
    "click",
    "hypothesis",
    "filelock",
    "pyautogui",
)


def _load_pyproject() -> dict[str, Any]:
    with _PYPROJECT_PATH.open("rb") as handle:
        loaded = tomllib.load(handle)
    if not isinstance(loaded, dict):
        raise AssertionError("pyproject.toml must parse as a TOML table")
    return loaded


def _normalized_name(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _project_name(data: dict[str, Any]) -> str:
    project = data.get("project")
    if isinstance(project, dict) and isinstance(project.get("name"), str):
        return project["name"]
    poetry = (data.get("tool") or {}).get("poetry")
    if isinstance(poetry, dict) and isinstance(poetry.get("name"), str):
        return poetry["name"]
    raise AssertionError("pyproject.toml must declare a project name")


def _python_requirement(data: dict[str, Any]) -> str:
    project = data.get("project")
    if isinstance(project, dict) and isinstance(project.get("requires-python"), str):
        return project["requires-python"]
    poetry = (data.get("tool") or {}).get("poetry")
    if isinstance(poetry, dict):
        dependencies = poetry.get("dependencies")
        if isinstance(dependencies, dict):
            python_spec = dependencies.get("python")
            if isinstance(python_spec, str):
                return python_spec
    raise AssertionError("pyproject.toml must declare a Python version requirement")


def _requirement_name(requirement: str) -> str:
    token = requirement.strip().strip("\"'")
    for separator in ("[", " ", ">", "<", "=", "!", "~", ";"):
        token = token.split(separator, 1)[0]
    return token


def _add_requirement_names(names: list[str], value: Any) -> None:
    if isinstance(value, str):
        extracted = _requirement_name(value)
        if extracted:
            names.append(extracted)
        return
    if isinstance(value, dict):
        version = value.get("version")
        if isinstance(version, str):
            _add_requirement_names(names, version)


def _normalized_names(names: list[str]) -> set[str]:
    return {
        _normalized_name(name)
        for name in names
        if name and _normalized_name(name) != "python"
    }


def _iter_runtime_dependency_names(data: dict[str, Any]) -> list[str]:
    names: list[str] = []

    project = data.get("project")
    if isinstance(project, dict):
        dependencies = project.get("dependencies")
        if isinstance(dependencies, list):
            for item in dependencies:
                _add_requirement_names(names, item)

    poetry = (data.get("tool") or {}).get("poetry")
    if isinstance(poetry, dict):
        poetry_dependencies = poetry.get("dependencies")
        if isinstance(poetry_dependencies, dict):
            names.extend(str(name) for name in poetry_dependencies)

    return names


def _iter_development_dependency_names(data: dict[str, Any]) -> list[str]:
    names: list[str] = []

    dependency_groups = data.get("dependency-groups")
    if isinstance(dependency_groups, dict):
        for group_deps in dependency_groups.values():
            if isinstance(group_deps, list):
                for item in group_deps:
                    _add_requirement_names(names, item)

    poetry = (data.get("tool") or {}).get("poetry")
    if isinstance(poetry, dict):
        legacy_dev = poetry.get("dev-dependencies")
        if isinstance(legacy_dev, dict):
            names.extend(str(name) for name in legacy_dev)
        groups = poetry.get("group")
        if isinstance(groups, dict):
            for group in groups.values():
                if not isinstance(group, dict):
                    continue
                group_deps = group.get("dependencies")
                if isinstance(group_deps, dict):
                    names.extend(str(name) for name in group_deps)

    return names


def _iter_optional_dependency_names(data: dict[str, Any]) -> list[str]:
    names: list[str] = []
    project = data.get("project")
    if isinstance(project, dict):
        optional = project.get("optional-dependencies")
        if isinstance(optional, dict):
            for group_deps in optional.values():
                if isinstance(group_deps, list):
                    for item in group_deps:
                        _add_requirement_names(names, item)
    return names


def _normalized_runtime_dependency_names(data: dict[str, Any]) -> set[str]:
    return _normalized_names(_iter_runtime_dependency_names(data))


def _normalized_development_dependency_names(data: dict[str, Any]) -> set[str]:
    return _normalized_names(_iter_development_dependency_names(data))


def _normalized_declared_dependency_names(data: dict[str, Any]) -> set[str]:
    return (
        _normalized_runtime_dependency_names(data)
        | _normalized_development_dependency_names(data)
        | _normalized_names(_iter_optional_dependency_names(data))
    )


class ProjectMetadataTests(unittest.TestCase):
    def test_pyproject_declares_neutral_package_identity(self) -> None:
        name = _project_name(_load_pyproject())
        self.assertIn(_normalized_name(name), _NEUTRAL_PROJECT_NAMES)

    def test_pyproject_requires_python_3_12_only(self) -> None:
        requirement = _python_requirement(_load_pyproject())
        self.assertIn(">=3.12", requirement)
        self.assertIn("<3.13", requirement)

    def test_pyproject_declares_pytest_and_pytest_cov(self) -> None:
        data = _load_pyproject()
        development = _normalized_development_dependency_names(data)
        runtime = _normalized_runtime_dependency_names(data)
        for required in _REQUIRED_DEV_DEPENDENCIES:
            with self.subTest(required=required):
                self.assertIn(_normalized_name(required), development)
                self.assertNotIn(_normalized_name(required), runtime)

    def test_pyyaml_is_the_only_non_python_runtime_dependency(self) -> None:
        runtime = _normalized_runtime_dependency_names(_load_pyproject())
        self.assertEqual(
            runtime,
            {_normalized_name(name) for name in _REQUIRED_RUNTIME_DEPENDENCIES},
        )

    def test_pyproject_keeps_runtime_and_toolchain_boundary(self) -> None:
        declared = _normalized_declared_dependency_names(_load_pyproject())
        for forbidden in _FORBIDDEN_DEPENDENCIES:
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(_normalized_name(forbidden), declared)
