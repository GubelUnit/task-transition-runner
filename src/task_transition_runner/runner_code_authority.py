"""Reference-mode steady-state source authority.

The source root is the Git toplevel of the running entry point. A full
lowercase HEAD and a clean checkout are required. Core imports must
resolve under that root; a resolved core file inside the target root is
a shadow. Source, target, and runtime roots stay pairwise disjoint. A
managed runtime keeps one persisted source SHA.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml


RUNTIME_SOURCE_PIN_NAME = "runner_source_pin.yaml"
MANAGED_RUNTIME_MARKER_NAME = "main_state.yaml"

_SCHEMA_VERSION = "1.0"
_PIN_KEYS = frozenset({"schema_version", "source_sha"})
_CORE_PACKAGE = "task_transition_runner"
_FULL_SHA = re.compile(r"^[0-9a-f]{40}\Z")

_SOURCE_ROOT_INVALID = "SOURCE_AUTHORITY_SOURCE_ROOT_INVALID"
_SOURCE_SHA_INVALID = "SOURCE_AUTHORITY_SOURCE_SHA_INVALID"
_SOURCE_SHA_MISMATCH = "SOURCE_AUTHORITY_SOURCE_SHA_MISMATCH"
_SOURCE_DIRTY = "SOURCE_AUTHORITY_SOURCE_DIRTY"
_IMPORT_PROVENANCE_INVALID = "SOURCE_AUTHORITY_IMPORT_PROVENANCE_INVALID"
_IMPORT_SHADOWED = "SOURCE_AUTHORITY_IMPORT_SHADOWED"
_ROOT_OVERLAP = "SOURCE_AUTHORITY_ROOT_OVERLAP"
_PIN_MISSING = "SOURCE_AUTHORITY_RUNTIME_PIN_MISSING"
_PIN_MISMATCH = "SOURCE_AUTHORITY_RUNTIME_PIN_MISMATCH"
_PIN_IDENTITY_INVALID = "RUNTIME_SOURCE_IDENTITY_INVALID"


class RunnerCodeAuthorityError(Exception):
    """Fail-closed reference-mode source-authority violation."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class SourceAuthorityReport:
    """Local source, root, import, and runtime-pin facts."""

    source_root: Path
    source_sha: str
    entry_point: Path
    target_root: Path
    runtime_root: Path
    runtime_pin_sha: str | None
    loaded_core_modules: tuple[str, ...]


def _require_lowercase_sha(value: str, reason_code: str) -> None:
    if _FULL_SHA.fullmatch(value) is None:
        raise RunnerCodeAuthorityError(reason_code)


def _git_stdout(cwd: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _git_stdout_if_ok(cwd: Path, *args: str) -> str | None:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _is_core_module_name(name: str) -> bool:
    parts = name.split(".")
    if parts[0] != _CORE_PACKAGE:
        return False
    return all(part.isidentifier() for part in parts)


def _separated(left: Path, right: Path) -> bool:
    return not left.is_relative_to(right) and not right.is_relative_to(left)


def _pin_bytes(source_sha: str) -> bytes:
    return (
        f"schema_version: '{_SCHEMA_VERSION}'\n"
        f"source_sha: '{source_sha}'\n"
    ).encode("utf-8")


def _load_pin_sha(path: Path) -> str:
    if not path.is_file():
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    try:
        text = path.read_bytes().decode("utf-8")
    except UnicodeDecodeError:
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID) from None
    node = yaml.compose(text, Loader=yaml.SafeLoader)
    if not isinstance(node, yaml.MappingNode):
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    keys = [key.value for key, _value in node.value]
    if len(keys) != len(set(keys)) or set(keys) != _PIN_KEYS:
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    payload = yaml.safe_load(text)
    if payload["schema_version"] != _SCHEMA_VERSION:
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    source_sha = payload["source_sha"]
    if not isinstance(source_sha, str):
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    _require_lowercase_sha(source_sha, _PIN_IDENTITY_INVALID)
    return source_sha


def _inventory(
    loaded_module_files: Mapping[str, Path | str] | None,
) -> dict[str, Path | str]:
    if loaded_module_files is None:
        return {
            name: path
            for name, path in collect_loaded_core_module_paths().items()
        }
    return dict(loaded_module_files)


def _core_module_names(
    loaded_module_files: Mapping[str, Path | str],
    source_root: Path,
    target_root: Path,
) -> tuple[str, ...]:
    names: list[str] = []
    for name in sorted(loaded_module_files):
        if not _is_core_module_name(name):
            raise RunnerCodeAuthorityError(_IMPORT_PROVENANCE_INVALID)
        resolved = Path(loaded_module_files[name]).resolve()
        if not resolved.is_file():
            raise RunnerCodeAuthorityError(_IMPORT_PROVENANCE_INVALID)
        if resolved.is_relative_to(target_root):
            raise RunnerCodeAuthorityError(_IMPORT_SHADOWED)
        if not resolved.is_relative_to(source_root):
            raise RunnerCodeAuthorityError(_IMPORT_PROVENANCE_INVALID)
        names.append(name)
    if not names:
        raise RunnerCodeAuthorityError(_IMPORT_PROVENANCE_INVALID)
    return tuple(names)


def _runtime_pin_sha(runtime_root: Path, source_sha: str) -> str | None:
    if not runtime_root.exists():
        return None
    pin_path = runtime_root / RUNTIME_SOURCE_PIN_NAME
    if pin_path.exists():
        pinned = _load_pin_sha(pin_path)
        if pinned != source_sha:
            raise RunnerCodeAuthorityError(_PIN_MISMATCH)
        return pinned
    marker = runtime_root / MANAGED_RUNTIME_MARKER_NAME
    if marker.is_file():
        raise RunnerCodeAuthorityError(_PIN_MISSING)
    return None


def derive_source_root(entry_point: Path) -> Path:
    """Return the Git toplevel that contains ``entry_point``."""

    entry = Path(entry_point)
    if not entry.is_file():
        raise RunnerCodeAuthorityError(_SOURCE_ROOT_INVALID)
    reported = _git_stdout_if_ok(entry.parent, "rev-parse", "--show-toplevel")
    if reported is None:
        raise RunnerCodeAuthorityError(_SOURCE_ROOT_INVALID)
    source_root = Path(reported).resolve()
    if not entry.resolve().is_relative_to(source_root):
        raise RunnerCodeAuthorityError(_SOURCE_ROOT_INVALID)
    return source_root


def collect_loaded_core_module_paths() -> dict[str, Path]:
    """Return loaded core modules that expose a string ``__file__``."""

    loaded: dict[str, Path] = {}
    for name, module in sys.modules.items():
        if not _is_core_module_name(name):
            continue
        file_attr = getattr(module, "__file__", None)
        if not isinstance(file_attr, str):
            continue
        loaded[name] = Path(file_attr).resolve()
    return loaded


def persist_runtime_source_pin(
    *,
    runtime_root: Path,
    source_sha: str,
) -> dict[str, str]:
    """Create or reprove the runtime source pin without rewriting a match."""

    _require_lowercase_sha(source_sha, _SOURCE_SHA_INVALID)
    runtime = Path(runtime_root)
    if not runtime.is_dir():
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    pin_path = runtime / RUNTIME_SOURCE_PIN_NAME
    if pin_path.is_dir():
        raise RunnerCodeAuthorityError(_PIN_IDENTITY_INVALID)
    if pin_path.is_file():
        pinned = _load_pin_sha(pin_path)
        if pinned != source_sha:
            raise RunnerCodeAuthorityError(_PIN_MISMATCH)
        return {"schema_version": _SCHEMA_VERSION, "source_sha": pinned}
    pin_path.write_bytes(_pin_bytes(source_sha))
    return {"schema_version": _SCHEMA_VERSION, "source_sha": source_sha}


def verify_steady_state_code_authority(
    *,
    entry_point: Path,
    target_root: Path,
    runtime_root: Path,
    expected_source_sha: str | None = None,
    loaded_module_files: Mapping[str, Path | str] | None = None,
) -> SourceAuthorityReport:
    """Prove reference-mode source authority without mutating Git or runtime."""

    source_root = derive_source_root(entry_point)
    source_sha = _git_stdout(source_root, "rev-parse", "HEAD")
    _require_lowercase_sha(source_sha, _SOURCE_SHA_INVALID)
    if expected_source_sha is not None:
        _require_lowercase_sha(expected_source_sha, _SOURCE_SHA_INVALID)
        if expected_source_sha != source_sha:
            raise RunnerCodeAuthorityError(_SOURCE_SHA_MISMATCH)
    target = Path(target_root)
    runtime = Path(runtime_root)
    if not target.is_dir() or (runtime.exists() and not runtime.is_dir()):
        raise RunnerCodeAuthorityError(_ROOT_OVERLAP)
    resolved_target = target.resolve()
    resolved_runtime = runtime.resolve()
    if not _separated(source_root, resolved_target):
        raise RunnerCodeAuthorityError(_ROOT_OVERLAP)
    if not _separated(source_root, resolved_runtime):
        raise RunnerCodeAuthorityError(_ROOT_OVERLAP)
    if not _separated(resolved_target, resolved_runtime):
        raise RunnerCodeAuthorityError(_ROOT_OVERLAP)
    if _git_stdout(source_root, "status", "--porcelain", "--untracked-files=all") != "":
        raise RunnerCodeAuthorityError(_SOURCE_DIRTY)
    names = _core_module_names(
        _inventory(loaded_module_files),
        source_root,
        resolved_target,
    )
    return SourceAuthorityReport(
        source_root=source_root,
        source_sha=source_sha,
        entry_point=Path(entry_point).resolve(),
        target_root=resolved_target,
        runtime_root=resolved_runtime,
        runtime_pin_sha=_runtime_pin_sha(runtime, source_sha),
        loaded_core_modules=names,
    )
