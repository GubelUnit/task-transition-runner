"""Synthetic pre-spawn executable, argv, Target, and Environment contract.

Keep:
- exact configured absolute executable or one PATH basename resolution
- empty PATH components ignored, never searched as the current working directory
- executable symlink once to the absolute canonical regular executable target
- PATH directory candidate skipped and remembered as unsuitable
- historical four-flag prefix --trust, --print, --force, stream-json
- exactly one --disable-auto-update after that prefix and before workspace
- optional nonblank model as opaque argv data
- shell=False, stdin=DEVNULL, resolved Target as cwd and --workspace
- Target separated from Runner Source and Runtime, no symlink leaf or parent component
- targeted CURSOR_API_ENDPOINT presence, including a set empty value
- explicit alias-like PATH basename when that name is the configured executable
- missing PATH as empty search, never os.defpath or CS_PATH
- newline, carriage-return, and mixed whitespace models omitted
- NUL in executable, model, or root as ADAPTER_INVOCATION_INVALID
- Source and Runtime neither equal to nor below Target

Drop:
- default binary, implicit agent/cursor-agent alias, or private absolute fallback
- relative executable_path after resolution, directory executable, or empty endpoint override
- os.defpath/CS_PATH fallback when PATH is unset
- empty PATH component CWD search, second PATH/alias/private fallback, or non-canonical symlink path
- shell=True, inherited stdin, --endpoint, or Environment copy/hash/project
- spawn, pipes, process groups, capture, or Cursor invocation
- a test helper that stands in for the missing product API
"""

from __future__ import annotations

import inspect
import os
import stat
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from task_transition_runner.adapters.cursor_headless import (
    build_cursor_argv,
    resolve_cursor_executable,
)

_REASON_NOT_FOUND = "ADAPTER_EXECUTABLE_NOT_FOUND"
_REASON_NOT_EXECUTABLE = "ADAPTER_EXECUTABLE_NOT_EXECUTABLE"
_REASON_INVALID = "ADAPTER_INVOCATION_INVALID"
_REASON_WORKSPACE = "ADAPTER_WORKSPACE_MISMATCH"
_MODE_ABSOLUTE = "absolute"
_MODE_PATH = "path"
_FIXED_FLAGS = (
    "--trust",
    "--print",
    "--force",
    "--output-format=stream-json",
)
_AUTO_UPDATE_SUPPRESSION_FLAG = "--disable-auto-update"
_SIG_AUTO_UPDATE_SUPPRESSION_MISSING = "CURSOR_AUTO_UPDATE_SUPPRESSION_FLAG_MISSING"
_SUPPRESSED_FIXED_FLAGS = (
    *_FIXED_FLAGS,
    _AUTO_UPDATE_SUPPRESSION_FLAG,
)
_FORBIDDEN_FLAGS = (
    "--stream-partial-output",
    "--resume",
    "--continue",
    "--api-key",
    "--header",
    "--endpoint",
    "--add-dir",
    "--worktree",
    "--plugin-dir",
    "--approve-mcps",
    "--sandbox",
)
_ALIAS_NAMES = ("agent", "cursor-agent", "cursor")
_PATH_BIN_NAME = "synthetic-oss5-path-bin"
_ABS_BIN_NAME = "synthetic-oss5-absolute-bin"
_MISSING_BIN_NAME = "synthetic-oss5-missing-bin"
_CANARY_ENDPOINT_VALUE = "SYNTHETIC-OSS5-CANARY-ENDPOINT-VALUE"
_CANARY_AUTH_VALUE = "SYNTHETIC-OSS5-CANARY-AUTH-VALUE"
_CANARY_ENV_NAME = "SYNTHETIC_OSS5_CANARY_ENVIRONMENT_NAME"
_CANARY_ENV_VALUE = "SYNTHETIC-OSS5-CANARY-ENVIRONMENT-VALUE"
_INJECTION = "synthetic;reboot $(id) | cat && true `uname`"
_SIG_EMPTY_PATH_CWD = (
    "empty PATH component selected executable from current working directory"
)
_SIG_SYMLINK_CANONICAL = (
    "valid executable symlink rejected or returned path was not the canonical target"
)
_SIG_SYMLINK_NOT_FOUND = (
    "dangling or cyclic executable symlink was not ADAPTER_EXECUTABLE_NOT_FOUND"
)
_FORBIDDEN_PARAMETER_NAMES = {
    "agent",
    "alias",
    "auth",
    "default",
    "endpoint",
    "env",
    "environment",
    "fallback",
    "file",
    "file_path",
    "logical_ref",
    "spawn",
    "source",
    "source_path",
    "stdin",
    "which",
}


class _Roots:
    def __init__(
        self,
        *,
        runtime_root: Path,
        target_root: Path,
        runner_source_root: Path,
        executable: Path,
    ) -> None:
        self.runtime_root = runtime_root
        self.target_root = target_root
        self.runner_source_root = runner_source_root
        self.executable = executable

    def snippets(self) -> tuple[str, ...]:
        return (
            str(self.runtime_root),
            str(self.target_root),
            str(self.runner_source_root),
            str(self.executable),
            _CANARY_ENDPOINT_VALUE,
            _CANARY_AUTH_VALUE,
            _CANARY_ENV_NAME,
            _CANARY_ENV_VALUE,
        )


def _write_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    path.chmod(0o755)
    return path


def _write_non_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    path.chmod(0o644)
    return path


def _write_symlink(link: Path, target: Path) -> Path:
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(target)
    assert link.is_symlink()
    return link


def _write_relative_symlink(link: Path, target: Path) -> Path:
    link.parent.mkdir(parents=True, exist_ok=True)
    relative = Path(os.path.relpath(os.fspath(target), os.fspath(link.parent)))
    link.symlink_to(relative)
    assert link.is_symlink()
    assert not link.readlink().is_absolute()
    return link


def _canonical(path: Path) -> str:
    return os.path.realpath(os.fspath(path))


def _layout(tmp_path: Path) -> _Roots:
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    runtime_root.mkdir()
    target_root.mkdir()
    runner_source_root.mkdir()
    executable = _write_executable(tmp_path / "bin" / _ABS_BIN_NAME)
    return _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
        executable=executable,
    )


def _abspath(path: Path) -> str:
    return os.path.abspath(os.fspath(path))


def _isolate_path(monkeypatch: pytest.MonkeyPatch, *directories: Path) -> None:
    monkeypatch.setenv("PATH", os.pathsep.join(str(directory) for directory in directories))
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)


def _resolved_absolute(path: Path) -> Any:
    return resolve_cursor_executable(configured_executable=_abspath(path))


def _plan(
    roots: _Roots,
    *,
    executable_path: str | None = None,
    model: str | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "executable_path": executable_path if executable_path is not None else _abspath(roots.executable),
        "target_root": roots.target_root,
        "runner_source_root": roots.runner_source_root,
        "runtime_root": roots.runtime_root,
    }
    if model is not None:
        kwargs["model"] = model
    return build_cursor_argv(**kwargs)


def _expect_reason(
    reason: str,
    action: Callable[[], object],
    *,
    snippets: tuple[str, ...] = (),
) -> BaseException:
    with pytest.raises(Exception) as exc_info:
        action()
    exc = exc_info.value
    message = str(exc)
    assert getattr(exc, "reason_code") == reason
    assert message == reason
    for snippet in snippets:
        assert snippet not in message
    return exc


def _assert_keyword_only(
    func: Callable[..., object],
    required: set[str],
    *,
    optional: set[str] = frozenset(),
) -> None:
    parameters = inspect.signature(func).parameters
    assert required <= set(parameters)
    assert optional <= set(parameters)
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)
    for name, parameter in parameters.items():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        if name in required:
            assert parameter.default is inspect.Parameter.empty
        if name in optional:
            assert parameter.default is None


def _flag_name(item: str) -> str:
    return item.split("=", 1)[0]


def _workspace_argument(argv: tuple[str, ...], *, model: str | None) -> str:
    if model is not None and model.strip() != "":
        return argv[-2]
    return argv[-1]


def _require_auto_update_suppression_before_workspace(
    argv: tuple[str, ...],
    *,
    executable: str,
    workspace: str,
    model: str | None = None,
) -> None:
    expected: tuple[str, ...] = (
        executable,
        *_SUPPRESSED_FIXED_FLAGS,
        workspace,
    )
    if model is not None and model.strip() != "":
        expected = (*expected, f"--model={model}")
    flag_count = sum(item == _AUTO_UPDATE_SUPPRESSION_FLAG for item in argv)
    if flag_count != 1 or argv != expected:
        pytest.fail(_SIG_AUTO_UPDATE_SUPPRESSION_MISSING)


def _install_environ_guard(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    violations: list[str] = []
    environ = os.environ
    original_keys = environ.keys
    original_items = environ.items
    original_values = environ.values
    original_copy = environ.copy
    original_iter = type(environ).__iter__

    def keys() -> Any:
        violations.append("keys")
        return original_keys()

    def items() -> Any:
        violations.append("items")
        return original_items()

    def values() -> Any:
        violations.append("values")
        return original_values()

    def copy() -> Any:
        violations.append("copy")
        return original_copy()

    def iter_env(self: Any) -> Any:
        if self is environ:
            violations.append("iter")
        return original_iter(self)

    monkeypatch.setattr(environ, "keys", keys)
    monkeypatch.setattr(environ, "items", items)
    monkeypatch.setattr(environ, "values", values)
    monkeypatch.setattr(environ, "copy", copy)
    monkeypatch.setattr(type(environ), "__iter__", iter_env)
    return violations


def _install_spawn_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*_args: Any, **_kwargs: Any) -> object:
        raise AssertionError("pre-spawn invocation must not spawn a child")

    monkeypatch.setattr(subprocess, "Popen", blocked)
    monkeypatch.setattr(subprocess, "run", blocked)
    monkeypatch.setattr(subprocess, "call", blocked)
    monkeypatch.setattr(subprocess, "check_call", blocked)
    monkeypatch.setattr(subprocess, "check_output", blocked)
    monkeypatch.setattr(os, "system", blocked)
    monkeypatch.setattr(os, "execv", blocked)
    monkeypatch.setattr(os, "execve", blocked)
    if hasattr(os, "posix_spawn"):
        monkeypatch.setattr(os, "posix_spawn", blocked)
    if hasattr(os, "posix_spawnp"):
        monkeypatch.setattr(os, "posix_spawnp", blocked)


def _assert_no_forbidden_flags(argv: tuple[str, ...]) -> None:
    names = [_flag_name(item) for item in argv]
    for flag in _FORBIDDEN_FLAGS:
        assert flag not in names
        assert all(not item.startswith(f"{flag}=") for item in argv)


def _assert_plan_privacy(plan: Any, *, snippets: tuple[str, ...]) -> None:
    assert not hasattr(plan, "environment")
    assert not hasattr(plan, "env")
    assert not hasattr(plan, "endpoint")
    rendered = " ".join(str(item) for item in (plan.argv, plan.cwd, plan.shell, plan.stdin))
    for snippet in snippets:
        assert snippet not in rendered


def test_resolve_cursor_executable_binds_keyword_configured_executable_only() -> None:
    _assert_keyword_only(resolve_cursor_executable, {"configured_executable"})


def test_resolve_cursor_executable_accepts_absolute_executable_without_path_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decoy_dir = tmp_path / "decoy-path"
    decoy = _write_executable(decoy_dir / _ABS_BIN_NAME)
    absolute = _write_executable(tmp_path / "real" / _ABS_BIN_NAME)
    _isolate_path(monkeypatch, decoy_dir)
    _install_spawn_guard(monkeypatch)

    resolved = resolve_cursor_executable(configured_executable=_abspath(absolute))

    assert resolved.path == _abspath(absolute)
    assert resolved.resolution_mode == _MODE_ABSOLUTE
    assert resolved.path != _abspath(decoy)
    assert os.path.isabs(resolved.path)
    assert not Path(resolved.path).is_symlink()
    assert os.access(resolved.path, os.X_OK)
    assert stat.S_ISREG(os.lstat(resolved.path).st_mode)


def test_resolve_cursor_executable_resolves_basename_once_through_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_dir = tmp_path / "path-a"
    second_dir = tmp_path / "path-b"
    first = _write_executable(first_dir / _PATH_BIN_NAME)
    second = _write_executable(second_dir / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, first_dir, second_dir)
    _install_spawn_guard(monkeypatch)

    resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)

    assert resolved.path == _abspath(first)
    assert resolved.resolution_mode == _MODE_PATH
    assert resolved.path != _abspath(second)
    assert os.path.isabs(resolved.path)
    assert os.access(resolved.path, os.X_OK)


def test_resolve_cursor_executable_rejects_missing_absolute_without_path_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    path_dir = tmp_path / "path-only"
    _write_executable(path_dir / _PATH_BIN_NAME)
    missing = tmp_path / "missing" / _ABS_BIN_NAME
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_NOT_FOUND,
        lambda: resolve_cursor_executable(configured_executable=_abspath(missing)),
        snippets=roots.snippets() + (_abspath(path_dir / _PATH_BIN_NAME),),
    )


def test_resolve_cursor_executable_rejects_missing_path_basename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "path-empty"
    path_dir.mkdir()
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_NOT_FOUND,
        lambda: resolve_cursor_executable(configured_executable=_MISSING_BIN_NAME),
        snippets=(_CANARY_ENDPOINT_VALUE, *_ALIAS_NAMES),
    )


def test_resolve_cursor_executable_rejects_missing_path_without_defpath_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    defpath_dir = tmp_path / "defpath"
    decoy = _write_executable(defpath_dir / _PATH_BIN_NAME)
    monkeypatch.delenv("PATH", raising=False)
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    monkeypatch.setattr(os, "defpath", str(defpath_dir))
    if hasattr(os, "confstr"):
        real_confstr = os.confstr

        def confstr(name: object) -> str:
            if name in {"CS_PATH", "PATH"}:
                return str(defpath_dir)
            return real_confstr(name)

        monkeypatch.setattr(os, "confstr", confstr)
    _install_spawn_guard(monkeypatch)

    assert "PATH" not in os.environ
    _expect_reason(
        _REASON_NOT_FOUND,
        lambda: resolve_cursor_executable(configured_executable=_PATH_BIN_NAME),
        snippets=(_abspath(decoy),),
    )


def test_resolve_cursor_executable_rejects_absolute_non_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocked = _write_non_executable(tmp_path / "noexec" / _ABS_BIN_NAME)
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_abspath(blocked)),
        snippets=(_CANARY_ENDPOINT_VALUE,),
    )


def test_resolve_cursor_executable_rejects_directory_as_not_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "directory-executable"
    directory.mkdir()
    directory.chmod(0o755)
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    assert stat.S_ISDIR(directory.lstat().st_mode)
    assert os.access(directory, os.X_OK)
    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_abspath(directory)),
        snippets=(_CANARY_ENDPOINT_VALUE,),
    )


def test_resolve_cursor_executable_skips_first_non_executable_path_hit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_dir = tmp_path / "path-noexec"
    second_dir = tmp_path / "path-exec"
    first = _write_non_executable(first_dir / _PATH_BIN_NAME)
    second = _write_executable(second_dir / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, first_dir, second_dir)
    _install_spawn_guard(monkeypatch)

    resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)

    assert resolved.path == _abspath(second)
    assert resolved.resolution_mode == _MODE_PATH
    assert resolved.path != _abspath(first)
    assert os.access(resolved.path, os.X_OK)
    assert not os.access(first, os.X_OK)


def test_resolve_cursor_executable_rejects_path_non_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "path-noexec"
    _write_non_executable(path_dir / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_PATH_BIN_NAME),
        snippets=(_CANARY_ENDPOINT_VALUE,),
    )


def test_resolve_cursor_executable_rejects_blank_configured_without_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "alias-path"
    for name in _ALIAS_NAMES:
        _write_executable(path_dir / name)
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    for configured in ("", " \t", "\n"):
        _expect_reason(
            _REASON_INVALID,
            lambda configured=configured: resolve_cursor_executable(
                configured_executable=configured
            ),
            snippets=(_abspath(path_dir / "cursor"), *_ALIAS_NAMES),
        )


def test_resolve_cursor_executable_does_not_alias_agent_or_cursor_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "aliases"
    aliases = {name: _write_executable(path_dir / name) for name in _ALIAS_NAMES}
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_NOT_FOUND,
        lambda: resolve_cursor_executable(configured_executable=_MISSING_BIN_NAME),
        snippets=tuple(_abspath(path) for path in aliases.values()),
    )


def test_resolve_cursor_executable_accepts_explicit_alias_like_path_basename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "explicit-aliases"
    aliases = {name: _write_executable(path_dir / name) for name in _ALIAS_NAMES}
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    for name, executable in aliases.items():
        resolved = resolve_cursor_executable(configured_executable=name)
        assert resolved.path == _abspath(executable)
        assert resolved.resolution_mode == _MODE_PATH
        assert os.path.basename(resolved.path) == name
        for other_name, other in aliases.items():
            if other_name != name:
                assert resolved.path != _abspath(other)


def test_resolve_cursor_executable_rejects_relative_path_with_separator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nested = _write_executable(tmp_path / "nested" / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: resolve_cursor_executable(
            configured_executable=str(Path("nested") / _PATH_BIN_NAME)
        ),
        snippets=(_abspath(nested),),
    )


def test_resolve_cursor_executable_rejects_empty_path_without_cwd_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cwd = tmp_path / "cwd-empty-path"
    cwd.mkdir()
    decoy = _write_executable(cwd / _PATH_BIN_NAME)
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PATH", "")
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    _install_spawn_guard(monkeypatch)

    assert Path.cwd() == cwd
    assert os.environ.get("PATH") == ""
    try:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_NOT_FOUND
        assert str(exc) == _REASON_NOT_FOUND
        assert _abspath(decoy) not in str(exc)
        return
    assert resolved.path != _abspath(decoy), _SIG_EMPTY_PATH_CWD
    pytest.fail(_SIG_EMPTY_PATH_CWD)


def test_resolve_cursor_executable_ignores_leading_empty_path_component_without_cwd_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cwd = tmp_path / "cwd-leading-empty"
    valid_dir = tmp_path / "path-valid-leading"
    cwd.mkdir()
    decoy = _write_executable(cwd / _PATH_BIN_NAME)
    valid = _write_executable(valid_dir / _PATH_BIN_NAME)
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PATH", os.pathsep.join(("", str(valid_dir))))
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        pytest.fail(f"{_SIG_EMPTY_PATH_CWD}: {getattr(exc, 'reason_code', type(exc).__name__)}")

    assert resolved.path == _abspath(valid), _SIG_EMPTY_PATH_CWD
    assert resolved.path != _abspath(decoy), _SIG_EMPTY_PATH_CWD
    assert resolved.resolution_mode == _MODE_PATH
    assert os.path.isabs(resolved.path)
    assert os.access(resolved.path, os.X_OK)


def test_resolve_cursor_executable_ignores_trailing_empty_path_component_without_cwd_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cwd = tmp_path / "cwd-trailing-empty"
    empty_dir = tmp_path / "path-empty-trailing"
    cwd.mkdir()
    empty_dir.mkdir()
    decoy = _write_executable(cwd / _PATH_BIN_NAME)
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PATH", os.pathsep.join((str(empty_dir), "")))
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_NOT_FOUND
        assert str(exc) == _REASON_NOT_FOUND
        assert _abspath(decoy) not in str(exc)
        return
    assert resolved.path != _abspath(decoy), _SIG_EMPTY_PATH_CWD
    pytest.fail(_SIG_EMPTY_PATH_CWD)


def test_resolve_cursor_executable_ignores_consecutive_empty_path_components_without_cwd_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cwd = tmp_path / "cwd-consecutive-empty"
    empty_dir = tmp_path / "path-empty-consecutive"
    valid_dir = tmp_path / "path-valid-consecutive"
    cwd.mkdir()
    empty_dir.mkdir()
    decoy = _write_executable(cwd / _PATH_BIN_NAME)
    valid = _write_executable(valid_dir / _PATH_BIN_NAME)
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PATH", os.pathsep.join((str(empty_dir), "", str(valid_dir))))
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        pytest.fail(f"{_SIG_EMPTY_PATH_CWD}: {getattr(exc, 'reason_code', type(exc).__name__)}")

    assert resolved.path == _abspath(valid), _SIG_EMPTY_PATH_CWD
    assert resolved.path != _abspath(decoy), _SIG_EMPTY_PATH_CWD
    assert resolved.resolution_mode == _MODE_PATH
    assert os.path.isabs(resolved.path)
    assert os.access(resolved.path, os.X_OK)


def test_resolve_cursor_executable_accepts_absolute_executable_symlink_as_canonical_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decoy_dir = tmp_path / "decoy-path"
    decoy = _write_executable(decoy_dir / _ABS_BIN_NAME)
    target = _write_executable(tmp_path / "real" / _ABS_BIN_NAME)
    link = _write_symlink(tmp_path / "link" / _ABS_BIN_NAME, target)
    _isolate_path(monkeypatch, decoy_dir)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_abspath(link))
    except Exception as exc:
        pytest.fail(f"{_SIG_SYMLINK_CANONICAL}: {getattr(exc, 'reason_code', type(exc).__name__)}")

    assert resolved.path == _canonical(link), _SIG_SYMLINK_CANONICAL
    assert resolved.path == _abspath(target), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(link), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(decoy)
    assert resolved.resolution_mode == _MODE_ABSOLUTE
    assert os.path.isabs(resolved.path)
    assert not Path(resolved.path).is_symlink()
    assert os.access(resolved.path, os.X_OK)
    assert stat.S_ISREG(os.lstat(resolved.path).st_mode)


def test_resolve_cursor_executable_accepts_path_executable_symlink_as_canonical_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "path-link"
    decoy_dir = tmp_path / "path-decoy"
    target = _write_executable(tmp_path / "canonical-target" / _PATH_BIN_NAME)
    link = _write_symlink(path_dir / _PATH_BIN_NAME, target)
    decoy = _write_executable(decoy_dir / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, path_dir, decoy_dir)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        pytest.fail(f"{_SIG_SYMLINK_CANONICAL}: {getattr(exc, 'reason_code', type(exc).__name__)}")

    assert resolved.path == _canonical(link), _SIG_SYMLINK_CANONICAL
    assert resolved.path == _abspath(target), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(link), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(decoy), _SIG_SYMLINK_CANONICAL
    assert resolved.resolution_mode == _MODE_PATH
    assert os.path.isabs(resolved.path)
    assert not Path(resolved.path).is_symlink()
    assert os.access(resolved.path, os.X_OK)
    assert stat.S_ISREG(os.lstat(resolved.path).st_mode)


def test_resolve_cursor_executable_rejects_dangling_absolute_symlink_as_not_found(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing" / _ABS_BIN_NAME
    link = _write_symlink(tmp_path / "dangling" / _ABS_BIN_NAME, missing)
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    try:
        resolve_cursor_executable(configured_executable=_abspath(link))
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_NOT_FOUND, _SIG_SYMLINK_NOT_FOUND
        assert str(exc) == _REASON_NOT_FOUND
        assert _abspath(missing) not in str(exc)
        assert _abspath(link) not in str(exc)
        return
    pytest.fail(_SIG_SYMLINK_NOT_FOUND)


def test_resolve_cursor_executable_rejects_cyclic_absolute_symlink_as_not_found(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    link = tmp_path / "cyclic" / _ABS_BIN_NAME
    link.parent.mkdir(parents=True)
    link.symlink_to(link.name)
    assert link.is_symlink()
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    try:
        resolve_cursor_executable(configured_executable=_abspath(link))
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_NOT_FOUND, _SIG_SYMLINK_NOT_FOUND
        assert str(exc) == _REASON_NOT_FOUND
        assert _abspath(link) not in str(exc)
        return
    pytest.fail(_SIG_SYMLINK_NOT_FOUND)


def test_resolve_cursor_executable_rejects_dangling_path_symlink_as_not_found(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "path-dangling"
    missing = tmp_path / "missing-path-target" / _PATH_BIN_NAME
    link = _write_symlink(path_dir / _PATH_BIN_NAME, missing)
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    try:
        resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_NOT_FOUND, _SIG_SYMLINK_NOT_FOUND
        assert str(exc) == _REASON_NOT_FOUND
        assert _abspath(missing) not in str(exc)
        assert _abspath(link) not in str(exc)
        return
    pytest.fail(_SIG_SYMLINK_NOT_FOUND)


def test_resolve_cursor_executable_rejects_absolute_symlink_to_directory_as_not_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "symlink-directory-target"
    directory.mkdir()
    directory.chmod(0o755)
    link = _write_symlink(tmp_path / "dir-link" / _ABS_BIN_NAME, directory)
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    assert stat.S_ISDIR(directory.lstat().st_mode)
    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_abspath(link)),
        snippets=(_CANARY_ENDPOINT_VALUE, _abspath(directory)),
    )


def test_resolve_cursor_executable_rejects_absolute_symlink_to_nonregular_as_not_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fifo = tmp_path / "fifo-target"
    os.mkfifo(fifo)
    fifo.chmod(0o755)
    link = _write_symlink(tmp_path / "fifo-link" / _ABS_BIN_NAME, fifo)
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    assert stat.S_ISFIFO(fifo.lstat().st_mode)
    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_abspath(link)),
        snippets=(_CANARY_ENDPOINT_VALUE, _abspath(fifo)),
    )


def test_resolve_cursor_executable_rejects_absolute_symlink_to_non_executable_as_not_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocked = _write_non_executable(tmp_path / "noexec-target" / _ABS_BIN_NAME)
    link = _write_symlink(tmp_path / "noexec-link" / _ABS_BIN_NAME, blocked)
    _isolate_path(monkeypatch, tmp_path / "empty-path")
    (tmp_path / "empty-path").mkdir()
    _install_spawn_guard(monkeypatch)

    assert not os.access(blocked, os.X_OK)
    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_abspath(link)),
        snippets=(_CANARY_ENDPOINT_VALUE, _abspath(blocked)),
    )


def test_resolve_cursor_executable_skips_path_directory_hit_for_later_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_dir = tmp_path / "path-directory-hit"
    second_dir = tmp_path / "path-exec"
    directory = first_dir / _PATH_BIN_NAME
    directory.mkdir(parents=True)
    directory.chmod(0o755)
    second = _write_executable(second_dir / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, first_dir, second_dir)
    _install_spawn_guard(monkeypatch)

    assert stat.S_ISDIR(directory.lstat().st_mode)
    resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)

    assert resolved.path == _abspath(second)
    assert resolved.resolution_mode == _MODE_PATH
    assert resolved.path != _abspath(directory)
    assert os.access(resolved.path, os.X_OK)
    assert stat.S_ISREG(os.lstat(resolved.path).st_mode)


def test_resolve_cursor_executable_rejects_path_directory_hit_without_later_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path_dir = tmp_path / "path-directory-only"
    directory = path_dir / _PATH_BIN_NAME
    directory.mkdir(parents=True)
    directory.chmod(0o755)
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    assert stat.S_ISDIR(directory.lstat().st_mode)
    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_PATH_BIN_NAME),
        snippets=(_CANARY_ENDPOINT_VALUE, _abspath(directory)),
    )


@pytest.mark.parametrize("unsuitable_kind", ("directory", "non_executable"))
def test_resolve_cursor_executable_rejects_empty_path_component_after_unsuitable_path_hit_without_cwd_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsuitable_kind: str,
) -> None:
    cwd = tmp_path / "cwd-after-unsuitable"
    first_dir = tmp_path / "path-unsuitable"
    cwd.mkdir()
    first_dir.mkdir()
    decoy = _write_executable(cwd / _PATH_BIN_NAME)
    candidate = first_dir / _PATH_BIN_NAME
    if unsuitable_kind == "directory":
        candidate.mkdir()
        candidate.chmod(0o755)
        assert stat.S_ISDIR(candidate.lstat().st_mode)
    else:
        _write_non_executable(candidate)
        assert not os.access(candidate, os.X_OK)
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PATH", os.pathsep.join((str(first_dir), "")))
    monkeypatch.delenv("CURSOR_API_ENDPOINT", raising=False)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    except Exception as exc:
        assert getattr(exc, "reason_code") == _REASON_NOT_EXECUTABLE
        assert str(exc) == _REASON_NOT_EXECUTABLE
        assert _abspath(decoy) not in str(exc)
        assert _abspath(candidate) not in str(exc)
        return
    assert resolved.path != _abspath(decoy), _SIG_EMPTY_PATH_CWD
    pytest.fail(_SIG_EMPTY_PATH_CWD)


def test_resolve_cursor_executable_accepts_relative_multihop_executable_symlink_as_canonical_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decoy_dir = tmp_path / "decoy-path"
    decoy = _write_executable(decoy_dir / _ABS_BIN_NAME)
    target = _write_executable(tmp_path / "real" / _ABS_BIN_NAME)
    middle = _write_relative_symlink(tmp_path / "mid" / _ABS_BIN_NAME, target)
    start = _write_relative_symlink(tmp_path / "start" / _ABS_BIN_NAME, middle)
    _isolate_path(monkeypatch, decoy_dir)
    _install_spawn_guard(monkeypatch)

    try:
        resolved = resolve_cursor_executable(configured_executable=_abspath(start))
    except Exception as exc:
        pytest.fail(f"{_SIG_SYMLINK_CANONICAL}: {getattr(exc, 'reason_code', type(exc).__name__)}")

    assert resolved.path == _canonical(start), _SIG_SYMLINK_CANONICAL
    assert resolved.path == _abspath(target), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(start), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(middle), _SIG_SYMLINK_CANONICAL
    assert resolved.path != _abspath(decoy)
    assert resolved.resolution_mode == _MODE_ABSOLUTE
    assert os.path.isabs(resolved.path)
    assert not Path(resolved.path).is_symlink()
    assert os.access(resolved.path, os.X_OK)
    assert stat.S_ISREG(os.lstat(resolved.path).st_mode)


@pytest.mark.parametrize("later_valid", (True, False))
def test_resolve_cursor_executable_skips_path_symlink_to_unsuitable_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    later_valid: bool,
) -> None:
    first_dir = tmp_path / "path-symlink-unsuitable"
    second_dir = tmp_path / "path-later-exec"
    directory = tmp_path / "unsuitable-directory-target"
    directory.mkdir()
    directory.chmod(0o755)
    link = _write_symlink(first_dir / _PATH_BIN_NAME, directory)
    if later_valid:
        later = _write_executable(second_dir / _PATH_BIN_NAME)
        _isolate_path(monkeypatch, first_dir, second_dir)
    else:
        later = None
        _isolate_path(monkeypatch, first_dir)
    _install_spawn_guard(monkeypatch)

    assert stat.S_ISDIR(directory.lstat().st_mode)
    assert link.is_symlink()
    if later_valid:
        resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
        assert later is not None
        assert resolved.path == _abspath(later)
        assert resolved.path != _abspath(link)
        assert resolved.path != _abspath(directory)
        assert resolved.resolution_mode == _MODE_PATH
        assert os.access(resolved.path, os.X_OK)
        assert stat.S_ISREG(os.lstat(resolved.path).st_mode)
        return
    _expect_reason(
        _REASON_NOT_EXECUTABLE,
        lambda: resolve_cursor_executable(configured_executable=_PATH_BIN_NAME),
        snippets=(_CANARY_ENDPOINT_VALUE, _abspath(directory), _abspath(link)),
    )


def test_build_cursor_argv_binds_keyword_only_roots_executable_and_optional_model() -> None:
    _assert_keyword_only(
        build_cursor_argv,
        {"executable_path", "target_root", "runner_source_root", "runtime_root"},
        optional={"model"},
    )


def test_build_cursor_argv_emits_fixed_flag_order_with_target_as_workspace_and_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path / "unused-path")
    (tmp_path / "unused-path").mkdir()
    _install_spawn_guard(monkeypatch)
    target = _abspath(roots.target_root)
    executable = _abspath(roots.executable)

    plan = _plan(roots)

    assert plan.argv[0] == executable
    assert tuple(plan.argv[1:5]) == _FIXED_FLAGS
    workspace = _workspace_argument(plan.argv, model=None)
    assert workspace == f"--workspace={target}"
    assert plan.cwd == target
    assert workspace.removeprefix("--workspace=") == plan.cwd
    _assert_no_forbidden_flags(tuple(plan.argv))
    _assert_plan_privacy(
        plan,
        snippets=(
            _CANARY_ENDPOINT_VALUE,
            _CANARY_AUTH_VALUE,
            _CANARY_ENV_NAME,
            _CANARY_ENV_VALUE,
        ),
    )


def test_build_cursor_argv_disables_auto_update_before_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path / "unused-path")
    (tmp_path / "unused-path").mkdir()
    _install_spawn_guard(monkeypatch)
    target = _abspath(roots.target_root)
    executable = _abspath(roots.executable)
    workspace = f"--workspace={target}"
    model = "synthetic-oss5-model"

    omitted = _plan(roots)
    supplied = _plan(roots, model=model)

    _require_auto_update_suppression_before_workspace(
        tuple(omitted.argv),
        executable=executable,
        workspace=workspace,
    )
    _require_auto_update_suppression_before_workspace(
        tuple(supplied.argv),
        executable=executable,
        workspace=workspace,
        model=model,
    )


def test_build_cursor_argv_appends_nonblank_model_as_opaque_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)
    model = "synthetic-oss5-model"

    plan = _plan(roots, model=model)

    assert plan.argv[-1] == f"--model={model}"
    assert _workspace_argument(plan.argv, model=model).startswith("--workspace=")
    assert _flag_name(plan.argv[-1]) == "--model"
    _assert_no_forbidden_flags(tuple(plan.argv))


def test_build_cursor_argv_omits_blank_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    omitted = _plan(roots)
    empty = _plan(roots, model="")
    whitespace = _plan(roots, model=" \t")
    newline = _plan(roots, model="\n")
    carriage = _plan(roots, model="\r")
    mixed = _plan(roots, model=" \n\t\r ")

    for plan in (omitted, empty, whitespace, newline, carriage, mixed):
        names = [_flag_name(item) for item in plan.argv]
        assert "--model" not in names
        assert plan.argv == omitted.argv
        assert _workspace_argument(plan.argv, model=None).startswith("--workspace=")
        _assert_no_forbidden_flags(tuple(plan.argv))


def test_build_cursor_argv_omits_forbidden_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    plan = _plan(roots, model="synthetic-oss5-model")

    _assert_no_forbidden_flags(tuple(plan.argv))
    assert "--endpoint" not in " ".join(plan.argv)
    assert "--api-key" not in " ".join(plan.argv)


def test_build_cursor_argv_sets_shell_false_and_stdin_devnull(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    plan = _plan(roots)

    assert plan.shell is False
    assert type(plan.shell) is bool
    assert plan.stdin is subprocess.DEVNULL


def test_build_cursor_argv_keeps_shell_metacharacters_as_argv_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    injected_target = tmp_path / _INJECTION
    injected_target.mkdir()
    roots.target_root = injected_target
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    plan = _plan(roots, model=_INJECTION)

    assert plan.shell is False
    assert plan.stdin is subprocess.DEVNULL
    assert plan.cwd == _abspath(injected_target)
    workspace = _workspace_argument(plan.argv, model=_INJECTION)
    assert workspace == f"--workspace={_abspath(injected_target)}"
    assert plan.argv[-1] == f"--model={_INJECTION}"
    assert plan.argv.count(_INJECTION) == 0
    assert _INJECTION in workspace
    assert _INJECTION in plan.argv[-1]
    _assert_no_forbidden_flags(tuple(plan.argv))


def test_build_cursor_argv_rejects_cursor_api_endpoint_without_environment_enumeration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    monkeypatch.setenv("CURSOR_API_ENDPOINT", _CANARY_ENDPOINT_VALUE)
    monkeypatch.setenv(_CANARY_ENV_NAME, _CANARY_ENV_VALUE)
    monkeypatch.setenv("CURSOR_API_KEY", _CANARY_AUTH_VALUE)
    violations = _install_environ_guard(monkeypatch)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots),
        snippets=(
            _CANARY_ENDPOINT_VALUE,
            _CANARY_AUTH_VALUE,
            _CANARY_ENV_NAME,
            _CANARY_ENV_VALUE,
        ),
    )
    assert violations == []


def test_build_cursor_argv_rejects_empty_cursor_api_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    monkeypatch.setenv("CURSOR_API_ENDPOINT", "")
    violations = _install_environ_guard(monkeypatch)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots),
        snippets=(
            _CANARY_ENDPOINT_VALUE,
            _CANARY_AUTH_VALUE,
            _CANARY_ENV_NAME,
            _CANARY_ENV_VALUE,
        ),
    )
    assert violations == []
    assert os.environ.get("CURSOR_API_ENDPOINT") == ""


def test_build_cursor_argv_does_not_project_or_copy_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    monkeypatch.setenv(_CANARY_ENV_NAME, _CANARY_ENV_VALUE)
    violations = _install_environ_guard(monkeypatch)
    _install_spawn_guard(monkeypatch)

    plan = _plan(roots)

    assert violations == []
    _assert_plan_privacy(
        plan,
        snippets=(
            _CANARY_ENDPOINT_VALUE,
            _CANARY_AUTH_VALUE,
            _CANARY_ENV_NAME,
            _CANARY_ENV_VALUE,
        ),
    )
    assert plan.shell is False
    assert plan.stdin is subprocess.DEVNULL


def test_build_cursor_argv_rejects_missing_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    missing = tmp_path / "missing-target"
    roots.target_root = missing
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots),
        snippets=roots.snippets(),
    )


def test_build_cursor_argv_rejects_non_directory_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    file_target = tmp_path / "file-target"
    file_target.write_bytes(b"")
    roots.target_root = file_target
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots),
        snippets=roots.snippets(),
    )


def test_build_cursor_argv_rejects_target_equal_to_or_below_runner_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    equal = _Roots(
        runtime_root=roots.runtime_root,
        target_root=roots.runner_source_root,
        runner_source_root=roots.runner_source_root,
        executable=roots.executable,
    )
    nested = roots.runner_source_root / "nested-target"
    nested.mkdir()
    below = _Roots(
        runtime_root=roots.runtime_root,
        target_root=nested,
        runner_source_root=roots.runner_source_root,
        executable=roots.executable,
    )

    _expect_reason(_REASON_WORKSPACE, lambda: _plan(equal), snippets=equal.snippets())
    _expect_reason(_REASON_WORKSPACE, lambda: _plan(below), snippets=below.snippets())


def test_build_cursor_argv_rejects_target_equal_to_or_below_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    equal = _Roots(
        runtime_root=roots.runtime_root,
        target_root=roots.runtime_root,
        runner_source_root=roots.runner_source_root,
        executable=roots.executable,
    )
    nested = roots.runtime_root / "nested-target"
    nested.mkdir()
    below = _Roots(
        runtime_root=roots.runtime_root,
        target_root=nested,
        runner_source_root=roots.runner_source_root,
        executable=roots.executable,
    )

    _expect_reason(_REASON_WORKSPACE, lambda: _plan(equal), snippets=equal.snippets())
    _expect_reason(_REASON_WORKSPACE, lambda: _plan(below), snippets=below.snippets())


def test_build_cursor_argv_rejects_source_or_runtime_equal_to_or_below_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    source_equal = _Roots(
        runtime_root=roots.runtime_root,
        target_root=roots.target_root,
        runner_source_root=roots.target_root,
        executable=roots.executable,
    )
    source_nested = roots.target_root / "nested-source"
    source_nested.mkdir()
    source_below = _Roots(
        runtime_root=roots.runtime_root,
        target_root=roots.target_root,
        runner_source_root=source_nested,
        executable=roots.executable,
    )
    runtime_equal = _Roots(
        runtime_root=roots.target_root,
        target_root=roots.target_root,
        runner_source_root=roots.runner_source_root,
        executable=roots.executable,
    )
    runtime_nested = roots.target_root / "nested-runtime"
    runtime_nested.mkdir()
    runtime_below = _Roots(
        runtime_root=runtime_nested,
        target_root=roots.target_root,
        runner_source_root=roots.runner_source_root,
        executable=roots.executable,
    )

    _expect_reason(_REASON_WORKSPACE, lambda: _plan(source_equal), snippets=source_equal.snippets())
    _expect_reason(_REASON_WORKSPACE, lambda: _plan(source_below), snippets=source_below.snippets())
    _expect_reason(_REASON_WORKSPACE, lambda: _plan(runtime_equal), snippets=runtime_equal.snippets())
    _expect_reason(_REASON_WORKSPACE, lambda: _plan(runtime_below), snippets=runtime_below.snippets())


def test_build_cursor_argv_rejects_nul_in_executable_model_or_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)
    executable = _abspath(roots.executable)
    target = _abspath(roots.target_root)
    source = _abspath(roots.runner_source_root)
    runtime = _abspath(roots.runtime_root)

    _expect_reason(
        _REASON_INVALID,
        lambda: resolve_cursor_executable(configured_executable=executable + "\0x"),
        snippets=roots.snippets(),
    )
    _expect_reason(
        _REASON_INVALID,
        lambda: build_cursor_argv(
            executable_path=executable + "\0x",
            target_root=roots.target_root,
            runner_source_root=roots.runner_source_root,
            runtime_root=roots.runtime_root,
        ),
        snippets=roots.snippets(),
    )
    _expect_reason(
        _REASON_INVALID,
        lambda: build_cursor_argv(
            executable_path=executable,
            target_root=roots.target_root,
            runner_source_root=roots.runner_source_root,
            runtime_root=roots.runtime_root,
            model="synthetic\0model",
        ),
        snippets=roots.snippets(),
    )
    _expect_reason(
        _REASON_INVALID,
        lambda: build_cursor_argv(
            executable_path=executable,
            target_root=target + "\0nested",
            runner_source_root=roots.runner_source_root,
            runtime_root=roots.runtime_root,
        ),
        snippets=roots.snippets(),
    )
    _expect_reason(
        _REASON_INVALID,
        lambda: build_cursor_argv(
            executable_path=executable,
            target_root=roots.target_root,
            runner_source_root=source + "\0nested",
            runtime_root=roots.runtime_root,
        ),
        snippets=roots.snippets(),
    )
    _expect_reason(
        _REASON_INVALID,
        lambda: build_cursor_argv(
            executable_path=executable,
            target_root=roots.target_root,
            runner_source_root=roots.runner_source_root,
            runtime_root=runtime + "\0nested",
        ),
        snippets=roots.snippets(),
    )


def test_build_cursor_argv_rejects_symlink_component_in_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    real_target = tmp_path / "real-target"
    real_target.mkdir()
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_target)
    roots.target_root = link_parent
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots),
        snippets=(_abspath(real_target),),
    )


def test_build_cursor_argv_rejects_symlink_in_target_parent_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    nested_target = real_parent / "nested-target"
    nested_target.mkdir()
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent)
    roots.target_root = link_parent / "nested-target"
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    assert not roots.target_root.is_symlink()
    assert link_parent.is_symlink()
    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots),
        snippets=(_abspath(nested_target),),
    )


def test_build_cursor_argv_rejects_relative_executable_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots, executable_path=roots.executable.name),
        snippets=roots.snippets(),
    )
    _expect_reason(
        _REASON_INVALID,
        lambda: _plan(roots, executable_path=str(Path("bin") / _ABS_BIN_NAME)),
        snippets=roots.snippets(),
    )


def test_build_cursor_argv_accepts_string_prefix_sibling_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime-root"
    target_root = tmp_path / "runtime-root-target"
    runner_source_root = tmp_path / "runtime-root-source"
    runtime_root.mkdir()
    target_root.mkdir()
    runner_source_root.mkdir()
    executable = _write_executable(tmp_path / "bin" / _ABS_BIN_NAME)
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
        executable=executable,
    )
    _isolate_path(monkeypatch, tmp_path)
    _install_spawn_guard(monkeypatch)

    plan = _plan(roots)

    assert plan.cwd == _abspath(target_root)
    assert _workspace_argument(plan.argv, model=None) == f"--workspace={_abspath(target_root)}"
    assert plan.shell is False
    assert plan.stdin is subprocess.DEVNULL


def test_invocation_helpers_do_not_spawn_a_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    path_dir = tmp_path / "path"
    _write_executable(path_dir / _PATH_BIN_NAME)
    _isolate_path(monkeypatch, path_dir)
    _install_spawn_guard(monkeypatch)

    absolute = _resolved_absolute(roots.executable)
    path_resolved = resolve_cursor_executable(configured_executable=_PATH_BIN_NAME)
    plan = _plan(roots, executable_path=absolute.path)

    assert absolute.resolution_mode == _MODE_ABSOLUTE
    assert path_resolved.resolution_mode == _MODE_PATH
    assert plan.shell is False
    assert plan.stdin is subprocess.DEVNULL
    assert plan.cwd == _abspath(roots.target_root)
