"""Reference-mode steady-state code authority.

OSS6-B20 binds the running entry point to one Git toplevel, one full
lowercase HEAD SHA, and a clean source checkout. OSS6-B21 keeps core
imports under that source root, rejects a target shadow, and requires
source, target, and runtime roots to be pairwise disjoint. OSS6-B22
binds a managed runtime to one persisted source SHA, including when a
terminal-rest marker is present, and blocks a different source SHA.

The source root is derived from the entry-point file with
``git rev-parse --show-toplevel``. It is not the package directory and
not a caller-supplied root. An absent runtime with no pin and no
``main_state.yaml`` file is unmanaged: verification must not create the
pin. Creating the pin is a separate persist call. A present pin or a
``main_state.yaml`` file makes the runtime managed.

``verify_steady_state_code_authority`` fails closed in this order:
source-root derivation, observed HEAD shape, expected-SHA shape,
expected-SHA mismatch, root separation, source cleanliness, import
provenance, then the persisted pin. Within imports, an invalid core
name or a missing file is provenance failure; a resolved file inside
the target root is a shadow. A pin file is validated before a missing
pin is reported. Verification does not interpret ``main_state.yaml``
bytes and does not mutate Git or runtime bytes.

A clean detached source checkout at the same full HEAD SHA stays
authorized. Branch attachment is not authority. Separate linked
worktrees may share one Git object database when their resolved
worktree roots stay pairwise disjoint. Same-object-db is neither
required nor a prohibition. Path resolution is authoritative. An entry
symlink under the source path whose target leaves that checkout is
``SOURCE_AUTHORITY_SOURCE_ROOT_INVALID``. The same code applies when
that resolved target is a file inside the target checkout: the target
repository does not become source authority. A symlink under source
whose resolved target stays inside the same source checkout remains
authorized, and the report carries that resolved in-source path. A
core-import symlink under source that resolves outside it is
``SOURCE_AUTHORITY_IMPORT_PROVENANCE_INVALID``; one that resolves
inside the target is ``SOURCE_AUTHORITY_IMPORT_SHADOWED``. A full
uppercase ``source_sha`` in the runtime pin is
``RUNTIME_SOURCE_IDENTITY_INVALID`` and is left byte-identical.

Not accepted as authority: source ancestry over the target,
``REVIEWED_BOOTSTRAP_PIN``, successor dispatch, attestation, a
migration contract, or tamper isolation. This module does not prove
init history, status projection, or command composition.
"""

from __future__ import annotations

import dataclasses
import inspect
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping

import pytest
import yaml

from task_transition_runner.runner_code_authority import (
    MANAGED_RUNTIME_MARKER_NAME,
    RUNTIME_SOURCE_PIN_NAME,
    RunnerCodeAuthorityError,
    SourceAuthorityReport,
    collect_loaded_core_module_paths,
    derive_source_root,
    persist_runtime_source_pin,
    verify_steady_state_code_authority,
)


_GIT_AUTHOR_NAME = "Code Authority Contract"
_GIT_AUTHOR_EMAIL = "code-authority@task-transition-runner.invalid"
_SCHEMA_VERSION = "1.0"
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
_REPORT_FIELDS = (
    "source_root",
    "source_sha",
    "entry_point",
    "target_root",
    "runtime_root",
    "runtime_pin_sha",
    "loaded_core_modules",
)
_DERIVE_PARAMETERS = ("entry_point",)
_COLLECT_PARAMETERS = ()
_PERSIST_PARAMETERS = ("runtime_root", "source_sha")
_VERIFY_PARAMETERS = (
    "entry_point",
    "target_root",
    "runtime_root",
    "expected_source_sha",
    "loaded_module_files",
)
_PUBLIC_CALLABLES = (
    derive_source_root,
    collect_loaded_core_module_paths,
    persist_runtime_source_pin,
    verify_steady_state_code_authority,
)
_DROPPED_SOURCE_MARKERS = (
    "REVIEWED_BOOTSTRAP_PIN",
    "MAIN_PIN",
    "pinned_runner_bootstrap",
    "successor_dispatch",
    "CheckoutPhase",
    "AuthorityMode",
    "WORKTREE_OBJECT_DATABASE_MISMATCH",
    "merge-base",
    "projects/AETHELGARD",
    "authorization_digest",
    "log --all",
    "rev-list --all",
    "HYDRA_GAIA",
)
_FORBIDDEN_PARAMETER_TOKENS = (
    "bootstrap",
    "successor",
    "ancestor",
    "ancestry",
    "migration",
    "attestation",
    "object_database",
    "common_dir",
    "checkout_phase",
    "authority_mode",
)


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True)
    subprocess.run(  # noqa: S603
        ["git", "-c", "init.defaultBranch=main", "init"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    )
    _git(path, "config", "user.name", _GIT_AUTHOR_NAME)
    _git(path, "config", "user.email", _GIT_AUTHOR_EMAIL)


def _commit_all(repo: Path, message: str) -> str:
    _git(repo, "add", "--all")
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _common_dir(repo: Path) -> Path:
    reported = Path(_git(repo, "rev-parse", "--git-common-dir"))
    if reported.is_absolute():
        return reported.resolve()
    return (repo / reported).resolve()


def _write_source_tree(source: Path) -> tuple[Path, Path]:
    package = source / "src" / "task_transition_runner"
    package.mkdir(parents=True)
    entry = package / "cli.py"
    core = package / "config.py"
    entry.write_text("# synthetic entry\n", encoding="utf-8")
    core.write_text("# synthetic core\n", encoding="utf-8")
    return entry, core


def _pin_text(source_sha: str, *, schema_version: str = _SCHEMA_VERSION) -> str:
    return f"schema_version: {schema_version!r}\nsource_sha: {source_sha!r}\n"


def _write_pin(runtime: Path, source_sha: str) -> Path:
    path = runtime / RUNTIME_SOURCE_PIN_NAME
    path.write_text(_pin_text(source_sha), encoding="utf-8")
    return path


def _topology(tmp_path: Path) -> dict[str, Any]:
    source = tmp_path / "source"
    target = tmp_path / "target"
    runtime = tmp_path / "runtime"
    _init_repo(source)
    entry, core = _write_source_tree(source)
    source_sha = _commit_all(source, "source baseline")
    _init_repo(target)
    (target / "project.txt").write_text("target baseline\n", encoding="utf-8")
    target_sha = _commit_all(target, "target baseline")
    runtime.mkdir()
    return {
        "source": source,
        "target": target,
        "runtime": runtime,
        "entry": entry,
        "core": core,
        "source_sha": source_sha,
        "target_sha": target_sha,
    }


def _modules(topology: Mapping[str, Any]) -> dict[str, Path]:
    return {"task_transition_runner.config": topology["core"]}


def _verify(
    topology: Mapping[str, Any],
    **overrides: Any,
) -> SourceAuthorityReport:
    arguments: dict[str, Any] = {
        "entry_point": topology["entry"],
        "target_root": topology["target"],
        "runtime_root": topology["runtime"],
        "loaded_module_files": _modules(topology),
    }
    arguments.update(overrides)
    return verify_steady_state_code_authority(**arguments)


def _assert_blocked(operation: Callable[[], Any], reason_code: str) -> None:
    with pytest.raises(RunnerCodeAuthorityError) as exc_info:
        operation()
    assert exc_info.value.reason_code == reason_code
    assert str(exc_info.value) == reason_code


def _runtime_files(runtime: Path) -> dict[str, bytes]:
    if not runtime.exists():
        return {}
    return {
        str(path.relative_to(runtime)): path.read_bytes()
        for path in sorted(runtime.rglob("*"))
        if path.is_file()
    }


def test_public_surface_is_reference_mode_only() -> None:
    assert RUNTIME_SOURCE_PIN_NAME == "runner_source_pin.yaml"
    assert MANAGED_RUNTIME_MARKER_NAME == "main_state.yaml"
    assert issubclass(RunnerCodeAuthorityError, Exception)
    assert dataclasses.is_dataclass(SourceAuthorityReport)
    assert SourceAuthorityReport.__dataclass_params__.frozen is True
    assert tuple(
        field.name for field in dataclasses.fields(SourceAuthorityReport)
    ) == _REPORT_FIELDS

    derive_signature = inspect.signature(derive_source_root)
    assert tuple(derive_signature.parameters) == _DERIVE_PARAMETERS
    collect_signature = inspect.signature(collect_loaded_core_module_paths)
    assert tuple(collect_signature.parameters) == _COLLECT_PARAMETERS
    persist_signature = inspect.signature(persist_runtime_source_pin)
    assert tuple(persist_signature.parameters) == _PERSIST_PARAMETERS
    verify_signature = inspect.signature(verify_steady_state_code_authority)
    assert tuple(verify_signature.parameters) == _VERIFY_PARAMETERS
    assert verify_signature.parameters["expected_source_sha"].default is None
    assert verify_signature.parameters["loaded_module_files"].default is None
    for function in (
        persist_runtime_source_pin,
        verify_steady_state_code_authority,
    ):
        for parameter in inspect.signature(function).parameters.values():
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    for function in _PUBLIC_CALLABLES:
        names = " ".join(inspect.signature(function).parameters)
        for token in _FORBIDDEN_PARAMETER_TOKENS:
            assert token not in names

    source_text = Path(verify_steady_state_code_authority.__code__.co_filename).read_text(
        encoding="utf-8"
    )
    for marker in _DROPPED_SOURCE_MARKERS:
        assert marker not in source_text


def test_entry_point_derives_git_toplevel_not_package_directory(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    source = topology["source"]
    entry = topology["entry"]

    derived = derive_source_root(entry)

    assert derived == source.resolve()
    assert derived != entry.parent.resolve()
    assert _git(source, "symbolic-ref", "--short", "HEAD") == "main"
    assert _common_dir(source) != _common_dir(topology["target"])
    merge_base = subprocess.run(  # noqa: S603
        ["git", "merge-base", topology["source_sha"], topology["target_sha"]],
        cwd=source,
        capture_output=True,
        text=True,
    )
    assert merge_base.returncode != 0


def test_clean_attached_source_authorizes_independent_unrelated_target(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    before_runtime = _runtime_files(topology["runtime"])
    head_before = topology["source_sha"]

    report = _verify(
        topology,
        expected_source_sha=head_before,
        loaded_module_files={
            "task_transition_runner.cli": topology["entry"],
            "task_transition_runner.config": str(topology["core"]),
        },
    )

    assert isinstance(report, SourceAuthorityReport)
    assert report.source_root == topology["source"].resolve()
    assert report.source_sha == head_before
    assert len(report.source_sha) == 40
    assert report.source_sha == report.source_sha.lower()
    assert report.entry_point == topology["entry"].resolve()
    assert report.target_root == topology["target"].resolve()
    assert report.runtime_root == topology["runtime"].resolve()
    assert report.runtime_pin_sha is None
    assert report.loaded_core_modules == (
        "task_transition_runner.cli",
        "task_transition_runner.config",
    )
    assert _git(topology["source"], "rev-parse", "HEAD") == head_before
    assert _git(topology["source"], "status", "--porcelain", "--untracked-files=all") == ""
    assert _runtime_files(topology["runtime"]) == before_runtime
    assert not (topology["runtime"] / RUNTIME_SOURCE_PIN_NAME).exists()


def test_detached_source_checkout_remains_authorized(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    source = topology["source"]
    source_sha = topology["source_sha"]
    _git(source, "checkout", "--detach", source_sha)
    symbolic_ref = subprocess.run(  # noqa: S603
        ["git", "symbolic-ref", "-q", "HEAD"],
        cwd=source,
        capture_output=True,
        text=True,
    )

    report = _verify(topology, expected_source_sha=source_sha)

    assert symbolic_ref.returncode != 0
    assert _git(source, "rev-parse", "HEAD") == source_sha
    assert _git(source, "status", "--porcelain", "--untracked-files=all") == ""
    assert report.source_root == source.resolve()
    assert report.source_sha == source_sha
    assert report.entry_point == topology["entry"].resolve()
    assert report.runtime_pin_sha is None


def test_linked_worktrees_share_one_object_database_and_remain_authorized(
    tmp_path: Path,
) -> None:
    primary = tmp_path / "primary"
    _init_repo(primary)
    _write_source_tree(primary)
    source_sha = _commit_all(primary, "shared baseline")
    _git(primary, "branch", "source-line")
    _git(primary, "branch", "target-line")
    source = tmp_path / "source"
    target = tmp_path / "target"
    runtime = tmp_path / "runtime"
    _git(primary, "worktree", "add", str(source), "source-line")
    _git(primary, "worktree", "add", str(target), "target-line")
    runtime.mkdir()
    entry = source / "src" / "task_transition_runner" / "cli.py"
    core = source / "src" / "task_transition_runner" / "config.py"

    report = verify_steady_state_code_authority(
        entry_point=entry,
        target_root=target,
        runtime_root=runtime,
        expected_source_sha=source_sha,
        loaded_module_files={"task_transition_runner.config": core},
    )

    assert _common_dir(source) == _common_dir(target)
    assert source.resolve() != target.resolve()
    assert not source.resolve().is_relative_to(target.resolve())
    assert not target.resolve().is_relative_to(source.resolve())
    assert _git(source, "symbolic-ref", "--short", "HEAD") == "source-line"
    assert _git(target, "symbolic-ref", "--short", "HEAD") == "target-line"
    assert report.source_root == source.resolve()
    assert report.source_sha == source_sha
    assert report.target_root == target.resolve()
    assert report.runtime_root == runtime.resolve()
    assert report.runtime_pin_sha is None
    assert report.loaded_core_modules == ("task_transition_runner.config",)


def test_unmanaged_absent_runtime_does_not_require_or_create_a_pin(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    absent_runtime = tmp_path / "absent-runtime"

    report = _verify(topology, runtime_root=absent_runtime)

    assert report.runtime_root == absent_runtime.resolve()
    assert report.runtime_pin_sha is None
    assert not absent_runtime.exists()


def test_derive_source_root_ignores_cleanliness(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    (topology["source"] / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    assert derive_source_root(topology["entry"]) == topology["source"].resolve()


@pytest.mark.parametrize(
    "entry_kind",
    ["missing", "directory", "outside_git"],
)
def test_source_root_rejects_non_entry_point(tmp_path: Path, entry_kind: str) -> None:
    topology = _topology(tmp_path)
    if entry_kind == "missing":
        entry = topology["source"] / "missing.py"
    elif entry_kind == "directory":
        entry = topology["source"]
    else:
        entry = tmp_path / "outside.py"
        entry.write_text("outside\n", encoding="utf-8")

    _assert_blocked(lambda: derive_source_root(entry), _SOURCE_ROOT_INVALID)
    _assert_blocked(
        lambda: _verify(topology, entry_point=entry, expected_source_sha="main"),
        _SOURCE_ROOT_INVALID,
    )


@pytest.mark.parametrize(
    "expected_source_sha",
    [
        "main",
        "abc",
        "0123456789abcdef0123456789abcdef0123456",
        "0123456789abcdef0123456789abcdef012345678",
        "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG",
        "",
    ],
)
def test_expected_source_sha_must_be_full_lowercase_hex(
    tmp_path: Path,
    expected_source_sha: str,
) -> None:
    topology = _topology(tmp_path)
    head_before = topology["source_sha"]
    (topology["source"] / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    nested_runtime = topology["source"] / "nested-runtime"
    nested_runtime.mkdir()

    _assert_blocked(
        lambda: _verify(
            topology,
            expected_source_sha=expected_source_sha,
            runtime_root=nested_runtime,
        ),
        _SOURCE_SHA_INVALID,
    )
    assert _git(topology["source"], "rev-parse", "HEAD") == head_before


def test_uppercase_full_sha_is_invalid_even_when_letters_match(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    expected_source_sha = ("a" + topology["source_sha"][1:]).upper()

    _assert_blocked(
        lambda: _verify(topology, expected_source_sha=expected_source_sha),
        _SOURCE_SHA_INVALID,
    )


def test_expected_source_sha_mismatch_precedes_overlap_dirt_and_pin(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    _write_pin(topology["runtime"], topology["source_sha"])
    (topology["source"] / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    _assert_blocked(
        lambda: _verify(
            topology,
            expected_source_sha=topology["target_sha"],
            target_root=topology["source"],
        ),
        _SOURCE_SHA_MISMATCH,
    )


def test_root_overlap_precedes_dirty_source(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    nested_runtime = topology["source"] / "nested-runtime"
    nested_runtime.mkdir()

    _assert_blocked(
        lambda: _verify(topology, runtime_root=nested_runtime),
        _ROOT_OVERLAP,
    )


@pytest.mark.parametrize(
    "overlap",
    [
        "target_equals_source",
        "runtime_equals_source",
        "runtime_equals_target",
        "target_inside_source",
        "source_inside_runtime",
        "source_inside_target",
        "target_missing",
        "runtime_is_file",
    ],
)
def test_pairwise_root_separation_fails_closed(tmp_path: Path, overlap: str) -> None:
    topology = _topology(tmp_path)
    overrides: dict[str, Any] = {}
    if overlap == "target_equals_source":
        overrides["target_root"] = topology["source"]
    elif overlap == "runtime_equals_source":
        overrides["runtime_root"] = topology["source"]
    elif overlap == "runtime_equals_target":
        overrides["runtime_root"] = topology["target"]
    elif overlap == "target_inside_source":
        nested_target = topology["source"] / "nested-target"
        nested_target.mkdir()
        overrides["target_root"] = nested_target
    elif overlap == "source_inside_runtime":
        runtime = tmp_path / "outer-runtime"
        runtime.mkdir()
        source = runtime / "source"
        _init_repo(source)
        entry, core = _write_source_tree(source)
        _commit_all(source, "nested source")
        overrides = {
            "entry_point": entry,
            "target_root": topology["target"],
            "runtime_root": runtime,
            "loaded_module_files": {"task_transition_runner.config": core},
        }
    elif overlap == "source_inside_target":
        target = tmp_path / "outer-target"
        target.mkdir()
        source = target / "source"
        _init_repo(source)
        entry, core = _write_source_tree(source)
        _commit_all(source, "source inside target")
        overrides = {
            "entry_point": entry,
            "target_root": target,
            "runtime_root": topology["runtime"],
            "loaded_module_files": {"task_transition_runner.config": core},
        }
    elif overlap == "target_missing":
        overrides["target_root"] = tmp_path / "missing-target"
    else:
        runtime_file = tmp_path / "runtime-file"
        runtime_file.write_text("not a directory\n", encoding="utf-8")
        overrides["runtime_root"] = runtime_file

    _assert_blocked(lambda: _verify(topology, **overrides), _ROOT_OVERLAP)


@pytest.mark.parametrize(
    "dirty_kind",
    ["untracked", "tracked_modification", "staged"],
)
def test_dirty_source_blocks_after_root_separation(
    tmp_path: Path,
    dirty_kind: str,
) -> None:
    topology = _topology(tmp_path)
    source = topology["source"]
    head_before = topology["source_sha"]
    if dirty_kind == "untracked":
        (source / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    elif dirty_kind == "tracked_modification":
        tracked = source / "src" / "task_transition_runner" / "config.py"
        tracked.write_text("# drifted core\n", encoding="utf-8")
    else:
        (source / "staged.txt").write_text("staged\n", encoding="utf-8")
        _git(source, "add", "--", "staged.txt")
    outside = tmp_path / "outside-module.py"
    outside.write_text("outside\n", encoding="utf-8")

    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={"task_transition_runner.config": outside},
        ),
        _SOURCE_DIRTY,
    )
    assert _git(source, "rev-parse", "HEAD") == head_before


def test_collect_loaded_core_modules_ignores_non_core_and_missing_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "task_transition_runner._contract_without_file",
        SimpleNamespace(),
    )
    monkeypatch.setitem(
        sys.modules,
        "not_core.with_file",
        SimpleNamespace(__file__=__file__),
    )
    monkeypatch.setitem(
        sys.modules,
        "task_transition_runner_evil",
        SimpleNamespace(__file__=__file__),
    )

    loaded = collect_loaded_core_module_paths()

    assert "task_transition_runner._contract_without_file" not in loaded
    assert "not_core.with_file" not in loaded
    assert "task_transition_runner_evil" not in loaded
    assert "task_transition_runner.runner_code_authority" in loaded
    for name, path in loaded.items():
        assert name == "task_transition_runner" or name.startswith(
            "task_transition_runner."
        )
        assert path.is_file()


def _hide_loaded_core_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(sys.modules):
        if name == "task_transition_runner" or name.startswith(
            "task_transition_runner."
        ):
            monkeypatch.delitem(sys.modules, name, raising=False)


def test_absent_inventory_uses_only_loaded_core_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topology = _topology(tmp_path)
    _hide_loaded_core_modules(monkeypatch)
    monkeypatch.setitem(
        sys.modules,
        "task_transition_runner.config",
        SimpleNamespace(__file__=str(topology["core"])),
    )

    report = _verify(topology, loaded_module_files=None)

    assert report.loaded_core_modules == ("task_transition_runner.config",)


def test_empty_loaded_inventory_is_provenance_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topology = _topology(tmp_path)

    _assert_blocked(
        lambda: _verify(topology, loaded_module_files={}),
        _IMPORT_PROVENANCE_INVALID,
    )
    _hide_loaded_core_modules(monkeypatch)
    _assert_blocked(
        lambda: _verify(topology, loaded_module_files=None),
        _IMPORT_PROVENANCE_INVALID,
    )


@pytest.mark.parametrize(
    "module_name",
    [
        "json",
        "task_transition_runner_evil",
        "task_transition_runner.",
        "task_transition_runner..config",
        "Task_transition_runner.config",
    ],
)
def test_non_core_import_name_is_provenance_failure(
    tmp_path: Path,
    module_name: str,
) -> None:
    topology = _topology(tmp_path)

    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={module_name: topology["core"]},
        ),
        _IMPORT_PROVENANCE_INVALID,
    )


def test_core_file_outside_source_is_provenance_failure(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    outside = tmp_path / "outside" / "config.py"
    outside.parent.mkdir()
    outside.write_text("# outside\n", encoding="utf-8")
    missing = topology["source"] / "src" / "task_transition_runner" / "missing.py"

    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={"task_transition_runner.config": outside},
        ),
        _IMPORT_PROVENANCE_INVALID,
    )
    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={"task_transition_runner.config": missing},
        ),
        _IMPORT_PROVENANCE_INVALID,
    )


def test_core_file_under_source_outside_package_directory_is_authorized(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    extra = topology["source"] / "pkg" / "extra.py"
    extra.parent.mkdir()
    extra.write_text("# extra core\n", encoding="utf-8")
    _commit_all(topology["source"], "extra core")
    topology["source_sha"] = _git(topology["source"], "rev-parse", "HEAD")

    report = _verify(
        topology,
        loaded_module_files={"task_transition_runner.extra": extra},
    )

    assert report.source_sha == topology["source_sha"]
    assert report.loaded_core_modules == ("task_transition_runner.extra",)


def test_target_import_shadow_precedes_pin(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    shadow = topology["target"] / "src" / "task_transition_runner" / "config.py"
    shadow.parent.mkdir(parents=True)
    shadow.write_text("# shadow\n", encoding="utf-8")
    _write_pin(topology["runtime"], "0123456789abcdef0123456789abcdef01234567")

    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={"task_transition_runner.config": shadow},
        ),
        _IMPORT_SHADOWED,
    )
    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={"json": shadow},
        ),
        _IMPORT_PROVENANCE_INVALID,
    )


@pytest.mark.parametrize(
    ("link_kind", "reason_code"),
    [
        ("entry_leaves_source", _SOURCE_ROOT_INVALID),
        ("entry_resolves_inside_target", _SOURCE_ROOT_INVALID),
        ("core_leaves_source", _IMPORT_PROVENANCE_INVALID),
        ("core_resolves_inside_target", _IMPORT_SHADOWED),
    ],
)
def test_resolved_symlink_targets_classify_source_and_import_authority(
    tmp_path: Path,
    link_kind: str,
    reason_code: str,
) -> None:
    topology = _topology(tmp_path)
    source = topology["source"]
    target = topology["target"]
    if link_kind == "core_resolves_inside_target":
        destination = target / "shadowed_core.py"
        destination.write_text("# shadowed\n", encoding="utf-8")
    elif link_kind == "entry_resolves_inside_target":
        destination = target / "washed_entry.py"
        destination.write_text("# target entry\n", encoding="utf-8")
    else:
        destination = tmp_path / "outside" / "escaped.py"
        destination.parent.mkdir()
        destination.write_text("# escaped\n", encoding="utf-8")
    link_name = (
        "escaped_entry.py" if link_kind.startswith("entry_") else "linked_config.py"
    )
    link = source / "src" / "task_transition_runner" / link_name
    link.symlink_to(destination)
    resolved = link.resolve()
    _commit_all(source, "symlink fixture")

    assert link.is_symlink()
    assert resolved == destination.resolve()
    assert not resolved.is_relative_to(source.resolve())
    assert _git(source, "status", "--porcelain", "--untracked-files=all") == ""
    if link_kind == "entry_leaves_source":
        _assert_blocked(
            lambda: _verify(topology, entry_point=link),
            reason_code,
        )
        return
    if link_kind == "entry_resolves_inside_target":
        assert resolved.is_relative_to(target.resolve())
        assert _git(target, "rev-parse", "--show-toplevel") == str(target.resolve())
        _assert_blocked(lambda: derive_source_root(link), reason_code)
        _assert_blocked(
            lambda: _verify(topology, entry_point=link),
            reason_code,
        )
        return
    if link_kind == "core_resolves_inside_target":
        assert resolved.is_relative_to(target.resolve())
    else:
        assert not resolved.is_relative_to(target.resolve())
    _assert_blocked(
        lambda: _verify(
            topology,
            loaded_module_files={"task_transition_runner.config": link},
        ),
        reason_code,
    )


def test_in_source_symlink_entry_remains_authorized(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    source = topology["source"]
    real_entry = topology["entry"]
    link = source / "src" / "task_transition_runner" / "linked_entry.py"
    link.symlink_to(real_entry)
    source_sha = _commit_all(source, "in-source symlink")
    resolved = link.resolve()
    assert link.is_symlink()
    assert resolved == real_entry.resolve()
    assert resolved.is_relative_to(source.resolve())
    assert _git(source, "status", "--porcelain", "--untracked-files=all") == ""

    derived = derive_source_root(link)
    report = _verify(topology, entry_point=link, expected_source_sha=source_sha)

    assert derived == source.resolve()
    assert report.source_root == source.resolve()
    assert report.source_sha == source_sha
    assert report.entry_point == resolved
    assert report.runtime_pin_sha is None


def test_directory_marker_does_not_manage_runtime(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    (topology["runtime"] / MANAGED_RUNTIME_MARKER_NAME).mkdir()

    report = _verify(topology)

    assert report.runtime_pin_sha is None


def test_managed_runtime_without_pin_blocks_without_reading_marker_bytes(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    marker = topology["runtime"] / MANAGED_RUNTIME_MARKER_NAME
    marker.write_bytes(b"TERMINAL_REST\nnot-yaml: [\n")
    before = marker.read_bytes()

    _assert_blocked(lambda: _verify(topology), _PIN_MISSING)
    assert marker.read_bytes() == before
    assert not (topology["runtime"] / RUNTIME_SOURCE_PIN_NAME).exists()


def test_persist_then_verify_pins_the_proven_source_sha(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    persisted = persist_runtime_source_pin(
        runtime_root=topology["runtime"],
        source_sha=topology["source_sha"],
    )
    pin_path = topology["runtime"] / RUNTIME_SOURCE_PIN_NAME
    first_bytes = pin_path.read_bytes()

    assert dict(persisted) == {
        "schema_version": _SCHEMA_VERSION,
        "source_sha": topology["source_sha"],
    }
    assert yaml.safe_load(first_bytes.decode("utf-8")) == dict(persisted)
    again = persist_runtime_source_pin(
        runtime_root=topology["runtime"],
        source_sha=topology["source_sha"],
    )
    assert dict(again) == dict(persisted)
    assert pin_path.read_bytes() == first_bytes

    report = _verify(topology)
    assert report.runtime_pin_sha == topology["source_sha"]
    assert report.source_sha == topology["source_sha"]
    assert pin_path.read_bytes() == first_bytes


def test_terminal_rest_keeps_pin_and_does_not_mutate(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    marker = topology["runtime"] / MANAGED_RUNTIME_MARKER_NAME
    marker.write_bytes(b"TERMINAL_REST\n")
    pin_path = _write_pin(topology["runtime"], topology["source_sha"])
    before_runtime = _runtime_files(topology["runtime"])
    head_before = topology["source_sha"]

    report = _verify(topology)

    assert report.runtime_pin_sha == head_before
    assert report.source_sha == head_before
    assert _runtime_files(topology["runtime"]) == before_runtime
    assert pin_path.read_bytes() == before_runtime[RUNTIME_SOURCE_PIN_NAME]
    assert marker.read_bytes() == b"TERMINAL_REST\n"
    assert _git(topology["source"], "rev-parse", "HEAD") == head_before
    assert _git(topology["source"], "status", "--porcelain", "--untracked-files=all") == ""

    pin_path.unlink()
    _assert_blocked(lambda: _verify(topology), _PIN_MISSING)
    assert marker.read_bytes() == b"TERMINAL_REST\n"
    assert _git(topology["source"], "rev-parse", "HEAD") == head_before


def test_source_change_blocks_even_when_expected_sha_matches_new_head(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    pin_path = _write_pin(topology["runtime"], topology["source_sha"])
    pin_before = pin_path.read_bytes()
    (topology["source"] / "changed.txt").write_text("changed\n", encoding="utf-8")
    new_head = _commit_all(topology["source"], "source change")
    assert new_head != topology["source_sha"]

    _assert_blocked(
        lambda: _verify(topology, expected_source_sha=new_head),
        _PIN_MISMATCH,
    )
    assert pin_path.read_bytes() == pin_before
    assert _git(topology["source"], "rev-parse", "HEAD") == new_head


def test_persist_rejects_a_different_sha_without_rewriting_the_pin(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    pin_path = _write_pin(topology["runtime"], topology["source_sha"])
    pin_before = pin_path.read_bytes()

    _assert_blocked(
        lambda: persist_runtime_source_pin(
            runtime_root=topology["runtime"],
            source_sha=topology["target_sha"],
        ),
        _PIN_MISMATCH,
    )
    assert pin_path.read_bytes() == pin_before


def test_persist_rejects_invalid_sha_and_missing_runtime(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    absent = tmp_path / "missing-runtime"

    _assert_blocked(
        lambda: persist_runtime_source_pin(
            runtime_root=absent,
            source_sha=topology["source_sha"],
        ),
        _PIN_IDENTITY_INVALID,
    )
    assert not absent.exists()
    _assert_blocked(
        lambda: persist_runtime_source_pin(
            runtime_root=topology["runtime"],
            source_sha="main",
        ),
        _SOURCE_SHA_INVALID,
    )
    assert not (topology["runtime"] / RUNTIME_SOURCE_PIN_NAME).exists()


@pytest.mark.parametrize(
    "pin_body",
    [
        "schema_version: '2.0'\nsource_sha: {sha}\n",
        "schema_version: '1.0'\nsource_sha: abc\n",
        "schema_version: '1.0'\nsource_sha: {sha}\nissuer: external\n",
        "schema_version: '1.0'\nsource_sha: {sha}\nsource_sha: {sha}\n",
        "- schema_version\n",
        "",
        "schema_version: '1.0'\nsource_sha: 1\n",
    ],
)
def test_malformed_pin_is_invalid_source_identity(
    tmp_path: Path,
    pin_body: str,
) -> None:
    topology = _topology(tmp_path)
    pin_path = topology["runtime"] / RUNTIME_SOURCE_PIN_NAME
    pin_path.write_text(
        pin_body.format(sha=repr(topology["source_sha"])),
        encoding="utf-8",
    )
    before = pin_path.read_bytes()
    marker = topology["runtime"] / MANAGED_RUNTIME_MARKER_NAME
    marker.write_text("TERMINAL_REST\n", encoding="utf-8")

    _assert_blocked(lambda: _verify(topology), _PIN_IDENTITY_INVALID)
    assert pin_path.read_bytes() == before


def test_non_utf8_or_directory_pin_is_invalid_source_identity(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    pin_path = topology["runtime"] / RUNTIME_SOURCE_PIN_NAME
    pin_path.write_bytes(b"\xff")

    _assert_blocked(lambda: _verify(topology), _PIN_IDENTITY_INVALID)

    pin_path.unlink()
    pin_path.mkdir()
    _assert_blocked(lambda: _verify(topology), _PIN_IDENTITY_INVALID)
    assert pin_path.is_dir()
    before_runtime = _runtime_files(topology["runtime"])

    _assert_blocked(
        lambda: persist_runtime_source_pin(
            runtime_root=topology["runtime"],
            source_sha=topology["source_sha"],
        ),
        _PIN_IDENTITY_INVALID,
    )
    assert pin_path.is_dir()
    assert _runtime_files(topology["runtime"]) == before_runtime


def test_valid_different_pin_blocks_before_missing_marker_semantics(
    tmp_path: Path,
) -> None:
    topology = _topology(tmp_path)
    other_sha = topology["target_sha"]
    assert other_sha != topology["source_sha"]
    pin_path = _write_pin(topology["runtime"], other_sha)
    before = pin_path.read_bytes()

    _assert_blocked(lambda: _verify(topology), _PIN_MISMATCH)
    assert pin_path.read_bytes() == before


def test_uppercase_runtime_pin_sha_is_invalid_identity(tmp_path: Path) -> None:
    topology = _topology(tmp_path)
    uppercase_sha = topology["source_sha"].upper()
    assert uppercase_sha != topology["source_sha"]
    assert len(uppercase_sha) == 40
    pin_path = _write_pin(topology["runtime"], uppercase_sha)
    before_pin = pin_path.read_bytes()
    before_runtime = _runtime_files(topology["runtime"])

    _assert_blocked(lambda: _verify(topology), _PIN_IDENTITY_INVALID)

    assert pin_path.read_bytes() == before_pin
    assert _runtime_files(topology["runtime"]) == before_runtime
    assert _git(topology["source"], "rev-parse", "HEAD") == topology["source_sha"]
