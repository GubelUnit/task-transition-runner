"""Synthetic pre-spawn evidence reservation and no-replace publication.

Keep:
- one attempt-owned 0700 bundle under a synthetic Runtime root
- Runtime separated from Target and Runner Source by resolved paths
- exclusive ``O_CREAT|O_EXCL|O_NOFOLLOW`` 0600 ``.part`` open before any write
- bounded append writes onto the already opened owned part, then file-fsync
- hard-link publication onto an absent validated final basename
- observed file-fsync, link, directory-fsync, owned-part unlink, directory-fsync
- ``ADAPTER_EVIDENCE_CONFLICT`` versus ``ADAPTER_EVIDENCE_IO_FAILED``

Drop:
- spawn, pipes, signals, timeouts, or process-group acceptance
- ``create_part(..., payload=...)`` as a combined open-and-write primitive
- rename, replace, final unlink, inode adoption, or cross-filesystem copy
- symlink following, string-prefix root checks, or operator paths
- a test helper that stands in for ``reserve_evidence_bundle``
"""

from __future__ import annotations

import errno
import inspect
import os
import shutil
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from task_transition_runner.adapters.cursor_stream import reserve_evidence_bundle

_ATTEMPT = "synthetic-oss5-attempt-01"
_ATTEMPT_OTHER = "synthetic-oss5-attempt-02"
_STDOUT_PART = "stdout.part"
_STDOUT_FINAL = "stdout"
_STDERR_PART = "stderr.part"
_STDERR_FINAL = "stderr"
_FOREIGN_PART = "foreign.part"
_STDOUT_BYTES = b"synthetic-oss5-evidence-stdout"
_STDOUT_CHUNK_A = b"synthetic-oss5-stdout-chunk-a"
_STDOUT_CHUNK_B = b"synthetic-oss5-stdout-chunk-b"
_STDERR_BYTES = b"synthetic-oss5-evidence-stderr"
_FOREIGN_BYTES = b"synthetic-oss5-foreign-part-bytes"
_PREEXISTING_BYTES = b"synthetic-oss5-preexisting-final-bytes"
_RACED_BYTES = b"synthetic-oss5-raced-final-bytes"
_IO_SECRET = "synthetic-oss5-io-secret"
_REASON_CONFLICT = "ADAPTER_EVIDENCE_CONFLICT"
_REASON_IO_FAILED = "ADAPTER_EVIDENCE_IO_FAILED"
_FORBIDDEN_PARAMETER_NAMES = {
    "argv",
    "auth",
    "child",
    "endpoint",
    "environment",
    "file",
    "file_path",
    "logical_ref",
    "model",
    "path",
    "process",
    "prompt",
    "session_id",
    "source",
    "source_path",
    "spawn",
    "stderr",
    "stdin",
    "stdout",
}
_INVALID_BASENAMES = (
    "",
    ".",
    "..",
    "stdout.part/nested",
    "../stdout.part",
    "/synthetic-oss5-absolute.part",
    "stdout\x00.part",
)
_INVALID_TOKENS = (
    "",
    ".",
    "..",
    "synthetic-oss5-attempt/nested",
    "../synthetic-oss5-attempt",
    "/synthetic-oss5-absolute-attempt",
    "synthetic-oss5-attempt\x00nested",
)
_INVALID_FINAL_BASENAMES = (
    "",
    ".",
    "..",
    "stdout/nested",
    "../stdout",
    "/synthetic-oss5-absolute-stdout",
    "stdout\x00nested",
)


class _Roots:
    def __init__(
        self,
        *,
        runtime_root: Path,
        target_root: Path,
        runner_source_root: Path,
        attempt_token: str = _ATTEMPT,
    ) -> None:
        self.runtime_root = runtime_root
        self.target_root = target_root
        self.runner_source_root = runner_source_root
        self.attempt_token = attempt_token

    def snippets(self) -> tuple[str, ...]:
        return (
            str(self.runtime_root),
            str(self.target_root),
            str(self.runner_source_root),
            str(self.runtime_root.resolve()),
            str(self.target_root.resolve()),
            str(self.runner_source_root.resolve()),
            _STDOUT_BYTES.decode("ascii"),
            _STDOUT_CHUNK_A.decode("ascii"),
            _STDOUT_CHUNK_B.decode("ascii"),
            _STDERR_BYTES.decode("ascii"),
            _FOREIGN_BYTES.decode("ascii"),
            _PREEXISTING_BYTES.decode("ascii"),
            _RACED_BYTES.decode("ascii"),
            _IO_SECRET,
        )


class _SyscallSpy:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.records: list[dict[str, Any]] = []
        self._real_open = os.open
        self._real_mkdir = os.mkdir
        self._real_fsync = os.fsync
        self._real_link = os.link
        self._real_unlink = os.unlink
        self._real_rename = os.rename
        self._real_replace = os.replace
        self._real_copy = shutil.copy
        self._real_copy2 = shutil.copy2
        self._real_copyfile = shutil.copyfile
        monkeypatch.setattr(os, "open", self._open)
        monkeypatch.setattr(os, "mkdir", self._mkdir)
        monkeypatch.setattr(os, "fsync", self._fsync)
        monkeypatch.setattr(os, "link", self._link)
        monkeypatch.setattr(os, "unlink", self._unlink)
        monkeypatch.setattr(os, "rename", self._rename)
        monkeypatch.setattr(os, "replace", self._replace)
        monkeypatch.setattr(shutil, "copy", self._copy)
        monkeypatch.setattr(shutil, "copy2", self._copy2)
        monkeypatch.setattr(shutil, "copyfile", self._copyfile)

    def reset(self) -> None:
        self.records = []

    def ops(self) -> list[str]:
        return [str(record["op"]) for record in self.records]

    def _open(self, path: str | bytes | os.PathLike[str], flags: int, mode: int = 0o777, *args: Any, dir_fd: int | None = None, **kwargs: Any) -> int:
        self.records.append(
            {
                "op": "open",
                "path": path,
                "flags": flags,
                "mode": mode,
                "dir_fd": dir_fd,
            }
        )
        return self._real_open(path, flags, mode, *args, dir_fd=dir_fd, **kwargs)

    def _mkdir(self, path: str | bytes | os.PathLike[str], mode: int = 0o777, *, dir_fd: int | None = None) -> None:
        self.records.append({"op": "mkdir", "path": path, "mode": mode, "dir_fd": dir_fd})
        return self._real_mkdir(path, mode, dir_fd=dir_fd)

    def _fsync(self, fd: int) -> None:
        info = os.fstat(fd)
        kind = "dir" if stat.S_ISDIR(info.st_mode) else "file"
        self.records.append({"op": f"fsync-{kind}", "ino": info.st_ino})
        return self._real_fsync(fd)

    def _link(
        self,
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        self.records.append(
            {
                "op": "link",
                "src": src,
                "dst": dst,
                "src_dir_fd": src_dir_fd,
                "dst_dir_fd": dst_dir_fd,
                "follow_symlinks": follow_symlinks,
            }
        )
        return self._real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    def _unlink(self, path: str | bytes | os.PathLike[str], *, dir_fd: int | None = None) -> None:
        self.records.append({"op": "unlink", "path": path, "dir_fd": dir_fd})
        return self._real_unlink(path, dir_fd=dir_fd)

    def _rename(self, src: str | bytes | os.PathLike[str], dst: str | bytes | os.PathLike[str], *args: Any, **kwargs: Any) -> None:
        self.records.append({"op": "rename", "src": src, "dst": dst})
        return self._real_rename(src, dst, *args, **kwargs)

    def _replace(self, src: str | bytes | os.PathLike[str], dst: str | bytes | os.PathLike[str], *args: Any, **kwargs: Any) -> None:
        self.records.append({"op": "replace", "src": src, "dst": dst})
        return self._real_replace(src, dst, *args, **kwargs)

    def _copy(self, *args: Any, **kwargs: Any) -> Any:
        self.records.append({"op": "copy", "args": args})
        return self._real_copy(*args, **kwargs)

    def _copy2(self, *args: Any, **kwargs: Any) -> Any:
        self.records.append({"op": "copy2", "args": args})
        return self._real_copy2(*args, **kwargs)

    def _copyfile(self, *args: Any, **kwargs: Any) -> Any:
        self.records.append({"op": "copyfile", "args": args})
        return self._real_copyfile(*args, **kwargs)


def _layout(tmp_path: Path, *, attempt_token: str = _ATTEMPT) -> _Roots:
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    runtime_root.mkdir()
    target_root.mkdir()
    runner_source_root.mkdir()
    return _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
        attempt_token=attempt_token,
    )


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _as_path(value: object) -> Path:
    assert isinstance(value, (str, os.PathLike))
    return Path(value)


def _base(value: object) -> str:
    return os.path.basename(os.fsdecode(os.fspath(value)))


def _is_same_or_descendant(path: Path, root: Path) -> bool:
    resolved = path.resolve()
    root_resolved = root.resolve()
    return resolved == root_resolved or root_resolved in resolved.parents


def _snapshot(root: Path) -> dict[str, tuple[int, int, bytes | None]]:
    entries: dict[str, tuple[int, int, bytes | None]] = {}
    for item in sorted(root.rglob("*")):
        relative = str(item.relative_to(root))
        info = item.lstat()
        payload: bytes | None
        if stat.S_ISREG(info.st_mode):
            payload = item.read_bytes()
        elif stat.S_ISLNK(info.st_mode):
            payload = os.fsencode(os.readlink(item))
        else:
            payload = None
        entries[relative] = (info.st_ino, info.st_mtime_ns, payload)
    return entries


def _reserve(roots: _Roots) -> Any:
    return reserve_evidence_bundle(
        runtime_root=roots.runtime_root,
        target_root=roots.target_root,
        runner_source_root=roots.runner_source_root,
        attempt_token=roots.attempt_token,
    )


def _written_part(bundle: Any, *, basename: str, payload: bytes) -> Any:
    part = bundle.create_part(basename=basename)
    bundle.write_part(part=part, payload=payload)
    return part


def _expect_reason(reason: str, action: Callable[[], object], *, snippets: tuple[str, ...] = ()) -> BaseException:
    with pytest.raises(Exception) as exc_info:
        action()
    exc = exc_info.value
    message = str(exc)
    assert getattr(exc, "reason_code") == reason
    assert message == reason
    for snippet in snippets:
        assert snippet not in message
    return exc


def _assert_keyword_only(func: Callable[..., object], required: set[str]) -> None:
    parameters = inspect.signature(func).parameters
    assert required <= set(parameters)
    assert _FORBIDDEN_PARAMETER_NAMES.isdisjoint(parameters)
    for name in required:
        parameter = parameters[name]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty


def _assert_bundle_home(bundle: Any, roots: _Roots) -> Path:
    directory = _as_path(bundle.directory)
    expected = roots.runtime_root / roots.attempt_token
    assert bundle.owner_token == roots.attempt_token
    assert directory.resolve() == expected.resolve()
    assert directory.is_dir()
    assert not directory.is_symlink()
    assert _mode(directory) == 0o700
    assert _is_same_or_descendant(directory, roots.runtime_root)
    assert directory.resolve() != roots.runtime_root.resolve()
    assert not _is_same_or_descendant(directory, roots.target_root)
    assert not _is_same_or_descendant(directory, roots.runner_source_root)
    assert not roots.runtime_root.is_symlink()
    assert not directory.is_symlink()
    return directory


def test_reserve_evidence_bundle_binds_keyword_roots_and_attempt_token() -> None:
    _assert_keyword_only(
        reserve_evidence_bundle,
        {"runtime_root", "target_root", "runner_source_root", "attempt_token"},
    )


def test_reserve_evidence_bundle_creates_exclusive_0700_owned_directory_under_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    target_before = _snapshot(roots.target_root)
    source_before = _snapshot(roots.runner_source_root)
    spy = _SyscallSpy(monkeypatch)

    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)

    mkdir_records = [record for record in spy.records if record["op"] == "mkdir"]
    assert mkdir_records
    assert any(record["mode"] == 0o700 for record in mkdir_records)
    assert target_before == _snapshot(roots.target_root)
    assert source_before == _snapshot(roots.runner_source_root)
    assert list(directory.iterdir()) == []


def test_reserve_evidence_bundle_lifecycle_publishes_complete_owned_bytes_by_hard_link(
    tmp_path: Path,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    _assert_keyword_only(bundle.create_part, {"basename"})
    _assert_keyword_only(bundle.write_part, {"part", "payload"})
    _assert_keyword_only(bundle.publish, {"part", "final_basename"})
    assert "payload" not in inspect.signature(bundle.create_part).parameters

    stdout_part = bundle.create_part(basename=_STDOUT_PART)
    stderr_part = bundle.create_part(basename=_STDERR_PART)
    stdout_path = directory / stdout_part.basename
    stderr_path = directory / stderr_part.basename

    assert stdout_part.basename == _STDOUT_PART
    assert stderr_part.basename == _STDERR_PART
    assert _mode(stdout_path) == 0o600
    assert _mode(stderr_path) == 0o600
    assert not stdout_path.is_symlink()
    assert not stderr_path.is_symlink()
    assert stdout_path.read_bytes() == b""
    assert stderr_path.read_bytes() == b""
    assert not (directory / _STDOUT_FINAL).exists()
    assert not (directory / _STDERR_FINAL).exists()

    bundle.write_part(part=stdout_part, payload=_STDOUT_BYTES)
    bundle.write_part(part=stderr_part, payload=_STDERR_BYTES)
    stdout_ino = stdout_path.lstat().st_ino
    stderr_ino = stderr_path.lstat().st_ino
    assert stdout_path.read_bytes() == _STDOUT_BYTES
    assert stderr_path.read_bytes() == _STDERR_BYTES
    assert not (directory / _STDOUT_FINAL).exists()
    assert not (directory / _STDERR_FINAL).exists()

    bundle.publish(part=stdout_part, final_basename=_STDOUT_FINAL)
    bundle.publish(part=stderr_part, final_basename=_STDERR_FINAL)

    stdout_final = directory / _STDOUT_FINAL
    stderr_final = directory / _STDERR_FINAL
    assert not stdout_path.exists()
    assert not stderr_path.exists()
    assert not stdout_final.is_symlink()
    assert not stderr_final.is_symlink()
    assert _mode(stdout_final) == 0o600
    assert _mode(stderr_final) == 0o600
    assert stdout_final.lstat().st_ino == stdout_ino
    assert stderr_final.lstat().st_ino == stderr_ino
    assert stdout_final.lstat().st_nlink == 1
    assert stderr_final.lstat().st_nlink == 1
    assert stdout_final.read_bytes() == _STDOUT_BYTES
    assert stderr_final.read_bytes() == _STDERR_BYTES


def test_reserve_evidence_bundle_observes_fsync_link_unlink_dir_fsync_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    spy = _SyscallSpy(monkeypatch)

    part = bundle.create_part(basename=_STDOUT_PART)
    create_ops = spy.ops()
    open_records = [record for record in spy.records if record["op"] == "open"]
    exclusive_opens = [
        record
        for record in open_records
        if int(record["flags"]) & os.O_CREAT
        and int(record["flags"]) & os.O_EXCL
        and int(record["flags"]) & os.O_NOFOLLOW
        and int(record["mode"]) == 0o600
    ]
    assert "open" in create_ops
    assert exclusive_opens
    assert "link" not in create_ops
    assert (_as_path(bundle.directory) / _STDOUT_PART).read_bytes() == b""

    spy.reset()
    bundle.write_part(part=part, payload=_STDOUT_BYTES)
    write_ops = spy.ops()
    assert "fsync-file" in write_ops
    assert "link" not in write_ops
    assert "unlink" not in write_ops
    assert (_as_path(bundle.directory) / _STDOUT_PART).read_bytes() == _STDOUT_BYTES
    assert not (_as_path(bundle.directory) / _STDOUT_FINAL).exists()

    spy.reset()
    bundle.publish(part=part, final_basename=_STDOUT_FINAL)
    durability_ops = [
        op
        for op in spy.ops()
        if op
        in {
            "link",
            "fsync-file",
            "fsync-dir",
            "unlink",
            "rename",
            "replace",
            "copy",
            "copy2",
            "copyfile",
        }
    ]
    if durability_ops[:1] == ["fsync-file"]:
        durability_ops = durability_ops[1:]
    link_records = [record for record in spy.records if record["op"] == "link"]
    unlink_records = [record for record in spy.records if record["op"] == "unlink"]

    assert durability_ops == ["link", "fsync-dir", "unlink", "fsync-dir"]
    assert link_records
    for record in link_records:
        assert record["follow_symlinks"] is False
        assert record["src_dir_fd"] is not None
        assert record["dst_dir_fd"] is not None
        assert _base(record["src"]) == _STDOUT_PART
        assert _base(record["dst"]) == _STDOUT_FINAL
    assert unlink_records
    for record in unlink_records:
        assert _base(record["path"]) == _STDOUT_PART
        assert _base(record["path"]) != _STDOUT_FINAL
    assert "rename" not in durability_ops
    assert "replace" not in durability_ops
    assert "copy" not in durability_ops
    assert "copy2" not in durability_ops
    assert "copyfile" not in durability_ops


def test_reserve_evidence_bundle_accepts_string_prefix_sibling_of_target_and_source(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "tgt-runtime"
    target_root = tmp_path / "tgt"
    runner_source_root = tmp_path / "src"
    runtime_root.mkdir()
    target_root.mkdir()
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )

    bundle = _reserve(roots)

    directory = _assert_bundle_home(bundle, roots)
    assert str(directory.resolve()).startswith(str(target_root.resolve()))
    assert not _is_same_or_descendant(directory, target_root)
    assert not _is_same_or_descendant(directory, runner_source_root)


def test_reserve_evidence_bundle_rejects_runtime_equal_to_target(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir()
    source = tmp_path / "runner-source"
    source.mkdir()
    roots = _Roots(
        runtime_root=shared,
        target_root=shared,
        runner_source_root=source,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(shared.iterdir()) == []


def test_reserve_evidence_bundle_rejects_runtime_under_target(tmp_path: Path) -> None:
    target_root = tmp_path / "target"
    runtime_root = target_root / "nested-runtime"
    runner_source_root = tmp_path / "runner-source"
    target_root.mkdir()
    runtime_root.mkdir()
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(runtime_root.iterdir()) == []


def test_reserve_evidence_bundle_rejects_runtime_equal_to_runner_source(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir()
    target_root = tmp_path / "target"
    target_root.mkdir()
    roots = _Roots(
        runtime_root=shared,
        target_root=target_root,
        runner_source_root=shared,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(shared.iterdir()) == []


def test_reserve_evidence_bundle_rejects_runtime_under_runner_source(tmp_path: Path) -> None:
    runner_source_root = tmp_path / "runner-source"
    runtime_root = runner_source_root / "nested-runtime"
    target_root = tmp_path / "target"
    runner_source_root.mkdir()
    runtime_root.mkdir()
    target_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(runtime_root.iterdir()) == []


def test_reserve_evidence_bundle_rejects_resolved_runtime_inside_target_via_symlink(
    tmp_path: Path,
) -> None:
    target_root = tmp_path / "target"
    nested = target_root / "nested-runtime"
    runtime_link = tmp_path / "runtime"
    runner_source_root = tmp_path / "runner-source"
    target_root.mkdir()
    nested.mkdir()
    runtime_link.symlink_to(nested)
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_link,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(nested.iterdir()) == []


def test_reserve_evidence_bundle_rejects_symlink_runtime_root_component(tmp_path: Path) -> None:
    real_runtime = tmp_path / "real-runtime"
    runtime_link = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    real_runtime.mkdir()
    runtime_link.symlink_to(real_runtime)
    target_root.mkdir()
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_link,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(real_runtime.iterdir()) == []


def test_reserve_evidence_bundle_rejects_preexisting_bundle_directory(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    preexisting = roots.runtime_root / roots.attempt_token
    preexisting.mkdir(mode=0o700)
    marker = preexisting / _FOREIGN_PART
    marker.write_bytes(_FOREIGN_BYTES)

    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert preexisting.is_dir()
    assert marker.read_bytes() == _FOREIGN_BYTES


def test_reserve_evidence_bundle_rejects_preexisting_bundle_symlink(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    planted = outside / _FOREIGN_PART
    planted.write_bytes(_FOREIGN_BYTES)
    (roots.runtime_root / roots.attempt_token).symlink_to(outside)

    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert planted.read_bytes() == _FOREIGN_BYTES
    assert (roots.runtime_root / roots.attempt_token).is_symlink()


def test_reserve_evidence_bundle_rejects_invalid_attempt_token(tmp_path: Path) -> None:
    for index, token in enumerate(_INVALID_TOKENS):
        case = tmp_path / f"case-{index}"
        case.mkdir()
        roots = _layout(case)
        roots.attempt_token = token
        _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
        assert list(roots.runtime_root.iterdir()) == []


def test_create_part_rejects_invalid_basename(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    for basename in _INVALID_BASENAMES:
        _expect_reason(
            _REASON_CONFLICT,
            lambda basename=basename: bundle.create_part(basename=basename),
            snippets=roots.snippets(),
        )
    assert list(directory.iterdir()) == []


def test_create_part_rejects_existing_regular_part_without_overwrite(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    planted = directory / _STDOUT_PART
    planted.write_bytes(_FOREIGN_BYTES)
    planted.chmod(0o600)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.create_part(basename=_STDOUT_PART),
        snippets=roots.snippets(),
    )
    assert planted.read_bytes() == _FOREIGN_BYTES


def test_create_part_rejects_symlink_part_without_following(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    outside = tmp_path / "outside-part"
    outside.write_bytes(_FOREIGN_BYTES)
    planted = directory / _STDOUT_PART
    planted.symlink_to(outside)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.create_part(basename=_STDOUT_PART),
        snippets=roots.snippets(),
    )
    assert planted.is_symlink()
    assert outside.read_bytes() == _FOREIGN_BYTES


def test_publish_rejects_existing_final_without_replace_or_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    final_path = directory / _STDOUT_FINAL
    final_path.write_bytes(_PREEXISTING_BYTES)
    final_path.chmod(0o600)
    preexisting_info = final_path.lstat()
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    owned_part_info = part_path.lstat()
    product_pin_fd = part._fd
    assert type(product_pin_fd) is int
    owned_pin_info = os.fstat(product_pin_fd)
    assert (owned_pin_info.st_dev, owned_pin_info.st_ino) == (
        owned_part_info.st_dev,
        owned_part_info.st_ino,
    )
    classification: dict[str, Any] = {
        "seen": False,
        "product_fd": None,
        "product_live": False,
        "product_dev": None,
        "product_ino": None,
    }
    spy = _SyscallSpy(monkeypatch)
    real_link = os.link

    def classify_link(
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        if _base(dst) == _STDOUT_FINAL:
            measured_fd = part._fd
            classification["seen"] = True
            classification["product_fd"] = measured_fd
            live = False
            if measured_fd is product_pin_fd and type(measured_fd) is int:
                try:
                    pin_info = os.fstat(measured_fd)
                except OSError:
                    pin_info = None
                else:
                    live = (pin_info.st_dev, pin_info.st_ino) == (
                        owned_pin_info.st_dev,
                        owned_pin_info.st_ino,
                    )
                    classification["product_dev"] = pin_info.st_dev
                    classification["product_ino"] = pin_info.st_ino
            classification["product_live"] = live
        return real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "link", classify_link)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert final_path.read_bytes() == _PREEXISTING_BYTES
    assert (final_path.lstat().st_dev, final_path.lstat().st_ino) == (
        preexisting_info.st_dev,
        preexisting_info.st_ino,
    )
    assert part_path.exists()
    assert not part_path.is_symlink()
    assert part_path.read_bytes() == _STDOUT_BYTES
    assert (part_path.lstat().st_dev, part_path.lstat().st_ino) == (
        owned_part_info.st_dev,
        owned_part_info.st_ino,
    )
    assert "rename" not in spy.ops()
    assert "replace" not in spy.ops()
    assert "copy" not in spy.ops()
    assert all(
        _base(record["path"]) != _STDOUT_FINAL
        for record in spy.records
        if record["op"] == "unlink"
    )

    product_fd_live_at_classification = (
        classification["seen"] is True
        and classification["product_live"] is True
        and classification["product_fd"] is product_pin_fd
        and (classification["product_dev"], classification["product_ino"])
        == (owned_pin_info.st_dev, owned_pin_info.st_ino)
    )
    product_fd_after = part._fd
    product_fd_closed_after_conflict = False
    if product_fd_after is None:
        product_fd_closed_after_conflict = True
    elif type(product_fd_after) is int:
        try:
            os.fstat(product_fd_after)
        except OSError:
            product_fd_closed_after_conflict = True

    try:
        bundle.write_part(part=part, payload=_STDOUT_CHUNK_B)
    except Exception as exc:
        write_exc = exc
    else:
        write_exc = None
    write_conflict = (
        write_exc is not None
        and getattr(write_exc, "reason_code", None) == _REASON_CONFLICT
        and str(write_exc) == _REASON_CONFLICT
    )
    if (
        not product_fd_live_at_classification
        or not product_fd_closed_after_conflict
        or not write_conflict
    ):
        pytest.fail(
            "existing-final conflict terminal state: product FD open after classification or API handle remained writable"
        )

    assert write_exc is not None
    for snippet in roots.snippets():
        assert snippet not in str(write_exc)
    assert final_path.read_bytes() == _PREEXISTING_BYTES
    assert (final_path.lstat().st_dev, final_path.lstat().st_ino) == (
        preexisting_info.st_dev,
        preexisting_info.st_ino,
    )
    assert part_path.read_bytes() == _STDOUT_BYTES
    assert (part_path.lstat().st_dev, part_path.lstat().st_ino) == (
        owned_part_info.st_dev,
        owned_part_info.st_ino,
    )


def test_publish_rejects_symlink_final_without_following(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    outside = tmp_path / "outside-final"
    outside.write_bytes(_PREEXISTING_BYTES)
    final_path = directory / _STDOUT_FINAL
    final_path.symlink_to(outside)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert final_path.is_symlink()
    assert outside.read_bytes() == _PREEXISTING_BYTES
    assert (directory / _STDOUT_PART).read_bytes() == _STDOUT_BYTES


def test_publish_rejects_destination_race_as_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    real_link = os.link
    real_open = os.open

    def racing_link(
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        fd = real_open(dst, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=dst_dir_fd)
        os.write(fd, _RACED_BYTES)
        os.close(fd)
        return real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "link", racing_link)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    final_path = directory / _STDOUT_FINAL
    assert final_path.read_bytes() == _RACED_BYTES
    assert (directory / _STDOUT_PART).read_bytes() == _STDOUT_BYTES


def test_publish_leaves_foreign_part_unchanged(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    foreign = directory / _FOREIGN_PART
    foreign.write_bytes(_FOREIGN_BYTES)
    foreign.chmod(0o600)
    foreign_ino = foreign.lstat().st_ino
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)

    assert foreign.exists()
    assert foreign.lstat().st_ino == foreign_ino
    assert foreign.read_bytes() == _FOREIGN_BYTES
    assert (directory / _STDOUT_FINAL).read_bytes() == _STDOUT_BYTES
    assert not (directory / _STDOUT_PART).exists()


def test_publish_does_not_call_rename_replace_or_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    spy = _SyscallSpy(monkeypatch)

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)

    assert "rename" not in spy.ops()
    assert "replace" not in spy.ops()
    assert "copy" not in spy.ops()
    assert "copy2" not in spy.ops()
    assert "copyfile" not in spy.ops()


def test_publish_cross_filesystem_exdev_is_io_failed_without_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    spy = _SyscallSpy(monkeypatch)

    def boom_link(*args: Any, **kwargs: Any) -> None:
        raise OSError(errno.EXDEV, _IO_SECRET)

    monkeypatch.setattr(os, "link", boom_link)

    _expect_reason(
        _REASON_IO_FAILED,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert not (directory / _STDOUT_FINAL).exists()
    assert (directory / _STDOUT_PART).read_bytes() == _STDOUT_BYTES
    assert "copy" not in spy.ops()
    assert "copy2" not in spy.ops()
    assert "copyfile" not in spy.ops()
    assert "rename" not in spy.ops()
    assert "replace" not in spy.ops()


def test_reserve_io_failure_is_not_conflict_and_omits_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    real_mkdir = os.mkdir

    def boom_mkdir(path: str | bytes | os.PathLike[str], mode: int = 0o777, *, dir_fd: int | None = None) -> None:
        if _base(path) == roots.attempt_token:
            raise OSError(errno.EIO, _IO_SECRET)
        return real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "mkdir", boom_mkdir)

    _expect_reason(_REASON_IO_FAILED, lambda: _reserve(roots), snippets=roots.snippets())
    assert not (roots.runtime_root / roots.attempt_token).exists()


def test_crash_window_final_is_absent_or_complete(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    final_path = directory / _STDOUT_FINAL
    part_path = directory / _STDOUT_PART

    assert not final_path.exists()
    assert part_path.read_bytes() == _STDOUT_BYTES

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)

    assert not part_path.exists()
    assert final_path.read_bytes() == _STDOUT_BYTES
    assert len(final_path.read_bytes()) == len(_STDOUT_BYTES)


def test_second_reserve_same_attempt_token_is_conflict(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    first = _reserve(roots)
    directory = _assert_bundle_home(first, roots)
    marker = directory / _FOREIGN_PART
    marker.write_bytes(_FOREIGN_BYTES)

    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert marker.read_bytes() == _FOREIGN_BYTES
    assert _mode(directory) == 0o700


def test_publish_rejects_invalid_final_basename(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    for final_basename in _INVALID_FINAL_BASENAMES:
        _expect_reason(
            _REASON_CONFLICT,
            lambda final_basename=final_basename: bundle.publish(
                part=part,
                final_basename=final_basename,
            ),
            snippets=roots.snippets(),
        )
    assert (directory / _STDOUT_PART).read_bytes() == _STDOUT_BYTES
    assert not (directory / _STDOUT_FINAL).exists()
    assert {path.name for path in directory.iterdir()} == {_STDOUT_PART}


def test_publish_rejects_foreign_bundle_handle(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    owner = _reserve(roots)
    other_roots = _Roots(
        runtime_root=roots.runtime_root,
        target_root=roots.target_root,
        runner_source_root=roots.runner_source_root,
        attempt_token=_ATTEMPT_OTHER,
    )
    other = _reserve(other_roots)
    owner_dir = _assert_bundle_home(owner, roots)
    other_dir = _assert_bundle_home(other, other_roots)
    part = _written_part(owner, basename=_STDOUT_PART, payload=_STDOUT_BYTES)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: other.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets() + other_roots.snippets(),
    )
    assert (owner_dir / _STDOUT_PART).read_bytes() == _STDOUT_BYTES
    assert not (owner_dir / _STDOUT_FINAL).exists()
    assert not (other_dir / _STDOUT_PART).exists()
    assert not (other_dir / _STDOUT_FINAL).exists()


def test_write_part_rejects_part_replaced_by_symlink(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = bundle.create_part(basename=_STDOUT_PART)
    part_path = directory / _STDOUT_PART
    outside = tmp_path / "outside-replaced-part"
    outside.write_bytes(_FOREIGN_BYTES)
    part_path.unlink()
    part_path.symlink_to(outside)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.write_part(part=part, payload=_STDOUT_BYTES),
        snippets=roots.snippets(),
    )
    assert part_path.is_symlink()
    assert outside.read_bytes() == _FOREIGN_BYTES
    assert not (directory / _STDOUT_FINAL).exists()


def test_publish_rejects_part_replaced_by_symlink(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    outside = tmp_path / "outside-replaced-source"
    outside.write_bytes(_FOREIGN_BYTES)
    part_path.unlink()
    part_path.symlink_to(outside)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert part_path.is_symlink()
    assert outside.read_bytes() == _FOREIGN_BYTES
    assert not (directory / _STDOUT_FINAL).exists()


def test_reserve_evidence_bundle_rejects_symlink_in_parent_component(tmp_path: Path) -> None:
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    runtime_real = real_parent / "runtime"
    runtime_real.mkdir()
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent)
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    target_root.mkdir()
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=link_parent / "runtime",
        target_root=target_root,
        runner_source_root=runner_source_root,
    )

    assert not roots.runtime_root.is_symlink()
    assert link_parent.is_symlink()
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(runtime_real.iterdir()) == []


def test_write_part_file_fsync_failure_is_io_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = bundle.create_part(basename=_STDOUT_PART)
    real_fsync = os.fsync

    def boom_fsync(fd: int) -> None:
        info = os.fstat(fd)
        if stat.S_ISREG(info.st_mode):
            raise OSError(errno.EIO, _IO_SECRET)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", boom_fsync)

    _expect_reason(
        _REASON_IO_FAILED,
        lambda: bundle.write_part(part=part, payload=_STDOUT_BYTES),
        snippets=roots.snippets(),
    )
    assert not (directory / _STDOUT_FINAL).exists()
    assert (directory / _STDOUT_PART).is_symlink() is False


def test_publish_directory_fsync_failure_is_io_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    real_fsync = os.fsync
    real_link = os.link
    linked = {"done": False}

    def spy_link(
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        result = real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        linked["done"] = True
        return result

    def boom_fsync(fd: int) -> None:
        info = os.fstat(fd)
        if linked["done"] and stat.S_ISDIR(info.st_mode):
            raise OSError(errno.EIO, _IO_SECRET)
        return real_fsync(fd)

    monkeypatch.setattr(os, "link", spy_link)
    monkeypatch.setattr(os, "fsync", boom_fsync)

    _expect_reason(
        _REASON_IO_FAILED,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert linked["done"] is True
    final_path = directory / _STDOUT_FINAL
    part_path = directory / _STDOUT_PART
    assert part_path.exists()
    assert not part_path.is_symlink()
    assert part_path.read_bytes() == _STDOUT_BYTES
    assert final_path.exists()
    assert not final_path.is_symlink()
    assert final_path.read_bytes() == _STDOUT_BYTES
    assert part_path.lstat().st_ino == final_path.lstat().st_ino


def test_publish_second_directory_fsync_failure_is_io_failed_after_owned_part_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    final_path = directory / _STDOUT_FINAL
    owned_info = part_path.lstat()
    product_pin_fd = part._fd
    assert type(product_pin_fd) is int
    spy = _SyscallSpy(monkeypatch)
    real_link = os.link
    real_unlink = os.unlink
    real_fsync = os.fsync
    linked = {"done": False}
    unlinked = {"done": False}

    def spy_link(
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        result = real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        if _base(src) == _STDOUT_PART and _base(dst) == _STDOUT_FINAL:
            linked["done"] = True
        return result

    def spy_unlink(
        path: str | bytes | os.PathLike[str],
        *,
        dir_fd: int | None = None,
    ) -> None:
        if _base(path) == _STDOUT_PART:
            unlinked["done"] = True
        return real_unlink(path, dir_fd=dir_fd)

    def boom_second_dir_fsync(fd: int) -> None:
        info = os.fstat(fd)
        if linked["done"] and unlinked["done"] and stat.S_ISDIR(info.st_mode):
            raise OSError(errno.EIO, _IO_SECRET)
        return real_fsync(fd)

    monkeypatch.setattr(os, "link", spy_link)
    monkeypatch.setattr(os, "unlink", spy_unlink)
    monkeypatch.setattr(os, "fsync", boom_second_dir_fsync)

    _expect_reason(
        _REASON_IO_FAILED,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert linked["done"] is True
    assert unlinked["done"] is True
    assert not part_path.exists()
    assert final_path.exists()
    assert not final_path.is_symlink()
    assert final_path.read_bytes() == _STDOUT_BYTES
    assert (final_path.lstat().st_dev, final_path.lstat().st_ino) == (
        owned_info.st_dev,
        owned_info.st_ino,
    )
    assert "rename" not in spy.ops()
    assert "replace" not in spy.ops()
    assert "copy" not in spy.ops()
    assert all(
        _base(record["path"]) != _STDOUT_FINAL
        for record in spy.records
        if record["op"] == "unlink"
    )
    product_fd_after = part._fd
    product_fd_closed = False
    if product_fd_after is None:
        product_fd_closed = True
    elif type(product_fd_after) is int:
        try:
            os.fstat(product_fd_after)
        except OSError:
            product_fd_closed = True
    assert product_fd_closed is True

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.write_part(part=part, payload=_STDOUT_CHUNK_B),
        snippets=roots.snippets(),
    )
    assert not part_path.exists()
    assert final_path.read_bytes() == _STDOUT_BYTES
    assert (final_path.lstat().st_dev, final_path.lstat().st_ino) == (
        owned_info.st_dev,
        owned_info.st_ino,
    )


def test_reserve_evidence_bundle_rejects_missing_runtime_root(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    target_root.mkdir()
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert not runtime_root.exists()


def test_publish_rejects_second_publish_of_same_part(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)
    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert (directory / _STDOUT_FINAL).read_bytes() == _STDOUT_BYTES
    assert not (directory / _STDOUT_PART).exists()


def test_reserve_evidence_bundle_rejects_target_symlink_to_runtime(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    runtime_root.mkdir()
    target_root.symlink_to(runtime_root)
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(runtime_root.iterdir()) == []


def test_reserve_evidence_bundle_rejects_runner_source_symlink_to_runtime(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    runtime_root.mkdir()
    target_root.mkdir()
    runner_source_root.symlink_to(runtime_root)
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert list(runtime_root.iterdir()) == []


def test_write_part_appends_successive_chunks_without_overwrite(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = bundle.create_part(basename=_STDOUT_PART)
    part_path = directory / _STDOUT_PART

    bundle.write_part(part=part, payload=_STDOUT_CHUNK_A)
    assert part_path.read_bytes() == _STDOUT_CHUNK_A
    bundle.write_part(part=part, payload=_STDOUT_CHUNK_B)
    assert part_path.read_bytes() == _STDOUT_CHUNK_A + _STDOUT_CHUNK_B
    assert not (directory / _STDOUT_FINAL).exists()

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)
    assert (directory / _STDOUT_FINAL).read_bytes() == _STDOUT_CHUNK_A + _STDOUT_CHUNK_B
    assert not part_path.exists()


def test_write_part_rejects_foreign_bundle_handle(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    owner = _reserve(roots)
    other_roots = _Roots(
        runtime_root=roots.runtime_root,
        target_root=roots.target_root,
        runner_source_root=roots.runner_source_root,
        attempt_token=_ATTEMPT_OTHER,
    )
    other = _reserve(other_roots)
    owner_dir = _assert_bundle_home(owner, roots)
    other_dir = _assert_bundle_home(other, other_roots)
    part = owner.create_part(basename=_STDOUT_PART)
    owner.write_part(part=part, payload=_STDOUT_BYTES)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: other.write_part(part=part, payload=_STDOUT_CHUNK_B),
        snippets=roots.snippets() + other_roots.snippets(),
    )
    assert (owner_dir / _STDOUT_PART).read_bytes() == _STDOUT_BYTES
    assert not (owner_dir / _STDOUT_FINAL).exists()
    assert not (other_dir / _STDOUT_PART).exists()
    assert not (other_dir / _STDOUT_FINAL).exists()


def test_write_part_rejects_after_successful_publish_and_keeps_final_bytes(
    tmp_path: Path,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)
    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.write_part(part=part, payload=_STDOUT_CHUNK_B),
        snippets=roots.snippets(),
    )
    assert (directory / _STDOUT_FINAL).read_bytes() == _STDOUT_BYTES
    assert not (directory / _STDOUT_PART).exists()


def test_reserve_evidence_bundle_rejects_runtime_root_as_file(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    target_root = tmp_path / "target"
    runner_source_root = tmp_path / "runner-source"
    runtime_root.write_bytes(_FOREIGN_BYTES)
    target_root.mkdir()
    runner_source_root.mkdir()
    roots = _Roots(
        runtime_root=runtime_root,
        target_root=target_root,
        runner_source_root=runner_source_root,
    )
    _expect_reason(_REASON_CONFLICT, lambda: _reserve(roots), snippets=roots.snippets())
    assert runtime_root.is_file()
    assert runtime_root.read_bytes() == _FOREIGN_BYTES


def test_publish_rejects_final_basename_equal_to_part_basename(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)

    _expect_reason(
        _REASON_CONFLICT,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_PART),
        snippets=roots.snippets(),
    )
    assert (directory / _STDOUT_PART).read_bytes() == _STDOUT_BYTES
    assert {path.name for path in directory.iterdir()} == {_STDOUT_PART}


def test_write_part_rejects_part_replaced_by_regular_file(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    original_fd = os.open(part_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        original_info = os.fstat(original_fd)
        part_path.unlink()
        part_path.write_bytes(_FOREIGN_BYTES)
        part_path.chmod(0o600)
        replacement_info = part_path.lstat()
        assert (replacement_info.st_dev, replacement_info.st_ino) != (
            original_info.st_dev,
            original_info.st_ino,
        )

        try:
            bundle.write_part(part=part, payload=_STDOUT_CHUNK_B)
        except Exception as exc:
            assert getattr(exc, "reason_code") == _REASON_CONFLICT
            assert str(exc) == _REASON_CONFLICT
            for snippet in roots.snippets():
                assert snippet not in str(exc)
        else:
            pytest.fail("regular replacement write: DID NOT RAISE ADAPTER_EVIDENCE_CONFLICT")

        held_info = os.fstat(original_fd)
        assert (held_info.st_dev, held_info.st_ino) == (
            original_info.st_dev,
            original_info.st_ino,
        )
        assert part_path.exists()
        assert not part_path.is_symlink()
        assert (part_path.lstat().st_dev, part_path.lstat().st_ino) == (
            replacement_info.st_dev,
            replacement_info.st_ino,
        )
        assert part_path.read_bytes() == _FOREIGN_BYTES
        assert not (directory / _STDOUT_FINAL).exists()
    finally:
        os.close(original_fd)


def test_publish_rejects_part_replaced_by_regular_file(tmp_path: Path) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    original_fd = os.open(part_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        original_info = os.fstat(original_fd)
        part_path.unlink()
        part_path.write_bytes(_FOREIGN_BYTES)
        part_path.chmod(0o600)
        replacement_info = part_path.lstat()
        assert (replacement_info.st_dev, replacement_info.st_ino) != (
            original_info.st_dev,
            original_info.st_ino,
        )

        try:
            bundle.publish(part=part, final_basename=_STDOUT_FINAL)
        except Exception as exc:
            assert getattr(exc, "reason_code") == _REASON_CONFLICT
            assert str(exc) == _REASON_CONFLICT
            for snippet in roots.snippets():
                assert snippet not in str(exc)
        else:
            pytest.fail("regular replacement publish: DID NOT RAISE ADAPTER_EVIDENCE_CONFLICT")

        held_info = os.fstat(original_fd)
        assert (held_info.st_dev, held_info.st_ino) == (
            original_info.st_dev,
            original_info.st_ino,
        )
        assert part_path.exists()
        assert not part_path.is_symlink()
        assert (part_path.lstat().st_dev, part_path.lstat().st_ino) == (
            replacement_info.st_dev,
            replacement_info.st_ino,
        )
        assert part_path.read_bytes() == _FOREIGN_BYTES
        assert not (directory / _STDOUT_FINAL).exists()
    finally:
        os.close(original_fd)


def test_write_part_rejects_after_post_link_directory_fsync_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    final_path = directory / _STDOUT_FINAL
    real_fsync = os.fsync
    real_link = os.link
    linked = {"done": False}

    def spy_link(
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        result = real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        linked["done"] = True
        return result

    def boom_fsync(fd: int) -> None:
        info = os.fstat(fd)
        if linked["done"] and stat.S_ISDIR(info.st_mode):
            raise OSError(errno.EIO, _IO_SECRET)
        return real_fsync(fd)

    monkeypatch.setattr(os, "link", spy_link)
    monkeypatch.setattr(os, "fsync", boom_fsync)

    _expect_reason(
        _REASON_IO_FAILED,
        lambda: bundle.publish(part=part, final_basename=_STDOUT_FINAL),
        snippets=roots.snippets(),
    )
    assert linked["done"] is True
    assert part_path.exists()
    assert not part_path.is_symlink()
    assert part_path.read_bytes() == _STDOUT_BYTES
    assert final_path.exists()
    assert not final_path.is_symlink()
    assert final_path.read_bytes() == _STDOUT_BYTES
    sealed_ino = part_path.lstat().st_ino
    assert sealed_ino == final_path.lstat().st_ino

    try:
        bundle.write_part(part=part, payload=_STDOUT_CHUNK_B)
    except Exception as exc:
        write_exc = exc
    else:
        write_exc = None

    final_bytes = final_path.read_bytes()
    conflict_raised = (
        write_exc is not None
        and getattr(write_exc, "reason_code", None) == _REASON_CONFLICT
        and str(write_exc) == _REASON_CONFLICT
    )
    if not conflict_raised or final_bytes != _STDOUT_BYTES:
        pytest.fail(
            "post-link fsync failure then write: DID NOT RAISE ADAPTER_EVIDENCE_CONFLICT and final bytes mutate"
        )

    assert write_exc is not None
    for snippet in roots.snippets():
        assert snippet not in str(write_exc)
    assert part_path.exists()
    assert not part_path.is_symlink()
    assert part_path.read_bytes() == _STDOUT_BYTES
    assert final_path.exists()
    assert not final_path.is_symlink()
    assert final_path.read_bytes() == _STDOUT_BYTES
    assert part_path.lstat().st_ino == sealed_ino
    assert final_path.lstat().st_ino == sealed_ino


def test_publish_rejects_source_replaced_between_ownership_check_and_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    final_path = directory / _STDOUT_FINAL
    real_link = os.link
    real_open = os.open
    real_stat = os.stat
    real_unlink = os.unlink
    product_pin_fd = part._fd
    assert type(product_pin_fd) is int
    owned_pin_info = os.fstat(product_pin_fd)
    swapped: dict[str, Any] = {
        "done": False,
        "foreign_fd": None,
        "foreign_dev": None,
        "foreign_ino": None,
    }
    identity_check: dict[str, Any] = {
        "seen": False,
        "product_fd": None,
        "product_live": False,
        "product_dev": None,
        "product_ino": None,
    }
    original_fd = os.open(part_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        original_info = os.fstat(original_fd)
        assert product_pin_fd is not original_fd

        def racing_link(
            src: str | bytes | os.PathLike[str],
            dst: str | bytes | os.PathLike[str],
            *,
            src_dir_fd: int | None = None,
            dst_dir_fd: int | None = None,
            follow_symlinks: bool = True,
        ) -> None:
            if _base(src) == _STDOUT_PART and _base(dst) == _STDOUT_FINAL:
                real_unlink(src, dir_fd=src_dir_fd)
                created = real_open(
                    src,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                    dir_fd=src_dir_fd,
                )
                try:
                    os.write(created, _FOREIGN_BYTES)
                    foreign_info = os.fstat(created)
                finally:
                    os.close(created)
                foreign_fd = real_open(
                    src,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=src_dir_fd,
                )
                swapped["foreign_fd"] = foreign_fd
                swapped["foreign_dev"] = foreign_info.st_dev
                swapped["foreign_ino"] = foreign_info.st_ino
                swapped["done"] = True
            return real_link(
                src,
                dst,
                src_dir_fd=src_dir_fd,
                dst_dir_fd=dst_dir_fd,
                follow_symlinks=follow_symlinks,
            )

        def identity_stat(
            path: str | bytes | os.PathLike[str],
            *args: Any,
            dir_fd: int | None = None,
            follow_symlinks: bool = True,
            **kwargs: Any,
        ) -> os.stat_result:
            if _base(path) == _STDOUT_FINAL and dir_fd is not None and not identity_check["seen"]:
                measured_fd = part._fd
                identity_check["seen"] = True
                identity_check["product_fd"] = measured_fd
                live = False
                if (
                    measured_fd is product_pin_fd
                    and measured_fd is not original_fd
                    and type(measured_fd) is int
                ):
                    try:
                        pin_info = os.fstat(measured_fd)
                    except OSError:
                        pin_info = None
                    else:
                        live = (pin_info.st_dev, pin_info.st_ino) == (
                            owned_pin_info.st_dev,
                            owned_pin_info.st_ino,
                        )
                        identity_check["product_dev"] = pin_info.st_dev
                        identity_check["product_ino"] = pin_info.st_ino
                identity_check["product_live"] = live
            return real_stat(
                path,
                *args,
                dir_fd=dir_fd,
                follow_symlinks=follow_symlinks,
                **kwargs,
            )

        monkeypatch.setattr(os, "link", racing_link)
        monkeypatch.setattr(os, "stat", identity_stat)

        try:
            bundle.publish(part=part, final_basename=_STDOUT_FINAL)
        except Exception as exc:
            publish_exc = exc
        else:
            publish_exc = None

        assert swapped["done"] is True
        foreign_fd = swapped["foreign_fd"]
        assert isinstance(foreign_fd, int)
        foreign_held = os.fstat(foreign_fd)
        os.lseek(foreign_fd, 0, os.SEEK_SET)
        foreign_held_bytes = os.read(foreign_fd, len(_FOREIGN_BYTES) + 1)
        conflict_raised = (
            publish_exc is not None
            and getattr(publish_exc, "reason_code", None) == _REASON_CONFLICT
            and str(publish_exc) == _REASON_CONFLICT
        )
        foreign_dirent_intact = False
        if final_path.exists() and not final_path.is_symlink():
            final_info = final_path.lstat()
            foreign_dirent_intact = (
                final_path.read_bytes() == _FOREIGN_BYTES
                and foreign_held_bytes == _FOREIGN_BYTES
                and (final_info.st_dev, final_info.st_ino)
                == (swapped["foreign_dev"], swapped["foreign_ino"])
                and (foreign_held.st_dev, foreign_held.st_ino)
                == (swapped["foreign_dev"], swapped["foreign_ino"])
            )
        product_fd_live_at_identity = (
            identity_check["seen"] is True
            and identity_check["product_live"] is True
            and identity_check["product_fd"] is product_pin_fd
            and identity_check["product_fd"] is not original_fd
            and identity_check["product_fd"] is not foreign_fd
            and (identity_check["product_dev"], identity_check["product_ino"])
            == (owned_pin_info.st_dev, owned_pin_info.st_ino)
        )
        product_fd_after = part._fd
        product_fd_closed_after_conflict = False
        if product_fd_after is None:
            product_fd_closed_after_conflict = True
        elif type(product_fd_after) is int:
            try:
                os.fstat(product_fd_after)
            except OSError:
                product_fd_closed_after_conflict = True
        if (
            not conflict_raised
            or not foreign_dirent_intact
            or not product_fd_live_at_identity
            or not product_fd_closed_after_conflict
        ):
            pytest.fail(
                "rebound 07B conflict: foreign Final-Dirent changed, consumed or not quarantined"
            )

        assert publish_exc is not None
        for snippet in roots.snippets():
            assert snippet not in str(publish_exc)
        held_info = os.fstat(original_fd)
        assert (held_info.st_dev, held_info.st_ino) == (
            original_info.st_dev,
            original_info.st_ino,
        )
        os.lseek(original_fd, 0, os.SEEK_SET)
        assert os.read(original_fd, len(_STDOUT_BYTES) + 1) == _STDOUT_BYTES
        assert (foreign_held.st_dev, foreign_held.st_ino) == (
            swapped["foreign_dev"],
            swapped["foreign_ino"],
        )
        assert foreign_held_bytes == _FOREIGN_BYTES
        assert part_path.exists()
        assert not part_path.is_symlink()
        assert part_path.read_bytes() == _FOREIGN_BYTES
        assert (part_path.lstat().st_dev, part_path.lstat().st_ino) == (
            swapped["foreign_dev"],
            swapped["foreign_ino"],
        )
        assert final_path.exists()
        assert not final_path.is_symlink()
        assert final_path.read_bytes() == _FOREIGN_BYTES
        assert (final_path.lstat().st_dev, final_path.lstat().st_ino) == (
            swapped["foreign_dev"],
            swapped["foreign_ino"],
        )

        try:
            bundle.write_part(part=part, payload=_STDOUT_CHUNK_B)
        except Exception as exc:
            write_exc = exc
        else:
            write_exc = None
        write_conflict = (
            write_exc is not None
            and getattr(write_exc, "reason_code", None) == _REASON_CONFLICT
            and str(write_exc) == _REASON_CONFLICT
        )
        if (
            not write_conflict
            or not foreign_dirent_intact
            or not product_fd_closed_after_conflict
        ):
            pytest.fail(
                "rebound 07B conflict: foreign Final-Dirent changed, consumed or not quarantined"
            )
        assert write_exc is not None
        for snippet in roots.snippets():
            assert snippet not in str(write_exc)
        os.lseek(original_fd, 0, os.SEEK_SET)
        assert os.read(original_fd, len(_STDOUT_BYTES) + 1) == _STDOUT_BYTES
        os.lseek(foreign_fd, 0, os.SEEK_SET)
        assert os.read(foreign_fd, len(_FOREIGN_BYTES) + 1) == _FOREIGN_BYTES
        assert part_path.read_bytes() == _FOREIGN_BYTES
        assert final_path.exists()
        assert not final_path.is_symlink()
        assert final_path.read_bytes() == _FOREIGN_BYTES
        assert (final_path.lstat().st_dev, final_path.lstat().st_ino) == (
            swapped["foreign_dev"],
            swapped["foreign_ino"],
        )
    finally:
        os.close(original_fd)
        foreign_fd = swapped["foreign_fd"]
        if isinstance(foreign_fd, int):
            os.close(foreign_fd)


def test_publish_keeps_product_fd_live_through_identity_check_and_first_directory_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _layout(tmp_path)
    bundle = _reserve(roots)
    directory = _assert_bundle_home(bundle, roots)
    part = _written_part(bundle, basename=_STDOUT_PART, payload=_STDOUT_BYTES)
    part_path = directory / _STDOUT_PART
    final_path = directory / _STDOUT_FINAL
    real_stat = os.stat
    real_fsync = os.fsync
    product_pin_fd = part._fd
    assert type(product_pin_fd) is int
    owned_info = os.fstat(product_pin_fd)
    probes: dict[str, Any] = {
        "identity_seen": False,
        "identity_live": False,
        "identity_fd": None,
        "identity_dev": None,
        "identity_ino": None,
        "first_dir_fsync_seen": False,
        "first_dir_fsync_live": False,
        "first_dir_fsync_fd": None,
        "first_dir_fsync_dev": None,
        "first_dir_fsync_ino": None,
    }

    def measure_product_fd() -> tuple[bool, object, object, object]:
        measured_fd = part._fd
        live = False
        dev: object = None
        ino: object = None
        if measured_fd is product_pin_fd and type(measured_fd) is int:
            try:
                pin_info = os.fstat(measured_fd)
            except OSError:
                pin_info = None
            else:
                live = (pin_info.st_dev, pin_info.st_ino) == (
                    owned_info.st_dev,
                    owned_info.st_ino,
                )
                dev = pin_info.st_dev
                ino = pin_info.st_ino
        return live, measured_fd, dev, ino

    def identity_stat(
        path: str | bytes | os.PathLike[str],
        *args: Any,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
        **kwargs: Any,
    ) -> os.stat_result:
        if (
            _base(path) == _STDOUT_FINAL
            and dir_fd is not None
            and not probes["identity_seen"]
        ):
            live, measured_fd, dev, ino = measure_product_fd()
            probes["identity_seen"] = True
            probes["identity_live"] = live
            probes["identity_fd"] = measured_fd
            probes["identity_dev"] = dev
            probes["identity_ino"] = ino
        return real_stat(
            path,
            *args,
            dir_fd=dir_fd,
            follow_symlinks=follow_symlinks,
            **kwargs,
        )

    def first_dir_fsync(fd: int) -> None:
        info = os.fstat(fd)
        if (
            probes["identity_seen"]
            and not probes["first_dir_fsync_seen"]
            and stat.S_ISDIR(info.st_mode)
        ):
            live, measured_fd, dev, ino = measure_product_fd()
            probes["first_dir_fsync_seen"] = True
            probes["first_dir_fsync_live"] = live
            probes["first_dir_fsync_fd"] = measured_fd
            probes["first_dir_fsync_dev"] = dev
            probes["first_dir_fsync_ino"] = ino
        return real_fsync(fd)

    monkeypatch.setattr(os, "stat", identity_stat)
    monkeypatch.setattr(os, "fsync", first_dir_fsync)

    bundle.publish(part=part, final_basename=_STDOUT_FINAL)

    pin_live_through_window = (
        probes["identity_seen"] is True
        and probes["first_dir_fsync_seen"] is True
        and probes["identity_live"] is True
        and probes["first_dir_fsync_live"] is True
        and probes["identity_fd"] is product_pin_fd
        and probes["first_dir_fsync_fd"] is product_pin_fd
        and (probes["identity_dev"], probes["identity_ino"])
        == (owned_info.st_dev, owned_info.st_ino)
        and (probes["first_dir_fsync_dev"], probes["first_dir_fsync_ino"])
        == (owned_info.st_dev, owned_info.st_ino)
    )
    if not pin_live_through_window:
        pytest.fail(
            "product FD not live through final identity check and first directory fsync"
        )

    assert not part_path.exists()
    assert final_path.exists()
    assert not final_path.is_symlink()
    assert final_path.read_bytes() == _STDOUT_BYTES
    try:
        bundle.write_part(part=part, payload=_STDOUT_CHUNK_B)
    except Exception as exc:
        write_exc = exc
    else:
        write_exc = None
    write_conflict = (
        write_exc is not None
        and getattr(write_exc, "reason_code", None) == _REASON_CONFLICT
        and str(write_exc) == _REASON_CONFLICT
    )
    if not write_conflict:
        pytest.fail(
            "product FD not live through final identity check and first directory fsync"
        )
    assert write_exc is not None
    for snippet in roots.snippets():
        assert snippet not in str(write_exc)
    assert final_path.read_bytes() == _STDOUT_BYTES
