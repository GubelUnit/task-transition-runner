"""PUBLIC-P-02 direct-GREEN witness from two real Cursor processes.

Task A must end DRAINED_AWAITING_TASK with a canonically derived settled
basis. Task B must use the materialized Task A behavior, consume the
ordinary continuation, and end SETTLED. The dirty host checkout is never
the runner source.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import yaml


_CANDIDATE_ROOT = Path(__file__).resolve().parents[2]
_BASE_COMMIT = "3f836c0cb47c6af925d14e5258d0488ab374317a"
_BASE_TREE = "fb0124da699dd448fce9cf5146b49df2f9938f9f"
_MANIFEST_SHA256 = "3bb1bb7e0896287fd84c46d3ebe5d31195da3062f3de8453b8a1d14811000eec"
_CURSOR_VERSION = "2026.10.14-941492e"
_HEX64 = set("0123456789abcdef")
_FORBIDDEN_RECEIPT_MARKERS = (
    "hydra",
    "gaia",
    "runenschmied",
    "orp",
    "/home/",
    "session_id",
    "gubel",
)
_TASK_A_PRODUCT = "src/reference_app/labels.py"
_TASK_B_PRODUCT = "src/reference_app/summary.py"
_CONTRACT_A = "tests/test_labels.py"
_CONTRACT_B = "tests/test_summary.py"
_BASELINE_TEST = "tests/test_environment.py"
_TASK_A_VALIDATION = "python -m pytest -q -p no:cacheprovider tests/test_labels.py"
_TASK_B_VALIDATION = "python -m pytest -q -p no:cacheprovider tests/test_summary.py"


def _stop(reason_code: str, detail: str) -> None:
    pytest.fail(f"{reason_code}: {detail}")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=check,
        capture_output=True,
        text=True,
    )


def _git_text(repo: Path, *args: str) -> str:
    return _git(repo, *args).stdout.strip()


def _load_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", f"yaml mapping missing: {path.name}")
    return loaded


def _root_identity(path: Path) -> dict[str, Any]:
    stat = path.resolve().stat()
    return {
        "path_sha256": _sha256(str(path.resolve()).encode("utf-8")),
        "device": stat.st_dev,
        "inode": stat.st_ino,
    }


def _require_environment() -> tuple[Path, Path]:
    if os.environ.get("OSS7_PUBLIC_P02_WITNESS") != "1":
        pytest.skip("OSS7_PUBLIC_P02_WITNESS is required")
    evidence = os.environ.get("PUBLIC_P02_EVIDENCE_ROOT")
    work = os.environ.get("PUBLIC_P02_WORK_ROOT")
    if not evidence or not work:
        pytest.skip("PUBLIC_P02_EVIDENCE_ROOT and PUBLIC_P02_WORK_ROOT are required")
    evidence_root = Path(evidence)
    work_root = Path(work)
    if evidence_root.exists() or work_root.exists():
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "bound roots already exist")
    if _git_text(_CANDIDATE_ROOT, "rev-parse", "HEAD") != _BASE_COMMIT:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "candidate HEAD drifted")
    if _git_text(_CANDIDATE_ROOT, "rev-parse", "HEAD^{tree}") != _BASE_TREE:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "candidate tree drifted")
    manifest = _CANDIDATE_ROOT / "examples/reference/bundle_manifest.json"
    if _sha256(manifest.read_bytes()) != _MANIFEST_SHA256:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "bundle manifest drifted")
    resolved_cursor = shutil_which("cursor-agent")
    if resolved_cursor is None or Path(resolved_cursor).name != "cursor-agent":
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "cursor executable drifted")
    completed = subprocess.run(
        [resolved_cursor, "--version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.stdout.strip() != _CURSOR_VERSION:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "cursor version drifted")
    return evidence_root, work_root


def shutil_which(name: str) -> str | None:
    return __import__("shutil").which(name)


def _assemble(candidate: Path, input_root: Path, seed_root: Path) -> str:
    document = json.loads(
        (candidate / "examples/reference/bundle_manifest.json").read_text(encoding="utf-8")
    )
    entries = document["entries"]
    copied: dict[str, bytes] = {}
    for entry in entries:
        source = candidate / str(entry["source"])
        payload = source.read_bytes()
        if _sha256(payload) != entry["sha256"] or len(payload) != entry["byte_length"]:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", f"manifest entry drifted: {entry['source']}")
        root = input_root if entry["destination_root"] == "input" else seed_root
        if entry["destination_root"] not in {"input", "seed"}:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "unexpected destination root")
        destination = root / str(entry["destination"])
        if not destination.resolve().is_relative_to(root.resolve()):
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "destination escapes assembly root")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        copied[f"{entry['destination_root']}/{entry['destination']}"] = payload
    digest = hashlib.sha256()
    for relative in sorted(copied):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(copied[relative]).digest())
    return digest.hexdigest()


def _scan_seed(seed_root: Path) -> None:
    files = {path.relative_to(seed_root).as_posix() for path in seed_root.rglob("*") if path.is_file()}
    if ".git" in files or any(name.startswith(".git/") for name in files):
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "seed contains git history")
    if _TASK_A_PRODUCT in files or _TASK_B_PRODUCT in files:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "seed contains a product implementation")
    required = {
        "pyproject.toml",
        "src/reference_app/__init__.py",
        _BASELINE_TEST,
        _CONTRACT_A,
        _CONTRACT_B,
    }
    if files != required:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "seed file set drifted")


def _copy_tree(source: Path, destination: Path) -> None:
    for path in source.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())


def _init_target(seed_root: Path, target: Path) -> str:
    _copy_tree(seed_root, target)
    _git(target, "init", "-b", "main")
    _git(target, "config", "user.name", "Public Reference Baseline")
    _git(target, "config", "user.email", "reference@example.com")
    _git(target, "add", "--all")
    _git(target, "commit", "-m", "neutral baseline")
    if _git_text(target, "rev-list", "--count", "HEAD") != "1":
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "target baseline is not a single commit")
    if (target / _TASK_A_PRODUCT).exists() or (target / _TASK_B_PRODUCT).exists():
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "baseline commit contains product bytes")
    return _git_text(target, "rev-parse", "HEAD")


def _clone_source(candidate: Path, source_root: Path) -> None:
    subprocess.run(
        ["git", "clone", "--quiet", "--no-hardlinks", str(candidate), str(source_root)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(source_root, "checkout", "--detach", _BASE_COMMIT)
    if _git_text(source_root, "rev-parse", "HEAD") != _BASE_COMMIT:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "clean source HEAD drifted")
    if _git_text(source_root, "status", "--porcelain") != "":
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "clean source is dirty")


def _argv0_token(cmdline: bytes) -> bytes:
    if cmdline == b"":
        return b""
    return cmdline.split(b"\0", 1)[0]


def _argv0_basename(token: bytes) -> bytes:
    if token == b"" or token.endswith(b"/"):
        return b""
    return token.rsplit(b"/", 1)[-1]


def _resolved_cursor_executable() -> str | None:
    resolved = shutil_which("cursor-agent")
    if resolved is None or Path(resolved).name != "cursor-agent":
        return None
    try:
        canonical = os.path.realpath(resolved)
    except OSError:
        return None
    if Path(canonical).name != "cursor-agent":
        return None
    return canonical


def _exact_argv0_is_cursor(cmdline: bytes) -> bool:
    return _argv0_basename(_argv0_token(cmdline)) == b"cursor-agent"


def _exact_executable_is_cursor(pid: int, canonical: str | None) -> bool:
    if canonical is None:
        return False
    link = Path("/proc") / str(pid) / "exe"
    try:
        executable = os.path.realpath(link)
    except OSError:
        return False
    return executable == canonical


def _is_cursor_process(pid: int, cmdline: bytes, canonical: str | None) -> bool:
    if _exact_argv0_is_cursor(cmdline):
        return True
    return _exact_executable_is_cursor(pid, canonical)


def _cursor_pids() -> set[int]:
    found: set[int] = set()
    canonical = _resolved_cursor_executable()
    proc = Path("/proc")
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if _is_cursor_process(int(entry.name), raw, canonical):
            found.add(int(entry.name))
    return found


class _CursorWatch:
    def __init__(self) -> None:
        self.pids: set[int] = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.pids.update(_cursor_pids())
            self._stop.wait(0.05)

    def __enter__(self) -> _CursorWatch:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        self._thread.join()


def _read_cmdline(pid: int) -> bytes | None:
    try:
        return (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return None


def _end_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            process.kill()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def test_cursor_process_identity_classifier() -> None:
    canonical = _resolved_cursor_executable()
    if canonical is None:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "cursor executable drifted")
    bash = shutil_which("bash")
    sleep_bin = shutil_which("sleep")
    if bash is None or sleep_bin is None:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "process identity probe tools missing")
    canonical_bytes = os.fsencode(canonical)
    if not _exact_argv0_is_cursor(canonical_bytes + b"\0--version\0"):
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "resolved cursor argv0 form was rejected")
    later_argument = b"/bin/bash\0-c\0sleep 30 # cursor-agent\0"
    if _exact_argv0_is_cursor(later_argument):
        _stop("PROCESS_BOUNDARY_CONTAMINATED", "later-argument cursor text matched argv0 identity")
    lookalike = b"/tmp/cursor-agent-observer\0--flag\0"
    if _exact_argv0_is_cursor(lookalike):
        _stop("PROCESS_BOUNDARY_CONTAMINATED", "argv0 lookalike matched exact cursor basename")
    positive = subprocess.Popen(
        [bash, "-c", 'exec -a "$1" "$2" 30', "cursor-form", canonical, sleep_bin],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    negative = subprocess.Popen(
        [bash, "-c", "while true; do sleep 1; done # cursor-agent", "observer", canonical],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 2.0
        positive_raw: bytes | None = None
        while time.monotonic() < deadline:
            raw = _read_cmdline(positive.pid)
            if raw is not None and _argv0_token(raw) == canonical_bytes:
                positive_raw = raw
                break
            time.sleep(0.01)
        if positive_raw is None:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "real cursor argv0 form was not observed")
        if not _exact_argv0_is_cursor(positive_raw):
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "real cursor argv0 form was rejected")
        if _exact_executable_is_cursor(positive.pid, canonical):
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "argv0 probe was accepted through executable identity")
        negative_raw = _read_cmdline(negative.pid)
        if negative_raw is None:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "observer shell cmdline disappeared")
        negative_argv0, _, negative_tail = negative_raw.partition(b"\0")
        if _argv0_basename(negative_argv0) == b"cursor-agent" or b"cursor-agent" not in negative_tail:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "observer shell did not carry later cursor text")
        if _is_cursor_process(negative.pid, negative_raw, canonical):
            _stop("PROCESS_BOUNDARY_CONTAMINATED", "observer shell classified as cursor")
        observed = _cursor_pids()
        if positive.pid not in observed:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "real cursor form missing from cursor pid set")
        if negative.pid in observed:
            _stop("PROCESS_BOUNDARY_CONTAMINATED", "observer shell entered cursor pid set")
    finally:
        _end_process_group(positive)
        _end_process_group(negative)


def _invoke_cli(
    source_root: Path,
    config_path: Path,
    command: str,
) -> tuple[dict[str, Any], set[int], str]:
    env = os.environ.copy()
    env.pop("CURSOR_API_ENDPOINT", None)
    env["PYTHONPATH"] = str(source_root / "src")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["GIT_AUTHOR_NAME"] = "Public Reference Baseline"
    env["GIT_AUTHOR_EMAIL"] = "reference@example.com"
    env["GIT_COMMITTER_NAME"] = "Public Reference Baseline"
    env["GIT_COMMITTER_EMAIL"] = "reference@example.com"
    watch = _CursorWatch()
    before = _cursor_pids()
    with watch:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys\nfrom task_transition_runner.cli import main\nraise SystemExit(main(sys.argv[1:]))",
                command,
                "--config",
                str(config_path),
            ],
            cwd=source_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
    observed = watch.pids - before
    loaded = yaml.safe_load(completed.stdout) if completed.stdout.strip() else None
    detail = completed.stderr[-500:]
    if not isinstance(loaded, dict):
        _stop(
            "REFERENCE_A_TO_B_WITNESS_INCOMPLETE",
            f"{command} returned no outcome mapping: {detail}",
        )
    return loaded, observed, detail


def _run_cli(source_root: Path, config_path: Path, command: str) -> tuple[dict[str, Any], set[int], str]:
    return _invoke_cli(source_root, config_path, command)


def _worker_files(runtime: Path) -> list[Path]:
    return sorted((runtime / "runs").glob("*/worker_result.yaml"))


def _worker_for(runtime: Path, task_id: str) -> dict[str, Any]:
    matches = []
    for path in _worker_files(runtime):
        payload = _load_yaml(path)
        if payload.get("task_id") == task_id:
            matches.append(payload)
    if len(matches) != 1:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", f"expected one worker for {task_id}")
    return matches[0]


def _write_decisions(runtime: Path, worker: dict[str, Any]) -> None:
    folder = runtime / "runs" / str(worker["origin_run_id"]) / "decisions"
    folder.mkdir(parents=True, exist_ok=True)
    shared = {
        "schema_version": "1.0",
        "task_id": worker["task_id"],
        "origin_run_id": worker["origin_run_id"],
        "promotion_attempt_id": worker["promotion_attempt_id"],
        "result_sha": worker["result_sha"],
        "ready": True,
        "evidence": [{"kind": "git_result", "ref": worker["result_sha"]}],
    }
    orchestrator = {"decision_type": "orchestrator", "promotion_class": "GREEN_PROMOTION", **shared}
    validator = {"decision_type": "validator", "verdict": "GO", **shared}
    (folder / "orchestrator_decision.yaml").write_text(
        yaml.safe_dump(orchestrator, sort_keys=False),
        encoding="utf-8",
    )
    (folder / "validator_decision.yaml").write_text(
        yaml.safe_dump(validator, sort_keys=False),
        encoding="utf-8",
    )


def _derive_basis(source_root: Path, config_path: Path) -> dict[str, Any]:
    program = """
import json, os, sys
from pathlib import Path
sys.path.insert(0, os.environ["CLEAN_SRC"])
from task_transition_runner.cli import _canonical_settled_predecessor_basis
from task_transition_runner.config import load_config
basis = _canonical_settled_predecessor_basis(load_config(Path(os.environ["RUN_CONFIG"])))
json.dump(basis, sys.stdout)
"""
    env = os.environ.copy()
    env["CLEAN_SRC"] = str(source_root / "src")
    env["RUN_CONFIG"] = str(config_path)
    env["PYTHONPATH"] = str(source_root / "src")
    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=source_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "canonical basis derivation failed")
    loaded = json.loads(completed.stdout)
    if not isinstance(loaded, dict) or loaded.get("kind") != "SETTLED_PROMOTION":
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "basis kind is not SETTLED_PROMOTION")
    return loaded


def _declared_validation_commands(document: dict[str, Any]) -> list[str]:
    active = document.get("active_task")
    validation = active.get("validation") if isinstance(active, dict) else None
    commands = validation.get("commands") if isinstance(validation, dict) else None
    if not isinstance(commands, list) or not commands:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "validation commands missing")
    resolved: list[str] = []
    for entry in commands:
        command = entry.get("command") if isinstance(entry, dict) else None
        if not isinstance(command, str) or command.strip() == "":
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "validation command missing")
        resolved.append(command)
    return resolved


def _drop_test_caches(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_dir() and path.name in {"__pycache__", ".pytest_cache"}:
            for child in sorted(path.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink()
            for child in sorted(path.rglob("*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            path.rmdir()


def _run_declared_command(target: Path, command: str) -> dict[str, Any]:
    argv = command.split()
    if argv and argv[0] == "python":
        argv[0] = sys.executable
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTEST_ADDOPTS"] = ""
    env.pop("COVERAGE_PROCESS_START", None)
    for inherited_name in list(env):
        if inherited_name.startswith("COV_CORE_"):
            env.pop(inherited_name, None)
    completed = subprocess.run(
        argv,
        cwd=target,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    _drop_test_caches(target)
    output = (completed.stdout + completed.stderr).encode("utf-8")
    return {
        "command": command,
        "exit_code": completed.returncode,
        "output_sha256": _sha256(output),
        "byte_length": len(output),
    }


def _assert_product_uses_labels(target: Path) -> None:
    source = (target / _TASK_B_PRODUCT).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = False
    used = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported = imported or any(alias.name.startswith("reference_app.labels") for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported = imported or node.module == "reference_app.labels" or (
                node.module == "reference_app" and any(alias.name == "labels" for alias in node.names)
            )
        if isinstance(node, ast.FunctionDef) and node.name == "public_summary":
            if len(node.args.args) != 1:
                continue
            parameter = node.args.args[0].arg
            used = any(
                isinstance(item, ast.Name) and item.id == parameter and isinstance(item.ctx, ast.Load)
                for item in ast.walk(node)
            )
    if not imported or not used:
        _stop("REFERENCE_TASK_B_CONTRACT_NOT_PROVEN", "summary does not use the labels result")


def _promotion_cycle(
    source_root: Path,
    config_path: Path,
    runtime: Path,
    task_id: str,
) -> tuple[dict[str, Any], dict[str, Any], set[int]]:
    awaiting, cursor_pids, awaiting_detail = _run_cli(source_root, config_path, "run")
    if awaiting.get("outcome") != "AWAITING_DECISIONS":
        _stop(
            "REFERENCE_A_TO_B_WITNESS_INCOMPLETE",
            f"{task_id} did not reach decisions: {awaiting.get('reason_code')} {awaiting_detail}",
        )
    if not cursor_pids:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", f"{task_id} did not spawn cursor-agent")
    worker = _worker_for(runtime, task_id)
    _write_decisions(runtime, worker)
    pending, _pending_pids, pending_detail = _run_cli(source_root, config_path, "run")
    if pending.get("outcome") != "PENDING_PROMOTION":
        _stop(
            "REFERENCE_A_TO_B_WITNESS_INCOMPLETE",
            f"{task_id} did not persist pending: {pending.get('reason_code')} {pending_detail}",
        )
    drained, _drain_pids, _drain_detail = _run_cli(source_root, config_path, "run")
    return awaiting, drained, cursor_pids


def _public_outcome(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "outcome": payload.get("outcome"),
        "reason_code": payload.get("reason_code"),
        "status": payload.get("status"),
    }


def _validation_digest(runtime: Path, origin_run_id: str) -> dict[str, Any]:
    path = runtime / "runs" / origin_run_id / "validation" / "baseline-validation.yaml"
    if not path.is_file():
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "persisted runner validation is missing")
    payload = _load_yaml(path)
    command = payload.get("command")
    if command != ["true"]:
        _stop("REFERENCE_BASELINE_CONTRACT_LEAK", "runner validation is not the init baseline")
    log = runtime / str(payload["output_ref"])
    return {
        "command": command,
        "exit_code": payload.get("exit_code"),
        "output_sha256": payload.get("output_sha256"),
        "log_sha256": _sha256(log.read_bytes()),
    }


def _assert_basis(basis: dict[str, Any], worker: dict[str, Any]) -> None:
    if set(basis) != {"kind", "predecessor"}:
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "basis keys drifted")
    predecessor = basis["predecessor"]
    expected = {
        "settled",
        "task_id",
        "origin_run_id",
        "promotion_attempt_id",
        "task_fingerprint",
        "instruction_sources",
    }
    if set(predecessor) != expected or predecessor["settled"] is not True:
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "predecessor projection drifted")
    if predecessor["task_id"] != worker["task_id"]:
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "basis task identity drifted")
    if predecessor["origin_run_id"] != worker["origin_run_id"]:
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "basis origin drifted")
    fingerprint = predecessor["task_fingerprint"]
    if not (isinstance(fingerprint, str) and len(fingerprint) == 64 and set(fingerprint) <= _HEX64):
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "task fingerprint is not canonical")
    sources = predecessor["instruction_sources"]
    if not isinstance(sources, list) or not sources:
        _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "instruction sources are missing")
    for source in sources:
        if set(source) != {"logical_ref", "sha256", "byte_length"}:
            _stop("ACTIVATION_PREDECESSOR_UNAVAILABLE", "instruction source keys drifted")


def _tock_sha(runtime: Path, origin_run_id: str) -> str:
    journal = _load_yaml(runtime / "promotion_journal.yaml")
    if journal.get("origin_run_id") != origin_run_id:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "journal origin drifted")
    for field in ("task_fingerprint", "task_snapshot_sha256", "instruction_signature_sha256"):
        value = journal.get(field)
        if not (isinstance(value, str) and len(value) == 64 and set(value) <= _HEX64):
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", f"journal anchor missing: {field}")
    tock = journal.get("tock_sha")
    if not isinstance(tock, str) or len(tock) != 40:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "tock identity missing")
    return tock


def _write_receipt(evidence_root: Path, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    lowered = encoded.decode("utf-8").lower()
    for marker in _FORBIDDEN_RECEIPT_MARKERS:
        if marker in lowered:
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "receipt redaction failed")
    destination = evidence_root / "public_p_02_trace.json"
    destination.write_bytes(encoded)


@pytest.mark.external_witness
def test_public_p02_direct_green_a_to_b() -> None:
    evidence_root, work_root = _require_environment()
    evidence_root.mkdir()
    work_root.mkdir()
    source_root = work_root / "runner-source"
    input_root = work_root / "input"
    seed_root = work_root / "seed"
    input_root.mkdir()
    seed_root.mkdir()
    _clone_source(_CANDIDATE_ROOT, source_root)
    assembled = _assemble(_CANDIDATE_ROOT, input_root, seed_root)
    _scan_seed(seed_root)
    config_path = input_root / "runner.yaml"
    config = _load_yaml(config_path)
    baseline_command = config["baseline_validation"]["command"]
    if baseline_command != "true" or "reference_task" in baseline_command:
        _stop("REFERENCE_BASELINE_CONTRACT_LEAK", "init baseline is not separated")
    if config["adapter"]["executable"] != "cursor-agent":
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "adapter executable is not cursor-agent")
    if config["adapter"]["timeout_seconds"] != 1800:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "frozen adapter timeout drifted")
    target = input_root / "target"
    runtime = input_root / "runtime"
    target.mkdir()
    runtime.mkdir()
    for left, right in (
        (source_root, target),
        (source_root, runtime),
        (source_root, evidence_root),
        (source_root, seed_root),
        (target, runtime),
        (target, evidence_root),
        (target, seed_root),
        (runtime, evidence_root),
        (evidence_root, input_root),
    ):
        if left.resolve().is_relative_to(right.resolve()) or right.resolve().is_relative_to(left.resolve()):
            _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "proof roots overlap")
    baseline_commit = _init_target(seed_root, target)
    task_a_commands = _declared_validation_commands(_load_yaml(input_root / "tasks.yaml"))
    task_b_commands = _declared_validation_commands(
        _load_yaml(input_root / "task_sources" / "task_b.yaml")
    )
    if task_a_commands != [_TASK_A_VALIDATION]:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "task A command is not the public labels contract")
    if task_b_commands != [_TASK_A_VALIDATION, _TASK_B_VALIDATION]:
        _stop(
            "REFERENCE_A_TO_B_WITNESS_INCOMPLETE",
            "task B commands are not labels then summary",
        )
    baseline = _run_declared_command(
        target,
        f"python -m pytest -q -p no:cacheprovider {_BASELINE_TEST}",
    )
    if baseline["exit_code"] != 0:
        _stop("REFERENCE_BASELINE_CONTRACT_LEAK", "seed baseline is not green")
    unmet_a = _run_declared_command(target, task_a_commands[0])
    if unmet_a["exit_code"] == 0:
        _stop("REFERENCE_BASELINE_CONTRACT_LEAK", "task A contract already passes")
    if _git_text(target, "status", "--porcelain") != "":
        _stop("REFERENCE_BASELINE_CONTRACT_LEAK", "baseline checks dirtied the target")
    init_payload, init_pids, init_detail = _run_cli(source_root, config_path, "init")
    if init_payload.get("outcome") != "INITIALIZED" or init_pids:
        _stop(
            "REFERENCE_A_TO_B_WITNESS_INCOMPLETE",
            f"init did not stay a baseline-only runner process: {init_payload.get('reason_code')} {init_detail}",
        )
    if (target / _TASK_A_PRODUCT).exists():
        _stop("REFERENCE_BASELINE_CONTRACT_LEAK", "init materialized task A")
    task_a_bytes = (input_root / "tasks.yaml").read_bytes()
    _awaiting_a, drained_a, cursor_a = _promotion_cycle(
        source_root,
        config_path,
        runtime,
        "reference-task-a",
    )
    if drained_a.get("outcome") != "DRAINED_AWAITING_TASK":
        _stop(
            "REFERENCE_A_TO_B_WITNESS_INCOMPLETE",
            f"task A did not drain: {drained_a.get('reason_code')}",
        )
    if drained_a.get("outcome") == "SETTLED":
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "task A claimed SETTLED")
    worker_a = _worker_for(runtime, "reference-task-a")
    if list(worker_a.get("changed_paths", [])) != [_TASK_A_PRODUCT]:
        _stop("PROMOTION_CHANGED_PATH_OUTSIDE_ALLOWED", "task A changed paths drifted")
    if (runtime / "post_tick_continuation.yaml").exists():
        _stop("CONTINUATION_NOT_CONSUMED", "task A created a continuation")
    basis = _derive_basis(source_root, config_path)
    _assert_basis(basis, worker_a)
    tock_a = _tock_sha(runtime, str(worker_a["origin_run_id"]))
    if tock_a not in _git_text(target, "rev-list", "--first-parent", "main"):
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "task A tock is not on main")
    contract_a = _run_declared_command(target, task_a_commands[0])
    if contract_a["exit_code"] != 0:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "task A contract failed")
    if _git_text(target, "status", "--porcelain") != "":
        _stop("PROMOTION_CHANGED_PATH_OUTSIDE_ALLOWED", "task A contract dirtied the target")
    task_b_source = input_root / "task_sources" / "task_b.yaml"
    (input_root / "tasks.yaml").write_bytes(task_b_source.read_bytes())
    _awaiting_b, settled_b, cursor_b = _promotion_cycle(
        source_root,
        config_path,
        runtime,
        "reference-task-b",
    )
    if settled_b.get("outcome") != "SETTLED" or settled_b.get("reason_code") != "CONTINUATION_CONSUMED":
        _stop(
            "CONTINUATION_NOT_CONSUMED",
            f"task B outcome drifted: {settled_b.get('outcome')} {settled_b.get('reason_code')}",
        )
    if cursor_a & cursor_b or not cursor_b:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "task B did not use a second cursor process")
    worker_b = _worker_for(runtime, "reference-task-b")
    if list(worker_b.get("changed_paths", [])) != [_TASK_B_PRODUCT]:
        _stop("PROMOTION_CHANGED_PATH_OUTSIDE_ALLOWED", "task B changed paths drifted")
    _assert_product_uses_labels(target)
    contract_a_again = _run_declared_command(target, task_b_commands[0])
    contract_b = _run_declared_command(target, task_b_commands[1])
    if contract_a_again["exit_code"] != 0 or contract_b["exit_code"] != 0:
        _stop("REFERENCE_TASK_B_CONTRACT_NOT_PROVEN", "task B did not rerun both contracts")
    if (target / _CONTRACT_A).read_bytes() != (seed_root / _CONTRACT_A).read_bytes():
        _stop("PROMOTION_CHANGED_PATH_OUTSIDE_ALLOWED", "task A contract bytes changed")
    if (target / _CONTRACT_B).read_bytes() != (seed_root / _CONTRACT_B).read_bytes():
        _stop("PROMOTION_CHANGED_PATH_OUTSIDE_ALLOWED", "task B contract bytes changed")
    tock_b = _tock_sha(runtime, str(worker_b["origin_run_id"]))
    if tock_a == tock_b:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "task B reused the task A tock")
    continuation = _load_yaml(runtime / "post_tick_continuation.yaml")
    if continuation.get("status") != "CONSUMED":
        _stop("CONTINUATION_NOT_CONSUMED", "continuation status is not CONSUMED")
    if _git_text(source_root, "rev-parse", "HEAD") != _BASE_COMMIT:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "clean source moved")
    if _git_text(source_root, "status", "--porcelain") != "":
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "clean source became dirty")
    if _git_text(_CANDIDATE_ROOT, "rev-parse", "HEAD") != _BASE_COMMIT:
        _stop("REFERENCE_A_TO_B_WITNESS_INCOMPLETE", "candidate moved during the witness")
    receipt = {
        "kind": "OSS7_SUPPORTED_CURSOR_A_TO_B",
        "status": "PASS",
        "candidate_binding": {
            "candidate_commit": _BASE_COMMIT,
            "candidate_tree": _BASE_TREE,
        },
        "bundle_binding": {
            "bundle_manifest_sha256": _MANIFEST_SHA256,
            "assembled_input_sha256": assembled,
            "initial_task_source_sha256": _sha256(task_a_bytes),
            "task_b_source_sha256": _sha256(task_b_source.read_bytes()),
        },
        "fresh_roots": {
            "input_root": _root_identity(input_root),
            "target_root": _root_identity(target),
            "runtime_root": _root_identity(runtime),
            "evidence_root": _root_identity(evidence_root),
        },
        "runner_source": {
            "commit": _BASE_COMMIT,
            "clean": True,
            "path_sha256": _root_identity(source_root)["path_sha256"],
        },
        "baseline": {
            "commit": baseline_commit,
            "init_command": baseline_command,
            "init_outcome": _public_outcome(init_payload),
            "environment_check": baseline,
        },
        "task_a": {
            "worker": "REAL_CURSOR_PROCESS",
            "cursor_pids": sorted(cursor_a),
            "required_outcome": "DRAINED_AWAITING_TASK",
            "observed_outcome": _public_outcome(drained_a),
            "settled_basis": "CANONICAL_A5_DERIVED",
            "basis_kind": basis["kind"],
            "basis_task_id": basis["predecessor"]["task_id"],
            "result_sha": worker_a["result_sha"],
            "tock_sha": tock_a,
            "changed_paths": worker_a["changed_paths"],
            "contracts": ["TASK_A_ONLY"],
            "contract_result": contract_a,
            "persisted_runner_validation": _validation_digest(runtime, str(worker_a["origin_run_id"])),
        },
        "task_b": {
            "worker": "SECOND_REAL_CURSOR_PROCESS",
            "cursor_pids": sorted(cursor_b),
            "required_outcome": "SETTLED",
            "required_reason": "CONTINUATION_CONSUMED",
            "observed_outcome": _public_outcome(settled_b),
            "dependency": "IMPORT_AND_USE_REAL_TASK_A_BEHAVIOR",
            "result_sha": worker_b["result_sha"],
            "tock_sha": tock_b,
            "changed_paths": worker_b["changed_paths"],
            "contracts": ["TASK_A", "TASK_B"],
            "contract_results": [contract_a_again, contract_b],
            "persisted_runner_validation": _validation_digest(runtime, str(worker_b["origin_run_id"])),
        },
        "ownership": {
            "accepted_rows": ["OSS7-B03", "OSS7-B15", "OSS7-B16"],
            "public_acceptance": ["PUBLIC-P-02"],
            "readiness_contributor": ["TASK_B_SUPPORTED_PATH"],
            "forbidden_acceptance": ["OSS7-B04", "PUBLIC-P-09"],
        },
    }
    _write_receipt(evidence_root, receipt)


def _load_postdecision_harness() -> Any:
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "test_cli_run.py"
    spec = importlib.util.spec_from_file_location(
        "postdecision_direct_green_harness",
        path,
    )
    if spec is None or spec.loader is None:
        raise AssertionError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_postdecision_baseline_validation_true_does_not_authorize_task_promotion(
    tmp_path: Path,
) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        baseline = Path(world["baseline_command"])
        baseline.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        (world["runtime"] / "manual_evidence.yaml").write_text(
            "kind: manual_evidence\nbaseline_validation: true\n",
            encoding="utf-8",
        )
        effect = harness._admit_sealed_pending(world)
        result = effect["result"]
        sealed_authority = (
            result.get("ok") is True
            and effect.get("tock_tree") == world["sealed_tree"]
            and effect.get("new_validation_files") == ()
        )
        if sealed_authority:
            return
        if result.get("reason_code") == "PROMOTION_VALIDATION_OUTCOME_MISMATCH" or (
            result.get("ok") is True and effect.get("new_validation_files")
        ):
            pytest.fail(harness._POSTDECISION_REPROOF_SIGNATURE)
        raise AssertionError((result, effect.get("new_validation_files"), effect.get("tock_tree")))
    finally:
        harness._assert_product_draft_stasis()


def test_postdecision_tock_tree_equals_sealed_predecision_candidate_tree(
    tmp_path: Path,
) -> None:
    harness = _load_postdecision_harness()
    try:
        world = harness._open_postdecision_pending(tmp_path)
        effect = harness._admit_sealed_pending(world)
        result = effect["result"]
        rebound = (
            result.get("ok") is True
            and effect.get("tock_tree") == world["sealed_tree"]
            and effect.get("new_validation_files") == ()
        )
        if rebound:
            return
        if effect.get("new_validation_files") or effect.get("tock_tree") != world["sealed_tree"]:
            pytest.fail(harness._POSTDECISION_REPROOF_SIGNATURE)
        raise AssertionError((result, effect.get("new_validation_files"), effect.get("tock_tree")))
    finally:
        harness._assert_product_draft_stasis()
