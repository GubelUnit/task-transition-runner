"""Synthetic candidate-only validation-entry contract."""

from __future__ import annotations

import re
import subprocess
import tempfile
import unittest
from pathlib import Path

_CANDIDATE_ROOT = Path(__file__).resolve().parent.parent
_VALIDATE_SCRIPT = _CANDIDATE_ROOT / "scripts" / "validate.sh"
_SMOKE_SELECTION = "tests/test_package_smoke.py"
_MISSING_SELECTION = "tests/this_selection_does_not_exist.py"
_FORBIDDEN_EXTERNAL_GATES = (
    "bandit",
    "pip-audit",
    "pip_audit",
    "terraform",
    "uvicorn",
    ".github",
    "fuzz",
)


def _require_validate_script() -> Path:
    if not _VALIDATE_SCRIPT.is_file():
        raise AssertionError("scripts/validate.sh fehlt")
    return _VALIDATE_SCRIPT


def _run_validate(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    script = _require_validate_script()
    return subprocess.run(
        ["bash", str(script), *args],
        cwd=str(cwd or _CANDIDATE_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


class ValidateEntryTests(unittest.TestCase):
    def test_validate_script_exists(self) -> None:
        _require_validate_script()

    def test_script_resolves_candidate_root_from_its_location(self) -> None:
        result = _run_validate(_SMOKE_SELECTION, cwd=Path(tempfile.gettempdir()))
        self.assertEqual(
            0,
            result.returncode,
            result.stdout + result.stderr,
        )

    def test_script_runs_candidate_pytest_with_coverage(self) -> None:
        text = _require_validate_script().read_text(encoding="utf-8")
        self.assertIn("poetry", text)
        self.assertIn("pytest", text)
        self.assertIn("--cov", text)
        self.assertIn("task_transition_runner", text)
        self.assertIn('"$@"', text)

    def test_script_propagates_arguments_and_exit_code(self) -> None:
        success = _run_validate(_SMOKE_SELECTION)
        self.assertEqual(0, success.returncode, success.stdout + success.stderr)
        self.assertTrue(
            _SMOKE_SELECTION in success.stdout or _SMOKE_SELECTION in success.stderr
        )

        failure = _run_validate(_MISSING_SELECTION)
        self.assertEqual(4, failure.returncode, failure.stdout + failure.stderr)
        self.assertTrue(
            _MISSING_SELECTION in failure.stdout or _MISSING_SELECTION in failure.stderr
        )

    def test_script_omits_external_gates_and_test_counts(self) -> None:
        text = _require_validate_script().read_text(encoding="utf-8")
        lowered = text.lower()
        for token in _FORBIDDEN_EXTERNAL_GATES:
            with self.subTest(token=token):
                self.assertNotIn(token.lower(), lowered)
        self.assertIsNone(
            re.search("|".join(f"/{segment}/" for segment in ("home", "users")), lowered)
        )
        self.assertIsNone(re.search(r"\b\d+\s+passed\b", lowered))
        self.assertIsNone(re.search(r"\bcollected\s+\d+\b", lowered))
