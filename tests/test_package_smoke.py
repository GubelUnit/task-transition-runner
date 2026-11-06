"""Synthetic package-namespace contract for the candidate skeleton."""

from __future__ import annotations

import importlib
import unittest


class PackageSmokeTests(unittest.TestCase):
    def test_task_transition_runner_namespace_is_importable(self) -> None:
        module = importlib.import_module("task_transition_runner")
        self.assertEqual(module.__name__, "task_transition_runner")
