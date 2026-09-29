#!/usr/bin/env python3

import os
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from decide_codec_benchmark_gate import decide  # noqa: E402

SCRIPT = Path(__file__).resolve().parent / "decide_codec_benchmark_gate.py"


class DecideTests(unittest.TestCase):
    def test_clean_first_pass_passes_without_confirmation(self) -> None:
        ok, _ = decide("success", "false", "skipped")
        self.assertTrue(ok)

    def test_confirmed_clear_passes(self) -> None:
        ok, _ = decide("success", "true", "success")
        self.assertTrue(ok)

    def test_confirmed_regression_fails(self) -> None:
        ok, reason = decide("success", "true", "failure")
        self.assertFalse(ok)
        self.assertIn("failure", reason)

    def test_skipped_confirmation_after_regression_fails(self) -> None:
        ok, _ = decide("success", "true", "skipped")
        self.assertFalse(ok)

    def test_cancelled_confirmation_fails(self) -> None:
        ok, _ = decide("success", "true", "cancelled")
        self.assertFalse(ok)

    def test_failed_first_pass_fails_regardless_of_confirmation(self) -> None:
        for confirmation in ("skipped", "success", "failure"):
            ok, _ = decide("failure", "", confirmation)
            self.assertFalse(ok, confirmation)

    def test_missing_decision_fails_closed(self) -> None:
        ok, reason = decide("success", "", "skipped")
        self.assertFalse(ok)
        self.assertIn("missing", reason)

    def test_unexpected_confirmation_after_clean_pass_fails(self) -> None:
        ok, _ = decide("success", "false", "success")
        self.assertFalse(ok)

    def test_exit_code_follows_decision(self) -> None:
        cases = (
            ({"FIRST_PASS": "success", "NEEDS_CONFIRMATION": "false", "CONFIRMATION": "skipped"}, 0),
            ({"FIRST_PASS": "success", "NEEDS_CONFIRMATION": "true", "CONFIRMATION": "failure"}, 1),
            ({}, 1),
        )
        for extra, expected in cases:
            env = {k: v for k, v in os.environ.items() if k not in
                   ("FIRST_PASS", "NEEDS_CONFIRMATION", "CONFIRMATION")}
            env.update(extra)
            result = subprocess.run(
                [sys.executable, str(SCRIPT)], env=env, capture_output=True, text=True
            )
            self.assertEqual(result.returncode, expected, result.stdout)


if __name__ == "__main__":
    unittest.main()
