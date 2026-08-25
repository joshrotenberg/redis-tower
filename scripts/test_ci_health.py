#!/usr/bin/env python3

import unittest

import ci_health


def run(minutes: int, *, attempt: int = 1, conclusion: str = "success") -> dict[str, object]:
    return {
        "status": "completed",
        "conclusion": conclusion,
        "run_attempt": attempt,
        "run_started_at": "2026-08-01T00:00:00Z",
        "updated_at": f"2026-08-01T00:{minutes:02}:00Z",
    }


class CiHealthTests(unittest.TestCase):
    def test_summary_tracks_wall_clock_reruns_and_failures(self) -> None:
        payload = ci_health.summarize(
            [run(4), run(5, attempt=2), run(8, conclusion="failure")],
            max_p95_minutes=10,
            max_rerun_rate=0.5,
        )

        self.assertEqual(payload["window"], 3)
        self.assertEqual(payload["wall_clock_minutes"]["p50"], 5)
        self.assertEqual(payload["wall_clock_minutes"]["p95"], 8)
        self.assertAlmostEqual(payload["rerun_signal"]["rate"], 1 / 3)
        self.assertAlmostEqual(payload["failure_signal"]["rate"], 1 / 3)
        self.assertTrue(payload["budgets"]["p95_within_budget"])
        self.assertTrue(payload["budgets"]["rerun_rate_within_budget"])

    def test_summary_excludes_cancelled_and_incomplete_runs(self) -> None:
        cancelled = run(30, conclusion="cancelled")
        in_progress = {"status": "in_progress", "conclusion": None}
        payload = ci_health.summarize(
            [run(4), cancelled, in_progress],
            max_p95_minutes=5,
            max_rerun_rate=0,
        )
        self.assertEqual(payload["window"], 1)
        self.assertEqual(payload["wall_clock_minutes"]["maximum"], 4)

    def test_window_is_applied_after_ineligible_runs_are_filtered(self) -> None:
        payload = ci_health.summarize(
            [run(30, conclusion="cancelled"), run(2), run(3)],
            window=2,
            max_p95_minutes=15,
            max_rerun_rate=0.1,
        )

        self.assertEqual(payload["window"], 2)
        self.assertEqual(payload["wall_clock_minutes"]["maximum"], 3)

    def test_summary_marks_exceeded_budgets(self) -> None:
        payload = ci_health.summarize(
            [run(20, attempt=2)], max_p95_minutes=10, max_rerun_rate=0.1
        )
        self.assertFalse(payload["budgets"]["p95_within_budget"])
        self.assertFalse(payload["budgets"]["rerun_rate_within_budget"])
        rendered = ci_health.markdown(payload)
        self.assertIn("over", rendered)


if __name__ == "__main__":
    unittest.main()
