#!/usr/bin/env python3

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
README = ROOT / "README.md"


def coverage_job() -> str:
    workflow = WORKFLOW.read_text()
    start = workflow.index("  coverage:\n")
    end = workflow.index("\n  features:\n", start)
    return workflow[start:end]


class CiWorkflowTests(unittest.TestCase):
    def test_coverage_job_does_not_depend_on_unconfigured_codecov(self) -> None:
        job = coverage_job()
        self.assertIn("    permissions:\n      contents: read\n", job)
        self.assertNotIn("codecov", job.lower())
        self.assertNotIn("CODECOV_TOKEN", job)

    def test_readme_does_not_advertise_unconfigured_codecov(self) -> None:
        self.assertNotIn("codecov", README.read_text().lower())

    def test_native_summary_and_artifact_are_retained(self) -> None:
        job = coverage_job()
        self.assertIn("cargo llvm-cov report --summary-only", job)
        self.assertIn("$GITHUB_STEP_SUMMARY", job)
        self.assertIn("uses: actions/upload-artifact@v4", job)
        self.assertIn("coverage-summary.txt", job)
        self.assertIn("lcov.info", job)
        self.assertIn("if-no-files-found: error", job)
        self.assertIn("retention-days: 30", job)


if __name__ == "__main__":
    unittest.main()
