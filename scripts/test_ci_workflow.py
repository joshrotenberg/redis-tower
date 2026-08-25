#!/usr/bin/env python3

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
CODECOV_ACTION = "codecov/codecov-action@fb8b3582c8e4def4969c97caa2f19720cb33a72f"


def coverage_job() -> str:
    workflow = WORKFLOW.read_text()
    start = workflow.index("  coverage:\n")
    end = workflow.index("\n  features:\n", start)
    return workflow[start:end]


class CiWorkflowTests(unittest.TestCase):
    def test_coverage_job_uses_oidc_and_fails_eligible_upload_errors(self) -> None:
        job = coverage_job()
        self.assertIn("    permissions:\n      contents: read\n      id-token: write\n", job)
        self.assertIn(f"uses: {CODECOV_ACTION} # v7.0.0", job)
        self.assertIn("use_oidc: true", job)
        self.assertIn("fail_ci_if_error: true", job)
        self.assertNotIn("CODECOV_TOKEN", job)

    def test_fork_pull_requests_skip_only_external_publication(self) -> None:
        job = coverage_job()
        condition = (
            "if: github.event_name != 'pull_request' || "
            "github.event.pull_request.head.repo.full_name == github.repository"
        )
        self.assertIn(condition, job)
        self.assertLess(job.index("Generate lcov report"), job.index(condition))
        self.assertLess(job.index("Retain coverage evidence"), job.index(condition))

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
