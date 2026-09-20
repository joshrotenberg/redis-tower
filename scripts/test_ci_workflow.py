#!/usr/bin/env python3

import re
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


def job(name: str) -> str:
    """Return one job's block, from its key to the start of the next job."""

    workflow = WORKFLOW.read_text()
    start = workflow.index(f"\n  {name}:\n") + 1
    following = re.compile(r"^  [A-Za-z0-9_-]+:$", re.MULTILINE)
    match = following.search(workflow, start + 1)
    return workflow[start : match.start()] if match else workflow[start:]


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


class CodecBenchmarkGateTests(unittest.TestCase):
    """The confirmation pass must not share a runner with the first pass."""

    def test_confirmation_is_its_own_job(self) -> None:
        first = job("bench-regression")
        confirm = job("bench-regression-confirm")

        self.assertIn(
            "    needs: bench-regression\n"
            "    if: needs.bench-regression.outputs.needs_confirmation == 'true'\n",
            confirm,
        )
        self.assertIn("      needs_confirmation: ", first)
        self.assertIn("    runs-on: ubuntu-latest\n", confirm)

    def test_first_pass_does_not_run_the_confirmation_benchmarks(self) -> None:
        first = job("bench-regression")

        self.assertNotIn("candidate-confirm", first)
        self.assertNotIn("main-confirm", first)

    def test_confirmation_job_reverses_the_benchmark_order(self) -> None:
        confirm = job("bench-regression-confirm")

        self.assertLess(
            confirm.index("--save-baseline candidate-confirm"),
            confirm.index("--save-baseline main-confirm"),
        )

    def test_confirmation_job_receives_the_first_pass_baselines(self) -> None:
        first = job("bench-regression")
        confirm = job("bench-regression-confirm")

        self.assertIn("criterion-baselines.tar.gz", first)
        self.assertIn("uses: actions/upload-artifact@v4", first)
        self.assertIn("criterion-baselines.tar.gz", confirm)
        self.assertIn("uses: actions/download-artifact@v4", confirm)

    def test_both_passes_use_the_same_per_benchmark_threshold(self) -> None:
        override = "--benchmark-threshold codec_decode/bulk_string_1kb=25"

        self.assertIn(override, job("bench-regression"))
        self.assertIn(override, job("bench-regression-confirm"))


if __name__ == "__main__":
    unittest.main()
