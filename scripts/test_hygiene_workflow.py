#!/usr/bin/env python3

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "hygiene.yml"


class HygieneWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text()

    def mutation_command(self) -> str:
        start = self.workflow.index("cargo mutants ")
        end = self.workflow.index("\n          status=$?", start)
        return " ".join(self.workflow[start:end].replace("\\", "").split())

    def test_cargo_mutants_version_is_pinned(self) -> None:
        self.assertIn("tool: cargo-mutants@27.1.0", self.workflow)

    def test_parallel_mutation_does_not_run_in_place(self) -> None:
        command = self.mutation_command()
        self.assertIn("--jobs 2", command)
        self.assertNotIn("--in-place", command)

    def test_mutation_evidence_has_an_explicit_output_directory(self) -> None:
        command = self.mutation_command()
        self.assertIn(
            '--output "mutation-results/$MUTATION_PACKAGE"',
            command,
        )


if __name__ == "__main__":
    unittest.main()
