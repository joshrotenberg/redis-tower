#!/usr/bin/env python3

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release-plz.yml"


class ReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text()

    def test_release_remains_manual_only(self) -> None:
        self.assertIn("workflow_dispatch:", self.workflow)
        self.assertNotIn("\n  push:", self.workflow)
        self.assertNotIn("\n  schedule:", self.workflow)

    def test_prepare_is_the_safe_default(self) -> None:
        self.assertIn("default: release-pr", self.workflow)
        self.assertIn("- release-pr", self.workflow)
        self.assertIn("- release", self.workflow)

    def test_action_runs_only_the_selected_operation(self) -> None:
        self.assertIn(
            "uses: MarcoIeni/release-plz-action@2eb1d8bcb770b4c48ccfaad919734b38b51958c9",
            self.workflow,
        )
        self.assertIn("command: ${{ inputs.command }}", self.workflow)
        self.assertIn('version: "0.3.160"', self.workflow)

    def test_runs_are_serialized_and_forks_are_excluded(self) -> None:
        self.assertIn("group: release", self.workflow)
        self.assertIn("cancel-in-progress: false", self.workflow)
        self.assertIn("if: github.repository_owner == 'joshrotenberg'", self.workflow)

    def test_publish_credential_is_explicit(self) -> None:
        self.assertIn("CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}", self.workflow)


if __name__ == "__main__":
    unittest.main()
