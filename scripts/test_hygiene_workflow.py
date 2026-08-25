#!/usr/bin/env python3

import unittest
from pathlib import Path

import mutation_plan


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

    def test_mutation_output_parent_is_created(self) -> None:
        self.assertIn(
            'mkdir -p "$MUTATION_OUTPUT"\n          shard_args=()',
            self.workflow,
        )

    def test_mutation_evidence_has_an_explicit_output_directory(self) -> None:
        command = self.mutation_command()
        self.assertIn(
            '--output "$MUTATION_OUTPUT"',
            command,
        )

    def test_mutation_workflow_uses_round_robin_shards(self) -> None:
        self.assertIn(
            'shard_args=(--shard "$MUTATION_SHARD/$MUTATION_SHARDS" '
            '--sharding round-robin)',
            self.workflow,
        )

    def test_package_aggregation_requires_every_shard(self) -> None:
        self.assertIn('--expected-reports "${{ matrix.shards }}"', self.workflow)
        self.assertIn('pattern: mutation-shard-${{ matrix.package }}-*', self.workflow)

    def test_plan_covers_publishable_packages_and_shards_large_crates(self) -> None:
        packages = [
            "redis-tower",
            "redis-tower-auth-aws",
            "redis-tower-commands",
        ]
        plan = mutation_plan.build_plan(packages)
        rows = plan["matrix"]["include"]
        self.assertEqual(len(rows), 17)
        self.assertEqual(
            {row["package"] for row in rows},
            set(packages),
        )
        self.assertEqual(
            len([row for row in rows if row["package"] == "redis-tower"]),
            12,
        )


if __name__ == "__main__":
    unittest.main()
