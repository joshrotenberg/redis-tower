import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from generate_test_conformance import (
    BEGIN_MARKER,
    END_MARKER,
    CompatibilityTarget,
    Inventory,
    TestBinary,
    WorkflowCommand,
    binary_schedule,
    collect_compatibility,
    command_selects_binary,
    derive_parity_variants,
    extract_run_blocks,
    fault_tests,
    main,
    parse_test_listing,
    render_report,
    replace_generated_section,
    split_cargo_test_commands,
)


def fixture_inventory() -> Inventory:
    return Inventory(
        binaries=(
            TestBinary(
                "redis-tower",
                "integration",
                ("cmd_get", "cmd_set", "resp3::cmd_get", "resp3::cmd_set"),
                (),
            ),
            TestBinary(
                "redis-tower-cluster",
                "cluster_integration",
                (
                    "cluster_master_failover_recovers",
                    "cmd_get",
                    "cmd_set",
                    "multiplexed::cmd_get",
                    "multiplexed::cmd_set",
                ),
                (
                    "cluster_master_failover_recovers",
                    "cmd_get",
                    "cmd_set",
                    "multiplexed::cmd_get",
                    "multiplexed::cmd_set",
                ),
            ),
            TestBinary(
                "redis-tower-sentinel",
                "sentinel_integration",
                ("cmd_get", "cmd_set", "multiplexed::cmd_get", "multiplexed::cmd_set"),
                ("cmd_get", "cmd_set", "multiplexed::cmd_get", "multiplexed::cmd_set"),
            ),
            TestBinary(
                "redis-chaos-tests",
                "fault_matrix",
                ("partition_recovers",),
                ("partition_recovers",),
            ),
        ),
        workflows=(
            WorkflowCommand(
                ".github/workflows/ci.yml",
                ("pull request",),
                "cargo test -p redis-tower --test '*' --all-features -- --test-threads=1",
            ),
            WorkflowCommand(
                ".github/workflows/ci.yml",
                ("pull request",),
                "cargo test -p redis-tower-cluster --test cluster_integration -- --ignored",
            ),
            WorkflowCommand(
                ".github/workflows/ci.yml",
                ("pull request",),
                "cargo test -p redis-tower-sentinel --test 'sentinel_*' -- --ignored",
            ),
            WorkflowCommand(
                ".github/workflows/nightly.yml",
                ("scheduled", "manual"),
                "cargo test -p redis-chaos-tests --test fault_matrix -- --ignored",
            ),
        ),
        compatibility=(
            CompatibilityTarget("Pull request", "Redis 8.0.6", "source build"),
            CompatibilityTarget("Nightly", "valkey-8.1", "valkey/valkey:8.1-alpine"),
        ),
    )


class TestConformanceGeneratorTests(unittest.TestCase):
    def test_parse_test_listing_ignores_benchmarks_and_summary(self) -> None:
        listing = "alpha: test\nmodule::beta: test\nthroughput: benchmark\n\n2 tests, 0 benchmarks\n"
        self.assertEqual(parse_test_listing(listing), ("alpha", "module::beta"))

    def test_ignored_tests_must_be_in_full_listing(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown tests"):
            TestBinary("redis-tower", "integration", ("one",), ("two",))

    def test_extracts_folded_and_literal_cargo_test_commands(self) -> None:
        workflow = """
on:
  pull_request:
jobs:
  test:
    steps:
      - run: cargo test -p redis-tower --test '*'
      - run: >-
          cargo test -p redis-tower-cluster
          --test cluster_integration -- --ignored
      - run: |
          cargo test -p redis-tower-sentinel --test sentinel_integration -- --ignored
          cargo test -p redis-tower-sentinel --test sentinel_failover -- --ignored
"""
        commands = tuple(
            command
            for block in extract_run_blocks(workflow)
            for command in split_cargo_test_commands(block)
        )
        self.assertEqual(len(commands), 4)
        self.assertIn("--test cluster_integration", commands[1])
        self.assertTrue(commands[2].endswith("--ignored"))

    def test_workflow_selector_distinguishes_normal_and_ignored(self) -> None:
        binary = TestBinary(
            "redis-tower-cluster", "cluster_integration", ("normal", "ignored"), ("ignored",)
        )
        ignored_workflow = WorkflowCommand(
            "ci.yml",
            ("pull request",),
            "cargo test -p redis-tower-cluster --test 'cluster_*' -- --ignored",
        )
        normal_workflow = WorkflowCommand(
            "ci.yml",
            ("pull request",),
            "cargo test -p redis-tower-cluster --test other",
        )
        self.assertEqual(command_selects_binary(ignored_workflow, binary), (False, True))
        self.assertEqual(command_selects_binary(normal_workflow, binary), (False, False))
        self.assertEqual(binary_schedule(binary, (ignored_workflow,)).pull_request, 1)

    def test_parity_is_derived_from_compiled_names(self) -> None:
        variants = derive_parity_variants(fixture_inventory().binaries)
        self.assertEqual(len(variants), 6)
        self.assertEqual({variant.cases for variant in variants}, {("cmd_get", "cmd_set")})
        report = render_report(fixture_inventory())
        self.assertIn("**12 compiled parity tests**", report)
        self.assertIn("**2 test cases × 6 client/topology expansions**", report)

    def test_report_exposes_unscheduled_tests_instead_of_claiming_zero(self) -> None:
        report = render_report(fixture_inventory())
        self.assertIn("| **Total** |  | **3** | **13** | **9** | **13** | **0** | **0** |", report)
        self.assertIn("cluster_master_failover_recovers", report)
        self.assertIn("partition_recovers", report)

    def test_collects_pull_request_and_nightly_compatibility(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_dir = root / ".github" / "workflows"
            workflow_dir.mkdir(parents=True)
            (workflow_dir / "ci.yml").write_text(
                'matrix:\n  redis: ["7.4.3", "8.0.6"]\n', encoding="utf-8"
            )
            (workflow_dir / "nightly.yml").write_text(
                """
jobs:
  version-matrix:
    strategy:
      matrix:
        include:
          - name: redis-8.8
            image: redis:8.8-alpine
          - name: valkey-8.1
            image: valkey/valkey:8.1-alpine
    services:
      redis:
        image: ${{ matrix.image }}
  another-job:
    runs-on: ubuntu-latest
""",
                encoding="utf-8",
            )
            targets = collect_compatibility(root)
        self.assertEqual(
            [(target.cadence, target.name, target.implementation) for target in targets],
            [
                ("Pull request", "Redis 7.4.3", "source build"),
                ("Pull request", "Redis 8.0.6", "source build"),
                ("Nightly", "redis-8.8", "redis:8.8-alpine"),
                ("Nightly", "valkey-8.1", "valkey/valkey:8.1-alpine"),
            ],
        )

    def test_fault_inventory_uses_semantic_name_fragments(self) -> None:
        binary = TestBinary(
            "redis-tower",
            "integration",
            (
                "acl_genpass_default",
                "cover_latency_latest",
                "default_tracking_caches_reads_and_observes_external_invalidation",
                "mux_cluster_handles_ask_then_moved_during_live_reshard",
            ),
            (),
        )
        self.assertEqual(
            [test for _binary, test in fault_tests((binary,))],
            ["mux_cluster_handles_ask_then_moved_during_live_reshard"],
        )

    def test_marker_replacement_preserves_human_authored_text(self) -> None:
        document = f"# Report\n\nBefore.\n\n{BEGIN_MARKER}\nold\n{END_MARKER}\n\nAfter.\n"
        updated = replace_generated_section(document, "new\nsection\n")
        self.assertEqual(
            updated,
            f"# Report\n\nBefore.\n\n{BEGIN_MARKER}\nnew\nsection\n{END_MARKER}\n\nAfter.\n",
        )
        with self.assertRaisesRegex(ValueError, "exactly one"):
            replace_generated_section("# no markers\n", "generated\n")

    def test_cli_update_and_check_with_captured_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inventory_path = root / "inventory.json"
            output = root / "TEST-CONFORMANCE.md"
            inventory_path.write_text(
                json.dumps(fixture_inventory().to_dict()), encoding="utf-8"
            )
            output.write_text(
                f"# Test conformance\n\n{BEGIN_MARKER}\nstale\n{END_MARKER}\n",
                encoding="utf-8",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "--inventory-file",
                            str(inventory_path),
                            "--output",
                            str(output),
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "--check",
                            "--inventory-file",
                            str(inventory_path),
                            "--output",
                            str(output),
                        ]
                    ),
                    0,
                )
            output.write_text(output.read_text(encoding="utf-8").replace("Compiled", "Old"), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "--check",
                            "--inventory-file",
                            str(inventory_path),
                            "--output",
                            str(output),
                        ]
                    ),
                    1,
                )


if __name__ == "__main__":
    unittest.main()
