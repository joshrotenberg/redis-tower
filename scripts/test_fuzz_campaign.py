#!/usr/bin/env python3

import argparse
import dataclasses
import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import fuzz_campaign


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "fuzz.yml"


class FuzzCampaignTests(unittest.TestCase):
    def make_campaign(self, root: Path) -> fuzz_campaign.Campaign:
        seeds = root / "fuzz" / "corpus-seeds" / "decode"
        seeds.mkdir(parents=True)
        (seeds / "simple.hex").write_text("2b4f4b0d0a\n")
        (root / "Cargo.toml").write_text("[workspace]\n")
        (root / "fuzz" / "Cargo.toml").write_text("[package]\nname='fuzz'\n")
        (root / "fuzz" / "Cargo.lock").write_text("version = 4\n")
        return fuzz_campaign.Campaign(
            repo_root=root,
            target="decode",
            duration_seconds=17,
            source_sha="abc123",
            seed_dir=seeds,
            corpus_dir=root / "fuzz" / "corpus" / "decode",
            artifact_dir=root / "fuzz" / "artifacts" / "decode",
            output_dir=root / "fuzz-results" / "decode",
        )

    def test_success_records_provenance_and_completed_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = self.make_campaign(Path(directory))

            def runner(command, **kwargs):
                self.assertIn("-max_total_time=17", command)
                (campaign.corpus_dir / "discovered").write_bytes(b"new")
                kwargs["stdout"].write(b"Done 123 runs in 17 second(s)\n")
                return subprocess.CompletedProcess(command, 0)

            times = iter((10.0, 27.5))
            stamps = iter(("start", "finish"))
            self.assertEqual(
                fuzz_campaign.run_campaign(
                    campaign,
                    runner=runner,
                    monotonic=lambda: next(times),
                    timestamp=lambda: next(stamps),
                ),
                0,
            )
            manifest = json.loads((campaign.output_dir / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "passed")
            self.assertEqual(manifest["exit_code"], 0)
            self.assertEqual(manifest["elapsed_seconds"], 17.5)
            self.assertEqual(manifest["source_sha"], "abc123")
            self.assertIn("fuzz/Cargo.lock", manifest["dependencies"])
            dependency = manifest["dependencies"]["fuzz/Cargo.lock"]
            retained = campaign.output_dir / dependency["artifact"]
            self.assertEqual(retained.read_text(), "version = 4\n")
            self.assertEqual(
                dependency["sha256"], hashlib.sha256(retained.read_bytes()).hexdigest()
            )
            self.assertEqual(manifest["reviewed_seeds"]["file_count"], 1)
            self.assertEqual(manifest["final_corpus"]["file_count"], 2)
            self.assertEqual((campaign.corpus_dir / "simple").read_bytes(), b"+OK\r\n")

    def test_failure_cannot_be_reported_as_passed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = self.make_campaign(Path(directory))

            def runner(command, **kwargs):
                return subprocess.CompletedProcess(command, 77)

            self.assertEqual(fuzz_campaign.run_campaign(campaign, runner=runner), 77)
            manifest = json.loads((campaign.output_dir / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["exit_code"], 77)

    def test_graceful_child_interruption_with_zero_exit_is_not_a_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = self.make_campaign(Path(directory))

            def runner(command, **kwargs):
                kwargs["stdout"].write(
                    b"libFuzzer: exiting as requested\n"
                    b"stat::number_of_executed_units: 12\n"
                )
                return subprocess.CompletedProcess(command, 0)

            times = iter((1.0, 30.0))
            self.assertEqual(
                fuzz_campaign.run_campaign(
                    campaign, runner=runner, monotonic=lambda: next(times)
                ),
                1,
            )
            manifest = json.loads((campaign.output_dir / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "incomplete")
            self.assertEqual(manifest["process_exit_code"], 0)
            self.assertEqual(manifest["exit_code"], 1)
            self.assertIn("early interruption", manifest["failure"])

    def test_preparation_failure_replaces_a_stale_pass_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = self.make_campaign(Path(directory))
            campaign.output_dir.mkdir(parents=True)
            manifest_path = campaign.output_dir / "manifest.json"
            manifest_path.write_text('{"status":"passed","exit_code":0}\n')
            shutil.rmtree(campaign.seed_dir)

            self.assertEqual(fuzz_campaign.run_campaign(campaign), 127)
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["source_sha"], "abc123")
            self.assertNotEqual(manifest["exit_code"], 0)

    def test_interruption_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = self.make_campaign(Path(directory))

            def runner(command, **kwargs):
                raise KeyboardInterrupt

            self.assertEqual(fuzz_campaign.run_campaign(campaign, runner=runner), 130)
            manifest = json.loads((campaign.output_dir / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "interrupted")
            self.assertNotEqual(manifest["status"], "passed")

    def test_zero_exit_without_normal_completion_evidence_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = dataclasses.replace(
                self.make_campaign(Path(directory)), duration_seconds=1
            )

            def runner(command, **kwargs):
                return subprocess.CompletedProcess(command, 0)

            times = iter((1.0, 3.0))
            self.assertEqual(
                fuzz_campaign.run_campaign(
                    campaign, runner=runner, monotonic=lambda: next(times)
                ),
                1,
            )
            manifest = json.loads((campaign.output_dir / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "incomplete")
            self.assertIn("completion evidence", manifest["failure"])

    def test_duration_is_positive_and_bounded(self) -> None:
        self.assertEqual(fuzz_campaign.positive_bounded_duration("1"), 1)
        self.assertEqual(fuzz_campaign.positive_bounded_duration("3600"), 3600)
        for value in ("0", "3601", "-1"):
            with self.assertRaises(argparse.ArgumentTypeError):
                fuzz_campaign.positive_bounded_duration(value)


class FuzzWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text()

    def test_campaign_is_scheduled_manual_and_bounded(self) -> None:
        self.assertIn("  schedule:\n", self.workflow)
        self.assertIn("  workflow_dispatch:\n", self.workflow)
        self.assertIn("duration_seconds:", self.workflow)
        self.assertIn("scripts/fuzz_campaign.py", self.workflow)
        self.assertIn('--duration-seconds "$FUZZ_DURATION_SECONDS"', self.workflow)

    def test_each_target_retains_manifest_corpus_crashes_and_versions(self) -> None:
        self.assertIn("target: [decode, decode_chunked]", self.workflow)
        self.assertIn("if: always()", self.workflow)
        self.assertIn("fuzz-results/${{ matrix.target }}", self.workflow)
        self.assertIn("fuzz/corpus/${{ matrix.target }}", self.workflow)
        self.assertIn("fuzz/artifacts/${{ matrix.target }}", self.workflow)
        self.assertIn("rust-version.txt", self.workflow)
        self.assertIn("cargo-fuzz-version.txt", self.workflow)
        self.assertIn("retention-days: 90", self.workflow)
        self.assertNotIn("continue-on-error", self.workflow)


if __name__ == "__main__":
    unittest.main()
