#!/usr/bin/env python3
"""Synthetic Criterion and stub-process tests; no Cargo or Rust execution."""

from contextlib import chdir
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import codec_evidence as evidence

ROOT = Path(__file__).resolve().parents[2]


def run_block(name):
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    step = text.split("      - name: " + name + "\n", 1)[1]
    step = step.split("\n      - ", 1)[0]
    return textwrap.dedent(step.split("        run: |\n", 1)[1])


class CodecEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "evidence"
        self.output.mkdir()

    def raw(self, root, label, mean=100):
        raw = root / "case" / label
        raw.mkdir(parents=True)
        (raw / "benchmark.json").write_text(json.dumps({"full_id": "case"}))
        (raw / "sample.json").write_text(
            json.dumps({"iters": [1] * 50, "times": [mean] * 50}))
        (raw / "estimates.json").write_text(json.dumps({
            "mean": {"point_estimate": mean, "confidence_interval": {
                "lower_bound": mean - 1, "upper_bound": mean + 1}}}))

    def subject(self, label="main", mean=100):
        root = self.output / label
        root.mkdir()
        (root / "Cargo.lock").write_text("version = 4\n")
        (root / "dependencies.json").write_text(json.dumps({
            "schema_version": 1, "view": "workspace-resolved",
            "packages": [{"name": "example",
                                               "version": "1.0", "source": "workspace",
                                               "resolved_features": []}]}))
        (root / "codec-executable").write_bytes(b"saved executable")
        for name in ("build.stdout", "build.stderr", "benchmark.stdout", "benchmark.stderr",
                     "resolution.stdout", "resolution.stderr"):
            (root / name).write_text("retained\n")
        (root / "case-list.stdout").write_text("case: benchmark\n")
        (root / "compiler-artifact.json").write_text("{}")
        self.raw(root / "criterion", label, mean)
        cwd = str(self.root / "crates/redis-tower-protocol")
        criterion = str(self.root / "target/criterion")
        for suffix in ("before", "after"):
            shutil.copyfile(root / "Cargo.lock", root / ("Cargo.lock." + suffix))
        resolution = {"schema_version": 1, "source_sha": "a" * 40,
                      "argv": ["cargo", "metadata", "--format-version", "1"],
                      "exit_status": 0, "classification": "reused",
                      "before": evidence.digest(root / "Cargo.lock.before"),
                      "after": evidence.digest(root / "Cargo.lock.after"),
                      "raw_diff_sha256": hashlib.sha256(b"").hexdigest()}
        (root / "lock-resolution.json").write_text(json.dumps(resolution))
        (root / "lock-resolution.diff").write_text("")
        record = dict.fromkeys(evidence.RECORD_FIELDS)
        record.update({
            "schema_version": 1, "label": label, "source_sha": "a" * 40,
            "source_tree": "b" * 40, "expected_source": "a" * 40,
            "harness_source": "c" * 40, "harness_files": {
                name: {"sha256": "d" * 64, "bytes": 1}
                for name in ("codec_evidence.py", "sanitize_metadata.py", "ci.yml")},
            "exit_status": 0, "error": None, "expected_cases": ["case"],
            "lock_resolution": resolution,
            "lock_before": evidence.digest(root / "Cargo.lock"),
            "lock_after": evidence.digest(root / "Cargo.lock"),
            "executable_before": evidence.digest(root / "codec-executable"),
            "executable_after": evidence.digest(root / "codec-executable"),
            "tools": {"rustc": "rustc test", "cargo": "cargo test"},
            "host_before": {"system": "test"}, "host_after": {"system": "test"},
            "environment": {}, "started_at": "2026-11-16T00:00:00+00:00",
            "finished_at": "2026-11-16T00:00:01+00:00", "duration_seconds": 1,
            "benchmark_cwd": cwd, "criterion_directory": criterion,
            "commands": [
                {"capture": "resolution", "argv": ["cargo", "metadata", "--format-version", "1"],
                 "cwd": str(self.root), "environment": {}, "exit_status": 0},
                {"capture": "build", "argv": ["cargo", "bench", "--locked", "-p",
                 "redis-tower-protocol", "--bench", "codec", "--no-run",
                 "--message-format=json", "-vv"], "cwd": str(self.root), "environment": {}},
                {"capture": "benchmark",
                 "argv": [str(root / "codec-executable"), *evidence.SETTINGS, label],
                 "cwd": cwd, "environment": {"CRITERION_HOME": criterion}},
            ],
        })
        (root / "record.json").write_text(json.dumps(record))
        return root

    def comparison(self, status=0):
        (self.output / "benchmark-comparison.txt").write_text("comparison diagnostics\n")
        (self.output / "benchmark-comparison.status").write_text(str(status) + "\n")

    def test_complete_subjects_seal_even_when_comparator_reports_regression(self):
        self.subject("candidate-confirm")
        self.subject("main-confirm")
        self.comparison(1)
        manifest = evidence.seal(self.output, ["candidate-confirm", "main-confirm"])
        self.assertTrue(manifest["evidence_complete"], manifest["issues"])
        self.assertIn("candidate-confirm/criterion/case/candidate-confirm/sample.json",
                      manifest["files"])
        evidence.verify(self.output)

    def test_missing_samples_are_retained_as_incomplete_evidence(self):
        subject = self.subject()
        self.comparison()
        (subject / "criterion/case/main/sample.json").unlink()
        manifest = evidence.seal(self.output, ["main"])
        self.assertFalse(manifest["evidence_complete"])
        self.assertTrue(any("sample.json" in issue for issue in manifest["issues"]))
        self.assertIn("main/record.json", manifest["files"])
        with self.assertRaises(evidence.EvidenceError):
            evidence.verify(self.output)

    def test_hash_mismatch_and_added_inventory_entry_are_rejected(self):
        self.subject()
        self.comparison()
        evidence.seal(self.output, ["main"])
        (self.output / "extra.txt").write_text("extra")
        with self.assertRaisesRegex(evidence.EvidenceError, "inventory"):
            evidence.verify(self.output)
        (self.output / "extra.txt").unlink()
        (self.output / "main/Cargo.lock").write_text("changed")
        with self.assertRaisesRegex(evidence.EvidenceError, "inventory"):
            evidence.verify(self.output)

    def test_record_schema_binary_lock_and_invocation_identity_are_required(self):
        for mutation in (
            lambda r: r.update(schema_version=9),
            lambda r: r.update(executable_after={"sha256": "d" * 64, "bytes": 16}),
            lambda r: r.update(lock_after={"sha256": "e" * 64, "bytes": 4}),
            lambda r: r.update(source_sha="f" * 40),
            lambda r: r.update(tools={}),
            lambda r: r.update(commands=[]),
            lambda r: r.update(lock_resolution={}),
        ):
            with self.subTest(mutation=mutation):
                subject = self.subject()
                record = json.loads((subject / "record.json").read_text())
                mutation(record)
                (subject / "record.json").write_text(json.dumps(record))
                self.comparison()
                result = evidence.seal(self.output, ["main"])
                self.assertFalse(result["evidence_complete"])
                shutil.rmtree(subject)

    def test_missing_case_and_malformed_samples_are_incomplete(self):
        subject = self.subject()
        record = json.loads((subject / "record.json").read_text())
        record["expected_cases"].append("missing")
        (subject / "record.json").write_text(json.dumps(record))
        self.comparison()
        result = evidence.seal(self.output, ["main"])
        self.assertTrue(any("cases" in issue for issue in result["issues"]))
        record["expected_cases"] = ["case"]
        (subject / "record.json").write_text(json.dumps(record))
        raw = subject / "criterion/case/main/sample.json"
        for samples in (
            {"iters": [0] * 50, "times": [100] * 50},
            {"iters": [1] * 50, "times": [100] * 49},
            {"iters": [1] * 50, "times": [float("nan")] * 50},
            {"iters": [1] * 50, "times": [200] * 50},
        ):
            raw.write_text(json.dumps(samples))
            result = evidence.seal(self.output, ["main"])
            self.assertFalse(result["evidence_complete"])

    def test_unknown_manifest_schema_and_labels_are_rejected(self):
        self.subject()
        self.comparison()
        manifest = evidence.seal(self.output, ["main"])
        manifest["schema_version"] = 2
        (self.output / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaises(evidence.EvidenceError):
            evidence.verify(self.output)
        with self.assertRaises(evidence.EvidenceError):
            evidence.seal(self.output, ["unexpected"])

    def test_environment_is_allowlisted(self):
        with mock.patch.dict(os.environ, {"SECRET_TOKEN": "never-retain", "GITHUB_RUN_ID": "123"}):
            collected = evidence.environment()
        self.assertEqual(collected["GITHUB_RUN_ID"], "123")
        self.assertNotIn("SECRET_TOKEN", collected)
        self.assertNotIn("never-retain", json.dumps(collected))

    def test_measurement_runs_saved_bytes_once_and_reuses_compatible_lock(self):
        self._measure_with_lock()

    def test_measurement_explicitly_resolves_subject_manifest_lock_change(self):
        self._measure_with_lock(update_lock=True)

    def test_measurement_records_creation_of_a_previously_absent_lock(self):
        self._measure_with_lock(initially_missing=True)

    def _measure_with_lock(self, update_lock=False, initially_missing=False):
        package = self.root / "crates/redis-tower-protocol"
        package.mkdir(parents=True)
        # Cargo metadata reuses a compatible lock, but may resolve a manifest
        # change explicitly before the subsequent --locked build.
        if not initially_missing:
            (self.root / "Cargo.lock").write_text("version = 4\n")
        if update_lock:
            (package / "Cargo.toml").write_text('[dependencies]\nbytes = "2"\n')
        workflow = self.root / "ci.yml"
        workflow.write_text("workflow identity\n")
        executable = self.root / "built-codec"
        executable.write_bytes(b"stub compiled executable")
        package_id = "path+file:///private/example#redis-tower-protocol@0.1.4"
        metadata = {
            "packages": [{"id": package_id, "name": "redis-tower-protocol",
                          "version": "0.1.4", "source": None,
                          "manifest_path": str(package / "Cargo.toml")}],
            "workspace_members": [package_id],
            "resolve": {"nodes": [{"id": package_id, "features": []}]},
        }
        measured = []
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            if argv[:2] == ["git", "rev-parse"]:
                stdout = ("b" if argv[2] == "HEAD^{tree}" else "a") * 40 + "\n"
            elif argv[:2] == ["git", "status"]:
                stdout = ""
            elif argv == ["rustc", "-vV"] or argv == ["cargo", "-vV"]:
                stdout = "fixed toolchain\n"
            elif argv[:2] == ["cargo", "metadata"]:
                if update_lock and "--locked" in argv:
                    return subprocess.CompletedProcess(argv, 101, "", "lockfile needs update")
                self.assertEqual(argv, ["cargo", "metadata", "--format-version", "1"])
                if update_lock:
                    (self.root / "Cargo.lock").write_text("version = 4\n# new manifest dependency\n")
                elif initially_missing:
                    (self.root / "Cargo.lock").write_text("version = 4\n")
                stdout = json.dumps(metadata)
            elif argv[:2] == ["cargo", "bench"]:
                self.assertIn("--no-run", argv)
                self.assertIn("--locked", argv)
                stdout = "\n".join(json.dumps(unit) for unit in [
                    {"reason": "compiler-artifact", "target": {"name": "bytes", "kind": ["lib"]},
                     "features": ["actual-dependency-feature"], "executable": None},
                    {"reason": "compiler-artifact", "target": {"name": "codec", "kind": ["bench"]},
                     "features": ["actual-codec-feature"], "executable": str(executable)},
                ]) + "\n"
            elif argv[0] == "ldd":
                stdout = "retained library identity\n"
            elif Path(argv[0]).name == "codec-executable":
                self.assertEqual(Path(kwargs["cwd"]), package)
                self.assertEqual(kwargs["env"]["CRITERION_HOME"], str(self.root / "target/criterion"))
                self.assertEqual(kwargs["env"]["CARGO_MANIFEST_DIR"], str(package))
                if argv[1:] == ["--list"]:
                    stdout = "case: benchmark\n"
                else:
                    self.assertEqual(argv[1:], [*evidence.SETTINGS, "main"])
                    measured.append(argv)
                    self.raw(self.root / "target/criterion", "main")
                    stdout = "measured once\n"
            else:
                self.fail(f"unexpected process, no Rust is allowed: {argv}")
            return subprocess.CompletedProcess(argv, 0, stdout, "")

        with chdir(self.root), mock.patch.object(evidence, "host", return_value={"test": "host"}), \
                mock.patch.object(evidence.subprocess, "run", side_effect=fake_run):
            result = evidence.measure(self.output, "main", "a" * 40, "c" * 40, workflow)
        self.assertEqual(result, 0)
        self.assertEqual(len(measured), 1)
        self.assertFalse(any(call[:2] == ["cargo", "generate-lockfile"] for call in calls))
        record = json.loads((self.output / "main/record.json").read_text())
        self.assertEqual(record["harness_source"], "c" * 40)
        self.assertEqual(record["source_sha"], "a" * 40)
        resolution = record["lock_resolution"]
        self.assertEqual(resolution["classification"],
                         "created" if initially_missing else "updated" if update_lock else "reused")
        self.assertEqual(resolution["exit_status"], 0)
        self.assertEqual(resolution["argv"], ["cargo", "metadata", "--format-version", "1"])
        self.assertEqual(resolution["after"], record["lock_before"])
        if update_lock:
            self.assertNotEqual(resolution["before"], resolution["after"])
            self.assertIn("new manifest dependency", (self.output / "main/lock-resolution.diff").read_text())
        elif not initially_missing:
            self.assertEqual(resolution["before"], resolution["after"])
        self.assertEqual(record["executable_before"], record["executable_after"])
        self.assertFalse((self.output / "main/metadata.stdout").exists())
        self.assertNotIn("/private/example", (self.output / "main/dependencies.json").read_text())
        view = json.loads((self.output / "main/dependencies.json").read_text())
        self.assertEqual(view["view"], "workspace-resolved")
        self.assertEqual(view["packages"][0]["resolved_features"], [])
        units = [json.loads(line) for line in (self.output / "main/build.stdout").read_text().splitlines()]
        self.assertEqual([unit["features"] for unit in units],
                         [["actual-dependency-feature"], ["actual-codec-feature"]])
        self.comparison()
        result = evidence.seal(self.output, ["main"])
        self.assertTrue(result["evidence_complete"], result["issues"])

    def test_failed_resolution_keeps_pre_post_locks_and_redacted_diagnostics(self):
        subject = self.output / "main"
        subject.mkdir()
        (self.root / "Cargo.lock").write_text("version = 4\n")
        metadata = '{"private": "https://user:password@example.invalid/repo?token=secret"}'
        stderr = "cannot resolve https://user:password@example.invalid/repo?token=secret from /private/repository/Cargo.toml"
        completed = subprocess.CompletedProcess(["cargo"], 101, metadata, stderr)
        record = {"source_sha": "a" * 40, "commands": []}
        with mock.patch.object(evidence.subprocess, "run", return_value=completed):
            with self.assertRaises(evidence.EvidenceError):
                evidence.resolve_dependencies(self.root, subject, record)
        resolution = json.loads((subject / "lock-resolution.json").read_text())
        self.assertEqual(resolution["classification"], "failed")
        self.assertEqual(resolution["exit_status"], 101)
        self.assertEqual(resolution["before"], resolution["after"])
        self.assertTrue((subject / "Cargo.lock.before").exists())
        self.assertTrue((subject / "Cargo.lock.after").exists())
        logs = (subject / "resolution.stdout").read_text() + (subject / "resolution.stderr").read_text()
        for secret in ("password", "token=secret", "/private/repository", '"private"'):
            self.assertNotIn(secret, logs)
        self.assertIn("cannot resolve", logs)

    def test_failed_setup_keeps_record_and_returns_failure(self):
        workflow = self.root / "ci.yml"
        workflow.write_text("test workflow")
        with chdir(self.root), mock.patch.object(evidence, "host", return_value={"test": "host"}), \
                mock.patch.object(evidence, "command", side_effect=evidence.EvidenceError("injected failure")):
            result = evidence.measure(self.output, "main", "a" * 40, "c" * 40, workflow)
        self.assertNotEqual(result, 0)
        record = json.loads((self.output / "main/record.json").read_text())
        self.assertIn("injected failure", record["error"])
        self.assertIsNotNone(record["finished_at"])

    def test_actual_confirmation_failure_is_preserved_and_followed_by_sealing(self):
        # Run the actual workflow shell and actual comparator with synthetic
        # baselines. Only git and critcmp are stubs; no Rust process is started.
        for label, mean in (("main", 100), ("candidate", 120),
                            ("main-confirm", 100), ("candidate-confirm", 120)):
            self.raw(self.root / "target/criterion", label, mean)
        self.subject("candidate-confirm", 120)
        self.subject("main-confirm", 100)
        scripts = self.root / "scripts"
        scripts.mkdir()
        shutil.copyfile(ROOT / "scripts/check_criterion_regressions.py",
                        scripts / "check_criterion_regressions.py")
        binaries = self.root / "bin"
        binaries.mkdir()
        for name in ("git", "critcmp"):
            stub = binaries / name
            stub.write_text("#!/bin/sh\nprintf 'stub diagnostic\\n'\n")
            stub.chmod(0o755)
        tools = self.root / "runner/codec-evidence-tools"
        tools.mkdir(parents=True)
        for name in ("codec_evidence.py", "sanitize_metadata.py"):
            shutil.copyfile(Path(__file__).with_name(name), tools / name)
        env = os.environ | {
            "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
            "CANDIDATE_SHA": "a" * 40, "RUNNER_TEMP": str(self.root / "runner"),
            "CODEC_EVIDENCE_DIR": str(self.output),
        }
        comparison = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", run_block("Compare confirmation results")],
            cwd=self.root, env=env, capture_output=True, text=True)
        self.assertEqual(comparison.returncode, 1, comparison.stderr)
        text = (self.root / "benchmark-comparison.txt").read_text()
        self.assertIn("REGRESSION", text)
        self.assertIn("exceeded", text)  # comparator stderr was retained too.
        self.assertEqual((self.root / "benchmark-comparison.status").read_text().strip(), "1")
        seal = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c",
             run_block("Seal complete codec evidence, including failed comparisons").replace(
                 "--labels main candidate", "--labels candidate-confirm main-confirm")],
            cwd=self.root, env=env, capture_output=True, text=True)
        self.assertEqual(seal.returncode, 0, seal.stdout + seal.stderr)
        evidence.verify(self.output)
        self.assertTrue((self.output / "candidate-confirm/criterion/case/candidate-confirm/sample.json").exists())


if __name__ == "__main__":
    unittest.main()
