#!/usr/bin/env python3

from __future__ import annotations

import copy
import importlib.util
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock


SCRIPT_DIR = Path(__file__).parent


def load_module(name: str):
    path = SCRIPT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


render_results = load_module("render_results")
sanitize_metadata = load_module("sanitize_metadata")
artifact_manifest = load_module("artifact_manifest")


def record(
    client: str,
    workload: str,
    payload: int,
    concurrency: int,
    *,
    commands_per_batch: int = 1,
) -> dict[str, object]:
    command_rates = [float(100 * concurrency - 1), float(100 * concurrency), float(100 * concurrency + 1)]
    batch_rates = [rate / commands_per_batch for rate in command_rates]
    samples = [
        {
            "run": index,
            "total_batches": 100,
            "total_commands": 100 * commands_per_batch,
            "errors": 0,
            "batches_per_sec": batch_rate,
            "commands_per_sec": command_rate,
            "p50_us": 10.0,
            "p90_us": 20.0,
            "p99_us": 30.0,
            "p999_us": 40.0,
            "max_us": 50.0,
        }
        for index, (batch_rate, command_rate) in enumerate(
            zip(batch_rates, command_rates, strict=True), start=1
        )
    ]
    stddev = math.sqrt(2.0 / 3.0)
    return {
        "schema_version": 2,
        "client_id": client,
        "workload": workload,
        "payload_bytes": payload,
        "concurrency": concurrency,
        "runs": 3,
        "commands_per_batch": commands_per_batch,
        "total_batches": 300,
        "total_commands": 300 * commands_per_batch,
        "errors": 0,
        "batches_per_sec_mean": 100.0 * concurrency / commands_per_batch,
        "batches_per_sec_stddev": stddev / commands_per_batch,
        "commands_per_sec_mean": float(100 * concurrency),
        "commands_per_sec_stddev": stddev,
        "p50_us": 10.0,
        "p90_us": 20.0,
        "p99_us": 30.0,
        "p999_us": 40.0,
        "max_us": 50.0,
        "samples": samples,
    }


def soak_fixture(
    *, chaos_index: int = 121, reconnect_index: int = 121, recovery_index: int = 121
) -> list[dict[str, Any]]:
    metadata: dict[str, Any] = {
        "schema_version": 1,
        "record_type": "metadata",
        "started_unix_ms": 1_800_000_000_000,
        "mode": "standalone",
        "workload": "get_validate",
        "key": "redis-tower:soak:standalone",
        "payload_bytes": 1024,
        "concurrency": 32,
        "warmup_secs": 60.0,
        "duration_secs": 14400.0,
        "report_interval_secs": 60.0,
        "operation_timeout_ms": 2000,
        "error_backoff_ms": 1,
        "startup_timeout_secs": 30.0,
        "recovery_timeout_secs": 30.0,
        "cluster_slot": 42,
        "cluster_node_timeout_ms": 1000,
        "standalone_port": 6481,
        "chaos": "standalone_sigkill",
        "chaos_after_secs": 7200.0,
        "reconnect_accounting": "exact_connection_event_reconnected",
        "recovery_accounting": "exact_connection_event_reconnected",
        "latency_accounting": "successful_get_completions_only",
        "rss_accounting": "current_soak_process_resident_set",
    }
    intervals = []
    reconnect_total = 0
    recovery_total = 0
    chaos_total = 0
    for index in range(1, 241):
        reconnect_delta = int(index == reconnect_index)
        recovery_delta = int(index == recovery_index)
        chaos_delta = int(index == chaos_index)
        reconnect_total += reconnect_delta
        recovery_total += recovery_delta
        chaos_total += chaos_delta
        errors = int(index == chaos_index)
        successes = 10
        attempts = successes + errors
        intervals.append(
            {
                "schema_version": 1,
                "record_type": "interval",
                "interval": index,
                "elapsed_secs": float(index * 60),
                "window_secs": 60.0,
                "operations": successes,
                "attempts": attempts,
                "successes": successes,
                "errors": errors,
                "ops_per_sec": successes / 60.0,
                "attempted_ops_per_sec": attempts / 60.0,
                "p50_us": 10,
                "p99_us": 20,
                "p999_us": 30,
                "max_us": 40,
                "reconnects": reconnect_delta,
                "reconnects_total": reconnect_total,
                "recoveries": recovery_delta,
                "recoveries_total": recovery_total,
                "chaos_injections": chaos_delta,
                "chaos_injections_total": chaos_total,
                "rss_bytes": 1024,
            }
        )
    successes = sum(record["successes"] for record in intervals)
    errors = sum(record["errors"] for record in intervals)
    attempts = successes + errors
    summary = {
        "schema_version": 1,
        "record_type": "summary",
        "elapsed_secs": 14400.0,
        "operations": successes,
        "attempts": attempts,
        "successes": successes,
        "errors": errors,
        "ops_per_sec": successes / 14400.0,
        "attempted_ops_per_sec": attempts / 14400.0,
        "p50_us": 10,
        "p99_us": 20,
        "p999_us": 30,
        "max_us": 40,
        "reconnects_total": reconnect_total,
        "recoveries_total": recovery_total,
        "chaos_injections_total": chaos_total,
        "rss_bytes": 1024,
    }
    return [metadata, *intervals, summary]


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8"
    )


class ResultTests(unittest.TestCase):
    def test_matrix_requires_every_cell_zero_errors_and_raw_samples(self) -> None:
        rows = [
            record(client, workload, payload, concurrency)
            for client in ("a", "b")
            for workload in ("Get", "Set")
            for payload in (16, 64)
            for concurrency in (1, 8)
        ]
        render_results.validate_matrix(
            rows,
            name="fixture",
            clients=("a", "b"),
            workloads=("Get", "Set"),
            payloads=(16, 64),
            concurrencies=(1, 8),
            require_samples=True,
        )

        broken = copy.deepcopy(rows)
        broken[0]["samples"][0]["commands_per_sec"] = 99_999.0
        with self.assertRaisesRegex(render_results.ResultError, "mean cannot be recomputed"):
            render_results.validate_matrix(
                broken,
                name="fixture",
                clients=("a", "b"),
                workloads=("Get", "Set"),
                payloads=(16, 64),
                concurrencies=(1, 8),
                require_samples=True,
            )

    def test_matrix_rejects_duplicate_missing_and_extra_cells(self) -> None:
        rows = [record("a", "Get", 16, 1)]
        with self.assertRaisesRegex(render_results.ResultError, "matrix mismatch"):
            render_results.validate_matrix(
                rows,
                name="fixture",
                clients=("a", "b"),
                workloads=("Get",),
                payloads=(16,),
                concurrencies=(1,),
            )
        with self.assertRaisesRegex(render_results.ResultError, "duplicate"):
            render_results.validate_matrix(
                rows * 2,
                name="fixture",
                clients=("a",),
                workloads=("Get",),
                payloads=(16,),
                concurrencies=(1,),
            )

    def test_pipeline_sweeps_are_intentionally_non_cartesian(self) -> None:
        depth_rows = [
            record("a", "Pipeline", payload, 1, commands_per_batch=10)
            for payload in render_results.PAYLOADS
        ]
        render_results.validate_matrix(
            depth_rows,
            name="pipeline-depth",
            clients=("a",),
            workloads=("Pipeline",),
            concurrencies=(1,),
            commands_per_batch=10,
            require_samples=True,
        )
        concurrency_rows = [
            record("a", "Pipeline", 1024, concurrency, commands_per_batch=100)
            for concurrency in render_results.CONCURRENCIES
        ]
        render_results.validate_matrix(
            concurrency_rows,
            name="pipeline-concurrency",
            clients=("a",),
            workloads=("Pipeline",),
            payloads=(1024,),
            commands_per_batch=100,
            require_samples=True,
        )
        with self.assertRaisesRegex(render_results.ResultError, "matrix mismatch"):
            render_results.validate_matrix(
                [*depth_rows, record("a", "Pipeline", 16, 8, commands_per_batch=10)],
                name="pipeline-depth",
                clients=("a",),
                workloads=("Pipeline",),
                concurrencies=(1,),
                commands_per_batch=10,
            )

    def test_cli_separates_incomplete_matrix_mode_from_publication(self) -> None:
        standalone = [
            record(client, workload, payload, concurrency)
            for client in render_results.STANDALONE_CLIENTS
            for workload in ("Set", "Get")
            for payload in render_results.PAYLOADS
            for concurrency in render_results.CONCURRENCIES
        ]
        cluster = [
            record(client, workload, payload, concurrency)
            for client in render_results.CLUSTER_CLIENTS
            for workload in ("Set", "Get")
            for payload in render_results.PAYLOADS
            for concurrency in render_results.CONCURRENCIES
        ]
        pipelines = {
            depth: [
                record(
                    client,
                    "Pipeline",
                    payload,
                    1,
                    commands_per_batch=depth,
                )
                for client in render_results.STANDALONE_CLIENTS
                for payload in render_results.PAYLOADS
            ]
            for depth in render_results.PIPELINE_DEPTHS
        }
        pipeline_concurrency = [
            record(client, "Pipeline", 1024, concurrency, commands_per_batch=100)
            for client in render_results.STANDALONE_CLIENTS
            for concurrency in render_results.CONCURRENCIES
        ]
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            standalone_path = directory / "standalone.json"
            cluster_path = directory / "cluster.json"
            concurrency_path = directory / "pipeline-concurrency.json"
            standalone_path.write_text(json.dumps(standalone), encoding="utf-8")
            cluster_path.write_text(json.dumps(cluster), encoding="utf-8")
            concurrency_path.write_text(json.dumps(pipeline_concurrency), encoding="utf-8")
            pipeline_args = []
            for depth, rows in pipelines.items():
                path = directory / f"pipeline-{depth}.json"
                path.write_text(json.dumps(rows), encoding="utf-8")
                pipeline_args.extend(("--pipeline-depth", f"{depth}={path}"))
            common = [
                "render_results.py",
                "--standalone",
                str(standalone_path),
                "--cluster",
                str(cluster_path),
                *pipeline_args,
                "--pipeline-concurrency",
                str(concurrency_path),
            ]

            matrix_output = directory / "matrix-output"
            with mock.patch.object(
                sys,
                "argv",
                [*common, "--matrix-only", "--output-dir", str(matrix_output)],
            ):
                self.assertEqual(render_results.main(), 0)
            incomplete = json.loads(
                (matrix_output / "summary.incomplete.json").read_text(encoding="utf-8")
            )
            self.assertFalse(incomplete["publication_complete"])
            self.assertFalse((matrix_output / "summary.json").exists())

            with mock.patch.object(
                sys, "argv", [*common, "--output-dir", str(directory / "missing-soak")]
            ):
                with self.assertRaisesRegex(render_results.ResultError, "requires --soak"):
                    render_results.main()

            soak_path = directory / "soak.jsonl"
            write_jsonl(soak_path, soak_fixture())
            publication_output = directory / "publication-output"
            with mock.patch.object(
                sys,
                "argv",
                [
                    *common,
                    "--soak",
                    str(soak_path),
                    "--output-dir",
                    str(publication_output),
                ],
            ):
                self.assertEqual(render_results.main(), 0)
            publication = json.loads(
                (publication_output / "summary.json").read_text(encoding="utf-8")
            )
            self.assertTrue(publication["publication_complete"])
            self.assertFalse((publication_output / "summary.incomplete.json").exists())

    def test_svg_render_is_deterministic_and_escapes_labels(self) -> None:
        standalone = [
            record("a&b", "Get", 1024, concurrency)
            for concurrency in render_results.CONCURRENCIES
        ]
        cluster = [
            record("cluster", "Get", 1024, concurrency)
            for concurrency in render_results.CONCURRENCIES
        ]
        with tempfile.TemporaryDirectory() as temporary:
            first = Path(temporary) / "first.svg"
            second = Path(temporary) / "second.svg"
            for output in (first, second):
                render_results.render_svg(
                    standalone,
                    cluster,
                    metric="p99_us",
                    title="p99 <test>",
                    output=output,
                )
            self.assertEqual(first.read_bytes(), second.read_bytes())
            text = first.read_text(encoding="utf-8")
            self.assertIn("p99 &lt;test&gt;", text)
            self.assertIn("a&amp;b", text)

    def test_four_hour_soak_requires_full_accounting(self) -> None:
        records = soak_fixture()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "soak.jsonl"
            write_jsonl(path, records)
            self.assertEqual(render_results.validate_soak(path), records[-1])

    def test_four_hour_soak_rejects_corruptions(self) -> None:
        cases = {
            "metadata": (lambda rows: rows[0].__setitem__("payload_bytes", 16), "metadata payload"),
            "window": (lambda rows: rows[1].__setitem__("window_secs", 30.0), "60-second window"),
            "rate": (lambda rows: rows[1].__setitem__("ops_per_sec", 99.0), "does not match"),
            "rss": (lambda rows: rows[1].__setitem__("rss_bytes", 0), "positive RSS"),
            "quantiles": (lambda rows: rows[1].__setitem__("p99_us", 5), "unordered"),
            "lifecycle": (
                lambda rows: rows[121].__setitem__("recoveries_total", 0),
                "does not reconcile",
            ),
            "summary_count": (
                lambda rows: rows[-1].__setitem__("successes", 1),
                "does not equal interval totals",
            ),
            "summary_rate": (
                lambda rows: rows[-1].__setitem__("ops_per_sec", 1.0),
                "does not match",
            ),
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "soak.jsonl"
            for name, (mutate, message) in cases.items():
                with self.subTest(name=name):
                    rows = soak_fixture()
                    mutate(rows)
                    write_jsonl(path, rows)
                    with self.assertRaisesRegex(render_results.ResultError, message):
                        render_results.validate_soak(path)

    def test_four_hour_soak_rejects_reconnect_before_chaos(self) -> None:
        records = soak_fixture(chaos_index=121, reconnect_index=120, recovery_index=121)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "soak.jsonl"
            write_jsonl(path, records)
            with self.assertRaisesRegex(render_results.ResultError, "reconnect.*before chaos"):
                render_results.validate_soak(path)


class MetadataTests(unittest.TestCase):
    def test_dependency_graph_removes_paths_credentials_and_raw_metadata(self) -> None:
        metadata = {
            "workspace_members": ["path+file:///Users/alice/private/repo#crate@0.1.0"],
            "packages": [
                {
                    "id": "path+file:///Users/alice/private/repo#crate@0.1.0",
                    "name": "crate",
                    "version": "0.1.0",
                    "source": None,
                    "manifest_path": "/Users/alice/private/repo/Cargo.toml",
                },
                {
                    "id": "git+https://alice:secret@example.com/team/repo.git?branch=main#abc",
                    "name": "dependency",
                    "version": "1.2.3",
                    "source": "git+https://alice:secret@example.com/team/repo.git?branch=main#abc",
                },
            ],
            "resolve": {
                "nodes": [
                    {
                        "id": "path+file:///Users/alice/private/repo#crate@0.1.0",
                        "features": ["z", "a"],
                        "deps": [],
                    },
                    {
                        "id": "git+https://alice:secret@example.com/team/repo.git?branch=main#abc",
                        "features": ["default"],
                        "deps": [],
                    },
                ]
            },
        }
        result = sanitize_metadata.sanitize(metadata)
        encoded = json.dumps(result)
        self.assertNotIn("/Users/", encoded)
        self.assertNotIn("alice:secret", encoded)
        self.assertNotIn("manifest_path", encoded)
        self.assertEqual(
            set(result["packages"][0]),
            {"name", "version", "source", "resolved_features"},
        )
        workspace = next(row for row in result["packages"] if row["name"] == "crate")
        self.assertEqual(workspace["source"], "workspace")
        self.assertEqual(workspace["resolved_features"], ["a", "z"])


class ManifestTests(unittest.TestCase):
    def test_matrix_only_manifest_is_resumable_but_never_publication_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_dir = Path(temporary)
            artifact_manifest.initialize(result_dir, "a" * 40, "matrix-only", "b" * 64)
            rendered = result_dir / "rendered"
            rendered.mkdir()
            (rendered / "summary.incomplete.json").write_text(
                json.dumps(
                    {
                        "mode": "matrix_only_development",
                        "publication_complete": False,
                        "incomplete_reason": "four_hour_soak_not_run",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            checkpoint = result_dir / "checkpoints" / "one"
            checkpoint.mkdir(parents=True)
            (checkpoint / "result.json").write_text("[]\n", encoding="utf-8")
            artifact_manifest.finalize(result_dir)
            artifact_manifest.verify(result_dir, "a" * 40, "matrix-only", "b" * 64)
            manifest = json.loads((result_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertTrue(manifest["run_complete"])
            self.assertFalse(manifest["publication_complete"])
            self.assertIn("four_hour_soak_missing", manifest["completion"])
            self.assertIn(
                "checkpoints/one/result.json",
                [artifact["path"] for artifact in manifest["artifacts"]],
            )

    def test_manifest_refuses_mismatched_provenance_and_unowned_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_dir = Path(temporary)
            (result_dir / "unknown").write_text("mine", encoding="utf-8")
            with self.assertRaisesRegex(artifact_manifest.ManifestError, "no provenance"):
                artifact_manifest.initialize(result_dir, "a" * 40, "publication", "b" * 64)
        with tempfile.TemporaryDirectory() as temporary:
            result_dir = Path(temporary)
            artifact_manifest.initialize(result_dir, "a" * 40, "publication", "b" * 64)
            with self.assertRaisesRegex(artifact_manifest.ManifestError, "different source"):
                artifact_manifest.initialize(result_dir, "c" * 40, "publication", "b" * 64)

    def test_manifest_refuses_partial_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_dir = Path(temporary)
            artifact_manifest.initialize(result_dir, "a" * 40, "matrix-only", "b" * 64)
            rendered = result_dir / "rendered"
            rendered.mkdir()
            (rendered / "summary.incomplete.json").write_text("{}\n", encoding="utf-8")
            partial = result_dir / "checkpoints" / "cell.partial"
            partial.mkdir(parents=True)
            (partial / "result.json").write_text("[]\n", encoding="utf-8")
            with self.assertRaisesRegex(artifact_manifest.ManifestError, "partial artifacts"):
                artifact_manifest.finalize(result_dir)


class RunnerContractTests(unittest.TestCase):
    def test_runner_has_privacy_sleep_and_mode_guards(self) -> None:
        runner = (SCRIPT_DIR / "run_publication.sh").read_text(encoding="utf-8")
        self.assertIn('script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")"', runner)
        self.assertIn("caffeinate -ims", runner)
        self.assertNotIn("caffeinate -dimsu", runner)
        self.assertNotIn("uname -a", runner)
        self.assertNotIn("hostname", runner)
        self.assertIn('run_mode="publication"', runner)
        self.assertIn('run_mode="matrix-only"', runner)
        self.assertNotIn("RUN_FOUR_HOUR_SOAK", runner)
        for variable in (
            "SOAK_MODE",
            "SOAK_CHAOS",
            "SOAK_DURATION_SECS",
            "SOAK_WARMUP_SECS",
            "SOAK_REPORT_INTERVAL_SECS",
            "SOAK_CHAOS_AFTER_SECS",
            "SOAK_CONCURRENCY",
            "SOAK_OPERATION_TIMEOUT_MS",
            "SOAK_ERROR_BACKOFF_MS",
            "SOAK_STARTUP_TIMEOUT_SECS",
            "SOAK_RECOVERY_TIMEOUT_SECS",
            "SOAK_PAYLOAD_BYTES",
            "SOAK_CLUSTER_SLOT",
            "SOAK_CLUSTER_NODE_TIMEOUT_MS",
            "SOAK_STANDALONE_PORT",
        ):
            self.assertGreaterEqual(runner.count(variable), 2, variable)


if __name__ == "__main__":
    unittest.main()
