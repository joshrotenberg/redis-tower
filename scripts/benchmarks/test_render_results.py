#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("render_results.py")
SPEC = importlib.util.spec_from_file_location("render_results", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
render_results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(render_results)


def record(
    client: str,
    workload: str,
    payload: int,
    concurrency: int,
    *,
    commands_per_batch: int = 1,
) -> dict[str, object]:
    return {
        "schema_version": 2,
        "client_id": client,
        "workload": workload,
        "payload_bytes": payload,
        "concurrency": concurrency,
        "runs": 3,
        "commands_per_batch": commands_per_batch,
        "total_commands": 100,
        "errors": 0,
        "commands_per_sec_mean": float(100 * concurrency),
        "commands_per_sec_stddev": 1.0,
        "p99_us": float(1000 // concurrency),
    }


class ResultTests(unittest.TestCase):
    def test_matrix_requires_every_cell_and_zero_errors(self) -> None:
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
        )

        rows[0]["errors"] = 1
        with self.assertRaisesRegex(render_results.ResultError, "reports errors"):
            render_results.validate_matrix(
                rows,
                name="fixture",
                clients=("a", "b"),
                workloads=("Get", "Set"),
                payloads=(16, 64),
                concurrencies=(1, 8),
            )

    def test_matrix_rejects_duplicate_or_missing_cells(self) -> None:
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

    def test_four_hour_soak_requires_complete_exact_accounting(self) -> None:
        metadata = {
            "schema_version": 1,
            "record_type": "metadata",
            "mode": "standalone",
            "duration_secs": 14400.0,
            "report_interval_secs": 60.0,
            "chaos": "standalone_sigkill",
            "chaos_after_secs": 7200.0,
            "reconnect_accounting": "exact_connection_event_reconnected",
        }
        intervals = [
            {
                "schema_version": 1,
                "record_type": "interval",
                "interval": index,
                "elapsed_secs": float(index * 60),
                "successes": 10,
                "errors": 1 if index == 120 else 0,
                "attempts": 11 if index == 120 else 10,
            }
            for index in range(1, 241)
        ]
        summary = {
            "schema_version": 1,
            "record_type": "summary",
            "elapsed_secs": 14400.1,
            "successes": 2400,
            "errors": 1,
            "attempts": 2401,
            "reconnects_total": 1,
            "recoveries_total": 1,
            "chaos_injections_total": 1,
            "p50_us": 10,
            "p99_us": 20,
            "p999_us": 30,
            "max_us": 40,
            "rss_bytes": 1024,
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "soak.jsonl"
            path.write_text(
                "\n".join(json.dumps(row) for row in [metadata, *intervals, summary])
                + "\n",
                encoding="utf-8",
            )
            self.assertEqual(render_results.validate_soak(path), summary)
            summary["reconnects_total"] = 2
            path.write_text(
                "\n".join(json.dumps(row) for row in [metadata, *intervals, summary])
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(render_results.ResultError, "exactly one reconnect"):
                render_results.validate_soak(path)


if __name__ == "__main__":
    unittest.main()
