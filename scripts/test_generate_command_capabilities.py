import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from generate_command_capabilities import (
    build_ledger,
    compare_metadata,
    serialized_json,
)
from generate_command_coverage import SOURCES
from fetch_command_metadata import fetch_snapshot


class CapabilityFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.metadata = root / "metadata"
        self.source = root / "src"
        self.details = root / "details.json"
        self.metadata.mkdir()
        self.source.mkdir()
        commands = {
            "GET": {"group": "string", "summary": "Get.", "since": "1.0.0"},
            "SUBSCRIBE": {"group": "pubsub", "summary": "Subscribe.", "since": "2.0.0"},
            "ACL LOG": {"group": "server", "summary": "ACL log.", "since": "6.0.0"},
            "OLD": {
                "group": "string",
                "summary": "Old.",
                "deprecated_since": "8.0.0",
            },
            "FAMILY": {"group": "server", "summary": "A container for commands."},
            "INTERNAL": {
                "group": "server",
                "summary": "Internal.",
                "doc_flags": ["syscmd"],
            },
        }
        for index, source in enumerate(SOURCES):
            payload = commands if index == 0 else {}
            (self.metadata / source.filename).write_text(
                json.dumps(payload), encoding="utf-8"
            )
        (self.source / "commands.rs").write_text(
            """
            impl Command for Get {
                fn name(&self) -> &str { "GET" }
            }
            impl Command for AclLogReset {
                fn name(&self) -> &str { "ACL LOG RESET" }
            }
            """,
            encoding="utf-8",
        )
        (self.root / "api.rs").write_text(
            "fn subscribe_bytes() {}\n", encoding="utf-8"
        )
        metadata_files = [
            {
                "filename": source.filename,
                "sha256": hashlib.sha256(
                    (self.metadata / source.filename).read_bytes()
                ).hexdigest(),
            }
            for source in SOURCES
        ]
        self.value = {
            "schema_version": 1,
            "provenance": {
                "redis_version": "fixture",
                "docs_revision": "0" * 40,
                "source_url": "https://example.invalid/fixture",
                "metadata_files": metadata_files,
            },
            "non_builder_commands": [
                {
                    "name": "SUBSCRIBE",
                    "disposition": "dedicated_api",
                    "api": ["FixturePubSub"],
                    "rationale": "Stateful streaming API.",
                    "source_refs": [
                        {"path": "api.rs", "symbol": "subscribe_bytes"}
                    ],
                }
            ],
            "command_details": [],
            "wire_contracts": [],
            "behaviors": [],
        }
        self.write_details()

    def write_details(self) -> None:
        self.details.write_text(json.dumps(self.value), encoding="utf-8")

    def build(self) -> dict[str, object]:
        return build_ledger(
            metadata_dir=self.metadata,
            details_path=self.details,
            source_dir=self.source,
            repo_root=self.root,
        )


class CommandCapabilityTests(unittest.TestCase):
    def test_refresh_requires_an_immutable_commit_and_records_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "snapshot"
            with self.assertRaisesRegex(ValueError, "full 40-character"):
                fetch_snapshot("main", output)

            payload = b'{"GET":{"group":"string"}}\n'
            with mock.patch("fetch_command_metadata.fetch", return_value=payload):
                provenance = fetch_snapshot("a" * 40, output)
            self.assertEqual(provenance["docs_revision"], "a" * 40)
            self.assertEqual(len(provenance["files"]), len(SOURCES))
            self.assertTrue((output / "PROVENANCE.json").is_file())

    def test_explicit_dispositions_scope_and_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            ledger = fixture.build()
            rows = {row["name"]: row for row in ledger["commands"]}

            self.assertEqual(
                rows["GET"]["availability"]["disposition"], "typed_builder"
            )
            self.assertEqual(
                rows["ACL LOG"]["availability"]["disposition"], "typed_builder"
            )
            self.assertEqual(
                rows["SUBSCRIBE"]["availability"]["disposition"], "dedicated_api"
            )
            self.assertEqual(
                rows["OLD"]["availability"]["disposition"], "excluded_deprecated"
            )
            self.assertEqual(
                rows["FAMILY"]["availability"]["disposition"], "excluded_container"
            )
            self.assertEqual(
                rows["INTERNAL"]["availability"]["disposition"], "excluded_system"
            )

    def test_missing_non_builder_disposition_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            fixture.value["non_builder_commands"] = []
            fixture.write_details()
            with self.assertRaisesRegex(ValueError, "exactly cover"):
                fixture.build()

    def test_duplicate_non_builder_identity_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            fixture.value["non_builder_commands"].append(
                copy.deepcopy(fixture.value["non_builder_commands"][0])
            )
            fixture.write_details()
            with self.assertRaisesRegex(ValueError, "duplicate non-builder identity"):
                fixture.build()

    def test_invalid_non_builder_disposition_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            fixture.value["non_builder_commands"][0]["disposition"] = "maybe"
            fixture.write_details()
            with self.assertRaisesRegex(ValueError, "invalid non-builder disposition"):
                fixture.build()

    def test_unknown_command_detail_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            fixture.value["command_details"] = [{"name": "MADE UP"}]
            fixture.write_details()
            with self.assertRaisesRegex(ValueError, "unknown names"):
                fixture.build()

    def test_stale_evidence_reference_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            fixture.value["non_builder_commands"][0]["source_refs"][0]["symbol"] = "gone"
            fixture.write_details()
            with self.assertRaisesRegex(ValueError, "absent from api.rs"):
                fixture.build()

    def test_generation_is_deterministic_and_offline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            first = serialized_json(fixture.build())
            second = serialized_json(fixture.build())
            self.assertEqual(first, second)

    def test_metadata_comparison_reports_definition_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = CapabilityFixture(Path(directory))
            candidate = Path(directory) / "candidate"
            candidate.mkdir()
            for source in SOURCES:
                value = json.loads((fixture.metadata / source.filename).read_text())
                if source == SOURCES[0]:
                    value["GET"]["since"] = "2.0.0"
                    value["GET"]["arguments"] = [{"name": "key", "type": "key"}]
                    value["NEW"] = {"group": "string", "summary": "New."}
                    del value["OLD"]
                (candidate / source.filename).write_text(json.dumps(value))

            report, drift = compare_metadata(fixture.metadata, candidate)
            self.assertTrue(drift)
            self.assertIn("`NEW`", report)
            self.assertIn("`OLD`", report)
            self.assertIn("`GET`: since, arguments/options", report)


if __name__ == "__main__":
    unittest.main()
