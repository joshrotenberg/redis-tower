import json
import tempfile
import unittest
from pathlib import Path

from generate_command_coverage import (
    COMMAND_ALIASES,
    SOURCES,
    collect_typed_names,
    load_metadata,
    render_report,
    resolve_typed_names,
)


class CommandCoverageTests(unittest.TestCase):
    def test_scope_and_alias_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata_dir = root / "metadata"
            source_dir = root / "src"
            metadata_dir.mkdir()
            source_dir.mkdir()

            for index, source in enumerate(SOURCES):
                commands = {}
                if index == 0:
                    commands = {
                        "GET": {"group": "string", "summary": "Gets a value."},
                        "OLD": {
                            "group": "string",
                            "summary": "Old command.",
                            "deprecated_since": "8.0.0",
                        },
                        "FAMILY": {
                            "group": "server",
                            "summary": "A container for family commands.",
                        },
                        "INTERNAL": {
                            "group": "server",
                            "summary": "Internal command.",
                            "doc_flags": ["syscmd"],
                        },
                    }
                (metadata_dir / source.filename).write_text(
                    json.dumps(commands), encoding="utf-8"
                )

            (source_dir / "commands.rs").write_text(
                """
                fn name(&self) -> &str { "GET" }
                fn name(&self) -> &str { "OLD" }
                """,
                encoding="utf-8",
            )

            metadata = load_metadata(metadata_dir)
            names = collect_typed_names(source_dir)
            resolved = resolve_typed_names(names, metadata)
            report = render_report(metadata, resolved)

            self.assertIn("**1/1 (100.0%)**", report)
            self.assertIn("`OLD`", report)
            self.assertNotIn("`FAMILY`", report)
            self.assertNotIn("`INTERNAL`", report)

    def test_unknown_command_name_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "MADEUP"):
            resolve_typed_names({"MADEUP"}, {})

    def test_every_alias_has_a_nonempty_target(self) -> None:
        for alias, targets in COMMAND_ALIASES.items():
            self.assertTrue(alias)
            self.assertTrue(targets)
            self.assertTrue(all(targets))


if __name__ == "__main__":
    unittest.main()
