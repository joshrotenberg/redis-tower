#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path

import check_release_hygiene


ROOT_MANIFEST = """
[workspace]
members = ["crates/good", "crates/private"]

[workspace.package]
license = "MIT OR Apache-2.0"
"""

GOOD_MANIFEST = """
[package]
name = "redis-tower-good"
version = "0.1.0"
description = "fixture"
license = "MIT"
repository = "https://example.com/repo"
homepage = "https://example.com/repo"
readme = "README.md"
rust-version = "1.88"

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
"""

PRIVATE_MANIFEST = """
[package]
name = "redis-tower-private"
version = "0.0.0"
publish = false
"""


class ReleaseHygieneTests(unittest.TestCase):
    def fixture(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        (root / "Cargo.toml").write_text(ROOT_MANIFEST)
        for name, manifest in (("good", GOOD_MANIFEST), ("private", PRIVATE_MANIFEST)):
            package = root / "crates" / name
            (package / "src").mkdir(parents=True)
            (package / "Cargo.toml").write_text(manifest)
            (package / "src" / "lib.rs").write_text("#![deny(missing_docs)]\n//! docs\n")
            (package / "README.md").write_text("[Guide](https://example.com/guide)\n")
            (package / "CHANGELOG.md").write_text("# Changelog\n\n## [Unreleased]\n")
        (root / "LICENSE-MIT").write_text("MIT\n")
        (root / "LICENSE-APACHE").write_text("Apache-2.0\n")
        return temporary, root

    def test_audit_accepts_complete_publishable_crate_and_ignores_private_crate(self) -> None:
        temporary, root = self.fixture()
        self.addCleanup(temporary.cleanup)
        packages, errors = check_release_hygiene.audit(root, package_contents=False)
        self.assertEqual([package.name for package in packages], ["redis-tower-good"])
        self.assertEqual(errors, [])

    def test_audit_reports_missing_docs_enforcement_and_docs_rs_metadata(self) -> None:
        temporary, root = self.fixture()
        self.addCleanup(temporary.cleanup)
        manifest = root / "crates" / "good" / "Cargo.toml"
        manifest.write_text(GOOD_MANIFEST.split("[package.metadata.docs.rs]")[0])
        (root / "crates" / "good" / "src" / "lib.rs").write_text("//! docs\n")
        _, errors = check_release_hygiene.audit(root, package_contents=False)
        self.assertTrue(any("metadata.docs.rs" in error for error in errors))
        self.assertTrue(any("deny missing_docs" in error for error in errors))

    def test_audit_rejects_repository_relative_release_readme_links(self) -> None:
        temporary, root = self.fixture()
        self.addCleanup(temporary.cleanup)
        (root / "crates" / "good" / "README.md").write_text(
            "[![License](https://img.example/license.svg)](../../LICENSE-MIT)\n"
        )

        _, errors = check_release_hygiene.audit(root, package_contents=False)

        self.assertTrue(any("repository-relative" in error for error in errors))

    def test_audit_requires_a_changelog_with_an_unreleased_section(self) -> None:
        temporary, root = self.fixture()
        self.addCleanup(temporary.cleanup)
        changelog = root / "crates" / "good" / "CHANGELOG.md"
        changelog.write_text("# Changelog\n")

        _, errors = check_release_hygiene.audit(root, package_contents=False)

        self.assertIn(
            "redis-tower-good: CHANGELOG.md must contain an Unreleased section",
            errors,
        )

    def test_audit_requires_dual_license_expression_and_files(self) -> None:
        temporary, root = self.fixture()
        self.addCleanup(temporary.cleanup)
        (root / "LICENSE-APACHE").unlink()

        _, errors = check_release_hygiene.audit(root, package_contents=False)

        self.assertIn("repository is missing LICENSE-APACHE", errors)


if __name__ == "__main__":
    unittest.main()
