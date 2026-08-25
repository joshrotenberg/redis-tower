#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import check_docs_links


class DocumentationLinkTests(unittest.TestCase):
    def test_linked_image_includes_outer_and_image_targets(self) -> None:
        self.assertEqual(
            check_docs_links.targets_from_markdown(
                "[![License](https://img.example/license.svg)](LICENSE)"
            ),
            ["LICENSE", "https://img.example/license.svg"],
        )

    def test_linked_image_missing_outer_target_is_reportable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            readme = root / "README.md"
            readme.write_text("[![License](https://img.example/license.svg)](LICENSE)\n")

            targets = check_docs_links.markdown_targets(readme)
            local = [
                check_docs_links.local_target(readme, target)
                for target in targets
                if check_docs_links.local_target(readme, target) is not None
            ]

            self.assertEqual(local, [((root / "LICENSE").resolve(), None)])
            self.assertFalse(local[0][0].exists())

    def test_canonical_repository_links_are_checked_against_local_tree(self) -> None:
        with mock.patch.object(check_docs_links, "ROOT", Path("/checkout")):
            resolved = check_docs_links.local_target(
                Path("/checkout/README.md"),
                "https://github.com/joshrotenberg/redis-tower/blob/main/docs/README.md",
            )

        self.assertEqual(resolved, (Path("/checkout/docs/README.md"), None))


if __name__ == "__main__":
    unittest.main()
