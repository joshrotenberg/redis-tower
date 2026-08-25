#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

import mutation_score


class MutationScoreTests(unittest.TestCase):
    def write_outcomes(self, root: Path, package: str, **counts: int) -> Path:
        path = root / package / "mutants.out" / "outcomes.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(counts))
        return path

    def test_report_aggregates_packages_and_excludes_unviable_from_score(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.write_outcomes(
                root, "redis-tower", caught=8, missed=1, timeout=1, unviable=3
            )
            second = self.write_outcomes(
                root, "redis-tower-core", caught=2, missed=0, timeout=0, unviable=1
            )

            payload = mutation_score.report([first, second])

            self.assertEqual(payload["total"]["caught"], 10)
            self.assertEqual(payload["total"]["scored"], 12)
            self.assertAlmostEqual(payload["total"]["score"], 10 / 12)
            self.assertIn("**83.3%**", mutation_score.markdown(payload, 0.75))

    def test_report_rejects_invalid_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_outcomes(
                Path(directory),
                "redis-tower",
                caught=1,
                missed=-1,
                timeout=0,
                unviable=0,
            )
            with self.assertRaisesRegex(ValueError, "missed"):
                mutation_score.report([path])

    def test_find_outcomes_scans_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = self.write_outcomes(
                root, "redis-tower", caught=1, missed=0, timeout=0, unviable=0
            )
            self.assertEqual(mutation_score.find_outcomes([root]), [expected])

    def test_collapses_shards_and_writes_aggregate_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.write_outcomes(
                root, "shard-1", caught=3, missed=1, timeout=0, unviable=2
            )
            second = self.write_outcomes(
                root, "shard-2", caught=2, missed=0, timeout=1, unviable=1
            )
            payload = mutation_score.collapse_package(
                mutation_score.report([first, second]), "redis-tower"
            )
            output = root / "package" / "mutants.out" / "outcomes.json"

            mutation_score.write_outcomes(output, payload)

            self.assertEqual(list(payload["packages"]), ["redis-tower"])
            self.assertEqual(payload["total"]["caught"], 5)
            self.assertEqual(
                json.loads(output.read_text()),
                {"caught": 5, "missed": 1, "timeout": 1, "unviable": 3},
            )

    def test_cli_rejects_missing_shard_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_outcomes(
                root, "shard-1", caught=1, missed=0, timeout=0, unviable=0
            )
            self.assertEqual(
                mutation_score.main([str(root), "--expected-reports", "2"]), 2
            )


if __name__ == "__main__":
    unittest.main()
