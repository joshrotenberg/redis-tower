import argparse
import json
import tempfile
import unittest
from pathlib import Path

from check_criterion_regressions import (
    Estimate,
    ThresholdTable,
    build_threshold_table,
    compare_estimates,
    load_estimates,
    main,
    parse_threshold_override,
)


class CriterionRegressionTests(unittest.TestCase):
    def test_loads_saved_criterion_mean(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            estimates_path = root / "codec" / "decode" / "main" / "estimates.json"
            estimates_path.parent.mkdir(parents=True)
            estimates_path.write_text(
                json.dumps(
                    {
                        "mean": {
                            "point_estimate": 100.0,
                            "confidence_interval": {
                                "lower_bound": 95.0,
                                "upper_bound": 105.0,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                load_estimates(root, "main"),
                {"codec/decode": Estimate(point=100.0, lower=95.0, upper=105.0)},
            )

    def test_only_clear_slowdowns_are_regressions(self) -> None:
        baseline = {
            "clear": Estimate(point=100.0, lower=98.0, upper=102.0),
            "noisy": Estimate(point=100.0, lower=90.0, upper=110.0),
        }
        candidate = {
            "clear": Estimate(point=115.0, lower=112.0, upper=118.0),
            "noisy": Estimate(point=115.0, lower=105.0, upper=125.0),
        }

        comparisons, added, removed = compare_estimates(baseline, candidate)

        self.assertEqual(added, [])
        self.assertEqual(removed, [])
        self.assertTrue(comparisons[0].is_regression(10.0))
        self.assertFalse(comparisons[1].is_regression(10.0))

    def test_main_fails_for_clear_regression(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_estimate(root, "main", 100.0, 98.0, 102.0)
            self._write_estimate(root, "candidate", 120.0, 117.0, 123.0)

            self.assertEqual(main(["--criterion-dir", str(root)]), 1)

    def test_confirmation_requires_the_same_benchmark_to_regress(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_estimate(root, "main", 100.0, 98.0, 102.0, "codec/decode")
            self._write_estimate(
                root, "candidate", 120.0, 117.0, 123.0, "codec/decode"
            )
            self._write_estimate(
                root, "main-confirm", 100.0, 98.0, 102.0, "codec/decode"
            )
            self._write_estimate(
                root, "candidate-confirm", 101.0, 99.0, 103.0, "codec/decode"
            )
            self._write_estimate(root, "main", 100.0, 98.0, 102.0, "codec/encode")
            self._write_estimate(
                root, "candidate", 101.0, 99.0, 103.0, "codec/encode"
            )
            self._write_estimate(
                root, "main-confirm", 100.0, 98.0, 102.0, "codec/encode"
            )
            self._write_estimate(
                root, "candidate-confirm", 120.0, 117.0, 123.0, "codec/encode"
            )

            self.assertEqual(
                main(
                    [
                        "--criterion-dir",
                        str(root),
                        "--confirmation-baseline",
                        "main-confirm",
                        "--confirmation-candidate",
                        "candidate-confirm",
                    ]
                ),
                0,
            )

    def test_confirmation_fails_for_reproducible_regression(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for baseline in ("main", "main-confirm"):
                self._write_estimate(root, baseline, 100.0, 98.0, 102.0)
            for candidate in ("candidate", "candidate-confirm"):
                self._write_estimate(root, candidate, 120.0, 117.0, 123.0)

            self.assertEqual(
                main(
                    [
                        "--criterion-dir",
                        str(root),
                        "--confirmation-baseline",
                        "main-confirm",
                        "--confirmation-candidate",
                        "candidate-confirm",
                    ]
                ),
                1,
            )

    def test_per_benchmark_threshold_overrides_the_default(self) -> None:
        table = ThresholdTable(default=10.0, overrides={"codec_decode/big": 25.0})

        self.assertEqual(table.for_benchmark("codec_decode/big"), 25.0)
        self.assertEqual(table.for_benchmark("codec_decode/small"), 10.0)
        self.assertTrue(table.is_overridden("codec_decode/big"))
        self.assertFalse(table.is_overridden("codec_decode/small"))

    def test_threshold_override_parsing(self) -> None:
        self.assertEqual(
            parse_threshold_override("codec_decode/bulk_string_1kb=25"),
            ("codec_decode/bulk_string_1kb", 25.0),
        )

        for malformed in ("no-equals-sign", "=25", "codec/decode=fast", "codec/x=-1"):
            with self.subTest(value=malformed):
                with self.assertRaises(argparse.ArgumentTypeError):
                    parse_threshold_override(malformed)

    def test_conflicting_thresholds_for_one_benchmark_are_rejected(self) -> None:
        self.assertEqual(
            build_threshold_table(10.0, [("codec/a", 25.0), ("codec/a", 25.0)]),
            ThresholdTable(default=10.0, overrides={"codec/a": 25.0}),
        )

        with self.assertRaises(ValueError):
            build_threshold_table(10.0, [("codec/a", 25.0), ("codec/a", 30.0)])

    def test_override_spares_a_benchmark_the_default_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_estimate(root, "main", 100.0, 98.0, 102.0)
            self._write_estimate(root, "candidate", 120.0, 117.0, 123.0)

            self.assertEqual(main(["--criterion-dir", str(root)]), 1)
            self.assertEqual(
                main(
                    [
                        "--criterion-dir",
                        str(root),
                        "--benchmark-threshold",
                        "codec/decode=25",
                    ]
                ),
                0,
            )

    def test_override_still_gates_above_its_own_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_estimate(root, "main", 100.0, 98.0, 102.0)
            self._write_estimate(root, "candidate", 130.0, 127.0, 133.0)

            self.assertEqual(
                main(
                    [
                        "--criterion-dir",
                        str(root),
                        "--benchmark-threshold",
                        "codec/decode=25",
                    ]
                ),
                1,
            )

    def test_override_naming_no_benchmark_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_estimate(root, "main", 100.0, 98.0, 102.0)
            self._write_estimate(root, "candidate", 101.0, 99.0, 103.0)

            self.assertEqual(
                main(
                    [
                        "--criterion-dir",
                        str(root),
                        "--benchmark-threshold",
                        "codec/renamed=25",
                    ]
                ),
                2,
            )

    def test_override_applies_to_the_confirmation_pass_too(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for baseline in ("main", "main-confirm"):
                self._write_estimate(root, baseline, 100.0, 98.0, 102.0)
            for candidate in ("candidate", "candidate-confirm"):
                self._write_estimate(root, candidate, 120.0, 117.0, 123.0)

            arguments = [
                "--criterion-dir",
                str(root),
                "--confirmation-baseline",
                "main-confirm",
                "--confirmation-candidate",
                "candidate-confirm",
            ]

            self.assertEqual(main(arguments), 1)
            self.assertEqual(
                main(arguments + ["--benchmark-threshold", "codec/decode=25"]), 0
            )

    @staticmethod
    def _write_estimate(
        root: Path,
        baseline: str,
        point: float,
        lower: float,
        upper: float,
        name: str = "codec/decode",
    ) -> None:
        path = root / name / baseline / "estimates.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "mean": {
                        "point_estimate": point,
                        "confidence_interval": {
                            "lower_bound": lower,
                            "upper_bound": upper,
                        },
                    }
                }
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
