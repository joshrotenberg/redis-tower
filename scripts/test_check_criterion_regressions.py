import json
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import tempfile
import unittest
from pathlib import Path

from check_criterion_regressions import (
    Estimate,
    compare_estimates,
    load_estimates,
    main,
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

    def test_missing_initial_regression_in_confirmation_fails_closed(self) -> None:
        for missing in (
            ("main-confirm",),
            ("candidate-confirm",),
            ("main-confirm", "candidate-confirm"),
        ):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                # Another clean benchmark must remain comparable: otherwise
                # empty-baseline/no-common-benchmark checks mask this gap.
                for saved in ("main", "candidate", "main-confirm", "candidate-confirm"):
                    self._write_estimate(root, saved, 100.0, 98.0, 102.0, "codec/encode")
                self._write_estimate(root, "main", 100.0, 98.0, 102.0)
                self._write_estimate(root, "candidate", 120.0, 117.0, 123.0)
                for saved in ("main-confirm", "candidate-confirm"):
                    if saved not in missing:
                        self._write_estimate(root, saved, 100.0, 98.0, 102.0)
                errors = StringIO()
                with redirect_stdout(StringIO()), redirect_stderr(errors):
                    status = main([
                        "--criterion-dir", str(root),
                        "--confirmation-baseline", "main-confirm",
                        "--confirmation-candidate", "candidate-confirm",
                    ])
                self.assertEqual(status, 2, "missing evidence must not clear a regression")
                self.assertIn("codec/decode", errors.getvalue())

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
