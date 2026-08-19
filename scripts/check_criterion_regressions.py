#!/usr/bin/env python3
"""Fail CI when Criterion reports a statistically clear performance regression."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Estimate:
    """A Criterion mean estimate and its confidence interval, in nanoseconds."""

    point: float
    lower: float
    upper: float


@dataclass(frozen=True)
class Comparison:
    """The result of comparing one benchmark across two saved baselines."""

    name: str
    change_percent: float
    confidence_intervals_overlap: bool

    def is_regression(self, threshold_percent: float) -> bool:
        return (
            self.change_percent > threshold_percent
            and not self.confidence_intervals_overlap
        )


def load_estimates(criterion_dir: Path, baseline: str) -> dict[str, Estimate]:
    """Load mean estimates from every benchmark in a saved Criterion baseline."""

    estimates: dict[str, Estimate] = {}
    pattern = f"**/{baseline}/estimates.json"
    for estimates_path in criterion_dir.glob(pattern):
        benchmark_path = estimates_path.parent.parent.relative_to(criterion_dir)
        name = benchmark_path.as_posix()
        with estimates_path.open(encoding="utf-8") as handle:
            mean = json.load(handle)["mean"]
        confidence_interval = mean["confidence_interval"]
        estimates[name] = Estimate(
            point=float(mean["point_estimate"]),
            lower=float(confidence_interval["lower_bound"]),
            upper=float(confidence_interval["upper_bound"]),
        )
    return estimates


def compare_estimates(
    baseline: dict[str, Estimate], candidate: dict[str, Estimate]
) -> tuple[list[Comparison], list[str], list[str]]:
    """Compare matching estimates and return comparisons, added, and removed names."""

    shared = sorted(baseline.keys() & candidate.keys())
    added = sorted(candidate.keys() - baseline.keys())
    removed = sorted(baseline.keys() - candidate.keys())
    comparisons = []

    for name in shared:
        before = baseline[name]
        after = candidate[name]
        if before.point <= 0:
            raise ValueError(f"baseline point estimate for {name} is not positive")
        comparisons.append(
            Comparison(
                name=name,
                change_percent=((after.point / before.point) - 1.0) * 100.0,
                confidence_intervals_overlap=not (
                    after.lower > before.upper or before.lower > after.upper
                ),
            )
        )

    return comparisons, added, removed


def print_comparisons(
    comparisons: list[Comparison],
    added: list[str],
    removed: list[str],
    threshold_percent: float,
) -> None:
    """Print one human-readable comparison set."""

    for comparison in comparisons:
        confidence = (
            "overlap"
            if comparison.confidence_intervals_overlap
            else "non-overlapping"
        )
        marker = (
            "REGRESSION"
            if comparison.is_regression(threshold_percent)
            else "ok"
        )
        print(
            f"{marker:10} {comparison.change_percent:+8.2f}%  "
            f"{confidence:15}  {comparison.name}"
        )

    for name in added:
        print(f"new benchmark (not gated): {name}")
    for name in removed:
        print(f"removed benchmark: {name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--criterion-dir",
        type=Path,
        default=Path("target/criterion"),
        help="Criterion output directory (default: target/criterion)",
    )
    parser.add_argument("--baseline", default="main", help="Saved baseline name")
    parser.add_argument("--candidate", default="candidate", help="Candidate name")
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Allowed mean-time increase as a percentage (default: 10)",
    )
    parser.add_argument(
        "--confirmation-baseline",
        help="Optional reversed-order confirmation baseline",
    )
    parser.add_argument(
        "--confirmation-candidate",
        help="Optional reversed-order confirmation candidate",
    )
    args = parser.parse_args(argv)

    if args.threshold < 0:
        parser.error("--threshold must be non-negative")
    if bool(args.confirmation_baseline) != bool(args.confirmation_candidate):
        parser.error(
            "--confirmation-baseline and --confirmation-candidate must be used together"
        )

    baseline = load_estimates(args.criterion_dir, args.baseline)
    candidate = load_estimates(args.criterion_dir, args.candidate)
    if not baseline:
        print(f"error: no Criterion estimates found for {args.baseline!r}", file=sys.stderr)
        return 2
    if not candidate:
        print(
            f"error: no Criterion estimates found for {args.candidate!r}",
            file=sys.stderr,
        )
        return 2

    comparisons, added, removed = compare_estimates(baseline, candidate)
    if not comparisons:
        print("error: the baselines have no benchmarks in common", file=sys.stderr)
        return 2

    regressions = [
        comparison
        for comparison in comparisons
        if comparison.is_regression(args.threshold)
    ]

    print(
        f"Criterion regression gate: {args.baseline} -> {args.candidate} "
        f"(limit: +{args.threshold:g}%)"
    )
    print_comparisons(comparisons, added, removed, args.threshold)

    if args.confirmation_baseline and args.confirmation_candidate:
        confirmation_baseline = load_estimates(
            args.criterion_dir, args.confirmation_baseline
        )
        confirmation_candidate = load_estimates(
            args.criterion_dir, args.confirmation_candidate
        )
        if not confirmation_baseline:
            print(
                "error: no Criterion estimates found for "
                f"{args.confirmation_baseline!r}",
                file=sys.stderr,
            )
            return 2
        if not confirmation_candidate:
            print(
                "error: no Criterion estimates found for "
                f"{args.confirmation_candidate!r}",
                file=sys.stderr,
            )
            return 2

        (
            confirmation_comparisons,
            confirmation_added,
            confirmation_removed,
        ) = compare_estimates(confirmation_baseline, confirmation_candidate)
        if not confirmation_comparisons:
            print(
                "error: the confirmation baselines have no benchmarks in common",
                file=sys.stderr,
            )
            return 2

        print(
            "\nReversed-order confirmation: "
            f"{args.confirmation_baseline} -> {args.confirmation_candidate}"
        )
        print_comparisons(
            confirmation_comparisons,
            confirmation_added,
            confirmation_removed,
            args.threshold,
        )
        confirmation_regression_names = {
            comparison.name
            for comparison in confirmation_comparisons
            if comparison.is_regression(args.threshold)
        }
        regressions = [
            comparison
            for comparison in regressions
            if comparison.name in confirmation_regression_names
        ]

    if regressions:
        qualifier = (
            "reproducibly "
            if args.confirmation_baseline and args.confirmation_candidate
            else ""
        )
        print(
            f"\n{len(regressions)} benchmark(s) {qualifier}exceeded "
            f"+{args.threshold:g}% with non-overlapping confidence intervals.",
            file=sys.stderr,
        )
        return 1

    if args.confirmation_baseline and args.confirmation_candidate:
        print("\nNo reproducible performance regressions found.")
    else:
        print("\nNo statistically clear performance regressions found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
