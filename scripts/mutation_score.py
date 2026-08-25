#!/usr/bin/env python3
"""Summarize cargo-mutants outcomes as stable JSON and Markdown evidence."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class MutationCounts:
    caught: int = 0
    missed: int = 0
    timeout: int = 0
    unviable: int = 0

    @property
    def scored(self) -> int:
        return self.caught + self.missed + self.timeout

    @property
    def score(self) -> float | None:
        return self.caught / self.scored if self.scored else None

    def __add__(self, other: "MutationCounts") -> "MutationCounts":
        return MutationCounts(
            caught=self.caught + other.caught,
            missed=self.missed + other.missed,
            timeout=self.timeout + other.timeout,
            unviable=self.unviable + other.unviable,
        )


def load_outcomes(path: Path) -> MutationCounts:
    payload = json.loads(path.read_text())
    values = {}
    for name in ("caught", "missed", "timeout", "unviable"):
        value = payload.get(name)
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"{path}: {name} must be a non-negative integer")
        values[name] = value
    return MutationCounts(**values)


def package_name(path: Path) -> str:
    parent = path.parent
    if parent.name == "mutants.out":
        parent = parent.parent
    return parent.name.removeprefix("mutation-")


def report(paths: list[Path]) -> dict[str, object]:
    packages: dict[str, MutationCounts] = {}
    for path in sorted(paths):
        name = package_name(path)
        if name in packages:
            raise ValueError(f"duplicate mutation report for package {name}")
        packages[name] = load_outcomes(path)
    if not packages:
        raise ValueError("no cargo-mutants outcomes were supplied")

    total = MutationCounts()
    for counts in packages.values():
        total += counts

    def serialize(counts: MutationCounts) -> dict[str, object]:
        result: dict[str, object] = asdict(counts)
        result["scored"] = counts.scored
        result["score"] = counts.score
        return result

    return {
        "schema_version": 1,
        "packages": {name: serialize(counts) for name, counts in packages.items()},
        "total": serialize(total),
    }


def collapse_package(payload: dict[str, object], name: str) -> dict[str, object]:
    """Collapse one or more shard reports into a single package row."""
    total = payload["total"]
    assert isinstance(total, dict)
    return {
        "schema_version": payload["schema_version"],
        "packages": {name: total},
        "total": total,
    }


def write_outcomes(path: Path, payload: dict[str, object]) -> None:
    """Write aggregate counts using cargo-mutants' outcomes schema."""
    total = payload["total"]
    assert isinstance(total, dict)
    counts = {
        name: total[name]
        for name in ("caught", "missed", "timeout", "unviable")
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(counts, indent=2, sort_keys=True) + "\n")


def markdown(payload: dict[str, object], minimum_score: float) -> str:
    lines = [
        "# Mutation testing score",
        "",
        "The conservative score is `caught / (caught + missed + timeout)`; "
        "unviable mutants are reported but excluded.",
        "",
        "| Package | Score | Caught | Missed | Timeout | Unviable |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    packages = payload["packages"]
    assert isinstance(packages, dict)
    for name, raw in packages.items():
        assert isinstance(raw, dict)
        score = raw["score"]
        display = "n/a" if score is None else f"{score:.1%}"
        lines.append(
            f"| {name} | {display} | {raw['caught']} | {raw['missed']} | "
            f"{raw['timeout']} | {raw['unviable']} |"
        )
    total = payload["total"]
    assert isinstance(total, dict)
    total_score = total["score"]
    display = "n/a" if total_score is None else f"{total_score:.1%}"
    meets_floor = total_score is not None and total_score >= minimum_score
    status = "meets" if meets_floor else "is below"
    lines.extend(
        [
            f"| **Total** | **{display}** | **{total['caught']}** | "
            f"**{total['missed']}** | **{total['timeout']}** | "
            f"**{total['unviable']}** |",
            "",
            f"The total score {status} the {minimum_score:.1%} tracking floor.",
        ]
    )
    return "\n".join(lines) + "\n"


def find_outcomes(inputs: list[Path]) -> list[Path]:
    paths: list[Path] = []
    for path in inputs:
        if path.is_dir():
            paths.extend(path.rglob("outcomes.json"))
        else:
            paths.append(path)
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--outcomes-output", type=Path)
    parser.add_argument("--package-name")
    parser.add_argument("--expected-reports", type=int)
    parser.add_argument("--minimum-score", type=float, default=0.0)
    parser.add_argument("--enforce", action="store_true")
    args = parser.parse_args(argv)
    if not 0.0 <= args.minimum_score <= 1.0:
        parser.error("--minimum-score must be between 0 and 1")

    try:
        paths = find_outcomes(args.inputs)
        if args.expected_reports is not None and len(paths) != args.expected_reports:
            raise ValueError(
                f"expected {args.expected_reports} mutation reports, found {len(paths)}"
            )
        payload = report(paths)
        if args.package_name:
            payload = collapse_package(payload, args.package_name)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        return 2

    rendered = markdown(payload, args.minimum_score)
    total = payload["total"]
    assert isinstance(total, dict)
    score = total["score"]
    payload["budget"] = {
        "minimum_score": args.minimum_score,
        "meets_floor": score is not None and score >= args.minimum_score,
    }
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(rendered)
    if args.outcomes_output:
        write_outcomes(args.outcomes_output, payload)
    print(rendered, end="")

    if args.enforce and (score is None or score < args.minimum_score):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
