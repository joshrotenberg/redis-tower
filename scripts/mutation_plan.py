#!/usr/bin/env python3
"""Build the GitHub Actions matrix for workspace mutation testing."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# Large packages are split using round-robin cargo-mutants shards so every job
# stays comfortably within the workflow's three-hour limit. The counts are
# based on the complete 2026-08-25 evidence run: redis-tower discovered 1,781
# mutants and redis-tower-commands discovered 3,562.
SHARD_COUNTS = {
    "redis-tower": 12,
    "redis-tower-commands": 4,
}


def publishable_packages(metadata: dict[str, object]) -> list[str]:
    workspace_members = set(metadata["workspace_members"])
    packages = metadata["packages"]
    assert isinstance(packages, list)
    return sorted(
        package["name"]
        for package in packages
        if package["id"] in workspace_members and package.get("publish") != []
    )


def build_plan(packages: list[str]) -> dict[str, object]:
    unknown = sorted(set(SHARD_COUNTS) - set(packages))
    if unknown:
        raise ValueError(f"mutation shard override references unknown package(s): {unknown}")

    shards = []
    package_rows = []
    for package in packages:
        count = SHARD_COUNTS.get(package, 1)
        package_rows.append({"package": package, "shards": count})
        shards.extend(
            {"package": package, "shard": shard, "shards": count}
            for shard in range(1, count + 1)
        )
    return {
        "matrix": {"include": shards},
        "packages": {"include": package_rows},
    }


def load_metadata() -> dict[str, object]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)

    plan = build_plan(publishable_packages(load_metadata()))
    if args.github_output:
        with args.github_output.open("a") as output:
            for name in ("matrix", "packages"):
                value = json.dumps(plan[name], separators=(",", ":"))
                output.write(f"{name}={value}\n")
    else:
        print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
