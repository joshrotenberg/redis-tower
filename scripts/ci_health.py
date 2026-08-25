#!/usr/bin/env python3
"""Report GitHub Actions wall-clock and rerun signals over a bounded window."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile without values")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def summarize(
    runs: list[dict[str, object]],
    *,
    window: int | None = None,
    max_p95_minutes: float,
    max_rerun_rate: float,
) -> dict[str, object]:
    completed = [
        run
        for run in runs
        if run.get("status") == "completed"
        and run.get("conclusion") not in {"cancelled", "skipped", None}
    ]
    if window is not None:
        completed = completed[:window]
    if not completed:
        raise ValueError("no completed workflow runs were available")

    durations = []
    reruns = 0
    failures = 0
    for run in completed:
        started = run.get("run_started_at")
        updated = run.get("updated_at")
        if not isinstance(started, str) or not isinstance(updated, str):
            raise ValueError("completed workflow run is missing timestamps")
        duration = (timestamp(updated) - timestamp(started)).total_seconds()
        if duration < 0:
            raise ValueError("workflow run has a negative duration")
        durations.append(duration)
        attempt = run.get("run_attempt", 1)
        if not isinstance(attempt, int) or attempt < 1:
            raise ValueError("workflow run has an invalid run_attempt")
        reruns += int(attempt > 1)
        failures += int(run.get("conclusion") != "success")

    p50 = percentile(durations, 0.50) / 60
    p95 = percentile(durations, 0.95) / 60
    rerun_rate = reruns / len(completed)
    failure_rate = failures / len(completed)
    return {
        "schema_version": 1,
        "window": len(completed),
        "wall_clock_minutes": {
            "p50": p50,
            "p95": p95,
            "maximum": max(durations) / 60,
        },
        "rerun_signal": {"count": reruns, "rate": rerun_rate},
        "failure_signal": {"count": failures, "rate": failure_rate},
        "budgets": {
            "max_p95_minutes": max_p95_minutes,
            "max_rerun_rate": max_rerun_rate,
            "p95_within_budget": p95 <= max_p95_minutes,
            "rerun_rate_within_budget": rerun_rate <= max_rerun_rate,
        },
    }


def markdown(payload: dict[str, object]) -> str:
    wall = payload["wall_clock_minutes"]
    rerun = payload["rerun_signal"]
    failure = payload["failure_signal"]
    budgets = payload["budgets"]
    assert isinstance(wall, dict)
    assert isinstance(rerun, dict)
    assert isinstance(failure, dict)
    assert isinstance(budgets, dict)
    p95_status = "within" if budgets["p95_within_budget"] else "over"
    rerun_status = "within" if budgets["rerun_rate_within_budget"] else "over"
    return (
        "# CI health\n\n"
        f"Window: {payload['window']} completed, non-cancelled runs.\n\n"
        "| Signal | Observed | Budget | Status |\n"
        "|---|---:|---:|---|\n"
        f"| Wall clock p50 | {wall['p50']:.2f} min | — | tracking |\n"
        f"| Wall clock p95 | {wall['p95']:.2f} min | "
        f"{budgets['max_p95_minutes']:.2f} min | {p95_status} |\n"
        f"| Rerun/flake signal | {rerun['rate']:.1%} ({rerun['count']}) | "
        f"{budgets['max_rerun_rate']:.1%} | {rerun_status} |\n"
        f"| Failure signal | {failure['rate']:.1%} ({failure['count']}) | — | tracking |\n"
        f"| Maximum wall clock | {wall['maximum']:.2f} min | — | tracking |\n"
    )


def fetch_runs(repository: str, workflow: str, token: str) -> list[dict[str, object]]:
    url = (
        f"https://api.github.com/repos/{repository}/actions/workflows/"
        f"{workflow}/runs?per_page=100&status=completed"
    )
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    runs = payload.get("workflow_runs")
    if not isinstance(runs, list):
        raise ValueError("GitHub response did not contain workflow_runs")
    return runs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path)
    source.add_argument("--repository")
    parser.add_argument("--workflow", default="ci.yml")
    parser.add_argument("--window", type=int, default=50)
    parser.add_argument("--max-p95-minutes", type=float, default=15.0)
    parser.add_argument("--max-rerun-rate", type=float, default=0.10)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--enforce", action="store_true")
    args = parser.parse_args(argv)
    if args.window < 1 or args.window > 100:
        parser.error("--window must be between 1 and 100")
    if args.max_p95_minutes <= 0:
        parser.error("--max-p95-minutes must be positive")
    if not 0.0 <= args.max_rerun_rate <= 1.0:
        parser.error("--max-rerun-rate must be between 0 and 1")

    try:
        if args.input:
            raw = json.loads(args.input.read_text())
            runs = raw["workflow_runs"] if isinstance(raw, dict) else raw
        else:
            token = os.environ.get("GITHUB_TOKEN")
            if not token:
                raise ValueError("GITHUB_TOKEN is required when using --repository")
            runs = fetch_runs(args.repository, args.workflow, token)
        if not isinstance(runs, list):
            raise ValueError("workflow runs input must be a list")
        payload = summarize(
            runs,
            window=args.window,
            max_p95_minutes=args.max_p95_minutes,
            max_rerun_rate=args.max_rerun_rate,
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        return 2

    rendered = markdown(payload)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(rendered)
    print(rendered, end="")

    budgets = payload["budgets"]
    assert isinstance(budgets, dict)
    if args.enforce and not (
        budgets["p95_within_budget"] and budgets["rerun_rate_within_budget"]
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
