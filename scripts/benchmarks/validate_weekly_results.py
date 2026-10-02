#!/usr/bin/env python3
"""Check weekly comparison completeness/accounting, not publication sealing."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Sequence

import render_results


REPLICA_CLIENTS = ("redis-tower-mux", "redis-tower-mux-replica")


def validate_weekly(
    result_dir: Path,
    package: str,
    *,
    payloads: Sequence[int],
    concurrencies: Sequence[int],
    runs: int,
    measurement_secs: float,
    pipeline_concurrencies: Sequence[int] = (1,),
    pipeline_commands: int = 100,
) -> int:
    """Return total validated cells; raise ResultError on any invalid matrix."""
    records = render_results.load_records(result_dir / f"{package}.json")
    common = dict(payloads=payloads, runs=runs, measurement_secs=measurement_secs,
                  require_samples=True)
    if package == "standalone-bench":
        if any(row.get("workload") not in ("Set", "Get", "Pipeline") for row in records):
            raise render_results.ResultError("standalone-bench has an unexpected workload")
        render_results.validate_matrix(
            [row for row in records if row["workload"] != "Pipeline"],
            name="weekly standalone GET/SET", clients=render_results.STANDALONE_CLIENTS,
            workloads=("Set", "Get"), concurrencies=concurrencies, **common,
        )
        render_results.validate_matrix(
            [row for row in records if row["workload"] == "Pipeline"],
            name="weekly standalone pipeline", clients=render_results.STANDALONE_CLIENTS,
            workloads=("Pipeline",), concurrencies=pipeline_concurrencies,
            commands_per_batch=pipeline_commands, **common,
        )
        return len(records)
    if package != "cluster-bench":
        raise render_results.ResultError(f"unsupported weekly package {package!r}")
    render_results.validate_matrix(
        records, name="weekly Cluster GET/SET", clients=render_results.CLUSTER_CLIENTS,
        workloads=("Set", "Get"), concurrencies=concurrencies, **common,
    )
    replicas = render_results.load_records(result_dir / "cluster-bench-replica.json")
    render_results.validate_matrix(
        replicas, name="weekly Cluster replica GET", clients=REPLICA_CLIENTS,
        workloads=("Get",), concurrencies=concurrencies, **common,
    )
    return len(records) + len(replicas)


def positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a positive decimal integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive decimal integer")
    return parsed


def integer_csv(value: str) -> tuple[int, ...]:
    parsed = tuple(positive_integer(part) for part in value.split(","))
    if len(set(parsed)) != len(parsed):
        raise argparse.ArgumentTypeError("expected distinct positive decimal integers")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--package", choices=("standalone-bench", "cluster-bench"), required=True)
    parser.add_argument("--payload-sizes", type=integer_csv, required=True)
    parser.add_argument("--concurrency", type=integer_csv, required=True)
    parser.add_argument("--runs", type=positive_integer, required=True)
    parser.add_argument("--secs", type=positive_integer, required=True)
    parser.add_argument("--pipeline-concurrency", type=integer_csv, default=(1,))
    parser.add_argument("--pipeline-commands", type=positive_integer, default=100)
    args = parser.parse_args()
    try:
        cells = validate_weekly(
            args.result_dir, args.package, payloads=args.payload_sizes,
            concurrencies=args.concurrency, runs=args.runs, measurement_secs=args.secs,
            pipeline_concurrencies=args.pipeline_concurrency,
            pipeline_commands=args.pipeline_commands,
        )
    except render_results.ResultError as error:
        print(f"weekly validation failed: {error}", file=sys.stderr)
        return 1
    print(f"validated {args.package}: {cells} cells, {cells * args.runs} retained samples")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
