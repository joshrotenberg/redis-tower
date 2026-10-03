#!/usr/bin/env python3
"""Check weekly comparison completeness/accounting, not publication sealing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

import render_results


REPLICA_CLIENTS = ("redis-tower-mux", "redis-tower-mux-replica")
CONFIG_FIELDS = {
    "schema_version", "package", "runs", "measurement_secs", "warmup_secs",
    "payload_sizes", "concurrencies", "pipeline_concurrencies", "pipeline_commands",
    "clients", "workloads", "replica_clients", "replica_workloads",
}


def policies(package: str) -> dict:
    """Fixed policies of this source checkout, not inferred from surviving rows."""
    if package == "standalone-bench":
        return dict(clients=list(render_results.STANDALONE_CLIENTS),
                    workloads=["Set", "Get", "Pipeline"], replica_clients=[], replica_workloads=[])
    if package == "cluster-bench":
        return dict(clients=list(render_results.CLUSTER_CLIENTS), workloads=["Set", "Get"],
                    replica_clients=list(REPLICA_CLIENTS), replica_workloads=["Get"])
    raise render_results.ResultError(f"unsupported weekly package {package!r}")


def validate_configuration(config: object, package: str) -> dict:
    """Reject unsupported schema, ambiguous types, invalid knobs or policy drift."""
    if type(config) is not dict or set(config) != CONFIG_FIELDS:
        raise render_results.ResultError("configuration has missing or unexpected fields")
    if type(config["schema_version"]) is not int or config["schema_version"] != 1:
        raise render_results.ResultError("configuration schema_version must be integer 1")
    if config["package"] != package:
        raise render_results.ResultError("configuration package does not match requested package")
    for field in ("runs", "measurement_secs", "warmup_secs", "pipeline_commands"):
        value = config[field]
        if type(value) is not int or value < (0 if field == "warmup_secs" else 1):
            raise render_results.ResultError(f"configuration has invalid {field}")
    for field in ("payload_sizes", "concurrencies", "pipeline_concurrencies"):
        values = config[field]
        if (type(values) is not list or not values
                or any(type(value) is not int or value <= 0 for value in values)
                or len(set(values)) != len(values)):
            raise render_results.ResultError(f"configuration has invalid {field}")
    for field, expected in policies(package).items():
        if config[field] != expected:
            raise render_results.ResultError(f"configuration {field} differs from source policies")
    return config


def load_configuration(result_dir: Path, package: str) -> dict:
    path = result_dir / "configuration.json"

    def unique_fields(pairs):
        fields = {}
        for key, value in pairs:
            if key in fields:
                raise render_results.ResultError("configuration contains a duplicate field")
            fields[key] = value
        return fields

    try:
        config = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_fields)
    except (OSError, ValueError) as error:
        raise render_results.ResultError(f"cannot read configuration {path}: {error}") from error
    return validate_configuration(config, package)


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
    parsed = nonnegative_integer(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("expected a positive decimal integer")
    return parsed


def nonnegative_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a positive decimal integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("expected a nonnegative decimal integer")
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--record-config", action="store_true",
                      help="write expected inputs before measurement; do not validate results")
    mode.add_argument("--recorded-config", action="store_true",
                      help="validate results using retained configuration.json")
    parser.add_argument("--payload-sizes", type=integer_csv)
    parser.add_argument("--concurrency", type=integer_csv)
    parser.add_argument("--runs", type=positive_integer)
    parser.add_argument("--secs", type=positive_integer)
    parser.add_argument("--warmup", type=nonnegative_integer)
    parser.add_argument("--pipeline-concurrency", type=integer_csv)
    parser.add_argument("--pipeline-commands", type=positive_integer)
    args = parser.parse_args()
    knobs = (args.payload_sizes, args.concurrency, args.runs, args.secs,
             args.pipeline_concurrency, args.pipeline_commands, args.warmup)
    if args.recorded_config:
        if any(value is not None for value in knobs):
            parser.error("--recorded-config cannot be combined with expected-input flags")
    elif any(value is None for value in knobs[:4]):
        parser.error("explicit inputs require --payload-sizes, --concurrency, --runs and --secs")
    elif args.record_config and args.warmup is None:
        parser.error("--record-config requires --warmup")
    elif not args.record_config and args.warmup is not None:
        parser.error("--warmup applies only to --record-config")
    try:
        if args.recorded_config:
            config = load_configuration(args.result_dir, args.package)
            args.payload_sizes = config["payload_sizes"]
            args.concurrency = config["concurrencies"]
            args.runs = config["runs"]
            args.secs = config["measurement_secs"]
            args.pipeline_concurrency = config["pipeline_concurrencies"]
            args.pipeline_commands = config["pipeline_commands"]
        else:
            args.pipeline_concurrency = args.pipeline_concurrency or (1,)
            args.pipeline_commands = args.pipeline_commands or 100
        if args.record_config:
            config = dict(schema_version=1, package=args.package, runs=args.runs,
                          measurement_secs=args.secs, warmup_secs=args.warmup,
                          payload_sizes=list(args.payload_sizes), concurrencies=list(args.concurrency),
                          pipeline_concurrencies=list(args.pipeline_concurrency),
                          pipeline_commands=args.pipeline_commands, **policies(args.package))
            validate_configuration(config, args.package)
            path = args.result_dir / "configuration.json"
            with path.open("x", encoding="utf-8") as output:
                output.write(json.dumps(config, indent=2, sort_keys=True) + "\n")
            print(f"recorded {args.package} expected configuration")
            return 0
        cells = validate_weekly(
            args.result_dir, args.package, payloads=args.payload_sizes,
            concurrencies=args.concurrency, runs=args.runs, measurement_secs=args.secs,
            pipeline_concurrencies=args.pipeline_concurrency,
            pipeline_commands=args.pipeline_commands,
        )
    except (render_results.ResultError, OSError) as error:
        print(f"weekly validation failed: {error}", file=sys.stderr)
        return 1
    print(f"validated {args.package}: {cells} cells, {cells * args.runs} retained samples")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
