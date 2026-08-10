#!/usr/bin/env python3
"""Validate publication benchmark matrices and render deterministic SVGs."""

from __future__ import annotations

import argparse
import html
import json
import math
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any


PAYLOADS = (16, 64, 1024, 16 * 1024, 100 * 1024)
CONCURRENCIES = (1, 8, 32, 128)
STANDALONE_CLIENTS = (
    "redis-tower",
    "redis-tower-mux",
    "redis-rs-sync",
    "redis-rs-async",
    "redis-rs-manager",
    "fred",
)
CLUSTER_CLIENTS = (
    "redis-tower",
    "redis-tower-mux",
    "redis-rs-sync",
    "redis-rs-async",
    "fred",
)
COLORS = (
    "#7b2cbf",
    "#e63946",
    "#457b9d",
    "#1d3557",
    "#2a9d8f",
    "#f4a261",
)


class ResultError(ValueError):
    """A benchmark artifact is incomplete or internally inconsistent."""


def load_records(path: Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ResultError(f"cannot read {path}: {error}") from error
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise ResultError(f"{path} must contain one JSON array of objects")
    return value


def _cell_key(row: dict[str, Any]) -> tuple[str, str, int, int]:
    try:
        return (
            str(row["client_id"]),
            str(row["workload"]),
            int(row["payload_bytes"]),
            int(row["concurrency"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResultError(f"invalid matrix identity fields: {row!r}") from error


def validate_matrix(
    records: Sequence[dict[str, Any]],
    *,
    name: str,
    clients: Sequence[str],
    workloads: Sequence[str],
    payloads: Sequence[int] = PAYLOADS,
    concurrencies: Sequence[int] = CONCURRENCIES,
    runs: int = 3,
    commands_per_batch: int | None = None,
) -> None:
    expected = {
        (client, workload, payload, concurrency)
        for client in clients
        for workload in workloads
        for payload in payloads
        for concurrency in concurrencies
    }
    actual: set[tuple[str, str, int, int]] = set()
    for row in records:
        key = _cell_key(row)
        if key in actual:
            raise ResultError(f"{name} contains duplicate cell {key!r}")
        actual.add(key)
        if row.get("schema_version") != 2:
            raise ResultError(f"{name} cell {key!r} is not schema version 2")
        if row.get("runs") != runs:
            raise ResultError(
                f"{name} cell {key!r} has runs={row.get('runs')!r}, expected {runs}"
            )
        if row.get("errors") != 0:
            raise ResultError(f"{name} cell {key!r} reports errors={row.get('errors')!r}")
        if not isinstance(row.get("total_commands"), int) or row["total_commands"] <= 0:
            raise ResultError(f"{name} cell {key!r} has no successful commands")
        for metric in ("commands_per_sec_mean", "commands_per_sec_stddev", "p99_us"):
            value = row.get(metric)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ResultError(f"{name} cell {key!r} has invalid {metric}={value!r}")
        if commands_per_batch is not None and row.get("commands_per_batch") != commands_per_batch:
            raise ResultError(
                f"{name} cell {key!r} has commands_per_batch="
                f"{row.get('commands_per_batch')!r}, expected {commands_per_batch}"
            )

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise ResultError(
            f"{name} matrix mismatch: {len(missing)} missing, {len(extra)} extra; "
            f"first missing={missing[:3]!r}, first extra={extra[:3]!r}"
        )


def _headline(
    records: Iterable[dict[str, Any]], workload: str = "Get", payload: int = 1024
) -> list[dict[str, Any]]:
    return sorted(
        (
            row
            for row in records
            if row["workload"] == workload and row["payload_bytes"] == payload
        ),
        key=lambda row: (row["client_id"], row["concurrency"]),
    )


def _fmt_metric(value: float, metric: str) -> str:
    if metric == "commands_per_sec_mean":
        if value >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if value >= 1_000:
            return f"{value / 1_000:.0f}k"
        return f"{value:.0f}"
    if value >= 1_000:
        return f"{value / 1_000:.1f}ms"
    return f"{value:.0f}us"


def render_svg(
    standalone: Sequence[dict[str, Any]],
    cluster: Sequence[dict[str, Any]],
    *,
    metric: str,
    title: str,
    output: Path,
) -> None:
    width, height = 1280, 620
    margin_left, margin_right = 82, 34
    plot_top, plot_bottom = 92, 505
    gap = 82
    panel_width = (width - margin_left - margin_right - gap) / 2
    panels = (
        ("Standalone — GET, 1 KiB", _headline(standalone)),
        ("Cluster — GET, 1 KiB", _headline(cluster)),
    )
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text{font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;fill:#17212b}",
        ".title{font-size:24px;font-weight:700}.panel{font-size:17px;font-weight:650}",
        ".axis{font-size:12px;fill:#53606d}.legend{font-size:12px}.grid{stroke:#dfe5eb;stroke-width:1}",
        ".frame{fill:#fff;stroke:#aab4bf;stroke-width:1}.line{fill:none;stroke-width:2.2}",
        "</style>",
        '<rect width="100%" height="100%" fill="#f8fafc"/>',
        f'<text class="title" x="{width / 2}" y="38" text-anchor="middle">{html.escape(title)}</text>',
        '<text class="axis" x="640" y="600" text-anchor="middle">Concurrency</text>',
    ]

    for panel_index, (panel_title, rows) in enumerate(panels):
        x0 = margin_left + panel_index * (panel_width + gap)
        x1 = x0 + panel_width
        values = [float(row[metric]) for row in rows]
        maximum = max(values, default=1.0)
        y_max = maximum * 1.08 if maximum > 0 else 1.0
        parts.append(
            f'<text class="panel" x="{(x0 + x1) / 2:.1f}" y="72" '
            f'text-anchor="middle">{html.escape(panel_title)}</text>'
        )
        parts.append(
            f'<rect class="frame" x="{x0:.1f}" y="{plot_top}" '
            f'width="{panel_width:.1f}" height="{plot_bottom - plot_top}"/>'
        )
        for tick in range(6):
            fraction = tick / 5
            y = plot_bottom - fraction * (plot_bottom - plot_top)
            value = fraction * y_max
            parts.append(
                f'<line class="grid" x1="{x0:.1f}" y1="{y:.1f}" x2="{x1:.1f}" y2="{y:.1f}"/>'
            )
            parts.append(
                f'<text class="axis" x="{x0 - 8:.1f}" y="{y + 4:.1f}" '
                f'text-anchor="end">{html.escape(_fmt_metric(value, metric))}</text>'
            )

        x_positions: dict[int, float] = {}
        for index, concurrency in enumerate(CONCURRENCIES):
            x = x0 + index * panel_width / (len(CONCURRENCIES) - 1)
            x_positions[concurrency] = x
            parts.append(
                f'<text class="axis" x="{x:.1f}" y="{plot_bottom + 20}" '
                f'text-anchor="middle">{concurrency}</text>'
            )

        by_client: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_client.setdefault(str(row["client_id"]), []).append(row)
        for color_index, client in enumerate(sorted(by_client)):
            color = COLORS[color_index % len(COLORS)]
            points = []
            for row in sorted(by_client[client], key=lambda item: item["concurrency"]):
                x = x_positions[int(row["concurrency"])]
                y = plot_bottom - float(row[metric]) / y_max * (plot_bottom - plot_top)
                points.append((x, y))
            point_text = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
            parts.append(
                f'<polyline class="line" stroke="{color}" points="{point_text}"/>'
            )
            parts.extend(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{color}"/>'
                for x, y in points
            )

        legend_y = 540
        legend_clients = sorted(by_client)
        for index, client in enumerate(legend_clients):
            color = COLORS[index % len(COLORS)]
            column = index % 3
            row_index = index // 3
            x = x0 + column * panel_width / 3
            y = legend_y + row_index * 20
            parts.append(f'<line x1="{x:.1f}" y1="{y}" x2="{x + 18:.1f}" y2="{y}" stroke="{color}" stroke-width="3"/>')
            parts.append(
                f'<text class="legend" x="{x + 23:.1f}" y="{y + 4}">{html.escape(client)}</text>'
            )

    parts.append("</svg>")
    output.write_text("\n".join(parts) + "\n", encoding="utf-8")


def parse_pipeline(value: str) -> tuple[int, Path]:
    depth, separator, raw_path = value.partition("=")
    if not separator:
        raise argparse.ArgumentTypeError("pipeline must use DEPTH=PATH")
    try:
        parsed_depth = int(depth)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid pipeline depth {depth!r}") from error
    if parsed_depth not in (10, 100, 1000):
        raise argparse.ArgumentTypeError("pipeline depth must be 10, 100, or 1000")
    return parsed_depth, Path(raw_path)


def validate_soak(path: Path) -> dict[str, Any]:
    try:
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    except (OSError, json.JSONDecodeError) as error:
        raise ResultError(f"cannot read soak artifact {path}: {error}") from error
    if len(records) != 242 or not all(isinstance(record, dict) for record in records):
        raise ResultError(
            f"four-hour soak must contain metadata, 240 intervals, and summary; got {len(records)} records"
        )
    metadata, *intervals, summary = records
    expected_metadata = {
        "schema_version": 1,
        "record_type": "metadata",
        "mode": "standalone",
        "duration_secs": 14_400.0,
        "report_interval_secs": 60.0,
        "chaos": "standalone_sigkill",
        "chaos_after_secs": 7_200.0,
        "reconnect_accounting": "exact_connection_event_reconnected",
    }
    for field, expected in expected_metadata.items():
        if metadata.get(field) != expected:
            raise ResultError(
                f"soak metadata {field}={metadata.get(field)!r}, expected {expected!r}"
            )

    total_successes = 0
    total_errors = 0
    previous_elapsed = 0.0
    for index, record in enumerate(intervals, start=1):
        if record.get("schema_version") != 1 or record.get("record_type") != "interval":
            raise ResultError(f"soak record {index} is not a schema-1 interval")
        if record.get("interval") != index:
            raise ResultError(
                f"soak interval sequence is discontinuous at {record.get('interval')!r}"
            )
        successes = record.get("successes")
        errors = record.get("errors")
        attempts = record.get("attempts")
        if not all(isinstance(value, int) and value >= 0 for value in (successes, errors, attempts)):
            raise ResultError(f"soak interval {index} has invalid operation counters")
        if attempts != successes + errors:
            raise ResultError(f"soak interval {index} violates attempts=successes+errors")
        elapsed = record.get("elapsed_secs")
        if not isinstance(elapsed, (int, float)) or elapsed <= previous_elapsed:
            raise ResultError(f"soak interval {index} has non-monotonic elapsed time")
        previous_elapsed = float(elapsed)
        total_successes += successes
        total_errors += errors

    if summary.get("schema_version") != 1 or summary.get("record_type") != "summary":
        raise ResultError("soak artifact does not end with a schema-1 summary")
    if summary.get("successes") != total_successes or summary.get("errors") != total_errors:
        raise ResultError("soak summary counters do not equal the interval totals")
    if summary.get("attempts") != total_successes + total_errors:
        raise ResultError("soak summary violates attempts=successes+errors")
    elapsed = summary.get("elapsed_secs")
    if not isinstance(elapsed, (int, float)) or not 14_399.0 <= elapsed <= 14_430.0:
        raise ResultError(f"soak summary elapsed_secs={elapsed!r} is not a four-hour run")
    if not isinstance(summary.get("successes"), int) or summary["successes"] <= 0:
        raise ResultError("soak summary has no successful operations")
    if summary.get("reconnects_total") != 1:
        raise ResultError(
            f"soak expected exactly one reconnect, got {summary.get('reconnects_total')!r}"
        )
    if summary.get("recoveries_total") != 1 or summary.get("chaos_injections_total") != 1:
        raise ResultError("soak did not record exactly one chaos injection and recovery")
    for metric in ("p50_us", "p99_us", "p999_us", "max_us", "rss_bytes"):
        if not isinstance(summary.get(metric), int) or summary[metric] <= 0:
            raise ResultError(f"soak summary has invalid {metric}={summary.get(metric)!r}")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--standalone", required=True, type=Path)
    parser.add_argument("--cluster", required=True, type=Path)
    parser.add_argument("--pipeline", action="append", required=True, type=parse_pipeline)
    parser.add_argument("--soak", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    standalone = load_records(args.standalone)
    cluster = load_records(args.cluster)
    validate_matrix(
        standalone,
        name="standalone",
        clients=STANDALONE_CLIENTS,
        workloads=("Set", "Get"),
    )
    validate_matrix(
        cluster,
        name="cluster",
        clients=CLUSTER_CLIENTS,
        workloads=("Set", "Get"),
    )

    pipelines: dict[int, list[dict[str, Any]]] = {}
    for depth, path in args.pipeline:
        if depth in pipelines:
            raise ResultError(f"duplicate pipeline depth {depth}")
        records = load_records(path)
        validate_matrix(
            records,
            name=f"pipeline-{depth}",
            clients=STANDALONE_CLIENTS,
            workloads=("Pipeline",),
            concurrencies=(1,),
            commands_per_batch=depth,
        )
        pipelines[depth] = records
    if set(pipelines) != {10, 100, 1000}:
        raise ResultError("pipeline artifacts must cover depths 10, 100, and 1000")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    render_svg(
        standalone,
        cluster,
        metric="commands_per_sec_mean",
        title="Redis client throughput vs concurrency",
        output=args.output_dir / "throughput-vs-concurrency.svg",
    )
    render_svg(
        standalone,
        cluster,
        metric="p99_us",
        title="Redis client p99 latency vs concurrency",
        output=args.output_dir / "p99-vs-concurrency.svg",
    )
    summary: dict[str, Any] = {
        "schema_version": 1,
        "chart_selection": {"workload": "Get", "payload_bytes": 1024},
        "standalone": _headline(standalone),
        "cluster": _headline(cluster),
        "pipeline": {str(depth): records for depth, records in sorted(pipelines.items())},
    }
    if args.soak is not None:
        summary["soak"] = validate_soak(args.soak)
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ResultError as error:
        raise SystemExit(f"benchmark result validation failed: {error}") from error
