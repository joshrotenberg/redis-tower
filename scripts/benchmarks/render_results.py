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
PIPELINE_DEPTHS = (10, 100, 1000)
PUBLICATION_MEASUREMENT_SECS = 10.0
SOAK_RSS_TAIL_WINDOW_INTERVALS = 10
SOAK_RSS_MAX_GROWTH_BYTES = 16 * 1024 * 1024
SOAK_RSS_MAX_GROWTH_FRACTION = 0.5
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
CLIENT_VARIANTS = {
    "redis-tower": "RedisTower",
    "redis-tower-mux": "RedisTowerMux",
    "redis-rs-sync": "RedisRsSync",
    "redis-rs-async": "RedisRsAsync",
    "redis-rs-manager": "RedisRsManager",
    "fred": "Fred",
}
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
    measurement_secs: float | None = None,
    require_samples: bool = False,
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
        if (
            type(row.get("client_id")) is not str
            or type(row.get("workload")) is not str
            or type(row.get("payload_bytes")) is not int
            or type(row.get("concurrency")) is not int
        ):
            raise ResultError(f"{name} has non-canonical matrix identity types: {row!r}")
        if key in actual:
            raise ResultError(f"{name} contains duplicate cell {key!r}")
        actual.add(key)
        if row.get("schema_version") != 2:
            raise ResultError(f"{name} cell {key!r} is not schema version 2")
        expected_client = CLIENT_VARIANTS.get(row["client_id"], row["client_id"])
        if row.get("client") != expected_client:
            raise ResultError(
                f"{name} cell {key!r} has client={row.get('client')!r}, "
                f"expected {expected_client!r}"
            )
        if row.get("runs") != runs:
            raise ResultError(
                f"{name} cell {key!r} has runs={row.get('runs')!r}, expected {runs}"
            )
        if row.get("errors") != 0:
            raise ResultError(f"{name} cell {key!r} reports errors={row.get('errors')!r}")
        if type(row.get("total_commands")) is not int or row["total_commands"] <= 0:
            raise ResultError(f"{name} cell {key!r} has no successful commands")
        expected_commands_per_batch = commands_per_batch or 1
        if row.get("commands_per_batch") != expected_commands_per_batch:
            raise ResultError(
                f"{name} cell {key!r} has commands_per_batch="
                f"{row.get('commands_per_batch')!r}, expected {expected_commands_per_batch}"
            )
        expected_latency_unit = "batch" if row["workload"] == "Pipeline" else "command"
        if row.get("latency_unit") != expected_latency_unit:
            raise ResultError(
                f"{name} cell {key!r} has latency_unit={row.get('latency_unit')!r}, "
                f"expected {expected_latency_unit!r}"
            )
        total_batches = row.get("total_batches")
        if type(total_batches) is not int or total_batches <= 0:
            raise ResultError(f"{name} cell {key!r} has no successful batches")
        if row["total_commands"] != total_batches * expected_commands_per_batch:
            raise ResultError(f"{name} cell {key!r} batch/command totals disagree")
        if row.get("total_ops") != total_batches:
            raise ResultError(f"{name} cell {key!r} legacy total_ops alias disagrees")
        for metric in ("batches_per_sec_mean", "batches_per_sec_stddev"):
            value = row.get(metric)
            if not _finite_number(value) or float(value) < 0:
                raise ResultError(f"{name} cell {key!r} has invalid {metric}={value!r}")
        if not _close(
            row.get("commands_per_sec_mean"),
            float(row["batches_per_sec_mean"]) * expected_commands_per_batch,
        ) or not _close(
            row.get("commands_per_sec_stddev"),
            float(row["batches_per_sec_stddev"]) * expected_commands_per_batch,
        ):
            raise ResultError(f"{name} cell {key!r} aggregate batch/command rates disagree")
        if not _close(row.get("ops_per_sec_mean"), float(row["batches_per_sec_mean"])) or not _close(
            row.get("ops_per_sec_stddev"), float(row["batches_per_sec_stddev"])
        ):
            raise ResultError(f"{name} cell {key!r} legacy ops/s aliases disagree")
        for metric in (
            "commands_per_sec_mean",
            "commands_per_sec_stddev",
            "p50_us",
            "p90_us",
            "p99_us",
            "p999_us",
            "max_us",
        ):
            value = row.get(metric)
            if not _finite_number(value) or float(value) < 0:
                raise ResultError(f"{name} cell {key!r} has invalid {metric}={value!r}")
        if not (
            float(row["p50_us"])
            <= float(row["p90_us"])
            <= float(row["p99_us"])
            <= float(row["p999_us"])
            <= float(row["max_us"])
        ):
            raise ResultError(f"{name} cell {key!r} has unordered aggregate quantiles")
        if require_samples:
            validate_samples(
                row,
                name=name,
                key=key,
                runs=runs,
                commands_per_batch=expected_commands_per_batch,
                measurement_secs=measurement_secs,
            )

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise ResultError(
            f"{name} matrix mismatch: {len(missing)} missing, {len(extra)} extra; "
            f"first missing={missing[:3]!r}, first extra={extra[:3]!r}"
        )


def _finite_number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(float(value))


def _population_stddev(values: Sequence[float]) -> float:
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _close(actual: Any, expected: float, *, absolute: float = 1e-6) -> bool:
    return _finite_number(actual) and math.isclose(
        float(actual), expected, rel_tol=1e-9, abs_tol=absolute
    )


def validate_samples(
    row: dict[str, Any],
    *,
    name: str,
    key: tuple[str, str, int, int],
    runs: int,
    commands_per_batch: int,
    measurement_secs: float | None,
) -> None:
    samples = row.get("samples")
    if not isinstance(samples, list) or len(samples) != runs:
        raise ResultError(
            f"{name} cell {key!r} must retain exactly {runs} raw run samples"
        )
    if not all(isinstance(sample, dict) for sample in samples):
        raise ResultError(f"{name} cell {key!r} has a non-object raw sample")
    if [sample.get("run") for sample in samples] != list(range(1, runs + 1)):
        raise ResultError(f"{name} cell {key!r} has invalid raw sample numbering")

    rates: list[float] = []
    latency_samples: dict[str, list[float]] = {
        metric: [] for metric in ("p50_us", "p90_us", "p99_us", "p999_us", "max_us")
    }
    total_commands = 0
    total_batches = 0
    total_errors = 0
    batch_rates: list[float] = []
    for sample in samples:
        batches = sample.get("total_batches")
        commands = sample.get("total_commands")
        errors = sample.get("errors")
        elapsed = sample.get("elapsed_secs")
        batch_rate = sample.get("batches_per_sec")
        rate = sample.get("commands_per_sec")
        if type(batches) is not int or batches <= 0:
            raise ResultError(f"{name} cell {key!r} has invalid raw batch count")
        if type(commands) is not int or commands <= 0:
            raise ResultError(f"{name} cell {key!r} has invalid raw command count")
        if type(errors) is not int or errors != 0:
            raise ResultError(f"{name} cell {key!r} has errors in a raw sample")
        if not _finite_number(rate) or float(rate) <= 0:
            raise ResultError(f"{name} cell {key!r} has invalid raw command rate")
        if not _finite_number(batch_rate) or float(batch_rate) <= 0:
            raise ResultError(f"{name} cell {key!r} has invalid raw batch rate")
        if not _finite_number(elapsed) or float(elapsed) <= 0:
            raise ResultError(f"{name} cell {key!r} has invalid raw elapsed time")
        if measurement_secs is not None:
            lower = measurement_secs * 0.95
            upper = measurement_secs + max(5.0, measurement_secs * 0.5)
            if not lower <= float(elapsed) <= upper:
                raise ResultError(
                    f"{name} cell {key!r} raw elapsed time {elapsed!r} is outside "
                    f"the expected {lower:.3f}..{upper:.3f}s measurement window"
                )
        if commands != batches * commands_per_batch:
            raise ResultError(f"{name} cell {key!r} raw batch/command counts disagree")
        if not _close(float(batch_rate), batches / float(elapsed)) or not _close(
            float(rate), commands / float(elapsed)
        ):
            raise ResultError(f"{name} cell {key!r} raw rates do not match counts/time")
        if not _close(float(rate), float(batch_rate) * commands_per_batch):
            raise ResultError(f"{name} cell {key!r} raw batch/command rates disagree")
        for metric in ("p50_us", "p90_us", "p99_us", "p999_us", "max_us"):
            if not _finite_number(sample.get(metric)) or float(sample[metric]) < 0:
                raise ResultError(
                    f"{name} cell {key!r} has invalid raw {metric}={sample.get(metric)!r}"
                )
        if not (
            float(sample["p50_us"])
            <= float(sample["p90_us"])
            <= float(sample["p99_us"])
            <= float(sample["p999_us"])
            <= float(sample["max_us"])
        ):
            raise ResultError(f"{name} cell {key!r} has unordered raw quantiles")
        total_batches += batches
        total_commands += commands
        total_errors += errors
        batch_rates.append(float(batch_rate))
        rates.append(float(rate))
        for metric in latency_samples:
            latency_samples[metric].append(float(sample[metric]))

    expected_mean = sum(rates) / len(rates)
    expected_stddev = _population_stddev(rates)
    if row.get("total_commands") != total_commands or row.get("errors") != total_errors:
        raise ResultError(f"{name} cell {key!r} aggregate counters do not match samples")
    if row.get("total_batches") != total_batches:
        raise ResultError(f"{name} cell {key!r} batch total does not match samples")
    if not _close(row.get("commands_per_sec_mean"), expected_mean):
        raise ResultError(f"{name} cell {key!r} mean cannot be recomputed from samples")
    if not _close(row.get("commands_per_sec_stddev"), expected_stddev):
        raise ResultError(f"{name} cell {key!r} stddev cannot be recomputed from samples")
    for metric, values in latency_samples.items():
        expected = max(values) if metric == "max_us" else sum(values) / runs
        if not _close(row.get(metric), expected):
            aggregation = "maximum" if metric == "max_us" else "mean"
            raise ResultError(
                f"{name} cell {key!r} {metric} does not match sample {aggregation}"
            )

    if not _close(row.get("batches_per_sec_mean"), sum(batch_rates) / runs):
        raise ResultError(f"{name} cell {key!r} batch mean does not match samples")
    if not _close(
        row.get("batches_per_sec_stddev"), _population_stddev(batch_rates)
    ):
        raise ResultError(f"{name} cell {key!r} batch stddev does not match samples")


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
    if parsed_depth not in PIPELINE_DEPTHS:
        raise argparse.ArgumentTypeError("pipeline depth must be 10, 100, or 1000")
    return parsed_depth, Path(raw_path)


def _nonnegative_integer(value: Any) -> bool:
    return type(value) is int and value >= 0


def _validate_quantiles(record: dict[str, Any], *, context: str) -> None:
    fields = ("p50_us", "p99_us", "p999_us", "max_us")
    values = [record.get(field) for field in fields]
    if record.get("successes") == 0:
        if any(value is not None for value in values):
            raise ResultError(f"{context} has latency quantiles without successes")
        return
    if not all(type(value) is int and value > 0 for value in values):
        raise ResultError(f"{context} has invalid latency quantiles {values!r}")
    if values != sorted(values):
        raise ResultError(f"{context} has unordered latency quantiles {values!r}")


def _validate_rate(
    record: dict[str, Any], *, numerator: str, rate: str, window: float, context: str
) -> None:
    expected = int(record[numerator]) / window
    if not _close(record.get(rate), expected):
        raise ResultError(
            f"{context} {rate}={record.get(rate)!r} does not match "
            f"{numerator}/{window:.6f}={expected:.6f}"
        )


def validate_soak(path: Path) -> dict[str, Any]:
    try:
        records = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, json.JSONDecodeError) as error:
        raise ResultError(f"cannot read soak artifact {path}: {error}") from error
    if len(records) != 242 or not all(isinstance(record, dict) for record in records):
        raise ResultError(
            "four-hour soak must contain metadata, exactly 240 intervals, "
            f"and summary; got {len(records)} records"
        )
    metadata, *intervals, summary = records
    expected_metadata = {
        "schema_version": 1,
        "record_type": "metadata",
        "mode": "standalone",
        "workload": "get_validate",
        "key": "redis-tower:soak:standalone",
        "payload_bytes": 1024,
        "concurrency": 32,
        "warmup_secs": 60.0,
        "duration_secs": 14_400.0,
        "report_interval_secs": 60.0,
        "operation_timeout_ms": 2_000,
        "error_backoff_ms": 1,
        "startup_timeout_secs": 30.0,
        "recovery_timeout_secs": 30.0,
        "cluster_slot": 42,
        "cluster_node_timeout_ms": 1_000,
        "standalone_port": 6_481,
        "chaos": "standalone_sigkill",
        "chaos_after_secs": 7_200.0,
        "reconnect_accounting": "exact_connection_event_reconnected",
        "recovery_accounting": "exact_connection_event_reconnected",
        "latency_accounting": "successful_get_completions_only",
        "rss_accounting": "current_soak_process_resident_set",
    }
    for field, expected in expected_metadata.items():
        if metadata.get(field) != expected:
            raise ResultError(
                f"soak metadata {field}={metadata.get(field)!r}, expected {expected!r}"
            )
    if not _nonnegative_integer(metadata.get("started_unix_ms")) or metadata["started_unix_ms"] == 0:
        raise ResultError("soak metadata has no valid start timestamp")

    totals = {"successes": 0, "errors": 0, "attempts": 0, "operations": 0}
    lifecycle_fields = ("reconnects", "recoveries", "chaos_injections")
    lifecycle_totals = {field: 0 for field in lifecycle_fields}
    lifecycle_events: dict[str, list[int]] = {field: [] for field in lifecycle_fields}
    error_intervals: list[int] = []
    rss_samples: list[int] = []
    previous_elapsed = 0.0
    for index, record in enumerate(intervals, start=1):
        context = f"soak interval {index}"
        if record.get("schema_version") != 1 or record.get("record_type") != "interval":
            raise ResultError(f"{context} is not a schema-1 interval")
        if record.get("interval") != index:
            raise ResultError(
                f"soak interval sequence is discontinuous at {record.get('interval')!r}"
            )
        counters = tuple(record.get(field) for field in totals)
        if not all(_nonnegative_integer(value) for value in counters):
            raise ResultError(f"{context} has invalid operation counters")
        if record["operations"] != record["successes"]:
            raise ResultError(f"{context} violates operations=successes")
        if record["attempts"] != record["successes"] + record["errors"]:
            raise ResultError(f"{context} violates attempts=successes+errors")
        if record["successes"] == 0:
            raise ResultError(f"{context} has no successful operations")
        if record["errors"] > 0:
            error_intervals.append(index)

        elapsed = record.get("elapsed_secs")
        window = record.get("window_secs")
        if not _finite_number(elapsed) or float(elapsed) <= previous_elapsed:
            raise ResultError(f"{context} has non-monotonic elapsed time")
        if not _finite_number(window) or not 55.0 <= float(window) <= 65.0:
            raise ResultError(f"{context} is not an approximately 60-second window")
        if abs(float(elapsed) - index * 60.0) > 5.0:
            raise ResultError(f"{context} elapsed time is not on the minute cadence")
        if not math.isclose(
            float(window), float(elapsed) - previous_elapsed, rel_tol=0.0, abs_tol=2.0
        ):
            raise ResultError(f"{context} window does not match elapsed-time delta")
        previous_elapsed = float(elapsed)

        _validate_rate(
            record,
            numerator="successes",
            rate="ops_per_sec",
            window=float(window),
            context=context,
        )
        _validate_rate(
            record,
            numerator="attempts",
            rate="attempted_ops_per_sec",
            window=float(window),
            context=context,
        )
        _validate_quantiles(record, context=context)
        if not _nonnegative_integer(record.get("rss_bytes")) or record["rss_bytes"] == 0:
            raise ResultError(f"{context} has no positive RSS sample")
        rss_samples.append(record["rss_bytes"])

        for field in lifecycle_fields:
            delta = record.get(field)
            total = record.get(f"{field}_total")
            if not _nonnegative_integer(delta) or not _nonnegative_integer(total):
                raise ResultError(f"{context} has invalid {field} accounting")
            if total != lifecycle_totals[field] + delta:
                raise ResultError(f"{context} {field} delta does not reconcile with total")
            if total < lifecycle_totals[field]:
                raise ResultError(f"{context} has non-monotonic {field} total")
            if delta > 1:
                raise ResultError(f"{context} records more than one {field} event")
            if delta:
                lifecycle_events[field].append(index)
            lifecycle_totals[field] = total

        for field in totals:
            totals[field] += record[field]

    if len(lifecycle_events["chaos_injections"]) != 1:
        raise ResultError("soak must contain exactly one chaos injection")
    if len(lifecycle_events["reconnects"]) != 1:
        raise ResultError("soak must contain exactly one reconnect")
    if len(lifecycle_events["recoveries"]) != 1:
        raise ResultError("soak must contain exactly one recovery")
    chaos_index = lifecycle_events["chaos_injections"][0]
    if chaos_index not in (120, 121):
        raise ResultError("soak chaos must be recorded in interval 120 or 121")
    if float(intervals[chaos_index - 1]["elapsed_secs"]) < 7_195.0:
        raise ResultError("soak chaos was recorded before the configured injection time")
    reconnect_index = lifecycle_events["reconnects"][0]
    recovery_index = lifecycle_events["recoveries"][0]
    if reconnect_index < chaos_index:
        raise ResultError("soak reconnect was recorded before chaos")
    if recovery_index < chaos_index:
        raise ResultError("soak recovery was recorded before chaos")
    if reconnect_index > chaos_index + 1 or recovery_index > chaos_index + 1:
        raise ResultError(
            "soak reconnect/recovery was recorded too late for the 30-second recovery bound"
        )
    if reconnect_index != recovery_index:
        raise ResultError("soak reconnect and recovery accounting is not paired")
    allowed_error_intervals = set(range(chaos_index, recovery_index + 1))
    unexpected_error_intervals = [
        index for index in error_intervals if index not in allowed_error_intervals
    ]
    if unexpected_error_intervals:
        raise ResultError(
            "soak has errors outside the chaos/recovery window in intervals "
            f"{unexpected_error_intervals[:8]!r}"
        )
    rss_window = SOAK_RSS_TAIL_WINDOW_INTERVALS
    rss_head_mean = sum(rss_samples[:rss_window]) / rss_window
    rss_tail_mean = sum(rss_samples[-rss_window:]) / rss_window
    allowed_rss_growth = max(
        SOAK_RSS_MAX_GROWTH_BYTES,
        rss_head_mean * SOAK_RSS_MAX_GROWTH_FRACTION,
    )
    if rss_tail_mean - rss_head_mean > allowed_rss_growth:
        raise ResultError(
            "soak RSS tail mean grew beyond the stability bound: "
            f"head={rss_head_mean:.0f}, tail={rss_tail_mean:.0f}, "
            f"allowed_growth={allowed_rss_growth:.0f} bytes"
        )

    if summary.get("schema_version") != 1 or summary.get("record_type") != "summary":
        raise ResultError("soak artifact does not end with a schema-1 summary")
    for field, expected in totals.items():
        if summary.get(field) != expected:
            raise ResultError(f"soak summary {field} does not equal interval totals")
    if summary["operations"] != summary["successes"]:
        raise ResultError("soak summary violates operations=successes")
    if summary["attempts"] != summary["successes"] + summary["errors"]:
        raise ResultError("soak summary violates attempts=successes+errors")
    elapsed = summary.get("elapsed_secs")
    if not _finite_number(elapsed) or not 14_399.0 <= float(elapsed) <= 14_405.0:
        raise ResultError(f"soak summary elapsed_secs={elapsed!r} is not a four-hour run")
    if not math.isclose(float(elapsed), previous_elapsed, rel_tol=0.0, abs_tol=2.0):
        raise ResultError("soak summary elapsed time does not match the final interval")
    if summary["successes"] <= 0:
        raise ResultError("soak summary has no successful operations")
    _validate_rate(
        summary,
        numerator="successes",
        rate="ops_per_sec",
        window=float(elapsed),
        context="soak summary",
    )
    _validate_rate(
        summary,
        numerator="attempts",
        rate="attempted_ops_per_sec",
        window=float(elapsed),
        context="soak summary",
    )
    _validate_quantiles(summary, context="soak summary")
    if not _nonnegative_integer(summary.get("rss_bytes")) or summary["rss_bytes"] == 0:
        raise ResultError("soak summary has no positive RSS sample")
    for field in lifecycle_fields:
        if summary.get(f"{field}_total") != lifecycle_totals[field]:
            raise ResultError(f"soak summary {field} total does not match intervals")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--standalone", action="append", type=Path)
    parser.add_argument("--cluster", action="append", type=Path)
    parser.add_argument(
        "--pipeline-depth", action="append", type=parse_pipeline
    )
    parser.add_argument(
        "--pipeline-concurrency", action="append", type=Path
    )
    parser.add_argument("--soak", type=Path)
    parser.add_argument("--validate-soak-only", action="store_true")
    parser.add_argument(
        "--matrix-only",
        action="store_true",
        help="validate development matrices but emit an explicitly incomplete summary",
    )
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    if args.validate_soak_only:
        if args.soak is None:
            raise ResultError("--validate-soak-only requires --soak")
        validate_soak(args.soak)
        return 0
    if not all(
        (
            args.standalone,
            args.cluster,
            args.pipeline_depth,
            args.pipeline_concurrency,
            args.output_dir,
        )
    ):
        raise ResultError(
            "matrix validation requires standalone, cluster, pipeline-depth, "
            "pipeline-concurrency, and output-dir arguments"
        )

    if args.matrix_only and args.soak is not None:
        raise ResultError("--matrix-only cannot be combined with --soak")
    if not args.matrix_only and args.soak is None:
        raise ResultError("publication validation requires --soak (or explicit --matrix-only)")

    standalone = [row for path in args.standalone for row in load_records(path)]
    cluster = [row for path in args.cluster for row in load_records(path)]
    validate_matrix(
        standalone,
        name="standalone",
        clients=STANDALONE_CLIENTS,
        workloads=("Set", "Get"),
        measurement_secs=PUBLICATION_MEASUREMENT_SECS,
        require_samples=True,
    )
    validate_matrix(
        cluster,
        name="cluster",
        clients=CLUSTER_CLIENTS,
        workloads=("Set", "Get"),
        measurement_secs=PUBLICATION_MEASUREMENT_SECS,
        require_samples=True,
    )

    pipelines: dict[int, list[dict[str, Any]]] = {}
    for depth, path in args.pipeline_depth:
        pipelines.setdefault(depth, []).extend(load_records(path))
    if set(pipelines) != set(PIPELINE_DEPTHS):
        raise ResultError("pipeline depth sweep must cover depths 10, 100, and 1000")
    for depth, records in pipelines.items():
        validate_matrix(
            records,
            name=f"pipeline-depth-{depth}",
            clients=STANDALONE_CLIENTS,
            workloads=("Pipeline",),
            concurrencies=(1,),
            commands_per_batch=depth,
            measurement_secs=PUBLICATION_MEASUREMENT_SECS,
            require_samples=True,
        )

    pipeline_concurrency = [
        row for path in args.pipeline_concurrency for row in load_records(path)
    ]
    validate_matrix(
        pipeline_concurrency,
        name="pipeline-concurrency",
        clients=STANDALONE_CLIENTS,
        workloads=("Pipeline",),
        payloads=(1024,),
        concurrencies=CONCURRENCIES,
        commands_per_batch=100,
        measurement_secs=PUBLICATION_MEASUREMENT_SECS,
        require_samples=True,
    )

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
        "schema_version": 2,
        "mode": "matrix_only_development" if args.matrix_only else "publication",
        "publication_complete": not args.matrix_only,
        "incomplete_reason": "four_hour_soak_not_run" if args.matrix_only else None,
        "chart_selection": {"workload": "Get", "payload_bytes": 1024},
        "standalone": _headline(standalone),
        "cluster": _headline(cluster),
        "pipeline_depth_sweep": {
            str(depth): records for depth, records in sorted(pipelines.items())
        },
        "pipeline_concurrency_sweep": pipeline_concurrency,
    }
    if args.soak is not None:
        summary["soak"] = validate_soak(args.soak)
    summary_name = "summary.incomplete.json" if args.matrix_only else "summary.json"
    (args.output_dir / summary_name).write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ResultError as error:
        raise SystemExit(f"benchmark result validation failed: {error}") from error
