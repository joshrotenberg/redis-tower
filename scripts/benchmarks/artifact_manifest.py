#!/usr/bin/env python3
"""Initialize, finalize, and verify resumable publication artifact sets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any


STATE_NAME = ".run-state.json"
MANIFEST_NAME = "manifest.json"
PUBLICATION_CONFIG: dict[str, Any] = {
    "build": {
        "cargo_profile": "release",
        "cargo_incremental": False,
        "isolated_target_directory": True,
    },
    "throughput": {
        "workloads": ["get", "set"],
        "standalone_clients": [
            "redis-tower",
            "redis-tower-mux",
            "redis-rs-sync",
            "redis-rs-async",
            "redis-rs-manager",
            "fred",
        ],
        "cluster_clients": [
            "redis-tower",
            "redis-tower-mux",
            "redis-rs-sync",
            "redis-rs-async",
            "fred",
        ],
        "payload_bytes": [16, 64, 1024, 16384, 102400],
        "concurrency": [1, 8, 32, 128],
        "standalone_port": 6480,
        "cluster_base_port": 17000,
        "duration_secs": 10,
        "warmup_secs": 2,
        "runs": 3,
        "raw_samples": True,
    },
    "pipeline_depth_sweep": {
        "clients": [
            "redis-tower",
            "redis-tower-mux",
            "redis-rs-sync",
            "redis-rs-async",
            "redis-rs-manager",
            "fred",
        ],
        "depths": [10, 100, 1000],
        "payload_bytes": [16, 64, 1024, 16384, 102400],
        "concurrency": 1,
        "standalone_port": 6480,
        "duration_secs": 10,
        "warmup_secs": 2,
        "runs": 3,
        "raw_samples": True,
    },
    "pipeline_concurrency_sweep": {
        "clients": [
            "redis-tower",
            "redis-tower-mux",
            "redis-rs-sync",
            "redis-rs-async",
            "redis-rs-manager",
            "fred",
        ],
        "depth": 100,
        "payload_bytes": 1024,
        "concurrency": [1, 8, 32, 128],
        "standalone_port": 6480,
        "duration_secs": 10,
        "warmup_secs": 2,
        "runs": 3,
        "raw_samples": True,
        "shared_cell": "depth=100,payload_bytes=1024,concurrency=1",
    },
    "soak": {
        "mode": "standalone",
        "workload": "get_validate",
        "chaos": "standalone-sigkill",
        "chaos_effect": "same_port_restart",
        "payload_bytes": 1024,
        "concurrency": 32,
        "duration_secs": 14400,
        "warmup_secs": 60,
        "report_interval_secs": 60,
        "chaos_after_secs": 7200,
        "operation_timeout_ms": 2000,
        "error_backoff_ms": 1,
        "startup_timeout_secs": 30,
        "recovery_timeout_secs": 30,
        "cluster_slot": 42,
        "cluster_node_timeout_ms": 1000,
        "standalone_port": 6481,
    },
}


class ManifestError(ValueError):
    """Artifact state is incomplete, corrupt, or from another provenance."""


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestError(f"cannot read {path.name}: {error}") from error


def _atomic_json(path: Path, value: Any) -> None:
    partial = path.with_name(path.name + ".partial")
    partial.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(partial, path)


def expected_state(source_sha: str, mode: str, lock_sha256: str) -> dict[str, Any]:
    if mode not in ("publication", "matrix-only"):
        raise ManifestError(f"invalid run mode {mode!r}")
    return {
        "schema_version": 1,
        "source_sha": source_sha,
        "cargo_lock_sha256": lock_sha256,
        "mode": mode,
        "config": PUBLICATION_CONFIG,
    }


def initialize(result_dir: Path, source_sha: str, mode: str, lock_sha256: str) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    state_path = result_dir / STATE_NAME
    expected = expected_state(source_sha, mode, lock_sha256)
    if state_path.exists():
        actual = _read_json(state_path)
        if actual != expected:
            raise ManifestError(
                "result directory belongs to a different source SHA, mode, or configuration"
            )
        return
    existing = [path.name for path in result_dir.iterdir()]
    if existing:
        raise ManifestError(
            "non-empty result directory has no provenance state; refusing to overwrite it"
        )
    _atomic_json(state_path, expected)


def _artifact_hashes(result_dir: Path) -> list[dict[str, Any]]:
    artifacts = []
    for path in sorted(result_dir.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(result_dir)
        if relative.as_posix() in (STATE_NAME, MANIFEST_NAME):
            continue
        if any(part.endswith(".partial") for part in relative.parts):
            continue
        if path.is_symlink():
            raise ManifestError(f"unexpected symlink artifact {relative.as_posix()!r}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ManifestError(f"unexpected non-regular artifact {relative.as_posix()!r}")
        data = path.read_bytes()
        artifacts.append(
            {
                "path": relative.as_posix(),
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
            }
        )
    return artifacts


def _partial_paths(result_dir: Path) -> list[str]:
    return sorted(
        path.relative_to(result_dir).as_posix()
        for path in result_dir.rglob("*")
        if path.name.endswith(".partial")
    )


def finalize(result_dir: Path) -> None:
    state = _read_json(result_dir / STATE_NAME)
    mode = state.get("mode")
    partials = _partial_paths(result_dir)
    if partials:
        raise ManifestError(f"incomplete partial artifacts remain: {partials!r}")
    required = (
        ("rendered/summary.json", "checkpoints/standalone-soak-4h/result.jsonl")
        if mode == "publication"
        else ("rendered/summary.incomplete.json",)
    )
    missing = [name for name in required if not (result_dir / name).is_file()]
    if missing:
        raise ManifestError(f"cannot finalize; required artifacts are missing: {missing!r}")
    summary = _read_json(result_dir / required[0])
    expected_publication = mode == "publication"
    expected_summary_mode = "publication" if expected_publication else "matrix_only_development"
    if (
        not isinstance(summary, dict)
        or summary.get("publication_complete") is not expected_publication
        or summary.get("mode") != expected_summary_mode
    ):
        raise ManifestError("rendered summary completion markers do not match run mode")
    if expected_publication and "soak" not in summary:
        raise ManifestError("publication summary does not contain validated soak evidence")
    if not expected_publication and (
        summary.get("incomplete_reason") != "four_hour_soak_not_run" or "soak" in summary
    ):
        raise ManifestError("matrix-only summary is not unmistakably incomplete")
    manifest = {
        **state,
        "run_complete": True,
        "publication_complete": mode == "publication",
        "completion": (
            "publication_evidence_complete"
            if mode == "publication"
            else "development_matrices_complete_four_hour_soak_missing"
        ),
        "artifacts": _artifact_hashes(result_dir),
    }
    _atomic_json(result_dir / MANIFEST_NAME, manifest)


def verify(result_dir: Path, source_sha: str, mode: str, lock_sha256: str) -> None:
    expected = expected_state(source_sha, mode, lock_sha256)
    state = _read_json(result_dir / STATE_NAME)
    if state != expected:
        raise ManifestError("run state does not match requested provenance")
    manifest = _read_json(result_dir / MANIFEST_NAME)
    for field, value in expected.items():
        if manifest.get(field) != value:
            raise ManifestError(f"manifest {field} does not match requested provenance")
    if manifest.get("run_complete") is not True:
        raise ManifestError("manifest does not describe a completed run")
    if manifest.get("publication_complete") is not (mode == "publication"):
        raise ManifestError("manifest publication completion marker is inconsistent")
    expected_completion = (
        "publication_evidence_complete"
        if mode == "publication"
        else "development_matrices_complete_four_hour_soak_missing"
    )
    if manifest.get("completion") != expected_completion:
        raise ManifestError("manifest completion description is inconsistent")
    partials = _partial_paths(result_dir)
    if partials:
        raise ManifestError(f"partial artifacts remain beside a completed manifest: {partials!r}")
    if manifest.get("artifacts") != _artifact_hashes(result_dir):
        raise ManifestError("artifact hashes do not match the manifest")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "verify"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--result-dir", required=True, type=Path)
        subparser.add_argument("--source-sha", required=True)
        subparser.add_argument("--lock-sha256", required=True)
        subparser.add_argument("--mode", required=True, choices=("publication", "matrix-only"))
    finalize_parser = subparsers.add_parser("finalize")
    finalize_parser.add_argument("--result-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.command == "init":
            initialize(args.result_dir, args.source_sha, args.mode, args.lock_sha256)
        elif args.command == "verify":
            verify(args.result_dir, args.source_sha, args.mode, args.lock_sha256)
        else:
            finalize(args.result_dir)
    except ManifestError as error:
        raise SystemExit(f"artifact manifest error: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
