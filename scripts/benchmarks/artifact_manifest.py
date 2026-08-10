#!/usr/bin/env python3
"""Own, validate, finalize, and verify publication benchmark artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

import render_results


STATE_NAME = ".run-state.json"
MANIFEST_NAME = "manifest.json"
FINGERPRINT_NAME = "execution-fingerprint.json"
PUBLICATION_CONFIG: dict[str, Any] = {
    "build": {
        "cargo_profile": "release",
        "cargo_incremental": False,
        "isolated_target_directory": True,
    },
    "throughput": {
        "workloads": ["set", "get"],
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
BUILD_ENVIRONMENT: dict[str, Any] = {
    "schema_version": 1,
    "execution_environment": "cleared_then_allowlisted",
    "inherited_names_without_values": [
        "PATH",
        "HOME",
        "TMPDIR",
        "CARGO_HOME",
        "RUSTUP_HOME",
    ],
    "normalized": {
        "CARGO_INCREMENTAL": "0",
        "CARGO_PROFILE_RELEASE_DEBUG": "false",
        "CARGO_TARGET_DIR": "isolated",
        "CARGO_TERM_COLOR": "never",
        "LANG": "C",
        "LC_ALL": "C",
        "SOURCE_DATE_EPOCH": "git commit timestamp",
    },
}


class ManifestError(ValueError):
    """Artifact state is incomplete, corrupt, or from another provenance."""


def _read_json(path: Path) -> Any:
    _require_regular_file(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestError(f"cannot read {path.name}: {error}") from error


def _atomic_json(path: Path, value: Any) -> None:
    partial = path.with_name(path.name + ".partial")
    partial.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(partial, path)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _json_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _file_record(path: Path) -> dict[str, Any]:
    _require_regular_file(path)
    data = path.read_bytes()
    return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def _require_regular_file(path: Path) -> None:
    if path.is_symlink():
        raise ManifestError(f"unexpected symlink artifact {path.name!r}")
    if not path.is_file():
        raise ManifestError(f"required regular file is missing: {path}")


def _require_result_directory(result_dir: Path, *, create: bool = False) -> None:
    if result_dir.is_symlink():
        raise ManifestError("result directory root must not be a symlink")
    if create:
        try:
            result_dir.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ManifestError(f"cannot create result directory: {error}") from error
        if result_dir.is_symlink():
            raise ManifestError("result directory root must not be a symlink")
    if not result_dir.is_dir():
        raise ManifestError("result directory root must be a regular directory")


def _assert_path_free(value: Any) -> None:
    if isinstance(value, dict):
        for nested in value.values():
            _assert_path_free(nested)
    elif isinstance(value, list):
        for nested in value:
            _assert_path_free(nested)
    elif isinstance(value, str) and (
        "file://" in value.lower()
        or re.search(r"(?:^|[\s=(])/(?:[^\s)]+)", value)
        or re.search(r"(?:^|[\s=(])[A-Za-z]:[\\/]", value)
    ):
        raise ManifestError("execution fingerprint contains a filesystem path")


def _validate_execution(execution: Any) -> None:
    if not isinstance(execution, dict) or set(execution) != {
        "hardware",
        "operating_system",
        "tools",
    }:
        raise ManifestError("execution fingerprint has an invalid execution section")
    hardware = execution["hardware"]
    operating_system = execution["operating_system"]
    tools = execution["tools"]
    if not isinstance(hardware, dict) or set(hardware) != {
        "cpu_model",
        "logical_cpu_count",
        "memory_bytes",
    }:
        raise ManifestError("execution fingerprint has invalid hardware fields")
    if (
        not isinstance(hardware["cpu_model"], str)
        or not hardware["cpu_model"].strip()
        or hardware["cpu_model"].strip().lower() == "unknown"
        or type(hardware["logical_cpu_count"]) is not int
        or hardware["logical_cpu_count"] <= 0
        or type(hardware["memory_bytes"]) is not int
        or hardware["memory_bytes"] <= 0
    ):
        raise ManifestError("execution fingerprint has unknown or nonnumeric hardware")
    required_os = {"name", "kernel_release", "architecture", "product", "version", "build"}
    if not isinstance(operating_system, dict) or set(operating_system) != required_os:
        raise ManifestError("execution fingerprint has invalid operating-system fields")
    for field in required_os - {"build"}:
        value = operating_system[field]
        if not isinstance(value, str) or not value.strip() or value.strip().lower() == "unknown":
            raise ManifestError(f"execution fingerprint has invalid OS {field}")
    if not isinstance(operating_system["build"], str):
        raise ManifestError("execution fingerprint has invalid OS build")
    required_tools = {"rustc_vv", "cargo_vv", "python", "redis_server", "redis_cli"}
    if not isinstance(tools, dict) or set(tools) != required_tools:
        raise ManifestError("execution fingerprint has invalid tool fields")
    if any(not isinstance(tools[field], str) or not tools[field].strip() for field in required_tools):
        raise ManifestError("execution fingerprint has incomplete tool versions")


def _load_fingerprint(
    path: Path, source_sha: str, mode: str, lock_sha256: str
) -> dict[str, Any]:
    fingerprint = _read_json(path)
    if not isinstance(fingerprint, dict):
        raise ManifestError("execution fingerprint must be a JSON object")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ManifestError("source SHA must be a canonical 40-character lowercase digest")
    if not re.fullmatch(r"[0-9a-f]{64}", lock_sha256):
        raise ManifestError("Cargo.lock SHA-256 must be a canonical lowercase digest")
    expected_fields = {
        "schema_version": 1,
        "source_sha": source_sha,
        "cargo_lock_sha256": lock_sha256,
        "mode": mode,
        "config": PUBLICATION_CONFIG,
    }
    for field, expected in expected_fields.items():
        if fingerprint.get(field) != expected:
            raise ManifestError(
                f"execution fingerprint {field} does not match requested provenance"
            )
    if set(fingerprint) != {*expected_fields, "execution"}:
        raise ManifestError("execution fingerprint has missing or unexpected fields")
    _validate_execution(fingerprint.get("execution"))
    _assert_path_free(fingerprint)
    return fingerprint


def fingerprint_digest(path: Path) -> str:
    value = _read_json(path)
    if not isinstance(value, dict):
        raise ManifestError("execution fingerprint must be a JSON object")
    return _json_sha256(value)


def fingerprint_description(fingerprint: dict[str, Any]) -> str:
    execution = fingerprint["execution"]
    hardware = execution["hardware"]
    operating_system = execution["operating_system"]
    tools = execution["tools"]
    lines = [
        "execution_fingerprint_schema=1",
        f"git_sha={fingerprint['source_sha']}",
        f"cargo_lock_sha256={fingerprint['cargo_lock_sha256']}",
        f"mode={fingerprint['mode']}",
        f"os_name={operating_system['name']}",
        f"os_product={operating_system['product']}",
        f"os_version={operating_system['version']}",
        f"os_build={operating_system['build']}",
        f"kernel_release={operating_system['kernel_release']}",
        f"architecture={operating_system['architecture']}",
        f"cpu_model={hardware['cpu_model']}",
        f"logical_cpu_count={hardware['logical_cpu_count']}",
        f"memory_bytes={hardware['memory_bytes']}",
    ]
    for name in ("rustc_vv", "cargo_vv", "python", "redis_server", "redis_cli"):
        lines.append(f"{name}={tools[name].replace(chr(10), ' | ')}")
    return "\n".join(lines) + "\n"


def expected_state(
    source_sha: str,
    mode: str,
    lock_sha256: str,
    fingerprint: dict[str, Any],
) -> dict[str, Any]:
    if mode not in ("publication", "matrix-only"):
        raise ManifestError(f"invalid run mode {mode!r}")
    return {
        "schema_version": 2,
        "source_sha": source_sha,
        "cargo_lock_sha256": lock_sha256,
        "mode": mode,
        "config": PUBLICATION_CONFIG,
        "execution_fingerprint_sha256": _json_sha256(fingerprint),
    }


def initialize(
    result_dir: Path,
    source_sha: str,
    mode: str,
    lock_sha256: str,
    fingerprint_path: Path,
) -> None:
    fingerprint = _load_fingerprint(fingerprint_path, source_sha, mode, lock_sha256)
    _require_result_directory(result_dir, create=True)
    state_path = result_dir / STATE_NAME
    expected = expected_state(source_sha, mode, lock_sha256, fingerprint)
    if state_path.exists() or state_path.is_symlink():
        actual = _read_json(state_path)
        if actual != expected:
            raise ManifestError(
                "result directory belongs to a different source, configuration, "
                "or execution host fingerprint"
            )
        return
    existing = [path.name for path in result_dir.iterdir()]
    if existing:
        raise ManifestError(
            "non-empty result directory has no provenance state; refusing to overwrite it"
        )
    _atomic_json(state_path, expected)


def checkpoint_specs(mode: str) -> list[dict[str, Any]]:
    if mode not in ("publication", "matrix-only"):
        raise ManifestError(f"invalid run mode {mode!r}")
    specs: list[dict[str, Any]] = []
    for payload in PUBLICATION_CONFIG["throughput"]["payload_bytes"]:
        for concurrency in PUBLICATION_CONFIG["throughput"]["concurrency"]:
            specs.append(
                {
                    "name": f"standalone-throughput-p{payload}-c{concurrency}",
                    "kind": "standalone-throughput",
                    "payload_bytes": payload,
                    "concurrency": concurrency,
                    "result_name": "result.json",
                }
            )
    for depth in PUBLICATION_CONFIG["pipeline_depth_sweep"]["depths"]:
        for payload in PUBLICATION_CONFIG["pipeline_depth_sweep"]["payload_bytes"]:
            roles = ["depth-sweep"]
            if depth == 100 and payload == 1024:
                roles.append("concurrency-sweep")
            specs.append(
                {
                    "name": f"standalone-pipeline-d{depth}-p{payload}-c1",
                    "kind": "standalone-pipeline",
                    "payload_bytes": payload,
                    "concurrency": 1,
                    "depth": depth,
                    "roles": roles,
                    "result_name": "result.json",
                }
            )
    for concurrency in PUBLICATION_CONFIG["pipeline_concurrency_sweep"]["concurrency"]:
        if concurrency == 1:
            continue
        specs.append(
            {
                "name": f"standalone-pipeline-d100-p1024-c{concurrency}",
                "kind": "standalone-pipeline",
                "payload_bytes": 1024,
                "concurrency": concurrency,
                "depth": 100,
                "roles": ["concurrency-sweep"],
                "result_name": "result.json",
            }
        )
    for payload in PUBLICATION_CONFIG["throughput"]["payload_bytes"]:
        for concurrency in PUBLICATION_CONFIG["throughput"]["concurrency"]:
            specs.append(
                {
                    "name": f"cluster-throughput-p{payload}-c{concurrency}",
                    "kind": "cluster-throughput",
                    "payload_bytes": payload,
                    "concurrency": concurrency,
                    "result_name": "result.json",
                }
            )
    if mode == "publication":
        specs.append(
            {
                "name": "standalone-soak-4h",
                "kind": "standalone-soak",
                "result_name": "result.jsonl",
            }
        )
    return specs


def _spec_by_name(name: str, mode: str) -> dict[str, Any]:
    matches = [spec for spec in checkpoint_specs(mode) if spec["name"] == name]
    if len(matches) != 1:
        raise ManifestError(f"checkpoint {name!r} is not part of the {mode} contract")
    return matches[0]


def checkpoint_config(spec: dict[str, Any]) -> dict[str, Any]:
    kind = spec["kind"]
    if kind == "standalone-throughput":
        config = deepcopy(PUBLICATION_CONFIG["throughput"])
        config.pop("cluster_clients")
        config["payload_bytes"] = spec["payload_bytes"]
        config["concurrency"] = spec["concurrency"]
    elif kind == "cluster-throughput":
        config = deepcopy(PUBLICATION_CONFIG["throughput"])
        config.pop("standalone_clients")
        config.pop("standalone_port")
        config["payload_bytes"] = spec["payload_bytes"]
        config["concurrency"] = spec["concurrency"]
    elif kind == "standalone-pipeline":
        config = deepcopy(PUBLICATION_CONFIG["pipeline_depth_sweep"])
        config.pop("depths")
        config["depth"] = spec["depth"]
        config["payload_bytes"] = spec["payload_bytes"]
        config["pipeline_concurrency"] = spec["concurrency"]
        config["roles"] = spec["roles"]
    elif kind == "standalone-soak":
        config = deepcopy(PUBLICATION_CONFIG["soak"])
    else:  # pragma: no cover - all specs are constructed above
        raise ManifestError(f"unknown checkpoint kind {kind!r}")
    return {"kind": kind, **config}


def expected_command(spec: dict[str, Any]) -> list[str]:
    kind = spec["kind"]
    throughput = PUBLICATION_CONFIG["throughput"]
    if kind == "standalone-throughput":
        return [
            "$CARGO_TARGET_DIR/release/standalone-bench",
            "--secs",
            str(throughput["duration_secs"]),
            "--warmup",
            str(throughput["warmup_secs"]),
            "--runs",
            str(throughput["runs"]),
            "--payload-sizes",
            str(spec["payload_bytes"]),
            "--concurrency",
            str(spec["concurrency"]),
            "--pipeline-concurrency",
            "1",
            "--pipeline-commands",
            "100",
            "--clients",
            ",".join(throughput["standalone_clients"]),
            "--workloads",
            ",".join(throughput["workloads"]),
            "--port",
            str(throughput["standalone_port"]),
            "--include-samples",
            "--json",
        ]
    if kind == "standalone-pipeline":
        pipeline = PUBLICATION_CONFIG["pipeline_depth_sweep"]
        return [
            "$CARGO_TARGET_DIR/release/standalone-bench",
            "--secs",
            str(pipeline["duration_secs"]),
            "--warmup",
            str(pipeline["warmup_secs"]),
            "--runs",
            str(pipeline["runs"]),
            "--payload-sizes",
            str(spec["payload_bytes"]),
            "--concurrency",
            "1",
            "--pipeline-concurrency",
            str(spec["concurrency"]),
            "--pipeline-commands",
            str(spec["depth"]),
            "--clients",
            ",".join(pipeline["clients"]),
            "--workloads",
            "pipeline",
            "--port",
            str(pipeline["standalone_port"]),
            "--include-samples",
            "--json",
        ]
    if kind == "cluster-throughput":
        return [
            "$CARGO_TARGET_DIR/release/cluster-bench",
            "--secs",
            str(throughput["duration_secs"]),
            "--warmup",
            str(throughput["warmup_secs"]),
            "--runs",
            str(throughput["runs"]),
            "--payload-sizes",
            str(spec["payload_bytes"]),
            "--concurrency",
            str(spec["concurrency"]),
            "--clients",
            ",".join(throughput["cluster_clients"]),
            "--base-port",
            str(throughput["cluster_base_port"]),
            "--scenario",
            "throughput",
            "--include-samples",
            "--json",
        ]
    if kind == "standalone-soak":
        soak = PUBLICATION_CONFIG["soak"]
        return [
            f"SOAK_MODE={soak['mode']}",
            f"SOAK_CHAOS={soak['chaos']}",
            f"SOAK_DURATION_SECS={soak['duration_secs']}",
            f"SOAK_WARMUP_SECS={soak['warmup_secs']}",
            f"SOAK_REPORT_INTERVAL_SECS={soak['report_interval_secs']}",
            f"SOAK_CHAOS_AFTER_SECS={soak['chaos_after_secs']}",
            f"SOAK_CONCURRENCY={soak['concurrency']}",
            f"SOAK_OPERATION_TIMEOUT_MS={soak['operation_timeout_ms']}",
            f"SOAK_ERROR_BACKOFF_MS={soak['error_backoff_ms']}",
            f"SOAK_STARTUP_TIMEOUT_SECS={soak['startup_timeout_secs']}",
            f"SOAK_RECOVERY_TIMEOUT_SECS={soak['recovery_timeout_secs']}",
            f"SOAK_PAYLOAD_BYTES={soak['payload_bytes']}",
            f"SOAK_CLUSTER_SLOT={soak['cluster_slot']}",
            f"SOAK_CLUSTER_NODE_TIMEOUT_MS={soak['cluster_node_timeout_ms']}",
            f"SOAK_STANDALONE_PORT={soak['standalone_port']}",
            "$CARGO_TARGET_DIR/release/soak-bench",
            "--jsonl",
        ]
    raise ManifestError(f"unknown checkpoint kind {kind!r}")


def commands_text(mode: str) -> str:
    specs = checkpoint_specs(mode)
    lines = [
        "# Environment is cleared, then only the names in build-environment.json are supplied.",
        "cargo fetch --locked",
        "CARGO_TARGET_DIR=$ISOLATED_TARGET cargo build --profile release --locked "
        "-p standalone-bench -p cluster-bench -p soak-bench",
    ]
    lines.extend(
        " ".join(expected_command(spec))
        for spec in specs
        if spec["kind"] == "standalone-throughput"
    )
    lines.extend(
        " ".join(expected_command(spec))
        for spec in specs
        if spec["kind"] == "standalone-pipeline" and "depth-sweep" in spec["roles"]
    )
    lines.append(
        "# Pipeline depth and concurrency sweeps share their identical "
        "depth=100,payload=1024,concurrency=1 cell."
    )
    lines.extend(
        " ".join(expected_command(spec))
        for spec in specs
        if spec["kind"] == "standalone-pipeline" and spec["roles"] == ["concurrency-sweep"]
    )
    lines.extend(
        " ".join(expected_command(spec))
        for spec in specs
        if spec["kind"] == "cluster-throughput"
    )
    if mode == "publication":
        soak = next(spec for spec in specs if spec["kind"] == "standalone-soak")
        lines.append(" ".join(expected_command(soak)))
    else:
        lines.append("# INCOMPLETE DEVELOPMENT MODE: the mandatory four-hour soak was not run.")
    return "\n".join(lines) + "\n"


def _validate_checkpoint_result(directory: Path, spec: dict[str, Any]) -> None:
    result = directory / spec["result_name"]
    try:
        if spec["kind"] == "standalone-soak":
            render_results.validate_soak(result)
            return
        records = render_results.load_records(result)
        if spec["kind"] == "standalone-throughput":
            render_results.validate_matrix(
                records,
                name=spec["name"],
                clients=render_results.STANDALONE_CLIENTS,
                workloads=("Set", "Get"),
                payloads=(spec["payload_bytes"],),
                concurrencies=(spec["concurrency"],),
                require_samples=True,
            )
        elif spec["kind"] == "cluster-throughput":
            render_results.validate_matrix(
                records,
                name=spec["name"],
                clients=render_results.CLUSTER_CLIENTS,
                workloads=("Set", "Get"),
                payloads=(spec["payload_bytes"],),
                concurrencies=(spec["concurrency"],),
                require_samples=True,
            )
        else:
            render_results.validate_matrix(
                records,
                name=spec["name"],
                clients=render_results.STANDALONE_CLIENTS,
                workloads=("Pipeline",),
                payloads=(spec["payload_bytes"],),
                concurrencies=(spec["concurrency"],),
                commands_per_batch=spec["depth"],
                require_samples=True,
            )
    except render_results.ResultError as error:
        raise ManifestError(f"checkpoint {spec['name']} failed semantic validation: {error}") from error


def _checkpoint_metadata(
    directory: Path,
    spec: dict[str, Any],
    execution_fingerprint_sha256: str,
) -> dict[str, Any]:
    config = checkpoint_config(spec)
    command = expected_command(spec)
    content = {
        spec["result_name"]: _file_record(directory / spec["result_name"]),
        "stderr.log": _file_record(directory / "stderr.log"),
    }
    return {
        "schema_version": 1,
        "name": spec["name"],
        "execution_fingerprint_sha256": execution_fingerprint_sha256,
        "config": config,
        "config_sha256": _json_sha256(config),
        "command": command,
        "command_sha256": _json_sha256(command),
        "content": content,
        "content_sha256": _json_sha256(content),
    }


def _checkpoint_entries(directory: Path) -> set[str]:
    if directory.is_symlink() or not directory.is_dir():
        raise ManifestError(f"checkpoint path is not a regular directory: {directory}")
    entries: set[str] = set()
    for path in directory.iterdir():
        if path.is_symlink():
            raise ManifestError(f"checkpoint contains symlink {path.name!r}")
        if not path.is_file():
            raise ManifestError(f"checkpoint contains non-file entry {path.name!r}")
        entries.add(path.name)
    return entries


def finalize_checkpoint(
    directory: Path,
    name: str,
    mode: str,
    execution_fingerprint_sha256: str,
    command: list[str],
) -> None:
    spec = _spec_by_name(name, mode)
    required_before = {spec["result_name"], "stderr.log"}
    allowed_before = (required_before, {*required_before, "checkpoint.json.partial"})
    if _checkpoint_entries(directory) not in allowed_before:
        raise ManifestError(
            f"checkpoint {name} must contain exactly {sorted(required_before)!r} before sealing"
        )
    expected = expected_command(spec)
    if command != expected:
        raise ManifestError(f"checkpoint {name} command does not match the publication contract")
    _validate_checkpoint_result(directory, spec)
    _atomic_json(
        directory / "checkpoint.json",
        _checkpoint_metadata(directory, spec, execution_fingerprint_sha256),
    )


def verify_checkpoint(
    directory: Path,
    name: str,
    mode: str,
    execution_fingerprint_sha256: str,
) -> None:
    spec = _spec_by_name(name, mode)
    required = {spec["result_name"], "stderr.log", "checkpoint.json"}
    if _checkpoint_entries(directory) != required:
        raise ManifestError(f"checkpoint {name} has a missing or unexpected artifact")
    actual = _read_json(directory / "checkpoint.json")
    expected = _checkpoint_metadata(directory, spec, execution_fingerprint_sha256)
    if actual != expected:
        raise ManifestError(f"checkpoint {name} metadata or content hashes do not match")
    _validate_checkpoint_result(directory, spec)


def expected_artifact_paths(mode: str) -> set[str]:
    paths = {
        FINGERPRINT_NAME,
        "environment.txt",
        "build-environment.json",
        "cargo-lock.txt",
        "cargo-lock.sha256",
        "dependency-graph.json",
        "commands.txt",
        "rendered/throughput-vs-concurrency.svg",
        "rendered/p99-vs-concurrency.svg",
        (
            "rendered/summary.json"
            if mode == "publication"
            else "rendered/summary.incomplete.json"
        ),
    }
    for spec in checkpoint_specs(mode):
        prefix = f"checkpoints/{spec['name']}"
        paths.update(
            {
                f"{prefix}/{spec['result_name']}",
                f"{prefix}/stderr.log",
                f"{prefix}/checkpoint.json",
            }
        )
    return paths


def _expected_directories(mode: str) -> set[str]:
    directories: set[str] = set()
    for raw_path in expected_artifact_paths(mode):
        parent = Path(raw_path).parent
        while parent != Path("."):
            directories.add(parent.as_posix())
            parent = parent.parent
    return directories


def _validate_inventory(result_dir: Path, mode: str) -> None:
    expected_files = expected_artifact_paths(mode)
    expected_directories = _expected_directories(mode)
    actual_files: set[str] = set()
    actual_directories: set[str] = set()
    for path in result_dir.rglob("*"):
        relative = path.relative_to(result_dir).as_posix()
        if path.is_symlink():
            raise ManifestError(f"unexpected symlink artifact {relative!r}")
        if path.is_dir():
            actual_directories.add(relative)
        elif path.is_file():
            if relative not in (STATE_NAME, MANIFEST_NAME):
                actual_files.add(relative)
        else:
            raise ManifestError(f"unexpected non-regular artifact {relative!r}")
    missing_files = sorted(expected_files - actual_files)
    extra_files = sorted(actual_files - expected_files)
    missing_directories = sorted(expected_directories - actual_directories)
    extra_directories = sorted(actual_directories - expected_directories)
    if missing_files or extra_files or missing_directories or extra_directories:
        raise ManifestError(
            "artifact inventory mismatch: "
            f"missing files={missing_files[:5]!r}, unexpected files={extra_files[:5]!r}, "
            f"missing directories={missing_directories[:5]!r}, "
            f"unexpected directories={extra_directories[:5]!r}"
        )


def _artifact_hashes(result_dir: Path, mode: str) -> list[dict[str, Any]]:
    _validate_inventory(result_dir, mode)
    artifacts = []
    for relative in sorted(expected_artifact_paths(mode)):
        record = _file_record(result_dir / relative)
        artifacts.append({"path": relative, **record})
    return artifacts


def _render_args(result_dir: Path, mode: str, output_dir: Path) -> list[str]:
    args: list[str] = []
    specs = checkpoint_specs(mode)
    for spec in specs:
        path = result_dir / "checkpoints" / spec["name"] / spec["result_name"]
        if spec["kind"] == "standalone-throughput":
            args.extend(("--standalone", str(path)))
        elif spec["kind"] == "cluster-throughput":
            args.extend(("--cluster", str(path)))
        elif spec["kind"] == "standalone-pipeline" and "depth-sweep" in spec["roles"]:
            args.extend(("--pipeline-depth", f"{spec['depth']}={path}"))
    for concurrency in PUBLICATION_CONFIG["pipeline_concurrency_sweep"]["concurrency"]:
        name = f"standalone-pipeline-d100-p1024-c{concurrency}"
        path = result_dir / "checkpoints" / name / "result.json"
        args.extend(("--pipeline-concurrency", str(path)))
    if mode == "publication":
        soak = result_dir / "checkpoints" / "standalone-soak-4h" / "result.jsonl"
        args.extend(("--soak", str(soak)))
    else:
        args.append("--matrix-only")
    args.extend(("--output-dir", str(output_dir)))
    return args


def regenerate_rendered(result_dir: Path, mode: str, output_dir: Path) -> None:
    renderer = Path(__file__).with_name("render_results.py")
    completed = subprocess.run(
        [sys.executable, str(renderer), *_render_args(result_dir, mode, output_dir)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ManifestError(f"raw artifacts do not regenerate valid rendered output: {detail}")


def _verify_rendered_reproduction(result_dir: Path, mode: str) -> None:
    with tempfile.TemporaryDirectory(prefix="redis-tower-render-") as temporary:
        regenerated = Path(temporary)
        regenerate_rendered(result_dir, mode, regenerated)
        expected_names = {
            "throughput-vs-concurrency.svg",
            "p99-vs-concurrency.svg",
            "summary.json" if mode == "publication" else "summary.incomplete.json",
        }
        actual_names = {
            path.name for path in regenerated.iterdir() if path.is_file() and not path.is_symlink()
        }
        if actual_names != expected_names or any(path.is_symlink() for path in regenerated.iterdir()):
            raise ManifestError("renderer regenerated an unexpected artifact inventory")
        for name in sorted(expected_names):
            if (regenerated / name).read_bytes() != (result_dir / "rendered" / name).read_bytes():
                raise ManifestError(f"rendered artifact {name!r} does not match raw evidence")


def _validate_summary(result_dir: Path, mode: str) -> None:
    summary_name = "summary.json" if mode == "publication" else "summary.incomplete.json"
    summary = _read_json(result_dir / "rendered" / summary_name)
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


def _validate_fingerprint_artifact(result_dir: Path, state: dict[str, Any]) -> None:
    source_sha = state.get("source_sha")
    lock_sha256 = state.get("cargo_lock_sha256")
    mode = state.get("mode")
    if not all(isinstance(value, str) for value in (source_sha, lock_sha256, mode)):
        raise ManifestError("run state provenance fields are invalid")
    fingerprint = _load_fingerprint(
        result_dir / FINGERPRINT_NAME,
        source_sha,
        mode,
        lock_sha256,
    )
    if state != expected_state(source_sha, mode, lock_sha256, fingerprint):
        raise ManifestError("run state does not match its execution fingerprint")


def _validate_lock_artifacts(result_dir: Path, state: dict[str, Any]) -> None:
    expected = state["cargo_lock_sha256"]
    lock = _file_record(result_dir / "cargo-lock.txt")
    if lock["sha256"] != expected:
        raise ManifestError("recorded Cargo.lock does not match run state")
    digest_text = (result_dir / "cargo-lock.sha256").read_text(encoding="utf-8")
    if digest_text != f"{expected}  cargo-lock.txt\n":
        raise ManifestError("recorded Cargo.lock digest file is invalid")


def _validate_root_evidence(result_dir: Path, state: dict[str, Any]) -> None:
    fingerprint = _read_json(result_dir / FINGERPRINT_NAME)
    if (result_dir / "environment.txt").read_text(
        encoding="utf-8"
    ) != fingerprint_description(fingerprint):
        raise ManifestError("environment.txt does not match the execution fingerprint")
    if _read_json(result_dir / "build-environment.json") != BUILD_ENVIRONMENT:
        raise ManifestError("build-environment.json does not match the execution contract")
    if (result_dir / "commands.txt").read_text(encoding="utf-8") != commands_text(
        state["mode"]
    ):
        raise ManifestError("commands.txt does not match the checkpoint commands")
    dependency_graph = _read_json(result_dir / "dependency-graph.json")
    if (
        not isinstance(dependency_graph, dict)
        or set(dependency_graph) != {"schema_version", "packages"}
        or dependency_graph.get("schema_version") != 1
        or not isinstance(dependency_graph.get("packages"), list)
    ):
        raise ManifestError("dependency graph is not sanitized schema version 1")
    for package in dependency_graph["packages"]:
        if (
            not isinstance(package, dict)
            or set(package) != {"name", "version", "source", "resolved_features"}
            or not all(
                isinstance(package.get(field), str)
                for field in ("name", "version", "source")
            )
            or not isinstance(package.get("resolved_features"), list)
            or not all(isinstance(feature, str) for feature in package["resolved_features"])
        ):
            raise ManifestError("dependency graph contains an unsanitized package")
    _assert_path_free(dependency_graph)


def finalize(result_dir: Path) -> None:
    _require_result_directory(result_dir)
    state = _read_json(result_dir / STATE_NAME)
    mode = state.get("mode")
    if mode not in ("publication", "matrix-only"):
        raise ManifestError("run state has an invalid mode")
    if (result_dir / MANIFEST_NAME).exists() or (result_dir / MANIFEST_NAME).is_symlink():
        raise ManifestError("completed manifest already exists; use verify")
    _validate_fingerprint_artifact(result_dir, state)
    _validate_inventory(result_dir, mode)
    _validate_lock_artifacts(result_dir, state)
    _validate_root_evidence(result_dir, state)
    for spec in checkpoint_specs(mode):
        verify_checkpoint(
            result_dir / "checkpoints" / spec["name"],
            spec["name"],
            mode,
            state["execution_fingerprint_sha256"],
        )
    _validate_summary(result_dir, mode)
    _verify_rendered_reproduction(result_dir, mode)
    artifacts = _artifact_hashes(result_dir, mode)
    manifest = {
        **state,
        "run_complete": True,
        "publication_complete": mode == "publication",
        "completion": (
            "publication_evidence_complete"
            if mode == "publication"
            else "development_matrices_complete_four_hour_soak_missing"
        ),
        "artifact_inventory_sha256": _json_sha256(artifacts),
        "artifacts": artifacts,
    }
    _atomic_json(result_dir / MANIFEST_NAME, manifest)


def verify(
    result_dir: Path,
    source_sha: str,
    mode: str,
    lock_sha256: str,
    fingerprint_path: Path,
) -> None:
    _require_result_directory(result_dir)
    fingerprint = _load_fingerprint(fingerprint_path, source_sha, mode, lock_sha256)
    expected = expected_state(source_sha, mode, lock_sha256, fingerprint)
    state = _read_json(result_dir / STATE_NAME)
    if state != expected:
        raise ManifestError("run state does not match requested provenance and execution host")
    _validate_fingerprint_artifact(result_dir, state)
    manifest = _read_json(result_dir / MANIFEST_NAME)
    expected_manifest_fields = {
        *expected,
        "run_complete",
        "publication_complete",
        "completion",
        "artifact_inventory_sha256",
        "artifacts",
    }
    if not isinstance(manifest, dict) or set(manifest) != expected_manifest_fields:
        raise ManifestError("manifest has missing or unexpected fields")
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
    _validate_inventory(result_dir, mode)
    _validate_lock_artifacts(result_dir, state)
    _validate_root_evidence(result_dir, state)
    for spec in checkpoint_specs(mode):
        verify_checkpoint(
            result_dir / "checkpoints" / spec["name"],
            spec["name"],
            mode,
            state["execution_fingerprint_sha256"],
        )
    _validate_summary(result_dir, mode)
    _verify_rendered_reproduction(result_dir, mode)
    artifacts = _artifact_hashes(result_dir, mode)
    if manifest.get("artifacts") != artifacts:
        raise ManifestError("artifact hashes do not match the manifest")
    if manifest.get("artifact_inventory_sha256") != _json_sha256(artifacts):
        raise ManifestError("artifact inventory digest does not match the manifest")


def _add_provenance_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--result-dir", required=True, type=Path)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--lock-sha256", required=True)
    parser.add_argument("--mode", required=True, choices=("publication", "matrix-only"))
    parser.add_argument("--fingerprint-file", required=True, type=Path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "verify"):
        _add_provenance_args(subparsers.add_parser(command))
    finalize_parser = subparsers.add_parser("finalize")
    finalize_parser.add_argument("--result-dir", required=True, type=Path)
    digest_parser = subparsers.add_parser("fingerprint-digest")
    digest_parser.add_argument("--fingerprint-file", required=True, type=Path)
    commands_parser = subparsers.add_parser("commands")
    commands_parser.add_argument(
        "--mode", required=True, choices=("publication", "matrix-only")
    )
    for command in ("checkpoint-finalize", "checkpoint-verify"):
        checkpoint_parser = subparsers.add_parser(command)
        checkpoint_parser.add_argument("--checkpoint-dir", required=True, type=Path)
        checkpoint_parser.add_argument("--name", required=True)
        checkpoint_parser.add_argument(
            "--mode", required=True, choices=("publication", "matrix-only")
        )
        checkpoint_parser.add_argument("--fingerprint-sha256", required=True)
        if command == "checkpoint-finalize":
            checkpoint_parser.add_argument("logical_command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.command == "init":
            initialize(
                args.result_dir,
                args.source_sha,
                args.mode,
                args.lock_sha256,
                args.fingerprint_file,
            )
        elif args.command == "verify":
            verify(
                args.result_dir,
                args.source_sha,
                args.mode,
                args.lock_sha256,
                args.fingerprint_file,
            )
        elif args.command == "finalize":
            finalize(args.result_dir)
        elif args.command == "fingerprint-digest":
            print(fingerprint_digest(args.fingerprint_file))
        elif args.command == "commands":
            print(commands_text(args.mode), end="")
        elif args.command == "checkpoint-finalize":
            command = args.logical_command
            if command[:1] == ["--"]:
                command = command[1:]
            finalize_checkpoint(
                args.checkpoint_dir,
                args.name,
                args.mode,
                args.fingerprint_sha256,
                command,
            )
        else:
            verify_checkpoint(
                args.checkpoint_dir,
                args.name,
                args.mode,
                args.fingerprint_sha256,
            )
    except ManifestError as error:
        raise SystemExit(f"artifact manifest error: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
