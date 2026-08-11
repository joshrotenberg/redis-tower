#!/usr/bin/env python3
"""Collect a canonical, path-free publication benchmark execution fingerprint."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Any

from artifact_manifest import PUBLICATION_CONFIG, fingerprint_description


class FingerprintError(ValueError):
    """The execution host cannot provide trustworthy publication evidence."""


def _command(arguments: list[str], *, required: bool = True) -> str:
    try:
        completed = subprocess.run(
            arguments,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=os.environ.copy(),
            check=False,
        )
    except OSError as error:
        if required:
            raise FingerprintError(f"cannot execute {arguments[0]}: {error}") from error
        return ""
    output = completed.stdout.strip()
    if completed.returncode != 0 or not output:
        if required:
            raise FingerprintError(f"cannot collect {arguments[0]} version information")
        return ""
    return output


def _sysctl(name: str) -> str:
    return _command(["sysctl", "-n", name], required=False)


def _linux_os_release() -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = Path("/etc/os-release").read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        key, separator, value = line.partition("=")
        if separator and key in {"PRETTY_NAME", "ID", "VERSION_ID", "BUILD_ID"}:
            values[key] = value.strip().strip('"')
    return values


def _linux_cpu_model() -> str:
    try:
        lines = Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for preferred in ("model name", "hardware", "processor"):
        for line in lines:
            key, separator, value = line.partition(":")
            if separator and key.strip().lower() == preferred:
                candidate = value.strip()
                if candidate and not candidate.isdigit():
                    return candidate
    lscpu = _command(["lscpu"], required=False)
    for line in lscpu.splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip().lower() == "model name" and value.strip():
            return value.strip()
    return ""


def _linux_memory_bytes() -> int | None:
    try:
        lines = Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        match = re.fullmatch(r"MemTotal:\s+([0-9]+)\s+kB", line)
        if match:
            return int(match.group(1)) * 1024
    return None


def _positive_integer(value: Any) -> bool:
    return type(value) is int and value > 0


def validate_execution(execution: dict[str, Any]) -> None:
    if set(execution) != {"hardware", "operating_system", "tools"}:
        raise FingerprintError("execution fingerprint sections are incomplete")
    hardware = execution.get("hardware")
    operating_system = execution.get("operating_system")
    tools = execution.get("tools")
    if not isinstance(hardware, dict) or not isinstance(operating_system, dict):
        raise FingerprintError("execution fingerprint host data is invalid")
    if not isinstance(tools, dict):
        raise FingerprintError("execution fingerprint tool data is invalid")

    missing_hardware = []
    model = hardware.get("cpu_model")
    if not isinstance(model, str) or not model.strip() or model.strip().lower() == "unknown":
        missing_hardware.append("CPU model")
    if not _positive_integer(hardware.get("logical_cpu_count")):
        missing_hardware.append("CPU count")
    if not _positive_integer(hardware.get("memory_bytes")):
        missing_hardware.append("RAM bytes")
    if missing_hardware:
        raise FingerprintError(
            "publication hardware preflight could not determine "
            + ", ".join(missing_hardware)
            + "; rerun outside the sandbox on the physical benchmark host"
        )

    for field in ("name", "kernel_release", "architecture", "product", "version"):
        value = operating_system.get(field)
        if not isinstance(value, str) or not value.strip() or value.strip().lower() == "unknown":
            raise FingerprintError(
                f"publication host preflight could not determine OS {field}; "
                "rerun outside the sandbox on the physical benchmark host"
            )
    expected_tools = {"rustc_vv", "cargo_vv", "python", "redis_server", "redis_cli"}
    if set(tools) != expected_tools or any(
        not isinstance(tools[field], str) or not tools[field].strip()
        for field in expected_tools
    ):
        raise FingerprintError("execution fingerprint tool versions are incomplete")


def _assert_path_free(value: Any) -> None:
    if isinstance(value, dict):
        for nested in value.values():
            _assert_path_free(nested)
    elif isinstance(value, list):
        for nested in value:
            _assert_path_free(nested)
    elif isinstance(value, str):
        if (
            "file://" in value.lower()
            or re.search(r"(?:^|[\s=(])/(?:[^\s)]+)", value)
            or re.search(r"(?:^|[\s=(])[A-Za-z]:[\\/]", value)
        ):
            raise FingerprintError("execution fingerprint unexpectedly contains a filesystem path")


def collect_execution() -> dict[str, Any]:
    system = platform.system()
    kernel_release = platform.release().strip()
    architecture = platform.machine().strip()
    logical_count = os.cpu_count()

    if system == "Darwin":
        cpu_model = _sysctl("machdep.cpu.brand_string")
        memory_raw = _sysctl("hw.memsize")
        try:
            memory_bytes = int(memory_raw)
        except ValueError:
            memory_bytes = None
        product = _command(["sw_vers", "-productName"], required=False)
        version = _command(["sw_vers", "-productVersion"], required=False)
        build = _command(["sw_vers", "-buildVersion"], required=False)
    elif system == "Linux":
        os_release = _linux_os_release()
        cpu_model = _linux_cpu_model()
        memory_bytes = _linux_memory_bytes()
        product = os_release.get("PRETTY_NAME") or os_release.get("ID", "")
        version = os_release.get("VERSION_ID") or platform.version().strip()
        build = os_release.get("BUILD_ID", "")
    else:
        cpu_model = platform.processor().strip()
        memory_bytes = None
        product = system
        version = platform.version().strip()
        build = ""

    execution = {
        "hardware": {
            "cpu_model": cpu_model,
            "logical_cpu_count": logical_count,
            "memory_bytes": memory_bytes,
        },
        "operating_system": {
            "name": system,
            "kernel_release": kernel_release,
            "architecture": architecture,
            "product": product,
            "version": version,
            "build": build,
        },
        "tools": {
            "rustc_vv": _command(["rustc", "-vV"]),
            "cargo_vv": _command(["cargo", "-vV"]),
            "python": f"{platform.python_implementation()} {platform.python_version()}",
            "redis_server": _command(["redis-server", "--version"]),
            "redis_cli": _command(["redis-cli", "--version"]),
        },
    }
    validate_execution(execution)
    _assert_path_free(execution)
    return execution


def build_fingerprint(
    source_sha: str,
    lock_sha256: str,
    mode: str,
    execution: dict[str, Any],
) -> dict[str, Any]:
    if mode not in ("publication", "matrix-only"):
        raise FingerprintError(f"invalid run mode {mode!r}")
    validate_execution(execution)
    minimum_memory = PUBLICATION_CONFIG["runtime"]["minimum_host_memory_bytes"]
    if mode == "publication" and execution["hardware"]["memory_bytes"] < minimum_memory:
        raise FingerprintError(
            "publication benchmarks require at least "
            f"{minimum_memory / (1024**3):.0f} GiB of host memory"
        )
    fingerprint = {
        "schema_version": 1,
        "source_sha": source_sha,
        "cargo_lock_sha256": lock_sha256,
        "mode": mode,
        "config": PUBLICATION_CONFIG,
        "execution": execution,
    }
    _assert_path_free(fingerprint)
    return fingerprint


def describe(fingerprint: dict[str, Any]) -> str:
    return fingerprint_description(fingerprint)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--source-sha", required=True)
    collect_parser.add_argument("--lock-sha256", required=True)
    collect_parser.add_argument(
        "--mode", required=True, choices=("publication", "matrix-only")
    )
    describe_parser = subparsers.add_parser("describe")
    describe_parser.add_argument("--fingerprint-file", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.command == "collect":
            fingerprint = build_fingerprint(
                args.source_sha,
                args.lock_sha256,
                args.mode,
                collect_execution(),
            )
            print(json.dumps(fingerprint, indent=2, sort_keys=True))
        else:
            fingerprint = json.loads(args.fingerprint_file.read_text(encoding="utf-8"))
            if not isinstance(fingerprint, dict):
                raise FingerprintError("execution fingerprint must be an object")
            validate_execution(fingerprint.get("execution", {}))
            _assert_path_free(fingerprint)
            print(describe(fingerprint), end="")
    except (FingerprintError, OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"benchmark fingerprint error: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
