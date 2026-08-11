#!/usr/bin/env python3
"""Fail-closed disk budgeting for fresh and resumable publication runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any


GIB_KIB = 1024 * 1024
FRESH_WORKSPACE_KIB = 10 * GIB_KIB
TEMPORARY_MINIMUM_KIB = 2 * GIB_KIB
RESULT_MINIMUM_KIB = 1 * GIB_KIB
TARGET_CREDIT_CAP_KIB = 6 * GIB_KIB
TARGET_MARKER_NAME = ".redis-tower-publication-target.json"


class DiskBudgetError(ValueError):
    """The run cannot safely fit on one of its filesystems."""


def _nearest_existing(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and not candidate.is_symlink():
        parent = candidate.parent
        if parent == candidate:
            break
        candidate = parent
    if candidate.is_symlink():
        raise DiskBudgetError(f"disk-budget path root must not be a symlink: {path}")
    if not candidate.exists():
        raise DiskBudgetError(f"cannot find an existing parent for disk-budget path: {path}")
    return candidate


def _filesystem_id(path: Path) -> int:
    try:
        return os.stat(_nearest_existing(path)).st_dev
    except OSError as error:
        raise DiskBudgetError(f"cannot identify filesystem for {path}: {error}") from error


def available_kib(path: Path) -> int:
    try:
        filesystem = os.statvfs(_nearest_existing(path))
    except OSError as error:
        raise DiskBudgetError(f"cannot determine free disk space for {path}: {error}") from error
    return filesystem.f_bavail * filesystem.f_frsize // 1024


def _allocated_bytes(path: Path) -> int:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise DiskBudgetError(f"cannot inspect owned run path {path}: {error}") from error
    blocks = getattr(metadata, "st_blocks", None)
    own_bytes = blocks * 512 if isinstance(blocks, int) else metadata.st_size
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        return own_bytes
    try:
        with os.scandir(path) as entries:
            return own_bytes + sum(_allocated_bytes(Path(entry.path)) for entry in entries)
    except OSError as error:
        raise DiskBudgetError(f"cannot measure owned run path {path}: {error}") from error


def allocated_kib(path: Path) -> int:
    if path.is_symlink():
        raise DiskBudgetError(f"owned run path root must not be a symlink: {path}")
    if not path.exists():
        return 0
    if not path.is_dir():
        raise DiskBudgetError(f"owned run path is not a directory: {path}")
    return (_allocated_bytes(path) + 1023) // 1024


def workspace_required_kib(*, resume: bool, owned_kib: int) -> int:
    if owned_kib < 0:
        raise DiskBudgetError("owned disk allocation cannot be negative")
    if not resume:
        return FRESH_WORKSPACE_KIB
    return FRESH_WORKSPACE_KIB - min(owned_kib, TARGET_CREDIT_CAP_KIB)


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_run_state(state_file: Path) -> dict[str, Any]:
    if state_file.is_symlink() or not state_file.is_file():
        raise DiskBudgetError("validated run state must be a regular file")
    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DiskBudgetError(f"cannot read validated run state: {error}") from error
    expected_fields = {
        "schema_version",
        "source_sha",
        "cargo_lock_sha256",
        "mode",
        "config",
        "execution_fingerprint_sha256",
    }
    if not isinstance(state, dict) or set(state) != expected_fields:
        raise DiskBudgetError("validated run state has missing or unexpected fields")
    if type(state["schema_version"]) is not int or state["schema_version"] != 2:
        raise DiskBudgetError("validated run state has an unsupported schema")
    if not isinstance(state["source_sha"], str) or not re.fullmatch(
        r"[0-9a-f]{40}", state["source_sha"]
    ):
        raise DiskBudgetError("validated run state has an invalid source SHA")
    for field in ("cargo_lock_sha256", "execution_fingerprint_sha256"):
        if not isinstance(state[field], str) or not re.fullmatch(
            r"[0-9a-f]{64}", state[field]
        ):
            raise DiskBudgetError(f"validated run state has an invalid {field}")
    if state["mode"] not in ("publication", "matrix-only"):
        raise DiskBudgetError("validated run state has an invalid mode")
    if not isinstance(state["config"], dict):
        raise DiskBudgetError("validated run state has an invalid config")
    return state


def _target_marker(state_file: Path) -> dict[str, Any]:
    state = _read_run_state(state_file)
    return {
        "schema_version": 1,
        "run_state_sha256": _canonical_sha256(state),
        "source_sha": state["source_sha"],
        "cargo_lock_sha256": state["cargo_lock_sha256"],
        "mode": state["mode"],
        "config_sha256": _canonical_sha256(state["config"]),
        "execution_fingerprint_sha256": state["execution_fingerprint_sha256"],
    }


def _require_target_directory(target: Path, source_sha: str) -> None:
    if target.name != f"publication-{source_sha}":
        raise DiskBudgetError("isolated target directory does not match the source SHA")
    if target.is_symlink():
        raise DiskBudgetError(f"owned target root must not be a symlink: {target}")
    if not target.is_dir():
        raise DiskBudgetError(f"owned target root must be a directory: {target}")


def _validate_target_marker(target: Path, expected: dict[str, Any]) -> None:
    marker_path = target / TARGET_MARKER_NAME
    if marker_path.is_symlink() or not marker_path.is_file():
        raise DiskBudgetError("isolated target has no regular provenance marker")
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DiskBudgetError(
            f"cannot read isolated-target provenance marker: {error}"
        ) from error
    if marker != expected:
        raise DiskBudgetError("isolated-target provenance marker does not match run state")


def claim_target(target: Path, state_file: Path) -> None:
    expected = _target_marker(state_file)
    marker_name = TARGET_MARKER_NAME
    partial_name = marker_name + ".partial"
    if target.is_symlink():
        raise DiskBudgetError(f"owned target root must not be a symlink: {target}")
    if target.exists():
        if not target.is_dir():
            raise DiskBudgetError(f"owned target root must be a directory: {target}")
        marker_path = target / marker_name
        if marker_path.exists() or marker_path.is_symlink():
            _require_target_directory(target, expected["source_sha"])
            _validate_target_marker(target, expected)
            return
        try:
            entries = list(target.iterdir())
        except OSError as error:
            raise DiskBudgetError(f"cannot inspect isolated target: {error}") from error
        partial_path = target / partial_name
        if entries == [partial_path]:
            if partial_path.is_symlink() or not partial_path.is_file():
                raise DiskBudgetError("incomplete isolated-target marker is not a regular file")
            try:
                partial = json.loads(partial_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise DiskBudgetError(
                    f"cannot read incomplete isolated-target marker: {error}"
                ) from error
            if partial != expected:
                raise DiskBudgetError(
                    "incomplete isolated-target marker does not match run state"
                )
            try:
                os.replace(partial_path, marker_path)
            except OSError as error:
                raise DiskBudgetError(
                    f"cannot promote incomplete isolated-target marker: {error}"
                ) from error
            _validate_target_marker(target, expected)
            return
        if entries:
            raise DiskBudgetError("refusing to claim a non-empty unmarked target directory")
    else:
        try:
            target.mkdir(parents=True)
        except OSError as error:
            raise DiskBudgetError(f"cannot create isolated target directory: {error}") from error
    _require_target_directory(target, expected["source_sha"])
    marker_path = target / marker_name
    partial_path = target / partial_name
    if partial_path.exists() or partial_path.is_symlink():
        raise DiskBudgetError("incomplete isolated-target marker already exists")
    try:
        with partial_path.open("x", encoding="utf-8") as output:
            output.write(json.dumps(expected, indent=2, sort_keys=True) + "\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial_path, marker_path)
    except OSError as error:
        raise DiskBudgetError(
            f"cannot create isolated-target provenance marker: {error}"
        ) from error
    _validate_target_marker(target, expected)


def owned_target_kib(workspace: Path, target: Path, state_file: Path) -> int:
    expected = _target_marker(state_file)
    _require_target_directory(target, expected["source_sha"])
    _validate_target_marker(target, expected)
    if _filesystem_id(target) != _filesystem_id(workspace):
        return 0
    return min(allocated_kib(target), TARGET_CREDIT_CAP_KIB)


def _require(label: str, available: int, required: int) -> None:
    if available < required:
        raise DiskBudgetError(
            f"{label} has {available / GIB_KIB:.2f} GiB free; "
            f"requires {required / GIB_KIB:.2f} GiB"
        )


def check_budget(
    *,
    mode: str,
    workspace: Path,
    temporary: Path,
    result: Path,
    target: Path,
    state_file: Path | None = None,
) -> dict[str, int]:
    if mode not in ("fresh", "resume", "minima"):
        raise DiskBudgetError(f"unknown disk-budget mode {mode!r}")
    if target.is_symlink():
        raise DiskBudgetError(f"owned target root must not be a symlink: {target}")
    temporary_free = available_kib(temporary)
    result_free = available_kib(result)
    _require("temporary filesystem", temporary_free, TEMPORARY_MINIMUM_KIB)
    _require("result filesystem", result_free, RESULT_MINIMUM_KIB)
    owned_kib = 0
    workspace_required = 0
    workspace_free = available_kib(workspace)
    if mode != "minima":
        if mode == "resume":
            if state_file is None:
                raise DiskBudgetError("resume disk budgeting requires validated run state")
            owned_kib = owned_target_kib(workspace, target, state_file)
        workspace_required = workspace_required_kib(
            resume=mode == "resume", owned_kib=owned_kib
        )
        _require("workspace/build filesystem", workspace_free, workspace_required)
    return {
        "workspace_free_kib": workspace_free,
        "workspace_owned_kib": owned_kib,
        "workspace_required_kib": workspace_required,
        "temporary_free_kib": temporary_free,
        "result_free_kib": result_free,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", required=True, choices=("fresh", "resume", "minima", "claim")
    )
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--temporary", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--state-file", type=Path)
    args = parser.parse_args()
    try:
        if args.mode == "claim":
            if args.state_file is None:
                raise DiskBudgetError("target claim requires validated run state")
            claim_target(args.target, args.state_file)
        else:
            check_budget(
                mode=args.mode,
                workspace=args.workspace,
                temporary=args.temporary,
                result=args.result,
                target=args.target,
                state_file=args.state_file,
            )
    except DiskBudgetError as error:
        raise SystemExit(f"disk budget preflight failed: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
