#!/usr/bin/env python3
"""Fail-closed disk budgeting for fresh and resumable publication runs."""

from __future__ import annotations

import argparse
import os
import stat
from pathlib import Path


GIB_KIB = 1024 * 1024
FRESH_WORKSPACE_KIB = 10 * GIB_KIB
TEMPORARY_MINIMUM_KIB = 2 * GIB_KIB
RESULT_MINIMUM_KIB = 1 * GIB_KIB


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
    return max(0, FRESH_WORKSPACE_KIB - owned_kib)


def _non_overlapping(paths: list[Path]) -> list[Path]:
    selected: list[Path] = []
    for candidate in sorted((path.resolve() for path in paths), key=lambda path: len(path.parts)):
        if any(candidate == parent or candidate.is_relative_to(parent) for parent in selected):
            continue
        selected.append(candidate)
    return selected


def owned_workspace_kib(workspace: Path, target: Path, result: Path) -> int:
    workspace_device = _filesystem_id(workspace)
    owned_paths = []
    for path in (target, result):
        if path.is_symlink():
            raise DiskBudgetError(f"owned run path root must not be a symlink: {path}")
        if path.exists() and _filesystem_id(path) == workspace_device:
            owned_paths.append(path)
    return sum(allocated_kib(path) for path in _non_overlapping(owned_paths))


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
            owned_kib = owned_workspace_kib(workspace, target, result)
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
    parser.add_argument("--mode", required=True, choices=("fresh", "resume", "minima"))
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--temporary", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--target", required=True, type=Path)
    args = parser.parse_args()
    try:
        check_budget(
            mode=args.mode,
            workspace=args.workspace,
            temporary=args.temporary,
            result=args.result,
            target=args.target,
        )
    except DiskBudgetError as error:
        raise SystemExit(f"disk budget preflight failed: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
