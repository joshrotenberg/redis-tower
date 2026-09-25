#!/usr/bin/env python3
"""Run one bounded cargo-fuzz campaign and retain reproducible evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence


MAX_DURATION_SECONDS = 3_600


@dataclass(frozen=True)
class Campaign:
    repo_root: Path
    target: str
    duration_seconds: int
    source_sha: str
    seed_dir: Path
    corpus_dir: Path
    artifact_dir: Path
    output_dir: Path
    cargo: str = "cargo"


def positive_bounded_duration(value: str) -> int:
    duration = int(value)
    if not 1 <= duration <= MAX_DURATION_SECONDS:
        raise argparse.ArgumentTypeError(
            f"duration must be between 1 and {MAX_DURATION_SECONDS} seconds"
        )
    return duration


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(128 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_provenance(path: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    files = sorted(item for item in path.rglob("*") if item.is_file()) if path.exists() else []
    total_bytes = 0
    for item in files:
        relative = item.relative_to(path).as_posix().encode()
        contents = item.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(contents).to_bytes(8, "big"))
        digest.update(contents)
        total_bytes += len(contents)
    return {
        "path": path.as_posix(),
        "file_count": len(files),
        "bytes": total_bytes,
        "sha256": digest.hexdigest(),
    }


def dependency_provenance(repo_root: Path) -> dict[str, str]:
    candidates = (
        repo_root / "Cargo.toml",
        repo_root / "Cargo.lock",
        repo_root / "fuzz" / "Cargo.toml",
        repo_root / "fuzz" / "Cargo.lock",
    )
    return {
        path.relative_to(repo_root).as_posix(): file_sha256(path)
        for path in candidates
        if path.is_file()
    }


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def prepare_corpus(seed_dir: Path, corpus_dir: Path) -> None:
    """Copy reviewed seeds, decoding `*.hex` fixtures to their wire bytes."""
    corpus_dir.mkdir(parents=True, exist_ok=True)
    if not seed_dir.is_dir():
        raise FileNotFoundError(f"reviewed seed directory does not exist: {seed_dir}")
    for seed in sorted(item for item in seed_dir.rglob("*") if item.is_file()):
        relative = seed.relative_to(seed_dir)
        destination = corpus_dir / relative
        if seed.suffix == ".hex":
            destination = destination.with_suffix("")
            contents = bytes.fromhex(seed.read_text())
        else:
            contents = seed.read_bytes()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(contents)


def run_campaign(
    campaign: Campaign,
    *,
    runner: Callable[..., subprocess.CompletedProcess[object]] = subprocess.run,
    monotonic: Callable[[], float] = time.monotonic,
    timestamp: Callable[[], str] = utc_now,
) -> int:
    if not 1 <= campaign.duration_seconds <= MAX_DURATION_SECONDS:
        raise ValueError("campaign duration is outside the bounded range")

    prepare_corpus(campaign.seed_dir, campaign.corpus_dir)
    campaign.artifact_dir.mkdir(parents=True, exist_ok=True)
    campaign.output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = campaign.output_dir / "manifest.json"
    log_path = campaign.output_dir / "campaign.log"
    command = [
        campaign.cargo,
        "fuzz",
        "run",
        campaign.target,
        str(campaign.corpus_dir),
        "--",
        f"-max_total_time={campaign.duration_seconds}",
        f"-artifact_prefix={campaign.artifact_dir}{os.sep}",
        "-print_final_stats=1",
    ]
    manifest: dict[str, object] = {
        "schema_version": 1,
        "status": "running",
        "source_sha": campaign.source_sha,
        "target": campaign.target,
        "duration_limit_seconds": campaign.duration_seconds,
        "started_at": timestamp(),
        "command": command,
        "dependencies": dependency_provenance(campaign.repo_root),
        "reviewed_seeds": tree_provenance(campaign.seed_dir),
        "starting_corpus": tree_provenance(campaign.corpus_dir),
    }
    write_json_atomic(manifest_path, manifest)
    started = monotonic()
    exit_code = 127
    status = "failed"
    failure: str | None = None
    try:
        with log_path.open("wb") as log:
            result = runner(
                command,
                cwd=campaign.repo_root,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
        exit_code = result.returncode
        status = "passed" if exit_code == 0 else "failed"
    except KeyboardInterrupt:
        exit_code = 130
        status = "interrupted"
        failure = "campaign interrupted"
    except OSError as error:
        failure = str(error)
    finally:
        manifest.update(
            {
                "status": status,
                "exit_code": exit_code,
                "completed_at": timestamp(),
                "elapsed_seconds": round(monotonic() - started, 6),
                "final_corpus": tree_provenance(campaign.corpus_dir),
                "artifacts": tree_provenance(campaign.artifact_dir),
            }
        )
        if failure is not None:
            manifest["failure"] = failure
        write_json_atomic(manifest_path, manifest)
    return exit_code


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--target", required=True)
    result.add_argument("--duration-seconds", required=True, type=positive_bounded_duration)
    result.add_argument("--source-sha", required=True)
    result.add_argument("--seed-dir", required=True, type=Path)
    result.add_argument("--corpus-dir", required=True, type=Path)
    result.add_argument("--artifact-dir", required=True, type=Path)
    result.add_argument("--output-dir", required=True, type=Path)
    result.add_argument("--cargo", default="cargo")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    return run_campaign(
        Campaign(
            repo_root=repo_root,
            target=args.target,
            duration_seconds=args.duration_seconds,
            source_sha=args.source_sha,
            seed_dir=args.seed_dir,
            corpus_dir=args.corpus_dir,
            artifact_dir=args.artifact_dir,
            output_dir=args.output_dir,
            cargo=args.cargo,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
