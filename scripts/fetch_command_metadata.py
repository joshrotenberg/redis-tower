#!/usr/bin/env python3
"""Fetch Redis command metadata at an explicit immutable docs commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

from generate_command_coverage import SOURCES, Source


COMMIT_PATTERN = re.compile(r"[0-9a-fA-F]{40}")


def fetch(url: str) -> bytes:
    request = urllib.request.Request(
        url, headers={"User-Agent": "redis-tower-command-metadata-refresh"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def fetch_snapshot(ref: str, output_dir: Path) -> dict[str, object]:
    if not COMMIT_PATTERN.fullmatch(ref):
        raise ValueError("--ref must be a full 40-character Git commit SHA")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"output directory is not empty: {output_dir}")

    license_url = f"https://raw.githubusercontent.com/redis/docs/{ref}/LICENSE"
    license_payload = fetch(license_url)
    if not license_payload:
        raise ValueError("upstream Redis documentation license is empty")

    downloads: list[tuple[Source, str, bytes, str]] = []
    for source in SOURCES:
        url = (
            "https://raw.githubusercontent.com/redis/docs/"
            f"{ref}/data/{source.filename}"
        )
        raw_payload = fetch(url)
        raw_digest = hashlib.sha256(raw_payload).hexdigest()
        # Validate the downloaded bytes before applying the repository's explicit
        # trailing-whitespace normalization.
        parsed = json.loads(raw_payload)
        if not isinstance(parsed, dict):
            raise ValueError(f"{source.filename}: metadata must be a JSON object")
        # Upstream metadata occasionally contains insignificant trailing spaces.
        # Normalize those so vendored files pass repository whitespace checks.
        payload = re.sub(rb"[ \t]+(?=\r?$)", b"", raw_payload, flags=re.MULTILINE)
        downloads.append((source, url, payload, raw_digest))

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "LICENSE").write_bytes(license_payload)
    files: list[dict[str, str]] = []
    for source, url, payload, raw_digest in downloads:
        (output_dir / source.filename).write_bytes(payload)
        files.append(
            {
                "filename": source.filename,
                "raw_sha256": raw_digest,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "source_url": url,
            }
        )

    provenance: dict[str, object] = {
        "docs_revision": ref.lower(),
        "normalization": "strip trailing spaces and tabs from every line",
        "license": {
            "filename": "LICENSE",
            "sha256": hashlib.sha256(license_payload).hexdigest(),
            "source_url": license_url,
        },
        "files": files,
    }
    (output_dir / "PROVENANCE.json").write_text(
        json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
    )
    return provenance


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        provenance = fetch_snapshot(args.ref, args.output_dir)
    except (OSError, ValueError, json.JSONDecodeError, urllib.error.URLError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(provenance, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
