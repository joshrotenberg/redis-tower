#!/usr/bin/env python3
"""Reduce `cargo metadata` to a public, path-free dependency graph."""

from __future__ import annotations

import json
import sys
from typing import Any
from urllib.parse import urlsplit, urlunsplit


class MetadataError(ValueError):
    """The Cargo metadata document cannot be safely normalized."""


def normalize_source(source: Any, *, workspace_member: bool) -> str:
    if source is None:
        return "workspace" if workspace_member else "path"
    if not isinstance(source, str):
        raise MetadataError(f"invalid Cargo package source {source!r}")
    if source.startswith("registry+"):
        return "crates.io" if "crates.io" in source else "registry"
    if source.startswith("git+"):
        parsed = urlsplit(source.removeprefix("git+"))
        if not parsed.scheme or not parsed.hostname:
            return "git"
        host = parsed.hostname.lower()
        port = f":{parsed.port}" if parsed.port is not None else ""
        path = parsed.path.removesuffix(".git")
        # Never retain URL credentials, revision queries, fragments, or a
        # filesystem-style source prefix in a public artifact.
        return "git+" + urlunsplit((parsed.scheme, host + port, path, "", ""))
    return source.split("+", 1)[0]


def sanitize(metadata: Any) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        raise MetadataError("Cargo metadata must be a JSON object")
    packages = metadata.get("packages")
    resolve = metadata.get("resolve")
    workspace_members = metadata.get("workspace_members")
    if not isinstance(packages, list) or not isinstance(resolve, dict):
        raise MetadataError("Cargo metadata is missing packages or resolve")
    if not isinstance(workspace_members, list):
        raise MetadataError("Cargo metadata is missing workspace_members")
    nodes = resolve.get("nodes")
    if not isinstance(nodes, list):
        raise MetadataError("Cargo metadata resolve graph has no nodes")

    features_by_id: dict[str, list[str]] = {}
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str):
            raise MetadataError("Cargo metadata contains an invalid resolve node")
        features = node.get("features")
        if not isinstance(features, list) or not all(
            isinstance(feature, str) for feature in features
        ):
            raise MetadataError("Cargo metadata node has invalid resolved features")
        features_by_id[node["id"]] = sorted(set(features))

    member_ids = set(workspace_members)
    output = []
    for package in packages:
        if not isinstance(package, dict):
            raise MetadataError("Cargo metadata contains an invalid package")
        package_id = package.get("id")
        name = package.get("name")
        version = package.get("version")
        if not all(isinstance(value, str) for value in (package_id, name, version)):
            raise MetadataError("Cargo metadata package has invalid identity fields")
        output.append(
            {
                "name": name,
                "version": version,
                "source": normalize_source(
                    package.get("source"), workspace_member=package_id in member_ids
                ),
                "resolved_features": features_by_id.get(package_id, []),
            }
        )
    output.sort(
        key=lambda package: (
            package["name"],
            package["version"],
            package["source"],
            package["resolved_features"],
        )
    )
    return {"schema_version": 1, "packages": output}


def main() -> int:
    try:
        metadata = json.load(sys.stdin)
        result = sanitize(metadata)
    except (json.JSONDecodeError, MetadataError) as error:
        raise SystemExit(f"metadata sanitization failed: {error}") from error
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
