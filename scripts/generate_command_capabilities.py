#!/usr/bin/env python3
"""Validate and generate the versioned Redis command capability ledger."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from generate_command_coverage import (
    COMMAND_ALIASES,
    SOURCES,
    collect_typed_names,
    load_metadata,
    normalize_name,
    resolve_typed_names,
)


SCHEMA_VERSION = 1
DEFAULT_METADATA_DIR = Path("conformance/redis-8.8")
DEFAULT_DETAILS = Path("conformance/command-capability-details.json")
DEFAULT_LEDGER = Path("conformance/command-capabilities.json")
DEFAULT_REPORT = Path("docs/COMMAND-CAPABILITIES.md")
DEFAULT_SOURCE_DIR = Path("crates/redis-tower-commands/src")
DOWNSTREAM_LEDGER_URL = (
    "https://github.com/redis-developer/redis-database-mcp-rs/blob/"
    "858a4dd84dfbc2102f80bdc18519e69afc47f263/docs/redis-command-coverage.md"
)

COMMAND_IMPL_PATTERN = re.compile(
    r"impl(?:\s*<[^>]+>)?\s+Command\s+for\s+([A-Za-z_][A-Za-z0-9_]*)"
    r"(?:<[^>]+>)?\s*\{.*?"
    r"fn\s+name\(&self\)\s*->\s*&str\s*\{\s*\"([^\"]+)\"",
    re.DOTALL,
)

DETAIL_STATUSES = {"verified", "partial", "unverified", "not_applicable"}
AVAILABILITY_DISPOSITIONS = {
    "typed_builder",
    "dedicated_api",
    "internal_protocol",
    "raw_only",
    "planned",
    "intentionally_unsupported",
    "excluded_deprecated",
    "excluded_container",
    "excluded_system",
}
NON_BUILDER_DISPOSITIONS = {
    "dedicated_api",
    "internal_protocol",
    "raw_only",
    "planned",
    "intentionally_unsupported",
}
WIRE_STATUSES = {"supported", "rejected", "planned", "unverified"}


@dataclass(frozen=True)
class TypedApi:
    api: str
    path: str
    symbol: str


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"{path} does not exist") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"{path}: invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path}: top level must be an object")
    return value


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metadata_scope(command: Any) -> str:
    if command.deprecated:
        return "deprecated"
    if command.container:
        return "container"
    if command.system:
        return "system"
    return "scoped"


def load_raw_metadata(metadata_dir: Path) -> dict[str, dict[str, Any]]:
    commands: dict[str, dict[str, Any]] = {}
    for source in SOURCES:
        path = metadata_dir / source.filename
        try:
            raw_commands = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as error:
            raise ValueError(f"missing pinned metadata file: {path}") from error
        except json.JSONDecodeError as error:
            raise ValueError(f"{path}: invalid JSON: {error}") from error
        if not isinstance(raw_commands, dict):
            raise ValueError(f"{path}: metadata must be an object")
        for raw_name, value in raw_commands.items():
            name = normalize_name(raw_name)
            if name in commands:
                raise ValueError(f"duplicate command in Redis metadata: {name}")
            if not isinstance(value, dict):
                raise ValueError(f"{path}: {name} metadata must be an object")
            commands[name] = {
                "tier": source.tier,
                "tier_label": source.label,
                "source_file": source.filename,
                "metadata": value,
            }
    return commands


def option_versions(arguments: Any) -> list[dict[str, str]]:
    found: dict[tuple[str, str], dict[str, str]] = {}

    def visit(values: Any) -> None:
        if not isinstance(values, list):
            return
        for argument in values:
            if not isinstance(argument, dict):
                continue
            token = argument.get("token")
            since = argument.get("since")
            if isinstance(token, str) and isinstance(since, str):
                key = (token.upper(), since)
                found[key] = {"token": token.upper(), "since": since}
            visit(argument.get("arguments"))

    visit(arguments)
    return [found[key] for key in sorted(found)]


def collect_typed_apis(source_dir: Path) -> dict[str, list[TypedApi]]:
    raw: dict[str, list[TypedApi]] = {}
    literal_names: set[str] = set()
    for source_path in sorted(source_dir.rglob("*.rs")):
        source = source_path.read_text(encoding="utf-8")
        for type_name, literal_name in COMMAND_IMPL_PATTERN.findall(source):
            name = normalize_name(literal_name)
            literal_names.add(name)
            raw.setdefault(name, []).append(
                TypedApi(
                    api=f"redis_tower_commands::{type_name}",
                    path=str(source_path),
                    symbol=type_name,
                )
            )

    scanner_names = collect_typed_names(source_dir)
    if literal_names != scanner_names:
        missing = sorted(scanner_names - literal_names)
        extra = sorted(literal_names - scanner_names)
        raise ValueError(
            "typed API parser disagrees with name scanner; "
            f"missing={missing}, extra={extra}"
        )
    return raw


def resolve_typed_apis(
    literal_apis: dict[str, list[TypedApi]], metadata: dict[str, Any]
) -> dict[str, list[TypedApi]]:
    resolve_typed_names(set(literal_apis), metadata)
    resolved: dict[str, list[TypedApi]] = {}
    for literal_name, apis in literal_apis.items():
        targets = (literal_name,) if literal_name in metadata else COMMAND_ALIASES[literal_name]
        for target in targets:
            resolved.setdefault(target, []).extend(apis)
    for name, apis in resolved.items():
        resolved[name] = sorted(set(apis), key=lambda item: (item.api, item.path))
    return resolved


def unique_by_name(values: Any, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(values, list):
        raise ValueError(f"{label} must be a list")
    result: dict[str, dict[str, Any]] = {}
    for item in values:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise ValueError(f"{label} entries require a string name")
        name = normalize_name(item["name"])
        if name in result:
            raise ValueError(f"duplicate {label} identity: {name}")
        result[name] = item
    return result


def unique_by_id(values: Any, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(values, list):
        raise ValueError(f"{label} must be a list")
    result: dict[str, dict[str, Any]] = {}
    for item in values:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise ValueError(f"{label} entries require a string id")
        identity = item["id"]
        if identity in result:
            raise ValueError(f"duplicate {label} identity: {identity}")
        result[identity] = item
    return result


def string_list(value: Any, label: str, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ValueError(f"{label} must be a list of non-empty strings")
    if nonempty and not value:
        raise ValueError(f"{label} must not be empty")
    return value


def validate_source_ref(repo_root: Path, value: Any, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    path_text = value.get("path")
    symbol = value.get("symbol")
    if not isinstance(path_text, str) or not path_text:
        raise ValueError(f"{label}.path must be a non-empty string")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError(f"{label}.symbol must be a non-empty string")
    path = repo_root / path_text
    if not path.is_file():
        raise ValueError(f"{label}: stale path {path_text}")
    if symbol not in path.read_text(encoding="utf-8"):
        raise ValueError(f"{label}: {symbol!r} is absent from {path_text}")


def validate_details(
    details: dict[str, Any],
    *,
    metadata_dir: Path,
    metadata: dict[str, Any],
    typed_apis: dict[str, list[TypedApi]],
    repo_root: Path,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    if details.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported capability detail schema {details.get('schema_version')!r}; "
            f"expected {SCHEMA_VERSION}"
        )
    provenance = details.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("provenance must be an object")
    for field in ("redis_version", "docs_revision", "source_url"):
        if not isinstance(provenance.get(field), str) or not provenance[field]:
            raise ValueError(f"provenance.{field} must be a non-empty string")
    if not re.fullmatch(r"[0-9a-f]{40}", provenance["docs_revision"]):
        raise ValueError("provenance.docs_revision must be a lowercase full commit SHA")
    if not provenance["source_url"].startswith("https://"):
        raise ValueError("provenance.source_url must be an https URL")
    files = provenance.get("metadata_files")
    if not isinstance(files, list):
        raise ValueError("provenance.metadata_files must be a list")
    expected_files = {source.filename for source in SOURCES}
    seen_files: set[str] = set()
    for item in files:
        if not isinstance(item, dict):
            raise ValueError("metadata file provenance entries must be objects")
        filename = item.get("filename")
        digest = item.get("sha256")
        if not isinstance(filename, str) or filename in seen_files:
            raise ValueError(f"duplicate or invalid metadata filename: {filename!r}")
        seen_files.add(filename)
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(f"{filename}: sha256 must be 64 lowercase hex characters")
        path = metadata_dir / filename
        if not path.is_file():
            raise ValueError(f"pinned metadata file is missing: {path}")
        actual = file_digest(path)
        if digest != actual:
            raise ValueError(
                f"pinned metadata digest mismatch for {filename}: {digest!r} != {actual}"
            )
    if seen_files != expected_files:
        raise ValueError(
            "metadata provenance files differ from the generator sources: "
            f"{sorted(seen_files)} != {sorted(expected_files)}"
        )

    non_builders = unique_by_name(details.get("non_builder_commands"), "non-builder")
    command_details = unique_by_name(details.get("command_details"), "command detail")
    behaviors = unique_by_id(details.get("behaviors"), "behavior")
    wire_contracts = unique_by_id(details.get("wire_contracts"), "wire contract")

    scoped = {name for name, command in metadata.items() if command.in_scope}
    missing = scoped - set(typed_apis)
    if set(non_builders) != missing:
        raise ValueError(
            "non-builder dispositions must exactly cover scoped names without typed "
            f"builders; missing={sorted(missing - set(non_builders))}, "
            f"extra={sorted(set(non_builders) - missing)}"
        )

    for name, entry in non_builders.items():
        disposition = entry.get("disposition")
        if disposition not in NON_BUILDER_DISPOSITIONS:
            raise ValueError(f"{name}: invalid non-builder disposition {disposition!r}")
        if not isinstance(entry.get("rationale"), str) or not entry["rationale"].strip():
            raise ValueError(f"{name}: non-builder rationale is required")
        apis = entry.get("api")
        if not isinstance(apis, list) or not all(isinstance(api, str) for api in apis):
            raise ValueError(f"{name}: api must be a string list")
        refs = entry.get("source_refs")
        if not isinstance(refs, list) or not refs:
            raise ValueError(f"{name}: source_refs must be a non-empty list")
        for index, ref in enumerate(refs):
            validate_source_ref(repo_root, ref, f"{name}.source_refs[{index}]")

    unknown_details = set(command_details) - set(metadata)
    if unknown_details:
        raise ValueError(f"command details reference unknown names: {sorted(unknown_details)}")

    for behavior_id, behavior in behaviors.items():
        expected_anchor = "behavior-" + re.sub(r"[^a-z0-9]+", "-", behavior_id.lower()).strip("-")
        if behavior.get("anchor") != expected_anchor:
            raise ValueError(
                f"behavior {behavior_id}: anchor must be {expected_anchor!r}"
            )
        if not isinstance(behavior.get("description"), str) or not behavior["description"].strip():
            raise ValueError(f"behavior {behavior_id}: description is required")
        for field in ("dimensions", "protocols", "features", "topologies"):
            string_list(behavior.get(field), f"behavior {behavior_id}.{field}", nonempty=True)
        if not isinstance(behavior.get("server"), str) or not behavior["server"].strip():
            raise ValueError(f"behavior {behavior_id}: server is required")
        tests = behavior.get("tests")
        ci = behavior.get("ci")
        if not isinstance(tests, list) or not tests:
            raise ValueError(f"behavior {behavior_id}.tests must be a non-empty list")
        if not isinstance(ci, list) or not ci:
            raise ValueError(f"behavior {behavior_id}.ci must be a non-empty list")
        for index, test in enumerate(tests):
            validate_source_ref(repo_root, test, f"behavior {behavior_id}.tests[{index}]")
        for index, workflow in enumerate(ci):
            validate_source_ref(
                repo_root, workflow, f"behavior {behavior_id}.ci[{index}]"
            )
        observed = behavior.get("last_observed_pass")
        if observed is not None:
            if not isinstance(observed, dict) or not all(
                isinstance(observed.get(key), str)
                for key in ("date", "commit", "url")
            ):
                raise ValueError(
                    f"behavior {behavior_id}: last_observed_pass needs date/commit/url"
                )
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", observed["date"]):
                raise ValueError(
                    f"behavior {behavior_id}: observed date must be YYYY-MM-DD"
                )
            if not re.fullmatch(r"[0-9a-f]{40}", observed["commit"]):
                raise ValueError(
                    f"behavior {behavior_id}: observed commit must be a full SHA"
                )
            if not observed["url"].startswith("https://"):
                raise ValueError(
                    f"behavior {behavior_id}: observed URL must use https"
                )

    for contract_id, contract in wire_contracts.items():
        if contract.get("status") not in WIRE_STATUSES:
            raise ValueError(
                f"wire contract {contract_id}: invalid status {contract.get('status')!r}"
            )
        if not isinstance(contract.get("description"), str) or not contract["description"].strip():
            raise ValueError(f"wire contract {contract_id}: description is required")
        implementation = contract.get("implementation")
        if not isinstance(implementation, list) or not implementation:
            raise ValueError(
                f"wire contract {contract_id}.implementation must be a non-empty list"
            )
        for index, ref in enumerate(implementation):
            validate_source_ref(
                repo_root, ref, f"wire contract {contract_id}.implementation[{index}]"
            )
        for behavior_id in contract.get("evidence", []):
            if behavior_id not in behaviors:
                raise ValueError(
                    f"wire contract {contract_id}: unknown behavior {behavior_id}"
                )

    for name, entry in command_details.items():
        for dimension in ("semantics", "operations"):
            value = entry.get(dimension)
            if value is None:
                continue
            if not isinstance(value, dict) or value.get("status") not in DETAIL_STATUSES:
                raise ValueError(f"{name}.{dimension}: invalid or missing status")
        for behavior_id in entry.get("evidence", []):
            if behavior_id not in behaviors:
                raise ValueError(f"{name}: unknown behavior {behavior_id}")

    return non_builders, command_details, behaviors, wire_contracts


def default_dimensions(scope: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if scope != "scoped":
        return (
            {"status": "not_applicable", "notes": "Outside the scoped denominator."},
            {"status": "not_applicable", "notes": "Outside the scoped denominator."},
        )
    return (
        {
            "status": "unverified",
            "binary_inputs": "unverified",
            "options": "unverified",
            "response": "unverified",
            "notes": "No command-specific semantic audit is recorded yet.",
        },
        {
            "status": "unverified",
            "keys": "unknown",
            "routing": "unknown",
            "session": "unknown",
            "notes": "No command-specific operational audit is recorded yet.",
        },
    )


def build_ledger(
    *,
    metadata_dir: Path,
    details_path: Path,
    source_dir: Path,
    repo_root: Path,
) -> dict[str, Any]:
    details = load_json(details_path)
    metadata = load_metadata(metadata_dir)
    raw_metadata = load_raw_metadata(metadata_dir)
    literal_apis = collect_typed_apis(source_dir)
    typed_apis = resolve_typed_apis(literal_apis, metadata)
    non_builders, command_details, behaviors, wire_contracts = validate_details(
        details,
        metadata_dir=metadata_dir,
        metadata=metadata,
        typed_apis=typed_apis,
        repo_root=repo_root,
    )

    rows: list[dict[str, Any]] = []
    for name in sorted(metadata):
        command = metadata[name]
        raw = raw_metadata[name]
        scope = metadata_scope(command)
        apis = typed_apis.get(name, [])
        if apis:
            availability = {
                "disposition": "typed_builder",
                "api": [item.api for item in apis],
                "rationale": "A dedicated typed Command implementation is present.",
                "source_refs": [
                    {"path": item.path, "symbol": item.symbol} for item in apis
                ],
            }
        elif scope == "scoped":
            entry = non_builders[name]
            availability = {
                key: entry[key]
                for key in ("disposition", "api", "rationale", "source_refs")
            }
        else:
            availability = {
                "disposition": f"excluded_{scope}",
                "api": [],
                "rationale": f"Pinned Redis metadata classifies this as {scope}.",
                "source_refs": [],
            }
        if availability["disposition"] not in AVAILABILITY_DISPOSITIONS:
            raise ValueError(
                f"{name}: internal invalid availability disposition "
                f"{availability['disposition']!r}"
            )

        semantics, operations = default_dimensions(scope)
        detail = command_details.get(name, {})
        semantics.update(detail.get("semantics", {}))
        operations.update(detail.get("operations", {}))
        if semantics["status"] == "not_applicable":
            for field in ("binary_inputs", "options", "response"):
                semantics[field] = "not_applicable"
        if operations["status"] == "not_applicable":
            for field in ("keys", "routing", "session"):
                operations[field] = "not_applicable"

        metadata_value = raw["metadata"]
        rows.append(
            {
                "name": name,
                "upstream": {
                    "tier": raw["tier"],
                    "tier_label": raw["tier_label"],
                    "source_file": raw["source_file"],
                    "group": command.group,
                    "since": metadata_value.get("since"),
                    "scope": scope,
                    "arguments_sha256": canonical_digest(
                        metadata_value.get("arguments", [])
                    ),
                    "option_versions": option_versions(
                        metadata_value.get("arguments", [])
                    ),
                },
                "availability": availability,
                "semantics": semantics,
                "operations": operations,
                "evidence": detail.get("evidence", []),
            }
        )

    provenance = dict(details["provenance"])
    provenance["entry_count"] = len(rows)
    provenance["scoped_count"] = sum(
        row["upstream"]["scope"] == "scoped" for row in rows
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "provenance": provenance,
        "commands": rows,
        "wire_contracts": [wire_contracts[key] for key in sorted(wire_contracts)],
        "behaviors": [behaviors[key] for key in sorted(behaviors)],
    }


def markdown_link_for_ref(ref: dict[str, str]) -> str:
    return f"[`{ref['symbol']}`](../{ref['path']})"


def text(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) or "—"
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("|", "\\|")
        .replace("\n", " ")
    )


def render_report(ledger: dict[str, Any]) -> str:
    rows = ledger["commands"]
    scoped = [row for row in rows if row["upstream"]["scope"] == "scoped"]
    disposition_counts: dict[str, int] = {}
    for row in scoped:
        disposition = row["availability"]["disposition"]
        disposition_counts[disposition] = disposition_counts.get(disposition, 0) + 1
    audited = [
        row
        for row in scoped
        if row["semantics"]["status"] != "unverified"
        or row["operations"]["status"] != "unverified"
    ]
    non_builders = [
        row for row in scoped if row["availability"]["disposition"] != "typed_builder"
    ]
    provenance = ledger["provenance"]

    lines = [
        "<!-- Generated by scripts/generate_command_capabilities.py; do not edit. -->",
        "",
        "# Redis command capability ledger",
        "",
        "This report separates four claims that cannot be reduced to one coverage "
        "percentage: command-name availability, typed semantics, RESP wire support, "
        "and session/topology behavior. A typed name does not prove every option, "
        "reply shape, protocol form, or routing mode.",
        "",
        "The machine-readable source is "
        "[`conformance/command-capabilities.json`](../conformance/command-capabilities.json). "
        "Unverified fields are explicit backlog, not implied support.",
        "",
        "## Pinned provenance",
        "",
        f"- Redis documentation version: **{provenance['redis_version']}**",
        f"- Redis documentation revision: "
        f"[`{provenance['docs_revision'][:12]}`]({provenance['source_url']})",
        f"- Pinned metadata entries: **{provenance['entry_count']}**",
        f"- Scoped command names: **{provenance['scoped_count']}**",
        "- Ordinary generation and CI are offline; each vendored metadata file is "
        "SHA-256 verified from the detail manifest.",
        "",
        "## Availability dispositions",
        "",
        "| Disposition | Scoped names | Meaning |",
        "|---|---:|---|",
    ]
    meanings = {
        "typed_builder": "Dedicated typed `Command` implementation.",
        "dedicated_api": "Supported by a stateful or higher-level API, not a stateless builder.",
        "internal_protocol": "Issued internally by routing/session machinery.",
        "raw_only": "Available only through an explicitly routed raw command.",
        "planned": "Planned typed or dedicated support; not currently available.",
        "intentionally_unsupported": "Deliberately outside the client API contract.",
    }
    for disposition in sorted(disposition_counts):
        lines.append(
            f"| `{disposition}` | {disposition_counts[disposition]} | "
            f"{meanings.get(disposition, 'Explicit disposition.')} |"
        )
    lines.extend(
        [
            "",
            "These counts describe availability only. They are not combined with "
            "semantic, wire, or operational verification into a headline score.",
            "",
            "## Scoped names without typed builders",
            "",
            "| Command | Disposition | Public API | Rationale |",
            "|---|---|---|---|",
        ]
    )
    for row in non_builders:
        availability = row["availability"]
        apis = "<br>".join(f"`{api}`" for api in availability["api"]) or "—"
        lines.append(
            f"| `{row['name']}` | `{availability['disposition']}` | {apis} | "
            f"{text(availability['rationale'])} |"
        )

    lines.extend(
        [
            "",
            "## Audited command semantics and operations",
            "",
            f"**{len(audited)}/{len(scoped)}** scoped names currently have a "
            "command-specific semantic or operational audit. The remaining rows are "
            "recorded as `unverified` in the machine ledger.",
            "",
            "| Command | Semantics | Binary inputs | Options / response | "
            "Routing / session | Evidence |",
            "|---|---|---|---|---|---|",
        ]
    )
    behavior_by_id = {item["id"]: item for item in ledger["behaviors"]}
    for row in audited:
        semantics = row["semantics"]
        operations = row["operations"]
        option_response = "; ".join(
            part
            for part in (
                semantics.get("options"),
                semantics.get("response"),
                semantics.get("notes"),
            )
            if part and part not in DETAIL_STATUSES
        )
        routing_session = "; ".join(
            part
            for part in (
                operations.get("routing"),
                operations.get("session"),
                operations.get("notes"),
            )
            if part and part not in {"unknown", "not_applicable"}
        )
        evidence = "<br>".join(
            f"[`{item}`](#{behavior_by_id[item]['anchor']})" for item in row["evidence"]
        ) or "—"
        lines.append(
            f"| `{row['name']}` | `{semantics['status']}` | "
            f"`{semantics.get('binary_inputs', 'unverified')}` | "
            f"{text(option_response)} | {text(routing_session)} | {evidence} |"
        )

    lines.extend(
        [
            "",
            "## Wire protocol contracts",
            "",
            "| Contract | Status | Current behavior | Evidence |",
            "|---|---|---|---|",
        ]
    )
    for contract in ledger["wire_contracts"]:
        evidence = "<br>".join(
            f"[`{item}`](#{behavior_by_id[item]['anchor']})"
            for item in contract.get("evidence", [])
        ) or "—"
        lines.append(
            f"| `{contract['id']}` | `{contract['status']}` | "
            f"{text(contract['description'])} | {evidence} |"
        )

    lines.extend(
        [
            "",
            "## Logical behavior evidence",
            "",
            "A source test, a configured workflow selector, and a dated successful run "
            "are separate evidence states. A later checkout must use its own attached "
            "checks rather than inheriting the observed result below.",
            "",
        ]
    )
    for behavior in ledger["behaviors"]:
        lines.extend(
            [
                f"### {behavior['anchor'].replace('-', ' ').capitalize()}",
                "",
                f"Logical ID: `{behavior['id']}`.",
                "",
                behavior["description"],
                "",
                f"- Dimensions: {', '.join(f'`{item}`' for item in behavior['dimensions'])}",
                f"- Matrix: server `{behavior['server']}`; protocols "
                f"`{', '.join(behavior['protocols'])}`; features "
                f"`{', '.join(behavior['features'])}`; topologies "
                f"`{', '.join(behavior['topologies'])}`",
                "- Source tests: "
                + ", ".join(markdown_link_for_ref(ref) for ref in behavior["tests"]),
                "- Configured CI: "
                + ", ".join(markdown_link_for_ref(ref) for ref in behavior["ci"]),
            ]
        )
        observed = behavior.get("last_observed_pass")
        if observed:
            lines.append(
                f"- Last observed pass: [{observed['date']} at "
                f"`{observed['commit'][:12]}`]({observed['url']})"
            )
        else:
            lines.append("- Last observed pass: **unverified**")
        lines.append("")

    lines.extend(
        [
            "## Offline validation and refresh",
            "",
            "Validate references, dispositions, source builders, pinned file hashes, "
            "and generated output without network access:",
            "",
            "```bash",
            "python3 scripts/generate_command_capabilities.py --check",
            "python3 scripts/generate_command_coverage.py --check",
            "```",
            "",
            "To review a newer Redis documentation snapshot, fetch an explicit commit "
            "into a separate directory and compare it before changing provenance:",
            "",
            "```bash",
            "python3 scripts/fetch_command_metadata.py --ref <redis-docs-commit> \\",
            "  --output-dir /tmp/redis-command-metadata",
            "python3 scripts/generate_command_capabilities.py \\",
            "  --compare-metadata /tmp/redis-command-metadata",
            "```",
            "",
            "The comparison reports added/removed names, scope changes, version/group "
            "changes, and argument/option metadata changes. Updating the five vendored "
            "files, their hashes, and every new disposition is a reviewed repository "
            "change. The downstream MCP project's "
            f"[Redis 8.10.1 application ledger]({DOWNSTREAM_LEDGER_URL}) "
            "has a different denominator and is not substituted for this client ledger.",
            "",
        ]
    )
    return "\n".join(lines)


def compare_metadata(
    baseline_dir: Path, candidate_dir: Path
) -> tuple[str, bool]:
    baseline_meta = load_metadata(baseline_dir)
    candidate_meta = load_metadata(candidate_dir)
    baseline_raw = load_raw_metadata(baseline_dir)
    candidate_raw = load_raw_metadata(candidate_dir)
    baseline_names = set(baseline_meta)
    candidate_names = set(candidate_meta)
    added = sorted(candidate_names - baseline_names)
    removed = sorted(baseline_names - candidate_names)
    changed: list[tuple[str, list[str]]] = []
    for name in sorted(baseline_names & candidate_names):
        fields: list[str] = []
        old = baseline_meta[name]
        new = candidate_meta[name]
        if metadata_scope(old) != metadata_scope(new):
            fields.append("scope")
        if old.group != new.group:
            fields.append("group")
        old_raw = baseline_raw[name]["metadata"]
        new_raw = candidate_raw[name]["metadata"]
        if old_raw.get("since") != new_raw.get("since"):
            fields.append("since")
        if canonical_digest(old_raw.get("arguments", [])) != canonical_digest(
            new_raw.get("arguments", [])
        ):
            fields.append("arguments/options")
        if fields:
            changed.append((name, fields))

    lines = [
        "# Redis command metadata comparison",
        "",
        f"- Added definitions: {len(added)}",
        f"- Removed definitions: {len(removed)}",
        f"- Changed scope/group/version/arguments: {len(changed)}",
        "",
    ]
    if added:
        lines.extend(["## Added", "", *(f"- `{name}`" for name in added), ""])
    if removed:
        lines.extend(["## Removed", "", *(f"- `{name}`" for name in removed), ""])
    if changed:
        lines.extend(
            [
                "## Changed",
                "",
                *(f"- `{name}`: {', '.join(fields)}" for name, fields in changed),
                "",
            ]
        )
    has_drift = bool(added or removed or changed)
    if not has_drift:
        lines.append("No tracked definition drift.")
    return "\n".join(lines), has_drift


def serialized_json(value: dict[str, Any]) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def check_or_write(path: Path, generated: str, *, check: bool) -> bool:
    if not check:
        path.write_text(generated, encoding="utf-8")
        print(f"wrote {path}")
        return True
    try:
        committed = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        print(f"error: {path} does not exist", file=sys.stderr)
        return False
    if committed == generated:
        print(f"{path} is current")
        return True
    diff = difflib.unified_diff(
        committed.splitlines(),
        generated.splitlines(),
        fromfile=str(path),
        tofile="generated",
        lineterm="",
    )
    print("\n".join(diff))
    print(f"error: {path} is stale", file=sys.stderr)
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--metadata-dir", type=Path, default=DEFAULT_METADATA_DIR)
    parser.add_argument("--details", type=Path, default=DEFAULT_DETAILS)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--compare-metadata", type=Path)
    args = parser.parse_args(argv)

    try:
        if args.compare_metadata is not None:
            report, drift = compare_metadata(args.metadata_dir, args.compare_metadata)
            print(report)
            return 1 if drift else 0
        repo_root = Path.cwd()
        ledger = build_ledger(
            metadata_dir=args.metadata_dir,
            details_path=args.details,
            source_dir=args.source_dir,
            repo_root=repo_root,
        )
        ledger_text = serialized_json(ledger)
        report_text = render_report(ledger)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    ledger_ok = check_or_write(args.ledger, ledger_text, check=args.check)
    report_ok = check_or_write(args.output, report_text, check=args.check)
    return 0 if ledger_ok and report_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
