#!/usr/bin/env python3
"""Generate the Redis 8.8 typed-command conformance report."""

from __future__ import annotations

import argparse
import difflib
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REDIS_DOCS_REF = "ad12d9cd6d10b53da2533ec3d7d7b2dae88bb2e0"
REDIS_VERSION = "8.8"
DEFAULT_OUTPUT = Path("COMMAND_COVERAGE.md")
DEFAULT_SOURCE_DIR = Path("crates/redis-tower-commands/src")


@dataclass(frozen=True)
class Source:
    tier: str
    label: str
    filename: str


SOURCES = (
    Source("T1", "Redis Core", "commands_core.json"),
    Source("T2", "Probabilistic", "commands_redisbloom.json"),
    Source("T3", "Search", "commands_redisearch.json"),
    Source("T4", "JSON", "commands_redisjson.json"),
    Source("T5", "Time Series", "commands_redistimeseries.json"),
)

# Some builders intentionally expose a narrower variant of one documented
# command, or share a family name across documented subcommands. Every alias is
# explicit so a newly invented name makes generation fail.
COMMAND_ALIASES: dict[str, tuple[str, ...]] = {
    "ACL LOG RESET": ("ACL LOG",),
    "DEBUG HELP": ("DEBUG",),
    "FT.CURSOR": ("FT.CURSOR DEL", "FT.CURSOR READ"),
}

NAME_PATTERN = re.compile(
    r'fn\s+name\(&self\)\s*->\s*&str\s*\{\s*"([^"]+)"\s*\}',
    re.MULTILINE,
)


@dataclass(frozen=True)
class CommandMetadata:
    name: str
    group: str
    tier: str
    tier_label: str
    deprecated: bool
    container: bool
    system: bool

    @property
    def in_scope(self) -> bool:
        return not (self.deprecated or self.container or self.system)


def normalize_name(name: str) -> str:
    return " ".join(name.upper().split())


def fetch_json(source: Source) -> dict[str, Any]:
    url = (
        "https://raw.githubusercontent.com/redis/docs/"
        f"{REDIS_DOCS_REF}/data/{source.filename}"
    )
    request = urllib.request.Request(
        url, headers={"User-Agent": "redis-tower-command-coverage"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.URLError as urllib_error:
        # The macOS system Python can lack a usable CA bundle. curl uses the
        # platform trust store and is also available on GitHub-hosted runners.
        try:
            result = subprocess.run(
                ["curl", "--fail", "--silent", "--show-error", "--location", url],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as curl_error:
            raise OSError(
                f"could not fetch {source.filename}: {urllib_error}; "
                f"curl fallback failed: {curl_error}"
            ) from curl_error
        return json.loads(result.stdout)


def load_metadata(metadata_dir: Path | None = None) -> dict[str, CommandMetadata]:
    commands: dict[str, CommandMetadata] = {}
    for source in SOURCES:
        if metadata_dir is None:
            raw_commands = fetch_json(source)
        else:
            with (metadata_dir / source.filename).open(encoding="utf-8") as handle:
                raw_commands = json.load(handle)

        for raw_name, metadata in raw_commands.items():
            name = normalize_name(raw_name)
            if name in commands:
                raise ValueError(f"duplicate command in Redis metadata: {name}")
            doc_flags = set(metadata.get("doc_flags") or [])
            summary = metadata.get("summary") or ""
            commands[name] = CommandMetadata(
                name=name,
                group=metadata["group"],
                tier=source.tier,
                tier_label=source.label,
                deprecated=(
                    metadata.get("deprecated_since") is not None
                    or "deprecated" in doc_flags
                ),
                container="container" in summary.lower(),
                system="syscmd" in doc_flags,
            )
    return commands


def collect_typed_names(source_dir: Path) -> set[str]:
    names: set[str] = set()
    for source_path in sorted(source_dir.rglob("*.rs")):
        source = source_path.read_text(encoding="utf-8")
        names.update(normalize_name(match) for match in NAME_PATTERN.findall(source))
    return names


def resolve_typed_names(
    typed_names: set[str], metadata: dict[str, CommandMetadata]
) -> set[str]:
    resolved: set[str] = set()
    unknown: list[str] = []
    for name in sorted(typed_names):
        if name in metadata:
            resolved.add(name)
        elif name in COMMAND_ALIASES:
            aliases = COMMAND_ALIASES[name]
            missing_aliases = [alias for alias in aliases if alias not in metadata]
            if missing_aliases:
                raise ValueError(
                    f"aliases for {name} are absent from Redis metadata: "
                    + ", ".join(missing_aliases)
                )
            resolved.update(aliases)
        else:
            unknown.append(name)

    if unknown:
        raise ValueError(
            "typed command names absent from the pinned Redis metadata: "
            + ", ".join(unknown)
            + ". Add an explicit COMMAND_ALIASES entry only for a documented variant."
        )
    return resolved


def percent(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "—"
    return f"{numerator / denominator * 100:.1f}%"


def render_report(
    metadata: dict[str, CommandMetadata], implemented_names: set[str]
) -> str:
    included = {name for name, command in metadata.items() if command.in_scope}
    implemented = included & implemented_names
    deprecated_implemented = sorted(
        name
        for name, command in metadata.items()
        if command.deprecated and name in implemented_names
    )
    excluded_containers = sum(command.container for command in metadata.values())
    excluded_system = sum(
        command.system and not command.container for command in metadata.values()
    )
    excluded_deprecated = sum(command.deprecated for command in metadata.values())

    groups: dict[tuple[str, str, str], list[CommandMetadata]] = {}
    for command in metadata.values():
        if command.in_scope:
            key = (command.tier, command.tier_label, command.group)
            groups.setdefault(key, []).append(command)

    complete_groups = sum(
        all(command.name in implemented for command in commands)
        for commands in groups.values()
    )

    lines = [
        "<!-- Generated by scripts/generate_command_coverage.py; do not edit. -->",
        "",
        "# Redis command coverage",
        "",
        f"`redis-tower` provides typed builders for **{len(implemented)}/{len(included)} "
        f"({percent(len(implemented), len(included))})** of the scoped Redis "
        f"{REDIS_VERSION} command surface. **{complete_groups}/{len(groups)} groups** "
        "have complete typed coverage. Commands without a dedicated builder remain "
        "available through `RawCommand` and `RawCommand::query`.",
        "",
        "The issue that introduced this report recorded a June 2026 baseline of "
        "**393/506 (77.7%)**, with 83.6% coverage of its Redis 8.6 comparison set. "
        "The headline above is regenerated from the current source tree.",
        "",
        "## Per-group coverage",
        "",
        "| Tier | Command group | Typed | Scoped | Coverage | Missing typed builders |",
        "|---|---|---:|---:|---:|---|",
    ]

    for (tier, _label, group), commands in sorted(groups.items()):
        command_names = {command.name for command in commands}
        group_implemented = command_names & implemented
        missing = sorted(command_names - implemented)
        missing_text = "<br>".join(f"`{name}`" for name in missing) or "—"
        lines.append(
            f"| {tier} | {group} | {len(group_implemented)} | {len(command_names)} | "
            f"{percent(len(group_implemented), len(command_names))} | {missing_text} |"
        )

    lines.extend(
        [
            "",
            "## Methodology",
            "",
            "The denominator is generated from the Redis documentation metadata at "
            f"[`redis/docs@{REDIS_DOCS_REF[:12]}`]"
            f"(https://github.com/redis/docs/tree/{REDIS_DOCS_REF}/data), the Redis "
            "8.8 GA documentation revision:",
            "",
        ]
    )
    for source in SOURCES:
        lines.append(f"- **{source.tier} — {source.label}:** `{source.filename}`")

    lines.extend(
        [
            "",
            "Deprecated commands, documentation-only command-family containers, and "
            "commands marked `syscmd` are excluded from the denominator. A command "
            "counts as implemented when a literal `Command::name()` in "
            "`redis-tower-commands` matches the upstream name. The small set of "
            "documented variants whose builder names differ is declared explicitly "
            "in the generator; any other unknown literal name fails generation.",
            "",
            f"The pinned metadata contains {len(metadata)} entries. This report excludes "
            f"{excluded_deprecated} deprecated entries, {excluded_containers} containers, "
            f"and {excluded_system} additional system commands.",
            "",
            "## Implemented but excluded as deprecated",
            "",
        ]
    )
    if deprecated_implemented:
        lines.extend(f"- `{name}`" for name in deprecated_implemented)
    else:
        lines.append("- None")

    lines.extend(
        [
            "",
            "Regenerate after adding or removing command builders:",
            "",
            "```bash",
            "python3 scripts/generate_command_coverage.py",
            "```",
            "",
            "CI runs the same generator with `--check`, so the committed report and "
            "typed command names cannot drift silently.",
            "",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if output is stale")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument(
        "--metadata-dir",
        type=Path,
        help="Read metadata files locally instead of fetching the pinned revision",
    )
    args = parser.parse_args(argv)

    try:
        metadata = load_metadata(args.metadata_dir)
        typed_names = collect_typed_names(args.source_dir)
        implemented_names = resolve_typed_names(typed_names, metadata)
        generated = render_report(metadata, implemented_names)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if args.check:
        try:
            committed = args.output.read_text(encoding="utf-8")
        except FileNotFoundError:
            print(f"error: {args.output} does not exist", file=sys.stderr)
            return 1
        if committed == generated:
            print(f"{args.output} is current")
            return 0
        diff = difflib.unified_diff(
            committed.splitlines(),
            generated.splitlines(),
            fromfile=str(args.output),
            tofile="generated",
            lineterm="",
        )
        print("\n".join(diff))
        print(
            f"\nerror: {args.output} is stale; run "
            "python3 scripts/generate_command_coverage.py",
            file=sys.stderr,
        )
        return 1

    args.output.write_text(generated, encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
