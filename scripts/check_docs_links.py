#!/usr/bin/env python3
"""Check repository-local links in the README and mdBook Markdown sources."""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
REPOSITORY_PATH = "/joshrotenberg/redis-tower"

FENCED_CODE = re.compile(r"^\s*(```|~~~).*?^\s*\1\s*$", re.MULTILINE | re.DOTALL)
INLINE_LINK = re.compile(r"!?\[[^]]*\]\(\s*(?:<([^>]+)>|([^\s)]+))")
LINKED_IMAGE = re.compile(
    r"\[\s*!\[[^]]*\]\(\s*(?:<[^>]+>|[^\s)]+)\s*\)\s*\]"
    r"\(\s*(?:<([^>]+)>|([^\s)]+))"
)
REFERENCE_LINK = re.compile(r"^\s*\[[^]]+\]:\s*(?:<([^>]+)>|([^\s]+))", re.MULTILINE)
ATX_HEADING = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+|$)(.*)$")
SETEXT_UNDERLINE = re.compile(r"^ {0,3}(?:=+|-+)[ \t]*$")
MARKDOWN_LINK_TEXT = re.compile(r"!?\[([^]]*)\](?:\([^)]*\)|\[[^]]*\])")
INLINE_CODE = re.compile(r"`+([^`]*)`+")
HTML_TAG = re.compile(r"<[^>]+>")


def markdown_text(source: Path) -> str:
    """Read Markdown with fenced code removed from structural checks."""

    return FENCED_CODE.sub("", source.read_text(encoding="utf-8"))


def targets_from_markdown(text: str) -> list[str]:
    """Return inline, linked-image, and reference-style link targets."""

    matches = [
        *LINKED_IMAGE.finditer(text),
        *INLINE_LINK.finditer(text),
        *REFERENCE_LINK.finditer(text),
    ]
    return [(match.group(1) or match.group(2)).strip() for match in matches]


def markdown_targets(source: Path) -> list[str]:
    """Return Markdown link targets outside fenced code."""

    return targets_from_markdown(markdown_text(source))


def canonical_repository_target(parsed_path: str) -> Path | None:
    """Map this repository's canonical GitHub URLs back to local paths."""

    path = parsed_path.rstrip("/")
    if path == REPOSITORY_PATH:
        return ROOT / "README.md"

    for marker in (f"{REPOSITORY_PATH}/blob/main/", f"{REPOSITORY_PATH}/tree/main/"):
        if parsed_path.startswith(marker):
            return ROOT / unquote(parsed_path.removeprefix(marker))
    return None


def local_target(source: Path, raw_target: str) -> tuple[Path, str | None] | None:
    """Resolve a local link and optional fragment, or skip an external link."""

    parsed = urlsplit(raw_target)
    if parsed.scheme or parsed.netloc or raw_target.startswith("//"):
        if parsed.scheme == "https" and parsed.netloc == "github.com":
            target = canonical_repository_target(parsed.path)
            if target is not None:
                fragment = unquote(parsed.fragment) if parsed.fragment else None
                return target.resolve(), fragment
        return None

    if parsed.path:
        path = Path(unquote(parsed.path))
        if path.is_absolute():
            return None
        target = (source.parent / path).resolve()
    else:
        target = source.resolve()

    fragment = unquote(parsed.fragment) if parsed.fragment else None
    return target, fragment


def heading_text(markdown: str) -> str:
    """Reduce common inline Markdown to the text used to form a heading ID."""

    markdown = re.sub(r"[ \t]+#+[ \t]*$", "", markdown.strip())
    markdown = MARKDOWN_LINK_TEXT.sub(lambda match: match.group(1), markdown)
    markdown = INLINE_CODE.sub(lambda match: match.group(1), markdown)
    markdown = HTML_TAG.sub("", markdown)
    markdown = html.unescape(markdown)
    markdown = re.sub(r"\\(.)", r"\1", markdown)
    markdown = re.sub(r"(?<!\w)_{1,3}(.+?)_{1,3}(?!\w)", r"\1", markdown)
    return markdown.replace("*", "").replace("~", "")


def heading_slug(markdown: str) -> str:
    """Return the basic GitHub/mdBook ID for one heading's Markdown text."""

    text = heading_text(markdown).lower()
    return "".join(
        character
        for character in (
            "-" if character.isspace() else character
            for character in text
            if character.isalnum() or character in "-_" or character.isspace()
        )
    )


def heading_anchors(markdown: str) -> set[str]:
    """Collect generated heading IDs, including mdBook's duplicate suffixes."""

    lines = FENCED_CODE.sub("", markdown).splitlines()
    headings: list[str] = []

    for index, line in enumerate(lines):
        atx = ATX_HEADING.match(line)
        if atx:
            headings.append(atx.group(1))
            continue

        if (
            line.strip()
            and index + 1 < len(lines)
            and SETEXT_UNDERLINE.match(lines[index + 1])
        ):
            headings.append(line.strip())

    anchors: set[str] = set()
    next_suffix: dict[str, int] = {}
    for heading in headings:
        base = heading_slug(heading)
        if not base:
            continue

        suffix = next_suffix.get(base, 0)
        candidate = base if suffix == 0 else f"{base}-{suffix}"
        while candidate in anchors:
            suffix += 1
            candidate = f"{base}-{suffix}"

        next_suffix[base] = suffix + 1
        anchors.add(candidate)

    return anchors


def markdown_heading_anchors(source: Path) -> set[str]:
    """Collect the generated heading IDs in one Markdown file."""

    return heading_anchors(source.read_text(encoding="utf-8"))


def self_test() -> None:
    """Exercise fragment resolution and the heading-slug cases we depend on."""

    source = Path("/tmp/docs/source.md")
    assert local_target(source, "#same") == (source.resolve(), "same")
    assert local_target(source, "other.md#cross") == (
        Path("/tmp/docs/other.md").resolve(),
        "cross",
    )
    assert local_target(source, "https://example.com/page#external") is None
    assert targets_from_markdown("[![CI](badge.svg)](LICENSE)") == [
        "LICENSE",
        "badge.svg",
    ]
    assert local_target(
        source,
        "https://github.com/joshrotenberg/redis-tower/blob/main/LICENSE-MIT",
    ) == (ROOT / "LICENSE-MIT", None)
    assert local_target(
        source,
        "https://github.com/joshrotenberg/redis-tower#license",
    ) == (ROOT / "README.md", "license")

    assert heading_slug("Cargo.toml") == "cargotoml"
    assert heading_slug("Arbitrary / not-yet-typed commands") == (
        "arbitrary--not-yet-typed-commands"
    )
    assert heading_slug("Use `GET` with *typed* commands") == (
        "use-get-with-typed-commands"
    )
    assert heading_anchors("# Repeat\n## Repeat\n```md\n# Hidden\n```\n") == {
        "repeat",
        "repeat-1",
    }


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        print("Documentation link checker self-tests passed.")
        return 0
    if sys.argv[1:]:
        print(f"usage: {Path(sys.argv[0]).name} [--self-test]", file=sys.stderr)
        return 2

    sources = sorted(
        {
            ROOT / "README.md",
            *ROOT.glob("crates/*/README.md"),
            *DOCS.rglob("*.md"),
        }
    )
    checked = 0
    checked_fragments = 0
    failures: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}

    for source in sources:
        for raw_target in markdown_targets(source):
            resolved = local_target(source, raw_target)
            if resolved is None:
                continue

            target, fragment = resolved
            checked += 1
            if not target.exists():
                failures.append(
                    f"{source.relative_to(ROOT)}: {raw_target!r} resolves to missing "
                    f"{target.relative_to(ROOT) if target.is_relative_to(ROOT) else target}"
                )
                continue

            if fragment and target.suffix.lower() in {".md", ".markdown"}:
                checked_fragments += 1
                anchors = anchor_cache.setdefault(target, markdown_heading_anchors(target))
                if fragment not in anchors:
                    display_target = (
                        target.relative_to(ROOT) if target.is_relative_to(ROOT) else target
                    )
                    failures.append(
                        f"{source.relative_to(ROOT)}: {raw_target!r} refers to missing "
                        f"heading #{fragment} in {display_target}"
                    )

    if failures:
        print("Broken local documentation links:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(
        f"Checked {checked} local links ({checked_fragments} heading fragments) "
        f"across {len(sources)} Markdown files."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
