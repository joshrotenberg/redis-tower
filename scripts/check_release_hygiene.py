#!/usr/bin/env python3
"""Audit publishable workspace crates before release."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from check_docs_links import markdown_targets


@dataclass(frozen=True)
class Package:
    name: str
    root: Path
    manifest: dict[str, object]


def load_toml(path: Path) -> dict[str, object]:
    with path.open("rb") as source:
        return tomllib.load(source)


def publishable_packages(root: Path) -> list[Package]:
    workspace = load_toml(root / "Cargo.toml")["workspace"]
    assert isinstance(workspace, dict)
    members = workspace.get("members")
    if not isinstance(members, list):
        raise ValueError("workspace.members must be an explicit list")
    packages = []
    for member in members:
        if not isinstance(member, str) or "*" in member:
            raise ValueError("workspace members must be literal paths")
        package_root = root / member
        manifest = load_toml(package_root / "Cargo.toml")
        package = manifest.get("package")
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        if not isinstance(name, str):
            raise ValueError(f"{member}: package.name must be a string")
        publish = package.get("publish", True)
        if publish is False or publish == [] or not name.startswith("redis-tower"):
            continue
        packages.append(Package(name=name, root=package_root, manifest=manifest))
    return sorted(packages, key=lambda package: package.name)


def inherited_or_present(package: dict[str, object], key: str) -> bool:
    value = package.get(key)
    return value is not None and value is not False


def audit_package(package: Package) -> list[str]:
    errors = []
    metadata = package.manifest.get("package")
    assert isinstance(metadata, dict)
    for key in ("version", "description", "license", "repository", "homepage", "readme", "rust-version"):
        if not inherited_or_present(metadata, key):
            errors.append(f"{package.name}: package.{key} is missing")

    docs = metadata.get("metadata")
    docs = docs.get("docs") if isinstance(docs, dict) else None
    docs = docs.get("rs") if isinstance(docs, dict) else None
    if not isinstance(docs, dict):
        errors.append(f"{package.name}: package.metadata.docs.rs is missing")
    else:
        if docs.get("all-features") is not True:
            errors.append(f"{package.name}: docs.rs must build all features")
        rustdoc_args = docs.get("rustdoc-args")
        if rustdoc_args != ["--cfg", "docsrs"]:
            errors.append(
                f"{package.name}: docs.rs rustdoc-args must be ['--cfg', 'docsrs']"
            )

    lib = package.root / "src" / "lib.rs"
    if not lib.is_file():
        errors.append(f"{package.name}: src/lib.rs is missing")
    elif "#![deny(missing_docs)]" not in lib.read_text():
        errors.append(f"{package.name}: src/lib.rs must deny missing_docs")

    changelog = package.root / "CHANGELOG.md"
    if not changelog.is_file():
        errors.append(f"{package.name}: CHANGELOG.md is missing")
    elif "## [Unreleased]" not in changelog.read_text():
        errors.append(f"{package.name}: CHANGELOG.md must contain an Unreleased section")
    errors.extend(audit_release_readme(package))
    return errors


def audit_release_readme(package: Package) -> list[str]:
    """Reject repository-relative links that break in packaged READMEs."""

    metadata = package.manifest.get("package")
    assert isinstance(metadata, dict)
    readme_value = metadata.get("readme")
    if not isinstance(readme_value, str):
        return []

    readme = (package.root / readme_value).resolve()
    if not readme.is_file():
        return [f"{package.name}: release README is missing: {readme_value}"]

    errors = []
    for target in markdown_targets(readme):
        parsed = urlsplit(target)
        if not parsed.scheme and not parsed.netloc and parsed.path:
            errors.append(
                f"{package.name}: release README target {target!r} is repository-relative; "
                "use a canonical URL"
            )
    return errors


def audit_repository_license(root: Path) -> list[str]:
    """Verify the workspace dual-license expression and source texts."""

    manifest = load_toml(root / "Cargo.toml")
    workspace = manifest.get("workspace")
    package = workspace.get("package") if isinstance(workspace, dict) else None
    errors = []
    if not isinstance(package, dict) or package.get("license") != "MIT OR Apache-2.0":
        errors.append("workspace package license must be 'MIT OR Apache-2.0'")
    for filename in ("LICENSE-MIT", "LICENSE-APACHE"):
        if not (root / filename).is_file():
            errors.append(f"repository is missing {filename}")
    return errors


def check_package_contents(root: Path, package: Package) -> list[str]:
    process = subprocess.run(
        [
            "cargo",
            "package",
            "-p",
            package.name,
            "--allow-dirty",
            "--no-verify",
            "--list",
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        detail = process.stderr.strip().splitlines()
        suffix = detail[-1] if detail else f"exit {process.returncode}"
        return [f"{package.name}: cargo package --list failed: {suffix}"]
    contents = set(process.stdout.splitlines())
    required = {"Cargo.toml", "CHANGELOG.md", "README.md", "src/lib.rs"}
    missing = sorted(required - contents)
    return [f"{package.name}: package is missing {path}" for path in missing]


def audit(root: Path, *, package_contents: bool) -> tuple[list[Package], list[str]]:
    packages = publishable_packages(root)
    if not packages:
        return [], ["no publishable redis-tower packages were found"]
    errors = audit_repository_license(root)
    for package in packages:
        errors.extend(audit_package(package))
        if package_contents:
            errors.extend(check_package_contents(root, package))
    return packages, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--check-package-contents", action="store_true")
    parser.add_argument("--list-packages", action="store_true")
    args = parser.parse_args(argv)
    try:
        packages, errors = audit(
            args.root.resolve(), package_contents=args.check_package_contents
        )
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        print(error, file=sys.stderr)
        return 2
    if args.list_packages:
        print("\n".join(package.name for package in packages))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    if not args.list_packages:
        print(f"Release hygiene passed for {len(packages)} publishable crates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
