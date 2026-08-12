#!/usr/bin/env python3
"""Refresh the generated inventory in the test-conformance documentation.

The test counts come from compiled libtest binaries rather than Rust source
syntax. This matters for macro-generated tests such as ``command_tests!`` and
for feature-gated integration targets. Workflow coverage and compatibility
targets are collected from the checked-in GitHub Actions configuration.
"""

from __future__ import annotations

import argparse
import difflib
import fnmatch
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


SCHEMA_VERSION = 1
DEFAULT_OUTPUT = Path("docs/TEST-CONFORMANCE.md")
BEGIN_MARKER = "<!-- BEGIN GENERATED TEST INVENTORY -->"
END_MARKER = "<!-- END GENERATED TEST INVENTORY -->"


@dataclass(frozen=True)
class Surface:
    package: str
    label: str


# This is a scope declaration, not an inventory snapshot. Counts, target
# names, ignored status, and workflow coverage are all discovered at runtime.
SURFACES = (
    Surface("redis-tower", "Standalone"),
    Surface("redis-tower-cluster", "Cluster"),
    Surface("redis-tower-sentinel", "Sentinel"),
    Surface("redis-tower-modules", "Modules"),
    Surface("redis-chaos-tests", "Fault injection"),
)
SCOREBOARD_PACKAGES = {
    "redis-tower",
    "redis-tower-cluster",
    "redis-tower-sentinel",
    "redis-tower-modules",
}
SURFACE_BY_PACKAGE = {surface.package: surface for surface in SURFACES}
SURFACE_ORDER = {surface.package: index for index, surface in enumerate(SURFACES)}

PARITY_PACKAGES = {
    "redis-tower",
    "redis-tower-cluster",
    "redis-tower-sentinel",
}
FAULT_NAME_FRAGMENTS = (
    "chaos",
    "circuit_breaker",
    "command_timeout",
    "connections_are_killed",
    "disconnect",
    "failover",
    "frozen",
    "killed_master",
    "partition",
    "pool_exhaustion",
    "reconnects_after_server_restart",
    "reshard",
    "sigkill",
    "toxiproxy",
)


@dataclass(frozen=True)
class TestBinary:
    package: str
    target: str
    tests: tuple[str, ...]
    ignored: tuple[str, ...]

    def __post_init__(self) -> None:
        tests = set(self.tests)
        ignored = set(self.ignored)
        if len(tests) != len(self.tests):
            raise ValueError(f"duplicate tests in {self.package}/{self.target}")
        if len(ignored) != len(self.ignored):
            raise ValueError(f"duplicate ignored tests in {self.package}/{self.target}")
        unknown = ignored - tests
        if unknown:
            raise ValueError(
                f"ignored listing for {self.package}/{self.target} contains "
                f"unknown tests: {', '.join(sorted(unknown))}"
            )

    @property
    def normal_count(self) -> int:
        return len(self.tests) - len(self.ignored)

    def to_dict(self) -> dict[str, Any]:
        return {
            "package": self.package,
            "target": self.target,
            "tests": list(self.tests),
            "ignored": list(self.ignored),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TestBinary:
        return cls(
            package=str(value["package"]),
            target=str(value["target"]),
            tests=tuple(sorted(str(test) for test in value["tests"])),
            ignored=tuple(sorted(str(test) for test in value["ignored"])),
        )


@dataclass(frozen=True)
class WorkflowCommand:
    path: str
    cadences: tuple[str, ...]
    command: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "cadences": list(self.cadences),
            "command": self.command,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkflowCommand:
        return cls(
            path=str(value["path"]),
            cadences=tuple(sorted(str(item) for item in value["cadences"])),
            command=str(value["command"]),
        )


@dataclass(frozen=True)
class CompatibilityTarget:
    cadence: str
    name: str
    implementation: str

    def to_dict(self) -> dict[str, str]:
        return {
            "cadence": self.cadence,
            "name": self.name,
            "implementation": self.implementation,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> CompatibilityTarget:
        return cls(
            cadence=str(value["cadence"]),
            name=str(value["name"]),
            implementation=str(value["implementation"]),
        )


@dataclass(frozen=True)
class Inventory:
    binaries: tuple[TestBinary, ...]
    workflows: tuple[WorkflowCommand, ...]
    compatibility: tuple[CompatibilityTarget, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "binaries": [binary.to_dict() for binary in self.binaries],
            "workflows": [workflow.to_dict() for workflow in self.workflows],
            "compatibility": [target.to_dict() for target in self.compatibility],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Inventory:
        schema_version = value.get("schema_version")
        if schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported inventory schema {schema_version!r}; "
                f"expected {SCHEMA_VERSION}"
            )
        binaries = tuple(TestBinary.from_dict(item) for item in value["binaries"])
        workflows = tuple(
            WorkflowCommand.from_dict(item) for item in value.get("workflows", [])
        )
        compatibility = tuple(
            CompatibilityTarget.from_dict(item)
            for item in value.get("compatibility", [])
        )
        return cls(
            binaries=tuple(sorted(binaries, key=binary_sort_key)),
            workflows=tuple(
                sorted(workflows, key=lambda item: (item.path, item.command))
            ),
            compatibility=compatibility,
        )


@dataclass(frozen=True)
class ScheduleCoverage:
    pull_request: int
    scheduled: int
    covered: int


@dataclass(frozen=True)
class ParityVariant:
    package: str
    target: str
    prefix: str
    cases: tuple[str, ...]


def binary_sort_key(binary: TestBinary) -> tuple[int, str, str]:
    return (
        SURFACE_ORDER.get(binary.package, len(SURFACE_ORDER)),
        binary.package,
        binary.target,
    )


def run_command(
    command: Sequence[str], *, cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise OSError(f"could not run {command[0]}: {error}") from error


def command_failure(result: subprocess.CompletedProcess[str], action: str) -> OSError:
    diagnostics: list[str] = []
    for line in result.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        rendered = message.get("message", {}).get("rendered")
        if rendered:
            diagnostics.append(rendered.rstrip())
    diagnostics.extend(line for line in result.stderr.splitlines() if line.strip())
    detail = "\n".join(diagnostics[-30:]) or f"exit status {result.returncode}"
    return OSError(f"{action} failed:\n{detail}")


def parse_test_listing(output: str) -> tuple[str, ...]:
    tests: list[str] = []
    for line in output.splitlines():
        name, separator, kind = line.rpartition(": ")
        if separator and kind == "test":
            tests.append(name)
    if len(tests) != len(set(tests)):
        raise ValueError("libtest returned duplicate test names")
    return tuple(sorted(tests))


def list_binary_tests(executable: Path, *, cwd: Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    all_result = run_command(
        [str(executable), "--list", "--format", "terse"], cwd=cwd
    )
    if all_result.returncode:
        raise command_failure(all_result, f"listing tests in {executable}")
    ignored_result = run_command(
        [str(executable), "--ignored", "--list", "--format", "terse"], cwd=cwd
    )
    if ignored_result.returncode:
        raise command_failure(
            ignored_result, f"listing ignored tests in {executable}"
        )
    tests = parse_test_listing(all_result.stdout)
    ignored = parse_test_listing(ignored_result.stdout)
    unknown = set(ignored) - set(tests)
    if unknown:
        raise ValueError(
            f"{executable} reported ignored tests absent from its full listing: "
            + ", ".join(sorted(unknown))
        )
    return tests, ignored


def collect_compiled_binaries(repo_root: Path, cargo: str) -> tuple[TestBinary, ...]:
    metadata_result = run_command(
        [cargo, "metadata", "--format-version", "1", "--no-deps"], cwd=repo_root
    )
    if metadata_result.returncode:
        raise command_failure(metadata_result, "cargo metadata")
    try:
        metadata = json.loads(metadata_result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"cargo metadata returned invalid JSON: {error}") from error

    package_names = {
        package["id"]: package["name"]
        for package in metadata["packages"]
        if package["name"] in SURFACE_BY_PACKAGE
    }
    missing = sorted(set(SURFACE_BY_PACKAGE) - set(package_names.values()))
    if missing:
        raise ValueError("workspace is missing conformance packages: " + ", ".join(missing))

    command = [cargo, "test", "--tests", "--all-features", "--no-run"]
    for surface in SURFACES:
        command.extend(["-p", surface.package])
    command.append("--message-format=json")
    env = dict(os.environ)
    env["CARGO_TERM_COLOR"] = "never"
    compile_result = run_command(command, cwd=repo_root, env=env)
    if compile_result.returncode:
        raise command_failure(compile_result, "compiling integration tests")

    artifacts: dict[tuple[str, str], Path] = {}
    for line in compile_result.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact":
            continue
        package = package_names.get(message.get("package_id"))
        target = message.get("target", {})
        if package is None or "test" not in target.get("kind", []):
            continue
        executable = message.get("executable")
        if executable is None:
            raise ValueError(f"no executable emitted for {package}/{target.get('name')}")
        key = (package, str(target["name"]))
        path = Path(executable)
        previous = artifacts.setdefault(key, path)
        if previous != path:
            raise ValueError(f"multiple executables emitted for {package}/{target['name']}")

    if not artifacts:
        raise ValueError("cargo did not emit any selected integration test binaries")

    binaries: list[TestBinary] = []
    for (package, target), executable in sorted(
        artifacts.items(),
        key=lambda item: (
            SURFACE_ORDER[item[0][0]],
            item[0][0],
            item[0][1],
        ),
    ):
        tests, ignored = list_binary_tests(executable, cwd=repo_root)
        binaries.append(TestBinary(package, target, tests, ignored))
    return tuple(binaries)


def workflow_cadences(source: str) -> tuple[str, ...]:
    cadences: list[str] = []
    if re.search(r"(?m)^\s*pull_request(?:_target)?:", source):
        cadences.append("pull request")
    if re.search(r"(?m)^\s*schedule:", source):
        cadences.append("scheduled")
    if re.search(r"(?m)^\s*workflow_dispatch:", source):
        cadences.append("manual")
    return tuple(cadences)


def extract_run_blocks(source: str) -> tuple[str, ...]:
    lines = source.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        match = re.match(r"^(\s*)(?:-\s+)?run:\s*(.*)$", lines[index])
        if match is None:
            index += 1
            continue
        indentation = len(match.group(1))
        value = match.group(2).strip()
        block_lines: list[str] = []
        if value not in {"|", "|-", "|+", ">", ">-", ">+"}:
            block_lines.append(value)
            index += 1
        else:
            index += 1
            while index < len(lines):
                line = lines[index]
                if line.strip() and len(line) - len(line.lstrip()) <= indentation:
                    break
                if line.strip():
                    block_lines.append(line.strip())
                index += 1
        block = "\n".join(block_lines).strip()
        if block:
            blocks.append(block)
    return tuple(blocks)


def split_cargo_test_commands(run_block: str) -> tuple[str, ...]:
    matches = list(re.finditer(r"\bcargo\s+test\b", run_block))
    commands: list[str] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(run_block)
        command = " ".join(run_block[match.start() : end].split())
        if command:
            commands.append(command)
    return tuple(commands)


def collect_workflows(repo_root: Path) -> tuple[WorkflowCommand, ...]:
    workflows: list[WorkflowCommand] = []
    workflow_dir = repo_root / ".github" / "workflows"
    for path in sorted((*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml"))):
        source = path.read_text(encoding="utf-8")
        cadences = workflow_cadences(source)
        relative = path.relative_to(repo_root).as_posix()
        for run_block in extract_run_blocks(source):
            for command in split_cargo_test_commands(run_block):
                workflows.append(WorkflowCommand(relative, cadences, command))
    return tuple(sorted(workflows, key=lambda item: (item.path, item.command)))


def parse_inline_versions(source: str) -> tuple[str, ...]:
    versions: list[str] = []
    for match in re.finditer(r"(?m)^\s*redis:\s*\[([^\]]+)]\s*$", source):
        versions.extend(re.findall(r"[\"']([^\"']+)[\"']", match.group(1)))
    return tuple(dict.fromkeys(versions))


def extract_job(source: str, job_name: str) -> str:
    lines = source.splitlines()
    start: int | None = None
    indentation: int | None = None
    for index, line in enumerate(lines):
        match = re.match(r"^(\s+)([A-Za-z0-9_-]+):\s*$", line)
        if match and match.group(2) == job_name:
            start = index
            indentation = len(match.group(1))
            break
    if start is None or indentation is None:
        return ""
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        match = re.match(r"^(\s+)([A-Za-z0-9_-]+):\s*$", line)
        if match and len(match.group(1)) == indentation:
            end = index
            break
    return "\n".join(lines[start:end])


def parse_matrix_includes(job_source: str) -> tuple[tuple[str, str], ...]:
    entries: list[tuple[str, str]] = []
    include_indent: int | None = None
    current_name: str | None = None
    current_image: str | None = None
    for line in job_source.splitlines():
        if include_indent is None:
            include_match = re.match(r"^(\s*)include:\s*$", line)
            if include_match is not None:
                include_indent = len(include_match.group(1))
            continue
        if line.strip() and len(line) - len(line.lstrip()) <= include_indent:
            break
        name_match = re.match(r"^\s*-\s+name:\s*[\"']?([^\"'#]+?)[\"']?\s*$", line)
        if name_match:
            if current_name is not None and current_image is not None:
                entries.append((current_name, current_image))
            current_name = name_match.group(1).strip()
            current_image = None
            continue
        image_match = re.match(r"^\s+image:\s*[\"']?([^\"'#]+?)[\"']?\s*$", line)
        if image_match and current_name is not None:
            current_image = image_match.group(1).strip()
    if current_name is not None and current_image is not None:
        entries.append((current_name, current_image))
    return tuple(entries)


def collect_compatibility(repo_root: Path) -> tuple[CompatibilityTarget, ...]:
    targets: list[CompatibilityTarget] = []
    ci_path = repo_root / ".github" / "workflows" / "ci.yml"
    if ci_path.exists():
        for version in parse_inline_versions(ci_path.read_text(encoding="utf-8")):
            targets.append(
                CompatibilityTarget("Pull request", f"Redis {version}", "source build")
            )

    nightly_path = repo_root / ".github" / "workflows" / "nightly.yml"
    if nightly_path.exists():
        source = nightly_path.read_text(encoding="utf-8")
        for name, image in parse_matrix_includes(extract_job(source, "version-matrix")):
            targets.append(CompatibilityTarget("Nightly", name, image))
    return tuple(targets)


def collect_inventory(repo_root: Path, cargo: str) -> Inventory:
    return Inventory(
        binaries=collect_compiled_binaries(repo_root, cargo),
        workflows=collect_workflows(repo_root),
        compatibility=collect_compatibility(repo_root),
    )


def load_inventory(path: Path) -> Inventory:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise OSError(f"inventory file does not exist: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid inventory JSON in {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"inventory root in {path} must be an object")
    return Inventory.from_dict(value)


def write_inventory(path: Path, inventory: Inventory) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(inventory.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def command_selects_binary(
    workflow: WorkflowCommand, binary: TestBinary
) -> tuple[bool, bool]:
    command = workflow.command
    escaped_package = re.escape(binary.package)
    package_selected = bool(
        re.search(
            rf"(?:^|\s)(?:-p|--package)(?:=|\s+)[\"']?{escaped_package}[\"']?(?=\s|$)",
            command,
        )
        or re.search(r"(?:^|\s)--workspace(?=\s|$)", command)
    )
    if not package_selected:
        return False, False

    if re.search(r"(?:^|\s)--(?:lib|doc|benches|bins?|examples?)(?=\s|=|$)", command):
        return False, False

    target_patterns = re.findall(
        r"(?:^|\s)--test(?:=|\s+)[\"']?([^\s\"']+)[\"']?", command
    )
    if target_patterns and not any(
        fnmatch.fnmatchcase(binary.target, pattern) for pattern in target_patterns
    ):
        return False, False

    include_ignored = bool(
        re.search(r"(?:^|\s)--(?:ignored|include-ignored)(?=\s|$)", command)
    )
    only_ignored = bool(re.search(r"(?:^|\s)--ignored(?=\s|$)", command))
    include_normal = not only_ignored
    return include_normal, include_ignored


def binary_schedule(binary: TestBinary, workflows: Iterable[WorkflowCommand]) -> ScheduleCoverage:
    pull_request_normal = False
    pull_request_ignored = False
    scheduled_normal = False
    scheduled_ignored = False
    any_normal = False
    any_ignored = False
    for workflow in workflows:
        normal, ignored = command_selects_binary(workflow, binary)
        if not (normal or ignored):
            continue
        if "pull request" in workflow.cadences:
            pull_request_normal |= normal
            pull_request_ignored |= ignored
        if "scheduled" in workflow.cadences:
            scheduled_normal |= normal
            scheduled_ignored |= ignored
        any_normal |= normal
        any_ignored |= ignored

    pull_request = (
        binary.normal_count * pull_request_normal
        + len(binary.ignored) * pull_request_ignored
    )
    scheduled = (
        binary.normal_count * scheduled_normal
        + len(binary.ignored) * scheduled_ignored
    )
    covered = binary.normal_count * any_normal + len(binary.ignored) * any_ignored
    return ScheduleCoverage(pull_request, scheduled, covered)


def derive_parity_variants(binaries: Iterable[TestBinary]) -> tuple[ParityVariant, ...]:
    variants: list[ParityVariant] = []
    for binary in binaries:
        if binary.package not in PARITY_PACKAGES:
            continue
        grouped: dict[str, set[str]] = {}
        for test in binary.tests:
            segments = test.split("::")
            try:
                command_index = next(
                    index for index, segment in enumerate(segments) if segment.startswith("cmd_")
                )
            except StopIteration:
                continue
            prefix = "::".join(segments[:command_index]) or "root expansion"
            case = "::".join(segments[command_index:])
            grouped.setdefault(prefix, set()).add(case)
        for prefix, cases in grouped.items():
            variants.append(
                ParityVariant(
                    binary.package,
                    binary.target,
                    prefix,
                    tuple(sorted(cases)),
                )
            )
    return tuple(
        sorted(
            variants,
            key=lambda variant: (
                SURFACE_ORDER.get(variant.package, len(SURFACE_ORDER)),
                variant.target,
                variant.prefix,
            ),
        )
    )


def fault_tests(binaries: Iterable[TestBinary]) -> tuple[tuple[TestBinary, str], ...]:
    selected: list[tuple[TestBinary, str]] = []
    for binary in binaries:
        for test in binary.tests:
            normalized = test.lower()
            if any(fragment in normalized for fragment in FAULT_NAME_FRAGMENTS):
                selected.append((binary, test))
    return tuple(selected)


def markdown_escape(value: str) -> str:
    return value.replace("|", "\\|")


def format_cadence_for_test(
    binary: TestBinary, test: str, workflows: Iterable[WorkflowCommand]
) -> str:
    is_ignored = test in set(binary.ignored)
    cadences: set[str] = set()
    for workflow in workflows:
        normal, ignored = command_selects_binary(workflow, binary)
        if (is_ignored and ignored) or (not is_ignored and normal):
            cadences.update(workflow.cadences)
    labels = []
    if "pull request" in cadences:
        labels.append("pull request")
    if "scheduled" in cadences:
        labels.append("nightly/scheduled")
    if "manual" in cadences and not labels:
        labels.append("manual")
    return ", ".join(labels) or "no workflow selector"


def render_report(inventory: Inventory) -> str:
    all_binaries = tuple(sorted(inventory.binaries, key=binary_sort_key))
    binaries = tuple(
        binary for binary in all_binaries if binary.package in SCOREBOARD_PACKAGES
    )
    total = sum(len(binary.tests) for binary in binaries)
    ignored = sum(len(binary.ignored) for binary in binaries)
    pull_request = 0
    scheduled = 0
    covered = 0
    rows: list[str] = []
    for surface in SURFACES:
        selected = [binary for binary in binaries if binary.package == surface.package]
        if not selected:
            continue
        surface_total = sum(len(binary.tests) for binary in selected)
        surface_ignored = sum(len(binary.ignored) for binary in selected)
        surface_pull_request = 0
        surface_scheduled = 0
        surface_covered = 0
        for binary in selected:
            coverage = binary_schedule(binary, inventory.workflows)
            surface_pull_request += coverage.pull_request
            surface_scheduled += coverage.scheduled
            surface_covered += coverage.covered
        pull_request += surface_pull_request
        scheduled += surface_scheduled
        covered += surface_covered
        rows.append(
            f"| {surface.label} | `{surface.package}` | {len(selected)} | "
            f"{surface_total} | {surface_ignored} | {surface_pull_request} | "
            f"{surface_scheduled} | {surface_total - surface_covered} |"
        )

    lines = [
        "<!-- Generated by scripts/generate_test_conformance.py; do not edit. -->",
        "",
        "### Compiled integration inventory",
        "",
        f"The scoreboard-scoped packages compile **{total} integration tests** across "
        f"**{len(binaries)} test binaries**. **{ignored}** tests are marked "
        "`#[ignore]` because they need explicit infrastructure; workflow selectors, "
        "rather than the annotation alone, determine whether they run.",
        "",
        "| Surface | Package | Binaries | Compiled | `#[ignore]` | Pull request | Scheduled | No workflow selector |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
        *rows,
        f"| **Total** |  | **{len(binaries)}** | **{total}** | **{ignored}** | "
        f"**{pull_request}** | **{scheduled}** | **{total - covered}** |",
        "",
        "Counts are unique compiled tests. A test selected by both pull-request and "
        "scheduled workflows appears in both cadence columns, but only once in the "
        "compiled total.",
        "",
        "### Cross-backend command parity",
        "",
    ]

    variants = derive_parity_variants(binaries)
    if variants:
        parity_total = sum(len(variant.cases) for variant in variants)
        corpora = {variant.cases for variant in variants}
        if len(corpora) == 1:
            corpus_size = len(next(iter(corpora)))
            lines.append(
                f"The shared command corpus produces **{parity_total} compiled parity "
                f"tests**: **{corpus_size} test cases × {len(variants)} "
                "client/topology expansions**."
            )
        else:
            distinct_cases = len({case for variant in variants for case in variant.cases})
            lines.append(
                f"The shared command suites produce **{parity_total} compiled parity "
                f"tests** across **{len(variants)} variants** and **{distinct_cases} "
                "distinct command cases**. Variant corpora currently differ."
            )
        lines.extend(
            [
                "",
                "| Surface | Test binary / variant | Compiled cases |",
                "|---|---|---:|",
            ]
        )
        for variant in variants:
            surface = SURFACE_BY_PACKAGE[variant.package].label
            variant_name = f"{variant.target} / {variant.prefix}"
            lines.append(
                f"| {surface} | `{markdown_escape(variant_name)}` | {len(variant.cases)} |"
            )
    else:
        lines.append("No compiled `command_tests!` expansions were detected.")

    lines.extend(["", "### Server compatibility matrix", ""])
    if inventory.compatibility:
        lines.extend(
            [
                "| Cadence | Target | Implementation source |",
                "|---|---|---|",
            ]
        )
        for target in inventory.compatibility:
            lines.append(
                f"| {target.cadence} | {markdown_escape(target.name)} | "
                f"`{markdown_escape(target.implementation)}` |"
            )
    else:
        lines.append("No Redis or Valkey compatibility targets were detected.")

    lines.extend(["", "### Fault and destructive-recovery inventory", ""])
    faults = fault_tests(all_binaries)
    if faults:
        lines.extend(
            [
                "| Surface | Test binary | Compiled test | Default | Workflow cadence |",
                "|---|---|---|---|---|",
            ]
        )
        for binary, test in faults:
            surface = SURFACE_BY_PACKAGE[binary.package].label
            default = "ignored" if test in set(binary.ignored) else "normal"
            cadence = format_cadence_for_test(binary, test, inventory.workflows)
            lines.append(
                f"| {surface} | `{markdown_escape(binary.target)}` | "
                f"`{markdown_escape(test)}` | {default} | {cadence} |"
            )
    else:
        lines.append("No compiled fault or destructive-recovery tests were detected.")

    lines.extend(
        [
            "",
            "### Reproducibility",
            "",
            "The generator compiles only the client-facing integration-test packages "
            "with all features and `--no-run`, asks each emitted libtest binary for its "
            "full and ignored listings, and inspects checked-in workflow `cargo test` "
            "selectors. Regenerate or verify the marked section with:",
            "",
            "```bash",
            "python3 scripts/generate_test_conformance.py",
            "python3 scripts/generate_test_conformance.py --check",
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def replace_generated_section(document: str, generated: str) -> str:
    if document.count(BEGIN_MARKER) != 1 or document.count(END_MARKER) != 1:
        raise ValueError(
            "output must contain exactly one generated inventory marker pair: "
            f"{BEGIN_MARKER!r} and {END_MARKER!r}"
        )
    begin = document.index(BEGIN_MARKER) + len(BEGIN_MARKER)
    end = document.index(END_MARKER)
    if begin >= end:
        raise ValueError("generated inventory markers are out of order")
    return document[:begin] + "\n" + generated.rstrip() + "\n" + document[end:]


def update_or_check(output: Path, generated: str, *, check: bool) -> int:
    try:
        current = output.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise OSError(f"output file does not exist: {output}") from error
    expected = replace_generated_section(current, generated)
    if current == expected:
        print(f"{output} test inventory is current")
        return 0
    if check:
        diff = difflib.unified_diff(
            current.splitlines(),
            expected.splitlines(),
            fromfile=str(output),
            tofile="generated",
            lineterm="",
        )
        print("\n".join(diff))
        print(
            f"\nerror: {output} test inventory is stale; run "
            "python3 scripts/generate_test_conformance.py",
            file=sys.stderr,
        )
        return 1
    output.write_text(expected, encoding="utf-8")
    print(f"updated generated test inventory in {output}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if the marked section is stale")
    parser.add_argument(
        "--print-section",
        action="store_true",
        help="Print generated Markdown without reading or changing the output file",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--cargo", default="cargo", help="Cargo executable to use")
    parser.add_argument(
        "--inventory-file",
        type=Path,
        help="Use a captured JSON inventory instead of compiling test binaries",
    )
    parser.add_argument(
        "--write-inventory",
        type=Path,
        help="Write the live inventory as deterministic JSON for review or tests",
    )
    args = parser.parse_args(argv)

    if args.check and args.print_section:
        parser.error("--check and --print-section cannot be combined")
    if args.inventory_file and args.write_inventory:
        parser.error("--inventory-file and --write-inventory cannot be combined")

    try:
        if args.inventory_file:
            inventory = load_inventory(args.inventory_file)
        else:
            inventory = collect_inventory(args.repo_root.resolve(), args.cargo)
        if args.write_inventory:
            write_inventory(args.write_inventory, inventory)
        generated = render_report(inventory)
        if args.print_section:
            print(generated, end="")
            return 0
        return update_or_check(args.output, generated, check=args.check)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
