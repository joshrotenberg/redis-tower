#!/usr/bin/env python3
"""Retain codec CI evidence; this does not decide the performance gate."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from sanitize_metadata import sanitize

LABELS = {"main", "candidate", "main-confirm", "candidate-confirm"}
SETTINGS = ["--bench", "--warm-up-time", "1", "--measurement-time", "2",
            "--sample-size", "50", "--save-baseline"]
ENVIRONMENT = {
    "CARGO_BUILD_JOBS", "CARGO_INCREMENTAL", "CARGO_TARGET_DIR",
    "CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS", "RUSTDOCFLAGS", "RUSTUP_TOOLCHAIN",
    "CC", "CXX", "AR", "CFLAGS", "CPPFLAGS", "LDFLAGS", "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH", "CARGO_MANIFEST_DIR", "CRITERION_HOME",
    "GITHUB_REPOSITORY", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_JOB",
    "GITHUB_WORKFLOW_SHA", "GITHUB_SHA", "RUNNER_NAME", "RUNNER_OS",
    "RUNNER_ARCH", "RUNNER_ENVIRONMENT", "ImageOS", "ImageVersion",
}
ENVIRONMENT |= {
    "CARGO_PROFILE_BENCH_" + suffix
    for suffix in ("OPT_LEVEL", "DEBUG", "DEBUG_ASSERTIONS", "OVERFLOW_CHECKS",
                   "LTO", "PANIC", "INCREMENTAL", "CODEGEN_UNITS", "RPATH", "STRIP")
}
RECORD_FIELDS = {
    "schema_version", "label", "expected_source", "source_sha", "source_tree",
    "harness_source", "harness_files", "started_at", "finished_at",
    "duration_seconds", "environment", "tools", "commands", "benchmark_cwd",
    "criterion_directory", "expected_cases", "exit_status", "error",
    "lock_before", "lock_after", "executable_before", "executable_after",
    "host_before", "host_after", "lock_resolution",
}


class EvidenceError(ValueError):
    """Evidence is incomplete or has changed."""


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def digest(path: Path) -> dict:
    return {"bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def environment(values=None) -> dict:
    values = os.environ if values is None else values
    return {key: values[key] for key in sorted(ENVIRONMENT) if key in values}


def stamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def host() -> dict:
    result = {
        "captured_at": stamp(), "system": platform.system(),
        "kernel": platform.release(), "architecture": platform.machine(),
        "cpu_count": os.cpu_count(),
    }
    for key, getter in (("load", os.getloadavg),
                        ("affinity", lambda: sorted(os.sched_getaffinity(0)))):
        try:
            result[key] = getter()
        except (AttributeError, OSError) as error:
            result[key] = {"unavailable": str(error)}
    for name in ("cpuinfo", "meminfo", "loadavg", "stat"):
        try:
            result[name] = Path("/proc", name).read_text()
        except OSError as error:
            result[name] = {"unavailable": str(error)}
    for name in ("scaling_governor", "scaling_cur_freq"):
        try:
            result[name] = Path("/sys/devices/system/cpu/cpu0/cpufreq", name).read_text()
        except OSError as error:
            result[name] = {"unavailable": str(error)}
    try:
        process = subprocess.run(
            ["ps", "-e", "-o", "comm=,pcpu=,pmem="], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        result["processes"] = {"exit_status": process.returncode,
                               "stdout": process.stdout, "stderr": process.stderr}
    except OSError as error:
        result["processes"] = {"unavailable": str(error)}
    return result


def command(argv: list[str], directory: Path, prefix: Path, record: dict,
            *, env=None, required=True, retain_stdout=True, redact=False):
    """Record argv/cwd and both streams, including on a nonzero exit."""
    record["commands"].append({"capture": prefix.name, "argv": argv, "cwd": str(directory),
                               "environment": environment(env)})
    try:
        completed = subprocess.run(
            argv, cwd=directory, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
    except OSError as error:
        prefix.with_suffix(".stderr").write_text((redact_diagnostic(str(error)) if redact else str(error)) + "\n")
        raise EvidenceError(f"cannot execute {argv[0]}") from error
    stdout = completed.stdout if retain_stdout else "[metadata JSON omitted; see dependencies.json]\n"
    stderr = redact_diagnostic(completed.stderr) if redact else completed.stderr
    prefix.with_suffix(".stdout").write_text(stdout)
    prefix.with_suffix(".stderr").write_text(stderr)
    record["commands"][-1]["exit_status"] = completed.returncode
    if required and completed.returncode:
        raise EvidenceError(f"{argv[0]} exited {completed.returncode}; see {prefix.name} logs")
    return completed


def redact_diagnostic(text: str) -> str:
    """Keep resolution errors without URL credentials, queries or local paths."""
    text = re.sub(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s\"'<>]+", "<url>", text)
    return re.sub(r"(?<![\w:])(?:/[A-Za-z0-9_.~+-]+){2,}[^\s\"'<>]*", "<path>", text)


def lock_diff(before: bytes, after: bytes) -> str:
    return "".join(difflib.unified_diff(
        before.decode("utf-8").splitlines(keepends=True),
        after.decode("utf-8").splitlines(keepends=True),
        fromfile="Cargo.lock.before", tofile="Cargo.lock.after"))


def resolve_dependencies(root: Path, subject: Path, record: dict) -> dict:
    """Let Cargo minimally resolve this subject; attest the exact lock change."""
    lock = root / "Cargo.lock"
    before = lock.read_bytes() if lock.exists() else b""
    before_identity = digest(lock) if lock.exists() else None
    if before_identity is not None:
        (subject / "Cargo.lock.before").write_bytes(before)
    argv = ["cargo", "metadata", "--format-version", "1"]
    resolution = {"schema_version": 1, "source_sha": record["source_sha"],
                  "argv": argv, "exit_status": None, "classification": "failed",
                  "before": before_identity, "after": None, "raw_diff_sha256": None}
    record["lock_resolution"] = resolution
    try:
        # Unlike --locked, this preserves cargo bench's normal behavior for a
        # manifest change. Cargo leaves compatible locked versions alone.
        completed = command(argv, root, subject / "resolution", record,
                            required=False, retain_stdout=False, redact=True)
        resolution["exit_status"] = completed.returncode
        if completed.returncode:
            raise EvidenceError(f"Cargo resolution exited {completed.returncode}; see resolution.stderr")
        metadata = json.loads(completed.stdout)
        dependency_view = sanitize(metadata)
        # Plain metadata resolves the workspace view, not selected bench units.
        # Every actual unit's compiler-artifact features remain in build.stdout.
        dependency_view["view"] = "workspace-resolved"
        write_json(subject / "dependencies.json", dependency_view)
        if not lock.exists():
            raise EvidenceError("Cargo resolution did not produce a lockfile")
        resolution["classification"] = (
            "created" if before_identity is None else
            "reused" if before_identity == digest(lock) else "updated")
        return metadata
    finally:
        after = lock.read_bytes() if lock.exists() else b""
        if lock.exists():
            resolution["after"] = digest(lock)
            (subject / "Cargo.lock.after").write_bytes(after)
            (subject / "Cargo.lock").write_bytes(after)
        difference = lock_diff(before, after)
        resolution["raw_diff_sha256"] = hashlib.sha256(difference.encode()).hexdigest()
        (subject / "lock-resolution.diff").write_text(redact_diagnostic(difference))
        write_json(subject / "lock-resolution.json", resolution)


def capture_raw(criterion: Path, destination: Path, label: str) -> None:
    if not criterion.exists():
        return
    for saved in sorted(criterion.rglob(label)):
        if not saved.is_dir():
            continue
        if saved.is_symlink() or any(path.is_symlink() for path in saved.rglob("*")):
            raise EvidenceError("Criterion evidence contains a symlink")
        target = destination / saved.relative_to(criterion)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(saved, target)


def measure(output: Path, label: str, expected_source: str,
            harness_source: str, workflow: Path) -> int:
    if label not in LABELS or not all(re.fullmatch(r"[0-9a-f]{40}", value)
                                     for value in (expected_source, harness_source)):
        raise EvidenceError("invalid label or source revision")
    root = Path.cwd().resolve()
    subject = output.resolve() / label
    subject.mkdir(parents=True, exist_ok=False)
    criterion = root / "target" / "criterion"
    started = time.monotonic()
    record = dict.fromkeys(RECORD_FIELDS)
    record.update(schema_version=1, label=label, expected_source=expected_source,
                  harness_source=harness_source, started_at=stamp(),
                  commands=[], expected_cases=[], environment=environment())
    try:
        record["harness_files"] = {
            "codec_evidence.py": digest(Path(__file__)),
            "sanitize_metadata.py": digest(Path(__file__).with_name("sanitize_metadata.py")),
            "ci.yml": digest(workflow),
        }
        for name, expression in (("source_sha", "HEAD"), ("source_tree", "HEAD^{tree}")):
            record[name] = command(["git", "rev-parse", expression], root,
                                   subject / name, record).stdout.strip()
        if record["source_sha"] != expected_source:
            raise EvidenceError("checked-out source differs from expected source")
        tracked = command(["git", "status", "--porcelain", "--untracked-files=no"],
                          root, subject / "tracked-status", record).stdout
        if tracked:
            raise EvidenceError("tracked source is dirty")
        record["tools"] = {}
        for tool in ("rustc", "cargo"):
            record["tools"][tool] = command([tool, "-vV"], root,
                                           subject / tool, record).stdout.strip()
        lock = root / "Cargo.lock"
        raw_metadata = resolve_dependencies(root, subject, record)
        record["lock_before"] = digest(lock)
        package = next(package for package in raw_metadata["packages"]
                       if package["name"] == "redis-tower-protocol"
                       and package["id"] in raw_metadata["workspace_members"])
        package_root = Path(package["manifest_path"]).parent
        record["benchmark_cwd"] = str(package_root)
        record["criterion_directory"] = str(criterion)
        build = command(
            ["cargo", "bench", "--locked", "-p", "redis-tower-protocol",
             "--bench", "codec", "--no-run", "--message-format=json", "-vv"],
            root, subject / "build", record,
        )
        artifacts = []
        for line in build.stdout.splitlines():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (message.get("reason") == "compiler-artifact"
                    and message.get("target", {}).get("name") == "codec"
                    and "bench" in message.get("target", {}).get("kind", [])
                    and message.get("executable")):
                artifacts.append(message)
        if len(artifacts) != 1:
            raise EvidenceError("build did not identify exactly one codec executable")
        write_json(subject / "compiler-artifact.json", artifacts[0])
        executable = Path(artifacts[0]["executable"])
        record["executable_before"] = digest(executable)
        saved = subject / "codec-executable"
        shutil.copy2(executable, saved)
        if digest(saved) != record["executable_before"]:
            raise EvidenceError("saved executable differs from the build output")
        measured_env = os.environ.copy()
        # Cargo runs a benchmark from its package root, and supplies --bench.
        # CRITERION_HOME pins the original workspace target/criterion location.
        measured_env["CRITERION_HOME"] = str(criterion)
        measured_env["CARGO_MANIFEST_DIR"] = str(package_root)
        listed = command([str(saved), "--list"], package_root,
                         subject / "case-list", record, env=measured_env)
        cases = [line.removesuffix(": benchmark") for line in listed.stdout.splitlines()
                 if line.endswith(": benchmark")]
        if not cases or len(cases) != len(set(cases)):
            raise EvidenceError("executable did not list unique benchmark cases")
        record["expected_cases"] = cases
        # Remove only this label's old saved baselines. Cache/stale output cannot
        # fill a missing measurement; other labels are needed by the comparator.
        if criterion.exists():
            for old in sorted(criterion.rglob(label)):
                if old.is_symlink():
                    raise EvidenceError("stale baseline is a symlink")
                if old.is_dir() and any((old / name).is_file() for name in
                                        ("benchmark.json", "sample.json", "estimates.json")):
                    shutil.rmtree(old)
        record["host_before"] = host()
        command(["ldd", str(saved)], package_root, subject / "dynamic-libraries",
                record, required=False)
        # One measured invocation, of the exact retained executable bytes.
        run = command([str(saved), *SETTINGS, label], package_root,
                      subject / "benchmark", record, env=measured_env, required=False)
        record["exit_status"] = run.returncode
        record["executable_after"] = digest(saved)
        record["lock_after"] = digest(lock)
        if record["executable_before"] != record["executable_after"]:
            raise EvidenceError("measured executable changed")
        if record["lock_before"] != record["lock_after"]:
            raise EvidenceError("resolved lock changed")
    except (EvidenceError, OSError, ValueError, KeyError, StopIteration) as error:
        record["error"] = str(error)
    finally:
        record["host_after"] = host()
        record["finished_at"] = stamp()
        record["duration_seconds"] = time.monotonic() - started
        try:
            capture_raw(criterion, subject / "criterion", label)
        except (OSError, EvidenceError) as error:
            record["error"] = str(error)
        write_json(subject / "record.json", record)
    return 2 if record["error"] is not None else (record["exit_status"] or 0)


def inventory(output: Path) -> dict:
    result = {}
    for path in sorted(output.rglob("*")):
        if path.is_symlink():
            raise EvidenceError("artifact contains a symlink")
        if path.is_file() and path != output / "manifest.json":
            result[path.relative_to(output).as_posix()] = digest(path)
    return result


def positive(value) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def issues_for(output: Path, labels: list[str]) -> list[str]:
    issues = []
    for label in labels:
        subject = output / label
        try:
            record = json.loads((subject / "record.json").read_text())
            if not isinstance(record, dict) or set(record) != RECORD_FIELDS:
                raise EvidenceError("record fields missing or unexpected")
            if record["schema_version"] != 1 or record["label"] != label:
                raise EvidenceError("unsupported record schema or label")
            if record["error"] is not None or record["exit_status"] != 0:
                issues.append(f"{label}: setup or measurement failed")
            if (record["source_sha"] != record["expected_source"]
                    or not re.fullmatch(r"[0-9a-f]{40}", record["source_sha"] or "")
                    or not re.fullmatch(r"[0-9a-f]{40}", record["source_tree"] or "")
                    or not re.fullmatch(r"[0-9a-f]{40}", record["harness_source"] or "")):
                issues.append(f"{label}: source identity incomplete")
            harness = record["harness_files"] or {}
            tools = record["tools"] or {}
            if (set(harness) != {"codec_evidence.py", "sanitize_metadata.py", "ci.yml"}
                    or any(not re.fullmatch(r"[0-9a-f]{64}", value.get("sha256", ""))
                           or type(value.get("bytes")) is not int or value["bytes"] <= 0
                           for value in harness.values())
                    or set(tools) != {"rustc", "cargo"} or not all(tools.values())
                    or not record["host_before"] or not record["host_after"]):
                issues.append(f"{label}: build or host identity incomplete")
            if not set(record["environment"] or {}).issubset(ENVIRONMENT):
                issues.append(f"{label}: unexpected environment fields")
            if (record["lock_before"] != record["lock_after"]
                    or record["lock_before"] != digest(subject / "Cargo.lock")):
                issues.append(f"{label}: lock identity mismatch")
            resolution = json.loads((subject / "lock-resolution.json").read_text())
            if resolution != record["lock_resolution"]:
                issues.append(f"{label}: resolution record mismatch")
            before_path = subject / "Cargo.lock.before"
            before = before_path.read_bytes() if before_path.exists() else b""
            before_identity = digest(before_path) if before_path.exists() else None
            after = (subject / "Cargo.lock.after").read_bytes()
            after_identity = digest(subject / "Cargo.lock.after")
            classification = ("created" if before_identity is None else
                              "reused" if before_identity == after_identity else "updated")
            if (resolution.get("schema_version") != 1
                    or resolution.get("source_sha") != record["source_sha"]
                    or resolution.get("argv") != ["cargo", "metadata", "--format-version", "1"]
                    or resolution.get("exit_status") != 0
                    or resolution.get("classification") != classification
                    or resolution.get("before") != before_identity
                    or resolution.get("after") != after_identity
                    or resolution.get("after") != record["lock_before"]
                    or resolution.get("raw_diff_sha256") != hashlib.sha256(lock_diff(before, after).encode()).hexdigest()
                    or (subject / "lock-resolution.diff").read_text() != redact_diagnostic(lock_diff(before, after))):
                issues.append(f"{label}: resolution provenance incomplete or changed")
            if (record["executable_before"] != record["executable_after"]
                    or record["executable_before"] != digest(subject / "codec-executable")):
                issues.append(f"{label}: executable identity mismatch")
            dependencies = json.loads((subject / "dependencies.json").read_text())
            if (dependencies.get("schema_version") != 1
                    or dependencies.get("view") != "workspace-resolved"
                    or not isinstance(dependencies.get("packages"), list)
                    or not dependencies["packages"]):
                issues.append(f"{label}: dependency identity incomplete")
            by_capture = {item["capture"]: item for item in record["commands"]}
            measured = by_capture.get("benchmark", {})
            if (measured.get("argv", [])[1:] != [*SETTINGS, label]
                    or Path(measured.get("argv", [""])[0]).name != "codec-executable"
                    or measured.get("cwd") != record["benchmark_cwd"]
                    or measured.get("environment", {}).get("CRITERION_HOME") != record["criterion_directory"]):
                issues.append(f"{label}: executable invocation identity incomplete")
            if by_capture.get("resolution", {}).get("argv") != ["cargo", "metadata", "--format-version", "1"]:
                issues.append(f"{label}: resolution invocation identity incomplete")
            build = by_capture.get("build", {})
            if build.get("argv") != ["cargo", "bench", "--locked", "-p", "redis-tower-protocol",
                                     "--bench", "codec", "--no-run", "--message-format=json", "-vv"]:
                issues.append(f"{label}: build invocation identity incomplete")
            for required in ("build.stdout", "build.stderr", "compiler-artifact.json",
                             "benchmark.stdout", "benchmark.stderr", "case-list.stdout",
                             "resolution.stdout", "resolution.stderr"):
                if not (subject / required).is_file():
                    issues.append(f"{label}: missing {required}")
            expected = record["expected_cases"]
            if not isinstance(expected, list) or not expected or len(set(expected)) != len(expected):
                raise EvidenceError("expected cases are missing or duplicated")
            actual = []
            for path in (subject / "criterion").rglob("benchmark.json"):
                if path.parent.name != label:
                    continue
                benchmark = json.loads(path.read_text())
                actual.append(benchmark["full_id"])
                samples = json.loads(path.with_name("sample.json").read_text())
                iterations, times = samples["iters"], samples["times"]
                if (len(iterations) != 50 or len(times) != 50
                        or not all(positive(value) for value in [*iterations, *times])):
                    raise EvidenceError("invalid sample count, iterations or times")
                mean = json.loads(path.with_name("estimates.json").read_text())["mean"]
                point = mean["point_estimate"]
                interval = mean["confidence_interval"]
                lower, upper = interval["lower_bound"], interval["upper_bound"]
                observed = statistics.mean(t / n for t, n in zip(times, iterations))
                if (not all(positive(value) for value in (point, lower, upper))
                        or not lower <= point <= upper
                        or not math.isclose(observed, point, rel_tol=1e-9)):
                    raise EvidenceError("invalid mean estimate or raw sample arithmetic")
            if sorted(actual) != sorted(expected):
                issues.append(f"{label}: recorded cases differ from executable list")
        except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError, EvidenceError) as error:
            issues.append(f"{label}: {error}")
    try:
        if not (output / "benchmark-comparison.txt").read_text().strip():
            raise EvidenceError("empty comparison")
        if int((output / "benchmark-comparison.status").read_text()) not in (0, 1):
            raise EvidenceError("comparison did not complete normally")
    except (OSError, ValueError) as error:
        issues.append(f"comparison: {error}")
    return issues


def seal(output: Path, labels: list[str]) -> dict:
    if not labels or len(set(labels)) != len(labels) or not set(labels).issubset(LABELS):
        raise EvidenceError("invalid expected labels")
    output.mkdir(parents=True, exist_ok=True)
    issues = issues_for(output, labels)
    manifest = {"schema_version": 1, "labels": labels, "issues": issues,
                "evidence_complete": not issues, "files": inventory(output)}
    write_json(output / "manifest.json", manifest)
    return manifest


def verify(output: Path) -> None:
    manifest = json.loads((output / "manifest.json").read_text())
    if (not isinstance(manifest, dict)
            or set(manifest) != {"schema_version", "labels", "issues", "evidence_complete", "files"}
            or manifest["schema_version"] != 1):
        raise EvidenceError("unsupported manifest schema")
    if inventory(output) != manifest["files"]:
        raise EvidenceError("artifact inventory or hashes changed")
    labels = manifest["labels"]
    if (not labels or len(set(labels)) != len(labels) or not set(labels).issubset(LABELS)):
        raise EvidenceError("invalid manifest labels")
    issues = issues_for(output, labels)
    if issues or manifest["issues"] or manifest["evidence_complete"] is not True:
        raise EvidenceError("artifact evidence is incomplete: " + "; ".join(issues))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("measure")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--expected-source", required=True)
    run.add_argument("--harness-source", required=True)
    run.add_argument("--workflow", type=Path, required=True)
    # Keep the original sample configuration visible in workflow commands.
    run.add_argument("--warm-up-time", choices=["1"], required=True)
    run.add_argument("--measurement-time", choices=["2"], required=True)
    run.add_argument("--sample-size", choices=["50"], required=True)
    run.add_argument("--save-baseline", choices=sorted(LABELS), required=True)
    finish = sub.add_parser("seal")
    finish.add_argument("--output", type=Path, required=True)
    finish.add_argument("--labels", nargs="+", required=True)
    finish.add_argument("--comparison", type=Path, required=True)
    check = sub.add_parser("verify")
    check.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        if args.action == "measure":
            return measure(args.output, args.save_baseline, args.expected_source,
                           args.harness_source, args.workflow)
        if args.action == "seal":
            args.output.mkdir(parents=True, exist_ok=True)
            for suffix in (".txt", ".status"):
                source = args.comparison.with_suffix(suffix)
                if source.exists():
                    shutil.copyfile(source, args.output / ("benchmark-comparison" + suffix))
            result = seal(args.output, args.labels)
            print(json.dumps({"evidence_complete": result["evidence_complete"],
                              "issues": result["issues"]}, indent=2))
            return 0 if result["evidence_complete"] else 2
        verify(args.output)
        return 0
    except (EvidenceError, OSError, ValueError) as error:
        print(f"codec evidence error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
