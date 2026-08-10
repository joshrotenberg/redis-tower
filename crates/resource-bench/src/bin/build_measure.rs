//! Measure clean compile time and stripped binary size for each subject.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use serde::Serialize;

#[derive(Clone, Copy)]
struct Subject {
    client: &'static str,
    feature: &'static str,
    binary: &'static str,
}

const SUBJECTS: [Subject; 3] = [
    Subject {
        client: "redis-tower",
        feature: "client-redis-tower",
        binary: "resource-redis-tower",
    },
    Subject {
        client: "redis-rs",
        feature: "client-redis-rs",
        binary: "resource-redis-rs",
    },
    Subject {
        client: "fred",
        feature: "client-fred",
        binary: "resource-fred",
    },
];

#[derive(Serialize)]
struct BuildArtifact {
    client: &'static str,
    clean_build_seconds: Vec<f64>,
    mean_clean_build_seconds: f64,
    stddev_clean_build_seconds: f64,
    unstripped_binary_bytes: u64,
    stripped_binary_bytes: u64,
}

#[derive(Serialize)]
struct BuildReport {
    schema_version: u32,
    os: &'static str,
    arch: &'static str,
    cargo_version: String,
    rustc_version: String,
    git_sha: String,
    resolved_dependency_versions: BTreeMap<String, String>,
    runs_per_client: usize,
    artifacts: Vec<BuildArtifact>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("build measurement failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let runs = env::var("RESOURCE_BUILD_RUNS")
        .map(|raw| {
            raw.parse::<usize>()
                .map_err(|error| format!("invalid RESOURCE_BUILD_RUNS={raw:?}: {error}"))
        })
        .unwrap_or(Ok(3))?;
    if runs == 0 {
        return Err("RESOURCE_BUILD_RUNS must be greater than zero".to_owned());
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("locate workspace: {error}"))?;
    prepare_dependencies(&workspace)?;
    let mut artifacts = Vec::with_capacity(SUBJECTS.len());
    for subject in SUBJECTS {
        artifacts.push(measure_subject(&workspace, subject, runs)?);
    }

    let report = BuildReport {
        schema_version: 1,
        os: env::consts::OS,
        arch: env::consts::ARCH,
        cargo_version: command_text("cargo", &["--version"])?,
        rustc_version: command_text("rustc", &["-Vv"])?,
        git_sha: command_text("git", &["rev-parse", "HEAD"])
            .unwrap_or_else(|_| "unknown".to_owned()),
        resolved_dependency_versions: resolved_dependency_versions(&workspace)?,
        runs_per_client: runs,
        artifacts,
    };

    if env::args().any(|arg| arg == "--json") {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|error| format!("serialize build report: {error}"))?
        );
    } else {
        println!(
            "{:<14} {:>14} {:>16} {:>16}",
            "client", "clean mean (s)", "binary (bytes)", "stripped (bytes)"
        );
        for artifact in &report.artifacts {
            println!(
                "{:<14} {:>14.2} {:>16} {:>16}",
                artifact.client,
                artifact.mean_clean_build_seconds,
                artifact.unstripped_binary_bytes,
                artifact.stripped_binary_bytes
            );
        }
    }
    Ok(())
}

fn prepare_dependencies(workspace: &Path) -> Result<(), String> {
    let status = Command::new("cargo")
        .current_dir(workspace)
        .arg("fetch")
        .stdout(Stdio::null())
        .status()
        .map_err(|error| format!("start cargo fetch: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("cargo fetch exited with {status}"))
    }
}

fn resolved_dependency_versions(workspace: &Path) -> Result<BTreeMap<String, String>, String> {
    let output = Command::new("cargo")
        .current_dir(workspace)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--all-features",
        ])
        .output()
        .map_err(|error| format!("run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!("cargo metadata exited with {}", output.status));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("parse cargo metadata: {error}"))?;
    let wanted = ["fred", "redis", "redis-tower"];
    let mut versions = BTreeMap::new();
    let packages = metadata["packages"]
        .as_array()
        .ok_or_else(|| "cargo metadata omitted packages".to_owned())?;
    for package in packages {
        let Some(name) = package["name"].as_str() else {
            continue;
        };
        if wanted.contains(&name)
            && let Some(version) = package["version"].as_str()
        {
            versions.insert(name.to_owned(), version.to_owned());
        }
    }
    Ok(versions)
}

fn measure_subject(
    workspace: &Path,
    subject: Subject,
    runs: usize,
) -> Result<BuildArtifact, String> {
    let mut samples = Vec::with_capacity(runs);
    let mut unstripped_binary_bytes = 0;
    let mut stripped_binary_bytes = 0;

    for run in 0..runs {
        let target = temporary_target(subject.client, run)?;
        let started = Instant::now();
        let status = Command::new("cargo")
            .current_dir(workspace)
            .args([
                "build",
                "--release",
                "--locked",
                "--package",
                "resource-bench",
                "--bin",
                subject.binary,
                "--no-default-features",
                "--features",
                subject.feature,
                "--target-dir",
            ])
            .arg(&target)
            .stdout(Stdio::null())
            .status()
            .map_err(|error| format!("start cargo for {}: {error}", subject.client))?;
        let elapsed = started.elapsed().as_secs_f64();
        if !status.success() {
            cleanup(&target);
            return Err(format!(
                "clean build for {} exited with {status}",
                subject.client
            ));
        }
        samples.push(elapsed);

        let binary =
            target
                .join("release")
                .join(format!("{}{}", subject.binary, env::consts::EXE_SUFFIX));
        unstripped_binary_bytes = file_size(&binary)?;
        let stripped = target.join(format!("{}.stripped", subject.binary));
        fs::copy(&binary, &stripped)
            .map_err(|error| format!("copy {} for stripping: {error}", binary.display()))?;
        let strip_status = Command::new("strip")
            .arg(&stripped)
            .status()
            .map_err(|error| format!("start strip for {}: {error}", subject.client))?;
        if !strip_status.success() {
            cleanup(&target);
            return Err(format!(
                "strip for {} exited with {strip_status}",
                subject.client
            ));
        }
        stripped_binary_bytes = file_size(&stripped)?;
        cleanup(&target);
    }

    Ok(BuildArtifact {
        client: subject.client,
        mean_clean_build_seconds: mean(&samples),
        stddev_clean_build_seconds: stddev(&samples),
        clean_build_seconds: samples,
        unstripped_binary_bytes,
        stripped_binary_bytes,
    })
}

fn temporary_target(client: &str, run: usize) -> Result<PathBuf, String> {
    let path = env::temp_dir().join(format!(
        "redis-tower-resource-build-{}-{client}-{run}",
        std::process::id()
    ));
    fs::create_dir(&path)
        .map_err(|error| format!("create isolated target {}: {error}", path.display()))?;
    Ok(path)
}

fn cleanup(path: &Path) {
    if let Err(error) = fs::remove_dir_all(path) {
        eprintln!("warning: could not remove {}: {error}", path.display());
    }
}

fn file_size(path: &Path) -> Result<u64, String> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("stat {}: {error}", path.display()))
}

fn command_text(command: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(command)
        .args(args)
        .output()
        .map_err(|error| format!("run {command}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{command} exited with {}", output.status));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = mean(values);
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_math_is_population_based() {
        let samples = [1.0, 2.0, 3.0];
        assert_eq!(mean(&samples), 2.0);
        assert!((stddev(&samples) - (2.0_f64 / 3.0).sqrt()).abs() < 1e-12);
        assert_eq!(stddev(&[2.0]), 0.0);
    }
}
