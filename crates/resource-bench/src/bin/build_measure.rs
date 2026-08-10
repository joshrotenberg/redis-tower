//! Measure clean compile time and stripped binary size for each subject.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use resource_bench::{ClientFeatureSet, FRED_FEATURES, REDIS_RS_FEATURES, REDIS_TOWER_FEATURES};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy)]
struct Subject {
    client: &'static str,
    features: ClientFeatureSet,
    binary: &'static str,
}

const SUBJECTS: [Subject; 3] = [
    Subject {
        client: "redis-tower",
        features: REDIS_TOWER_FEATURES,
        binary: "resource-redis-tower",
    },
    Subject {
        client: "redis-rs",
        features: REDIS_RS_FEATURES,
        binary: "resource-redis-rs",
    },
    Subject {
        client: "fred",
        features: FRED_FEATURES,
        binary: "resource-fred",
    },
];

#[derive(Serialize)]
struct BuildArtifact {
    client: &'static str,
    client_features: ClientFeatureSet,
    resolved_dependency_graph: String,
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
    git_dirty: bool,
    cargo_lock_sha256: String,
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
        schema_version: 2,
        os: env::consts::OS,
        arch: env::consts::ARCH,
        cargo_version: command_text_in(&workspace, "cargo", &["--version"])?,
        rustc_version: command_text_in(&workspace, "rustc", &["-Vv"])?,
        git_sha: git_sha(&workspace)?,
        git_dirty: git_is_dirty(&workspace)?,
        cargo_lock_sha256: sha256_file(&workspace.join("Cargo.lock"))?,
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
                subject.features.harness_feature,
                "--target-dir",
            ])
            .arg(target.path())
            .stdout(Stdio::null())
            .status()
            .map_err(|error| format!("start cargo for {}: {error}", subject.client))?;
        let elapsed = started.elapsed().as_secs_f64();
        if !status.success() {
            return Err(format!(
                "clean build for {} exited with {status}",
                subject.client
            ));
        }
        samples.push(elapsed);

        (unstripped_binary_bytes, stripped_binary_bytes) =
            measure_artifact(target, subject.binary, OsStr::new("strip"))?;
    }

    Ok(BuildArtifact {
        client: subject.client,
        client_features: subject.features,
        resolved_dependency_graph: resolved_dependency_graph(workspace, subject)?,
        mean_clean_build_seconds: mean(&samples),
        stddev_clean_build_seconds: stddev(&samples),
        clean_build_seconds: samples,
        unstripped_binary_bytes,
        stripped_binary_bytes,
    })
}

fn resolved_dependency_graph(workspace: &Path, subject: Subject) -> Result<String, String> {
    command_text_in(
        workspace,
        "cargo",
        &[
            "tree",
            "--color",
            "never",
            "--locked",
            "--package",
            "resource-bench",
            "--no-default-features",
            "--features",
            subject.features.harness_feature,
            "--edges",
            "normal,features",
            "--no-dedupe",
        ],
    )
    .map_err(|error| format!("resolve dependency graph for {}: {error}", subject.client))
}

static TARGET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct IsolatedTarget {
    path: PathBuf,
}

impl IsolatedTarget {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for IsolatedTarget {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!("warning: could not remove {}: {error}", self.path.display());
        }
    }
}

fn temporary_target(client: &str, run: usize) -> Result<IsolatedTarget, String> {
    temporary_target_in(&env::temp_dir(), client, run)
}

fn temporary_target_in(base: &Path, client: &str, run: usize) -> Result<IsolatedTarget, String> {
    let sequence = TARGET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = base.join(format!(
        "redis-tower-resource-build-{}-{sequence}-{client}-{run}",
        std::process::id()
    ));
    fs::create_dir(&path)
        .map_err(|error| format!("create isolated target {}: {error}", path.display()))?;
    Ok(IsolatedTarget { path })
}

fn measure_artifact(
    target: IsolatedTarget,
    binary_name: &str,
    strip_program: &OsStr,
) -> Result<(u64, u64), String> {
    let binary = target
        .path()
        .join("release")
        .join(format!("{binary_name}{}", env::consts::EXE_SUFFIX));
    let unstripped_binary_bytes = file_size(&binary)?;
    let stripped = target.path().join(format!("{binary_name}.stripped"));
    fs::copy(&binary, &stripped)
        .map_err(|error| format!("copy {} for stripping: {error}", binary.display()))?;
    let strip_status = Command::new(strip_program)
        .arg(&stripped)
        .status()
        .map_err(|error| format!("start strip for {binary_name}: {error}"))?;
    if !strip_status.success() {
        return Err(format!(
            "strip for {binary_name} exited with {strip_status}"
        ));
    }
    let stripped_binary_bytes = file_size(&stripped)?;
    Ok((unstripped_binary_bytes, stripped_binary_bytes))
}

fn file_size(path: &Path) -> Result<u64, String> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("stat {}: {error}", path.display()))
}

fn command_text_in(directory: &Path, command: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(command)
        .current_dir(directory)
        .args(args)
        .output()
        .map_err(|error| format!("run {command}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{command} exited with {}", output.status));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_sha(workspace: &Path) -> Result<String, String> {
    command_text_in(workspace, "git", &["rev-parse", "HEAD"])
}

fn git_is_dirty(workspace: &Path) -> Result<bool, String> {
    command_text_in(
        workspace,
        "git",
        &["status", "--porcelain", "--untracked-files=normal"],
    )
    .map(|status| !status.is_empty())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
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

    #[test]
    fn isolated_target_is_removed_after_stat_and_strip_errors() {
        let base = env::temp_dir();

        let missing = temporary_target_in(&base, "missing", 0).expect("create missing target");
        let missing_path = missing.path().to_owned();
        let error = measure_artifact(missing, "not-built", OsStr::new("strip"))
            .expect_err("missing binary should fail stat");
        assert!(error.contains("stat"));
        assert!(!missing_path.exists());

        let strip = temporary_target_in(&base, "strip", 0).expect("create strip target");
        let strip_path = strip.path().to_owned();
        let release = strip.path().join("release");
        fs::create_dir(&release).expect("create release directory");
        fs::write(
            release.join(format!("built{}", env::consts::EXE_SUFFIX)),
            b"not an executable",
        )
        .expect("create pretend binary");
        let error = measure_artifact(
            strip,
            "built",
            OsStr::new("resource-bench-command-that-does-not-exist"),
        )
        .expect_err("missing strip program should fail");
        assert!(error.contains("start strip"));
        assert!(!strip_path.exists());
    }

    #[test]
    fn git_sha_is_resolved_from_workspace_outside_the_caller_cwd() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("locate workspace");
        let expected =
            command_text_in(&workspace, "git", &["rev-parse", "HEAD"]).expect("read expected SHA");
        let original = env::current_dir().expect("read current directory");
        env::set_current_dir(env::temp_dir()).expect("move to unrelated directory");
        let actual = git_sha(&workspace);
        env::set_current_dir(original).expect("restore current directory");

        assert_eq!(actual.expect("read SHA outside workspace"), expected);
    }
}
