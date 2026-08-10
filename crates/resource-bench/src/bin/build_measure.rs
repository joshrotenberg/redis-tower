//! Measure clean compile time and stripped binary size for each subject.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use resource_bench::{ClientFeatureSet, FRED_FEATURES, REDIS_RS_FEATURES, REDIS_TOWER_FEATURES};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy)]
struct Subject {
    client: &'static str,
    dependency: &'static str,
    features: ClientFeatureSet,
    binary: &'static str,
}

const SUBJECTS: [Subject; 3] = [
    Subject {
        client: "redis-tower",
        dependency: "redis-tower",
        features: REDIS_TOWER_FEATURES,
        binary: "resource-redis-tower",
    },
    Subject {
        client: "redis-rs",
        dependency: "redis",
        features: REDIS_RS_FEATURES,
        binary: "resource-redis-rs",
    },
    Subject {
        client: "fred",
        dependency: "fred",
        features: FRED_FEATURES,
        binary: "resource-fred",
    },
];

#[derive(Serialize)]
struct BuildArtifact {
    client: &'static str,
    dependency: &'static str,
    dependency_version: String,
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
    let result = install_cleanup_handler().and_then(|()| {
        if let Some(target) = env::var_os("RESOURCE_BUILD_SIGNAL_WRITER_DIR") {
            run_signal_test_writer(Path::new(&target))
        } else if let Some(base) = env::var_os("RESOURCE_BUILD_SIGNAL_TEST_DIR") {
            run_signal_test_helper(Path::new(&base))
        } else {
            run()
        }
    });
    while TERMINATING.load(Ordering::SeqCst) {
        std::thread::park();
    }
    if let Err(error) = result {
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
    let resolved_dependency_versions = resolved_dependency_versions(&workspace)?;
    let mut artifacts = Vec::with_capacity(SUBJECTS.len());
    for subject in SUBJECTS {
        let version = resolved_dependency_versions
            .get(subject.dependency)
            .ok_or_else(|| {
                format!(
                    "cargo metadata omitted {} for {}",
                    subject.dependency, subject.client
                )
            })?;
        artifacts.push(measure_subject(&workspace, subject, version, runs)?);
    }

    let report = BuildReport {
        schema_version: 3,
        os: env::consts::OS,
        arch: env::consts::ARCH,
        cargo_version: command_text_in(&workspace, "cargo", &["--version"])?,
        rustc_version: command_text_in(&workspace, "rustc", &["-Vv"])?,
        git_sha: git_sha(&workspace)?,
        git_dirty: git_is_dirty(&workspace)?,
        cargo_lock_sha256: sha256_file(&workspace.join("Cargo.lock"))?,
        resolved_dependency_versions,
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
    dependency_version: &str,
    runs: usize,
) -> Result<BuildArtifact, String> {
    let mut samples = Vec::with_capacity(runs);
    let mut unstripped_binary_bytes = 0;
    let mut stripped_binary_bytes = 0;

    for run in 0..runs {
        let target = temporary_target(subject.client, run)?;
        let started = Instant::now();
        let mut command = Command::new("cargo");
        command.current_dir(workspace).args([
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
        ]);
        command.arg(target.path()).stdout(Stdio::null());
        let status = command_status_with_cleanup(&mut command)
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
        dependency: subject.dependency,
        dependency_version: dependency_version.to_owned(),
        client_features: subject.features,
        resolved_dependency_graph: resolved_dependency_graph(
            workspace,
            subject,
            dependency_version,
        )?,
        mean_clean_build_seconds: mean(&samples),
        stddev_clean_build_seconds: stddev(&samples),
        clean_build_seconds: samples,
        unstripped_binary_bytes,
        stripped_binary_bytes,
    })
}

fn resolved_dependency_graph(
    workspace: &Path,
    subject: Subject,
    dependency_version: &str,
) -> Result<String, String> {
    let graph = command_text_in(
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
            "normal,build,features",
        ],
    )
    .map_err(|error| format!("resolve dependency graph for {}: {error}", subject.client))?;
    let graph = normalize_dependency_graph(&graph, workspace);
    if graph.len() >= 1_000_000 {
        return Err(format!(
            "dependency graph for {} is unexpectedly large: {} bytes",
            subject.client,
            graph.len()
        ));
    }
    for build_only_dependency in ["ctrlc", "sha2"] {
        if graph.contains(&format!("{build_only_dependency} v")) {
            return Err(format!(
                "dependency graph for {} contains build-measure-only dependency {build_only_dependency}",
                subject.client
            ));
        }
    }
    let dependency_marker = format!("{} v{dependency_version}", subject.dependency);
    if !graph.contains(&dependency_marker) {
        return Err(format!(
            "dependency graph for {} omitted {dependency_marker}",
            subject.client
        ));
    }
    for feature in subject.features.dependency_features {
        let feature_marker = format!("{} feature \"{feature}\"", subject.dependency);
        if !graph.contains(&feature_marker) {
            return Err(format!(
                "dependency graph for {} omitted {feature_marker}",
                subject.client
            ));
        }
    }
    Ok(graph)
}

fn normalize_dependency_graph(graph: &str, workspace: &Path) -> String {
    let workspace = workspace.to_string_lossy();
    graph.replace(workspace.as_ref(), "$WORKSPACE")
}

static TARGET_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static TERMINATING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static ACTIVE_TARGETS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
static CLEANUP_HANDLER: OnceLock<Result<(), String>> = OnceLock::new();
#[cfg(unix)]
static ACTIVE_PROCESS_GROUPS: OnceLock<Mutex<BTreeSet<i32>>> = OnceLock::new();

fn active_targets() -> &'static Mutex<BTreeSet<PathBuf>> {
    ACTIVE_TARGETS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

fn lock_active_targets() -> std::sync::MutexGuard<'static, BTreeSet<PathBuf>> {
    active_targets()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(unix)]
fn active_process_groups() -> &'static Mutex<BTreeSet<i32>> {
    ACTIVE_PROCESS_GROUPS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(unix)]
fn lock_active_process_groups() -> std::sync::MutexGuard<'static, BTreeSet<i32>> {
    active_process_groups()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(all(test, unix))]
fn process_group_is_alive(group: i32) -> bool {
    // SAFETY: signal 0 performs an existence/permission check and does not
    // deliver a signal. The negated PID addresses the child's process group.
    let result = unsafe { libc::kill(-group, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(unix)]
fn signal_process_group(group: i32, signal: i32) {
    // SAFETY: group is derived from a child PID created as a process-group
    // leader. Failure is harmless here; the group may already have exited.
    let _ = unsafe { libc::kill(-group, signal) };
}

#[cfg(unix)]
fn signal_active_process_groups(signal: i32) {
    let groups = lock_active_process_groups();
    for group in groups.iter().copied() {
        signal_process_group(group, signal);
    }
}

#[cfg(unix)]
fn wait_for_no_active_process_groups(deadline: Instant) -> bool {
    loop {
        // Re-read the registry on every iteration. In particular, do not keep
        // process-group IDs across a sleep: the child waiter must be able to
        // reap and unregister a group before its ID can be reused.
        if lock_active_process_groups().is_empty() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
fn terminate_active_process_groups() {
    signal_active_process_groups(libc::SIGTERM);
    if wait_for_no_active_process_groups(Instant::now() + Duration::from_secs(2)) {
        return;
    }

    // Reacquire the registry before signaling again. A group reaped during the
    // grace period is no longer eligible to receive SIGKILL.
    signal_active_process_groups(libc::SIGKILL);
    let forced_deadline = Instant::now() + Duration::from_secs(2);
    let _ = wait_for_no_active_process_groups(forced_deadline);
}

#[cfg(not(unix))]
fn terminate_active_process_groups() {}

struct RegisteredChild {
    child: Option<Child>,
    #[cfg(unix)]
    process_group: Option<i32>,
}

impl RegisteredChild {
    fn spawn(command: &mut Command) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            // Holding the registry lock across spawn and insertion prevents a
            // termination handler from observing an unregistered live child.
            // The flag check prevents new children after termination begins.
            let mut groups = lock_active_process_groups();
            if TERMINATING.load(Ordering::SeqCst) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "termination is already in progress",
                ));
            }

            command.process_group(0);
            let mut child = command.spawn()?;
            let group = match i32::try_from(child.id()) {
                Ok(group) => group,
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(std::io::Error::other(error));
                }
            };
            if !groups.insert(group) {
                signal_process_group(group, libc::SIGKILL);
                let _ = child.wait();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("process group {group} is already registered"),
                ));
            }

            Ok(Self {
                child: Some(child),
                process_group: Some(group),
            })
        }
        #[cfg(not(unix))]
        {
            command.spawn().map(|child| Self { child: Some(child) })
        }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("registered child is live").id()
    }

    #[cfg(all(test, unix))]
    fn process_group(&self) -> i32 {
        self.process_group.expect("registered child has a group")
    }

    fn wait(mut self) -> std::io::Result<ExitStatus> {
        #[cfg(unix)]
        {
            let group = self.process_group.expect("registered child has a group");
            loop {
                {
                    let mut groups = lock_active_process_groups();
                    match self
                        .child
                        .as_mut()
                        .expect("registered child is live")
                        .try_wait()
                    {
                        Ok(Some(status)) => {
                            // A PID/PGID can only be reused after this reap.
                            // Keep the registry locked until the now-stale ID
                            // is removed so the signal handler cannot use it.
                            let removed = groups.remove(&group);
                            debug_assert!(removed, "reaped process group was not registered");
                            self.process_group = None;
                            self.child.take();
                            return Ok(status);
                        }
                        Ok(None) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error),
                    }
                }
                // Let the signal handler acquire the registry between polls.
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        #[cfg(not(unix))]
        {
            let status = self
                .child
                .as_mut()
                .expect("registered child is live")
                .wait();
            if status.is_ok() {
                self.child.take();
            }
            status
        }
    }
}

impl Drop for RegisteredChild {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            let Some(group) = self.process_group else {
                return;
            };
            let Some(child) = self.child.as_mut() else {
                return;
            };

            // Keep the group registered and the registry locked through the
            // successful reap. This is the error-path counterpart to wait().
            let mut groups = lock_active_process_groups();
            signal_process_group(group, libc::SIGKILL);
            loop {
                match child.wait() {
                    Ok(_) => {
                        groups.remove(&group);
                        self.process_group = None;
                        self.child.take();
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        // An unreaped child prevents PID reuse. Retaining the
                        // registry entry is safer than exposing a stale ID.
                        eprintln!("warning: could not reap process group {group}: {error}");
                        break;
                    }
                }
            }
        }
        #[cfg(not(unix))]
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn command_status_with_cleanup(command: &mut Command) -> std::io::Result<ExitStatus> {
    RegisteredChild::spawn(command)?.wait()
}

fn install_cleanup_handler() -> Result<(), String> {
    CLEANUP_HANDLER
        .get_or_init(|| {
            let _ = active_targets();
            #[cfg(unix)]
            let _ = active_process_groups();
            ctrlc::set_handler(|| {
                TERMINATING.store(true, Ordering::SeqCst);
                terminate_active_process_groups();
                cleanup_active_targets_for_signal();
                std::process::exit(130);
            })
            .map_err(|error| format!("install build cleanup signal handler: {error}"))
        })
        .clone()
}

fn cleanup_active_targets_for_signal() {
    let targets = lock_active_targets();
    for path in targets.iter() {
        if let Err(error) = fs::remove_dir_all(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "warning: could not remove {} during signal cleanup: {error}",
                path.display()
            );
        }
    }
}

fn run_signal_test_helper(base: &Path) -> Result<(), String> {
    let target = temporary_target_in(base, "signal-test", 0)?;
    let executable =
        env::current_exe().map_err(|error| format!("locate signal-test executable: {error}"))?;
    let mut writer = Command::new(executable);
    writer
        .env("RESOURCE_BUILD_SIGNAL_WRITER_DIR", target.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let writer = RegisteredChild::spawn(&mut writer)
        .map_err(|error| format!("start signal-test writer: {error}"))?;
    println!("{}\t{}", target.path().display(), writer.id());
    std::io::stdout()
        .flush()
        .map_err(|error| format!("flush signal-test target and writer PID: {error}"))?;
    let status = writer
        .wait()
        .map_err(|error| format!("run signal-test writer: {error}"))?;
    Err(format!(
        "signal-test writer exited unexpectedly with {status}"
    ))
}

fn run_signal_test_writer(target: &Path) -> Result<(), String> {
    loop {
        fs::write(target.join("writer-active"), b"active")
            .map_err(|error| format!("write signal-test activity marker: {error}"))?;
        std::thread::sleep(Duration::from_millis(10));
    }
}

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
        let mut targets = lock_active_targets();
        match fs::remove_dir_all(&self.path) {
            Ok(()) => {
                targets.remove(&self.path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                targets.remove(&self.path);
            }
            Err(error) => {
                // Keep the path registered so a later termination signal gets
                // one more cleanup attempt before the process exits.
                eprintln!("warning: could not remove {}: {error}", self.path.display());
            }
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
    let mut targets = lock_active_targets();
    if !targets.insert(path.clone()) {
        return Err(format!(
            "isolated target {} is already active",
            path.display()
        ));
    }
    if let Err(error) = fs::create_dir(&path) {
        targets.remove(&path);
        return Err(format!(
            "create isolated target {}: {error}",
            path.display()
        ));
    }
    drop(targets);
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
    let mut strip = Command::new(strip_program);
    strip.arg(&stripped);
    let strip_status = command_status_with_cleanup(&mut strip)
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

    static CWD_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn aggregate_math_is_population_based() {
        let samples = [1.0, 2.0, 3.0];
        assert_eq!(mean(&samples), 2.0);
        assert!((stddev(&samples) - (2.0_f64 / 3.0).sqrt()).abs() < 1e-12);
        assert_eq!(stddev(&[2.0]), 0.0);
    }

    #[cfg(unix)]
    #[test]
    fn normally_exited_child_is_reaped_and_unregistered_atomically() {
        let mut command = Command::new("true");
        let child = RegisteredChild::spawn(&mut command).expect("spawn registered child");
        let group = child.process_group();
        assert!(lock_active_process_groups().contains(&group));

        let status = child.wait().expect("wait for registered child");

        assert!(status.success());
        assert!(!lock_active_process_groups().contains(&group));
        assert!(!process_group_is_alive(group));
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
        let _lock = CWD_TEST_LOCK.lock().expect("lock caller-CWD test");
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

    #[test]
    fn dependency_graphs_are_bounded_normalized_and_caller_cwd_independent() {
        let _lock = CWD_TEST_LOCK.lock().expect("lock caller-CWD test");
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("locate workspace");
        let versions = resolved_dependency_versions(&workspace).expect("resolve client versions");
        let collect = || {
            SUBJECTS
                .iter()
                .copied()
                .map(|subject| {
                    let version = versions
                        .get(subject.dependency)
                        .expect("subject version is present");
                    resolved_dependency_graph(&workspace, subject, version)
                        .expect("resolve subject graph")
                })
                .collect::<Vec<_>>()
        };
        let from_workspace = collect();
        let original = env::current_dir().expect("read current directory");
        env::set_current_dir(env::temp_dir()).expect("move to unrelated directory");
        let from_unrelated = collect();
        env::set_current_dir(original).expect("restore current directory");

        assert_eq!(from_workspace, from_unrelated);
        assert!(
            from_workspace
                .iter()
                .any(|graph| graph.contains("[build-dependencies]"))
        );
        for graph in from_workspace {
            assert!(graph.len() < 1_000_000);
            assert!(graph.contains("$WORKSPACE/crates/resource-bench"));
            assert!(!graph.contains(workspace.to_string_lossy().as_ref()));
            assert!(!graph.contains("ctrlc v"));
            assert!(!graph.contains("sha2 v"));
        }
    }

    #[test]
    fn dependency_graph_normalization_is_stable_across_checkout_paths() {
        let checkout_a = Path::new("/tmp/worker-a/redis-tower");
        let checkout_b = Path::new("/opt/worker-b/redis-tower");
        let graph_a = format!(
            "resource-bench v0.0.0 ({}/crates/resource-bench)\n└── redis v1.5.0\n",
            checkout_a.display()
        );
        let graph_b = format!(
            "resource-bench v0.0.0 ({}/crates/resource-bench)\n└── redis v1.5.0\n",
            checkout_b.display()
        );

        assert_eq!(
            normalize_dependency_graph(&graph_a, checkout_a),
            normalize_dependency_graph(&graph_b, checkout_b)
        );
    }

    #[test]
    fn runner_temp_output_does_not_hide_real_checkout_changes() {
        let repo = temporary_target_in(&env::temp_dir(), "dirty-state-repo", 0)
            .expect("create temporary repository");
        command_text_in(repo.path(), "git", &["init", "--quiet"]).expect("initialize repository");
        command_text_in(
            repo.path(),
            "git",
            &["config", "user.email", "resource-bench@example.invalid"],
        )
        .expect("configure git email");
        command_text_in(
            repo.path(),
            "git",
            &["config", "user.name", "Resource Bench"],
        )
        .expect("configure git name");
        let tracked = repo.path().join("tracked.txt");
        fs::write(&tracked, "original\n").expect("write tracked fixture");
        command_text_in(repo.path(), "git", &["add", "tracked.txt"]).expect("stage fixture");
        command_text_in(repo.path(), "git", &["commit", "--quiet", "-m", "fixture"])
            .expect("commit fixture");
        assert!(!git_is_dirty(repo.path()).expect("inspect clean repository"));

        let runner_temp = temporary_target_in(&env::temp_dir(), "runner-temp-output", 0)
            .expect("create simulated runner temp");
        fs::write(runner_temp.path().join("build-footprint.json"), "{}\n")
            .expect("write workflow-equivalent redirected output");
        assert!(!git_is_dirty(repo.path()).expect("runner temp must not dirty checkout"));

        fs::write(&tracked, "changed\n").expect("modify tracked fixture");
        assert!(git_is_dirty(repo.path()).expect("tracked change must be visible"));
        fs::write(&tracked, "original\n").expect("restore tracked fixture");
        assert!(!git_is_dirty(repo.path()).expect("repository should be clean again"));

        fs::write(repo.path().join("unrelated.txt"), "untracked\n")
            .expect("write unrelated untracked file");
        assert!(git_is_dirty(repo.path()).expect("untracked change must be visible"));
    }
}
