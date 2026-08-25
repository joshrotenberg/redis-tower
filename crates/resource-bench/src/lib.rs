//! Shared machinery for process-isolated Redis client resource probes.
//!
//! Each client adapter is compiled into its own binary so measurements do not
//! include another client's dependency graph. The probe reports peak resident
//! memory before and after opening independent connections, then measures
//! process CPU while offering a fixed GET rate.

use std::env;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout_at};

/// Key used by every client implementation during the fixed-rate workload.
pub const FIXTURE_KEY: &str = "resource-bench:payload";

/// Exact Cargo feature selection used to compile one subject client.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ClientFeatureSet {
    /// Feature on `resource-bench` that selects the subject dependency.
    pub harness_feature: &'static str,
    /// Whether the subject dependency enables its default features.
    pub dependency_default_features: bool,
    /// Explicit features enabled on the subject dependency.
    pub dependency_features: &'static [&'static str],
}

/// Feature selection for the redis-tower subject.
pub const REDIS_TOWER_FEATURES: ClientFeatureSet = ClientFeatureSet {
    harness_feature: "client-redis-tower",
    dependency_default_features: false,
    dependency_features: &[],
};

/// Feature selection for the redis-rs subject.
pub const REDIS_RS_FEATURES: ClientFeatureSet = ClientFeatureSet {
    harness_feature: "client-redis-rs",
    dependency_default_features: false,
    dependency_features: &["tokio-comp"],
};

/// Feature selection for the Fred subject.
pub const FRED_FEATURES: ClientFeatureSet = ClientFeatureSet {
    harness_feature: "client-fred",
    dependency_default_features: false,
    dependency_features: &["i-keys"],
};

/// Client operations needed by the common measurement harness.
#[async_trait]
pub trait ProbeConnection: Send + Sized + 'static {
    /// Open one independent physical connection.
    async fn connect(url: &str) -> Result<Self, String>;

    /// Store the workload fixture.
    async fn set_fixture(&mut self, value: &str) -> Result<(), String>;

    /// Fetch and validate the workload fixture.
    async fn get_fixture(&mut self, expected: &[u8]) -> Result<(), String>;
}

/// Environment-driven probe configuration.
#[derive(Clone, Serialize)]
pub struct ProbeConfig {
    /// Redis URL used by the subject client. Never include it in artifacts,
    /// because it may contain a username or password.
    #[serde(skip_serializing)]
    redis_url: String,
    /// Constant redaction marker; deployment endpoint details are never emitted.
    pub redis_endpoint: String,
    /// Number of independent live connections retained during the probe.
    pub connections: usize,
    /// Aggregate GET rate offered across all connections.
    pub target_ops_per_sec: u64,
    /// Unmeasured CPU-workload warmup.
    pub warmup_secs: u64,
    /// Measured CPU-workload duration.
    pub duration_secs: u64,
    /// Maximum time to drain an operation launched before a window deadline.
    pub drain_timeout_ms: u64,
    /// Expected value length for every successful GET.
    pub payload_bytes: usize,
}

impl ProbeConfig {
    /// Read configuration from the `RESOURCE_*` environment variables.
    pub fn from_env() -> Result<Self, String> {
        let redis_url =
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_owned());
        let config = Self {
            redis_endpoint: safe_redis_endpoint(&redis_url),
            redis_url,
            connections: positive_env("RESOURCE_CONNECTIONS", 100)?,
            target_ops_per_sec: positive_env("RESOURCE_TARGET_OPS_PER_SEC", 5_000)?,
            warmup_secs: env_number("RESOURCE_WARMUP_SECS", 2)?,
            duration_secs: positive_env("RESOURCE_DURATION_SECS", 10)?,
            drain_timeout_ms: positive_env("RESOURCE_DRAIN_TIMEOUT_MS", 1_000)?,
            payload_bytes: positive_env("RESOURCE_PAYLOAD_BYTES", 1_024)?,
        };

        if config.target_ops_per_sec > 1_000_000_000 {
            return Err("RESOURCE_TARGET_OPS_PER_SEC must not exceed 1000000000".to_owned());
        }
        if config.drain_timeout_ms > 60_000 {
            return Err("RESOURCE_DRAIN_TIMEOUT_MS must not exceed 60000".to_owned());
        }
        Ok(config)
    }
}

fn safe_redis_endpoint(_raw: &str) -> String {
    // Even a scheme, port, or database number can disclose deployment details.
    // Reports therefore carry only a stable marker, while the raw URL remains
    // available privately to the selected client adapter.
    "<redacted>".to_owned()
}

#[cfg(test)]
fn serialized_config_for_url(raw: &str) -> String {
    let config = ProbeConfig {
        redis_url: raw.to_owned(),
        redis_endpoint: safe_redis_endpoint(raw),
        connections: 1,
        target_ops_per_sec: 1,
        warmup_secs: 0,
        duration_secs: 1,
        drain_timeout_ms: 100,
        payload_bytes: 1,
    };
    serde_json::to_string(&config).expect("serialize probe configuration")
}

fn env_number<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(raw) => raw
            .parse()
            .map_err(|error| format!("invalid {name}={raw:?}: {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
}

fn positive_env<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr + PartialEq + Default + Copy,
    T::Err: std::fmt::Display,
{
    let value = env_number(name, default)?;
    if value == T::default() {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(value)
}

/// Complete machine-readable result from one client process.
#[derive(Serialize)]
pub struct ProbeReport {
    /// Artifact schema version.
    pub schema_version: u32,
    /// Subject client name.
    pub client: &'static str,
    /// Exact subject dependency feature configuration.
    pub client_features: ClientFeatureSet,
    /// Operating-system identifier.
    pub os: &'static str,
    /// CPU architecture identifier.
    pub arch: &'static str,
    /// Runtime configuration.
    pub config: ProbeConfig,
    /// Peak-RSS measurements.
    pub rss: RssReport,
    /// Fixed-offered-rate CPU measurements.
    pub cpu: CpuReport,
}

/// Peak resident-set measurements around connection establishment.
#[derive(Debug, Serialize)]
pub struct RssReport {
    /// Process peak RSS after runtime startup and before connecting.
    pub baseline_peak_bytes: u64,
    /// Process peak RSS while all subject connections are live.
    pub connected_peak_bytes: u64,
    /// Saturating difference between connected and baseline peaks.
    pub connection_delta_bytes: u64,
    /// Amortized connection delta.
    pub bytes_per_connection: f64,
    /// Peak RSS after the fixed-rate workload.
    pub post_workload_peak_bytes: u64,
}

/// CPU and completion accounting for the fixed offered-rate window.
#[derive(Debug, Serialize)]
pub struct CpuReport {
    /// Requested aggregate rate.
    pub target_ops_per_sec: u64,
    /// Attempted operations divided by the operation-launch window.
    pub attempted_ops_per_sec: f64,
    /// Successful, payload-validated operations divided by wall time.
    pub achieved_ops_per_sec: f64,
    /// Total attempts in the measured window.
    pub attempted_ops: u64,
    /// Successful payload-validated GETs.
    pub successful_ops: u64,
    /// Command failures, misses, and payload mismatches.
    pub errors: u64,
    /// Operations canceled only after the bounded post-window drain expired.
    pub cutoff_ops: u64,
    /// Intended operation-launch window.
    pub launch_window_seconds: f64,
    /// Time spent after the launch deadline draining an in-flight operation.
    pub drain_seconds: f64,
    /// Total measured wall time, including the bounded drain.
    pub wall_seconds: f64,
    /// User plus system CPU consumed in the measured window.
    pub process_cpu_seconds: f64,
    /// Process CPU divided by wall time; may exceed 100% with multiple cores.
    pub process_cpu_percent: f64,
}

#[derive(Clone, Copy, Debug)]
struct UsageSnapshot {
    peak_rss_bytes: u64,
    cpu_seconds: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct WindowStats {
    attempted: u64,
    successful: u64,
    errors: u64,
    cutoff: u64,
}

impl WindowStats {
    fn add(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.successful += other.successful;
        self.errors += other.errors;
        self.cutoff += other.cutoff;
    }
}

#[derive(Clone, Copy)]
enum WindowErrorMode {
    Fatal,
    Count,
}

struct WorkerOutcome<C> {
    connection: Option<C>,
    stats: WindowStats,
    fatal_error: Option<String>,
}

/// Run one isolated client probe and print either JSON (`--json`) or a concise
/// human-readable summary.
pub async fn run_client<C: ProbeConnection>(
    client: &'static str,
    client_features: ClientFeatureSet,
) -> Result<(), String> {
    let config = ProbeConfig::from_env()?;
    let json = env::args().any(|arg| arg == "--json");
    let report = measure::<C>(client, client_features, config).await?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|error| format!("serialize report: {error}"))?
        );
    } else {
        print_human(&report);
    }
    Ok(())
}

async fn measure<C: ProbeConnection>(
    client: &'static str,
    client_features: ClientFeatureSet,
    config: ProbeConfig,
) -> Result<ProbeReport, String> {
    // Allocate and touch the fixture before the RSS baseline so its bytes are
    // not misattributed to the connection population.
    let payload = "x".repeat(config.payload_bytes);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let baseline = usage_snapshot()?;

    let mut connections = Vec::with_capacity(config.connections);
    for index in 0..config.connections {
        let connection = C::connect(&config.redis_url)
            .await
            .map_err(|error| format!("open connection {index}: {error}"))?;
        connections.push(connection);
    }

    connections
        .first_mut()
        .expect("positive connection count")
        .set_fixture(&payload)
        .await?;
    for (index, connection) in connections.iter_mut().enumerate() {
        connection
            .get_fixture(payload.as_bytes())
            .await
            .map_err(|error| format!("verify connection {index}: {error}"))?;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    let connected = usage_snapshot()?;
    let connection_delta = connected
        .peak_rss_bytes
        .saturating_sub(baseline.peak_rss_bytes);

    let payload = Arc::<[u8]>::from(payload.into_bytes());
    let drain_timeout = Duration::from_millis(config.drain_timeout_ms);
    if config.warmup_secs > 0 {
        let (returned, _) = run_window(
            connections,
            Duration::from_secs(config.warmup_secs),
            drain_timeout,
            config.target_ops_per_sec,
            payload.clone(),
            WindowErrorMode::Fatal,
        )
        .await?;
        connections =
            restore_after_warmup(returned, &config.redis_url, &payload, drain_timeout).await?;
    }

    let cpu_before = usage_snapshot()?;
    let wall_start = Instant::now();
    let (connections, window) = run_window(
        connections,
        Duration::from_secs(config.duration_secs),
        drain_timeout,
        config.target_ops_per_sec,
        payload,
        WindowErrorMode::Count,
    )
    .await?;
    let wall_seconds = wall_start.elapsed().as_secs_f64();
    let launch_window_seconds = config.duration_secs as f64;
    let drain_seconds = (wall_seconds - launch_window_seconds).max(0.0);
    let cpu_after = usage_snapshot()?;
    let process_cpu_seconds = (cpu_after.cpu_seconds - cpu_before.cpu_seconds).max(0.0);
    let target_ops_per_sec = config.target_ops_per_sec;

    // Retain all connections until every measurement has been captured.
    let _connections = connections;

    Ok(ProbeReport {
        schema_version: 3,
        client,
        client_features,
        os: env::consts::OS,
        arch: env::consts::ARCH,
        config,
        rss: RssReport {
            baseline_peak_bytes: baseline.peak_rss_bytes,
            connected_peak_bytes: connected.peak_rss_bytes,
            connection_delta_bytes: connection_delta,
            bytes_per_connection: connection_delta as f64 / _connections.len() as f64,
            post_workload_peak_bytes: cpu_after.peak_rss_bytes,
        },
        cpu: CpuReport {
            target_ops_per_sec,
            attempted_ops_per_sec: window.attempted as f64 / launch_window_seconds,
            achieved_ops_per_sec: window.successful as f64 / wall_seconds,
            attempted_ops: window.attempted,
            successful_ops: window.successful,
            errors: window.errors,
            cutoff_ops: window.cutoff,
            launch_window_seconds,
            drain_seconds,
            wall_seconds,
            process_cpu_seconds,
            process_cpu_percent: process_cpu_seconds / wall_seconds * 100.0,
        },
    })
}

async fn restore_after_warmup<C: ProbeConnection>(
    connections: Vec<Option<C>>,
    redis_url: &str,
    expected: &[u8],
    operation_timeout: Duration,
) -> Result<Vec<C>, String> {
    let mut restored = Vec::with_capacity(connections.len());
    for (index, connection) in connections.into_iter().enumerate() {
        if let Some(mut connection) = connection
            && matches!(
                tokio::time::timeout(operation_timeout, connection.get_fixture(expected)).await,
                Ok(Ok(()))
            )
        {
            restored.push(connection);
            continue;
        }

        let mut connection = tokio::time::timeout(operation_timeout, C::connect(redis_url))
            .await
            .map_err(|_| format!("replacement connection {index} timed out"))?
            .map_err(|error| format!("replace warmup connection {index}: {error}"))?;
        tokio::time::timeout(operation_timeout, connection.get_fixture(expected))
            .await
            .map_err(|_| format!("replacement connection {index} validation timed out"))?
            .map_err(|error| format!("validate replacement connection {index}: {error}"))?;
        restored.push(connection);
    }
    Ok(restored)
}

async fn run_window<C: ProbeConnection>(
    connections: Vec<C>,
    duration: Duration,
    drain_timeout: Duration,
    target_ops_per_sec: u64,
    expected: Arc<[u8]>,
    error_mode: WindowErrorMode,
) -> Result<(Vec<Option<C>>, WindowStats), String> {
    let worker_count = connections.len();
    let start = TokioInstant::now();
    let launch_deadline = start
        .checked_add(duration)
        .ok_or_else(|| "resource window duration exceeds the clock range".to_owned())?;
    let drain_deadline = launch_deadline
        .checked_add(drain_timeout)
        .ok_or_else(|| "resource drain timeout exceeds the clock range".to_owned())?;

    let mut handles: Vec<JoinHandle<WorkerOutcome<C>>> = Vec::with_capacity(worker_count);
    for (worker_index, mut connection) in connections.into_iter().enumerate() {
        let expected = expected.clone();
        handles.push(tokio::spawn(async move {
            let mut stats = WindowStats::default();
            let mut fatal_error = None;
            let mut ordinal = worker_index;
            while let Some(offset) = aggregate_schedule_offset(ordinal, target_ops_per_sec) {
                let Some(scheduled_at) = start.checked_add(offset) else {
                    break;
                };
                if scheduled_at >= launch_deadline {
                    break;
                }

                sleep_until(scheduled_at).await;
                if TokioInstant::now() >= launch_deadline {
                    break;
                }

                stats.attempted += 1;
                match timeout_at(drain_deadline, connection.get_fixture(&expected)).await {
                    Ok(Ok(())) => stats.successful += 1,
                    Ok(Err(error)) => {
                        stats.errors += 1;
                        if matches!(error_mode, WindowErrorMode::Fatal) {
                            fatal_error = Some(error);
                            break;
                        }
                    }
                    Err(_) => {
                        stats.cutoff += 1;
                        return WorkerOutcome {
                            connection: None,
                            stats,
                            fatal_error,
                        };
                    }
                }
                let Some(next) = ordinal.checked_add(worker_count) else {
                    break;
                };
                ordinal = next;
                if TokioInstant::now() >= launch_deadline {
                    break;
                }
            }
            WorkerOutcome {
                connection: Some(connection),
                stats,
                fatal_error,
            }
        }));
    }

    let mut returned = Vec::with_capacity(worker_count);
    let mut aggregate = WindowStats::default();
    let mut first_error = None;
    for (worker_index, handle) in handles.into_iter().enumerate() {
        let outcome = match handle.await {
            Ok(outcome) => outcome,
            Err(error) => {
                first_error.get_or_insert_with(|| {
                    format!("resource worker {worker_index} failed: {error}")
                });
                continue;
            }
        };
        returned.push(outcome.connection);
        aggregate.add(outcome.stats);
        if let Some(error) = outcome.fatal_error {
            first_error.get_or_insert_with(|| {
                format!("resource worker {worker_index} returned an error: {error}")
            });
        }
    }
    // Preserve the launch window when the final centered slot is earlier.
    sleep_until(launch_deadline).await;
    if aggregate.attempted != aggregate.successful + aggregate.errors + aggregate.cutoff {
        return Err("resource worker accounting invariant violated".to_owned());
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok((returned, aggregate))
}

/// Center each operation in its aggregate-rate time slice. Workers take every
/// Nth slice, where N is the connection count, so low rates do not make every
/// connection wake at the same instant or wait for a whole per-worker period.
fn aggregate_schedule_offset(ordinal: usize, target_ops_per_sec: u64) -> Option<Duration> {
    let numerator = (ordinal as u128)
        .checked_mul(2)?
        .checked_add(1)?
        .checked_mul(1_000_000_000)?;
    let denominator = u128::from(target_ops_per_sec).checked_mul(2)?;
    if denominator == 0 {
        return None;
    }
    let nanoseconds = numerator / denominator;
    let seconds = nanoseconds / 1_000_000_000;
    let subsec_nanos = (nanoseconds % 1_000_000_000) as u32;
    Some(Duration::new(u64::try_from(seconds).ok()?, subsec_nanos))
}

#[cfg(unix)]
fn usage_snapshot() -> Result<UsageSnapshot, String> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the pointed-to rusage on success, and the
    // pointer is valid for the duration of the call.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(format!(
            "getrusage failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the successful getrusage call above initialized this value.
    let usage = unsafe { usage.assume_init() };
    let user = timeval_seconds(usage.ru_utime);
    let system = timeval_seconds(usage.ru_stime);
    #[cfg(target_os = "macos")]
    let peak_rss_bytes = usage.ru_maxrss.max(0) as u64;
    #[cfg(not(target_os = "macos"))]
    let peak_rss_bytes = (usage.ru_maxrss.max(0) as u64).saturating_mul(1_024);

    Ok(UsageSnapshot {
        peak_rss_bytes,
        cpu_seconds: user + system,
    })
}

#[cfg(unix)]
fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

#[cfg(not(unix))]
fn usage_snapshot() -> Result<UsageSnapshot, String> {
    Err("resource-bench currently supports Unix hosts".to_owned())
}

fn print_human(report: &ProbeReport) {
    println!("client: {}", report.client);
    println!(
        "rss: {} bytes across {} connections ({:.1} bytes/connection)",
        report.rss.connection_delta_bytes,
        report.config.connections,
        report.rss.bytes_per_connection
    );
    println!(
        "cpu: {:.1}% at {:.1} successful ops/s (target {}, {} errors, {} cutoffs)",
        report.cpu.process_cpu_percent,
        report.cpu.achieved_ops_per_sec,
        report.cpu.target_ops_per_sec,
        report.cpu.errors,
        report.cpu.cutoff_ops
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct FakeConnection;

    #[async_trait]
    impl ProbeConnection for FakeConnection {
        async fn connect(_url: &str) -> Result<Self, String> {
            Ok(Self)
        }

        async fn set_fixture(&mut self, _value: &str) -> Result<(), String> {
            Ok(())
        }

        async fn get_fixture(&mut self, _expected: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }

    struct DelayedConnection;

    #[async_trait]
    impl ProbeConnection for DelayedConnection {
        async fn connect(_url: &str) -> Result<Self, String> {
            Ok(Self)
        }

        async fn set_fixture(&mut self, _value: &str) -> Result<(), String> {
            Ok(())
        }

        async fn get_fixture(&mut self, _expected: &[u8]) -> Result<(), String> {
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok(())
        }
    }

    static REPLACEMENT_GENERATION: AtomicU64 = AtomicU64::new(1);

    struct CancellationSensitiveConnection {
        generation: u64,
        canceled: Arc<AtomicBool>,
    }

    struct CancellationGuard {
        canceled: Arc<AtomicBool>,
        completed: bool,
    }

    impl Drop for CancellationGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.canceled.store(true, Ordering::SeqCst);
            }
        }
    }

    #[async_trait]
    impl ProbeConnection for CancellationSensitiveConnection {
        async fn connect(_url: &str) -> Result<Self, String> {
            Ok(Self {
                generation: REPLACEMENT_GENERATION.fetch_add(1, Ordering::SeqCst),
                canceled: Arc::new(AtomicBool::new(false)),
            })
        }

        async fn set_fixture(&mut self, _value: &str) -> Result<(), String> {
            Ok(())
        }

        async fn get_fixture(&mut self, _expected: &[u8]) -> Result<(), String> {
            if self.generation == 0 {
                let mut guard = CancellationGuard {
                    canceled: self.canceled.clone(),
                    completed: false,
                };
                tokio::time::sleep(Duration::from_secs(10)).await;
                guard.completed = true;
            }
            Ok(())
        }
    }

    struct ErrorConnection;

    #[async_trait]
    impl ProbeConnection for ErrorConnection {
        async fn connect(_url: &str) -> Result<Self, String> {
            Ok(Self)
        }

        async fn set_fixture(&mut self, _value: &str) -> Result<(), String> {
            Ok(())
        }

        async fn get_fixture(&mut self, _expected: &[u8]) -> Result<(), String> {
            Err("warmup read failed".to_owned())
        }
    }

    #[test]
    fn window_stats_adds_every_counter() {
        let mut left = WindowStats {
            attempted: 3,
            successful: 2,
            errors: 1,
            cutoff: 0,
        };
        left.add(WindowStats {
            attempted: 5,
            successful: 4,
            errors: 1,
            cutoff: 0,
        });
        assert_eq!(left.attempted, 8);
        assert_eq!(left.successful, 6);
        assert_eq!(left.errors, 2);
    }

    #[cfg(unix)]
    #[test]
    fn usage_snapshot_has_nonzero_cpu_or_rss() {
        let usage = usage_snapshot().expect("getrusage should work on the test host");
        assert!(usage.peak_rss_bytes > 0 || usage.cpu_seconds > 0.0);
    }

    #[cfg(not(unix))]
    #[test]
    fn usage_snapshot_reports_unsupported_host() {
        let error = match usage_snapshot() {
            Ok(_) => panic!("resource usage sampling unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(error, "resource-bench currently supports Unix hosts");
    }

    #[test]
    fn report_serialization_never_exposes_redis_credentials() {
        let cases: &[(&str, &[&str])] = &[
            (
                "redis://alice:ordinary-secret@private-host:6380/2?token=query-secret#fragment-secret",
                &[
                    "alice",
                    "ordinary-secret",
                    "private-host",
                    "6380",
                    "query-secret",
                    "fragment-secret",
                ],
            ),
            (
                "rediss://alice:encoded%40secret@private-host:6381/3",
                &["encoded%40secret", "private-host", "alice", "6381"],
            ),
            (
                "redis://alice:slash-secret/value@private-host:6380/2",
                &["slash-secret", "private-host", "alice"],
            ),
            (
                "redis://alice:question-secret?value@private-host:6380/2",
                &["question-secret", "private-host", "alice"],
            ),
            (
                "redis://alice:hash-secret#value@private-host:6380/2",
                &["hash-secret", "private-host", "alice"],
            ),
            (
                "redis://deployment.internal:6379/0",
                &["deployment.internal", "6379"],
            ),
        ];

        for (raw, secrets) in cases {
            let json = serialized_config_for_url(raw);
            let report: serde_json::Value = serde_json::from_str(&json).expect("parse report JSON");
            assert_eq!(report["redis_endpoint"], "<redacted>", "{json}");
            assert!(!json.contains(raw), "{json}");
            for secret in *secrets {
                assert!(!json.contains(secret), "{json}");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn aggregate_schedule_handles_rates_below_connection_count() {
        let connections = (0..100).map(|_| FakeConnection).collect();
        let started = TokioInstant::now();
        let (_, stats) = run_window(
            connections,
            Duration::from_secs(10),
            Duration::from_secs(1),
            1,
            Arc::from(&b"x"[..]),
            WindowErrorMode::Count,
        )
        .await
        .expect("run fake resource window");

        assert_eq!(stats.attempted, 10);
        assert_eq!(stats.successful, 10);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.cutoff, 0);
        assert_eq!(
            stats.attempted,
            stats.successful + stats.errors + stats.cutoff
        );
        assert!(started.elapsed() >= Duration::from_secs(9));
        assert!(started.elapsed() <= Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_tail_operation_drains_without_becoming_a_client_error() {
        let started = TokioInstant::now();
        let (_, stats) = run_window(
            vec![DelayedConnection],
            Duration::from_millis(100),
            Duration::from_millis(100),
            100,
            Arc::from(&b"x"[..]),
            WindowErrorMode::Count,
        )
        .await
        .expect("run delayed resource window");

        assert_eq!(stats.attempted, 2);
        assert_eq!(stats.successful, 2);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.cutoff, 0);
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(started.elapsed() <= Duration::from_millis(200));
    }

    #[tokio::test(start_paused = true)]
    async fn canceled_warmup_connection_is_replaced_before_measurement() {
        REPLACEMENT_GENERATION.store(1, Ordering::SeqCst);
        let canceled = Arc::new(AtomicBool::new(false));
        let initial = CancellationSensitiveConnection {
            generation: 0,
            canceled: canceled.clone(),
        };
        let (connections, stats) = run_window(
            vec![initial],
            Duration::from_secs(1),
            Duration::from_millis(100),
            1,
            Arc::from(&b"x"[..]),
            WindowErrorMode::Fatal,
        )
        .await
        .expect("warmup cutoff is accounted separately");

        assert_eq!(stats.attempted, 1);
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.cutoff, 1);
        assert!(connections[0].is_none());
        assert!(canceled.load(Ordering::SeqCst));

        let restored = restore_after_warmup(
            connections,
            "redis://unused/",
            b"x",
            Duration::from_millis(100),
        )
        .await
        .expect("replace canceled warmup connection");
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].generation, 1);
        assert!(!restored[0].canceled.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn real_warmup_errors_are_fatal() {
        let result = run_window(
            vec![ErrorConnection],
            Duration::from_secs(1),
            Duration::from_millis(100),
            1,
            Arc::from(&b"x"[..]),
            WindowErrorMode::Fatal,
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("warmup client error must abort measurement"),
        };
        assert!(error.contains("warmup read failed"));
    }
}
