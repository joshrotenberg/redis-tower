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
use tokio::time::{MissedTickBehavior, interval_at};

/// Key used by every client implementation during the fixed-rate workload.
pub const FIXTURE_KEY: &str = "resource-bench:payload";

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
#[derive(Clone, Debug, Serialize)]
pub struct ProbeConfig {
    /// Redis URL used by the subject client.
    pub redis_url: String,
    /// Number of independent live connections retained during the probe.
    pub connections: usize,
    /// Aggregate GET rate offered across all connections.
    pub target_ops_per_sec: u64,
    /// Unmeasured CPU-workload warmup.
    pub warmup_secs: u64,
    /// Measured CPU-workload duration.
    pub duration_secs: u64,
    /// Expected value length for every successful GET.
    pub payload_bytes: usize,
}

impl ProbeConfig {
    /// Read configuration from the `RESOURCE_*` environment variables.
    pub fn from_env() -> Result<Self, String> {
        let config = Self {
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_owned()),
            connections: positive_env("RESOURCE_CONNECTIONS", 100)?,
            target_ops_per_sec: positive_env("RESOURCE_TARGET_OPS_PER_SEC", 5_000)?,
            warmup_secs: env_number("RESOURCE_WARMUP_SECS", 2)?,
            duration_secs: positive_env("RESOURCE_DURATION_SECS", 10)?,
            payload_bytes: positive_env("RESOURCE_PAYLOAD_BYTES", 1_024)?,
        };

        if config.target_ops_per_sec > 1_000_000_000 {
            return Err("RESOURCE_TARGET_OPS_PER_SEC must not exceed 1000000000".to_owned());
        }
        Ok(config)
    }
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
#[derive(Debug, Serialize)]
pub struct ProbeReport {
    /// Artifact schema version.
    pub schema_version: u32,
    /// Subject client name.
    pub client: &'static str,
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
    /// Attempted operations divided by measured wall time.
    pub attempted_ops_per_sec: f64,
    /// Successful, payload-validated operations divided by wall time.
    pub achieved_ops_per_sec: f64,
    /// Total attempts in the measured window.
    pub attempted_ops: u64,
    /// Successful payload-validated GETs.
    pub successful_ops: u64,
    /// Command failures, misses, and payload mismatches.
    pub errors: u64,
    /// Measured wall time.
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
}

impl WindowStats {
    fn add(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.successful += other.successful;
        self.errors += other.errors;
    }
}

/// Run one isolated client probe and print either JSON (`--json`) or a concise
/// human-readable summary.
pub async fn run_client<C: ProbeConnection>(client: &'static str) -> Result<(), String> {
    let config = ProbeConfig::from_env()?;
    let json = env::args().any(|arg| arg == "--json");
    let report = measure::<C>(client, config).await?;

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
    if config.warmup_secs > 0 {
        let (returned, _) = run_window(
            connections,
            Duration::from_secs(config.warmup_secs),
            config.target_ops_per_sec,
            payload.clone(),
        )
        .await?;
        connections = returned;
    }

    let cpu_before = usage_snapshot()?;
    let wall_start = Instant::now();
    let (connections, window) = run_window(
        connections,
        Duration::from_secs(config.duration_secs),
        config.target_ops_per_sec,
        payload,
    )
    .await?;
    let wall_seconds = wall_start.elapsed().as_secs_f64();
    let cpu_after = usage_snapshot()?;
    let process_cpu_seconds = (cpu_after.cpu_seconds - cpu_before.cpu_seconds).max(0.0);
    let target_ops_per_sec = config.target_ops_per_sec;

    // Retain all connections until every measurement has been captured.
    let _connections = connections;

    Ok(ProbeReport {
        schema_version: 1,
        client,
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
            attempted_ops_per_sec: window.attempted as f64 / wall_seconds,
            achieved_ops_per_sec: window.successful as f64 / wall_seconds,
            attempted_ops: window.attempted,
            successful_ops: window.successful,
            errors: window.errors,
            wall_seconds,
            process_cpu_seconds,
            process_cpu_percent: process_cpu_seconds / wall_seconds * 100.0,
        },
    })
}

async fn run_window<C: ProbeConnection>(
    connections: Vec<C>,
    duration: Duration,
    target_ops_per_sec: u64,
    expected: Arc<[u8]>,
) -> Result<(Vec<C>, WindowStats), String> {
    let worker_count = connections.len();
    let per_worker_rate = target_ops_per_sec as f64 / worker_count as f64;
    let period = Duration::from_secs_f64((1.0 / per_worker_rate).max(0.000_001));
    let deadline = tokio::time::Instant::now() + duration;

    let mut handles: Vec<JoinHandle<(C, WindowStats)>> = Vec::with_capacity(worker_count);
    for mut connection in connections {
        let expected = expected.clone();
        handles.push(tokio::spawn(async move {
            let mut stats = WindowStats::default();
            let mut ticker = interval_at(tokio::time::Instant::now() + period, period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                stats.attempted += 1;
                match connection.get_fixture(&expected).await {
                    Ok(()) => stats.successful += 1,
                    Err(_) => stats.errors += 1,
                }
            }
            (connection, stats)
        }));
    }

    let mut returned = Vec::with_capacity(worker_count);
    let mut aggregate = WindowStats::default();
    for handle in handles {
        let (connection, stats) = handle
            .await
            .map_err(|error| format!("resource worker failed: {error}"))?;
        returned.push(connection);
        aggregate.add(stats);
    }
    Ok((returned, aggregate))
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
        "cpu: {:.1}% at {:.1} successful ops/s (target {}, {} errors)",
        report.cpu.process_cpu_percent,
        report.cpu.achieved_ops_per_sec,
        report.cpu.target_ops_per_sec,
        report.cpu.errors
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_stats_adds_every_counter() {
        let mut left = WindowStats {
            attempted: 3,
            successful: 2,
            errors: 1,
        };
        left.add(WindowStats {
            attempted: 5,
            successful: 4,
            errors: 1,
        });
        assert_eq!(left.attempted, 8);
        assert_eq!(left.successful, 6);
        assert_eq!(left.errors, 2);
    }

    #[test]
    fn usage_snapshot_has_nonzero_cpu_or_rss() {
        let usage = usage_snapshot().expect("getrusage should work on the test host");
        assert!(usage.peak_rss_bytes > 0 || usage.cpu_seconds > 0.0);
    }
}
