//! Opt-in Redis Cluster topology-churn workloads.
//!
//! The throughput benchmark deliberately keeps its long-standing defaults and
//! output schema.  This module contains the separate reshard/failover runner:
//! two async clients are driven against the same affected slot while an
//! injected topology event is in progress, then their stable, churn, and
//! recovery windows are reported independently.

use std::future::Future;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use redis::AsyncCommands;
use redis_tower::metrics_layer::{
    ClusterRedirectKind, ClusterTopologyRefreshOutcome, ErrorKind, MetricsRecorder,
};
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_commands::{Get as TowerGet, Set as TowerSet};
use serde::Serialize;

use crate::clients::ClientKind;
use crate::runner::{mean, new_histogram, std_dev};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChurnScenario {
    Reshard,
    Failover,
}

impl ChurnScenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reshard => "reshard",
            Self::Failover => "failover",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChurnWorkload {
    Get,
    Set,
}

impl ChurnWorkload {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "set",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChurnConfig {
    /// Unmeasured client warmup before the stable baseline.
    pub warmup: Duration,
    /// Stable window used as the latency-delta baseline.
    pub baseline: Duration,
    /// Measured window after the topology event has completed.
    pub recovery: Duration,
    pub concurrency: usize,
    pub workload: ChurnWorkload,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyReport {
    pub samples: u64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
}

impl LatencyReport {
    fn from_histogram(histogram: &Histogram<u64>) -> Self {
        if histogram.is_empty() {
            return Self::default();
        }
        Self {
            samples: histogram.len(),
            p50_us: histogram.value_at_quantile(0.50) as f64,
            p90_us: histogram.value_at_quantile(0.90) as f64,
            p99_us: histogram.value_at_quantile(0.99) as f64,
            p999_us: histogram.value_at_quantile(0.999) as f64,
            max_us: histogram.max() as f64,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PhaseReport {
    pub successes: u64,
    pub errors: u64,
    pub latency: LatencyReport,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RedirectReport {
    pub ask: u64,
    pub moved: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TopologyRefreshReport {
    pub success: u64,
    pub partial: u64,
    pub error: u64,
    pub mean_duration_ms: f64,
    pub max_duration_ms: f64,
}

#[derive(Clone, Debug, Default)]
pub struct ChurnEventReport {
    /// Time spent injecting the event, including bounded topology convergence.
    pub event_duration: Duration,
    /// Time from injection start until the harness observed the new owner.
    pub topology_convergence: Option<Duration>,
    /// A compact human-readable summary (slot and old/new owner, for example).
    pub topology_change: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChurnReport {
    pub scenario: ChurnScenario,
    pub client: String,
    pub workload: ChurnWorkload,
    pub concurrency: usize,
    pub stable: PhaseReport,
    pub churn: PhaseReport,
    pub recovery: PhaseReport,
    /// Failed operations after injection began.  These are reported rather
    /// than asserted on: local scheduler and Redis election timing vary.
    pub dropped_ops: u64,
    /// Time from the exact fault/redirect trigger to the first successful
    /// operation completed after it. This remains meaningful when a client
    /// stalls through failover without surfacing an error.
    pub time_to_first_success_ms: Option<f64>,
    /// Time from the final post-trigger error until the first success observed
    /// after it. `None` when the client surfaced no errors or never recovered.
    pub recovery_after_error_ms: Option<f64>,
    /// Time between the first and last observed post-injection errors.
    pub error_window_ms: Option<f64>,
    pub event_duration_ms: f64,
    pub topology_convergence_ms: Option<f64>,
    pub topology_change: Option<String>,
    pub p99_delta_us: Option<f64>,
    pub p99_delta_pct: Option<f64>,
    pub p999_delta_us: Option<f64>,
    pub p999_delta_pct: Option<f64>,
    /// Redis-tower exposes redirect hooks; redis-rs does not, so this is
    /// `None` for redis-rs rather than a misleading zero.
    pub redirects: Option<RedirectReport>,
    /// Redis-tower exposes topology-refresh hooks; redis-rs does not.
    pub topology_refreshes: Option<TopologyRefreshReport>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AggregatedChurnReport {
    pub scenario: ChurnScenario,
    pub client: String,
    pub workload: ChurnWorkload,
    pub concurrency: usize,
    pub runs: usize,
    pub stable_p99_us_mean: Option<f64>,
    pub stable_p999_us_mean: Option<f64>,
    pub churn_p99_us_mean: Option<f64>,
    pub churn_p999_us_mean: Option<f64>,
    pub p99_delta_us_mean: Option<f64>,
    pub p99_delta_pct_mean: Option<f64>,
    pub p999_delta_us_mean: Option<f64>,
    pub p999_delta_pct_mean: Option<f64>,
    pub dropped_ops: u64,
    pub stable_successes: u64,
    pub stable_errors: u64,
    pub churn_successes: u64,
    pub churn_errors: u64,
    pub recovery_successes: u64,
    pub recovery_errors: u64,
    /// Error rate across the conservative churn and recovery phases.
    pub churn_error_rate_pct: f64,
    pub first_success_runs: usize,
    pub time_to_first_success_ms_mean: Option<f64>,
    /// Runs that surfaced at least one error after the confirmed event marker.
    pub runs_with_errors: usize,
    /// Confirmed-error runs with a subsequent successful completion.
    pub recovered_after_error_runs: usize,
    pub recovery_after_error_ms_mean: Option<f64>,
    pub error_window_ms_mean: Option<f64>,
    pub topology_convergence_ms_mean: Option<f64>,
    pub topology_changes: Vec<String>,
    pub event_duration_ms_mean: f64,
    pub event_duration_ms_stddev: f64,
    pub redirects: Option<RedirectReport>,
    pub topology_refreshes: Option<TopologyRefreshReport>,
}

/// Aggregate repeated runs of one scenario/client cell. Percentiles remain
/// informational: the benchmark intentionally has no timing assertions.
pub fn aggregate_churn(reports: &[ChurnReport]) -> AggregatedChurnReport {
    let first = reports.first().expect("at least one churn report per cell");
    let values = |f: fn(&ChurnReport) -> f64| reports.iter().map(f).collect::<Vec<_>>();
    let optional_mean = |f: fn(&ChurnReport) -> Option<f64>| {
        let values = reports.iter().filter_map(f).collect::<Vec<_>>();
        (!values.is_empty()).then(|| mean(&values))
    };
    let event_durations = values(|r| r.event_duration_ms);
    let first_success_values = reports
        .iter()
        .filter_map(|report| report.time_to_first_success_ms)
        .collect::<Vec<_>>();
    let runs_with_errors = reports
        .iter()
        .filter(|report| report.error_window_ms.is_some())
        .count();
    let recovered_after_error_values = reports
        .iter()
        .filter_map(|report| report.recovery_after_error_ms)
        .collect::<Vec<_>>();

    let redirects = reports
        .iter()
        .try_fold(RedirectReport::default(), |mut total, report| {
            let redirects = report.redirects.as_ref()?;
            total.ask += redirects.ask;
            total.moved += redirects.moved;
            Some(total)
        });
    let topology_refreshes =
        reports
            .iter()
            .try_fold(TopologyRefreshReport::default(), |mut total, report| {
                let refreshes = report.topology_refreshes.as_ref()?;
                let prior_count = total.success + total.partial + total.error;
                let count = refreshes.success + refreshes.partial + refreshes.error;
                let combined_count = prior_count + count;
                if combined_count > 0 {
                    total.mean_duration_ms = (total.mean_duration_ms * prior_count as f64
                        + refreshes.mean_duration_ms * count as f64)
                        / combined_count as f64;
                }
                total.success += refreshes.success;
                total.partial += refreshes.partial;
                total.error += refreshes.error;
                total.max_duration_ms = total.max_duration_ms.max(refreshes.max_duration_ms);
                Some(total)
            });

    AggregatedChurnReport {
        scenario: first.scenario,
        client: first.client.clone(),
        workload: first.workload,
        concurrency: first.concurrency,
        runs: reports.len(),
        stable_p99_us_mean: optional_mean(|r| {
            (r.stable.latency.samples > 0).then_some(r.stable.latency.p99_us)
        }),
        stable_p999_us_mean: optional_mean(|r| {
            (r.stable.latency.samples > 0).then_some(r.stable.latency.p999_us)
        }),
        churn_p99_us_mean: optional_mean(|r| {
            (r.churn.latency.samples > 0).then_some(r.churn.latency.p99_us)
        }),
        churn_p999_us_mean: optional_mean(|r| {
            (r.churn.latency.samples > 0).then_some(r.churn.latency.p999_us)
        }),
        p99_delta_us_mean: optional_mean(|r| r.p99_delta_us),
        p99_delta_pct_mean: optional_mean(|r| r.p99_delta_pct),
        p999_delta_us_mean: optional_mean(|r| r.p999_delta_us),
        p999_delta_pct_mean: optional_mean(|r| r.p999_delta_pct),
        dropped_ops: reports.iter().map(|r| r.dropped_ops).sum(),
        stable_successes: reports.iter().map(|r| r.stable.successes).sum(),
        stable_errors: reports.iter().map(|r| r.stable.errors).sum(),
        churn_successes: reports.iter().map(|r| r.churn.successes).sum(),
        churn_errors: reports.iter().map(|r| r.churn.errors).sum(),
        recovery_successes: reports.iter().map(|r| r.recovery.successes).sum(),
        recovery_errors: reports.iter().map(|r| r.recovery.errors).sum(),
        churn_error_rate_pct: {
            let errors = reports.iter().map(|r| r.dropped_ops).sum::<u64>();
            let successes = reports
                .iter()
                .map(|r| r.churn.successes + r.recovery.successes)
                .sum::<u64>();
            if errors + successes == 0 {
                0.0
            } else {
                errors as f64 / (errors + successes) as f64 * 100.0
            }
        },
        first_success_runs: first_success_values.len(),
        time_to_first_success_ms_mean: (first_success_values.len() == reports.len())
            .then(|| mean(&first_success_values)),
        runs_with_errors,
        recovered_after_error_runs: recovered_after_error_values.len(),
        recovery_after_error_ms_mean: (runs_with_errors > 0
            && recovered_after_error_values.len() == runs_with_errors)
            .then(|| mean(&recovered_after_error_values)),
        error_window_ms_mean: optional_mean(|r| r.error_window_ms),
        topology_convergence_ms_mean: optional_mean(|r| r.topology_convergence_ms),
        topology_changes: reports
            .iter()
            .filter_map(|report| report.topology_change.clone())
            .collect(),
        event_duration_ms_mean: mean(&event_durations),
        event_duration_ms_stddev: std_dev(&event_durations),
        redirects,
        topology_refreshes,
    }
}

#[derive(Default)]
pub struct ChurnMetrics {
    ask_redirects: AtomicU64,
    moved_redirects: AtomicU64,
    refresh_success: AtomicU64,
    refresh_partial: AtomicU64,
    refresh_error: AtomicU64,
    refresh_duration_us: AtomicU64,
    refresh_max_us: AtomicU64,
}

impl ChurnMetrics {
    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            ask: self.ask_redirects.load(Ordering::Relaxed),
            moved: self.moved_redirects.load(Ordering::Relaxed),
            refresh_success: self.refresh_success.load(Ordering::Relaxed),
            refresh_partial: self.refresh_partial.load(Ordering::Relaxed),
            refresh_error: self.refresh_error.load(Ordering::Relaxed),
            refresh_duration_us: self.refresh_duration_us.load(Ordering::Relaxed),
            refresh_max_us: self.refresh_max_us.load(Ordering::Relaxed),
        }
    }
}

impl MetricsRecorder for ChurnMetrics {
    fn command_completed(&self, _command: &str, _duration: Duration, _error: Option<ErrorKind>) {}

    fn cluster_redirected(&self, kind: ClusterRedirectKind) {
        match kind {
            ClusterRedirectKind::Ask => self.ask_redirects.fetch_add(1, Ordering::Relaxed),
            ClusterRedirectKind::Moved => self.moved_redirects.fetch_add(1, Ordering::Relaxed),
        };
    }

    fn cluster_topology_refresh_completed(
        &self,
        duration: Duration,
        outcome: ClusterTopologyRefreshOutcome,
    ) {
        match outcome {
            ClusterTopologyRefreshOutcome::Success => {
                self.refresh_success.fetch_add(1, Ordering::Relaxed)
            }
            ClusterTopologyRefreshOutcome::Partial => {
                self.refresh_partial.fetch_add(1, Ordering::Relaxed)
            }
            ClusterTopologyRefreshOutcome::Error => {
                self.refresh_error.fetch_add(1, Ordering::Relaxed)
            }
        };
        let duration_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.refresh_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.refresh_max_us
            .fetch_max(duration_us, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MetricsSnapshot {
    ask: u64,
    moved: u64,
    refresh_success: u64,
    refresh_partial: u64,
    refresh_error: u64,
    refresh_duration_us: u64,
    refresh_max_us: u64,
}

impl MetricsSnapshot {
    fn reports(self) -> (RedirectReport, TopologyRefreshReport) {
        let count = self.refresh_success + self.refresh_partial + self.refresh_error;
        (
            RedirectReport {
                ask: self.ask,
                moved: self.moved,
            },
            TopologyRefreshReport {
                success: self.refresh_success,
                partial: self.refresh_partial,
                error: self.refresh_error,
                mean_duration_ms: if count == 0 {
                    0.0
                } else {
                    self.refresh_duration_us as f64 / count as f64 / 1_000.0
                },
                max_duration_ms: self.refresh_max_us as f64 / 1_000.0,
            },
        )
    }
}

#[derive(Clone)]
pub enum ChurnClient {
    TowerMux {
        client: MultiplexedClusterClient,
        metrics: Arc<ChurnMetrics>,
    },
    RedisRsAsync(redis::cluster_async::ClusterConnection),
}

impl ChurnClient {
    pub async fn connect_tower_mux(seed: &str) -> Result<Self, String> {
        let metrics = Arc::new(ChurnMetrics::default());
        let client = MultiplexedClusterClient::builder(seed)
            .metrics_recorder(metrics.clone())
            .connect()
            .await
            .map_err(|error| error.to_string())?;
        Ok(Self::TowerMux { client, metrics })
    }

    pub async fn connect_redis_rs(seed_urls: &[String]) -> Result<Self, String> {
        let client = redis::cluster::ClusterClient::new(seed_urls.to_vec())
            .map_err(|error| error.to_string())?;
        client
            .get_async_connection()
            .await
            .map(Self::RedisRsAsync)
            .map_err(|error| error.to_string())
    }

    fn kind(&self) -> ClientKind {
        match self {
            Self::TowerMux { .. } => ClientKind::RedisTowerMux,
            Self::RedisRsAsync(_) => ClientKind::RedisRsAsync,
        }
    }

    fn metrics(&self) -> Option<MetricsSnapshot> {
        match self {
            Self::TowerMux { metrics, .. } => Some(metrics.snapshot()),
            Self::RedisRsAsync(_) => None,
        }
    }

    async fn execute(&mut self, workload: ChurnWorkload, key: &str) -> Result<(), ()> {
        match self {
            Self::TowerMux { client, .. } => match workload {
                ChurnWorkload::Get => client
                    .execute(TowerGet::new(key))
                    .await
                    .and_then(|value| {
                        (value.as_deref() == Some(b"value".as_slice()))
                            .then_some(())
                            .ok_or_else(|| {
                                redis_tower_core::RedisError::Redis(
                                    "benchmark key missing or corrupt".into(),
                                )
                            })
                    })
                    .map_err(|_| ()),
                ChurnWorkload::Set => client
                    .execute(TowerSet::new(key, "value"))
                    .await
                    .map(|_| ())
                    .map_err(|_| ()),
            },
            Self::RedisRsAsync(client) => match workload {
                ChurnWorkload::Get => client
                    .get::<_, Option<Vec<u8>>>(key)
                    .await
                    .and_then(|value| {
                        (value.as_deref() == Some(b"value".as_slice()))
                            .then_some(())
                            .ok_or_else(|| {
                                redis::RedisError::from((
                                    redis::ErrorKind::UnexpectedReturnType,
                                    "benchmark key missing or corrupt",
                                ))
                            })
                    })
                    .map_err(|_| ()),
                ChurnWorkload::Set => client.set::<_, _, ()>(key, "value").await.map_err(|_| ()),
            },
        }
    }

    pub(crate) async fn shutdown(self) {
        if let Self::TowerMux { client, .. } = self {
            client.shutdown().await;
        }
    }
}

/// Seed the single affected-slot key before clients capture their stable
/// topology and baseline. The value is intentionally tiny so this benchmark
/// measures redirect/recovery behavior rather than payload transfer.
pub async fn seed_key(owner_addr: &str, key: &str) -> Result<(), String> {
    let client =
        redis::Client::open(format!("redis://{owner_addr}/")).map_err(|error| error.to_string())?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| error.to_string())?;
    connection
        .set::<_, _, ()>(key, "value")
        .await
        .map_err(|error| error.to_string())?;
    let replicas: i64 = redis::cmd("WAIT")
        .arg(1)
        .arg(5_000)
        .query_async(&mut connection)
        .await
        .map_err(|error| error.to_string())?;
    if replicas < 1 {
        return Err(format!(
            "benchmark key did not reach a replica before failover (WAIT returned {replicas})"
        ));
    }
    Ok(())
}

const PHASE_WARMUP: u8 = 0;
const PHASE_STABLE: u8 = 1;
const PHASE_CHURN: u8 = 2;
const PHASE_RECOVERY: u8 = 3;
const PHASE_STOP: u8 = 4;

struct WorkerStats {
    stable: PhaseStats,
    churn: PhaseStats,
    recovery: PhaseStats,
    post_trigger_events: Vec<RecoveryEvent>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecoveryEvent {
    elapsed_ns: u64,
    success: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct RecoveryState {
    first_success_ns: Option<u64>,
    first_error_ns: Option<u64>,
    last_error_ns: Option<u64>,
    first_success_after_last_error_ns: Option<u64>,
}

struct WorkerState {
    stats: WorkerStats,
    in_flight_phase: Option<u8>,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            stable: PhaseStats::new(),
            churn: PhaseStats::new(),
            recovery: PhaseStats::new(),
            post_trigger_events: Vec::new(),
        }
    }

    fn phase_mut(&mut self, phase: u8) -> Option<&mut PhaseStats> {
        match phase {
            PHASE_STABLE => Some(&mut self.stable),
            PHASE_CHURN => Some(&mut self.churn),
            PHASE_RECOVERY => Some(&mut self.recovery),
            _ => None,
        }
    }

    fn record(&mut self, phase: u8, success: bool, latency: Duration, elapsed_ns: u64) {
        if let Some(window) = self.phase_mut(phase) {
            window.record(success, latency);
            if matches!(phase, PHASE_CHURN | PHASE_RECOVERY) {
                self.post_trigger_events.push(RecoveryEvent {
                    elapsed_ns,
                    success,
                });
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        self.stable.merge(&other.stable);
        self.churn.merge(&other.churn);
        self.recovery.merge(&other.recovery);
        self.post_trigger_events
            .extend_from_slice(&other.post_trigger_events);
    }

    fn recovery_state(&self, trigger_ns: u64) -> RecoveryState {
        let events = self
            .post_trigger_events
            .iter()
            .filter(|event| event.elapsed_ns >= trigger_ns)
            .collect::<Vec<_>>();
        let first_success_ns = self
            .post_trigger_events
            .iter()
            .filter(|event| event.success && event.elapsed_ns >= trigger_ns)
            .map(|event| event.elapsed_ns)
            .min();
        let first_error_ns = events
            .iter()
            .filter(|event| !event.success)
            .map(|event| event.elapsed_ns)
            .min();
        let last_error_ns = events
            .iter()
            .filter(|event| !event.success)
            .map(|event| event.elapsed_ns)
            .max();
        let first_success_after_last_error_ns = last_error_ns.and_then(|last_error| {
            events
                .iter()
                .filter(|event| event.success && event.elapsed_ns >= last_error)
                .map(|event| event.elapsed_ns)
                .min()
        });

        RecoveryState {
            first_success_ns,
            first_error_ns,
            last_error_ns,
            first_success_after_last_error_ns,
        }
    }
}

impl WorkerState {
    fn new() -> Self {
        Self {
            stats: WorkerStats::new(),
            in_flight_phase: None,
        }
    }

    fn begin_operation(&mut self, phase: u8) {
        self.in_flight_phase = Some(phase);
    }

    fn complete_operation(
        &mut self,
        measured_phase: u8,
        success: bool,
        latency: Duration,
        elapsed_ns: u64,
    ) {
        self.stats
            .record(measured_phase, success, latency, elapsed_ns);
        self.in_flight_phase = None;
    }

    fn abort_in_flight(&mut self, elapsed_ns: u64) {
        let Some(started_phase) = self.in_flight_phase.take() else {
            return;
        };
        let measured_phase = completed_phase(started_phase, PHASE_STOP);
        self.stats
            .record(measured_phase, false, Duration::ZERO, elapsed_ns);
    }

    fn take_stats(&mut self) -> WorkerStats {
        std::mem::replace(&mut self.stats, WorkerStats::new())
    }
}

struct PhaseStats {
    successes: u64,
    errors: u64,
    histogram: Histogram<u64>,
}

impl PhaseStats {
    fn new() -> Self {
        Self {
            successes: 0,
            errors: 0,
            histogram: new_histogram(),
        }
    }

    fn record(&mut self, success: bool, latency: Duration) {
        if success {
            self.successes += 1;
            self.histogram
                .saturating_record(latency.as_micros().clamp(1, u128::from(u64::MAX)) as u64);
        } else {
            self.errors += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        self.successes += other.successes;
        self.errors += other.errors;
        let _ = self.histogram.add(&other.histogram);
    }

    fn report(&self) -> PhaseReport {
        PhaseReport {
            successes: self.successes,
            errors: self.errors,
            latency: LatencyReport::from_histogram(&self.histogram),
        }
    }
}

async fn worker_loop(
    mut client: ChurnClient,
    key: Arc<str>,
    workload: ChurnWorkload,
    phase: Arc<AtomicU8>,
    state: Arc<Mutex<WorkerState>>,
    run_started: Instant,
) {
    loop {
        let started_phase = phase.load(Ordering::Acquire);
        if started_phase == PHASE_STOP {
            return;
        }
        {
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            state.begin_operation(started_phase);
        }
        let started = Instant::now();
        let success = client.execute(workload, &key).await.is_ok();
        let finished = Instant::now();
        let finished_phase = phase.load(Ordering::Acquire);
        let measured_phase = completed_phase(started_phase, finished_phase);
        let elapsed_ns = elapsed_ns(run_started, finished);
        {
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            state.complete_operation(
                measured_phase,
                success,
                finished.duration_since(started),
                elapsed_ns,
            );
        }
        if !success {
            // A disconnected client can fail synchronously. A tiny fixed
            // backoff prevents a hot retry loop from monopolizing the runtime
            // and turning "dropped ops" into a CPU-speed benchmark.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

fn elapsed_ns(run_started: Instant, finished: Instant) -> u64 {
    finished
        .duration_since(run_started)
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn completed_phase(started_phase: u8, finished_phase: u8) -> u8 {
    match (started_phase, finished_phase) {
        // An operation that began in the stable window but crossed the exact
        // trigger belongs to churn; otherwise a blocking failover call would
        // inflate the stable baseline and disappear from churn tail latency.
        (PHASE_WARMUP | PHASE_STABLE, PHASE_CHURN | PHASE_RECOVERY | PHASE_STOP) => PHASE_CHURN,
        // Preserve the phase in which an already-measured operation began.
        (phase @ (PHASE_STABLE | PHASE_CHURN | PHASE_RECOVERY), _) => phase,
        // A warmup call finishing in stable remains unmeasured.
        (phase, _) => phase,
    }
}

struct ClientRun {
    kind: ClientKind,
    metrics: Option<MetricsSnapshot>,
    stats: WorkerStats,
}

struct RunningWorker {
    state: Arc<Mutex<WorkerState>>,
    handle: tokio::task::JoinHandle<()>,
}

struct RunningClient {
    kind: ClientKind,
    before_metrics: Option<MetricsSnapshot>,
    client: ChurnClient,
    workers: Vec<RunningWorker>,
}

/// Control passed to the topology-event injector.
///
/// Call [`mark_churn_started`](Self::mark_churn_started) immediately before an
/// operation that can affect workers, then call [`mark_triggered`](Self::mark_triggered)
/// once the key move or process kill is confirmed. The first boundary keeps
/// transition traffic out of the stable baseline; the second is the origin for
/// recovery timing.
#[derive(Clone)]
pub struct ChurnTrigger {
    phase: Arc<AtomicU8>,
    trigger_ns: Arc<AtomicU64>,
    run_started: Instant,
}

impl ChurnTrigger {
    /// Begin the conservative churn phase before applying the topology event.
    pub fn mark_churn_started(&self) {
        self.phase.fetch_max(PHASE_CHURN, Ordering::AcqRel);
    }

    /// Confirm the workload-visible event and start recovery timing.
    /// Repeated calls are harmless; only the first timestamp wins.
    pub fn mark_triggered(&self) {
        self.mark_churn_started();
        let elapsed_ns = self
            .run_started
            .elapsed()
            .as_nanos()
            .clamp(1, u128::from(u64::MAX)) as u64;
        let _ =
            self.trigger_ns
                .compare_exchange(0, elapsed_ns, Ordering::AcqRel, Ordering::Acquire);
    }

    fn elapsed_since_trigger(&self) -> Option<Duration> {
        let trigger_ns = self.trigger_ns.load(Ordering::Acquire);
        (trigger_ns != 0).then(|| {
            Duration::from_nanos(
                self.run_started
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64
                    - trigger_ns,
            )
        })
    }
}

/// Drive all clients concurrently while `inject` performs one topology event.
/// The injector owns the event-specific harness operations; the runner owns
/// phase boundaries and comparable client measurements. The injector must call
/// [`ChurnTrigger::mark_churn_started`] before applying the event and
/// [`ChurnTrigger::mark_triggered`] once it is confirmed.
pub async fn run_churn<F, Fut>(
    scenario: ChurnScenario,
    clients: Vec<ChurnClient>,
    key: String,
    config: ChurnConfig,
    inject: F,
) -> Result<Vec<ChurnReport>, String>
where
    F: FnOnce(ChurnTrigger) -> Fut,
    Fut: Future<Output = Result<ChurnEventReport, String>>,
{
    if clients.is_empty() {
        return Err("at least one churn client is required".into());
    }
    if config.concurrency == 0 {
        return Err("churn concurrency must be at least one".into());
    }

    let phase = Arc::new(AtomicU8::new(PHASE_WARMUP));
    let trigger_ns = Arc::new(AtomicU64::new(0));
    let run_started = Instant::now();
    let key: Arc<str> = Arc::from(key);
    let mut client_handles = Vec::with_capacity(clients.len());
    for client in clients {
        let kind = client.kind();
        let metrics = client.metrics();
        let mut workers = Vec::with_capacity(config.concurrency);
        for _ in 0..config.concurrency {
            let state = Arc::new(Mutex::new(WorkerState::new()));
            let handle = tokio::spawn(worker_loop(
                client.clone(),
                key.clone(),
                config.workload,
                phase.clone(),
                state.clone(),
                run_started,
            ));
            workers.push(RunningWorker { state, handle });
        }
        client_handles.push(RunningClient {
            kind,
            before_metrics: metrics,
            client,
            workers,
        });
    }

    tokio::time::sleep(config.warmup).await;
    phase.store(PHASE_STABLE, Ordering::Release);
    tokio::time::sleep(config.baseline).await;

    let trigger = ChurnTrigger {
        phase: phase.clone(),
        trigger_ns: trigger_ns.clone(),
        run_started,
    };
    let injection = inject(trigger.clone()).await;
    let mut event = match injection {
        Ok(event) => event,
        Err(error) => {
            phase.store(PHASE_STOP, Ordering::Release);
            abort_workers(client_handles).await;
            return Err(error);
        }
    };
    let event_started_ns = trigger_ns.load(Ordering::Acquire);
    if event_started_ns == 0 {
        phase.store(PHASE_STOP, Ordering::Release);
        abort_workers(client_handles).await;
        return Err("churn injector completed without marking the trigger".into());
    }
    if event.event_duration.is_zero() {
        event.event_duration = trigger.elapsed_since_trigger().unwrap_or_default();
    }

    phase.store(PHASE_RECOVERY, Ordering::Release);
    tokio::time::sleep(config.recovery).await;
    phase.store(PHASE_STOP, Ordering::Release);

    let mut runs = Vec::with_capacity(client_handles.len());
    let join_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    for running in client_handles {
        let mut merged = WorkerStats::new();
        for worker in running.workers {
            let stats = finalize_worker(worker, join_deadline, run_started).await;
            merged.merge(&stats);
        }
        let after_metrics = running.client.metrics();
        running.client.shutdown().await;
        let metrics = match (running.before_metrics, after_metrics) {
            (Some(before), Some(after)) => Some(MetricsSnapshot {
                ask: after.ask.saturating_sub(before.ask),
                moved: after.moved.saturating_sub(before.moved),
                refresh_success: after.refresh_success.saturating_sub(before.refresh_success),
                refresh_partial: after.refresh_partial.saturating_sub(before.refresh_partial),
                refresh_error: after.refresh_error.saturating_sub(before.refresh_error),
                refresh_duration_us: after
                    .refresh_duration_us
                    .saturating_sub(before.refresh_duration_us),
                refresh_max_us: after.refresh_max_us,
            }),
            _ => None,
        };
        runs.push(ClientRun {
            kind: running.kind,
            metrics,
            stats: merged,
        });
    }

    Ok(runs
        .into_iter()
        .map(|run| build_report(scenario, config, event_started_ns, &event, run))
        .collect())
}

async fn abort_workers(clients: Vec<RunningClient>) {
    for running in clients {
        for worker in running.workers {
            worker.handle.abort();
            let _ = worker.handle.await;
        }
        running.client.shutdown().await;
    }
}

async fn finalize_worker(
    worker: RunningWorker,
    join_deadline: tokio::time::Instant,
    run_started: Instant,
) -> WorkerStats {
    let RunningWorker { state, mut handle } = worker;
    let interrupted = if handle.is_finished() {
        handle.await.is_err()
    } else {
        match tokio::time::timeout_at(join_deadline, &mut handle).await {
            Ok(result) => result.is_err(),
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                true
            }
        }
    };

    let now = elapsed_ns(run_started, Instant::now());
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if interrupted {
        // A task that panicked or had to be canceled may still own one request.
        // Preserve all prior samples and count that unresolved request as a
        // dropped operation in the phase it crossed into.
        state.abort_in_flight(now);
    }
    state.take_stats()
}

fn build_report(
    scenario: ChurnScenario,
    config: ChurnConfig,
    event_started_ns: u64,
    event: &ChurnEventReport,
    run: ClientRun,
) -> ChurnReport {
    let stable = run.stats.stable.report();
    let churn = run.stats.churn.report();
    let recovery = run.stats.recovery.report();
    let recovery_state = run.stats.recovery_state(event_started_ns);
    let first_success_ns = recovery_state.first_success_ns;
    let delta =
        |current: &LatencyReport, baseline: &LatencyReport, select: fn(&LatencyReport) -> f64| {
            if current.samples == 0 || baseline.samples == 0 {
                return (None, None);
            }
            let current = select(current);
            let baseline = select(baseline);
            let absolute = current - baseline;
            let percent = (baseline > 0.0).then_some(absolute / baseline * 100.0);
            (Some(absolute), percent)
        };
    let (p99_delta_us, p99_delta_pct) =
        delta(&churn.latency, &stable.latency, |report| report.p99_us);
    let (p999_delta_us, p999_delta_pct) =
        delta(&churn.latency, &stable.latency, |report| report.p999_us);
    let (redirects, topology_refreshes) = match run.metrics {
        Some(metrics) => {
            let (redirects, refreshes) = metrics.reports();
            (Some(redirects), Some(refreshes))
        }
        None => (None, None),
    };

    ChurnReport {
        scenario,
        client: format!("{:?}", run.kind),
        workload: config.workload,
        concurrency: config.concurrency,
        dropped_ops: churn.errors + recovery.errors,
        time_to_first_success_ms: first_success_ns
            .map(|time| time.saturating_sub(event_started_ns) as f64 / 1_000_000.0),
        recovery_after_error_ms: match (
            recovery_state.last_error_ns,
            recovery_state.first_success_after_last_error_ns,
        ) {
            (Some(last), Some(success)) => Some(success.saturating_sub(last) as f64 / 1_000_000.0),
            _ => None,
        },
        error_window_ms: match (recovery_state.first_error_ns, recovery_state.last_error_ns) {
            (Some(first), Some(last)) => Some(last.saturating_sub(first) as f64 / 1_000_000.0),
            _ => None,
        },
        event_duration_ms: event.event_duration.as_secs_f64() * 1_000.0,
        topology_convergence_ms: event
            .topology_convergence
            .map(|duration| duration.as_secs_f64() * 1_000.0),
        topology_change: event.topology_change.clone(),
        p99_delta_us,
        p99_delta_pct,
        p999_delta_us,
        p999_delta_pct,
        redirects,
        topology_refreshes,
        stable,
        churn,
        recovery,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase(p99: f64, p999: f64, successes: u64, errors: u64) -> PhaseReport {
        PhaseReport {
            successes,
            errors,
            latency: LatencyReport {
                samples: successes,
                p50_us: p99 / 2.0,
                p90_us: p99 * 0.9,
                p99_us: p99,
                p999_us: p999,
                max_us: p999 * 2.0,
            },
        }
    }

    fn report(run: usize) -> ChurnReport {
        ChurnReport {
            scenario: ChurnScenario::Reshard,
            client: "RedisTowerMux".into(),
            workload: ChurnWorkload::Get,
            concurrency: 8,
            stable: phase(100.0, 200.0, 1000, 0),
            churn: phase(200.0 + run as f64 * 20.0, 400.0, 800, run as u64),
            recovery: phase(110.0, 220.0, 900, 0),
            dropped_ops: run as u64,
            time_to_first_success_ms: Some(run as f64),
            recovery_after_error_ms: (run > 0).then_some(run as f64 * 6.0),
            error_window_ms: (run > 0).then_some(run as f64 * 5.0),
            event_duration_ms: 100.0 + run as f64 * 10.0,
            topology_convergence_ms: Some(80.0 + run as f64),
            topology_change: Some("slot 42: 17000 -> 17001".into()),
            p99_delta_us: Some(100.0 + run as f64 * 20.0),
            p99_delta_pct: Some(100.0 + run as f64 * 20.0),
            p999_delta_us: Some(200.0),
            p999_delta_pct: Some(100.0),
            redirects: Some(RedirectReport {
                ask: run as u64,
                moved: 1,
            }),
            topology_refreshes: Some(TopologyRefreshReport {
                success: 1,
                partial: 0,
                error: 0,
                mean_duration_ms: 5.0 + run as f64,
                max_duration_ms: 5.0 + run as f64,
            }),
        }
    }

    #[test]
    fn latency_report_is_empty_without_samples() {
        let report = LatencyReport::from_histogram(&new_histogram());
        assert_eq!(report.samples, 0);
        assert_eq!(report.p999_us, 0.0);
    }

    #[test]
    fn latency_report_includes_tail_percentiles() {
        let mut histogram = new_histogram();
        for value in 1..=1000 {
            histogram.saturating_record(value);
        }
        let report = LatencyReport::from_histogram(&histogram);
        assert_eq!(report.samples, 1000);
        assert!(report.p99_us <= report.p999_us);
        assert!(report.p999_us <= report.max_us);
    }

    #[test]
    fn aggregate_churn_sums_events_and_averages_informational_timings() {
        let aggregate = aggregate_churn(&[report(0), report(1), report(2)]);
        assert_eq!(aggregate.runs, 3);
        assert_eq!(aggregate.dropped_ops, 3);
        assert_eq!(aggregate.stable_p99_us_mean, Some(100.0));
        assert_eq!(aggregate.churn_p99_us_mean, Some(220.0));
        assert_eq!(aggregate.p99_delta_us_mean, Some(120.0));
        assert_eq!(aggregate.event_duration_ms_mean, 110.0);
        assert!((aggregate.event_duration_ms_stddev - 8.1649658).abs() < 1e-6);
        let redirects = aggregate.redirects.expect("tower redirect hooks");
        assert_eq!(redirects.ask, 3);
        assert_eq!(redirects.moved, 3);
        let refreshes = aggregate.topology_refreshes.expect("tower topology hooks");
        assert_eq!(refreshes.success, 3);
        assert_eq!(refreshes.mean_duration_ms, 6.0);
        assert_eq!(refreshes.max_duration_ms, 7.0);
    }

    #[test]
    fn aggregate_keeps_unavailable_redis_rs_hooks_null() {
        let mut redis_rs = report(1);
        redis_rs.client = "RedisRsAsync".into();
        redis_rs.redirects = None;
        redis_rs.topology_refreshes = None;
        let aggregate = aggregate_churn(&[redis_rs]);
        assert!(aggregate.redirects.is_none());
        assert!(aggregate.topology_refreshes.is_none());
    }

    #[test]
    fn aggregate_does_not_hide_a_run_that_never_recovers() {
        let recovered = report(1);
        let mut wedged = report(2);
        wedged.time_to_first_success_ms = None;
        wedged.recovery_after_error_ms = None;

        let aggregate = aggregate_churn(&[recovered, wedged]);
        assert_eq!(aggregate.first_success_runs, 1);
        assert_eq!(aggregate.runs_with_errors, 2);
        assert_eq!(aggregate.recovered_after_error_runs, 1);
        assert!(aggregate.time_to_first_success_ms_mean.is_none());
        assert!(aggregate.recovery_after_error_ms_mean.is_none());
    }

    #[test]
    fn aggregate_keeps_tail_deltas_null_when_churn_has_no_successes() {
        let mut failed = report(1);
        failed.churn = phase(0.0, 0.0, 0, 25);
        failed.p99_delta_us = None;
        failed.p99_delta_pct = None;
        failed.p999_delta_us = None;
        failed.p999_delta_pct = None;

        let aggregate = aggregate_churn(&[failed]);
        assert!(aggregate.churn_p99_us_mean.is_none());
        assert!(aggregate.churn_p999_us_mean.is_none());
        assert!(aggregate.p99_delta_us_mean.is_none());
        assert_eq!(aggregate.dropped_ops, 1);
        assert!(aggregate.churn_error_rate_pct > 0.0);
        let json = serde_json::to_value(&aggregate).unwrap();
        assert!(json["churn_p99_us_mean"].is_null());
        assert!(json["p99_delta_us_mean"].is_null());
    }

    #[test]
    fn aggregate_keeps_stable_tails_null_without_baseline_successes() {
        let mut failed = report(1);
        failed.stable = phase(0.0, 0.0, 0, 5);
        failed.p99_delta_us = None;
        failed.p99_delta_pct = None;
        failed.p999_delta_us = None;
        failed.p999_delta_pct = None;

        let aggregate = aggregate_churn(&[failed]);
        assert!(aggregate.stable_p99_us_mean.is_none());
        assert!(aggregate.stable_p999_us_mean.is_none());
        let json = serde_json::to_value(&aggregate).unwrap();
        assert!(json["stable_p99_us_mean"].is_null());
        assert!(json["stable_p999_us_mean"].is_null());
    }

    #[test]
    fn operations_crossing_the_trigger_are_churn_samples() {
        assert_eq!(completed_phase(PHASE_STABLE, PHASE_CHURN), PHASE_CHURN);
        assert_eq!(completed_phase(PHASE_STABLE, PHASE_RECOVERY), PHASE_CHURN);
        assert_eq!(completed_phase(PHASE_CHURN, PHASE_RECOVERY), PHASE_CHURN);
        assert_eq!(completed_phase(PHASE_RECOVERY, PHASE_STOP), PHASE_RECOVERY);
        assert_eq!(completed_phase(PHASE_WARMUP, PHASE_STABLE), PHASE_WARMUP);
    }

    fn recovery_state(events: &[(u64, bool)]) -> RecoveryState {
        let mut stats = WorkerStats::new();
        stats.post_trigger_events = events
            .iter()
            .map(|&(elapsed_ns, success)| RecoveryEvent {
                elapsed_ns,
                success,
            })
            .collect();
        stats.recovery_state(0)
    }

    #[test]
    fn recovery_uses_event_time_instead_of_callback_order() {
        // The success callback is observed first even though the error
        // completed earlier. Recovery is still the success at t=200.
        let state = recovery_state(&[(200, true), (100, false)]);
        assert_eq!(state.first_success_ns, Some(200));
        assert_eq!(state.first_error_ns, Some(100));
        assert_eq!(state.last_error_ns, Some(100));
        assert_eq!(state.first_success_after_last_error_ns, Some(200));
    }

    #[test]
    fn recovery_retains_the_next_success_when_a_later_error_overtakes_one() {
        // Callback order differs from completion order: the delayed t=200
        // error invalidates t=150 but must retain the t=250 success.
        let state = recovery_state(&[(100, false), (150, true), (250, true), (200, false)]);
        assert_eq!(state.first_success_ns, Some(150));
        assert_eq!(state.first_error_ns, Some(100));
        assert_eq!(state.last_error_ns, Some(200));
        assert_eq!(state.first_success_after_last_error_ns, Some(250));
    }

    #[test]
    fn recovery_is_absent_without_a_success_after_the_final_error() {
        let state = recovery_state(&[(90, true), (100, false), (110, false)]);
        assert_eq!(state.first_success_ns, Some(90));
        assert_eq!(state.first_error_ns, Some(100));
        assert_eq!(state.last_error_ns, Some(110));
        assert!(state.first_success_after_last_error_ns.is_none());
    }

    #[test]
    fn recovery_ignores_transition_events_before_the_confirmed_trigger() {
        let mut stats = WorkerStats::new();
        stats.post_trigger_events = vec![
            RecoveryEvent {
                elapsed_ns: 90,
                success: true,
            },
            RecoveryEvent {
                elapsed_ns: 100,
                success: false,
            },
            RecoveryEvent {
                elapsed_ns: 120,
                success: true,
            },
        ];

        let state = stats.recovery_state(100);
        assert_eq!(state.first_success_ns, Some(120));
        assert_eq!(state.first_error_ns, Some(100));
        assert_eq!(state.first_success_after_last_error_ns, Some(120));
    }

    #[tokio::test]
    async fn forced_abort_preserves_history_and_counts_the_in_flight_request() {
        let state = Arc::new(Mutex::new(WorkerState::new()));
        {
            let mut state = state.lock().unwrap();
            state
                .stats
                .record(PHASE_STABLE, true, Duration::from_micros(7), 10);
            state.begin_operation(PHASE_CHURN);
        }
        let handle = tokio::spawn(std::future::pending::<()>());
        let run_started = Instant::now();
        let stats = finalize_worker(
            RunningWorker { state, handle },
            tokio::time::Instant::now(),
            run_started,
        )
        .await;

        assert_eq!(stats.stable.successes, 1);
        assert_eq!(stats.churn.errors, 1);
        assert_eq!(stats.post_trigger_events.len(), 1);
        assert!(!stats.post_trigger_events[0].success);
    }

    #[test]
    fn reports_serialize_with_explicit_scenario_and_tail_metrics() {
        let json = serde_json::to_value(aggregate_churn(&[report(1)])).unwrap();
        assert_eq!(json["scenario"], "reshard");
        assert_eq!(json["workload"], "get");
        assert!(json.get("stable_p999_us_mean").is_some());
        assert!(json.get("churn_p999_us_mean").is_some());
        assert!(json.get("topology_convergence_ms_mean").is_some());
    }
}
