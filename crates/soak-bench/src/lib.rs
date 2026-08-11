//! Constant-memory, fault-injecting soak harness for redis-tower.
//!
//! The hot path never appends one entry per operation. Every worker owns an
//! interval HDR histogram and a full-run HDR histogram, and hands interval
//! snapshots to the coordinator only between bounded Redis commands.

#![forbid(unsafe_code)]

use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use redis_server_wrapper::chaos;
use redis_server_wrapper::{RedisServer, RedisServerHandle};
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::reconnect::{
    ConnectionEvent, ConnectionEventBus, ConnectionEventRecvError, ReconnectConfig,
    UrlConnectionFactory,
};
use redis_tower::{MultiplexedClient, RedisConnection};
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_commands::{Get, Set};
use redis_tower_test::cluster::{ClusterFixture, key_for_slot};
use serde::Serialize;
use tokio::sync::{Barrier, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};

// The accepted operation timeout remains 60 seconds. The extra minute keeps a
// completion at that exact edge recordable despite scheduler wakeup latency.
const HISTOGRAM_MAX_US: u64 = 120_000_000;
const HISTOGRAM_SIGFIG: u8 = 3;
const EVENT_CAPACITY: usize = 4096;
const SCHEMA_VERSION: u8 = 1;
const PROCESS_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_CLEANUP_POLL: Duration = Duration::from_millis(25);
const EVENT_BOUNDARY_TIMEOUT: Duration = Duration::from_secs(3);
const EVENT_BOUNDARY_MARKER: &str = "redis-tower:soak:lifecycle-boundary";

static STANDALONE_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Result returned by soak harness configuration and execution.
pub type RunResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Result<T> = RunResult<T>;

/// Tokio tasks are detached when a bare `JoinHandle` is dropped. Destructive
/// chaos work must instead be cancelled whenever its owner is cancelled.
struct AbortOnDropTask<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T>
where
    T: Send + 'static,
{
    fn spawn(future: impl std::future::Future<Output = T> + Send + 'static) -> Self {
        Self {
            handle: Some(tokio::spawn(future)),
        }
    }

    fn abort(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    async fn join(mut self) -> std::result::Result<T, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("abort-on-drop task is joined once")
            .await;
        self.handle.take();
        result
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct ChaosRun {
    task: AbortOnDropTask<Result<()>>,
    completion: Option<oneshot::Receiver<std::result::Result<Instant, String>>>,
    release: Option<oneshot::Sender<()>>,
}

impl ChaosRun {
    fn take_completion(
        &mut self,
    ) -> Result<oneshot::Receiver<std::result::Result<Instant, String>>> {
        self.completion
            .take()
            .ok_or_else(|| "chaos completion receiver was already consumed".into())
    }

    fn abort(&mut self) {
        self.task.abort();
    }

    async fn finish(mut self) -> Result<()> {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        self.task
            .join()
            .await
            .map_err(|error| format!("chaos task panicked: {error}"))??;
        Ok(())
    }

    async fn cancel(mut self) {
        self.task.abort();
        let _ = self.task.join().await;
    }
}

/// Topology driven by the harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// One managed Redis process and a reconnecting multiplexed client.
    Standalone,
    /// A managed three-master, three-replica Redis Cluster.
    Cluster,
}

impl Mode {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "standalone" => Ok(Self::Standalone),
            "cluster" => Ok(Self::Cluster),
            _ => Err(format!(
                "SOAK_MODE must be standalone or cluster, got {value:?}"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Cluster => "cluster",
        }
    }
}

/// Optional mid-run failure injected by the harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChaosMode {
    /// Leave the managed topology healthy for the whole run.
    None,
    /// SIGKILL standalone Redis and start a fresh process on the same port.
    StandaloneSigkill,
    /// SIGKILL the six-node fixture master that owns the workload key.
    ClusterMasterKill,
}

impl ChaosMode {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "none" | "off" => Ok(Self::None),
            "standalone-sigkill" | "sigkill" => Ok(Self::StandaloneSigkill),
            "cluster-master-kill" | "master-kill" => Ok(Self::ClusterMasterKill),
            _ => Err(format!(
                "SOAK_CHAOS must be none, standalone-sigkill, or cluster-master-kill, got {value:?}"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::StandaloneSigkill => "standalone_sigkill_same_port_restart",
            Self::ClusterMasterKill => "cluster_slot_owner_sigkill",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputFormat {
    Human,
    JsonLines,
}

/// Runtime configuration loaded from environment variables and `--jsonl`.
#[derive(Clone, Debug)]
pub struct Config {
    mode: Mode,
    chaos: ChaosMode,
    duration: Duration,
    warmup: Duration,
    report_interval: Duration,
    chaos_after: Duration,
    concurrency: usize,
    operation_timeout: Duration,
    error_backoff: Duration,
    startup_timeout: Duration,
    recovery_timeout: Duration,
    payload_bytes: usize,
    cluster_slot: u16,
    cluster_node_timeout_ms: u64,
    standalone_port: Option<u16>,
    output: OutputFormat,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            chaos: ChaosMode::None,
            duration: Duration::from_secs(4 * 60 * 60),
            warmup: Duration::from_secs(60),
            report_interval: Duration::from_secs(60),
            chaos_after: Duration::from_secs(2 * 60 * 60),
            concurrency: 32,
            operation_timeout: Duration::from_secs(2),
            error_backoff: Duration::from_millis(1),
            startup_timeout: Duration::from_secs(30),
            recovery_timeout: Duration::from_secs(30),
            payload_bytes: 16,
            cluster_slot: 42,
            cluster_node_timeout_ms: 1_000,
            standalone_port: None,
            output: OutputFormat::Human,
        }
    }
}

impl Config {
    /// Parse the documented environment variables and output-format flag.
    pub fn from_env_and_args() -> Result<Self> {
        let mut config = Self {
            mode: Mode::parse(&env_string("SOAK_MODE", "standalone"))?,
            chaos: ChaosMode::parse(&env_string("SOAK_CHAOS", "none"))?,
            duration: Duration::from_secs(env_parse("SOAK_DURATION_SECS", 14_400_u64)?),
            warmup: Duration::from_secs(env_parse("SOAK_WARMUP_SECS", 60_u64)?),
            report_interval: Duration::from_secs(env_parse("SOAK_REPORT_INTERVAL_SECS", 60_u64)?),
            chaos_after: Duration::from_secs(env_parse("SOAK_CHAOS_AFTER_SECS", 7_200_u64)?),
            concurrency: env_parse("SOAK_CONCURRENCY", 32_usize)?,
            operation_timeout: Duration::from_millis(env_parse(
                "SOAK_OPERATION_TIMEOUT_MS",
                2_000_u64,
            )?),
            error_backoff: Duration::from_millis(env_parse("SOAK_ERROR_BACKOFF_MS", 1_u64)?),
            startup_timeout: Duration::from_secs(env_parse("SOAK_STARTUP_TIMEOUT_SECS", 30_u64)?),
            recovery_timeout: Duration::from_secs(env_parse("SOAK_RECOVERY_TIMEOUT_SECS", 30_u64)?),
            payload_bytes: env_parse("SOAK_PAYLOAD_BYTES", 16_usize)?,
            cluster_slot: env_parse("SOAK_CLUSTER_SLOT", 42_u16)?,
            cluster_node_timeout_ms: env_parse("SOAK_CLUSTER_NODE_TIMEOUT_MS", 1_000_u64)?,
            standalone_port: env_optional("SOAK_STANDALONE_PORT")?,
            output: OutputFormat::Human,
        };

        for argument in std::env::args().skip(1) {
            match argument.as_str() {
                "--jsonl" => config.output = OutputFormat::JsonLines,
                "--human" => config.output = OutputFormat::Human,
                other => return Err(format!("unknown argument {other:?}; try --help").into()),
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.duration.is_zero() {
            return Err("SOAK_DURATION_SECS must be greater than zero".into());
        }
        if self.report_interval.is_zero() {
            return Err("SOAK_REPORT_INTERVAL_SECS must be greater than zero".into());
        }
        if self.concurrency == 0 {
            return Err("SOAK_CONCURRENCY must be greater than zero".into());
        }
        if self.operation_timeout.is_zero()
            || self.startup_timeout.is_zero()
            || self.recovery_timeout.is_zero()
        {
            return Err(
                "operation, startup, and recovery timeouts must be greater than zero".into(),
            );
        }
        if self.operation_timeout > Duration::from_secs(60) {
            return Err(
                "SOAK_OPERATION_TIMEOUT_MS must not exceed the supported 60000ms operation bound"
                    .into(),
            );
        }
        if self.error_backoff.is_zero() {
            return Err("SOAK_ERROR_BACKOFF_MS must be greater than zero".into());
        }
        if self.payload_bytes == 0 {
            return Err("SOAK_PAYLOAD_BYTES must be greater than zero".into());
        }
        if self.cluster_slot >= 16_384 {
            return Err(format!(
                "SOAK_CLUSTER_SLOT must be below 16384, got {}",
                self.cluster_slot
            )
            .into());
        }
        if self.chaos != ChaosMode::None
            && (self.chaos_after.is_zero() || self.chaos_after >= self.duration)
        {
            return Err(
                "SOAK_CHAOS_AFTER_SECS must be inside the measured duration when chaos is enabled"
                    .into(),
            );
        }
        match (self.mode, self.chaos) {
            (Mode::Standalone, ChaosMode::ClusterMasterKill)
            | (Mode::Cluster, ChaosMode::StandaloneSigkill) => {
                return Err(format!(
                    "SOAK_MODE={} is incompatible with SOAK_CHAOS={}",
                    self.mode.as_str(),
                    self.chaos.as_str()
                )
                .into());
            }
            _ => {}
        }
        Ok(())
    }

    /// Usage and environment reference printed by `--help`.
    pub fn help() -> &'static str {
        "soak-bench: constant-memory redis-tower soak and chaos harness\n\
\n\
Usage: cargo run --release -p soak-bench -- [--human|--jsonl]\n\
\n\
Environment:\n\
  SOAK_MODE=standalone|cluster                 (default: standalone)\n\
  SOAK_CHAOS=none|standalone-sigkill|cluster-master-kill\n\
  SOAK_DURATION_SECS=14400                     measured time after warmup\n\
  SOAK_WARMUP_SECS=60                          discarded warmup\n\
  SOAK_REPORT_INTERVAL_SECS=60                 interval line cadence\n\
  SOAK_CHAOS_AFTER_SECS=7200                   offset into measured time\n\
  SOAK_CONCURRENCY=32                          async workers\n\
  SOAK_OPERATION_TIMEOUT_MS=2000               bound for every GET (max: 60000)\n\
  SOAK_ERROR_BACKOFF_MS=1                      non-zero fail-fast error backoff\n\
  SOAK_STARTUP_TIMEOUT_SECS=30                 fixture/process startup bound\n\
  SOAK_RECOVERY_TIMEOUT_SECS=30                fault recovery bound\n\
  SOAK_PAYLOAD_BYTES=16                        validated GET payload\n\
  SOAK_CLUSTER_SLOT=42                         affected cluster slot\n\
  SOAK_CLUSTER_NODE_TIMEOUT_MS=1000            failover detector setting\n\
  SOAK_STANDALONE_PORT=<port>                  default: reserve an ephemeral port\n"
    }
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| format!("invalid {name}={value:?}: {error}").into()),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("could not read {name}: {error}").into()),
    }
}

fn env_optional<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|error| format!("invalid {name}={value:?}: {error}").into()),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("could not read {name}: {error}").into()),
    }
}

/// Run the configured managed topology and workload.
pub async fn run(config: Config) -> Result<()> {
    match config.mode {
        Mode::Standalone => run_standalone(config).await,
        Mode::Cluster => run_cluster(config).await,
    }
}

#[derive(Clone)]
enum SoakClient {
    Standalone(MultiplexedClient),
    Cluster(MultiplexedClusterClient),
}

impl SoakClient {
    async fn get_matches(&self, key: &str, expected: &[u8]) -> bool {
        let result = match self {
            Self::Standalone(client) => client.execute(Get::new(key)).await,
            Self::Cluster(client) => client.execute(Get::new(key)).await,
        };
        matches!(result, Ok(Some(value)) if value.as_ref() == expected)
    }

    async fn shutdown(self) {
        match self {
            Self::Standalone(client) => client.shutdown().await,
            Self::Cluster(client) => client.shutdown().await,
        }
    }
}

#[derive(Default)]
struct LifecycleCounters {
    standalone_reconnect_recoveries: AtomicU64,
    standalone_reconnect_notify: Notify,
    cluster_recoveries: AtomicU64,
    chaos_injections: AtomicU64,
    event_lagged: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
struct CounterSnapshot {
    reconnects: u64,
    recoveries: u64,
    chaos_injections: u64,
    event_lagged: u64,
}

impl LifecycleCounters {
    fn snapshot(&self, reconnects_supported: bool) -> CounterSnapshot {
        let paired = self.standalone_reconnect_recoveries.load(Ordering::Acquire);
        CounterSnapshot {
            reconnects: if reconnects_supported { paired } else { 0 },
            recoveries: if reconnects_supported {
                paired
            } else {
                self.cluster_recoveries.load(Ordering::Acquire)
            },
            chaos_injections: self.chaos_injections.load(Ordering::Acquire),
            event_lagged: self.event_lagged.load(Ordering::Acquire),
        }
    }
}

impl CounterSnapshot {
    fn delta(self, previous: Self) -> Self {
        Self {
            reconnects: self.reconnects.saturating_sub(previous.reconnects),
            recoveries: self.recoveries.saturating_sub(previous.recoveries),
            chaos_injections: self
                .chaos_injections
                .saturating_sub(previous.chaos_injections),
            event_lagged: self.event_lagged.saturating_sub(previous.event_lagged),
        }
    }
}

struct EventMonitor {
    bus: ConnectionEventBus,
    snapshots: mpsc::UnboundedReceiver<(u64, CounterSnapshot)>,
    task: AbortOnDropTask<()>,
    next_boundary_to_publish: u64,
    next_boundary_to_observe: u64,
}

impl EventMonitor {
    fn publish_boundary(&mut self) -> Result<(Instant, u64)> {
        let boundary = self.reserve_boundaries(1)?;
        let marked_at = Instant::now();
        publish_event_boundary(&self.bus, boundary)?;
        Ok((marked_at, boundary))
    }

    async fn await_boundary(&mut self, boundary: u64) -> Result<CounterSnapshot> {
        if boundary != self.next_boundary_to_observe {
            return Err(format!(
                "connection event boundary requested out of order: expected {}, requested {boundary}",
                self.next_boundary_to_observe
            )
            .into());
        }
        let (observed, snapshot) = timeout(EVENT_BOUNDARY_TIMEOUT, self.snapshots.recv())
            .await
            .map_err(|_| "timed out draining the connection event boundary")?
            .ok_or("connection event monitor stopped before a boundary snapshot")?;
        if observed != boundary {
            return Err(format!(
                "connection event boundary ordering changed: expected {boundary}, observed {observed}"
            )
            .into());
        }
        self.next_boundary_to_observe += 1;
        Ok(snapshot)
    }

    #[cfg(test)]
    async fn capture_boundary(&mut self) -> Result<CounterSnapshot> {
        let (_, boundary) = self.publish_boundary()?;
        self.await_boundary(boundary).await
    }

    async fn await_next_boundary(&mut self) -> Result<CounterSnapshot> {
        self.await_boundary(self.next_boundary_to_observe).await
    }

    fn arm_boundaries(
        &mut self,
        started: Instant,
        duration: Duration,
        report_interval: Duration,
    ) -> Result<EventBoundarySchedule> {
        let count = duration.as_nanos().div_ceil(report_interval.as_nanos());
        let count = u64::try_from(count)
            .map_err(|_| "measurement has too many lifecycle reporting boundaries")?;
        let first_boundary = self.reserve_boundaries(count)?;
        let bus = self.bus.clone();
        let task = AbortOnDropTask::spawn(async move {
            let finish = started + duration;
            let mut deadline = (started + report_interval).min(finish);
            let mut boundary = first_boundary;
            loop {
                sleep_until(deadline).await;
                publish_event_boundary(&bus, boundary)?;
                if deadline == finish {
                    return Ok(());
                }
                deadline = (deadline + report_interval).min(finish);
                boundary += 1;
            }
        });
        Ok(EventBoundarySchedule { task })
    }

    fn reserve_boundaries(&mut self, count: u64) -> Result<u64> {
        let first = self.next_boundary_to_publish;
        self.next_boundary_to_publish = first
            .checked_add(count)
            .ok_or("connection event boundary sequence overflowed")?;
        Ok(first)
    }

    async fn cancel(mut self) {
        self.task.abort();
        let _ = self.task.join().await;
    }
}

struct EventBoundarySchedule {
    task: AbortOnDropTask<Result<()>>,
}

impl EventBoundarySchedule {
    async fn finish(self) -> Result<()> {
        self.task
            .join()
            .await
            .map_err(|error| format!("connection event boundary task panicked: {error}"))?
    }
}

fn publish_event_boundary(bus: &ConnectionEventBus, boundary: u64) -> Result<()> {
    let published = bus.publish(ConnectionEvent::Failover {
        previous: Some(Arc::from(EVENT_BOUNDARY_MARKER)),
        current: Some(Arc::from(format!("{EVENT_BOUNDARY_MARKER}:{boundary}"))),
    });
    if !published {
        return Err("connection event boundary marker had no subscriber".into());
    }
    Ok(())
}

fn event_boundary_number(event: &ConnectionEvent) -> Option<u64> {
    let ConnectionEvent::Failover { previous, current } = event else {
        return None;
    };
    if previous.as_deref() != Some(EVENT_BOUNDARY_MARKER) {
        return None;
    }
    current
        .as_deref()?
        .strip_prefix(EVENT_BOUNDARY_MARKER)?
        .strip_prefix(':')?
        .parse()
        .ok()
}

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HISTOGRAM_MAX_US, HISTOGRAM_SIGFIG)
        .expect("fixed valid HDR histogram bounds")
}

struct Stats {
    successes: u64,
    errors: u64,
    latency: Histogram<u64>,
}

impl Stats {
    fn new() -> Self {
        Self {
            successes: 0,
            errors: 0,
            latency: new_histogram(),
        }
    }

    fn record(&mut self, success: bool, elapsed: Duration) {
        if success {
            self.successes += 1;
            let micros = elapsed.as_micros().max(1);
            assert!(
                micros <= u128::from(HISTOGRAM_MAX_US),
                "successful latency {micros}us exceeds the validated HDR range"
            );
            self.latency
                .record(micros as u64)
                .expect("validated latency fits the HDR histogram");
        } else {
            self.errors += 1;
        }
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.successes += other.successes;
        self.errors += other.errors;
        self.latency
            .add(&other.latency)
            .map_err(|error| format!("could not merge HDR histograms: {error}").into())
    }

    fn operations(&self) -> u64 {
        self.successes + self.errors
    }
}

struct WorkerStats {
    interval: Stats,
    aggregate: Stats,
    pending: Option<CompletedOperation>,
}

#[derive(Clone, Copy)]
struct CompletedOperation {
    success: bool,
    elapsed: Duration,
    completed: Instant,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            interval: Stats::new(),
            aggregate: Stats::new(),
            pending: None,
        }
    }

    fn record_completion(
        &mut self,
        operation: CompletedOperation,
        interval_deadline: Instant,
        measurement_finish: Instant,
    ) {
        if operation.completed > measurement_finish {
            return;
        }
        self.aggregate.record(operation.success, operation.elapsed);
        if operation.completed <= interval_deadline {
            self.interval.record(operation.success, operation.elapsed);
        } else {
            assert!(
                self.pending.replace(operation).is_none(),
                "one worker cannot have two commands crossing a blocked interval boundary"
            );
        }
    }

    fn take_interval(&mut self, next_deadline: Instant) -> Stats {
        let closed = std::mem::replace(&mut self.interval, Stats::new());
        if self
            .pending
            .is_some_and(|operation| operation.completed <= next_deadline)
        {
            let operation = self.pending.take().expect("pending operation exists");
            self.interval.record(operation.success, operation.elapsed);
        }
        closed
    }
}

enum WorkerCommand {
    Start {
        barrier: Arc<Barrier>,
        schedule: oneshot::Receiver<MeasurementSchedule>,
    },
    Snapshot {
        next_deadline: Instant,
        reply: oneshot::Sender<Stats>,
    },
    Stop {
        reply: oneshot::Sender<WorkerStats>,
    },
}

#[derive(Clone, Copy)]
struct MeasurementSchedule {
    started: Instant,
    first_deadline: Instant,
    finish: Instant,
}

struct MeasurementStarter {
    schedules: Vec<oneshot::Sender<MeasurementSchedule>>,
}

impl MeasurementStarter {
    fn begin(self, schedule: MeasurementSchedule) -> Result<()> {
        for sender in self.schedules {
            sender
                .send(schedule)
                .map_err(|_| "soak worker stopped before the measurement schedule")?;
        }
        Ok(())
    }
}

struct WorkerGroup {
    controls: Vec<mpsc::Sender<WorkerCommand>>,
    joins: Vec<JoinHandle<()>>,
    command_timeout: Duration,
}

impl WorkerGroup {
    fn spawn(
        client: SoakClient,
        key: Arc<str>,
        expected: Arc<[u8]>,
        concurrency: usize,
        operation_timeout: Duration,
        error_backoff: Duration,
    ) -> Self {
        let mut controls = Vec::with_capacity(concurrency);
        let mut joins = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let (tx, rx) = mpsc::channel(4);
            controls.push(tx);
            joins.push(tokio::spawn(worker_loop(
                client.clone(),
                Arc::clone(&key),
                Arc::clone(&expected),
                operation_timeout,
                error_backoff,
                rx,
            )));
        }
        Self {
            controls,
            joins,
            command_timeout: worker_control_timeout(operation_timeout, error_backoff),
        }
    }

    async fn prepare_measurement(&self) -> Result<MeasurementStarter> {
        let barrier = Arc::new(Barrier::new(self.controls.len() + 1));
        let mut schedules = Vec::with_capacity(self.controls.len());
        for control in &self.controls {
            let (schedule, receive_schedule) = oneshot::channel();
            control
                .send(WorkerCommand::Start {
                    barrier: Arc::clone(&barrier),
                    schedule: receive_schedule,
                })
                .await
                .map_err(|_| "soak worker stopped before the measurement barrier")?;
            schedules.push(schedule);
        }
        timeout(self.command_timeout, barrier.wait())
            .await
            .map_err(|_| "timed out starting soak measurement")?;
        Ok(MeasurementStarter { schedules })
    }

    async fn snapshot(&self, next_deadline: Instant) -> Result<Stats> {
        let mut replies = Vec::with_capacity(self.controls.len());
        for control in &self.controls {
            let (reply, receive) = oneshot::channel();
            control
                .send(WorkerCommand::Snapshot {
                    next_deadline,
                    reply,
                })
                .await
                .map_err(|_| "soak worker stopped before an interval snapshot")?;
            replies.push(receive);
        }
        let mut merged = Stats::new();
        for receive in replies {
            let stats = timeout(self.command_timeout, receive)
                .await
                .map_err(|_| "timed out waiting for a soak interval snapshot")?
                .map_err(|_| "soak worker dropped an interval snapshot")?;
            merged.merge(&stats)?;
        }
        Ok(merged)
    }

    async fn stop(mut self) -> Result<(Stats, Stats)> {
        let mut replies = Vec::with_capacity(self.controls.len());
        for control in &self.controls {
            let (reply, receive) = oneshot::channel();
            control
                .send(WorkerCommand::Stop { reply })
                .await
                .map_err(|_| "soak worker stopped before shutdown")?;
            replies.push(receive);
        }

        let mut interval = Stats::new();
        let mut aggregate = Stats::new();
        for receive in replies {
            let stats = timeout(self.command_timeout, receive)
                .await
                .map_err(|_| "timed out waiting for a soak worker to stop")?
                .map_err(|_| "soak worker dropped its final statistics")?;
            interval.merge(&stats.interval)?;
            aggregate.merge(&stats.aggregate)?;
        }
        for join in self.joins.drain(..) {
            join.await
                .map_err(|error| format!("soak worker panicked: {error}"))?;
        }
        Ok((interval, aggregate))
    }
}

fn worker_control_timeout(operation_timeout: Duration, error_backoff: Duration) -> Duration {
    operation_timeout
        .saturating_add(error_backoff)
        .saturating_add(Duration::from_secs(5))
}

impl Drop for WorkerGroup {
    fn drop(&mut self) {
        for join in &self.joins {
            join.abort();
        }
    }
}

async fn worker_loop(
    client: SoakClient,
    key: Arc<str>,
    expected: Arc<[u8]>,
    operation_timeout: Duration,
    error_backoff: Duration,
    mut commands: mpsc::Receiver<WorkerCommand>,
) {
    let mut measuring = false;
    let mut interval_deadline = None;
    let mut measurement_finish = None;
    let mut stats = WorkerStats::new();
    loop {
        let command = if interval_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            match commands.recv().await {
                Some(command) => Some(command),
                None => return,
            }
        } else {
            match commands.try_recv() {
                Ok(command) => Some(command),
                Err(mpsc::error::TryRecvError::Disconnected) => return,
                Err(mpsc::error::TryRecvError::Empty) => None,
            }
        };

        match command {
            Some(WorkerCommand::Start { barrier, schedule }) => {
                stats = WorkerStats::new();
                barrier.wait().await;
                let Ok(schedule) = schedule.await else {
                    return;
                };
                sleep_until(schedule.started).await;
                measuring = true;
                interval_deadline = Some(schedule.first_deadline);
                measurement_finish = Some(schedule.finish);
                continue;
            }
            Some(WorkerCommand::Snapshot {
                next_deadline,
                reply,
            }) => {
                let _ = reply.send(stats.take_interval(next_deadline));
                interval_deadline = Some(next_deadline);
                continue;
            }
            Some(WorkerCommand::Stop { reply }) => {
                let _ = reply.send(stats);
                return;
            }
            None => {}
        }

        let started = Instant::now();
        let success = timeout(
            operation_timeout,
            client.get_matches(key.as_ref(), expected.as_ref()),
        )
        .await
        .unwrap_or(false);
        let completed = Instant::now();
        let elapsed = started.elapsed();
        if measuring {
            stats.record_completion(
                CompletedOperation {
                    success,
                    elapsed,
                    completed,
                },
                interval_deadline.expect("measuring workers have an interval deadline"),
                measurement_finish.expect("measuring workers have a final deadline"),
            );
        }
        if !success && !error_backoff.is_zero() {
            sleep(error_backoff).await;
        }
    }
}

#[derive(Serialize)]
struct MetadataRecord<'a> {
    schema_version: u8,
    record_type: &'static str,
    started_unix_ms: u128,
    mode: Mode,
    workload: &'static str,
    key: &'a str,
    payload_bytes: usize,
    concurrency: usize,
    warmup_secs: f64,
    duration_secs: f64,
    report_interval_secs: f64,
    operation_timeout_ms: u128,
    error_backoff_ms: u128,
    startup_timeout_secs: f64,
    recovery_timeout_secs: f64,
    cluster_slot: u16,
    cluster_node_timeout_ms: u64,
    standalone_port: Option<u16>,
    chaos: ChaosMode,
    chaos_after_secs: Option<f64>,
    reconnect_accounting: &'static str,
    recovery_accounting: &'static str,
    latency_accounting: &'static str,
    rss_accounting: &'static str,
}

#[derive(Serialize)]
struct IntervalRecord {
    schema_version: u8,
    record_type: &'static str,
    interval: u64,
    elapsed_secs: f64,
    window_secs: f64,
    operations: u64,
    attempts: u64,
    successes: u64,
    errors: u64,
    ops_per_sec: f64,
    attempted_ops_per_sec: f64,
    p50_us: Option<u64>,
    p99_us: Option<u64>,
    p999_us: Option<u64>,
    max_us: Option<u64>,
    reconnects: Option<u64>,
    reconnects_total: Option<u64>,
    recoveries: u64,
    recoveries_total: u64,
    chaos_injections: u64,
    chaos_injections_total: u64,
    rss_bytes: Option<u64>,
}

#[derive(Serialize)]
struct SummaryRecord {
    schema_version: u8,
    record_type: &'static str,
    elapsed_secs: f64,
    operations: u64,
    attempts: u64,
    successes: u64,
    errors: u64,
    ops_per_sec: f64,
    attempted_ops_per_sec: f64,
    p50_us: Option<u64>,
    p99_us: Option<u64>,
    p999_us: Option<u64>,
    max_us: Option<u64>,
    reconnects_total: Option<u64>,
    recoveries_total: u64,
    chaos_injections_total: u64,
    rss_bytes: Option<u64>,
}

struct Reporter {
    output: OutputFormat,
    reconnects_supported: bool,
}

impl Reporter {
    fn metadata(&self, config: &Config, key: &str) -> Result<()> {
        let started_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let record = MetadataRecord {
            schema_version: SCHEMA_VERSION,
            record_type: "metadata",
            started_unix_ms,
            mode: config.mode,
            workload: "get_validate",
            key,
            payload_bytes: config.payload_bytes,
            concurrency: config.concurrency,
            warmup_secs: config.warmup.as_secs_f64(),
            duration_secs: config.duration.as_secs_f64(),
            report_interval_secs: config.report_interval.as_secs_f64(),
            operation_timeout_ms: config.operation_timeout.as_millis(),
            error_backoff_ms: config.error_backoff.as_millis(),
            startup_timeout_secs: config.startup_timeout.as_secs_f64(),
            recovery_timeout_secs: config.recovery_timeout.as_secs_f64(),
            cluster_slot: config.cluster_slot,
            cluster_node_timeout_ms: config.cluster_node_timeout_ms,
            standalone_port: config.standalone_port,
            chaos: config.chaos,
            chaos_after_secs: (config.chaos != ChaosMode::None)
                .then_some(config.chaos_after.as_secs_f64()),
            reconnect_accounting: match config.mode {
                Mode::Standalone => "exact_connection_event_reconnected",
                Mode::Cluster => "not_exposed_by_cluster_client",
            },
            recovery_accounting: match config.mode {
                Mode::Standalone => "exact_connection_event_reconnected",
                Mode::Cluster => "harness_observed_slot_owner_change_plus_successful_client_get",
            },
            latency_accounting: "successful_get_completions_only",
            rss_accounting: "current_soak_process_resident_set",
        };
        match self.output {
            OutputFormat::JsonLines => print_json_line(&record),
            OutputFormat::Human => {
                println!(
                    "soak start mode={} workload={} concurrency={} warmup={:.0}s duration={:.0}s report_every={:.0}s chaos={} reconnect_accounting={} recovery_accounting={}",
                    config.mode.as_str(),
                    record.workload,
                    config.concurrency,
                    config.warmup.as_secs_f64(),
                    config.duration.as_secs_f64(),
                    config.report_interval.as_secs_f64(),
                    config.chaos.as_str(),
                    record.reconnect_accounting,
                    record.recovery_accounting,
                );
                flush_stdout()
            }
        }
    }

    fn interval(
        &self,
        interval: u64,
        elapsed: Duration,
        window: Duration,
        stats: &Stats,
        counters: CounterSnapshot,
        counter_delta: CounterSnapshot,
    ) -> Result<()> {
        let record = IntervalRecord {
            schema_version: SCHEMA_VERSION,
            record_type: "interval",
            interval,
            elapsed_secs: elapsed.as_secs_f64(),
            window_secs: window.as_secs_f64(),
            operations: stats.successes,
            attempts: stats.operations(),
            successes: stats.successes,
            errors: stats.errors,
            ops_per_sec: stats.successes as f64 / window.as_secs_f64().max(f64::MIN_POSITIVE),
            attempted_ops_per_sec: stats.operations() as f64
                / window.as_secs_f64().max(f64::MIN_POSITIVE),
            p50_us: quantile(&stats.latency, 0.50),
            p99_us: quantile(&stats.latency, 0.99),
            p999_us: quantile(&stats.latency, 0.999),
            max_us: max_latency(&stats.latency),
            reconnects: self
                .reconnects_supported
                .then_some(counter_delta.reconnects),
            reconnects_total: self.reconnects_supported.then_some(counters.reconnects),
            recoveries: counter_delta.recoveries,
            recoveries_total: counters.recoveries,
            chaos_injections: counter_delta.chaos_injections,
            chaos_injections_total: counters.chaos_injections,
            rss_bytes: current_rss_bytes(),
        };
        match self.output {
            OutputFormat::JsonLines => print_json_line(&record),
            OutputFormat::Human => {
                let reconnects = record
                    .reconnects
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_owned());
                println!(
                    "soak interval={} elapsed={:.1}s window={:.1}s successful_ops={} attempts={} ops/s={:.0} attempted_ops/s={:.0} p50={}us p99={}us p999={}us max={}us errors={} reconnects={} recoveries={} chaos={} rss={}",
                    record.interval,
                    record.elapsed_secs,
                    record.window_secs,
                    record.operations,
                    record.attempts,
                    record.ops_per_sec,
                    record.attempted_ops_per_sec,
                    display_latency(record.p50_us),
                    display_latency(record.p99_us),
                    display_latency(record.p999_us),
                    display_latency(record.max_us),
                    record.errors,
                    reconnects,
                    record.recoveries,
                    record.chaos_injections,
                    display_bytes(record.rss_bytes),
                );
                flush_stdout()
            }
        }
    }

    fn summary(&self, elapsed: Duration, stats: &Stats, counters: CounterSnapshot) -> Result<()> {
        let record = SummaryRecord {
            schema_version: SCHEMA_VERSION,
            record_type: "summary",
            elapsed_secs: elapsed.as_secs_f64(),
            operations: stats.successes,
            attempts: stats.operations(),
            successes: stats.successes,
            errors: stats.errors,
            ops_per_sec: stats.successes as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
            attempted_ops_per_sec: stats.operations() as f64
                / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
            p50_us: quantile(&stats.latency, 0.50),
            p99_us: quantile(&stats.latency, 0.99),
            p999_us: quantile(&stats.latency, 0.999),
            max_us: max_latency(&stats.latency),
            reconnects_total: self.reconnects_supported.then_some(counters.reconnects),
            recoveries_total: counters.recoveries,
            chaos_injections_total: counters.chaos_injections,
            rss_bytes: current_rss_bytes(),
        };
        match self.output {
            OutputFormat::JsonLines => print_json_line(&record),
            OutputFormat::Human => {
                let reconnects = record
                    .reconnects_total
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_owned());
                println!(
                    "soak summary elapsed={:.1}s successful_ops={} attempts={} ops/s={:.0} attempted_ops/s={:.0} p50={}us p99={}us p999={}us max={}us errors={} reconnects={} recoveries={} chaos={} rss={}",
                    record.elapsed_secs,
                    record.operations,
                    record.attempts,
                    record.ops_per_sec,
                    record.attempted_ops_per_sec,
                    display_latency(record.p50_us),
                    display_latency(record.p99_us),
                    display_latency(record.p999_us),
                    display_latency(record.max_us),
                    record.errors,
                    reconnects,
                    record.recoveries_total,
                    record.chaos_injections_total,
                    display_bytes(record.rss_bytes),
                );
                flush_stdout()
            }
        }
    }
}

fn quantile(histogram: &Histogram<u64>, quantile: f64) -> Option<u64> {
    (!histogram.is_empty()).then(|| histogram.value_at_quantile(quantile))
}

fn max_latency(histogram: &Histogram<u64>) -> Option<u64> {
    (!histogram.is_empty()).then(|| histogram.max())
}

fn display_latency(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn display_bytes(value: Option<u64>) -> String {
    value
        .map(|value| format!("{:.1}MiB", value as f64 / (1024.0 * 1024.0)))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn print_json_line(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    flush_stdout()
}

fn flush_stdout() -> Result<()> {
    io::stdout().flush().map_err(Into::into)
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    parse_linux_rss(&status)
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> Option<u64> {
    let output = ProcessCommand::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[cfg(target_os = "linux")]
fn parse_linux_rss(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    line.split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

struct Measurement {
    elapsed: Duration,
    stats: Stats,
    counter_baseline: CounterSnapshot,
    counter_final: CounterSnapshot,
}

struct MeasurementTarget {
    client: SoakClient,
    key: Arc<str>,
    expected: Arc<[u8]>,
    counters: Arc<LifecycleCounters>,
    reconnects_supported: bool,
}

async fn measure(
    config: &Config,
    target: MeasurementTarget,
    measurement_started: Option<oneshot::Sender<Instant>>,
    mut chaos: Option<&mut ChaosRun>,
    mut event_monitor: Option<&mut EventMonitor>,
) -> Result<Measurement> {
    let MeasurementTarget {
        client,
        key,
        expected,
        counters,
        reconnects_supported,
    } = target;
    let reporter = Reporter {
        output: config.output,
        reconnects_supported,
    };
    reporter.metadata(config, key.as_ref())?;

    let workers = WorkerGroup::spawn(
        client,
        key,
        expected,
        config.concurrency,
        config.operation_timeout,
        config.error_backoff,
    );
    if !config.warmup.is_zero() {
        eprintln!(
            "soak-bench: discarding {:.0}s warmup",
            config.warmup.as_secs_f64()
        );
        sleep(config.warmup).await;
    }
    let starter = workers.prepare_measurement().await?;
    let (started, counter_baseline, mut boundary_schedule) = if reconnects_supported {
        let monitor = event_monitor
            .as_deref_mut()
            .ok_or("standalone measurement is missing its connection event monitor")?;
        // Publish the ordered lifecycle marker first, then release workers at
        // that timestamp. Draining the marker cannot steal measured runtime.
        let (started, start_boundary) = monitor.publish_boundary()?;
        let finish = started + config.duration;
        starter.begin(MeasurementSchedule {
            started,
            first_deadline: (started + config.report_interval).min(finish),
            finish,
        })?;
        // Boundary publication is independent of worker snapshots. A command
        // crossing several intervals therefore cannot move lifecycle events
        // into an earlier interval or past the final deadline.
        let schedule = monitor.arm_boundaries(started, config.duration, config.report_interval)?;
        let baseline = monitor.await_boundary(start_boundary).await?;
        (started, baseline, Some(schedule))
    } else {
        let started = Instant::now();
        let finish = started + config.duration;
        let baseline = counters.snapshot(false);
        starter.begin(MeasurementSchedule {
            started,
            first_deadline: (started + config.report_interval).min(finish),
            finish,
        })?;
        (started, baseline, None)
    };
    let finish = started + config.duration;
    if let Some(started_tx) = measurement_started {
        let _ = started_tx.send(started);
    }

    let mut completion = match chaos.as_mut() {
        Some(run) => Some(run.take_completion()?),
        None => None,
    };
    let mut chaos_completed = completion.is_none();

    let mut reported_stats = Stats::new();
    let interval_result: Result<(u64, Instant, CounterSnapshot, CounterSnapshot)> = async {
        let mut boundary = (started + config.report_interval).min(finish);
        let mut previous_boundary = started;
        let mut interval_index = 0_u64;
        let mut previous_counters = CounterSnapshot::default();

        while boundary < finish {
            wait_for_boundary(boundary, finish, &mut completion, &mut chaos_completed).await?;
            let counter_snapshot = lifecycle_boundary_snapshot(
                &counters,
                reconnects_supported,
                event_monitor.as_deref_mut(),
            )
            .await?
            .delta(counter_baseline);
            let next_boundary = (boundary + config.report_interval).min(finish);
            let stats = workers.snapshot(next_boundary).await?;
            reported_stats.merge(&stats)?;
            interval_index += 1;
            reporter.interval(
                interval_index,
                boundary.duration_since(started),
                boundary.duration_since(previous_boundary),
                &stats,
                counter_snapshot,
                counter_snapshot.delta(previous_counters),
            )?;
            previous_counters = counter_snapshot;
            previous_boundary = boundary;
            boundary = next_boundary;
        }

        wait_for_boundary(finish, finish, &mut completion, &mut chaos_completed).await?;
        let standalone_final = if reconnects_supported {
            Some(
                lifecycle_boundary_snapshot(
                    &counters,
                    reconnects_supported,
                    event_monitor.as_deref_mut(),
                )
                .await?,
            )
        } else {
            None
        };
        require_chaos_completion_at_deadline(finish, &mut completion, &mut chaos_completed)?;
        let counter_final = match standalone_final {
            Some(mut snapshot) => {
                // The marker freezes ordered connection events. Completion's
                // oneshot synchronizes the chaos-task atomics separately.
                snapshot.chaos_injections = counters.chaos_injections.load(Ordering::Acquire);
                snapshot
            }
            None => counters.snapshot(false),
        };
        if let Some(schedule) = boundary_schedule.take() {
            schedule.finish().await?;
        }
        Ok((
            interval_index,
            previous_boundary,
            previous_counters,
            counter_final,
        ))
    }
    .await;

    let (interval_index, previous_boundary, previous_counters, counter_final) =
        match interval_result {
            Ok(result) => result,
            Err(error) => {
                if let Some(run) = chaos.as_mut() {
                    run.abort();
                }
                let _ = workers.stop().await;
                return Err(error);
            }
        };

    let (last_interval, aggregate) = match workers.stop().await {
        Ok(stats) => stats,
        Err(error) => {
            if let Some(run) = chaos.as_mut() {
                run.abort();
            }
            return Err(error);
        }
    };
    reported_stats.merge(&last_interval)?;
    if reported_stats.operations() != aggregate.operations()
        || reported_stats.successes != aggregate.successes
        || reported_stats.errors != aggregate.errors
    {
        return Err(format!(
            "interval statistics lost or duplicated work: intervals={} successes={} errors={}, summary={} successes={} errors={}",
            reported_stats.operations(),
            reported_stats.successes,
            reported_stats.errors,
            aggregate.operations(),
            aggregate.successes,
            aggregate.errors,
        )
        .into());
    }
    let counter_snapshot = counter_final.delta(counter_baseline);
    reporter.interval(
        interval_index + 1,
        config.duration,
        finish.duration_since(previous_boundary),
        &last_interval,
        counter_snapshot,
        counter_snapshot.delta(previous_counters),
    )?;
    Ok(Measurement {
        elapsed: config.duration,
        stats: aggregate,
        counter_baseline,
        counter_final,
    })
}

async fn lifecycle_boundary_snapshot(
    counters: &LifecycleCounters,
    reconnects_supported: bool,
    event_monitor: Option<&mut EventMonitor>,
) -> Result<CounterSnapshot> {
    if reconnects_supported {
        event_monitor
            .ok_or("standalone measurement is missing its connection event monitor")?
            .await_next_boundary()
            .await
    } else {
        Ok(counters.snapshot(false))
    }
}

async fn wait_for_boundary(
    boundary: Instant,
    measurement_finish: Instant,
    completion: &mut Option<oneshot::Receiver<std::result::Result<Instant, String>>>,
    chaos_completed: &mut bool,
) -> Result<()> {
    loop {
        if *chaos_completed {
            sleep_until(boundary).await;
            return Ok(());
        }
        let receiver = completion
            .as_mut()
            .ok_or("chaos completion receiver disappeared before completion")?;
        tokio::select! {
            _ = sleep_until(boundary) => return Ok(()),
            result = receiver => {
                match result {
                    Ok(Ok(completed_at)) if completed_at <= measurement_finish => {
                        *chaos_completed = true;
                        *completion = None;
                    }
                    Ok(Ok(_)) => {
                        return Err(
                            "chaos recovery completed after the measurement deadline".into(),
                        );
                    }
                    Ok(Err(error)) => return Err(format!("chaos recovery failed: {error}").into()),
                    Err(_) => return Err("chaos task ended without reporting recovery".into()),
                }
            }
        }
    }
}

fn require_chaos_completion_at_deadline(
    deadline: Instant,
    completion: &mut Option<oneshot::Receiver<std::result::Result<Instant, String>>>,
    chaos_completed: &mut bool,
) -> Result<()> {
    if *chaos_completed {
        return Ok(());
    }
    let receiver = completion
        .as_mut()
        .ok_or("chaos completion receiver disappeared before the measurement deadline")?;
    match receiver.try_recv() {
        Ok(Ok(completed_at)) if completed_at <= deadline => {
            *chaos_completed = true;
            *completion = None;
            Ok(())
        }
        Ok(Ok(_)) => Err("chaos recovery completed after the measurement deadline".into()),
        Ok(Err(error)) => Err(format!("chaos recovery failed: {error}").into()),
        Err(oneshot::error::TryRecvError::Empty) => {
            Err("chaos recovery did not complete before the measurement deadline".into())
        }
        Err(oneshot::error::TryRecvError::Closed) => {
            Err("chaos task ended without reporting recovery".into())
        }
    }
}

fn report_summary(
    config: &Config,
    reconnects_supported: bool,
    measurement: &Measurement,
) -> Result<()> {
    Reporter {
        output: config.output,
        reconnects_supported,
    }
    .summary(
        measurement.elapsed,
        &measurement.stats,
        measurement
            .counter_final
            .delta(measurement.counter_baseline),
    )
}

async fn run_standalone(config: Config) -> Result<()> {
    let port = match config.standalone_port {
        Some(port) => port,
        None => reserve_ephemeral_port()?,
    };
    let key: Arc<str> = Arc::from("redis-tower:soak:standalone");
    let payload = "x".repeat(config.payload_bytes);
    let expected: Arc<[u8]> = Arc::from(payload.as_bytes());
    let server = start_standalone(port, config.startup_timeout).await?;
    let address = server.addr();
    seed_standalone(&address, key.as_ref(), &payload, config.operation_timeout).await?;

    let counters = Arc::new(LifecycleCounters::default());
    let events = ConnectionEventBus::new(EVENT_CAPACITY);
    let event_stream = events.subscribe();
    let mut event_monitor =
        spawn_event_monitor(event_stream, Arc::clone(&counters), events.clone());
    let pipeline = AutoPipelineConfig {
        response_timeout: Some(config.operation_timeout),
        queue_capacity: config.concurrency.saturating_mul(4).max(128),
        ..AutoPipelineConfig::default()
    };
    let reconnect = AutoPipelineReconnectConfig::new(
        ReconnectConfig::default()
            .base_delay(Duration::from_millis(25))
            .max_delay(Duration::from_millis(250))
            .connect_timeout(config.operation_timeout),
    );
    let standalone = MultiplexedClient::from_factory_with_events(
        UrlConnectionFactory::new(format!("redis://{address}/")),
        pipeline,
        reconnect,
        events,
    )
    .await?;
    let client = SoakClient::Standalone(standalone);

    let (measurement_started_tx, measurement_started_rx) = oneshot::channel();
    let mut server_without_chaos = None;
    let mut chaos_run = None;
    match config.chaos {
        ChaosMode::None => {
            server_without_chaos = Some(server);
        }
        ChaosMode::StandaloneSigkill => {
            chaos_run = Some(spawn_standalone_chaos(
                server,
                client.clone(),
                Arc::clone(&key),
                payload.clone(),
                Arc::clone(&expected),
                Arc::clone(&counters),
                config.clone(),
                measurement_started_rx,
            ));
        }
        ChaosMode::ClusterMasterKill => unreachable!("configuration validation rejects this"),
    }

    let measurement = measure(
        &config,
        MeasurementTarget {
            client: client.clone(),
            key,
            expected,
            counters: Arc::clone(&counters),
            reconnects_supported: true,
        },
        (config.chaos != ChaosMode::None).then_some(measurement_started_tx),
        chaos_run.as_mut(),
        Some(&mut event_monitor),
    )
    .await;

    if measurement.is_err() {
        if let Some(run) = chaos_run.take() {
            run.cancel().await;
        }
        event_monitor.cancel().await;
        client.shutdown().await;
        drop(server_without_chaos);
        return measurement.map(|_| ());
    }

    event_monitor.cancel().await;
    client.shutdown().await;
    if let Some(run) = chaos_run.take() {
        run.finish().await?;
    }
    drop(server_without_chaos);

    let measurement = measurement.expect("measurement error returned above");
    let final_counters = measurement
        .counter_final
        .delta(measurement.counter_baseline);
    let lagged = final_counters.event_lagged;
    if lagged != 0 {
        return Err(format!(
            "connection event subscriber lagged by {lagged} event(s); reconnect total is not exact"
        )
        .into());
    }
    verify_requested_chaos(&config, final_counters)?;
    report_summary(&config, true, &measurement)
}

async fn run_cluster(config: Config) -> Result<()> {
    let fixture = Arc::new(
        ClusterFixture::builder()
            .startup_timeout(config.startup_timeout)
            .readiness_timeout(config.startup_timeout)
            .operation_timeout(config.operation_timeout)
            .cluster_node_timeout(config.cluster_node_timeout_ms)
            .start()
            .await?,
    );
    let key: Arc<str> = Arc::from(key_for_slot(config.cluster_slot));
    let payload = "x".repeat(config.payload_bytes);
    let expected: Arc<[u8]> = Arc::from(payload.as_bytes());
    seed_cluster(
        fixture.as_ref(),
        config.cluster_slot,
        key.as_ref(),
        &payload,
    )
    .await?;

    let pipeline = AutoPipelineConfig {
        response_timeout: Some(config.operation_timeout),
        queue_capacity: config.concurrency.saturating_mul(4).max(128),
        ..AutoPipelineConfig::default()
    };
    let reconnect = AutoPipelineReconnectConfig::new(
        ReconnectConfig::default()
            .max_retries(8)
            .base_delay(Duration::from_millis(25))
            .max_delay(Duration::from_millis(250))
            .connect_timeout(config.operation_timeout),
    );
    let cluster = MultiplexedClusterClient::builder(fixture.seed_addr())
        .pipeline_config(pipeline)
        .reconnect_config(reconnect)
        .connect()
        .await?;
    let client = SoakClient::Cluster(cluster);
    if !client.get_matches(key.as_ref(), expected.as_ref()).await {
        client.shutdown().await;
        return Err("cluster workload key failed validation before warmup".into());
    }

    let counters = Arc::new(LifecycleCounters::default());
    let (measurement_started_tx, measurement_started_rx) = oneshot::channel();
    let mut chaos_run = None;
    if config.chaos == ChaosMode::ClusterMasterKill {
        chaos_run = Some(spawn_cluster_chaos(
            Arc::clone(&fixture),
            client.clone(),
            Arc::clone(&key),
            Arc::clone(&expected),
            Arc::clone(&counters),
            config.clone(),
            measurement_started_rx,
        ));
    }

    let measurement = measure(
        &config,
        MeasurementTarget {
            client: client.clone(),
            key,
            expected,
            counters: Arc::clone(&counters),
            reconnects_supported: false,
        },
        (config.chaos != ChaosMode::None).then_some(measurement_started_tx),
        chaos_run.as_mut(),
        None,
    )
    .await;

    if measurement.is_err() {
        if let Some(run) = chaos_run.take() {
            run.cancel().await;
        }
        client.shutdown().await;
        return measurement.map(|_| ());
    }
    client.shutdown().await;
    if let Some(run) = chaos_run.take() {
        run.finish().await?;
    }
    let measurement = measurement.expect("measurement error returned above");
    verify_requested_chaos(
        &config,
        measurement
            .counter_final
            .delta(measurement.counter_baseline),
    )?;
    report_summary(&config, false, &measurement)?;
    drop(fixture);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProcessIdentity {
    pid: u32,
    command: String,
    started: String,
    cwd: PathBuf,
}

struct StandaloneCleanup {
    base_dir: PathBuf,
    node_dir: PathBuf,
    config_path: PathBuf,
    pid_path: PathBuf,
    owner_path: PathBuf,
    owner_token: String,
    port: u16,
    adopted: Option<ProcessIdentity>,
    cleanup_timeout: Duration,
}

impl StandaloneCleanup {
    fn new(port: u16) -> Result<Self> {
        let process = std::process::id();
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let (base_dir, owner_token) = (0..32_u64)
            .find_map(|attempt| {
                let sequence = STANDALONE_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
                let token = format!("{process}-{port}-{created}-{sequence}-{attempt}");
                let path = std::env::temp_dir().join(format!("redis-tower-soak-{token}"));
                match fs::create_dir(&path) {
                    Ok(()) => Some(Ok((path, token))),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .ok_or("could not allocate a unique standalone ownership directory")??;
        let owner_path = base_dir.join("owner");
        if let Err(error) = fs::write(&owner_path, &owner_token) {
            let _ = fs::remove_dir(&base_dir);
            return Err(error.into());
        }
        let node_dir = base_dir.join(format!("node-{port}"));
        Ok(Self {
            config_path: node_dir.join("redis.conf"),
            pid_path: node_dir.join("redis.pid"),
            base_dir,
            node_dir,
            owner_path,
            owner_token,
            port,
            adopted: None,
            cleanup_timeout: PROCESS_CLEANUP_TIMEOUT,
        })
    }

    fn adopt(&mut self, pid: u32) -> Result<()> {
        let identity = self.owned_identity_for_pid(pid).ok_or_else(|| {
            format!(
                "Redis pid={pid} did not match its unique soak ownership identity at {}",
                self.base_dir.display()
            )
        })?;
        self.adopted = Some(identity);
        Ok(())
    }

    fn owned_identity(&self) -> Option<ProcessIdentity> {
        match &self.adopted {
            Some(identity) if self.identity_still_owned(identity) => Some(identity.clone()),
            Some(_) => None,
            None => self
                .pid_path
                .exists()
                .then(|| read_pid(&self.pid_path))
                .flatten()
                .and_then(|pid| self.owned_identity_for_pid(pid)),
        }
    }

    fn owned_identity_for_pid(&self, pid: u32) -> Option<ProcessIdentity> {
        if read_pid(&self.pid_path) != Some(pid)
            || fs::read_to_string(&self.owner_path).ok()?.trim() != self.owner_token
            || !self.config_matches()
        {
            return None;
        }
        let identity = inspect_process(pid)?;
        if !redis_command_matches(&identity.command, &self.config_path, self.port)
            || !same_directory(&identity.cwd, &self.node_dir)
        {
            return None;
        }
        Some(identity)
    }

    fn identity_still_owned(&self, expected: &ProcessIdentity) -> bool {
        self.owned_identity_for_pid(expected.pid).as_ref() == Some(expected)
    }

    fn config_matches(&self) -> bool {
        let Ok(config) = fs::read_to_string(&self.config_path) else {
            return false;
        };
        config_directive_equals(&config, "port", &self.port.to_string())
            && config_directive_equals(&config, "pidfile", &self.pid_path.display().to_string())
            && config_directive_equals(&config, "dir", &self.node_dir.display().to_string())
    }

    fn signal_owned(&self, expected: &ProcessIdentity, signal: &str) -> bool {
        if !self.identity_still_owned(expected) {
            return false;
        }
        ProcessCommand::new("kill")
            .args([signal, &expected.pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn terminate_owned(&self, expected: &ProcessIdentity) {
        if !self.signal_owned(expected, "-TERM") {
            return;
        }
        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        while self.identity_still_owned(expected) && std::time::Instant::now() < deadline {
            std::thread::sleep(PROCESS_CLEANUP_POLL);
        }
        if self.identity_still_owned(expected) {
            let _ = self.signal_owned(expected, "-KILL");
        }
    }

    fn cleanup(&mut self) {
        let deadline = std::time::Instant::now() + self.cleanup_timeout;
        let mut identity = self.owned_identity();
        if let Some(expected) = &identity {
            self.terminate_owned(expected);
        }

        let mut quiet_since = None;
        loop {
            if identity.is_none() && self.adopted.is_none() {
                identity = self.owned_identity();
                if let Some(expected) = &identity {
                    self.terminate_owned(expected);
                }
            }
            let owned_alive = identity
                .as_ref()
                .is_some_and(|expected| self.identity_still_owned(expected));
            // Port state is observation only. It can extend the quiescence
            // wait, but it never authorizes a signal or Redis command.
            if !owned_alive && !port_is_open(self.port) {
                let quiet = quiet_since.get_or_insert_with(std::time::Instant::now);
                if quiet.elapsed() >= Duration::from_millis(150) {
                    break;
                }
            } else {
                quiet_since = None;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(PROCESS_CLEANUP_POLL);
        }

        if let Some(expected) = &identity
            && self.identity_still_owned(expected)
        {
            let _ = self.signal_owned(expected, "-KILL");
        }
        if identity
            .as_ref()
            .is_none_or(|expected| !self.identity_still_owned(expected))
        {
            let _ = fs::remove_dir_all(&self.base_dir);
        }
    }
}

fn config_directive_equals(config: &str, directive: &str, expected: &str) -> bool {
    config.lines().any(|line| {
        let mut fields = line.trim().splitn(2, char::is_whitespace);
        fields.next() == Some(directive)
            && fields
                .next()
                .is_some_and(|value| value.trim().trim_matches('"') == expected)
    })
}

impl Drop for StandaloneCleanup {
    fn drop(&mut self) {
        self.cleanup();
    }
}

struct ManagedStandalone {
    handle: Option<RedisServerHandle>,
    cleanup: StandaloneCleanup,
}

impl ManagedStandalone {
    fn handle(&self) -> &RedisServerHandle {
        self.handle
            .as_ref()
            .expect("managed standalone handle exists until drop")
    }

    fn addr(&self) -> String {
        self.handle().addr()
    }

    fn port(&self) -> u16 {
        self.handle().port()
    }

    fn pid(&self) -> u32 {
        self.handle().pid()
    }

    async fn sigkill_and_confirm(mut self, wait: Duration) -> Result<(u16, u32)> {
        let port = self.port();
        let pid = self.pid();
        let identity = self.cleanup.owned_identity().ok_or_else(|| {
            format!("refusing to SIGKILL Redis pid={pid}: its ownership identity changed")
        })?;
        chaos::kill_node(self.handle());
        wait_for_process_death(&self.cleanup, &identity, port, wait).await?;
        self.handle
            .take()
            .expect("killed standalone handle remains owned")
            .detach();
        // Drop the old PID-scoped directory guard before a replacement binds
        // the same port, so it can never target the replacement.
        drop(self);
        Ok((port, pid))
    }
}

impl Drop for ManagedStandalone {
    fn drop(&mut self) {
        // redis-server-wrapper 0.4.x performs an unsafe kill-by-port fallback
        // in its handle Drop. Detach it; only our PID-identity guard may stop
        // the process, and port state is used solely to observe quiescence.
        if let Some(handle) = self.handle.take() {
            handle.detach();
        }
    }
}

fn reserve_ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

async fn start_standalone(port: u16, startup_timeout: Duration) -> Result<ManagedStandalone> {
    let cleanup = StandaloneCleanup::new(port)?;
    let start = RedisServer::new()
        // Publication provenance fingerprints `redis-server` from PATH. Pin
        // the wrapper to that executable instead of its Redis Stack preference.
        .redis_server_bin("redis-server")
        .no_stack_modules()
        .port(port)
        .dir(&cleanup.base_dir)
        .save(false)
        .appendonly(false)
        .start();
    let handle = timeout(startup_timeout, start)
        .await
        .map_err(|_| format!("timed out after {startup_timeout:?} starting Redis on port {port}"))?
        .map_err(BoxError::from)?;
    // Wrap the raw handle before the first cancellation point. The wrapper's
    // Drop detaches redis-server-wrapper's unsafe kill-by-port fallback and
    // leaves shutdown exclusively to our PID-identity guard.
    let mut server = ManagedStandalone {
        handle: Some(handle),
        cleanup,
    };
    let reported_dir = server.handle().run(&["CONFIG", "GET", "dir"]).await;
    let expected_dir = server.cleanup.node_dir.display().to_string();
    if !matches!(reported_dir, Ok(ref reply) if reply.contains(&expected_dir)) {
        return Err(format!(
            "Redis on port {port} did not report this run's unique directory {}",
            server.cleanup.node_dir.display()
        )
        .into());
    }
    server.cleanup.adopt(server.pid())?;
    Ok(server)
}

fn read_pid(path: &Path) -> Option<u32> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    ProcessCommand::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    false
}

fn inspect_process(pid: u32) -> Option<ProcessIdentity> {
    if !pid_alive(pid) {
        return None;
    }
    let command = process_field(pid, "command")?;
    let started = process_field(pid, "lstart")?;
    let cwd = process_cwd(pid)?;
    Some(ProcessIdentity {
        pid,
        command,
        started,
        cwd,
    })
}

fn process_field(pid: u32, field: &str) -> Option<String> {
    let output = ProcessCommand::new("ps")
        .args(["-o", &format!("{field}="), "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

#[cfg(target_os = "linux")]
fn process_cwd(pid: u32) -> Option<PathBuf> {
    fs::read_link(format!("/proc/{pid}/cwd")).ok()
}

#[cfg(not(target_os = "linux"))]
fn process_cwd(pid: u32) -> Option<PathBuf> {
    let output = ProcessCommand::new("lsof")
        .args(["-a", "-p", &pid.to_string(), "-d", "cwd", "-Fn"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix('n'))
        .map(PathBuf::from)
}

fn redis_command_matches(command: &str, config_path: &Path, port: u16) -> bool {
    let Some(program) = command.split_whitespace().next() else {
        return false;
    };
    let name = program.rsplit('/').next().unwrap_or(program);
    matches!(name, "redis-server" | "redis-stack-server")
        && (command.contains(&config_path.display().to_string())
            || command
                .split_whitespace()
                .any(|argument| argument.ends_with(&format!(":{port}"))))
}

fn same_directory(actual: &Path, expected: &Path) -> bool {
    match (fs::canonicalize(actual), fs::canonicalize(expected)) {
        (Ok(actual), Ok(expected)) => actual == expected,
        _ => actual == expected,
    }
}

fn port_is_open(port: u16) -> bool {
    TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(20),
    )
    .is_ok()
}

async fn wait_for_process_death(
    cleanup: &StandaloneCleanup,
    identity: &ProcessIdentity,
    port: u16,
    wait: Duration,
) -> Result<()> {
    let deadline = Instant::now() + wait;
    loop {
        if !cleanup.identity_still_owned(identity) && !port_is_open(port) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "SIGKILL did not stop standalone Redis pid={} port={port} within {wait:?}",
                identity.pid
            )
            .into());
        }
        sleep(PROCESS_CLEANUP_POLL).await;
    }
}

async fn seed_standalone(
    address: &str,
    key: &str,
    payload: &str,
    operation_timeout: Duration,
) -> Result<()> {
    timeout(operation_timeout, async {
        let mut connection = RedisConnection::connect(address).await?;
        connection.execute(Set::new(key, payload)).await?;
        let value = connection.execute(Get::new(key)).await?;
        if !matches!(value, Some(value) if value.as_ref() == payload.as_bytes()) {
            return Err(redis_tower::RedisError::Redis(
                "soak seed key was missing or corrupt".into(),
            ));
        }
        Ok::<(), redis_tower::RedisError>(())
    })
    .await
    .map_err(|_| format!("timed out seeding standalone Redis at {address}"))??;
    Ok(())
}

async fn seed_cluster(fixture: &ClusterFixture, slot: u16, key: &str, payload: &str) -> Result<()> {
    let topology = fixture.topology().await?;
    let owner = topology
        .owner_of_slot(slot)
        .ok_or_else(|| format!("cluster slot {slot} has no owner"))?;
    let response = fixture
        .run_node(owner.index, &["SET", key, payload])
        .await?;
    if response.trim() != "OK" {
        return Err(format!("cluster seed SET returned {response:?}").into());
    }
    let replicas = fixture
        .run_node(owner.index, &["WAIT", "1", "5000"])
        .await?;
    let replicas = replicas
        .trim()
        .parse::<u64>()
        .map_err(|error| format!("cluster seed WAIT returned {replicas:?}: {error}"))?;
    if replicas < 1 {
        return Err("cluster seed did not reach a replica before the chaos run".into());
    }
    Ok(())
}

fn spawn_event_monitor(
    mut stream: redis_tower::ConnectionEventStream,
    counters: Arc<LifecycleCounters>,
    bus: ConnectionEventBus,
) -> EventMonitor {
    let (snapshot_tx, snapshots) = mpsc::unbounded_channel();
    let task = AbortOnDropTask::spawn(async move {
        loop {
            match stream.recv().await {
                Ok(event) if event_boundary_number(&event).is_some() => {
                    let boundary = event_boundary_number(&event)
                        .expect("guard established a lifecycle boundary marker");
                    if snapshot_tx
                        .send((boundary, counters.snapshot(true)))
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(ConnectionEvent::Reconnected { .. }) => {
                    counters
                        .standalone_reconnect_recoveries
                        .fetch_add(1, Ordering::AcqRel);
                    counters.standalone_reconnect_notify.notify_one();
                }
                Ok(_) => {}
                Err(ConnectionEventRecvError::Lagged { skipped }) => {
                    counters.event_lagged.fetch_add(skipped, Ordering::AcqRel);
                }
                Err(ConnectionEventRecvError::Closed) => return,
                Err(_) => {
                    counters.event_lagged.fetch_add(1, Ordering::AcqRel);
                    return;
                }
            }
        }
    });
    EventMonitor {
        bus,
        snapshots,
        task,
        next_boundary_to_publish: 1,
        next_boundary_to_observe: 1,
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_standalone_chaos(
    server: ManagedStandalone,
    client: SoakClient,
    key: Arc<str>,
    payload: String,
    expected: Arc<[u8]>,
    counters: Arc<LifecycleCounters>,
    config: Config,
    measurement_started: oneshot::Receiver<Instant>,
) -> ChaosRun {
    let (completion_tx, completion_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = AbortOnDropTask::spawn(async move {
        let result: Result<ManagedStandalone> = async {
            let started = measurement_started
                .await
                .map_err(|_| "measurement stopped before standalone chaos could start")?;
            sleep_until(started + config.chaos_after).await;

            let recovery_deadline = Instant::now() + config.recovery_timeout;
            let port = server.port();
            let old_pid = server.pid();
            let reconnect_before = counters
                .standalone_reconnect_recoveries
                .load(Ordering::Acquire);
            eprintln!(
                "soak-bench: SIGKILL standalone Redis pid={old_pid} port={port} at +{:.1}s",
                config.chaos_after.as_secs_f64()
            );
            let remaining = remaining_until(recovery_deadline, "confirming standalone SIGKILL")?;
            let (confirmed_port, confirmed_pid) = server.sigkill_and_confirm(remaining).await?;
            debug_assert_eq!(confirmed_port, port);
            debug_assert_eq!(confirmed_pid, old_pid);
            counters.chaos_injections.fetch_add(1, Ordering::AcqRel);

            let remaining = remaining_until(recovery_deadline, "restarting standalone Redis")?;
            let replacement = start_standalone(port, config.startup_timeout.min(remaining)).await?;
            if replacement.pid() == old_pid {
                return Err("standalone replacement reused the killed process PID".into());
            }
            if replacement.port() != port {
                return Err(format!(
                    "standalone replacement moved from port {port} to {}",
                    replacement.port()
                )
                .into());
            }
            let remaining = remaining_until(recovery_deadline, "seeding standalone replacement")?;
            seed_standalone(
                &replacement.addr(),
                key.as_ref(),
                &payload,
                config.operation_timeout.min(remaining),
            )
            .await?;
            let remaining = remaining_until(recovery_deadline, "probing standalone recovery")?;
            wait_for_client_recovery(
                &client,
                key.as_ref(),
                expected.as_ref(),
                config.operation_timeout.min(remaining),
                remaining,
            )
            .await?;
            let remaining = remaining_until(
                recovery_deadline,
                "observing the standalone reconnect event",
            )?;
            wait_for_counter_increment(&counters, reconnect_before, remaining).await?;
            eprintln!(
                "soak-bench: standalone client recovered on port={port} replacement_pid={}",
                replacement.pid()
            );
            Ok(replacement)
        }
        .await;

        drop(client);
        match result {
            Ok(replacement) => {
                let _ = completion_tx.send(Ok(Instant::now()));
                let _ = release_rx.await;
                drop(replacement);
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                let _ = completion_tx.send(Err(message));
                Err(error)
            }
        }
    });
    ChaosRun {
        task,
        completion: Some(completion_rx),
        release: Some(release_tx),
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_cluster_chaos(
    fixture: Arc<ClusterFixture>,
    client: SoakClient,
    key: Arc<str>,
    expected: Arc<[u8]>,
    counters: Arc<LifecycleCounters>,
    config: Config,
    measurement_started: oneshot::Receiver<Instant>,
) -> ChaosRun {
    let (completion_tx, completion_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = AbortOnDropTask::spawn(async move {
        let result: Result<()> = async {
            let started = measurement_started
                .await
                .map_err(|_| "measurement stopped before cluster chaos could start")?;
            sleep_until(started + config.chaos_after).await;

            let old_owner = fixture.kill_slot_owner(config.cluster_slot).await?;
            counters.chaos_injections.fetch_add(1, Ordering::AcqRel);
            eprintln!(
                "soak-bench: SIGKILL cluster slot={} owner={} ({}) at +{:.1}s",
                config.cluster_slot,
                old_owner.id,
                old_owner.addr,
                config.chaos_after.as_secs_f64()
            );

            let recovery_deadline = Instant::now() + config.recovery_timeout;
            let remaining = remaining_until(recovery_deadline, "polling cluster topology")?;
            let new_owner = fixture
                .wait_for_slot_owner_change(config.cluster_slot, &old_owner.id, remaining)
                .await?;
            let remaining = remaining_until(recovery_deadline, "probing cluster recovery")?;
            wait_for_client_recovery(
                &client,
                key.as_ref(),
                expected.as_ref(),
                config.operation_timeout.min(remaining),
                remaining,
            )
            .await?;
            counters.cluster_recoveries.fetch_add(1, Ordering::AcqRel);
            eprintln!(
                "soak-bench: cluster harness observed slot={} owner change {} -> {} and a successful client GET",
                config.cluster_slot, old_owner.id, new_owner.id
            );
            Ok(())
        }
        .await;

        drop(client);
        drop(fixture);
        match result {
            Ok(()) => {
                let _ = completion_tx.send(Ok(Instant::now()));
                let _ = release_rx.await;
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                let _ = completion_tx.send(Err(message));
                Err(error)
            }
        }
    });
    ChaosRun {
        task,
        completion: Some(completion_rx),
        release: Some(release_tx),
    }
}

async fn wait_for_client_recovery(
    client: &SoakClient,
    key: &str,
    expected: &[u8],
    operation_timeout: Duration,
    recovery_timeout: Duration,
) -> Result<()> {
    timeout(recovery_timeout, async {
        loop {
            if timeout(operation_timeout, client.get_matches(key, expected))
                .await
                .unwrap_or(false)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| format!("client did not recover within {recovery_timeout:?}"))?;
    Ok(())
}

fn remaining_until(deadline: Instant, operation: &str) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| format!("recovery deadline elapsed while {operation}").into())
}

async fn wait_for_counter_increment(
    counters: &LifecycleCounters,
    previous: u64,
    wait: Duration,
) -> Result<()> {
    let deadline = Instant::now() + wait;
    loop {
        if counters
            .standalone_reconnect_recoveries
            .load(Ordering::Acquire)
            > previous
        {
            return Ok(());
        }
        let notified = counters.standalone_reconnect_notify.notified();
        if counters
            .standalone_reconnect_recoveries
            .load(Ordering::Acquire)
            > previous
        {
            return Ok(());
        }
        let remaining = deadline.checked_duration_since(Instant::now()).ok_or(
            "client recovered but its ConnectionEvent::Reconnected was not observed in time",
        )?;
        timeout(remaining, notified).await.map_err(
            |_| "client recovered but its ConnectionEvent::Reconnected was not observed in time",
        )?;
    }
}

fn verify_requested_chaos(config: &Config, counters: CounterSnapshot) -> Result<()> {
    match config.chaos {
        ChaosMode::None => {
            if counters.chaos_injections != 0 {
                return Err("chaos was recorded even though SOAK_CHAOS=none".into());
            }
        }
        ChaosMode::StandaloneSigkill | ChaosMode::ClusterMasterKill => {
            if counters.chaos_injections != 1 {
                return Err(format!(
                    "requested one chaos injection, observed {}",
                    counters.chaos_injections
                )
                .into());
            }
            if counters.recoveries == 0 {
                return Err(
                    "the requested chaos run completed without an observed recovery".into(),
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_histograms_merge_without_per_operation_storage() {
        let mut first = Stats::new();
        let mut second = Stats::new();
        for value in 1..=10_000_u64 {
            first.record(true, Duration::from_micros(value));
            second.record(value % 10 != 0, Duration::from_micros(value * 2));
        }
        first.merge(&second).unwrap();
        assert_eq!(first.successes, 19_000);
        assert_eq!(first.errors, 1_000);
        assert_eq!(first.latency.len(), 19_000);
        assert!(first.latency.value_at_quantile(0.99) > 9_000);
    }

    #[test]
    fn interval_reset_keeps_the_full_run_histogram() {
        let started = Instant::now();
        let first_deadline = started + Duration::from_secs(1);
        let second_deadline = started + Duration::from_secs(2);
        let mut worker = WorkerStats::new();
        worker.record_completion(
            CompletedOperation {
                success: true,
                elapsed: Duration::from_micros(10),
                completed: started + Duration::from_millis(100),
            },
            first_deadline,
            second_deadline,
        );
        worker.record_completion(
            CompletedOperation {
                success: false,
                elapsed: Duration::from_micros(20),
                completed: started + Duration::from_millis(200),
            },
            first_deadline,
            second_deadline,
        );
        let first = worker.take_interval(second_deadline);
        assert_eq!(first.operations(), 2);
        assert_eq!(worker.interval.operations(), 0);
        assert_eq!(worker.aggregate.operations(), 2);
        worker.record_completion(
            CompletedOperation {
                success: true,
                elapsed: Duration::from_micros(30),
                completed: started + Duration::from_millis(1_500),
            },
            second_deadline,
            second_deadline,
        );
        assert_eq!(worker.interval.operations(), 1);
        assert_eq!(worker.aggregate.operations(), 3);
    }

    #[test]
    fn cross_boundary_completion_is_carried_and_counted_exactly_once() {
        let started = Instant::now();
        let first_deadline = started + Duration::from_secs(1);
        let second_deadline = started + Duration::from_secs(2);
        let mut worker = WorkerStats::new();
        worker.record_completion(
            CompletedOperation {
                success: true,
                elapsed: Duration::from_millis(200),
                completed: first_deadline + Duration::from_millis(50),
            },
            first_deadline,
            second_deadline,
        );

        let first = worker.take_interval(second_deadline);
        worker.record_completion(
            CompletedOperation {
                success: false,
                elapsed: Duration::from_millis(10),
                completed: first_deadline + Duration::from_millis(500),
            },
            second_deadline,
            second_deadline,
        );
        let second = worker.take_interval(second_deadline);
        assert_eq!(first.operations(), 0);
        assert_eq!(second.successes, 1);
        assert_eq!(second.errors, 1);
        assert_eq!(first.operations() + second.operations(), 2);
        assert_eq!(worker.aggregate.operations(), 2);
        assert_eq!(worker.aggregate.successes, 1);
        assert_eq!(worker.aggregate.errors, 1);
    }

    #[test]
    fn completion_after_final_deadline_is_excluded() {
        let started = Instant::now();
        let finish = started + Duration::from_secs(1);
        let mut worker = WorkerStats::new();
        worker.record_completion(
            CompletedOperation {
                success: false,
                elapsed: Duration::from_secs(1),
                completed: finish + Duration::from_nanos(1),
            },
            finish,
            finish,
        );
        assert_eq!(worker.interval.operations(), 0);
        assert_eq!(worker.aggregate.operations(), 0);
    }

    #[test]
    fn counter_snapshots_report_interval_deltas() {
        let previous = CounterSnapshot {
            reconnects: 2,
            recoveries: 1,
            chaos_injections: 0,
            event_lagged: 4,
        };
        let current = CounterSnapshot {
            reconnects: 5,
            recoveries: 2,
            chaos_injections: 1,
            event_lagged: 6,
        };
        let delta = current.delta(previous);
        assert_eq!(delta.reconnects, 3);
        assert_eq!(delta.recoveries, 1);
        assert_eq!(delta.chaos_injections, 1);
        assert_eq!(delta.event_lagged, 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parses_linux_rss_in_kibibytes() {
        let status = "Name:\tsoak-bench\nVmRSS:\t   1234 kB\nThreads:\t8\n";
        assert_eq!(parse_linux_rss(status), Some(1234 * 1024));
    }

    #[test]
    fn validates_mode_specific_chaos() {
        let config = Config {
            mode: Mode::Cluster,
            chaos: ChaosMode::StandaloneSigkill,
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_error_backoff_and_latency_above_histogram_range() {
        let zero_backoff = Config {
            error_backoff: Duration::ZERO,
            ..Config::default()
        };
        assert!(zero_backoff.validate().is_err());

        let excessive_timeout = Config {
            operation_timeout: Duration::from_secs(61),
            ..Config::default()
        };
        assert!(excessive_timeout.validate().is_err());

        let maximum_timeout = Config {
            operation_timeout: Duration::from_secs(60),
            ..Config::default()
        };
        assert!(maximum_timeout.validate().is_ok());

        let mut stats = Stats::new();
        stats.record(true, Duration::from_millis(60_001));
        assert_eq!(stats.successes, 1);
    }

    #[test]
    fn worker_control_timeout_includes_large_error_backoff() {
        assert_eq!(
            worker_control_timeout(Duration::from_secs(2), Duration::from_secs(90)),
            Duration::from_secs(97)
        );
    }

    #[test]
    fn standalone_reconnect_and_recovery_are_one_atomic_snapshot() {
        let counters = LifecycleCounters::default();
        counters
            .standalone_reconnect_recoveries
            .store(7, Ordering::Relaxed);
        let snapshot = counters.snapshot(true);
        assert_eq!(snapshot.reconnects, 7);
        assert_eq!(snapshot.recoveries, 7);
    }

    #[tokio::test]
    async fn lifecycle_boundaries_exclude_queued_warmup_and_post_deadline_events() {
        let counters = Arc::new(LifecycleCounters::default());
        let bus = ConnectionEventBus::new(16);
        let stream = bus.subscribe();
        let mut monitor = spawn_event_monitor(stream, Arc::clone(&counters), bus.clone());
        let reconnect = || ConnectionEvent::Reconnected {
            attempts: 1,
            elapsed: Duration::from_millis(1),
        };

        assert!(bus.publish(reconnect()));
        let baseline = monitor.capture_boundary().await.unwrap();
        assert!(bus.publish(reconnect()));
        let final_snapshot = monitor.capture_boundary().await.unwrap();
        assert!(bus.publish(reconnect()));
        wait_for_counter_increment(&counters, 2, Duration::from_secs(1))
            .await
            .unwrap();

        let measured = final_snapshot.delta(baseline);
        assert_eq!(baseline.reconnects, 1);
        assert_eq!(measured.reconnects, 1);
        assert_eq!(measured.recoveries, 1);
        assert_eq!(counters.snapshot(true).reconnects, 3);
        monitor.cancel().await;
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_boundaries_are_published_while_reporting_is_blocked() {
        let counters = Arc::new(LifecycleCounters::default());
        let bus = ConnectionEventBus::new(16);
        let stream = bus.subscribe();
        let mut monitor = spawn_event_monitor(stream, Arc::clone(&counters), bus.clone());
        let reconnect = || ConnectionEvent::Reconnected {
            attempts: 1,
            elapsed: Duration::from_millis(1),
        };

        let (started, start_boundary) = monitor.publish_boundary().unwrap();
        let baseline = monitor.await_boundary(start_boundary).await.unwrap();
        let schedule = monitor
            .arm_boundaries(started, Duration::from_secs(3), Duration::from_secs(1))
            .unwrap();

        tokio::task::yield_now().await;
        sleep(Duration::from_millis(2_500)).await;
        assert!(bus.publish(reconnect()));
        sleep(Duration::from_millis(500)).await;
        schedule.finish().await.unwrap();
        assert!(bus.publish(reconnect()));

        let first = monitor.await_next_boundary().await.unwrap();
        let second = monitor.await_next_boundary().await.unwrap();
        let final_snapshot = monitor.await_next_boundary().await.unwrap();
        let after_final = monitor.capture_boundary().await.unwrap();

        assert_eq!(first.delta(baseline).reconnects, 0);
        assert_eq!(second.delta(first).reconnects, 0);
        assert_eq!(final_snapshot.delta(second).reconnects, 1);
        assert_eq!(after_final.delta(final_snapshot).reconnects, 1);
        monitor.cancel().await;
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_recovery_after_measurement_deadline_is_rejected_and_cancelled() {
        let recoveries = Arc::new(AtomicU64::new(0));
        let delayed_recoveries = Arc::clone(&recoveries);
        let (completion_tx, completion_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task = AbortOnDropTask::spawn(async move {
            sleep(Duration::from_secs(11)).await;
            delayed_recoveries.fetch_add(1, Ordering::Relaxed);
            let _ = completion_tx.send(Ok(Instant::now()));
            let _ = release_rx.await;
            Ok(())
        });
        let mut chaos = ChaosRun {
            task,
            completion: Some(completion_rx),
            release: Some(release_tx),
        };
        let mut completion = Some(chaos.take_completion().unwrap());
        let mut completed = false;
        let deadline = Instant::now() + Duration::from_secs(10);

        sleep(Duration::from_secs(10)).await;
        let error = require_chaos_completion_at_deadline(deadline, &mut completion, &mut completed)
            .expect_err("late cluster recovery must fail at the measurement boundary");
        assert!(error.to_string().contains("measurement deadline"));
        chaos.cancel().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(recoveries.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_recovery_with_post_deadline_timestamp_is_rejected() {
        let deadline = Instant::now() + Duration::from_secs(10);
        let (completion_tx, completion_rx) = oneshot::channel();
        completion_tx
            .send(Ok(deadline + Duration::from_nanos(1)))
            .unwrap();
        let mut completion = Some(completion_rx);
        let mut completed = false;

        let error = require_chaos_completion_at_deadline(deadline, &mut completion, &mut completed)
            .expect_err("a queued post-deadline completion must remain late");
        assert!(error.to_string().contains("after the measurement deadline"));
        assert!(!completed);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_abort_owned_task_prevents_delayed_destructive_work() {
        let injections = Arc::new(AtomicU64::new(0));
        let delayed_injections = Arc::clone(&injections);
        let task = AbortOnDropTask::spawn(async move {
            sleep(Duration::from_secs(5)).await;
            delayed_injections.fetch_add(1, Ordering::Relaxed);
        });
        drop(task);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(injections.load(Ordering::Relaxed), 0);
    }

    #[cfg(unix)]
    #[test]
    fn stale_pidfile_naming_this_process_is_not_owned() {
        let port = reserve_ephemeral_port().unwrap();
        let mut cleanup = StandaloneCleanup::new(port).unwrap();
        cleanup.cleanup_timeout = Duration::from_millis(250);
        write_guard_config(&cleanup, std::process::id());
        assert!(cleanup.owned_identity().is_none());
        let base_dir = cleanup.base_dir.clone();
        drop(cleanup);
        assert!(!base_dir.exists());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_occupied_port_is_preserved_after_failed_startup() {
        let port = reserve_ephemeral_port().unwrap();
        let existing = start_standalone(port, Duration::from_secs(20))
            .await
            .expect("start existing Redis");
        let existing_pid = existing.pid();

        let result = start_standalone(port, Duration::from_secs(3)).await;
        assert!(result.is_err());
        assert!(existing.handle().is_alive().await);
        assert!(pid_alive(existing_pid));
        assert!(port_is_open(port));
        drop(existing);
        wait_until_process_is_gone(existing_pid, port).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_stale_reused_redis_pid_is_not_owned() {
        let port = reserve_ephemeral_port().unwrap();
        let existing = start_standalone(port, Duration::from_secs(20))
            .await
            .expect("start unrelated Redis");
        let existing_pid = existing.pid();
        let mut cleanup = StandaloneCleanup::new(port).unwrap();
        cleanup.cleanup_timeout = Duration::from_millis(300);
        write_guard_config(&cleanup, existing_pid);

        assert!(cleanup.owned_identity().is_none());
        drop(cleanup);
        assert!(existing.handle().is_alive().await);
        assert!(pid_alive(existing_pid));
        drop(existing);
        wait_until_process_is_gone(existing_pid, port).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_late_unowned_pidfile_does_not_authorize_cleanup() {
        let port = reserve_ephemeral_port().unwrap();
        let existing = start_standalone(port, Duration::from_secs(20))
            .await
            .expect("start unrelated Redis");
        let existing_pid = existing.pid();
        let mut cleanup = StandaloneCleanup::new(port).unwrap();
        cleanup.cleanup_timeout = Duration::from_millis(500);
        fs::create_dir_all(&cleanup.node_dir).unwrap();
        fs::write(&cleanup.config_path, matching_guard_config(&cleanup)).unwrap();
        let pid_path = cleanup.pid_path.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            fs::write(pid_path, existing_pid.to_string()).unwrap();
        });

        drop(cleanup);
        writer.join().unwrap();
        assert!(existing.handle().is_alive().await);
        assert!(pid_alive(existing_pid));
        drop(existing);
        wait_until_process_is_gone(existing_pid, port).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_startup_cancellation_reaps_pid_and_port() {
        let port = reserve_ephemeral_port().unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let task = AbortOnDropTask::spawn(async move {
            let server = start_standalone(port, Duration::from_secs(20))
                .await
                .expect("start managed Redis");
            let _ = started_tx.send(server.pid());
            std::future::pending::<()>().await;
            drop(server);
        });
        let pid = timeout(Duration::from_secs(20), started_rx)
            .await
            .expect("startup bound")
            .expect("startup signal");
        assert!(pid_alive(pid));
        assert!(port_is_open(port));
        drop(task);
        wait_until_process_is_gone(pid, port).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_startup_timeout_leaves_no_orphan_listener() {
        let port = reserve_ephemeral_port().unwrap();
        let result = start_standalone(port, Duration::from_nanos(1)).await;
        assert!(result.is_err());
        timeout(Duration::from_secs(4), async {
            while port_is_open(port) {
                sleep(PROCESS_CLEANUP_POLL).await;
            }
        })
        .await
        .expect("startup timeout cleanup bound");
        assert!(!port_is_open(port));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_sigkill_is_confirmed_before_same_port_restart() {
        let port = reserve_ephemeral_port().unwrap();
        let server = start_standalone(port, Duration::from_secs(20))
            .await
            .expect("start managed Redis");
        let old_pid = server.pid();
        let (confirmed_port, confirmed_pid) = server
            .sigkill_and_confirm(Duration::from_secs(5))
            .await
            .expect("confirm SIGKILL");
        assert_eq!(confirmed_port, port);
        assert_eq!(confirmed_pid, old_pid);
        assert!(!pid_alive(old_pid));
        assert!(!port_is_open(port));

        let replacement = start_standalone(port, Duration::from_secs(20))
            .await
            .expect("restart on the same port");
        assert_ne!(replacement.pid(), old_pid);
        drop(replacement);
        assert!(!port_is_open(port));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires redis-server and redis-cli on PATH"]
    async fn live_cluster_recovery_after_measurement_boundary_fails() {
        let config = Config {
            mode: Mode::Cluster,
            chaos: ChaosMode::ClusterMasterKill,
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            report_interval: Duration::from_millis(500),
            chaos_after: Duration::from_millis(100),
            concurrency: 2,
            operation_timeout: Duration::from_millis(500),
            error_backoff: Duration::from_millis(1),
            startup_timeout: Duration::from_secs(30),
            recovery_timeout: Duration::from_secs(10),
            payload_bytes: 16,
            cluster_slot: 42,
            cluster_node_timeout_ms: 1_000,
            standalone_port: None,
            output: OutputFormat::JsonLines,
        };
        config.validate().unwrap();
        let error = run(config)
            .await
            .expect_err("cluster recovery after the one-second run must fail");
        assert!(error.to_string().contains("measurement deadline"));
    }

    #[cfg(unix)]
    async fn wait_until_process_is_gone(pid: u32, port: u16) {
        timeout(Duration::from_secs(5), async {
            while pid_alive(pid) || port_is_open(port) {
                sleep(PROCESS_CLEANUP_POLL).await;
            }
        })
        .await
        .expect("managed process cleanup bound");
        assert!(!pid_alive(pid));
        assert!(!port_is_open(port));
    }

    #[cfg(unix)]
    fn write_guard_config(cleanup: &StandaloneCleanup, pid: u32) {
        fs::create_dir_all(&cleanup.node_dir).unwrap();
        fs::write(&cleanup.config_path, matching_guard_config(cleanup)).unwrap();
        fs::write(&cleanup.pid_path, pid.to_string()).unwrap();
    }

    #[cfg(unix)]
    fn matching_guard_config(cleanup: &StandaloneCleanup) -> String {
        format!(
            "port {}\npidfile \"{}\"\ndir \"{}\"\n",
            cleanup.port,
            cleanup.pid_path.display(),
            cleanup.node_dir.display(),
        )
    }
}
