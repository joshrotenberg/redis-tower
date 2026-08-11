//! Tower layer for collecting per-command metrics at the Frame level.
//!
//! Framework-agnostic: users implement [`MetricsRecorder`] for their
//! metrics backend (prometheus, metrics crate, etc.).
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use redis_tower::metrics_layer::{MetricsLayer, MetricsRecorder, ErrorKind};
//!
//! struct MyRecorder;
//!
//! impl MetricsRecorder for MyRecorder {
//!     fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
//!         match error {
//!             None => println!("{command} took {duration:?} (ok)"),
//!             Some(kind) => println!("{command} took {duration:?} (error: {kind:?})"),
//!         }
//!     }
//! }
//!
//! let layer = MetricsLayer::new(MyRecorder);
//! # let _ = layer;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Frame, RedisError};
use tower_service::Service;

#[cfg(feature = "metrics")]
use crate::multiplexed::MultiplexedClient;
use crate::pool::HealthProbeKind;
#[cfg(feature = "metrics")]
use crate::pool::{ConnectionPool, PoolStats};

/// Categorizes a Redis error for metrics labeling.
///
/// Used as the `error` argument to [`MetricsRecorder::command_completed`].
/// Each variant corresponds to a meaningful alert/dashboard category.
///
/// # Categories
///
/// - `Connection` — transport-level failures: IO errors, closed connections.
///   These are transient and typically require reconnection.
/// - `Timeout` — pool acquisition timeout; indicates pool exhaustion.
/// - `WrongType` — Redis `WRONGTYPE` error; indicates an application bug.
/// - `CircuitOpen` — circuit breaker is open; request rejected without
///   touching Redis.
/// - `QueueFull` — the auto-pipeline channel is full; caller should shed load.
/// - `Auth` — authentication failure (`NOAUTH`, `WRONGPASS`).
/// - `Other` — all other errors (generic Redis errors, protocol errors, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transport-level error (IO error, connection closed).
    Connection,
    /// Pool acquisition timed out.
    Timeout,
    /// Redis `WRONGTYPE` error.
    WrongType,
    /// Circuit breaker is open.
    CircuitOpen,
    /// Auto-pipeline queue is full.
    QueueFull,
    /// Authentication error (`NOAUTH`, `WRONGPASS`).
    Auth,
    /// All other errors.
    Other,
}

impl ErrorKind {
    /// Map a [`RedisError`] to the most specific [`ErrorKind`].
    pub fn from_error(e: &RedisError) -> Self {
        match e {
            RedisError::Connection { .. } | RedisError::ConnectionClosed => ErrorKind::Connection,
            RedisError::PoolAcquisitionTimeout { .. } => ErrorKind::Timeout,
            RedisError::CircuitOpen => ErrorKind::CircuitOpen,
            RedisError::QueueFull => ErrorKind::QueueFull,
            RedisError::Redis(msg) => {
                if msg.starts_with("WRONGTYPE") {
                    ErrorKind::WrongType
                } else if msg.starts_with("NOAUTH") || msg.starts_with("WRONGPASS") {
                    ErrorKind::Auth
                } else {
                    ErrorKind::Other
                }
            }
            _ => ErrorKind::Other,
        }
    }

    /// Classify a Redis-level error frame (i.e. `Frame::Error`) by its
    /// error prefix.
    fn from_frame_error(bytes: &bytes::Bytes) -> Self {
        let msg = std::str::from_utf8(bytes).unwrap_or("");
        if msg.starts_with("WRONGTYPE") {
            ErrorKind::WrongType
        } else if msg.starts_with("NOAUTH") || msg.starts_with("WRONGPASS") {
            ErrorKind::Auth
        } else {
            ErrorKind::Other
        }
    }

    #[cfg(feature = "metrics")]
    fn as_label(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Timeout => "timeout",
            Self::WrongType => "wrong_type",
            Self::CircuitOpen => "circuit_open",
            Self::QueueFull => "queue_full",
            Self::Auth => "auth",
            Self::Other => "_OTHER",
        }
    }
}

/// The bounded set of redirect responses emitted by Redis Cluster.
///
/// Keeping redirect kinds as an enum rather than an arbitrary string prevents
/// accidental high-cardinality metric labels. The target node and hash slot
/// belong in tracing events; redirect counters need only distinguish the two
/// protocol-defined redirect modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterRedirectKind {
    /// A permanent slot-owner redirect (`MOVED`).
    Moved,
    /// A temporary migrating-slot redirect (`ASK`).
    Ask,
}

impl ClusterRedirectKind {
    #[cfg(feature = "metrics")]
    fn as_label(self) -> &'static str {
        match self {
            Self::Moved => "moved",
            Self::Ask => "ask",
        }
    }
}

/// The bounded outcome of a Redis Cluster topology refresh attempt.
///
/// `Partial` distinguishes useful best-effort progress from a refresh that
/// reconciled every desired node, without introducing an arbitrary string
/// label into metrics backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterTopologyRefreshOutcome {
    /// Every desired node service was reconciled successfully.
    Success,
    /// The topology was updated, but one or more desired node services could
    /// not be built during this attempt.
    Partial,
    /// Topology discovery or reconciliation failed.
    Error,
}

/// A bounded client-side cache event suitable for metrics labels.
///
/// Cache keys and command arguments are intentionally absent: emitting them
/// would create unbounded-cardinality metric series and could expose user
/// data. Applications that need the current aggregate totals can also read a
/// [`crate::CacheStatistics`] snapshot from a cached client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheEvent {
    /// A response was served from the local cache.
    Hit,
    /// A cacheable command was not present (or had expired) locally.
    Miss,
    /// Redis or a local write invalidated one or more keys.
    Invalidation,
    /// A cached entry was removed by invalidation, expiry, or capacity bounds.
    Eviction,
}

impl CacheEvent {
    #[cfg(feature = "metrics")]
    fn as_label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Invalidation => "invalidation",
            Self::Eviction => "eviction",
        }
    }
}

impl ClusterTopologyRefreshOutcome {
    #[cfg(feature = "metrics")]
    fn as_label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Partial => "partial",
            Self::Error => "error",
        }
    }
}

/// Receives metric events for each Redis command. Users implement this
/// trait to integrate with their metrics framework (prometheus, metrics
/// crate, OpenTelemetry, etc.).
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Called after each command completes.
    ///
    /// - `command`: the Redis command name (e.g. `"GET"`, `"SET"`), or
    ///   `"UNKNOWN"` if it could not be extracted from the frame.
    /// - `duration`: wall-clock time from call to completion.
    /// - `error`: `None` on success; `Some(kind)` on failure. The kind
    ///   enables labeled counters such as
    ///   `redis_command_errors_total{command="GET",kind="connection"}`.
    ///
    /// Note: `Frame::Error` responses (Redis-level errors such as
    /// `WRONGTYPE`) are classified as failures, not successes.
    fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>);

    /// Called after a command completes with an optional cluster-node label.
    ///
    /// Cluster clients pass the node that ultimately handled the command when
    /// node labeling is enabled. The label is optional because node addresses
    /// create one metric series per cluster member and applications may prefer
    /// the lower-cardinality aggregate.
    ///
    /// The default implementation discards `node` and delegates to
    /// [`command_completed`](Self::command_completed), so existing recorder
    /// implementations automatically receive cluster command metrics without
    /// an API break.
    fn command_completed_on_node(
        &self,
        command: &str,
        duration: Duration,
        error: Option<ErrorKind>,
        node: Option<&str>,
    ) {
        let _ = node;
        self.command_completed(command, duration, error);
    }

    /// Called when a Redis Cluster command receives a redirect response.
    ///
    /// [`ClusterRedirectKind`] bounds the possible metric label values. Node,
    /// slot, and retry-attempt details are intentionally left to tracing so
    /// this counter cannot grow with cluster size or workload keys.
    ///
    /// The default implementation is a no-op.
    fn cluster_redirected(&self, kind: ClusterRedirectKind) {
        let _ = kind;
    }

    /// Called after a Redis Cluster topology refresh attempt completes.
    ///
    /// `duration` covers the complete refresh attempt and `outcome` supplies a
    /// bounded result suitable for counters and histograms.
    ///
    /// The default implementation is a no-op.
    fn cluster_topology_refresh_completed(
        &self,
        duration: Duration,
        outcome: ClusterTopologyRefreshOutcome,
    ) {
        let _ = (duration, outcome);
    }

    /// Called after each auto-pipeline flush.
    ///
    /// `batch_size` is the number of frames sent in that flush. A histogram
    /// of this value reveals whether pipelining is effective (`batch_size >
    /// 1`) or whether individual callers flush immediately (`batch_size ==
    /// 1`).
    ///
    /// Default implementation is a no-op so existing implementors are not
    /// affected.
    fn pipeline_flushed(&self, batch_size: usize) {
        let _ = batch_size;
    }

    /// Called when the client-side cache records an aggregate event.
    ///
    /// `count` is normally one, but may be greater when a single invalidation
    /// removes several cached command variants. The event kind is deliberately
    /// bounded and carries no Redis keys or command arguments.
    ///
    /// The default implementation is a no-op.
    fn cache_event(&self, event: CacheEvent, count: u64) {
        let _ = (event, count);
    }

    /// Called when a caller finishes waiting to acquire a pool connection.
    ///
    /// `duration` includes both immediately successful acquisitions and time
    /// spent queued behind a busy connection. `timed_out` is `true` when the
    /// configured acquisition timeout elapsed before a connection became
    /// available.
    ///
    /// The default implementation is a no-op so custom recorders can opt in
    /// to pool metrics without a breaking trait change.
    fn pool_acquisition_completed(&self, pool_name: &str, duration: Duration, timed_out: bool) {
        let _ = (pool_name, duration, timed_out);
    }

    /// Called when a pooled connection fails its health check.
    ///
    /// The default implementation is a no-op.
    fn pool_health_check_failed(&self, pool_name: &str) {
        let _ = pool_name;
    }

    /// Called after a failed pooled connection is successfully replaced.
    ///
    /// The default implementation is a no-op.
    fn pool_connection_replaced(&self, pool_name: &str) {
        let _ = pool_name;
    }

    /// Called after one active pool health probe completes.
    ///
    /// Probe kinds and outcomes are bounded for safe metric labels. Lag is
    /// present only for replication-lag probes.
    fn pool_health_probe_completed(
        &self,
        pool_name: &str,
        kind: HealthProbeKind,
        duration: Duration,
        healthy: bool,
        replication_lag_bytes: Option<u64>,
    ) {
        let _ = (pool_name, kind, duration, healthy, replication_lag_bytes);
    }

    /// Called after an idle reaper removes one or more pool connections.
    fn pool_connections_reaped(&self, pool_name: &str, count: usize) {
        let _ = (pool_name, count);
    }
}

impl<R> MetricsRecorder for Arc<R>
where
    R: MetricsRecorder + ?Sized,
{
    fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
        (**self).command_completed(command, duration, error);
    }

    fn command_completed_on_node(
        &self,
        command: &str,
        duration: Duration,
        error: Option<ErrorKind>,
        node: Option<&str>,
    ) {
        (**self).command_completed_on_node(command, duration, error, node);
    }

    fn cluster_redirected(&self, kind: ClusterRedirectKind) {
        (**self).cluster_redirected(kind);
    }

    fn cluster_topology_refresh_completed(
        &self,
        duration: Duration,
        outcome: ClusterTopologyRefreshOutcome,
    ) {
        (**self).cluster_topology_refresh_completed(duration, outcome);
    }

    fn pipeline_flushed(&self, batch_size: usize) {
        (**self).pipeline_flushed(batch_size);
    }

    fn cache_event(&self, event: CacheEvent, count: u64) {
        (**self).cache_event(event, count);
    }

    fn pool_acquisition_completed(&self, pool_name: &str, duration: Duration, timed_out: bool) {
        (**self).pool_acquisition_completed(pool_name, duration, timed_out);
    }

    fn pool_health_check_failed(&self, pool_name: &str) {
        (**self).pool_health_check_failed(pool_name);
    }

    fn pool_connection_replaced(&self, pool_name: &str) {
        (**self).pool_connection_replaced(pool_name);
    }

    fn pool_health_probe_completed(
        &self,
        pool_name: &str,
        kind: HealthProbeKind,
        duration: Duration,
        healthy: bool,
        replication_lag_bytes: Option<u64>,
    ) {
        (**self).pool_health_probe_completed(
            pool_name,
            kind,
            duration,
            healthy,
            replication_lag_bytes,
        );
    }

    fn pool_connections_reaped(&self, pool_name: &str, count: usize) {
        (**self).pool_connections_reaped(pool_name, count);
    }
}

/// A [`MetricsRecorder`] backed by the [`metrics`](https://docs.rs/metrics)
/// facade.
///
/// This recorder lets applications choose their metrics exporter at startup
/// without coupling redis-tower to Prometheus, OpenTelemetry, or another
/// concrete backend. Enable the `metrics` crate feature, install a compatible
/// global recorder in the application, and pass this value to
/// [`MetricsLayer`],
/// [`crate::AutoPipelineConfig`], or [`crate::PoolConfig`].
///
/// Metric labels use command and pool names supplied by the application, plus
/// bounded success/error outcomes and [`ErrorKind`] variants. Applications
/// that build commands dynamically should normalize command names before
/// exposing them to an untrusted caller to avoid high-cardinality series.
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
#[derive(Debug, Default, Clone, Copy)]
pub struct MetricsFacadeRecorder;

#[cfg(feature = "metrics")]
impl MetricsFacadeRecorder {
    /// Create a metrics-facade recorder.
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "metrics")]
fn record_command_metrics(
    command: &str,
    duration: Duration,
    error: Option<ErrorKind>,
    node: Option<&str>,
) {
    let error = error.map(ErrorKind::as_label);
    let outcome = if error.is_some() { "error" } else { "success" };

    let mut duration_labels = vec![
        metrics::Label::new("db.system.name", "redis"),
        metrics::Label::new("db.operation.name", command.to_owned()),
    ];
    let mut counter_labels = vec![
        metrics::Label::new("db.operation.name", command.to_owned()),
        metrics::Label::new("outcome", outcome),
    ];

    if let Some(error) = error {
        duration_labels.push(metrics::Label::new("error.type", error));
        counter_labels.push(metrics::Label::new("error.type", error));
    }
    if let Some(node) = node {
        let node = node.to_owned();
        duration_labels.push(metrics::Label::new(
            "redis_tower.cluster.node",
            node.clone(),
        ));
        counter_labels.push(metrics::Label::new("redis_tower.cluster.node", node));
    }

    metrics::histogram!(
        description: "Duration of Redis client operations",
        unit: metrics::Unit::Seconds,
        "db.client.operation.duration",
        duration_labels,
    )
    .record(duration.as_secs_f64());
    metrics::counter!(
        description: "Redis commands completed",
        unit: metrics::Unit::Count,
        "redis_tower.commands",
        counter_labels,
    )
    .increment(1);
}

#[cfg(feature = "metrics")]
impl MetricsRecorder for MetricsFacadeRecorder {
    fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
        record_command_metrics(command, duration, error, None);
    }

    fn command_completed_on_node(
        &self,
        command: &str,
        duration: Duration,
        error: Option<ErrorKind>,
        node: Option<&str>,
    ) {
        record_command_metrics(command, duration, error, node);
    }

    fn cluster_redirected(&self, kind: ClusterRedirectKind) {
        metrics::counter!(
            description: "Redis Cluster redirect responses",
            unit: metrics::Unit::Count,
            "redis_tower.cluster.redirects",
            "kind" => kind.as_label(),
        )
        .increment(1);
    }

    fn cluster_topology_refresh_completed(
        &self,
        duration: Duration,
        outcome: ClusterTopologyRefreshOutcome,
    ) {
        let outcome = outcome.as_label();
        metrics::histogram!(
            description: "Duration of Redis Cluster topology refresh attempts",
            unit: metrics::Unit::Seconds,
            "redis_tower.cluster.topology_refresh.duration",
            "outcome" => outcome,
        )
        .record(duration.as_secs_f64());
        metrics::counter!(
            description: "Redis Cluster topology refresh attempts",
            unit: metrics::Unit::Count,
            "redis_tower.cluster.topology_refreshes",
            "outcome" => outcome,
        )
        .increment(1);
    }

    fn pipeline_flushed(&self, batch_size: usize) {
        metrics::histogram!(
            description: "Redis commands sent in each automatic pipeline flush",
            unit: metrics::Unit::Count,
            "redis_tower.pipeline.batch_size",
        )
        .record(batch_size as f64);
    }

    fn cache_event(&self, event: CacheEvent, count: u64) {
        metrics::counter!(
            description: "Redis client-side cache events",
            unit: metrics::Unit::Count,
            "redis_tower.cache.events",
            "event" => event.as_label(),
        )
        .increment(count);
    }

    fn pool_acquisition_completed(&self, pool_name: &str, duration: Duration, timed_out: bool) {
        metrics::histogram!(
            description: "Time spent waiting to acquire a Redis pool connection",
            unit: metrics::Unit::Seconds,
            "db.client.connection.wait_time",
            "db.client.connection.pool.name" => pool_name.to_owned(),
        )
        .record(duration.as_secs_f64());

        if timed_out {
            metrics::counter!(
                description: "Redis pool connection acquisition timeouts",
                unit: metrics::Unit::Count,
                "db.client.connection.timeouts",
                "db.client.connection.pool.name" => pool_name.to_owned(),
            )
            .increment(1);
        }
    }

    fn pool_health_check_failed(&self, pool_name: &str) {
        metrics::counter!(
            description: "Redis pool connection health-check failures",
            unit: metrics::Unit::Count,
            "redis_tower.pool.health_check_failures",
            "db.client.connection.pool.name" => pool_name.to_owned(),
        )
        .increment(1);
    }

    fn pool_connection_replaced(&self, pool_name: &str) {
        metrics::counter!(
            description: "Redis pool connections replaced after failed health checks",
            unit: metrics::Unit::Count,
            "redis_tower.pool.connection_replacements",
            "db.client.connection.pool.name" => pool_name.to_owned(),
        )
        .increment(1);
    }

    fn pool_health_probe_completed(
        &self,
        pool_name: &str,
        kind: HealthProbeKind,
        duration: Duration,
        healthy: bool,
        replication_lag_bytes: Option<u64>,
    ) {
        let outcome = if healthy { "healthy" } else { "unhealthy" };
        metrics::histogram!(
            description: "Duration of active Redis pool health probes",
            unit: metrics::Unit::Seconds,
            "redis_tower.pool.health_probe.duration",
            "db.client.connection.pool.name" => pool_name.to_owned(),
            "probe" => kind.as_str(),
            "outcome" => outcome,
        )
        .record(duration.as_secs_f64());
        metrics::counter!(
            description: "Active Redis pool health probe outcomes",
            unit: metrics::Unit::Count,
            "redis_tower.pool.health_probes",
            "db.client.connection.pool.name" => pool_name.to_owned(),
            "probe" => kind.as_str(),
            "outcome" => outcome,
        )
        .increment(1);
        metrics::gauge!(
            description: "Latest Redis replication lag observed by a pool probe",
            unit: metrics::Unit::Bytes,
            "redis_tower.pool.replication_lag",
            "db.client.connection.pool.name" => pool_name.to_owned(),
        )
        .set(replication_lag_bytes.unwrap_or(0) as f64);
        metrics::gauge!(
            description: "Whether the Redis pool replication-lag gauge has a current observation",
            unit: metrics::Unit::Count,
            "redis_tower.pool.replication_lag_observed",
            "db.client.connection.pool.name" => pool_name.to_owned(),
        )
        .set(if replication_lag_bytes.is_some() {
            1.0
        } else {
            0.0
        });
    }

    fn pool_connections_reaped(&self, pool_name: &str, count: usize) {
        metrics::counter!(
            description: "Redis pool connections removed after becoming idle",
            unit: metrics::Unit::Count,
            "redis_tower.pool.connections_reaped",
            "db.client.connection.pool.name" => pool_name.to_owned(),
        )
        .increment(count as u64);
    }
}

/// A cancellation handle for a background metrics snapshot exporter.
///
/// Dropping the handle cancels the exporter task immediately. Call
/// [`shutdown`](Self::shutdown) to emit one final snapshot, cancel the task,
/// and wait for it to finish.
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
#[must_use = "dropping the handle immediately stops metrics export"]
pub struct MetricsExporterHandle {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "metrics")]
impl MetricsExporterHandle {
    fn spawn(interval: Duration, mut record: impl FnMut() + Send + 'static) -> Self {
        assert!(
            !interval.is_zero(),
            "stats export interval must be non-zero"
        );

        let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        record();
                        break;
                    },
                    _ = ticker.tick() => record(),
                }
            }
        });

        Self {
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    fn cancel(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    /// Emit a final snapshot, cancel the exporter, and wait for it to finish.
    pub async fn shutdown(mut self) {
        self.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

#[cfg(feature = "metrics")]
impl Drop for MetricsExporterHandle {
    fn drop(&mut self) {
        self.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(feature = "metrics")]
fn pool_gauge_values(stats: &PoolStats) -> (usize, usize) {
    let used_connections = stats.size.saturating_sub(stats.idle_count);
    let pending_requests = stats.total_inflight.saturating_sub(used_connections);
    (used_connections, pending_requests)
}

#[cfg(feature = "metrics")]
fn record_pool_stats(pool_name: &str, stats: &PoolStats) {
    let (used_connections, pending_requests) = pool_gauge_values(stats);
    let pool_name = pool_name.to_owned();

    metrics::gauge!(
        description: "Redis pool connections by state",
        unit: metrics::Unit::Count,
        "db.client.connection.count",
        "db.client.connection.pool.name" => pool_name.clone(),
        "db.client.connection.state" => "idle",
    )
    .set(stats.idle_count as f64);
    metrics::gauge!(
        description: "Redis pool connections by state",
        unit: metrics::Unit::Count,
        "db.client.connection.count",
        "db.client.connection.pool.name" => pool_name.clone(),
        "db.client.connection.state" => "used",
    )
    .set(used_connections as f64);
    metrics::gauge!(
        description: "Maximum Redis pool connection count",
        unit: metrics::Unit::Count,
        "db.client.connection.max",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(stats.max_size as f64);
    metrics::gauge!(
        description: "Minimum Redis pool connection count",
        unit: metrics::Unit::Count,
        "db.client.connection.min",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(stats.min_size as f64);
    metrics::gauge!(
        description: "Redis requests waiting for a pool connection",
        unit: metrics::Unit::Count,
        "db.client.connection.pending_requests",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(pending_requests as f64);
    metrics::gauge!(
        description: "Redis commands active or waiting in the pool",
        unit: metrics::Unit::Count,
        "redis_tower.pool.inflight_commands",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(stats.total_inflight as f64);
    metrics::gauge!(
        description: "Highest in-flight command count on one Redis pool connection",
        unit: metrics::Unit::Count,
        "redis_tower.pool.max_inflight_per_connection",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(stats.max_inflight as f64);
    metrics::gauge!(
        description: "Redis pool connections by active-probe health state",
        unit: metrics::Unit::Count,
        "redis_tower.pool.health_count",
        "db.client.connection.pool.name" => pool_name.clone(),
        "state" => "healthy",
    )
    .set(stats.healthy_count as f64);
    metrics::gauge!(
        description: "Redis pool connections by active-probe health state",
        unit: metrics::Unit::Count,
        "redis_tower.pool.health_count",
        "db.client.connection.pool.name" => pool_name.clone(),
        "state" => "unhealthy",
    )
    .set(stats.unhealthy_count as f64);
    metrics::gauge!(
        description: "Redis pool connections by active-probe health state",
        unit: metrics::Unit::Count,
        "redis_tower.pool.health_count",
        "db.client.connection.pool.name" => pool_name.clone(),
        "state" => "unknown",
    )
    .set(stats.unknown_health_count as f64);
    metrics::gauge!(
        description: "Cumulative Redis pool connections removed by idle reaping",
        unit: metrics::Unit::Count,
        "redis_tower.pool.connections_reaped_total",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(stats.reaped_connections as f64);
    metrics::gauge!(
        description: "Maximum latest Redis replication lag across pool slots",
        unit: metrics::Unit::Bytes,
        "redis_tower.pool.max_replication_lag",
        "db.client.connection.pool.name" => pool_name.clone(),
    )
    .set(stats.max_replication_lag_bytes.unwrap_or(0) as f64);
    metrics::gauge!(
        description: "Whether the maximum Redis pool replication-lag gauge has a current observation",
        unit: metrics::Unit::Count,
        "redis_tower.pool.max_replication_lag_observed",
        "db.client.connection.pool.name" => pool_name,
    )
    .set(if stats.max_replication_lag_bytes.is_some() {
        1.0
    } else {
        0.0
    });
}

#[cfg(feature = "metrics")]
fn record_queue_depth(pipeline_name: &str, queue_depth: usize) {
    metrics::gauge!(
        description: "Redis auto-pipeline requests waiting in the internal queue",
        unit: metrics::Unit::Count,
        "redis_tower.pipeline.queue_depth",
        "redis_tower.pipeline.name" => pipeline_name.to_owned(),
    )
    .set(queue_depth as f64);
}

/// Spawn a task that periodically exports a connection pool's utilization.
///
/// The first snapshot is emitted immediately. Subsequent snapshots are
/// emitted every `interval`. The exporter uses the pool's configured name as
/// the `db.client.connection.pool.name` label and records idle/used connection
/// counts, configured capacity, pending requests, and in-flight work through
/// the global `metrics` recorder.
///
/// # Panics
///
/// Panics if `interval` is zero or if called outside a Tokio runtime.
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub fn spawn_pool_stats_exporter<S>(
    pool: ConnectionPool<S>,
    interval: Duration,
) -> MetricsExporterHandle
where
    S: Send + 'static,
{
    let pool_name = pool.name().to_owned();
    MetricsExporterHandle::spawn(interval, move || {
        record_pool_stats(&pool_name, &pool.stats())
    })
}

/// Spawn a task that periodically exports an auto-pipeline's queue depth.
///
/// The first snapshot is emitted immediately. Subsequent snapshots are
/// emitted every `interval` as `redis_tower.pipeline.queue_depth`, labeled by
/// the supplied stable `pipeline_name`. The exporter keeps a lightweight clone
/// of the client alive until it is dropped or shut down.
///
/// # Panics
///
/// Panics if `interval` is zero or if called outside a Tokio runtime.
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub fn spawn_queue_depth_exporter(
    client: MultiplexedClient,
    pipeline_name: impl Into<String>,
    interval: Duration,
) -> MetricsExporterHandle {
    let pipeline_name = pipeline_name.into();
    MetricsExporterHandle::spawn(interval, move || {
        record_queue_depth(&pipeline_name, client.queue_depth())
    })
}

/// Tower `Layer` that produces [`MetricsService`] wrappers.
///
/// Wraps each inner service with a [`MetricsService`] that records
/// per-command latency and error category via the provided
/// [`MetricsRecorder`].
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use tower::ServiceBuilder;
/// use redis_tower::FrameService;
/// use redis_tower::metrics_layer::MetricsLayer;
/// #
/// # use std::time::Duration;
/// # use redis_tower::metrics_layer::{ErrorKind, MetricsRecorder};
/// # struct MyRecorder;
/// # impl MetricsRecorder for MyRecorder {
/// #     fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
/// #         let _ = (command, duration, error);
/// #     }
/// # }
///
/// let frame_service = FrameService::connect("127.0.0.1:6379").await?;
/// let layer = MetricsLayer::new(MyRecorder);
/// let svc = ServiceBuilder::new()
///     .layer(layer)
///     .service(frame_service);
/// # let _ = svc;
/// # Ok(())
/// # }
/// ```
pub struct MetricsLayer<R> {
    recorder: Arc<R>,
}

impl<R: MetricsRecorder> MetricsLayer<R> {
    /// Create a new metrics layer with the given recorder.
    pub fn new(recorder: R) -> Self {
        Self {
            recorder: Arc::new(recorder),
        }
    }
}

impl<R: MetricsRecorder, S> tower_layer::Layer<S> for MetricsLayer<R> {
    type Service = MetricsService<S, R>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            recorder: Arc::clone(&self.recorder),
        }
    }
}

/// Tower `Service` that records per-command metrics via a [`MetricsRecorder`].
///
/// Created by [`MetricsLayer`] or directly via [`MetricsService::new`].
/// Extracts the command name from each request frame and reports the
/// command name, wall-clock duration, and error category after each call
/// completes.
pub struct MetricsService<S, R> {
    inner: S,
    recorder: Arc<R>,
}

impl<S, R> Clone for MetricsService<S, R>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            recorder: Arc::clone(&self.recorder),
        }
    }
}

impl<S, R> MetricsService<S, R> {
    /// Create a new metrics service wrapping an inner Frame service.
    pub fn new(inner: S, recorder: Arc<R>) -> Self {
        Self { inner, recorder }
    }
}

/// Extract the command name from a Redis command frame.
///
/// Expects `Frame::Array(Some(vec))` where the first element is
/// `Frame::BulkString(Some(bytes))`.
fn extract_command_name(frame: &Frame) -> Option<&str> {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return None,
    };
    match &items[0] {
        Frame::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
        _ => None,
    }
}

impl<S, R> Service<Frame> for MetricsService<S, R>
where
    S: Service<Frame, Response = Frame, Error = RedisError>,
    S::Future: Send + 'static,
    R: MetricsRecorder,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let command_name = extract_command_name(&request)
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase();
        let start = Instant::now();
        let recorder = Arc::clone(&self.recorder);
        let future = self.inner.call(request);

        Box::pin(async move {
            let result = future.await;
            let elapsed = start.elapsed();
            let error_kind = match &result {
                Ok(Frame::Error(bytes)) => Some(ErrorKind::from_frame_error(bytes)),
                Ok(_) => None,
                Err(e) => Some(ErrorKind::from_error(e)),
            };
            recorder.command_completed(&command_name, elapsed, error_kind);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(feature = "metrics")]
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CapturedMetric {
        name: String,
        labels: Vec<(String, String)>,
    }

    #[cfg(feature = "metrics")]
    impl CapturedMetric {
        fn from_key(key: &metrics::Key) -> Self {
            Self {
                name: key.name().to_owned(),
                labels: key
                    .labels()
                    .map(|label| (label.key().to_owned(), label.value().to_owned()))
                    .collect(),
            }
        }

        fn label(&self, name: &str) -> Option<&str> {
            self.labels
                .iter()
                .find_map(|(key, value)| (key == name).then_some(value.as_str()))
        }
    }

    #[cfg(feature = "metrics")]
    struct CapturedGauge {
        metric: CapturedMetric,
        value: Arc<Mutex<f64>>,
    }

    #[cfg(feature = "metrics")]
    impl std::ops::Deref for CapturedGauge {
        type Target = CapturedMetric;

        fn deref(&self) -> &Self::Target {
            &self.metric
        }
    }

    #[cfg(feature = "metrics")]
    impl CapturedGauge {
        fn value(&self) -> f64 {
            *self.value.lock().unwrap()
        }
    }

    #[cfg(feature = "metrics")]
    struct CapturingGauge {
        value: Arc<Mutex<f64>>,
    }

    #[cfg(feature = "metrics")]
    impl metrics::GaugeFn for CapturingGauge {
        fn increment(&self, value: f64) {
            *self.value.lock().unwrap() += value;
        }

        fn decrement(&self, value: f64) {
            *self.value.lock().unwrap() -= value;
        }

        fn set(&self, value: f64) {
            *self.value.lock().unwrap() = value;
        }
    }

    #[cfg(feature = "metrics")]
    #[derive(Default)]
    struct CapturingFacadeRecorder {
        counters: Mutex<Vec<CapturedMetric>>,
        gauges: Mutex<Vec<CapturedGauge>>,
        histograms: Mutex<Vec<CapturedMetric>>,
    }

    #[cfg(feature = "metrics")]
    impl metrics::Recorder for CapturingFacadeRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            self.counters
                .lock()
                .unwrap()
                .push(CapturedMetric::from_key(key));
            metrics::Counter::noop()
        }

        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            let value = Arc::new(Mutex::new(0.0));
            self.gauges.lock().unwrap().push(CapturedGauge {
                metric: CapturedMetric::from_key(key),
                value: Arc::clone(&value),
            });
            metrics::Gauge::from_arc(Arc::new(CapturingGauge { value }))
        }

        fn register_histogram(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            self.histograms
                .lock()
                .unwrap()
                .push(CapturedMetric::from_key(key));
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn extract_name_from_get() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
        ]));
        assert_eq!(extract_command_name(&frame), Some("GET"));
    }

    #[test]
    fn extract_name_from_set() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        assert_eq!(extract_command_name(&frame), Some("SET"));
    }

    #[test]
    fn extract_name_empty_array() {
        let frame = Frame::Array(Some(vec![]));
        assert_eq!(extract_command_name(&frame), None);
    }

    #[test]
    fn extract_name_null_frame() {
        assert_eq!(extract_command_name(&Frame::Null), None);
    }

    #[test]
    fn extract_name_none_array() {
        let frame = Frame::Array(None);
        assert_eq!(extract_command_name(&frame), None);
    }

    #[test]
    fn error_kind_from_error_connection() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::ConnectionClosed),
            ErrorKind::Connection
        );
    }

    #[test]
    fn error_kind_from_error_timeout() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::PoolAcquisitionTimeout {
                waited: std::time::Duration::from_millis(50),
                pool_size: 4,
            }),
            ErrorKind::Timeout
        );
    }

    #[test]
    fn error_kind_from_error_wrongtype() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into()
            )),
            ErrorKind::WrongType
        );
    }

    #[test]
    fn error_kind_from_error_circuit_open() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::CircuitOpen),
            ErrorKind::CircuitOpen
        );
    }

    #[test]
    fn error_kind_from_error_queue_full() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::QueueFull),
            ErrorKind::QueueFull
        );
    }

    #[test]
    fn error_kind_from_error_auth_noauth() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis("NOAUTH Authentication required".into())),
            ErrorKind::Auth
        );
    }

    #[test]
    fn error_kind_from_error_auth_wrongpass() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis(
                "WRONGPASS invalid username-password pair".into()
            )),
            ErrorKind::Auth
        );
    }

    #[test]
    fn error_kind_from_error_other() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis("ERR unknown command".into())),
            ErrorKind::Other
        );
    }

    #[test]
    fn error_kind_from_frame_error_wrongtype() {
        assert_eq!(
            ErrorKind::from_frame_error(&Bytes::from("WRONGTYPE value is not a set")),
            ErrorKind::WrongType
        );
    }

    #[test]
    fn error_kind_from_frame_error_other() {
        assert_eq!(
            ErrorKind::from_frame_error(&Bytes::from("ERR syntax error")),
            ErrorKind::Other
        );
    }

    struct TestRecorder {
        error_kind: Mutex<Option<Option<ErrorKind>>>,
        duration_ns: AtomicU64,
    }

    impl TestRecorder {
        fn new() -> Self {
            Self {
                error_kind: Mutex::new(None),
                duration_ns: AtomicU64::new(0),
            }
        }

        fn was_called(&self) -> bool {
            self.error_kind.lock().unwrap().is_some()
        }

        fn recorded_error(&self) -> Option<ErrorKind> {
            self.error_kind.lock().unwrap().flatten()
        }
    }

    impl MetricsRecorder for TestRecorder {
        fn command_completed(&self, _command: &str, duration: Duration, error: Option<ErrorKind>) {
            *self.error_kind.lock().unwrap() = Some(error);
            self.duration_ns
                .store(duration.as_nanos() as u64, Ordering::SeqCst);
        }
    }

    #[test]
    fn custom_recorders_inherit_noop_pool_hooks() {
        let recorder = TestRecorder::new();
        recorder.pool_acquisition_completed("default", Duration::from_millis(2), false);
        recorder.pool_health_check_failed("default");
        recorder.pool_connection_replaced("default");
        assert!(!recorder.was_called());
    }

    #[test]
    fn existing_recorders_receive_node_completions_through_the_original_hook() {
        let recorder = TestRecorder::new();
        recorder.command_completed_on_node(
            "GET",
            Duration::from_millis(2),
            Some(ErrorKind::Connection),
            Some("127.0.0.1:7000"),
        );
        recorder.cluster_redirected(ClusterRedirectKind::Moved);
        recorder.cluster_topology_refresh_completed(
            Duration::from_millis(3),
            ClusterTopologyRefreshOutcome::Success,
        );

        assert!(recorder.was_called());
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::Connection));
    }

    #[derive(Default)]
    struct ClusterHookRecorder {
        node: Mutex<Option<String>>,
        redirect: Mutex<Option<ClusterRedirectKind>>,
        refresh: Mutex<Option<(Duration, ClusterTopologyRefreshOutcome)>>,
    }

    impl MetricsRecorder for ClusterHookRecorder {
        fn command_completed(
            &self,
            _command: &str,
            _duration: Duration,
            _error: Option<ErrorKind>,
        ) {
        }

        fn command_completed_on_node(
            &self,
            _command: &str,
            _duration: Duration,
            _error: Option<ErrorKind>,
            node: Option<&str>,
        ) {
            *self.node.lock().unwrap() = node.map(str::to_owned);
        }

        fn cluster_redirected(&self, kind: ClusterRedirectKind) {
            *self.redirect.lock().unwrap() = Some(kind);
        }

        fn cluster_topology_refresh_completed(
            &self,
            duration: Duration,
            outcome: ClusterTopologyRefreshOutcome,
        ) {
            *self.refresh.lock().unwrap() = Some((duration, outcome));
        }
    }

    #[test]
    fn arc_delegates_cluster_hooks_to_its_metrics_recorder() {
        let recorder = Arc::new(ClusterHookRecorder::default());

        MetricsRecorder::command_completed_on_node(
            &recorder,
            "SET",
            Duration::from_millis(1),
            None,
            Some("127.0.0.1:7001"),
        );
        MetricsRecorder::cluster_redirected(&recorder, ClusterRedirectKind::Ask);
        MetricsRecorder::cluster_topology_refresh_completed(
            &recorder,
            Duration::from_millis(4),
            ClusterTopologyRefreshOutcome::Partial,
        );

        assert_eq!(
            recorder.node.lock().unwrap().as_deref(),
            Some("127.0.0.1:7001")
        );
        assert_eq!(
            *recorder.redirect.lock().unwrap(),
            Some(ClusterRedirectKind::Ask)
        );
        assert_eq!(
            *recorder.refresh.lock().unwrap(),
            Some((
                Duration::from_millis(4),
                ClusterTopologyRefreshOutcome::Partial
            ))
        );
    }

    #[test]
    fn arc_delegates_to_its_metrics_recorder() {
        let recorder = Arc::new(TestRecorder::new());
        MetricsRecorder::command_completed(
            &recorder,
            "GET",
            Duration::from_millis(2),
            Some(ErrorKind::Connection),
        );
        assert!(recorder.was_called());
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::Connection));
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn facade_recorder_uses_semantic_convention_labels() {
        let capture = CapturingFacadeRecorder::default();
        let recorder = MetricsFacadeRecorder::new();
        metrics::with_local_recorder(&capture, || {
            recorder.command_completed("GET", Duration::from_millis(2), None);
            recorder.command_completed(
                "SET",
                Duration::from_millis(3),
                Some(ErrorKind::Connection),
            );
            recorder.command_completed("CUSTOM", Duration::from_millis(1), Some(ErrorKind::Other));
        });

        let histograms = capture.histograms.lock().unwrap();
        let success = &histograms[0];
        assert_eq!(success.name, "db.client.operation.duration");
        assert_eq!(success.label("db.system.name"), Some("redis"));
        assert_eq!(success.label("db.operation.name"), Some("GET"));
        assert_eq!(success.label("error.type"), None);

        let failure = &histograms[1];
        assert_eq!(failure.label("db.operation.name"), Some("SET"));
        assert_eq!(failure.label("error.type"), Some("connection"));

        let fallback = &histograms[2];
        assert_eq!(fallback.label("db.operation.name"), Some("CUSTOM"));
        assert_eq!(fallback.label("error.type"), Some("_OTHER"));

        let counters = capture.counters.lock().unwrap();
        assert_eq!(counters[0].label("outcome"), Some("success"));
        assert_eq!(counters[1].label("outcome"), Some("error"));
        assert_eq!(counters[1].label("error.type"), Some("connection"));
        assert_eq!(counters[2].label("error.type"), Some("_OTHER"));
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn facade_recorder_adds_the_optional_cluster_node_label() {
        let capture = CapturingFacadeRecorder::default();
        let recorder = MetricsFacadeRecorder::new();
        metrics::with_local_recorder(&capture, || {
            recorder.command_completed_on_node(
                "GET",
                Duration::from_millis(2),
                None,
                Some("127.0.0.1:7000"),
            );
            recorder.command_completed_on_node(
                "SET",
                Duration::from_millis(3),
                Some(ErrorKind::Connection),
                None,
            );
        });

        let histograms = capture.histograms.lock().unwrap();
        assert_eq!(
            histograms[0].label("redis_tower.cluster.node"),
            Some("127.0.0.1:7000")
        );
        assert_eq!(histograms[1].label("redis_tower.cluster.node"), None);

        let counters = capture.counters.lock().unwrap();
        assert_eq!(
            counters[0].label("redis_tower.cluster.node"),
            Some("127.0.0.1:7000")
        );
        assert_eq!(counters[1].label("redis_tower.cluster.node"), None);
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn facade_recorder_emits_bounded_cluster_event_labels() {
        let capture = CapturingFacadeRecorder::default();
        let recorder = MetricsFacadeRecorder::new();
        metrics::with_local_recorder(&capture, || {
            recorder.cluster_redirected(ClusterRedirectKind::Moved);
            recorder.cluster_redirected(ClusterRedirectKind::Ask);
            recorder.cluster_topology_refresh_completed(
                Duration::from_millis(3),
                ClusterTopologyRefreshOutcome::Success,
            );
            recorder.cluster_topology_refresh_completed(
                Duration::from_millis(4),
                ClusterTopologyRefreshOutcome::Partial,
            );
            recorder.cluster_topology_refresh_completed(
                Duration::from_millis(5),
                ClusterTopologyRefreshOutcome::Error,
            );
        });

        let counters = capture.counters.lock().unwrap();
        let redirect_kinds: Vec<_> = counters
            .iter()
            .filter(|metric| metric.name == "redis_tower.cluster.redirects")
            .map(|metric| metric.label("kind").unwrap())
            .collect();
        assert_eq!(redirect_kinds, ["moved", "ask"]);

        let refresh_outcomes: Vec<_> = counters
            .iter()
            .filter(|metric| metric.name == "redis_tower.cluster.topology_refreshes")
            .map(|metric| metric.label("outcome").unwrap())
            .collect();
        assert_eq!(refresh_outcomes, ["success", "partial", "error"]);

        let histograms = capture.histograms.lock().unwrap();
        let duration_outcomes: Vec<_> = histograms
            .iter()
            .filter(|metric| metric.name == "redis_tower.cluster.topology_refresh.duration")
            .map(|metric| metric.label("outcome").unwrap())
            .collect();
        assert_eq!(duration_outcomes, ["success", "partial", "error"]);
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn pending_requests_exclude_connections_doing_work() {
        let stats = PoolStats {
            size: 4,
            idle_count: 1,
            total_inflight: 7,
            max_inflight: 3,
            min_size: 2,
            max_size: 8,
            healthy_count: 2,
            unhealthy_count: 1,
            unknown_health_count: 1,
            max_replication_lag_bytes: Some(42),
            reaped_connections: 5,
        };
        assert_eq!(pool_gauge_values(&stats), (3, 4));
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn pool_stats_use_canonical_names_and_labels() {
        let capture = CapturingFacadeRecorder::default();
        let stats = PoolStats {
            size: 4,
            idle_count: 1,
            total_inflight: 7,
            max_inflight: 3,
            min_size: 2,
            max_size: 8,
            healthy_count: 2,
            unhealthy_count: 1,
            unknown_health_count: 1,
            max_replication_lag_bytes: Some(42),
            reaped_connections: 5,
        };
        metrics::with_local_recorder(&capture, || record_pool_stats("primary", &stats));

        let gauges = capture.gauges.lock().unwrap();
        let idle = gauges
            .iter()
            .find(|metric| {
                metric.name == "db.client.connection.count"
                    && metric.label("db.client.connection.state") == Some("idle")
            })
            .unwrap();
        assert_eq!(
            idle.label("db.client.connection.pool.name"),
            Some("primary")
        );
        assert_eq!(idle.value(), 1.0);
        let max = gauges
            .iter()
            .find(|metric| metric.name == "db.client.connection.max")
            .unwrap();
        assert_eq!(max.value(), 8.0);
        let pending = gauges
            .iter()
            .find(|metric| metric.name == "db.client.connection.pending_requests")
            .unwrap();
        assert_eq!(pending.value(), 4.0);
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn unknown_replication_lag_resets_value_and_freshness_gauges() {
        let capture = CapturingFacadeRecorder::default();
        let recorder = MetricsFacadeRecorder::new();
        metrics::with_local_recorder(&capture, || {
            recorder.pool_health_probe_completed(
                "primary",
                HealthProbeKind::ReplicationLag,
                Duration::from_millis(1),
                true,
                Some(42),
            );
            recorder.pool_health_probe_completed(
                "primary",
                HealthProbeKind::ReplicationLag,
                Duration::from_millis(1),
                false,
                None,
            );
        });

        let gauges = capture.gauges.lock().unwrap();
        let lag = gauges
            .iter()
            .rfind(|metric| metric.name == "redis_tower.pool.replication_lag")
            .unwrap();
        let observed = gauges
            .iter()
            .rfind(|metric| metric.name == "redis_tower.pool.replication_lag_observed")
            .unwrap();
        assert_eq!(lag.value(), 0.0);
        assert_eq!(observed.value(), 0.0);
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn queue_depth_uses_a_named_gauge() {
        let capture = CapturingFacadeRecorder::default();
        metrics::with_local_recorder(&capture, || record_queue_depth("commands", 7));

        let gauges = capture.gauges.lock().unwrap();
        let queue = gauges
            .iter()
            .find(|metric| metric.name == "redis_tower.pipeline.queue_depth")
            .unwrap();
        assert_eq!(queue.label("redis_tower.pipeline.name"), Some("commands"));
        assert_eq!(queue.value(), 7.0);
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn pool_stats_exporter_shuts_down_without_redis() {
        let pool =
            ConnectionPool::from_connections(vec![(), ()], crate::pool::PoolConfig::default())
                .unwrap();
        let exporter = spawn_pool_stats_exporter(pool, Duration::from_millis(1));
        tokio::task::yield_now().await;
        exporter.shutdown().await;
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn exporter_shutdown_emits_a_final_snapshot() {
        let snapshots = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task_snapshots = Arc::clone(&snapshots);
        let exporter = MetricsExporterHandle::spawn(Duration::from_secs(60), move || {
            task_snapshots.fetch_add(1, Ordering::SeqCst);
        });

        while snapshots.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let before_shutdown = snapshots.load(Ordering::SeqCst);

        exporter.shutdown().await;

        assert_eq!(snapshots.load(Ordering::SeqCst), before_shutdown + 1);
    }

    /// A mock service that always returns SimpleString "OK".
    #[derive(Clone)]
    struct OkService;

    impl Service<Frame> for OkService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            Box::pin(async { Ok(Frame::SimpleString("OK".into())) })
        }
    }

    /// A mock service that always returns a transport error.
    struct ErrService;

    impl Service<Frame> for ErrService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            Box::pin(async { Err(RedisError::ConnectionClosed) })
        }
    }

    /// A mock service that returns a Frame::Error (Redis-level error).
    struct FrameErrService {
        msg: &'static str,
    }

    impl Service<Frame> for FrameErrService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            let msg = self.msg;
            Box::pin(async move { Ok(Frame::Error(Bytes::from(msg))) })
        }
    }

    #[tokio::test]
    async fn records_success() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(OkService, Arc::clone(&recorder));

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_ok());
        assert!(recorder.was_called());
        assert_eq!(recorder.recorded_error(), None);
        assert!(recorder.duration_ns.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn cloned_services_share_a_non_clone_recorder() {
        let recorder = Arc::new(TestRecorder::new());
        let svc = MetricsService::new(OkService, Arc::clone(&recorder));
        let mut cloned = svc.clone();

        let frame = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("PING")))]));
        assert!(cloned.call(frame).await.is_ok());
        assert!(recorder.was_called());
    }

    #[tokio::test]
    async fn records_failure_connection_closed() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(ErrService, Arc::clone(&recorder));

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("val"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_err());
        assert!(recorder.was_called());
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::Connection));
    }

    #[tokio::test]
    async fn frame_error_is_classified_as_failure() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(
            FrameErrService {
                msg: "WRONGTYPE Operation against a key holding the wrong kind of value",
            },
            Arc::clone(&recorder),
        );

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SADD"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("member"))),
        ]));

        // The service returns Ok(Frame::Error(...)) -- a Redis-level error,
        // not a transport error. The Result is Ok.
        let result = svc.call(frame).await;
        assert!(result.is_ok());
        assert!(recorder.was_called());
        // But the recorder should classify it as a WrongType failure.
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::WrongType));
    }

    #[tokio::test]
    async fn frame_error_generic_classified_as_other() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(
            FrameErrService {
                msg: "ERR syntax error",
            },
            Arc::clone(&recorder),
        );

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_ok());
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::Other));
    }
}
