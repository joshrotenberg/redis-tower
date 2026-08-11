//! Tower-native connection pool.
//!
//! Pools N connections and dispatches commands via round-robin or random
//! strategy. Generic over any connection type that implements
//! [`RedisExecutor`], so it works uniformly with standalone, cluster,
//! and sentinel connections.
//!
//! # Why pool the client, not the node
//!
//! For cluster deployments, each pooled entry is a complete
//! `ClusterConnection` that manages its own node topology and redirect
//! handling internally. The pool dispatches across N independent cluster
//! clients. This avoids the common pitfall (seen in redis-rs + bb8) where
//! individual node connections are pooled separately from cluster routing.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::pool::ConnectionPool;
//! use redis_tower::RedisConnection;
//! use redis_tower::commands::Set;
//!
//! // Standalone pool
//! let pool = ConnectionPool::connect(4, || async {
//!     RedisConnection::connect("127.0.0.1:6379").await
//! }).await?;
//!
//! // Use from multiple tasks
//! let p = pool.clone();
//! tokio::spawn(async move {
//!     p.execute(Set::new("key", "val")).await.unwrap();
//! });
//! # let _ = pool;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use redis_tower_commands::{Info, Ping, Role};
use redis_tower_core::{Command, Frame, RedisError};
#[cfg(test)]
use tokio::sync::MappedMutexGuard;
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;

use crate::executor::RedisExecutor;
use crate::metrics_layer::MetricsRecorder;

/// A type-erased factory for creating pooled connections.
///
/// Implement this trait to give a [`ConnectionPool`] the ability to replace
/// dead connections after a failed health-check PING. When a PING fails and
/// a factory is present, the pool calls [`PoolFactory::create`] to obtain a
/// fresh connection and substitutes it into the dead slot before proceeding.
pub trait PoolFactory: Send + Sync + 'static {
    /// The connection type this factory creates.
    type Connection: RedisExecutor + Send + 'static;

    /// Create a new connection.
    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>>;
}

/// Health state last observed by an active pool prober.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolHealthState {
    /// The slot has not been probed since it was created or replaced.
    Unknown,
    /// The most recent probe succeeded and satisfied its policy.
    Healthy,
    /// The most recent probe failed or did not satisfy its policy.
    Unhealthy,
}

impl PoolHealthState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::Healthy => 1,
            Self::Unhealthy => 2,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Healthy,
            2 => Self::Unhealthy,
            _ => Self::Unknown,
        }
    }
}

/// Redis replication role used by [`RoleHealthProbe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisRole {
    /// A writable primary (Redis reports this as `master`).
    Primary,
    /// A read-only replica (Redis may report `slave` or `replica`).
    Replica,
    /// A Sentinel process.
    Sentinel,
}

/// Bounded probe kind used by metrics backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthProbeKind {
    /// `PING` liveness probe.
    Ping,
    /// `ROLE` role-verification probe.
    Role,
    /// `INFO replication` link and offset-lag probe.
    ReplicationLag,
    /// User-supplied probe.
    Custom,
}

impl HealthProbeKind {
    /// Stable low-cardinality label for metrics and logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ping => "ping",
            Self::Role => "role",
            Self::ReplicationLag => "replication_lag",
            Self::Custom => "custom",
        }
    }
}

/// Successful output from a [`HealthProbe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthProbeResult {
    /// Whether the connection satisfies the probe's health policy.
    pub healthy: bool,
    /// Replication lag in bytes when the probe observes it.
    pub replication_lag_bytes: Option<u64>,
}

impl HealthProbeResult {
    /// A healthy result without a replication-lag observation.
    pub const fn healthy() -> Self {
        Self {
            healthy: true,
            replication_lag_bytes: None,
        }
    }

    /// An unhealthy result without a replication-lag observation.
    pub const fn unhealthy() -> Self {
        Self {
            healthy: false,
            replication_lag_bytes: None,
        }
    }

    fn with_replication_lag(healthy: bool, replication_lag_bytes: u64) -> Self {
        Self {
            healthy,
            replication_lag_bytes: Some(replication_lag_bytes),
        }
    }
}

/// Asynchronous health check for one pooled connection.
///
/// Implementations should perform one bounded logical check and return a
/// structured observation. The prober applies its own timeout, records the
/// result, and updates [`PoolStats`].
pub trait HealthProbe<S>: Send + Sync + 'static {
    /// Bounded probe kind used by metrics.
    fn kind(&self) -> HealthProbeKind {
        HealthProbeKind::Custom
    }

    /// Check one connection.
    fn probe<'a>(
        &'a self,
        connection: &'a mut S,
    ) -> Pin<Box<dyn Future<Output = Result<HealthProbeResult, RedisError>> + Send + 'a>>;
}

/// Default active liveness probe, implemented with Redis `PING`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PingHealthProbe;

impl<S> HealthProbe<S> for PingHealthProbe
where
    S: RedisExecutor + Send + 'static,
{
    fn kind(&self) -> HealthProbeKind {
        HealthProbeKind::Ping
    }

    fn probe<'a>(
        &'a self,
        connection: &'a mut S,
    ) -> Pin<Box<dyn Future<Output = Result<HealthProbeResult, RedisError>> + Send + 'a>> {
        Box::pin(async move {
            connection.execute(Ping::new()).await?;
            Ok(HealthProbeResult::healthy())
        })
    }
}

/// Active probe that verifies the role returned by Redis `ROLE`.
#[derive(Debug, Clone, Copy)]
pub struct RoleHealthProbe {
    expected: RedisRole,
}

impl RoleHealthProbe {
    /// Require `expected` from every probed connection.
    pub const fn new(expected: RedisRole) -> Self {
        Self { expected }
    }
}

impl<S> HealthProbe<S> for RoleHealthProbe
where
    S: RedisExecutor + Send + 'static,
{
    fn kind(&self) -> HealthProbeKind {
        HealthProbeKind::Role
    }

    fn probe<'a>(
        &'a self,
        connection: &'a mut S,
    ) -> Pin<Box<dyn Future<Output = Result<HealthProbeResult, RedisError>> + Send + 'a>> {
        Box::pin(async move {
            let frame = connection.execute(Role::new()).await?;
            let observed = parse_role_frame(&frame)?;
            Ok(if observed == self.expected {
                HealthProbeResult::healthy()
            } else {
                HealthProbeResult::unhealthy()
            })
        })
    }
}

/// Active `INFO replication` probe with a maximum permitted offset lag.
///
/// Run this probe against a primary. Redis reports the primary's current
/// replication offset and each directly connected replica's offset in the
/// primary's `INFO replication` response, which lets the probe calculate the
/// largest byte lag without contacting a second server. A primary with no
/// connected replicas is unhealthy. Replica-local INFO does not contain the
/// upstream primary's current offset, so probing a replica is also unhealthy
/// rather than reporting a misleading zero-byte lag.
#[derive(Debug, Clone, Copy)]
pub struct ReplicationLagHealthProbe {
    max_lag_bytes: u64,
}

impl ReplicationLagHealthProbe {
    /// Mark replicas unhealthy when their replication offset trails by more
    /// than `max_lag_bytes`, or while their primary link is down/synchronizing.
    pub const fn new(max_lag_bytes: u64) -> Self {
        Self { max_lag_bytes }
    }
}

impl<S> HealthProbe<S> for ReplicationLagHealthProbe
where
    S: RedisExecutor + Send + 'static,
{
    fn kind(&self) -> HealthProbeKind {
        HealthProbeKind::ReplicationLag
    }

    fn probe<'a>(
        &'a self,
        connection: &'a mut S,
    ) -> Pin<Box<dyn Future<Output = Result<HealthProbeResult, RedisError>> + Send + 'a>> {
        Box::pin(async move {
            let info = connection
                .execute(Info::new().section("replication"))
                .await?;
            let observation = parse_replication_info(&info)?;
            let healthy = observation.link_up
                && !observation.sync_in_progress
                && observation.lag_bytes <= self.max_lag_bytes;
            Ok(HealthProbeResult::with_replication_lag(
                healthy,
                observation.lag_bytes,
            ))
        })
    }
}

/// Configuration for an explicitly spawned active health prober.
#[derive(Debug, Clone, Copy)]
pub struct HealthProberConfig {
    /// Delay between complete sweeps of the pool.
    pub interval: Duration,
    /// Maximum duration of one connection probe.
    pub timeout: Duration,
}

impl Default for HealthProberConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(2),
        }
    }
}

impl HealthProberConfig {
    /// Set the delay between complete pool sweeps.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the timeout applied to each connection probe.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// A snapshot of current [`ConnectionPool`] utilization.
///
/// Obtained via [`ConnectionPool::stats`]. All values are point-in-time
/// reads of the underlying atomic counters; concurrent commands may cause
/// values to change between calls.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total number of connections in the pool.
    pub size: usize,
    /// Number of connections with zero in-flight commands (idle).
    pub idle_count: usize,
    /// Total in-flight command count across all connections.
    pub total_inflight: usize,
    /// Highest in-flight count on a single connection.
    pub max_inflight: usize,
    /// Configured lower bound for idle reaping.
    pub min_size: usize,
    /// Configured upper bound for contention-driven growth.
    pub max_size: usize,
    /// Slots whose most recent active probe was healthy.
    pub healthy_count: usize,
    /// Slots whose most recent active probe was unhealthy.
    pub unhealthy_count: usize,
    /// Slots that have not been actively probed since creation/replacement.
    pub unknown_health_count: usize,
    /// Largest replication lag reported by the latest slot observations.
    pub max_replication_lag_bytes: Option<u64>,
    /// Cumulative number of idle connections removed by a reaper.
    pub reaped_connections: u64,
}

/// Dispatch strategy for distributing commands across pooled connections.
#[derive(Debug, Clone, Copy, Default)]
pub enum DispatchStrategy {
    /// Cycle through connections sequentially (default).
    #[default]
    RoundRobin,
    /// Pick a random connection for each command.
    Random,
    /// Pick the connection with the fewest in-flight commands.
    /// Best for workloads with variable command latency (e.g., mix of
    /// GET and SORT). Falls back to round-robin on ties.
    LeastConnections,
}

/// Configuration for a connection pool.
#[derive(Clone)]
pub struct PoolConfig {
    /// Stable name used to identify this pool in metrics.
    ///
    /// Applications with multiple pools should give each one a distinct name
    /// so their utilization and acquisition metrics remain distinguishable.
    /// Defaults to `"redis-tower"`.
    pub name: String,
    /// Legacy fixed size and fallback for omitted dynamic bounds.
    ///
    /// With neither `min_size` nor `max_size`, the pool remains fixed at this
    /// size. Prefer [`Self::bounds`] when opting into dynamic sizing.
    pub size: usize,
    /// Optional minimum number of live connections.
    ///
    /// When omitted, [`Self::size`] is used. Idle reaping never shrinks below
    /// this value. Set this below `max_size` to opt into dynamic sizing.
    pub min_size: Option<usize>,
    /// Optional maximum number of live connections.
    ///
    /// When omitted, [`Self::size`] is used. A factory-backed pool creates new
    /// connections up to this bound when every live slot is contended.
    pub max_size: Option<usize>,
    /// How long a completely idle connection may remain before an explicitly
    /// spawned idle reaper removes it.
    ///
    /// Setting this value does not spawn a task. Call
    /// [`ConnectionPool::spawn_idle_reaper`] and retain its owned handle.
    pub idle_timeout: Option<Duration>,
    /// How to select which connection handles each command.
    pub dispatch: DispatchStrategy,
    /// If set, connections idle longer than this duration are PINGed before use.
    ///
    /// This provides lazy health checking: when a connection has been idle
    /// beyond this interval, a PING is sent before dispatching the actual
    /// command. If the PING fails and the pool has a factory, the dead slot
    /// is replaced before the command is retried. If no factory is present,
    /// the error is returned to the caller.
    pub health_check_interval: Option<Duration>,
    /// Maximum time to wait for a connection slot to become available.
    ///
    /// When all connections in the pool are busy, new callers block waiting
    /// for a slot to free up. If the wait exceeds this duration,
    /// [`RedisError::PoolAcquisitionTimeout`] is returned, so pool exhaustion
    /// fails fast as a timeout instead of stalling the caller indefinitely.
    ///
    /// Defaults to [`PoolConfig::DEFAULT_ACQUISITION_TIMEOUT`] (5 seconds).
    /// Set it to `None` -- via [`PoolConfig::disable_acquisition_timeout`] --
    /// to wait forever, restoring the previous unbounded behavior.
    /// [`Duration::ZERO`] is distinct from `None`: it permits an immediately
    /// available slot but never waits for a busy one.
    /// A command wrapped in [`redis_tower_core::WithDeadline`] can impose an
    /// earlier limit; the earliest deadline wins.
    pub acquisition_timeout: Option<Duration>,
    /// Optional recorder for pool lifecycle metrics.
    ///
    /// When set, the pool reports connection acquisition latency and timeouts,
    /// lazy health-check failures, and successful connection replacements.
    pub metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl std::fmt::Debug for PoolConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConfig")
            .field("name", &self.name)
            .field("size", &self.size)
            .field("min_size", &self.min_size)
            .field("max_size", &self.max_size)
            .field("idle_timeout", &self.idle_timeout)
            .field("dispatch", &self.dispatch)
            .field("health_check_interval", &self.health_check_interval)
            .field("acquisition_timeout", &self.acquisition_timeout)
            .field(
                "metrics_recorder",
                &self.metrics_recorder.as_ref().map(|_| "<recorder>"),
            )
            .finish()
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            name: "redis-tower".to_owned(),
            size: 4,
            min_size: None,
            max_size: None,
            idle_timeout: None,
            dispatch: DispatchStrategy::RoundRobin,
            health_check_interval: None,
            acquisition_timeout: Some(Self::DEFAULT_ACQUISITION_TIMEOUT),
            metrics_recorder: None,
        }
    }
}

impl PoolConfig {
    /// Default upper bound on how long a caller waits for a connection slot.
    ///
    /// Used by [`PoolConfig::default`]. Five seconds is long enough to ride
    /// out brief contention but short enough that genuine pool exhaustion
    /// surfaces as a [`RedisError::PoolAcquisitionTimeout`] rather than an
    /// unbounded stall that masquerades as a hang.
    pub const DEFAULT_ACQUISITION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Set the stable name used to identify this pool in metrics.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the pool size.
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Set the minimum number of live connections maintained by the pool.
    pub fn min_size(mut self, min_size: usize) -> Self {
        self.min_size = Some(min_size);
        self
    }

    /// Set the maximum number of live connections permitted in the pool.
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    /// Set both dynamic sizing bounds.
    pub fn bounds(mut self, min_size: usize, max_size: usize) -> Self {
        self.min_size = Some(min_size);
        self.max_size = Some(max_size);
        self
    }

    /// Set the idle duration after which a reaper may remove a connection.
    ///
    /// This only configures policy. No background task exists until
    /// [`ConnectionPool::spawn_idle_reaper`] is called.
    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    /// Set the dispatch strategy.
    pub fn dispatch(mut self, strategy: DispatchStrategy) -> Self {
        self.dispatch = strategy;
        self
    }

    /// Set the health check interval.
    ///
    /// If set, connections idle longer than this are PINGed before use
    /// to verify they are still alive.
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = Some(interval);
        self
    }

    /// Set the maximum time to wait for a connection slot.
    ///
    /// If all connections are busy when a command is submitted and a slot
    /// does not free up within `timeout`, the call returns
    /// [`RedisError::PoolAcquisitionTimeout`]. Overrides the bounded
    /// [`PoolConfig::DEFAULT_ACQUISITION_TIMEOUT`]. Pass [`Duration::ZERO`] for
    /// fail-fast acquisition that never waits; use
    /// [`Self::disable_acquisition_timeout`] to wait without a bound.
    pub fn acquisition_timeout(mut self, timeout: Duration) -> Self {
        self.acquisition_timeout = Some(timeout);
        self
    }

    /// Wait forever for a connection slot, disabling the acquisition timeout.
    ///
    /// This restores the previous behavior where a saturated pool blocks the
    /// caller indefinitely. Prefer the bounded
    /// [`default`](PoolConfig::default) unless an unbounded wait is genuinely
    /// what you want -- an unbounded wait turns pool exhaustion into a silent
    /// hang rather than a surfaced [`RedisError::PoolAcquisitionTimeout`].
    pub fn disable_acquisition_timeout(mut self) -> Self {
        self.acquisition_timeout = None;
        self
    }

    /// Set the recorder that receives pool lifecycle metric events.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }
}

fn frame_text(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::SimpleString(value)
        | Frame::BulkString(Some(value))
        | Frame::VerbatimString(_, value) => Some(value.as_ref()),
        _ => None,
    }
}

fn parse_role_frame(frame: &Frame) -> Result<RedisRole, RedisError> {
    let Frame::Array(Some(parts)) = frame else {
        return Err(RedisError::UnexpectedResponse {
            expected: "ROLE array",
            actual: format!("{frame:?}"),
        });
    };
    let Some(role) = parts.first().and_then(frame_text) else {
        return Err(RedisError::UnexpectedResponse {
            expected: "ROLE array beginning with a role name",
            actual: format!("{frame:?}"),
        });
    };
    match role {
        b"master" => Ok(RedisRole::Primary),
        b"slave" | b"replica" => Ok(RedisRole::Replica),
        b"sentinel" => Ok(RedisRole::Sentinel),
        _ => Err(RedisError::UnexpectedResponse {
            expected: "master, slave/replica, or sentinel ROLE",
            actual: String::from_utf8_lossy(role).into_owned(),
        }),
    }
}

struct ReplicationObservation {
    link_up: bool,
    sync_in_progress: bool,
    lag_bytes: u64,
}

fn parse_replication_info(info: &str) -> Result<ReplicationObservation, RedisError> {
    let field = |name: &str| {
        info.lines().find_map(|line| {
            let (key, value) = line.trim_end_matches('\r').split_once(':')?;
            (key == name).then_some(value)
        })
    };

    if field("role") != Some("master") {
        return Err(RedisError::UnexpectedResponse {
            expected: "primary INFO replication with connected replica offsets",
            actual: info.to_owned(),
        });
    }

    let primary_offset = field("master_repl_offset")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| RedisError::UnexpectedResponse {
            expected: "numeric master_repl_offset in INFO replication",
            actual: info.to_owned(),
        })?;
    let connected_replicas = field("connected_slaves")
        .or_else(|| field("connected_replicas"))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| RedisError::UnexpectedResponse {
            expected: "numeric connected_slaves in primary INFO replication",
            actual: info.to_owned(),
        })?;

    let mut replica_offsets = Vec::with_capacity(connected_replicas);
    let mut all_online = true;
    for line in info.lines() {
        let Some((key, value)) = line.trim_end_matches('\r').split_once(':') else {
            continue;
        };
        let suffix = key
            .strip_prefix("slave")
            .or_else(|| key.strip_prefix("replica"));
        if !suffix.is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        }) {
            continue;
        }

        let replica_field = |name: &str| {
            value.split(',').find_map(|entry| {
                let (key, value) = entry.split_once('=')?;
                (key == name).then_some(value)
            })
        };
        let state = replica_field("state").ok_or_else(|| RedisError::UnexpectedResponse {
            expected: "state in primary replica INFO entry",
            actual: line.to_owned(),
        })?;
        let offset = replica_field("offset")
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| RedisError::UnexpectedResponse {
                expected: "numeric offset in primary replica INFO entry",
                actual: line.to_owned(),
            })?;
        all_online &= state == "online";
        replica_offsets.push(offset);
    }

    if replica_offsets.len() != connected_replicas {
        return Err(RedisError::UnexpectedResponse {
            expected: "one parseable INFO entry per connected replica",
            actual: info.to_owned(),
        });
    }

    Ok(ReplicationObservation {
        link_up: connected_replicas > 0 && all_online,
        sync_in_progress: !replica_offsets.is_empty() && !all_online,
        lag_bytes: replica_offsets
            .into_iter()
            .map(|offset| primary_offset.saturating_sub(offset))
            .max()
            .unwrap_or(0),
    })
}

/// Return the current epoch time in milliseconds.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The high bit of `admission_state` permanently marks the pool closed; the
/// remaining bits count accepted operations. Combining both values in one
/// atomic makes admission linearizable with [`ConnectionPool::close`].
const POOL_CLOSED_BIT: usize = 1usize << (usize::BITS - 1);
const POOL_ACTIVE_MASK: usize = !POOL_CLOSED_BIT;

/// Shared state behind the pool's Arc.
struct PoolSlot<S> {
    connection: Mutex<Option<S>>,
    active: AtomicBool,
    inflight: AtomicUsize,
    last_used: AtomicU64,
    health: AtomicU8,
    replication_lag_bytes: AtomicU64,
}

impl<S> PoolSlot<S> {
    fn active(connection: S, now: u64) -> Self {
        Self {
            connection: Mutex::new(Some(connection)),
            active: AtomicBool::new(true),
            inflight: AtomicUsize::new(0),
            last_used: AtomicU64::new(now),
            health: AtomicU8::new(PoolHealthState::Unknown.as_u8()),
            replication_lag_bytes: AtomicU64::new(u64::MAX),
        }
    }

    fn vacant(now: u64) -> Self {
        Self {
            connection: Mutex::new(None),
            active: AtomicBool::new(false),
            inflight: AtomicUsize::new(0),
            last_used: AtomicU64::new(now),
            health: AtomicU8::new(PoolHealthState::Unknown.as_u8()),
            replication_lag_bytes: AtomicU64::new(u64::MAX),
        }
    }

    #[cfg(test)]
    async fn lock(&self) -> MappedMutexGuard<'_, S> {
        MutexGuard::map(self.connection.lock().await, |connection| {
            connection
                .as_mut()
                .expect("active pool slot must contain a connection")
        })
    }
}

struct PoolInner<S> {
    /// Stable pool name attached to metric events.
    name: String,
    /// Fixed-capacity slot table. `active` determines which slots participate
    /// in dispatch; inactive slots contain `None` and can be filled on growth.
    connections: Vec<PoolSlot<S>>,
    min_size: usize,
    max_size: usize,
    idle_timeout_ms: Option<u64>,
    reaped_connections: AtomicU64,
    /// Serializes slot creation and reaping. Connection creation may await,
    /// so this is an async mutex separate from the short topology lock.
    scale_lock: Mutex<()>,
    /// Protects active/inactive transitions against reservation selection.
    topology_lock: RwLock<()>,
    index: AtomicUsize,
    dispatch: DispatchStrategy,
    /// Health check interval in milliseconds, or 0 if disabled.
    health_check_interval_ms: u64,
    /// Acquisition timeout. `None` means unlimited; zero means do not wait.
    acquisition_timeout: Option<Duration>,
    /// Optional factory used to replace dead connections after a failed PING.
    factory: Option<Arc<dyn ErasedPoolFactory<Connection = S>>>,
    /// Optional recorder for pool lifecycle events.
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
    /// Closed flag and pool-wide accepted-operation count in one atomic word.
    /// The high bit is the closed flag; the remaining bits are the count.
    admission_state: AtomicUsize,
}

/// Owns one pool-wide admission until an accepted operation finishes.
struct PoolAdmission<'a> {
    state: &'a AtomicUsize,
}

impl<'a> PoolAdmission<'a> {
    fn try_new(state: &'a AtomicUsize) -> Result<Self, RedisError> {
        let mut current = state.load(Ordering::Acquire);
        loop {
            if current & POOL_CLOSED_BIT != 0 {
                return Err(RedisError::ConnectionClosed);
            }
            assert!(
                current & POOL_ACTIVE_MASK != POOL_ACTIVE_MASK,
                "pool active-operation counter overflow"
            );

            match state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Self { state }),
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for PoolAdmission<'_> {
    fn drop(&mut self) {
        let previous = self.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous & POOL_ACTIVE_MASK > 0);
    }
}

#[derive(Clone, Copy)]
struct AcquisitionBudget {
    started: Instant,
    started_at: TokioInstant,
    request_deadline: Option<TokioInstant>,
    pool_timeout: Option<Duration>,
}

impl AcquisitionBudget {
    fn new(pool_timeout: Option<Duration>, request_deadline: Option<TokioInstant>) -> Self {
        Self {
            started: Instant::now(),
            started_at: TokioInstant::now(),
            request_deadline,
            pool_timeout,
        }
    }

    fn request_expired(self) -> bool {
        self.request_deadline
            .is_some_and(|deadline| deadline <= TokioInstant::now())
    }

    fn effective_wait_deadline(self) -> Option<(TokioInstant, Option<Duration>)> {
        let pool_deadline = self.pool_timeout.map(|duration| self.started_at + duration);
        match (self.request_deadline, pool_deadline) {
            (Some(request), Some(pool)) if request <= pool => Some((request, None)),
            (Some(_request), Some(pool)) => Some((pool, self.pool_timeout)),
            (Some(request), None) => Some((request, None)),
            (None, Some(pool)) => Some((pool, self.pool_timeout)),
            (None, None) => None,
        }
    }

    fn timeout_error(self, pool_size: usize, pool_timeout: Option<Duration>) -> RedisError {
        match pool_timeout {
            Some(waited) => RedisError::PoolAcquisitionTimeout { waited, pool_size },
            None => RedisError::CommandTimeout,
        }
    }

    async fn wait<F, T>(self, future: F, pool_size: usize) -> Result<T, RedisError>
    where
        F: Future<Output = T>,
    {
        let Some((deadline, pool_timeout)) = self.effective_wait_deadline() else {
            return Ok(future.await);
        };
        if deadline <= TokioInstant::now() {
            return Err(self.timeout_error(pool_size, pool_timeout));
        }
        match tokio::time::timeout_at(deadline, future).await {
            Ok(value) if deadline > TokioInstant::now() => Ok(value),
            Ok(_) | Err(_) => Err(self.timeout_error(pool_size, pool_timeout)),
        }
    }

    fn elapsed(self) -> Duration {
        self.started.elapsed()
    }
}

/// Owns one in-flight reservation until a command finishes or is cancelled.
///
/// Keeping the decrement in `Drop` makes every await in the execution path
/// cancellation-safe: dropping the command future releases its reservation
/// whether it is waiting for a connection, checking health, replacing a dead
/// connection, or awaiting the command response.
struct InflightReservation<'a, S> {
    slot: &'a PoolSlot<S>,
    index: usize,
    _admission: PoolAdmission<'a>,
}

impl<'a, S> InflightReservation<'a, S> {
    fn new(slot: &'a PoolSlot<S>, index: usize, admission: PoolAdmission<'a>) -> Self {
        slot.inflight.fetch_add(1, Ordering::Release);
        Self {
            slot,
            index,
            _admission: admission,
        }
    }

    fn index(&self) -> usize {
        self.index
    }

    /// Move this reservation to an alternate slot selected by `acquire`.
    fn transfer_to(&mut self, slot: &'a PoolSlot<S>, index: usize) {
        if index == self.index {
            return;
        }

        slot.inflight.fetch_add(1, Ordering::Release);
        self.slot.inflight.fetch_sub(1, Ordering::Release);
        self.slot = slot;
        self.index = index;
    }
}

impl<S> Drop for InflightReservation<'_, S> {
    fn drop(&mut self) {
        let previous = self.slot.inflight.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0);
    }
}

/// Object-safe wrapper around [`PoolFactory`] that erases the concrete type.
///
/// `PoolFactory` cannot itself be made into a trait object because of the
/// associated type, so we use this helper to expose the same surface via
/// `dyn`.
trait ErasedPoolFactory: Send + Sync + 'static {
    type Connection: Send + 'static;
    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>>;
}

impl<F: PoolFactory> ErasedPoolFactory for F {
    type Connection = F::Connection;
    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
        PoolFactory::create(self)
    }
}

impl<S> PoolInner<S>
where
    S: Send + 'static,
{
    fn topology_read(&self) -> std::sync::RwLockReadGuard<'_, ()> {
        self.topology_lock
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn topology_write(&self) -> std::sync::RwLockWriteGuard<'_, ()> {
        self.topology_lock
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn current_size(&self) -> usize {
        self.connections
            .iter()
            .filter(|slot| slot.active.load(Ordering::Acquire))
            .count()
    }

    /// Move an existing reservation only when `index` is still active.
    ///
    /// Holding the topology read lock through the in-flight increment makes
    /// the transfer atomic with idle reaping: a reaper either removes the slot
    /// first and this returns `false`, or observes the new reservation and
    /// leaves the slot active.
    fn transfer_reservation_if_active<'a>(
        &'a self,
        reservation: &mut InflightReservation<'a, S>,
        index: usize,
    ) -> bool {
        let topology = self.topology_read();
        if !self.connections[index].active.load(Ordering::Acquire) {
            return false;
        }
        reservation.transfer_to(&self.connections[index], index);
        drop(topology);
        true
    }

    /// Create and activate one connection when capacity remains.
    async fn grow_one(&self) -> Result<Option<usize>, RedisError> {
        let _scale = self.scale_lock.lock().await;
        if self.current_size() >= self.max_size {
            return Ok(None);
        }
        let Some(factory) = &self.factory else {
            return Ok(None);
        };
        let Some(index) = self
            .connections
            .iter()
            .position(|slot| !slot.active.load(Ordering::Acquire))
        else {
            return Ok(None);
        };

        let connection = ErasedPoolFactory::create(factory.as_ref()).await?;
        let mut slot = self.connections[index].connection.lock().await;
        let _topology = self.topology_write();
        if self.connections[index].active.load(Ordering::Acquire) {
            return Ok(None);
        }
        *slot = Some(connection);
        self.connections[index]
            .last_used
            .store(now_millis(), Ordering::Release);
        self.connections[index]
            .health
            .store(PoolHealthState::Unknown.as_u8(), Ordering::Release);
        self.connections[index]
            .replication_lag_bytes
            .store(u64::MAX, Ordering::Release);
        self.connections[index]
            .active
            .store(true, Ordering::Release);
        Ok(Some(index))
    }

    /// Select a live connection according to the dispatch strategy.
    async fn reserve_next(
        &self,
        budget: AcquisitionBudget,
    ) -> Result<InflightReservation<'_, S>, RedisError> {
        let admission = PoolAdmission::try_new(&self.admission_state)?;
        loop {
            let topology = self.topology_read();
            let active: Vec<usize> = self
                .connections
                .iter()
                .enumerate()
                .filter_map(|(index, slot)| slot.active.load(Ordering::Acquire).then_some(index))
                .collect();
            if !active.is_empty() {
                let index = match self.dispatch {
                    DispatchStrategy::RoundRobin => {
                        active[self.index.fetch_add(1, Ordering::Relaxed) % active.len()]
                    }
                    DispatchStrategy::Random => {
                        // Simple xorshift-based pseudo-random from the atomic
                        // counter. It is distribution, not cryptography.
                        let mut value = self.index.fetch_add(7, Ordering::Relaxed);
                        value ^= value << 13;
                        value ^= value >> 7;
                        value ^= value << 17;
                        active[value % active.len()]
                    }
                    DispatchStrategy::LeastConnections => *active
                        .iter()
                        .min_by_key(|index| {
                            self.connections[**index].inflight.load(Ordering::Acquire)
                        })
                        .expect("active slots are non-empty"),
                };
                let reservation =
                    InflightReservation::new(&self.connections[index], index, admission);
                drop(topology);
                return Ok(reservation);
            }
            drop(topology);

            let grown = match budget.wait(self.grow_one(), self.current_size()).await {
                Ok(result) => result,
                Err(error) => {
                    self.record_acquisition(budget.elapsed(), true);
                    return Err(error);
                }
            };
            match grown {
                Ok(Some(_)) => {}
                Ok(None) => {
                    self.record_acquisition(budget.elapsed(), false);
                    return Err(RedisError::ConnectionClosed);
                }
                Err(error) => {
                    self.record_acquisition(budget.elapsed(), false);
                    return Err(error);
                }
            }
        }
    }

    /// Acquire a connection guard, avoiding head-of-line blocking.
    ///
    /// `reservation` owns the strategy-selected slot's in-flight count. If that
    /// slot is immediately lockable it is used directly. Otherwise a `try_lock`
    /// scan looks for any idle connection so the request is not forced to queue
    /// behind a long-running command on the preferred slot while another
    /// connection sits free; when a free slot is found the reservation is moved
    /// to it. If every slot is busy, the call awaits the preferred slot,
    /// honoring the acquisition timeout.
    ///
    /// Returns the slot index actually acquired together with its guard.
    async fn acquire<'a>(
        &'a self,
        reservation: &mut InflightReservation<'a, S>,
        budget: AcquisitionBudget,
    ) -> Result<(usize, MutexGuard<'a, Option<S>>), RedisError> {
        let preferred = reservation.index();

        if budget.request_expired() {
            self.record_acquisition(budget.elapsed(), true);
            return Err(RedisError::CommandTimeout);
        }

        // Fast path: the preferred slot is free.
        if let Ok(guard) = self.connections[preferred].connection.try_lock() {
            if budget.request_expired() {
                drop(guard);
                self.record_acquisition(budget.elapsed(), true);
                return Err(RedisError::CommandTimeout);
            }
            self.record_acquisition(budget.elapsed(), false);
            return Ok((preferred, guard));
        }

        // Head-of-line-blocking avoidance: the preferred slot is busy, so scan
        // the remaining slots for any immediately free connection before
        // committing to an await on the busy slot.
        let capacity = self.connections.len();
        for offset in 1..capacity {
            let i = (preferred + offset) % capacity;
            if !self.connections[i].active.load(Ordering::Acquire) {
                continue;
            }
            if let Ok(guard) = self.connections[i].connection.try_lock() {
                if budget.request_expired() {
                    drop(guard);
                    self.record_acquisition(budget.elapsed(), true);
                    return Err(RedisError::CommandTimeout);
                }
                let topology = self.topology_read();
                if !self.connections[i].active.load(Ordering::Acquire) {
                    drop(topology);
                    drop(guard);
                    continue;
                }
                reservation.transfer_to(&self.connections[i], i);
                drop(topology);
                self.record_acquisition(budget.elapsed(), false);
                return Ok((i, guard));
            }
        }

        // All live slots are busy. A dynamic factory-backed pool grows on
        // observed contention before it queues behind an existing slot.
        if self.current_size() < self.max_size {
            let growth = budget.wait(self.grow_one(), self.current_size()).await;
            match growth {
                Ok(Ok(Some(index))) => {
                    if self.transfer_reservation_if_active(reservation, index) {
                        let guard = match budget
                            .wait(
                                self.connections[index].connection.lock(),
                                self.current_size(),
                            )
                            .await
                        {
                            Ok(guard) => guard,
                            Err(error) => {
                                self.record_acquisition(budget.elapsed(), true);
                                return Err(error);
                            }
                        };
                        self.record_acquisition(budget.elapsed(), false);
                        return Ok((index, guard));
                    }
                    // An idle reaper removed the newly activated slot before
                    // this caller could reserve it. Fall back to a still-live
                    // slot instead of returning an inactive connection.
                }
                Ok(Ok(None)) | Ok(Err(_)) => {
                    // Another caller filled capacity, or opportunistic growth
                    // failed. Existing live slots can still serve this request.
                }
                Err(error) => {
                    self.record_acquisition(budget.elapsed(), true);
                    return Err(error);
                }
            }
        }

        // Every slot is busy. Await the preferred slot until the earliest of
        // the command's absolute deadline and the pool's static acquisition
        // timeout. Preserve the error that identifies which budget expired.
        let guard = match budget
            .wait(
                self.connections[preferred].connection.lock(),
                self.current_size(),
            )
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                self.record_acquisition(budget.elapsed(), true);
                return Err(error);
            }
        };
        self.record_acquisition(budget.elapsed(), false);
        Ok((preferred, guard))
    }

    fn record_acquisition(&self, duration: Duration, timed_out: bool) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.pool_acquisition_completed(&self.name, duration, timed_out);
        }
    }

    fn record_health_check_failed(&self) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.pool_health_check_failed(&self.name);
        }
    }

    fn record_connection_replaced(&self) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.pool_connection_replaced(&self.name);
        }
    }

    fn record_health_probe(
        &self,
        kind: HealthProbeKind,
        duration: Duration,
        healthy: bool,
        replication_lag_bytes: Option<u64>,
    ) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.pool_health_probe_completed(
                &self.name,
                kind,
                duration,
                healthy,
                replication_lag_bytes,
            );
        }
    }

    fn record_connections_reaped(&self, count: usize) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.pool_connections_reaped(&self.name, count);
        }
    }

    async fn reap_idle_once(&self) -> usize {
        let Some(idle_timeout_ms) = self.idle_timeout_ms else {
            return 0;
        };
        let _scale = self.scale_lock.lock().await;
        let now = now_millis();
        let mut removed = Vec::new();
        let topology = self.topology_write();
        let mut active = self.current_size();
        for slot in self.connections.iter().rev() {
            if active <= self.min_size {
                break;
            }
            if !slot.active.load(Ordering::Acquire)
                || slot.inflight.load(Ordering::Acquire) != 0
                || now.saturating_sub(slot.last_used.load(Ordering::Acquire)) < idle_timeout_ms
            {
                continue;
            }
            let Ok(mut connection) = slot.connection.try_lock() else {
                continue;
            };
            if slot.inflight.load(Ordering::Acquire) != 0 {
                continue;
            }
            slot.active.store(false, Ordering::Release);
            if let Some(connection) = connection.take() {
                removed.push(connection);
                active -= 1;
            }
            slot.health
                .store(PoolHealthState::Unknown.as_u8(), Ordering::Release);
            slot.replication_lag_bytes
                .store(u64::MAX, Ordering::Release);
        }
        drop(topology);
        let count = removed.len();
        drop(removed);
        if count > 0 {
            self.reaped_connections
                .fetch_add(count as u64, Ordering::AcqRel);
            self.record_connections_reaped(count);
        }
        count
    }
}

/// Owned lifecycle handle for an active pool health-prober task.
///
/// Dropping the handle cancels the task. Use [`Self::shutdown`] to cancel and
/// wait for termination.
#[must_use = "dropping the handle immediately stops active health probing"]
pub struct HealthProberHandle {
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl HealthProberHandle {
    /// Cancel the prober and wait for its task to finish.
    pub async fn shutdown(mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for HealthProberHandle {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Owned lifecycle handle for an idle-connection reaper task.
///
/// Dropping the handle cancels the task. Use [`Self::shutdown`] to cancel and
/// wait for termination.
#[must_use = "dropping the handle immediately stops idle-connection reaping"]
pub struct IdleReaperHandle {
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl IdleReaperHandle {
    /// Cancel the reaper and wait for its task to finish.
    pub async fn shutdown(mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for IdleReaperHandle {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// A pool of Redis connections that dispatches commands across them.
///
/// Generic over `S: RedisExecutor`, so it works with:
/// - `RedisConnection` (standalone)
/// - `ClusterConnection` (cluster -- each entry manages its own topology)
/// - `SentinelConnection` (sentinel -- each entry discovers its own master)
/// - `ResilientConnection` (standalone with auto-reconnect)
/// - Any custom type implementing `RedisExecutor`
///
/// The pool implements `Clone` via `Arc` for cross-task sharing and
/// implements `RedisExecutor` itself for composability.
///
/// # Concurrency
///
/// `ConnectionPool<S>` is `Clone + Send + Sync` when `S: Send`. All clones
/// share the same pool via `Arc`. Each individual connection is protected by
/// its own `Mutex`, so up to N commands can execute in parallel (one per
/// pooled connection). The dispatch strategy controls which connection handles
/// each command.
pub struct ConnectionPool<S> {
    inner: Arc<PoolInner<S>>,
}

impl<S> Clone for ConnectionPool<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> ConnectionPool<S>
where
    S: Send + 'static,
{
    fn resolved_bounds(config: &PoolConfig) -> (usize, usize) {
        let min_size = config.min_size.unwrap_or(config.size);
        let max_size = config.max_size.unwrap_or(config.size);
        assert!(max_size > 0, "pool max_size must be at least 1");
        assert!(
            min_size <= max_size,
            "pool min_size ({min_size}) must not exceed max_size ({max_size})"
        );
        if let Some(idle_timeout) = config.idle_timeout {
            assert!(
                !idle_timeout.is_zero(),
                "pool idle_timeout must be non-zero"
            );
        }
        (min_size, max_size)
    }

    fn from_initial(
        config: PoolConfig,
        initial: Vec<S>,
        min_size: usize,
        max_size: usize,
        factory: Option<Arc<dyn ErasedPoolFactory<Connection = S>>>,
    ) -> Self {
        assert!(initial.len() <= max_size);
        let now = now_millis();
        let mut connections = Vec::with_capacity(max_size);
        connections.extend(
            initial
                .into_iter()
                .map(|connection| PoolSlot::active(connection, now)),
        );
        connections.extend((connections.len()..max_size).map(|_| PoolSlot::vacant(now)));
        let health_check_interval_ms = config
            .health_check_interval
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        let idle_timeout_ms = config
            .idle_timeout
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64);

        Self {
            inner: Arc::new(PoolInner {
                name: config.name,
                connections,
                min_size,
                max_size,
                idle_timeout_ms,
                reaped_connections: AtomicU64::new(0),
                scale_lock: Mutex::new(()),
                topology_lock: RwLock::new(()),
                index: AtomicUsize::new(0),
                dispatch: config.dispatch,
                health_check_interval_ms,
                acquisition_timeout: config.acquisition_timeout,
                factory,
                metrics_recorder: config.metrics_recorder,
                admission_state: AtomicUsize::new(0),
            }),
        }
    }

    /// Create a pool by calling a factory function `size` times.
    ///
    /// Each call to `factory` should return a new, independent connection.
    /// For cluster connections, each entry will discover its own topology.
    pub async fn connect<F, Fut>(size: usize, factory: F) -> Result<Self, RedisError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<S, RedisError>>,
    {
        Self::connect_with_config(PoolConfig::default().size(size), factory).await
    }

    /// Create a pool with custom configuration.
    pub async fn connect_with_config<F, Fut>(
        config: PoolConfig,
        factory: F,
    ) -> Result<Self, RedisError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<S, RedisError>>,
    {
        let (min_size, max_size) = Self::resolved_bounds(&config);
        assert_eq!(
            min_size, max_size,
            "dynamic sizing requires connect_with_factory"
        );
        let mut connections = Vec::with_capacity(min_size);
        for _ in 0..min_size {
            let conn = factory().await?;
            connections.push(conn);
        }
        Ok(Self::from_initial(
            config,
            connections,
            min_size,
            max_size,
            None,
        ))
    }

    /// Create a pool with custom configuration and a [`PoolFactory`].
    ///
    /// The factory is used both to build the initial connections and to
    /// replace any connection slot that fails a health-check PING. This
    /// ensures that a single dead connection does not permanently degrade
    /// pool capacity.
    pub async fn connect_with_factory<Fact>(
        config: PoolConfig,
        factory: Fact,
    ) -> Result<Self, RedisError>
    where
        Fact: PoolFactory<Connection = S>,
        S: RedisExecutor,
    {
        let (min_size, max_size) = Self::resolved_bounds(&config);
        let mut connections = Vec::with_capacity(min_size);
        for _ in 0..min_size {
            let conn = factory.create().await?;
            connections.push(conn);
        }
        Ok(Self::from_initial(
            config,
            connections,
            min_size,
            max_size,
            Some(Arc::new(factory)),
        ))
    }

    /// Create a factory-backed pool without opening a connection.
    ///
    /// The first command creates the first slot; later acquisition contention
    /// grows the pool up to `config.max_size` (or `config.size` when omitted).
    /// The lazy pool's effective minimum is zero, regardless of
    /// `config.min_size`, so an explicitly spawned idle reaper may return it
    /// to zero while the application is inactive.
    pub fn connect_lazy<Fact>(config: PoolConfig, factory: Fact) -> Self
    where
        Fact: PoolFactory<Connection = S>,
        S: RedisExecutor,
    {
        let max_size = config.max_size.unwrap_or(config.size);
        assert!(max_size > 0, "pool max_size must be at least 1");
        if let Some(idle_timeout) = config.idle_timeout {
            assert!(
                !idle_timeout.is_zero(),
                "pool idle_timeout must be non-zero"
            );
        }
        Self::from_initial(config, Vec::new(), 0, max_size, Some(Arc::new(factory)))
    }

    /// Build a pool from pre-created connections using the given [`PoolConfig`].
    ///
    /// Sizing fields are ignored here because a pool without a retained
    /// factory cannot restore reaped capacity. The pool is fixed at the number
    /// of supplied connections. Dispatch, health-check, acquisition-timeout,
    /// name, and metrics settings are applied normally.
    ///
    /// # Errors
    ///
    /// Returns an error if `connections` is empty.
    pub fn from_connections(connections: Vec<S>, config: PoolConfig) -> Result<Self, RedisError> {
        if connections.is_empty() {
            return Err(RedisError::InvalidUrl(
                "pool requires at least one connection".into(),
            ));
        }

        let size = connections.len();
        Ok(Self::from_initial(config, connections, size, size, None))
    }

    /// Returns the number of connections in the pool.
    pub fn size(&self) -> usize {
        self.inner.current_size()
    }

    /// Returns the stable name used to identify this pool in metrics.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the dispatch strategy.
    pub fn dispatch_strategy(&self) -> DispatchStrategy {
        self.inner.dispatch
    }

    /// Returns a snapshot of current pool utilization.
    ///
    /// Reads the per-connection in-flight atomic counters to compute idle
    /// and active connection counts. This is a non-blocking, snapshot-in-
    /// time read — values may have changed by the time the caller acts on
    /// them.
    ///
    /// Useful for emitting `redis_pool_connections_active` /
    /// `redis_pool_connections_idle` Prometheus metrics.
    pub fn stats(&self) -> PoolStats {
        let mut total_inflight = 0usize;
        let mut max_inflight = 0usize;
        let mut idle_count = 0usize;
        let mut healthy_count = 0usize;
        let mut unhealthy_count = 0usize;
        let mut unknown_health_count = 0usize;
        let mut max_replication_lag_bytes = None;
        let topology = self.inner.topology_read();
        for slot in &self.inner.connections {
            if !slot.active.load(Ordering::Acquire) {
                continue;
            }
            let v = slot.inflight.load(Ordering::Relaxed);
            total_inflight += v;
            if v > max_inflight {
                max_inflight = v;
            }
            if v == 0 {
                idle_count += 1;
            }
            match PoolHealthState::from_u8(slot.health.load(Ordering::Acquire)) {
                PoolHealthState::Healthy => healthy_count += 1,
                PoolHealthState::Unhealthy => unhealthy_count += 1,
                PoolHealthState::Unknown => unknown_health_count += 1,
            }
            let lag = slot.replication_lag_bytes.load(Ordering::Acquire);
            if lag != u64::MAX {
                max_replication_lag_bytes =
                    Some(max_replication_lag_bytes.map_or(lag, |current: u64| current.max(lag)));
            }
        }
        let size = healthy_count + unhealthy_count + unknown_health_count;
        drop(topology);
        PoolStats {
            size,
            idle_count,
            total_inflight,
            max_inflight,
            min_size: self.inner.min_size,
            max_size: self.inner.max_size,
            healthy_count,
            unhealthy_count,
            unknown_health_count,
            max_replication_lag_bytes,
            reaped_connections: self.inner.reaped_connections.load(Ordering::Acquire),
        }
    }

    /// Remove idle connections above the configured minimum immediately.
    ///
    /// This is a one-shot operation and does not spawn a task. It returns zero
    /// when no `idle_timeout` is configured or no eligible slot exists.
    pub async fn reap_idle_connections(&self) -> usize {
        self.inner.reap_idle_once().await
    }

    /// Explicitly spawn periodic idle-connection reaping.
    ///
    /// Pool construction never starts this task. The first sweep runs
    /// immediately, then every `interval`. Dropping the returned handle stops
    /// the task.
    ///
    /// # Panics
    ///
    /// Panics when `interval` is zero, no idle timeout is configured, or this
    /// method is called outside a Tokio runtime.
    pub fn spawn_idle_reaper(&self, interval: Duration) -> IdleReaperHandle {
        assert!(!interval.is_zero(), "idle reaper interval must be non-zero");
        assert!(
            self.inner.idle_timeout_ms.is_some(),
            "spawn_idle_reaper requires PoolConfig::idle_timeout"
        );
        let pool = self.clone();
        let (cancel, mut cancelled) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = cancelled.changed() => {
                        if changed.is_err() || *cancelled.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if pool.is_closed() {
                            break;
                        }
                        pool.reap_idle_connections().await;
                    }
                }
            }
        });
        IdleReaperHandle {
            cancel,
            task: Some(task),
        }
    }

    /// Returns `true` once [`close`](Self::close) has been called on this pool
    /// or any of its clones.
    pub fn is_closed(&self) -> bool {
        self.inner.admission_state.load(Ordering::Acquire) & POOL_CLOSED_BIT != 0
    }

    /// Gracefully close the pool, draining in-flight commands.
    ///
    /// Flips a shared "closed" bit so that every clone of this pool rejects new
    /// commands with [`RedisError::ConnectionClosed`], then waits for all
    /// in-flight commands to finish before returning. Each accepted command
    /// atomically increments a pool-wide operation count while confirming the
    /// pool is open, so closing and admission have one unambiguous order.
    ///
    /// This is the SIGTERM drain path: stop accepting work, let outstanding
    /// commands complete, then exit. It does not itself close the underlying
    /// connections -- dropping the pool (and its last clone) releases them --
    /// but it guarantees no command is mid-flight when the connections are
    /// dropped.
    ///
    /// Because the state is shared through the pool's `Arc`, calling `close` on
    /// one clone drains and closes the pool seen by every clone. Commands
    /// admitted before the close transition finish normally; commands racing
    /// after it are rejected.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower::{ConnectionPool, RedisConnection};
    ///
    /// let pool =
    ///     ConnectionPool::connect(4, || RedisConnection::connect("127.0.0.1:6379")).await?;
    ///
    /// // On shutdown (SIGTERM, ctrl-c, ...): stop accepting new work and drain
    /// // the pool before exit.
    /// pool.close().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(self) {
        // Atomically close admission while preserving the accepted-operation
        // count. A racing reservation is therefore either included in the
        // count or observes the closed bit and is rejected.
        self.inner
            .admission_state
            .fetch_or(POOL_CLOSED_BIT, Ordering::AcqRel);

        // A closed pool accepts no new work, so the active portion of the
        // state is monotonically non-increasing and eventually reaches zero.
        loop {
            if self.inner.admission_state.load(Ordering::Acquire) & POOL_ACTIVE_MASK == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Select and reserve the next connection based on dispatch strategy.
    ///
    /// The returned RAII guard releases the in-flight count on drop, including
    /// when an execution future is cancelled at an await point.
    async fn next_index(
        &self,
        budget: AcquisitionBudget,
    ) -> Result<InflightReservation<'_, S>, RedisError> {
        self.inner.reserve_next(budget).await
    }
}

impl<S> ConnectionPool<S>
where
    S: RedisExecutor + Send + 'static,
{
    /// Explicitly spawn active PING probing with an owned lifecycle handle.
    ///
    /// Pool construction never starts a prober. The first sweep runs
    /// immediately. Dropping the returned handle stops the task.
    pub fn spawn_health_prober(&self, config: HealthProberConfig) -> HealthProberHandle {
        self.spawn_health_prober_with(config, PingHealthProbe)
    }

    /// Explicitly spawn active probing with a custom [`HealthProbe`].
    ///
    /// Results are observational: they update [`PoolStats`] and
    /// [`MetricsRecorder`] without silently changing dispatch policy.
    ///
    /// # Panics
    ///
    /// Panics when the interval or timeout is zero, or when called outside a
    /// Tokio runtime.
    pub fn spawn_health_prober_with<P>(
        &self,
        config: HealthProberConfig,
        probe: P,
    ) -> HealthProberHandle
    where
        P: HealthProbe<S>,
    {
        assert!(
            !config.interval.is_zero(),
            "health prober interval must be non-zero"
        );
        assert!(
            !config.timeout.is_zero(),
            "health prober timeout must be non-zero"
        );
        let pool = self.clone();
        let probe = Arc::new(probe);
        let kind = probe.kind();
        let (cancel, mut cancelled) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = cancelled.changed() => {
                        if changed.is_err() || *cancelled.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if pool.is_closed() {
                            break;
                        }
                        let active: Vec<usize> = {
                            let _topology = pool.inner.topology_read();
                            pool.inner.connections.iter().enumerate()
                                .filter_map(|(index, slot)| {
                                    slot.active.load(Ordering::Acquire).then_some(index)
                                })
                                .collect()
                        };
                        for index in active {
                            if *cancelled.borrow() {
                                break;
                            }
                            // Admission is linearized with close(): a probe
                            // that starts first is drained, while a probe that
                            // loses the race sees the closed bit and the task
                            // exits without touching a connection.
                            let admission = match PoolAdmission::try_new(
                                &pool.inner.admission_state,
                            ) {
                                Ok(admission) => admission,
                                Err(_) => break,
                            };
                            let slot = &pool.inner.connections[index];
                            if !slot.active.load(Ordering::Acquire) {
                                continue;
                            }
                            // Active probing is observational and must never
                            // turn ordinary command contention into a health
                            // failure. Busy slots are retried on the next
                            // sweep instead of spending the probe timeout on
                            // their mutex.
                            let Ok(mut connection) = slot.connection.try_lock() else {
                                continue;
                            };
                            let Some(connection) = connection.as_mut() else {
                                continue;
                            };
                            let started = Instant::now();
                            let outcome = tokio::time::timeout(
                                config.timeout,
                                probe.probe(connection),
                            )
                            .await;
                            let result = match outcome {
                                Ok(Ok(result)) => result,
                                Ok(Err(_)) | Err(_) => HealthProbeResult::unhealthy(),
                            };
                            // Reaping may deactivate a slot while this task is
                            // waiting for its mutex. Do not publish a false
                            // failure for an intentionally removed connection.
                            if !slot.active.load(Ordering::Acquire) {
                                continue;
                            }
                            slot.health.store(
                                if result.healthy {
                                    PoolHealthState::Healthy.as_u8()
                                } else {
                                    PoolHealthState::Unhealthy.as_u8()
                                },
                                Ordering::Release,
                            );
                            slot.replication_lag_bytes.store(
                                result.replication_lag_bytes.unwrap_or(u64::MAX),
                                Ordering::Release,
                            );
                            if !result.healthy {
                                pool.inner.record_health_check_failed();
                            }
                            pool.inner.record_health_probe(
                                kind,
                                started.elapsed(),
                                result.healthy,
                                result.replication_lag_bytes,
                            );
                            drop(admission);
                        }
                    }
                }
            }
        });
        HealthProberHandle {
            cancel,
            task: Some(task),
        }
    }
}

impl<S> RedisExecutor for ConnectionPool<S>
where
    S: RedisExecutor + Send + 'static,
{
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let deadline = cmd.deadline();
            let budget = AcquisitionBudget::new(inner.acquisition_timeout, deadline);
            // Keep reservation creation inside the async body. Constructing a
            // trait-method future must not reserve a slot before that future is
            // first polled. Admission and the closed check are one atomic step.
            let mut reservation = inner.reserve_next(budget).await?;
            // Acquire a connection, scanning for a free slot first to avoid
            // head-of-line blocking on a busy one, then falling back to an
            // awaited (optionally timed) lock on the preferred slot.
            let (idx, mut conn) = inner.acquire(&mut reservation, budget).await?;
            let connection = conn.as_mut().ok_or(RedisError::ConnectionClosed)?;

            // Lazy health check: PING if idle beyond the threshold.
            // Gate the syscall behind the interval check to avoid calling
            // SystemTime::now() on every execute when health checks are disabled.
            if inner.health_check_interval_ms > 0 {
                let last = inner.connections[idx].last_used.load(Ordering::Acquire);
                let now = now_millis();
                if now.saturating_sub(last) >= inner.health_check_interval_ms
                    && let Err(ping_err) = connection.execute(Ping::new()).await
                {
                    inner.record_health_check_failed();
                    // PING failed. Attempt to replace the dead slot via the factory.
                    if let Some(ref factory) = inner.factory {
                        match factory.create().await {
                            Ok(fresh) => {
                                *connection = fresh;
                                inner.connections[idx]
                                    .last_used
                                    .store(now_millis(), Ordering::Release);
                                inner.connections[idx]
                                    .health
                                    .store(PoolHealthState::Unknown.as_u8(), Ordering::Release);
                                inner.record_connection_replaced();
                            }
                            Err(replace_err) => {
                                drop(conn);
                                return Err(replace_err);
                            }
                        }
                    } else {
                        drop(conn);
                        return Err(ping_err);
                    }
                }
            }

            let result = connection.execute(cmd).await;
            if inner.health_check_interval_ms > 0 || inner.idle_timeout_ms.is_some() {
                inner.connections[idx]
                    .last_used
                    .store(now_millis(), Ordering::Release);
            }
            drop(conn);
            result
        }
    }
}

// Also implement for &ConnectionPool so it can be used without mut
// (the pool handles interior mutability via per-connection Mutex).
impl<S> ConnectionPool<S>
where
    S: RedisExecutor + Send + 'static,
{
    /// Execute a command through the pool.
    ///
    /// This is the primary API. The pool selects a connection via the
    /// configured dispatch strategy and executes the command on it. To avoid
    /// head-of-line blocking, if the strategy-selected connection is currently
    /// busy the pool first scans for any idle connection (via `try_lock`) and
    /// uses that instead, only awaiting a busy slot when every connection is in
    /// use.
    ///
    /// If `health_check_interval` is configured and the selected connection
    /// has been idle longer than the interval, a PING is sent first to
    /// verify the connection is alive. If the PING fails and the pool was
    /// built with a [`PoolFactory`] (via [`ConnectionPool::connect_with_factory`]),
    /// the dead slot is replaced with a fresh connection before the actual
    /// command is executed. If no factory is available, the PING error is
    /// returned to the caller.
    ///
    /// If `cmd` is wrapped in [`redis_tower_core::WithDeadline`], pool
    /// acquisition observes that same absolute deadline. The request deadline
    /// and [`PoolConfig::acquisition_timeout`] do not add together: whichever
    /// expires first determines the error.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        let budget = AcquisitionBudget::new(self.inner.acquisition_timeout, deadline);
        let mut reservation = self.next_index(budget).await?;
        // Acquire a connection, scanning for a free slot first to avoid
        // head-of-line blocking on a busy one, then falling back to an awaited
        // (optionally timed) lock on the preferred slot.
        let (idx, mut conn) = self.inner.acquire(&mut reservation, budget).await?;
        let connection = conn.as_mut().ok_or(RedisError::ConnectionClosed)?;

        // Lazy health check: PING if idle beyond the threshold.
        // Gate the syscall behind the interval check to avoid calling
        // SystemTime::now() on every execute when health checks are disabled.
        if self.inner.health_check_interval_ms > 0 {
            let last = self.inner.connections[idx]
                .last_used
                .load(Ordering::Acquire);
            let now = now_millis();
            if now.saturating_sub(last) >= self.inner.health_check_interval_ms
                && let Err(ping_err) = connection.execute(Ping::new()).await
            {
                self.inner.record_health_check_failed();
                // PING failed. Attempt to replace the dead slot via the factory.
                if let Some(ref factory) = self.inner.factory {
                    match factory.create().await {
                        Ok(fresh) => {
                            *connection = fresh;
                            self.inner.connections[idx]
                                .last_used
                                .store(now_millis(), Ordering::Release);
                            self.inner.connections[idx]
                                .health
                                .store(PoolHealthState::Unknown.as_u8(), Ordering::Release);
                            self.inner.record_connection_replaced();
                        }
                        Err(replace_err) => {
                            drop(conn);
                            return Err(replace_err);
                        }
                    }
                } else {
                    drop(conn);
                    return Err(ping_err);
                }
            }
        }

        let result = connection.execute(cmd).await;
        if self.inner.health_check_interval_ms > 0 || self.inner.idle_timeout_ms.is_some() {
            self.inner.connections[idx]
                .last_used
                .store(now_millis(), Ordering::Release);
        }
        drop(conn);
        result
    }

    /// Send a PING to every connection in the pool.
    ///
    /// Acquires each connection sequentially and sends `PING`. Returns
    /// `Ok(())` if all connections respond successfully. Returns the first
    /// error encountered if any connection is unhealthy.
    ///
    /// Useful for Kubernetes readiness probes and `/health` endpoints. For
    /// a fast single-connection liveness check, call
    /// [`execute`](ConnectionPool::execute) with [`Ping`]
    /// directly.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        let _admission = PoolAdmission::try_new(&self.inner.admission_state)?;
        for slot in &self.inner.connections {
            if !slot.active.load(Ordering::Acquire) {
                continue;
            }
            let mut conn = slot.connection.lock().await;
            let Some(conn) = conn.as_mut() else {
                continue;
            };
            if let Err(error) = conn.execute(Ping::new()).await {
                self.inner.record_health_check_failed();
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics_layer::ErrorKind;
    use bytes::Bytes;
    use redis_tower_core::{Frame, WithDeadline};
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct CountingRecorder {
        acquisitions: StdMutex<Vec<(String, Duration, bool)>>,
        health_check_failures: AtomicUsize,
        connection_replacements: AtomicUsize,
        health_probes: StdMutex<Vec<(HealthProbeKind, bool, Option<u64>)>>,
        reaped_connections: AtomicUsize,
    }

    impl CountingRecorder {
        fn acquisitions(&self) -> Vec<(String, Duration, bool)> {
            self.acquisitions.lock().unwrap().clone()
        }
    }

    impl MetricsRecorder for CountingRecorder {
        fn command_completed(
            &self,
            _command: &str,
            _duration: Duration,
            _error: Option<ErrorKind>,
        ) {
        }

        fn pool_acquisition_completed(&self, pool_name: &str, duration: Duration, timed_out: bool) {
            self.acquisitions
                .lock()
                .unwrap()
                .push((pool_name.to_owned(), duration, timed_out));
        }

        fn pool_health_check_failed(&self, _pool_name: &str) {
            self.health_check_failures.fetch_add(1, Ordering::Relaxed);
        }

        fn pool_connection_replaced(&self, _pool_name: &str) {
            self.connection_replacements.fetch_add(1, Ordering::Relaxed);
        }

        fn pool_health_probe_completed(
            &self,
            _pool_name: &str,
            kind: HealthProbeKind,
            _duration: Duration,
            healthy: bool,
            replication_lag_bytes: Option<u64>,
        ) {
            self.health_probes
                .lock()
                .unwrap()
                .push((kind, healthy, replication_lag_bytes));
        }

        fn pool_connections_reaped(&self, _pool_name: &str, count: usize) {
            self.reaped_connections.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Mock connection for testing pool dispatch without Redis.
    struct MockConn {
        _id: usize,
        responses: tokio::sync::Mutex<VecDeque<Frame>>,
        call_count: AtomicUsize,
    }

    impl MockConn {
        fn new(id: usize, responses: Vec<Frame>) -> Self {
            Self {
                _id: id,
                responses: tokio::sync::Mutex::new(VecDeque::from(responses)),
                call_count: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    impl RedisExecutor for MockConn {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let frame = self
                .responses
                .try_lock()
                .ok()
                .and_then(|mut q| q.pop_front())
                .unwrap_or(Frame::Null);
            async move { cmd.parse_response(frame) }
        }
    }

    struct DelayedConn {
        delay: Duration,
        call_count: AtomicUsize,
    }

    impl DelayedConn {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                call_count: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    impl RedisExecutor for DelayedConn {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let delay = self.delay;
            async move {
                tokio::time::sleep(delay).await;
                cmd.parse_response(Frame::SimpleString(Bytes::from("PONG")))
            }
        }
    }

    #[derive(Clone)]
    struct DelayedFactory {
        delay: Duration,
        creates: Arc<AtomicUsize>,
    }

    impl DelayedFactory {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                creates: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn creates(&self) -> usize {
            self.creates.load(Ordering::Acquire)
        }
    }

    impl PoolFactory for DelayedFactory {
        type Connection = DelayedConn;

        fn create(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
            self.creates.fetch_add(1, Ordering::AcqRel);
            let delay = self.delay;
            Box::pin(async move { Ok(DelayedConn::new(delay)) })
        }
    }

    #[derive(Clone)]
    struct SlowFactory {
        connect_delay: Duration,
        creates: Arc<AtomicUsize>,
    }

    impl SlowFactory {
        fn new(connect_delay: Duration) -> Self {
            Self {
                connect_delay,
                creates: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl PoolFactory for SlowFactory {
        type Connection = DelayedConn;

        fn create(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
            self.creates.fetch_add(1, Ordering::AcqRel);
            let delay = self.connect_delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(DelayedConn::new(Duration::ZERO))
            })
        }
    }

    #[derive(Clone)]
    struct CancelThenConnectFactory {
        calls: Arc<AtomicUsize>,
        first_entered: Arc<tokio::sync::Notify>,
    }

    impl PoolFactory for CancelThenConnectFactory {
        type Connection = MockConn;

        fn create(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            let entered = Arc::clone(&self.first_entered);
            Box::pin(async move {
                if call == 0 {
                    entered.notify_one();
                    futures::future::pending::<()>().await;
                }
                Ok(MockConn::new(
                    call,
                    vec![Frame::SimpleString(Bytes::from("PONG"))],
                ))
            })
        }
    }

    /// Mock factory that hands out pre-built MockConn instances one at a time.
    #[derive(Clone)]
    struct MockFactory {
        conns: Arc<tokio::sync::Mutex<VecDeque<MockConn>>>,
        creates: Arc<AtomicUsize>,
    }

    impl MockFactory {
        fn new(conns: Vec<MockConn>) -> Self {
            Self {
                conns: Arc::new(tokio::sync::Mutex::new(VecDeque::from(conns))),
                creates: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn creates(&self) -> usize {
            self.creates.load(Ordering::Acquire)
        }
    }

    impl PoolFactory for MockFactory {
        type Connection = MockConn;

        fn create(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
            let conns = Arc::clone(&self.conns);
            let creates = Arc::clone(&self.creates);
            Box::pin(async move {
                creates.fetch_add(1, Ordering::AcqRel);
                conns
                    .lock()
                    .await
                    .pop_front()
                    .ok_or_else(|| RedisError::InvalidUrl("no more mock connections".into()))
            })
        }
    }

    #[tokio::test]
    async fn pool_default_config() {
        let config = PoolConfig::default();
        assert_eq!(config.name, "redis-tower");
        assert_eq!(config.size, 4);
        assert!(matches!(config.dispatch, DispatchStrategy::RoundRobin));
        assert!(config.metrics_recorder.is_none());
    }

    #[tokio::test]
    async fn pool_config_builder() {
        let recorder = Arc::new(CountingRecorder::default());
        let config = PoolConfig::default()
            .name("primary-cache")
            .size(8)
            .dispatch(DispatchStrategy::Random)
            .metrics_recorder(recorder);
        let debug = format!("{config:?}");

        assert_eq!(config.name, "primary-cache");
        assert_eq!(config.size, 8);
        assert!(matches!(config.dispatch, DispatchStrategy::Random));
        assert!(config.metrics_recorder.is_some());
        assert!(debug.contains("primary-cache"));
        assert!(debug.contains("<recorder>"));
    }

    #[tokio::test]
    async fn pool_from_connections() {
        let conns = vec![MockConn::new(0, vec![]), MockConn::new(1, vec![])];
        let pool =
            ConnectionPool::from_connections(conns, PoolConfig::default().name("read-replicas"))
                .unwrap();
        assert_eq!(pool.size(), 2);
        assert_eq!(pool.name(), "read-replicas");
    }

    #[tokio::test]
    async fn pool_empty_connections_fails() {
        let result = ConnectionPool::<MockConn>::from_connections(vec![], PoolConfig::default());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pool_round_robin_distributes() {
        use redis_tower_commands::Ping;

        let conns = vec![
            MockConn::new(
                0,
                vec![
                    Frame::SimpleString(Bytes::from("PONG")),
                    Frame::SimpleString(Bytes::from("PONG")),
                ],
            ),
            MockConn::new(
                1,
                vec![
                    Frame::SimpleString(Bytes::from("PONG")),
                    Frame::SimpleString(Bytes::from("PONG")),
                ],
            ),
        ];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::RoundRobin),
        )
        .unwrap();

        // 4 commands should distribute 2 to each connection.
        for _ in 0..4 {
            let _: String = pool.execute(Ping::new()).await.unwrap();
        }

        // Check distribution via the atomic counter -- pool alternates.
        // Connection 0 got calls 0, 2; connection 1 got calls 1, 3.
        let c0 = pool.inner.connections[0].lock().await;
        let c1 = pool.inner.connections[1].lock().await;
        assert_eq!(c0.calls(), 2);
        assert_eq!(c1.calls(), 2);
    }

    #[tokio::test]
    async fn pool_connect_factory() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        let pool = ConnectionPool::connect(3, || {
            let c = c.clone();
            async move {
                let id = c.fetch_add(1, Ordering::Relaxed);
                Ok::<_, RedisError>(MockConn::new(id, vec![]))
            }
        })
        .await
        .unwrap();

        assert_eq!(pool.size(), 3);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn pool_clone_shares_state() {
        use redis_tower_commands::Ping;

        let conns = vec![MockConn::new(
            0,
            vec![
                Frame::SimpleString(Bytes::from("PONG")),
                Frame::SimpleString(Bytes::from("PONG")),
            ],
        )];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::RoundRobin),
        )
        .unwrap();
        let pool2 = pool.clone();

        let _: String = pool.execute(Ping::new()).await.unwrap();
        let _: String = pool2.execute(Ping::new()).await.unwrap();

        let c0 = pool.inner.connections[0].lock().await;
        assert_eq!(c0.calls(), 2); // Both clones hit the same connection.
    }

    #[tokio::test]
    async fn pool_random_dispatch() {
        use redis_tower_commands::Ping;

        let mut conns = Vec::new();
        for i in 0..4 {
            conns.push(MockConn::new(
                i,
                (0..10)
                    .map(|_| Frame::SimpleString(Bytes::from("PONG")))
                    .collect(),
            ));
        }

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::Random),
        )
        .unwrap();

        for _ in 0..20 {
            let _: String = pool.execute(Ping::new()).await.unwrap();
        }

        // All 20 calls should have been distributed (not all to one connection).
        let mut total = 0;
        for c in &pool.inner.connections {
            total += c.lock().await.calls();
        }
        assert_eq!(total, 20);
    }

    #[tokio::test]
    async fn pool_execute_returns_correct_response() {
        use redis_tower_commands::Get;

        let conns = vec![MockConn::new(
            0,
            vec![Frame::BulkString(Some(Bytes::from("hello")))],
        )];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::RoundRobin),
        )
        .unwrap();
        let result: Option<Bytes> = pool.execute(Get::new("key")).await.unwrap();
        assert_eq!(result, Some(Bytes::from("hello")));
    }

    #[tokio::test]
    async fn pool_propagates_errors() {
        use redis_tower_commands::Get;

        let conns = vec![MockConn::new(
            0,
            vec![Frame::Error(Bytes::from("ERR something went wrong"))],
        )];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::RoundRobin),
        )
        .unwrap();
        let result = pool.execute(Get::new("key")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pool_least_connections_prefers_idle() {
        use redis_tower_commands::Ping;

        // Connection 0 has 0 inflight, connection 1 has 0 inflight.
        // With LeastConnections, sequential calls should still distribute
        // since inflight is decremented after each completes.
        let conns = vec![
            MockConn::new(
                0,
                (0..10)
                    .map(|_| Frame::SimpleString(Bytes::from("PONG")))
                    .collect(),
            ),
            MockConn::new(
                1,
                (0..10)
                    .map(|_| Frame::SimpleString(Bytes::from("PONG")))
                    .collect(),
            ),
        ];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::LeastConnections),
        )
        .unwrap();

        // Sequential calls -- all inflight counts are 0 after each completes,
        // so least-connections falls back to picking index 0 each time.
        for _ in 0..4 {
            let _: String = pool.execute(Ping::new()).await.unwrap();
        }

        let c0 = pool.inner.connections[0].lock().await;
        let c1 = pool.inner.connections[1].lock().await;
        // In sequential mode, connection 0 always has the lowest (tied) count,
        // so it gets all calls.
        assert_eq!(c0.calls(), 4);
        assert_eq!(c1.calls(), 0);
    }

    #[tokio::test]
    async fn pool_least_connections_inflight_incremented_by_next_index() {
        // Verify that next_index() atomically increments the inflight counter
        // so concurrent callers cannot all pick the same connection.
        let conns = vec![
            MockConn::new(
                0,
                (0..10)
                    .map(|_| Frame::SimpleString(Bytes::from("PONG")))
                    .collect(),
            ),
            MockConn::new(
                1,
                (0..10)
                    .map(|_| Frame::SimpleString(Bytes::from("PONG")))
                    .collect(),
            ),
        ];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::LeastConnections),
        )
        .unwrap();

        // Both start at 0. First next_index() picks 0 and increments it.
        let reservation0 = pool
            .next_index(AcquisitionBudget::new(pool.inner.acquisition_timeout, None))
            .await
            .unwrap();
        assert_eq!(reservation0.index(), 0);
        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Acquire),
            1
        );
        assert_eq!(
            pool.inner.connections[1].inflight.load(Ordering::Acquire),
            0
        );

        // Second call should now pick connection 1 (inflight 0 < 1).
        let reservation1 = pool
            .next_index(AcquisitionBudget::new(pool.inner.acquisition_timeout, None))
            .await
            .unwrap();
        assert_eq!(reservation1.index(), 1);
        assert_eq!(
            pool.inner.connections[1].inflight.load(Ordering::Acquire),
            1
        );

        drop((reservation0, reservation1));
        assert_eq!(pool.stats().total_inflight, 0);
    }

    #[tokio::test]
    async fn pool_inflight_counters_are_zero_after_completion() {
        use redis_tower_commands::Ping;

        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::LeastConnections),
        )
        .unwrap();
        let _: String = pool.execute(Ping::new()).await.unwrap();

        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn pool_health_check_config() {
        let config = PoolConfig::default().health_check_interval(Duration::from_secs(30));
        assert_eq!(config.health_check_interval, Some(Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn pool_health_check_pings_stale_connection() {
        use redis_tower_commands::Ping;

        // Provide 2 PONG responses: one for the health check PING, one for the actual command.
        let conns = vec![MockConn::new(
            0,
            vec![
                Frame::SimpleString(Bytes::from("PONG")),
                Frame::SimpleString(Bytes::from("PONG")),
            ],
        )];

        // Use a very short health check interval (1 ms) so it always triggers.
        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default()
                .dispatch(DispatchStrategy::RoundRobin)
                .health_check_interval(Duration::from_millis(1)),
        )
        .unwrap();

        // Set last_used to 0 (epoch) so the connection appears stale.
        pool.inner.connections[0]
            .last_used
            .store(0, Ordering::Release);

        let _: String = pool.execute(Ping::new()).await.unwrap();

        // The connection should have received 2 calls: the health check PING + the actual PING.
        let c0 = pool.inner.connections[0].lock().await;
        assert_eq!(c0.calls(), 2);
    }

    #[tokio::test]
    async fn pool_health_check_skips_fresh_connection() {
        use redis_tower_commands::Ping;

        // Only provide 1 PONG response -- health check should NOT trigger.
        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];

        // Use a very long health check interval so it never triggers.
        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default()
                .dispatch(DispatchStrategy::RoundRobin)
                .health_check_interval(Duration::from_secs(3600)),
        )
        .unwrap();

        let _: String = pool.execute(Ping::new()).await.unwrap();

        // Only 1 call -- no health check PING was sent.
        let c0 = pool.inner.connections[0].lock().await;
        assert_eq!(c0.calls(), 1);
    }

    #[tokio::test]
    async fn pool_no_health_check_when_disabled() {
        use redis_tower_commands::Ping;

        // Only 1 PONG response available.
        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];

        // No health check interval set (default).
        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();

        // Set last_used to 0 so connection appears stale.
        pool.inner.connections[0]
            .last_used
            .store(0, Ordering::Release);

        let _: String = pool.execute(Ping::new()).await.unwrap();

        // Only 1 call -- health check is disabled.
        let c0 = pool.inner.connections[0].lock().await;
        assert_eq!(c0.calls(), 1);
    }

    /// Verify that a dead connection slot is replaced by the factory after a
    /// failed health-check PING (issue #339).
    ///
    /// Sequence:
    ///   1. Pool is built via `connect_with_factory`; the initial connection
    ///      is a MockConn that returns an error for its first call (the health-
    ///      check PING will fail).
    ///   2. The factory supplies a fresh MockConn with a PONG response.
    ///   3. `execute(Ping::new())` is called; the health check triggers (last_used
    ///      is set to 0 so the slot appears stale).
    ///   4. The dead PING fails → factory creates a replacement → command
    ///      succeeds against the fresh connection.
    #[tokio::test]
    async fn pool_health_check_dead_connection_replaced() {
        use redis_tower_commands::Ping;

        let recorder = Arc::new(CountingRecorder::default());
        // The factory serves two connections in order:
        //   slot 0 (initial): dead — first call returns an error (simulates a
        //     stale/closed connection whose health-check PING will fail).
        //   replacement: healthy — returns PONG for the actual command.
        let factory = MockFactory::new(vec![
            MockConn::new(0, vec![Frame::Error(Bytes::from("ERR connection closed"))]),
            MockConn::new(1, vec![Frame::SimpleString(Bytes::from("PONG"))]),
        ]);

        let mut pool = ConnectionPool::connect_with_factory(
            PoolConfig::default()
                .name("write-pool")
                .size(1)
                .health_check_interval(Duration::from_millis(1))
                .metrics_recorder(recorder.clone()),
            factory,
        )
        .await
        .unwrap();

        // Make the connection appear stale so the health check triggers.
        pool.inner.connections[0]
            .last_used
            .store(0, Ordering::Release);

        // The execute call should:
        //  1. Trigger the health check (stale threshold exceeded).
        //  2. The PING on the dead connection returns an error.
        //  3. The factory creates the fresh connection and replaces the slot.
        //  4. The actual Ping command is sent on the fresh connection and succeeds.
        let result: String = RedisExecutor::execute(&mut pool, Ping::new())
            .await
            .unwrap();
        assert_eq!(result, "PONG");
        assert_eq!(recorder.health_check_failures.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.connection_replacements.load(Ordering::Relaxed), 1);
    }

    /// Verify that when a health check PING fails and no factory is available,
    /// the error is returned to the caller (original behaviour preserved).
    #[tokio::test]
    async fn pool_health_check_dead_no_factory_returns_error() {
        use redis_tower_commands::Ping;

        let recorder = Arc::new(CountingRecorder::default());
        let dead_conn = MockConn::new(0, vec![Frame::Error(Bytes::from("ERR connection closed"))]);

        // No factory — use from_connections.
        let pool = ConnectionPool::from_connections(
            vec![dead_conn],
            PoolConfig::default()
                .dispatch(DispatchStrategy::RoundRobin)
                .health_check_interval(Duration::from_millis(1))
                .metrics_recorder(recorder.clone()),
        )
        .unwrap();

        pool.inner.connections[0]
            .last_used
            .store(0, Ordering::Release);

        let result = pool.execute(Ping::new()).await;
        assert!(result.is_err(), "expected error when no factory present");
        assert_eq!(recorder.health_check_failures.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.connection_replacements.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn pool_failed_replacement_is_not_recorded() {
        use redis_tower_commands::Ping;

        let recorder = Arc::new(CountingRecorder::default());
        // The sole factory connection is installed initially, leaving no fresh
        // connection available when its lazy health check subsequently fails.
        let factory = MockFactory::new(vec![MockConn::new(
            0,
            vec![Frame::Error(Bytes::from("ERR connection closed"))],
        )]);
        let pool = ConnectionPool::connect_with_factory(
            PoolConfig::default()
                .size(1)
                .health_check_interval(Duration::from_millis(1))
                .metrics_recorder(recorder.clone()),
            factory,
        )
        .await
        .unwrap();
        pool.inner.connections[0]
            .last_used
            .store(0, Ordering::Release);

        let result: Result<String, _> = pool.execute(Ping::new()).await;

        assert!(result.is_err(), "replacement factory should be exhausted");
        assert_eq!(recorder.health_check_failures.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.connection_replacements.load(Ordering::Relaxed), 0);
    }

    /// When acquisition_timeout is set and all slots are busy,
    /// execute() returns PoolAcquisitionTimeout promptly.
    #[tokio::test]
    async fn pool_acquisition_timeout_fires_when_all_busy() {
        use redis_tower_commands::Ping;
        use std::time::Instant;

        // Single-connection pool so it's easy to saturate.
        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().acquisition_timeout(Duration::from_millis(50)),
        )
        .unwrap();

        // Hold the lock on slot 0 to simulate a busy connection.
        let _guard = pool.inner.connections[0].lock().await;

        let start = Instant::now();
        let result = pool.execute(Ping::new()).await;
        let elapsed = start.elapsed();

        assert!(
            matches!(result, Err(RedisError::PoolAcquisitionTimeout { .. })),
            "expected PoolAcquisitionTimeout, got {result:?}"
        );
        // Should return well under 1 second (timeout is 50 ms).
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn zero_acquisition_timeout_fails_fast_when_all_slots_are_busy() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().acquisition_timeout(Duration::ZERO),
        )
        .unwrap();
        let _guard = pool.inner.connections[0].lock().await;

        let result: Result<String, _> = pool.execute(Ping::new()).await;

        assert!(matches!(
            result,
            Err(RedisError::PoolAcquisitionTimeout {
                waited: Duration::ZERO,
                pool_size: 1,
            })
        ));
        assert_eq!(pool.stats().total_inflight, 0);
    }

    #[tokio::test]
    async fn zero_acquisition_timeout_still_uses_an_immediately_available_slot() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().acquisition_timeout(Duration::ZERO),
        )
        .unwrap();

        let result: String = pool.execute(Ping::new()).await.unwrap();

        assert_eq!(result, "PONG");
    }

    #[tokio::test]
    async fn request_deadline_bounds_pool_acquisition_without_static_timeout() {
        let recorder = Arc::new(CountingRecorder::default());
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default()
                .disable_acquisition_timeout()
                .metrics_recorder(recorder.clone()),
        )
        .unwrap();
        let _guard = pool.inner.connections[0].lock().await;

        let result: Result<String, _> = pool
            .execute(WithDeadline::after(Ping::new(), Duration::from_millis(25)))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(pool.stats().total_inflight, 0);
        let acquisitions = recorder.acquisitions();
        assert_eq!(acquisitions.len(), 1);
        assert!(acquisitions[0].2);
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_wins_when_lock_is_released_at_deadline() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().disable_acquisition_timeout(),
        )
        .unwrap();
        let deadline = TokioInstant::now() + Duration::from_secs(1);

        let holder_pool = pool.clone();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let guard = holder_pool.inner.connections[0].lock().await;
            locked_tx.send(()).unwrap();
            tokio::time::sleep_until(deadline).await;
            drop(guard);
        });
        locked_rx.await.unwrap();

        let caller_pool = pool.clone();
        let caller = tokio::spawn(async move {
            caller_pool
                .execute(WithDeadline::new(Ping::new(), deadline))
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        holder.await.unwrap();
        let result: Result<String, _> = caller.await.unwrap();
        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(pool.inner.connections[0].lock().await.calls(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn static_timeout_wins_when_lock_is_released_at_deadline() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().acquisition_timeout(Duration::from_secs(1)),
        )
        .unwrap();

        let holder_pool = pool.clone();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let guard = holder_pool.inner.connections[0].lock().await;
            locked_tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(guard);
        });
        locked_rx.await.unwrap();

        let caller_pool = pool.clone();
        let caller = tokio::spawn(async move { caller_pool.execute(Ping::new()).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        holder.await.unwrap();
        let result: Result<String, _> = caller.await.unwrap();
        assert!(matches!(
            result,
            Err(RedisError::PoolAcquisitionTimeout {
                waited,
                pool_size: 1,
            }) if waited == Duration::from_secs(1)
        ));
        assert_eq!(pool.inner.connections[0].lock().await.calls(), 0);
    }

    #[tokio::test]
    async fn static_pool_timeout_wins_when_earlier_than_request_deadline() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().acquisition_timeout(Duration::from_millis(20)),
        )
        .unwrap();
        let _guard = pool.inner.connections[0].lock().await;

        let result: Result<String, _> = pool
            .execute(WithDeadline::after(Ping::new(), Duration::from_secs(1)))
            .await;

        assert!(matches!(
            result,
            Err(RedisError::PoolAcquisitionTimeout { .. })
        ));
        assert_eq!(pool.stats().total_inflight, 0);
    }

    #[tokio::test]
    async fn expired_request_does_not_execute_on_an_idle_pool() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default(),
        )
        .unwrap();
        let command =
            WithDeadline::new(Ping::new(), TokioInstant::now() - Duration::from_millis(1));

        let result: Result<String, _> = pool.execute(command).await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(pool.stats().total_inflight, 0);
        assert_eq!(pool.inner.connections[0].lock().await.calls(), 0);
    }

    #[tokio::test]
    async fn one_deadline_budget_spans_pool_acquisition_and_execution() {
        use crate::{CommandTimeoutLayer, ExecutorService};
        use tower_layer::Layer;
        use tower_service::Service;

        let pool = ConnectionPool::from_connections(
            vec![DelayedConn::new(Duration::from_millis(500))],
            PoolConfig::default().disable_acquisition_timeout(),
        )
        .unwrap();

        // Occupy the only slot first, then release it after part of the
        // request's absolute budget has already been consumed.
        let holder_pool = pool.clone();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let _guard = holder_pool.inner.connections[0].lock().await;
            let _ = locked_tx.send(());
            tokio::time::sleep(Duration::from_millis(30)).await;
        });
        locked_rx.await.unwrap();

        let mut service = CommandTimeoutLayer::new(Duration::from_secs(1))
            .with_request_deadlines()
            .layer(ExecutorService::new(pool.clone()));
        let result: Result<String, _> = service
            .call(WithDeadline::after(Ping::new(), Duration::from_millis(200)))
            .await;

        holder.await.unwrap();
        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(pool.stats().total_inflight, 0);
        assert_eq!(pool.inner.connections[0].lock().await.calls(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_pooled_connection_does_not_reuse_unread_response() {
        use crate::{CommandTimeoutLayer, ExecutorService};
        use futures::{SinkExt, StreamExt};
        use redis_tower_core::{RedisConnection, RedisStream};
        use redis_tower_protocol::RespCodec;
        use tokio::sync::oneshot;
        use tokio_util::codec::Framed;
        use tower_layer::Layer;
        use tower_service::Service;

        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let (wire_tx, wire_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (late_attempt_tx, late_attempt_rx) = oneshot::channel();

        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(RedisStream::Unix(server), RespCodec::new());
            framed
                .next()
                .await
                .expect("client closed before first pooled command")
                .expect("client sent an invalid first pooled command");
            wire_tx.send(()).unwrap();

            release_rx.await.unwrap();
            let _ = framed
                .send(Frame::SimpleString(Bytes::from_static(b"LATE")))
                .await;
            late_attempt_tx.send(()).unwrap();

            match tokio::time::timeout(Duration::from_secs(1), framed.next()).await {
                Ok(None) | Ok(Some(Err(_))) => {}
                Ok(Some(Ok(frame))) => panic!("timed-out pool slot was reused: {frame:?}"),
                Err(_) => panic!("timed-out pool socket remained open"),
            }
        });

        let connection = RedisConnection::from_stream(RedisStream::Unix(client));
        let pool = ConnectionPool::from_connections(
            vec![connection],
            PoolConfig::default().disable_acquisition_timeout(),
        )
        .unwrap();
        let mut service = CommandTimeoutLayer::new(Duration::from_secs(1))
            .with_request_deadlines()
            .layer(ExecutorService::new(pool.clone()));
        let mut first =
            Box::pin(service.call(WithDeadline::after(Ping::new(), Duration::from_millis(100))));

        tokio::select! {
            result = &mut first => panic!("pooled call completed before reaching wire: {result:?}"),
            observed = wire_rx => observed.unwrap(),
        }
        assert!(matches!(first.await, Err(RedisError::CommandTimeout)));

        release_tx.send(()).unwrap();
        late_attempt_rx.await.unwrap();

        let successor: Result<String, _> = pool.execute(Ping::new()).await;
        assert!(matches!(successor, Err(RedisError::ConnectionClosed)));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn pool_records_successful_and_timed_out_acquisitions() {
        use redis_tower_commands::Ping;

        let recorder = Arc::new(CountingRecorder::default());
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default()
                .name("acquisition-test")
                .acquisition_timeout(Duration::from_millis(10))
                .metrics_recorder(recorder.clone()),
        )
        .unwrap();

        let pong: String = pool.execute(Ping::new()).await.unwrap();
        assert_eq!(pong, "PONG");

        let _guard = pool.inner.connections[0].lock().await;
        let result: Result<String, _> = pool.execute(Ping::new()).await;
        assert!(matches!(
            result,
            Err(RedisError::PoolAcquisitionTimeout { .. })
        ));

        let acquisitions = recorder.acquisitions();
        assert_eq!(acquisitions.len(), 2);
        assert_eq!(acquisitions[0].0, "acquisition-test");
        assert!(!acquisitions[0].2);
        assert_eq!(acquisitions[1].0, "acquisition-test");
        assert!(acquisitions[1].1 >= Duration::from_millis(10));
        assert!(acquisitions[1].2);
        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Acquire),
            0
        );
    }

    /// The default config now bounds the acquisition wait, so callers fail
    /// fast on a saturated pool instead of stalling forever.
    #[test]
    fn pool_default_config_bounds_acquisition_timeout() {
        let config = PoolConfig::default();
        assert_eq!(
            config.acquisition_timeout,
            Some(PoolConfig::DEFAULT_ACQUISITION_TIMEOUT),
        );
        assert_eq!(
            PoolConfig::DEFAULT_ACQUISITION_TIMEOUT,
            Duration::from_secs(5),
        );
        // disable_acquisition_timeout opts back into the unbounded wait.
        assert_eq!(
            PoolConfig::default()
                .disable_acquisition_timeout()
                .acquisition_timeout,
            None,
        );
    }

    /// With the acquisition timeout disabled, execute() blocks until the lock
    /// is available rather than timing out — the opt-in unbounded behavior.
    #[tokio::test]
    async fn pool_no_acquisition_timeout_blocks_until_available() {
        use redis_tower_commands::Ping;

        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().disable_acquisition_timeout(),
        )
        .unwrap();

        // Release the lock after a short delay on a background task.
        let pool2 = pool.clone();
        tokio::spawn(async move {
            let _guard = pool2.inner.connections[0].lock().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            // guard drops here, releasing the lock
        });

        // Give the spawned task time to acquire the lock.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // execute() should block until the background task releases, then succeed.
        let result: String = pool.execute(Ping::new()).await.unwrap();
        assert_eq!(result, "PONG");
    }

    /// Head-of-line-blocking avoidance: when the strategy-preferred connection
    /// is busy but another sits idle, execute() dispatches to the idle one via
    /// the try_lock scan instead of queuing behind the busy slot.
    #[tokio::test]
    async fn pool_avoids_head_of_line_blocking() {
        use redis_tower_commands::Ping;
        use std::time::Instant;

        let conns = vec![
            MockConn::new(0, vec![Frame::SimpleString(Bytes::from("PONG"))]),
            MockConn::new(1, vec![Frame::SimpleString(Bytes::from("PONG"))]),
        ];

        // RoundRobin with a fresh index counter: the first execute prefers
        // slot 0.
        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().dispatch(DispatchStrategy::RoundRobin),
        )
        .unwrap();

        // Simulate a long-running command holding the preferred slot (0).
        let busy = pool.inner.connections[0].lock().await;

        // execute() prefers slot 0 (busy) but should fall through to the idle
        // slot 1 and return promptly rather than waiting on the held lock.
        let start = Instant::now();
        let pong: String = pool.execute(Ping::new()).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(pong, "PONG");
        assert!(
            elapsed < Duration::from_millis(500),
            "execute should not block on the busy slot, took {elapsed:?}"
        );

        drop(busy);

        // The command ran on the idle slot 1, not the busy slot 0.
        assert_eq!(pool.inner.connections[1].lock().await.calls(), 1);
        assert_eq!(pool.inner.connections[0].lock().await.calls(), 0);
        // Inflight accounting was moved off the preferred slot and released.
        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Acquire),
            0
        );
        assert_eq!(
            pool.inner.connections[1].inflight.load(Ordering::Acquire),
            0
        );
    }

    #[tokio::test]
    async fn pool_stats_all_idle_on_fresh_pool() {
        let conns = vec![
            MockConn::new(0, vec![]),
            MockConn::new(1, vec![]),
            MockConn::new(2, vec![]),
        ];

        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();
        let stats = pool.stats();

        assert_eq!(stats.size, 3);
        assert_eq!(stats.idle_count, 3);
        assert_eq!(stats.total_inflight, 0);
        assert_eq!(stats.max_inflight, 0);
    }

    #[tokio::test]
    async fn pool_stats_size_matches_pool_size() {
        let conns = vec![MockConn::new(0, vec![]), MockConn::new(1, vec![])];

        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();

        assert_eq!(pool.stats().size, pool.size());
    }

    #[tokio::test]
    async fn pool_health_check_all_healthy() {
        use redis_tower_commands::Ping;

        // 2 connections, each with a PONG response for the health check.
        let conns = vec![
            MockConn::new(0, vec![Frame::SimpleString(Bytes::from("PONG"))]),
            MockConn::new(1, vec![Frame::SimpleString(Bytes::from("PONG"))]),
        ];

        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();
        let result = pool.health_check().await;
        assert!(result.is_ok());

        // Both connections should have received one PING call each.
        let c0 = pool.inner.connections[0].lock().await;
        let c1 = pool.inner.connections[1].lock().await;
        assert_eq!(c0.calls(), 1);
        assert_eq!(c1.calls(), 1);

        // Suppress unused import warning in test.
        let _: std::marker::PhantomData<Ping> = std::marker::PhantomData;
    }

    #[tokio::test]
    async fn pool_health_check_returns_first_error() {
        let recorder = Arc::new(CountingRecorder::default());
        // Connection 0 returns an error frame (no healthy response).
        let conns = vec![
            MockConn::new(0, vec![Frame::Error(Bytes::from("ERR connection dead"))]),
            MockConn::new(1, vec![Frame::SimpleString(Bytes::from("PONG"))]),
        ];

        let pool = ConnectionPool::from_connections(
            conns,
            PoolConfig::default().metrics_recorder(recorder.clone()),
        )
        .unwrap();
        let result = pool.health_check().await;
        assert!(result.is_err());
        assert_eq!(recorder.health_check_failures.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.connection_replacements.load(Ordering::Relaxed), 0);
    }

    /// A connection whose `execute` blocks until the test releases it, used to
    /// hold a command in-flight while `close()` is observed.
    struct BlockingConn {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl RedisExecutor for BlockingConn {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);
            async move {
                started.notify_one();
                release.notified().await;
                cmd.parse_response(Frame::SimpleString(Bytes::from("PONG")))
            }
        }
    }

    #[tokio::test]
    async fn pool_unpolled_trait_future_never_reserves_connection() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default(),
        )
        .unwrap();
        let mut executor_view = pool.clone();

        let future = RedisExecutor::execute(&mut executor_view, Ping::new());

        assert_eq!(pool.stats().total_inflight, 0);
        drop(future);
        assert_eq!(pool.stats().total_inflight, 0);
        tokio::time::timeout(Duration::from_secs(1), pool.close())
            .await
            .expect("an unpolled command future must not prevent pool close");
    }

    #[tokio::test]
    async fn pool_abort_while_waiting_releases_reservation() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().disable_acquisition_timeout(),
        )
        .unwrap();
        let connection_guard = pool.inner.connections[0].lock().await;
        let task_pool = pool.clone();
        let task = tokio::spawn(async move { task_pool.execute(Ping::new()).await });

        tokio::time::timeout(Duration::from_secs(1), async {
            while pool.stats().total_inflight == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("command should reserve a slot before waiting for its lock");
        assert_eq!(pool.stats().total_inflight, 1);

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.stats().total_inflight, 0);
        drop(connection_guard);

        tokio::time::timeout(Duration::from_secs(1), pool.close())
            .await
            .expect("an aborted acquisition must not prevent pool close");
    }

    #[tokio::test]
    async fn pool_abort_during_command_releases_reservation() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let pool = ConnectionPool::from_connections(
            vec![BlockingConn {
                started: Arc::clone(&started),
                release,
            }],
            PoolConfig::default(),
        )
        .unwrap();
        let mut task_pool = pool.clone();
        let task =
            tokio::spawn(async move { RedisExecutor::execute(&mut task_pool, Ping::new()).await });

        started.notified().await;
        assert_eq!(pool.stats().total_inflight, 1);

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.stats().total_inflight, 0);

        tokio::time::timeout(Duration::from_secs(1), pool.close())
            .await
            .expect("an aborted in-command await must not prevent pool close");
    }

    #[tokio::test]
    async fn pool_abort_after_alternate_slot_transfer_releases_reservation() {
        let alternate_started = Arc::new(tokio::sync::Notify::new());
        let pool = ConnectionPool::from_connections(
            vec![
                BlockingConn {
                    started: Arc::new(tokio::sync::Notify::new()),
                    release: Arc::new(tokio::sync::Notify::new()),
                },
                BlockingConn {
                    started: Arc::clone(&alternate_started),
                    release: Arc::new(tokio::sync::Notify::new()),
                },
            ],
            PoolConfig::default().dispatch(DispatchStrategy::RoundRobin),
        )
        .unwrap();

        // The first round-robin reservation prefers slot 0. Holding its mutex
        // forces acquisition to transfer that reservation to idle slot 1.
        let preferred_guard = pool.inner.connections[0].lock().await;
        let task_pool = pool.clone();
        let task = tokio::spawn(async move { task_pool.execute(Ping::new()).await });

        tokio::time::timeout(Duration::from_secs(1), alternate_started.notified())
            .await
            .expect("command should start on the alternate slot");
        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Acquire),
            0
        );
        assert_eq!(
            pool.inner.connections[1].inflight.load(Ordering::Acquire),
            1
        );
        assert_eq!(
            pool.inner.admission_state.load(Ordering::Acquire) & POOL_ACTIVE_MASK,
            1
        );

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Acquire),
            0
        );
        assert_eq!(
            pool.inner.connections[1].inflight.load(Ordering::Acquire),
            0
        );
        assert_eq!(
            pool.inner.admission_state.load(Ordering::Acquire) & POOL_ACTIVE_MASK,
            0
        );
        drop(preferred_guard);

        tokio::time::timeout(Duration::from_secs(1), pool.close())
            .await
            .expect("an aborted alternate-slot command must not prevent pool close");
    }

    #[tokio::test]
    async fn pool_close_rejects_new_commands() {
        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];
        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();
        assert!(!pool.is_closed());

        let executor_view = pool.clone();
        pool.close().await;

        assert!(executor_view.is_closed());

        // The inherent execute path rejects new commands.
        let result: Result<String, _> = executor_view.execute(Ping::new()).await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));

        // The RedisExecutor path rejects them too.
        let mut executor_view = executor_view;
        let result: Result<String, _> =
            RedisExecutor::execute(&mut executor_view, Ping::new()).await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn pool_close_is_prompt_when_idle() {
        let conns = vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )];
        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();
        // An idle pool drains immediately; guard against a hang with a timeout.
        tokio::time::timeout(Duration::from_secs(1), pool.close())
            .await
            .expect("close on an idle pool should return promptly");
    }

    #[tokio::test]
    async fn pool_close_drains_in_flight_command() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let conn = BlockingConn {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        };
        let pool = ConnectionPool::from_connections(vec![conn], PoolConfig::default()).unwrap();

        // Start a command and wait until it is actually in-flight.
        let cmd_pool = pool.clone();
        let cmd_task = tokio::spawn(async move { cmd_pool.execute(Ping::new()).await });
        started.notified().await;

        // close() must not return while the command is still in-flight.
        let close_task = tokio::spawn(async move { pool.close().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !close_task.is_finished(),
            "close() should block until in-flight commands drain"
        );

        // Release the command; both it and close() then complete.
        release.notify_one();
        let cmd_result: Result<String, _> = cmd_task.await.unwrap();
        assert!(cmd_result.is_ok());
        tokio::time::timeout(Duration::from_secs(1), close_task)
            .await
            .expect("close() should return once the pool drains")
            .unwrap();
    }

    #[tokio::test]
    async fn pool_close_drains_in_flight_active_probe() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let pool = ConnectionPool::from_connections(
            vec![BlockingConn {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }],
            PoolConfig::default(),
        )
        .unwrap();
        let prober = pool.spawn_health_prober(
            HealthProberConfig::default()
                .interval(Duration::from_secs(60))
                .timeout(Duration::from_secs(60)),
        );
        started.notified().await;

        let close_pool = pool.clone();
        let close_task = tokio::spawn(async move { close_pool.close().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !close_task.is_finished(),
            "close() must wait while an admitted active probe owns a connection"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), close_task)
            .await
            .expect("close should finish after the active probe completes")
            .unwrap();
        prober.shutdown().await;
    }

    #[tokio::test]
    async fn reservation_transfer_rejects_a_reaped_growth_slot() {
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(0, vec![]), MockConn::new(1, vec![])],
            PoolConfig::default(),
        )
        .unwrap();
        {
            let _topology = pool.inner.topology_write();
            pool.inner.connections[1]
                .active
                .store(false, Ordering::Release);
            *pool.inner.connections[1].connection.try_lock().unwrap() = None;
        }

        let admission = PoolAdmission::try_new(&pool.inner.admission_state).unwrap();
        let mut reservation = InflightReservation::new(&pool.inner.connections[0], 0, admission);
        assert!(
            !pool
                .inner
                .transfer_reservation_if_active(&mut reservation, 1),
            "an inactive slot must be rejected after losing the reaper race"
        );
        assert_eq!(reservation.index(), 0);
        assert_eq!(
            pool.inner.connections[0].inflight.load(Ordering::Acquire),
            1
        );
        assert_eq!(
            pool.inner.connections[1].inflight.load(Ordering::Acquire),
            0
        );
    }

    #[tokio::test]
    async fn lazy_pool_creates_nothing_until_first_command() {
        let factory = MockFactory::new(vec![MockConn::new(
            0,
            vec![Frame::SimpleString(Bytes::from("PONG"))],
        )]);
        let observed = factory.clone();
        let pool = ConnectionPool::connect_lazy(PoolConfig::default().max_size(2), factory);

        assert_eq!(pool.size(), 0);
        assert_eq!(observed.creates(), 0);

        let pong: String = pool.execute(Ping::new()).await.unwrap();
        assert_eq!(pong, "PONG");
        assert_eq!(pool.size(), 1);
        assert_eq!(observed.creates(), 1);
        assert_eq!(pool.stats().min_size, 0);
        assert_eq!(pool.stats().max_size, 2);
    }

    #[tokio::test]
    async fn lazy_pool_growth_obeys_acquisition_timeout() {
        let pool = ConnectionPool::connect_lazy(
            PoolConfig::default()
                .max_size(1)
                .acquisition_timeout(Duration::from_millis(10)),
            SlowFactory::new(Duration::from_secs(60)),
        );

        let result: Result<String, _> =
            tokio::time::timeout(Duration::from_secs(1), pool.execute(Ping::new()))
                .await
                .expect("pool acquisition timeout must cancel a slow factory");
        assert!(matches!(
            result,
            Err(RedisError::PoolAcquisitionTimeout { .. })
        ));
        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn contended_growth_obeys_command_deadline() {
        let pool = ConnectionPool::connect_with_factory(
            PoolConfig::default()
                .bounds(1, 2)
                .disable_acquisition_timeout(),
            SlowFactory::new(Duration::from_millis(50)),
        )
        .await
        .unwrap();
        let busy = pool.inner.connections[0].lock().await;

        let result: Result<String, _> = pool
            .execute(WithDeadline::after(Ping::new(), Duration::from_millis(10)))
            .await;
        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(pool.size(), 1, "timed-out growth must not activate a slot");
        drop(busy);
    }

    #[tokio::test]
    async fn cancelling_lazy_growth_releases_the_scale_lock() {
        let calls = Arc::new(AtomicUsize::new(0));
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let pool = ConnectionPool::connect_lazy(
            PoolConfig::default().max_size(1),
            CancelThenConnectFactory {
                calls: Arc::clone(&calls),
                first_entered: Arc::clone(&first_entered),
            },
        );

        let first_pool = pool.clone();
        let first = tokio::spawn(async move { first_pool.execute(Ping::new()).await });
        first_entered.notified().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let pong: String = tokio::time::timeout(Duration::from_secs(1), pool.execute(Ping::new()))
            .await
            .expect("a canceled factory must release the pool's scale lock")
            .unwrap();
        assert_eq!(pong, "PONG");
        assert_eq!(calls.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn dynamic_pool_grows_on_contention_up_to_max_size() {
        let factory = DelayedFactory::new(Duration::from_millis(30));
        let observed = factory.clone();
        let pool =
            ConnectionPool::connect_with_factory(PoolConfig::default().bounds(1, 3), factory)
                .await
                .unwrap();

        let mut tasks = Vec::new();
        for _ in 0..3 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move { pool.execute(Ping::new()).await }));
        }
        for task in tasks {
            let result: Result<String, _> = task.await.unwrap();
            assert_eq!(result.unwrap(), "PONG");
        }

        assert_eq!(pool.size(), 3);
        assert_eq!(observed.creates(), 3);
        let stats = pool.stats();
        assert_eq!((stats.min_size, stats.max_size), (1, 3));
        assert_eq!(stats.idle_count, 3);
    }

    #[tokio::test]
    async fn idle_reaping_shrinks_to_minimum_and_updates_stats_and_metrics() {
        let recorder = Arc::new(CountingRecorder::default());
        let factory = DelayedFactory::new(Duration::from_millis(20));
        let pool = ConnectionPool::connect_with_factory(
            PoolConfig::default()
                .bounds(1, 3)
                .idle_timeout(Duration::from_millis(1))
                .metrics_recorder(recorder.clone()),
            factory,
        )
        .await
        .unwrap();

        let mut tasks = Vec::new();
        for _ in 0..3 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move { pool.execute(Ping::new()).await }));
        }
        for task in tasks {
            let result: Result<String, _> = task.await.unwrap();
            result.unwrap();
        }
        assert_eq!(pool.size(), 3);

        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(pool.reap_idle_connections().await, 2);
        let stats = pool.stats();
        assert_eq!(stats.size, 1);
        assert_eq!(stats.reaped_connections, 2);
        assert_eq!(recorder.reaped_connections.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn active_ping_prober_updates_stats_metrics_and_stops_on_shutdown() {
        let recorder = Arc::new(CountingRecorder::default());
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().metrics_recorder(recorder.clone()),
        )
        .unwrap();
        assert_eq!(pool.stats().unknown_health_count, 1);

        let handle = pool
            .spawn_health_prober(HealthProberConfig::default().interval(Duration::from_secs(60)));
        tokio::time::timeout(Duration::from_secs(1), async {
            while pool.stats().healthy_count != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the immediate prober sweep should complete");

        handle.shutdown().await;
        let probes = recorder.health_probes.lock().unwrap().clone();
        assert_eq!(probes, vec![(HealthProbeKind::Ping, true, None)]);
        assert_eq!(pool.stats().unhealthy_count, 0);
    }

    #[tokio::test]
    async fn active_prober_skips_busy_slots_without_marking_them_unhealthy() {
        let recorder = Arc::new(CountingRecorder::default());
        let pool = ConnectionPool::from_connections(
            vec![MockConn::new(
                0,
                vec![Frame::SimpleString(Bytes::from("PONG"))],
            )],
            PoolConfig::default().metrics_recorder(recorder.clone()),
        )
        .unwrap();
        let busy = pool.inner.connections[0].lock().await;
        let handle = pool.spawn_health_prober(
            HealthProberConfig::default()
                .interval(Duration::from_secs(60))
                .timeout(Duration::from_millis(5)),
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.shutdown().await;
        assert_eq!(pool.stats().unknown_health_count, 1);
        assert_eq!(pool.stats().unhealthy_count, 0);
        assert!(recorder.health_probes.lock().unwrap().is_empty());
        assert_eq!(recorder.health_check_failures.load(Ordering::Acquire), 0);
        drop(busy);
    }

    #[test]
    fn role_probe_parser_accepts_primary_replica_and_sentinel() {
        let role = |name: &'static [u8]| {
            Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from_static(
                name,
            )))]))
        };
        assert_eq!(
            parse_role_frame(&role(b"master")).unwrap(),
            RedisRole::Primary
        );
        assert_eq!(
            parse_role_frame(&role(b"replica")).unwrap(),
            RedisRole::Replica
        );
        assert_eq!(
            parse_role_frame(&role(b"sentinel")).unwrap(),
            RedisRole::Sentinel
        );
    }

    #[test]
    fn replication_probe_parser_checks_link_sync_and_offset_lag() {
        let healthy = parse_replication_info(
            "role:master\r\nconnected_slaves:2\r\nmaster_repl_offset:120\r\nslave0:ip=127.0.0.1,port=6380,state=online,offset=100,lag=0\r\nslave1:ip=127.0.0.1,port=6381,state=online,offset=115,lag=0\r\n",
        )
        .unwrap();
        assert!(healthy.link_up);
        assert!(!healthy.sync_in_progress);
        assert_eq!(healthy.lag_bytes, 20);

        let no_replicas = parse_replication_info(
            "role:master\r\nconnected_slaves:0\r\nmaster_repl_offset:120\r\n",
        )
        .unwrap();
        assert!(!no_replicas.link_up);
        assert_eq!(no_replicas.lag_bytes, 0);

        assert!(
            parse_replication_info(
                "role:slave\r\nmaster_link_status:up\r\nmaster_repl_offset:120\r\nslave_repl_offset:120\r\n",
            )
            .is_err(),
            "replica-local offsets cannot establish upstream byte lag"
        );
    }

    #[tokio::test]
    async fn built_in_role_probe_enforces_expected_role() {
        let mut primary = MockConn::new(
            0,
            vec![Frame::Array(Some(vec![Frame::BulkString(Some(
                Bytes::from("master"),
            ))]))],
        );
        let result = RoleHealthProbe::new(RedisRole::Primary)
            .probe(&mut primary)
            .await
            .unwrap();
        assert!(result.healthy);

        let mut replica = MockConn::new(
            1,
            vec![Frame::Array(Some(vec![Frame::BulkString(Some(
                Bytes::from("replica"),
            ))]))],
        );
        let result = RoleHealthProbe::new(RedisRole::Primary)
            .probe(&mut replica)
            .await
            .unwrap();
        assert!(!result.healthy);
    }

    #[tokio::test]
    async fn built_in_replication_lag_probe_enforces_threshold() {
        let info = "role:master\r\nconnected_slaves:1\r\nmaster_repl_offset:120\r\nslave0:ip=127.0.0.1,port=6380,state=online,offset=100,lag=0\r\n";
        let mut connection = MockConn::new(0, vec![Frame::BulkString(Some(Bytes::from(info)))]);
        let result = ReplicationLagHealthProbe::new(19)
            .probe(&mut connection)
            .await
            .unwrap();
        assert!(!result.healthy);
        assert_eq!(result.replication_lag_bytes, Some(20));
    }
}
