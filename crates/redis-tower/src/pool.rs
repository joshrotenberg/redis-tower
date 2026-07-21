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
//! ```ignore
//! use redis_tower::pool::{ConnectionPool, PoolConfig};
//! use redis_tower::RedisConnection;
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
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use redis_tower_commands::Ping;
use redis_tower_core::{Command, RedisError};
use tokio::sync::{Mutex, MutexGuard};

use crate::executor::RedisExecutor;

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
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of connections in the pool.
    pub size: usize,
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
    pub acquisition_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size: 4,
            dispatch: DispatchStrategy::RoundRobin,
            health_check_interval: None,
            acquisition_timeout: Some(Self::DEFAULT_ACQUISITION_TIMEOUT),
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

    /// Set the pool size.
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
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
    /// [`PoolConfig::DEFAULT_ACQUISITION_TIMEOUT`].
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
}

/// Return the current epoch time in milliseconds.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Shared state behind the pool's Arc.
struct PoolInner<S> {
    connections: Vec<Mutex<S>>,
    /// Per-connection in-flight command count for LeastConnections dispatch.
    inflight: Vec<AtomicUsize>,
    /// Per-connection last-use timestamp (epoch millis).
    last_used: Vec<AtomicU64>,
    index: AtomicUsize,
    dispatch: DispatchStrategy,
    /// Health check interval in milliseconds, or 0 if disabled.
    health_check_interval_ms: u64,
    /// Acquisition timeout in nanoseconds, or 0 if unlimited.
    acquisition_timeout_ns: u64,
    /// Optional factory used to replace dead connections after a failed PING.
    factory: Option<Arc<dyn ErasedPoolFactory<Connection = S>>>,
    /// Set once [`ConnectionPool::close`] is called. Shared across clones, so a
    /// close on any clone makes every clone reject new commands.
    closed: AtomicBool,
}

/// Object-safe wrapper around [`PoolFactory`] that erases the concrete type.
///
/// `PoolFactory` cannot itself be made into a trait object because of the
/// associated type, so we use this helper to expose the same surface via
/// `dyn`.
trait ErasedPoolFactory: Send + Sync + 'static {
    type Connection: RedisExecutor + Send + 'static;
    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>>;
}

impl<F: PoolFactory> ErasedPoolFactory for F {
    type Connection = F::Connection;
    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
        PoolFactory::create(self)
    }
}

impl<S> PoolInner<S> {
    /// Acquire a connection guard, avoiding head-of-line blocking.
    ///
    /// `preferred` is the strategy-selected slot whose in-flight counter has
    /// already been incremented by [`ConnectionPool::next_index`]. If that slot
    /// is immediately lockable it is used directly. Otherwise a `try_lock` scan
    /// looks for any idle connection so the request is not forced to queue
    /// behind a long-running command on the preferred slot while another
    /// connection sits free; when a free slot is found the in-flight accounting
    /// is moved from `preferred` to it. If every slot is busy, the call awaits
    /// the preferred slot, honoring the acquisition timeout.
    ///
    /// Returns the slot index actually acquired together with its guard.
    async fn acquire(&self, preferred: usize) -> Result<(usize, MutexGuard<'_, S>), RedisError> {
        // Fast path: the preferred slot is free.
        if let Ok(guard) = self.connections[preferred].try_lock() {
            return Ok((preferred, guard));
        }

        // Head-of-line-blocking avoidance: the preferred slot is busy, so scan
        // the remaining slots for any immediately free connection before
        // committing to an await on the busy slot.
        let len = self.connections.len();
        for offset in 1..len {
            let i = (preferred + offset) % len;
            if let Ok(guard) = self.connections[i].try_lock() {
                // Move the in-flight accounting from the preferred slot to the
                // one we actually acquired.
                self.inflight[i].fetch_add(1, Ordering::Release);
                self.inflight[preferred].fetch_sub(1, Ordering::Release);
                return Ok((i, guard));
            }
        }

        // Every slot is busy. Await the preferred slot, honoring the optional
        // acquisition timeout.
        let guard = if self.acquisition_timeout_ns > 0 {
            let timeout_dur = Duration::from_nanos(self.acquisition_timeout_ns);
            match tokio::time::timeout(timeout_dur, self.connections[preferred].lock()).await {
                Ok(guard) => guard,
                Err(_elapsed) => {
                    self.inflight[preferred].fetch_sub(1, Ordering::Release);
                    return Err(RedisError::PoolAcquisitionTimeout {
                        waited: timeout_dur,
                        pool_size: len,
                    });
                }
            }
        } else {
            self.connections[preferred].lock().await
        };
        Ok((preferred, guard))
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
        assert!(config.size > 0, "pool size must be at least 1");

        let mut connections = Vec::with_capacity(config.size);
        for _ in 0..config.size {
            let conn = factory().await?;
            connections.push(Mutex::new(conn));
        }

        let now = now_millis();
        let inflight = (0..connections.len())
            .map(|_| AtomicUsize::new(0))
            .collect();
        let last_used = (0..connections.len())
            .map(|_| AtomicU64::new(now))
            .collect();
        let health_check_interval_ms = config
            .health_check_interval
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let acquisition_timeout_ns = config
            .acquisition_timeout
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections,
                inflight,
                last_used,
                index: AtomicUsize::new(0),
                dispatch: config.dispatch,
                health_check_interval_ms,
                acquisition_timeout_ns,
                factory: None,
                closed: AtomicBool::new(false),
            }),
        })
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
        assert!(config.size > 0, "pool size must be at least 1");

        let mut connections = Vec::with_capacity(config.size);
        for _ in 0..config.size {
            let conn = factory.create().await?;
            connections.push(Mutex::new(conn));
        }

        let now = now_millis();
        let inflight = (0..connections.len())
            .map(|_| AtomicUsize::new(0))
            .collect();
        let last_used = (0..connections.len())
            .map(|_| AtomicU64::new(now))
            .collect();
        let health_check_interval_ms = config
            .health_check_interval
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let acquisition_timeout_ns = config
            .acquisition_timeout
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections,
                inflight,
                last_used,
                index: AtomicUsize::new(0),
                dispatch: config.dispatch,
                health_check_interval_ms,
                acquisition_timeout_ns,
                factory: Some(Arc::new(factory)),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Build a pool from pre-created connections using the given [`PoolConfig`].
    ///
    /// The `config.size` field is ignored here because the pool size is
    /// determined by the number of connections supplied. All other config
    /// fields (`dispatch`, `health_check_interval`) are applied normally.
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

        let now = now_millis();
        let inflight = (0..connections.len())
            .map(|_| AtomicUsize::new(0))
            .collect();
        let last_used = (0..connections.len())
            .map(|_| AtomicU64::new(now))
            .collect();
        let health_check_interval_ms = config
            .health_check_interval
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let acquisition_timeout_ns = config
            .acquisition_timeout
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mutexed: Vec<Mutex<S>> = connections.into_iter().map(Mutex::new).collect();

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections: mutexed,
                inflight,
                last_used,
                index: AtomicUsize::new(0),
                dispatch: config.dispatch,
                health_check_interval_ms,
                acquisition_timeout_ns,
                factory: None,
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Returns the number of connections in the pool.
    pub fn size(&self) -> usize {
        self.inner.connections.len()
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
        for counter in &self.inner.inflight {
            let v = counter.load(Ordering::Relaxed);
            total_inflight += v;
            if v > max_inflight {
                max_inflight = v;
            }
            if v == 0 {
                idle_count += 1;
            }
        }
        PoolStats {
            size: self.inner.connections.len(),
            idle_count,
            total_inflight,
            max_inflight,
        }
    }

    /// Returns `true` once [`close`](Self::close) has been called on this pool
    /// or any of its clones.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Gracefully close the pool, draining in-flight commands.
    ///
    /// Flips a shared "closed" flag so that every clone of this pool rejects
    /// new commands with [`RedisError::ConnectionClosed`], then waits for all
    /// in-flight commands to finish before returning. Each accepted command
    /// increments a per-slot in-flight counter for its whole duration, so the
    /// pool is drained once every counter reaches zero.
    ///
    /// This is the SIGTERM drain path: stop accepting work, let outstanding
    /// commands complete, then exit. It does not itself close the underlying
    /// connections -- dropping the pool (and its last clone) releases them --
    /// but it guarantees no command is mid-flight when the connections are
    /// dropped.
    ///
    /// Because the flag is shared through the pool's `Arc`, calling `close` on
    /// one clone drains and closes the pool seen by every clone. The draining
    /// wait is best-effort with respect to a command submitted at the exact
    /// instant the flag flips; such a command may still slip through and run to
    /// completion.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // On SIGTERM: stop accepting new work and drain the pool before exit.
    /// tokio::signal::ctrl_c().await?;
    /// pool.close().await;
    /// ```
    pub async fn close(self) {
        // Signal all clones to stop accepting new commands.
        self.inner.closed.store(true, Ordering::Release);
        // Wait for in-flight commands to drain. A closed pool accepts no new
        // work, so this sum is monotonically non-increasing and reaches zero.
        loop {
            let inflight: usize = self
                .inner
                .inflight
                .iter()
                .map(|c| c.load(Ordering::Acquire))
                .sum();
            if inflight == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Select the next connection index based on dispatch strategy.
    ///
    /// This also increments the inflight counter for the chosen connection.
    /// The caller is responsible for decrementing after the command completes.
    fn next_index(&self) -> usize {
        let len = self.inner.connections.len();
        let idx = match self.inner.dispatch {
            DispatchStrategy::RoundRobin => self.inner.index.fetch_add(1, Ordering::Relaxed) % len,
            DispatchStrategy::Random => {
                // Simple xorshift-based pseudo-random from the atomic counter.
                // Not cryptographic, but good enough for load distribution.
                let mut x = self.inner.index.fetch_add(7, Ordering::Relaxed);
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % len
            }
            DispatchStrategy::LeastConnections => {
                // Find the connection with the fewest in-flight commands.
                // On ties, pick the first (effectively round-robin among tied).
                let mut min_idx = 0;
                let mut min_val = self.inner.inflight[0].load(Ordering::Acquire);
                for i in 1..len {
                    let val = self.inner.inflight[i].load(Ordering::Acquire);
                    if val < min_val {
                        min_val = val;
                        min_idx = i;
                    }
                }
                min_idx
            }
        };
        // Increment atomically with selection so concurrent callers
        // see each other's choices for LeastConnections dispatch.
        // For other strategies the counter is maintained for consistency
        // and potential observability.
        self.inner.inflight[idx].fetch_add(1, Ordering::Release);
        idx
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
        // Reject new commands on a closed pool before reserving a slot, so
        // close() can drain the in-flight counters to zero.
        let closed = inner.closed.load(Ordering::Acquire);
        let preferred = if closed { 0 } else { self.next_index() };
        async move {
            if closed {
                return Err(RedisError::ConnectionClosed);
            }
            // inflight already incremented by next_index().
            // Acquire a connection, scanning for a free slot first to avoid
            // head-of-line blocking on a busy one, then falling back to an
            // awaited (optionally timed) lock on the preferred slot.
            let (idx, mut conn) = inner.acquire(preferred).await?;

            // Lazy health check: PING if idle beyond the threshold.
            // Gate the syscall behind the interval check to avoid calling
            // SystemTime::now() on every execute when health checks are disabled.
            if inner.health_check_interval_ms > 0 {
                let last = inner.last_used[idx].load(Ordering::Acquire);
                let now = now_millis();
                if now.saturating_sub(last) >= inner.health_check_interval_ms
                    && let Err(ping_err) = conn.execute(Ping::new()).await
                {
                    // PING failed. Attempt to replace the dead slot via the factory.
                    if let Some(ref factory) = inner.factory {
                        match factory.create().await {
                            Ok(fresh) => {
                                *conn = fresh;
                                inner.last_used[idx].store(now_millis(), Ordering::Release);
                            }
                            Err(replace_err) => {
                                drop(conn);
                                inner.inflight[idx].fetch_sub(1, Ordering::Release);
                                return Err(replace_err);
                            }
                        }
                    } else {
                        drop(conn);
                        inner.inflight[idx].fetch_sub(1, Ordering::Release);
                        return Err(ping_err);
                    }
                }
            }

            let result = conn.execute(cmd).await;
            // Only update the last-used timestamp when health checks are enabled;
            // when disabled the timestamp is never read, making the store dead work.
            if inner.health_check_interval_ms > 0 {
                inner.last_used[idx].store(now_millis(), Ordering::Release);
            }
            drop(conn);
            inner.inflight[idx].fetch_sub(1, Ordering::Release);
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
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        // Reject new commands on a closed pool before reserving a slot, so
        // close() can drain the in-flight counters to zero.
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RedisError::ConnectionClosed);
        }
        let preferred = self.next_index();
        // inflight already incremented by next_index().
        // Acquire a connection, scanning for a free slot first to avoid
        // head-of-line blocking on a busy one, then falling back to an awaited
        // (optionally timed) lock on the preferred slot.
        let (idx, mut conn) = self.inner.acquire(preferred).await?;

        // Lazy health check: PING if idle beyond the threshold.
        // Gate the syscall behind the interval check to avoid calling
        // SystemTime::now() on every execute when health checks are disabled.
        if self.inner.health_check_interval_ms > 0 {
            let last = self.inner.last_used[idx].load(Ordering::Acquire);
            let now = now_millis();
            if now.saturating_sub(last) >= self.inner.health_check_interval_ms
                && let Err(ping_err) = conn.execute(Ping::new()).await
            {
                // PING failed. Attempt to replace the dead slot via the factory.
                if let Some(ref factory) = self.inner.factory {
                    match factory.create().await {
                        Ok(fresh) => {
                            *conn = fresh;
                            self.inner.last_used[idx].store(now_millis(), Ordering::Release);
                        }
                        Err(replace_err) => {
                            drop(conn);
                            self.inner.inflight[idx].fetch_sub(1, Ordering::Release);
                            return Err(replace_err);
                        }
                    }
                } else {
                    drop(conn);
                    self.inner.inflight[idx].fetch_sub(1, Ordering::Release);
                    return Err(ping_err);
                }
            }
        }

        let result = conn.execute(cmd).await;
        // Only update the last-used timestamp when health checks are enabled;
        // when disabled the timestamp is never read, making the store dead work.
        if self.inner.health_check_interval_ms > 0 {
            self.inner.last_used[idx].store(now_millis(), Ordering::Release);
        }
        drop(conn);
        self.inner.inflight[idx].fetch_sub(1, Ordering::Release);
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
        for i in 0..self.inner.connections.len() {
            let mut conn = self.inner.connections[i].lock().await;
            conn.execute(Ping::new()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_core::Frame;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

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

    /// Mock factory that hands out pre-built MockConn instances one at a time.
    struct MockFactory {
        conns: Arc<tokio::sync::Mutex<VecDeque<MockConn>>>,
    }

    impl MockFactory {
        fn new(conns: Vec<MockConn>) -> Self {
            Self {
                conns: Arc::new(tokio::sync::Mutex::new(VecDeque::from(conns))),
            }
        }
    }

    impl PoolFactory for MockFactory {
        type Connection = MockConn;

        fn create(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
            let conns = Arc::clone(&self.conns);
            Box::pin(async move {
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
        assert_eq!(config.size, 4);
        assert!(matches!(config.dispatch, DispatchStrategy::RoundRobin));
    }

    #[tokio::test]
    async fn pool_config_builder() {
        let config = PoolConfig::default()
            .size(8)
            .dispatch(DispatchStrategy::Random);
        assert_eq!(config.size, 8);
        assert!(matches!(config.dispatch, DispatchStrategy::Random));
    }

    #[tokio::test]
    async fn pool_from_connections() {
        let conns = vec![MockConn::new(0, vec![]), MockConn::new(1, vec![])];
        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();
        assert_eq!(pool.size(), 2);
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
        let idx0 = pool.next_index();
        assert_eq!(idx0, 0);
        assert_eq!(pool.inner.inflight[0].load(Ordering::Acquire), 1);
        assert_eq!(pool.inner.inflight[1].load(Ordering::Acquire), 0);

        // Second call should now pick connection 1 (inflight 0 < 1).
        let idx1 = pool.next_index();
        assert_eq!(idx1, 1);
        assert_eq!(pool.inner.inflight[1].load(Ordering::Acquire), 1);

        // Clean up the counters so other assertions don't break.
        pool.inner.inflight[0].fetch_sub(1, Ordering::Release);
        pool.inner.inflight[1].fetch_sub(1, Ordering::Release);
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

        assert_eq!(pool.inner.inflight[0].load(Ordering::Relaxed), 0);
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
        pool.inner.last_used[0].store(0, Ordering::Release);

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
        pool.inner.last_used[0].store(0, Ordering::Release);

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

        // The factory serves two connections in order:
        //   slot 0 (initial): dead — first call returns an error (simulates a
        //     stale/closed connection whose health-check PING will fail).
        //   replacement: healthy — returns PONG for the actual command.
        let factory = MockFactory::new(vec![
            MockConn::new(0, vec![Frame::Error(Bytes::from("ERR connection closed"))]),
            MockConn::new(1, vec![Frame::SimpleString(Bytes::from("PONG"))]),
        ]);

        let pool = ConnectionPool::connect_with_factory(
            PoolConfig::default()
                .size(1)
                .health_check_interval(Duration::from_millis(1)),
            factory,
        )
        .await
        .unwrap();

        // Make the connection appear stale so the health check triggers.
        pool.inner.last_used[0].store(0, Ordering::Release);

        // The execute call should:
        //  1. Trigger the health check (stale threshold exceeded).
        //  2. The PING on the dead connection returns an error.
        //  3. The factory creates the fresh connection and replaces the slot.
        //  4. The actual Ping command is sent on the fresh connection and succeeds.
        let result: String = pool.execute(Ping::new()).await.unwrap();
        assert_eq!(result, "PONG");
    }

    /// Verify that when a health check PING fails and no factory is available,
    /// the error is returned to the caller (original behaviour preserved).
    #[tokio::test]
    async fn pool_health_check_dead_no_factory_returns_error() {
        use redis_tower_commands::Ping;

        let dead_conn = MockConn::new(0, vec![Frame::Error(Bytes::from("ERR connection closed"))]);

        // No factory — use from_connections.
        let pool = ConnectionPool::from_connections(
            vec![dead_conn],
            PoolConfig::default()
                .dispatch(DispatchStrategy::RoundRobin)
                .health_check_interval(Duration::from_millis(1)),
        )
        .unwrap();

        pool.inner.last_used[0].store(0, Ordering::Release);

        let result = pool.execute(Ping::new()).await;
        assert!(result.is_err(), "expected error when no factory present");
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
        assert_eq!(pool.inner.inflight[0].load(Ordering::Acquire), 0);
        assert_eq!(pool.inner.inflight[1].load(Ordering::Acquire), 0);
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
        // Connection 0 returns an error frame (no healthy response).
        let conns = vec![
            MockConn::new(0, vec![Frame::Error(Bytes::from("ERR connection dead"))]),
            MockConn::new(1, vec![Frame::SimpleString(Bytes::from("PONG"))]),
        ];

        let pool = ConnectionPool::from_connections(conns, PoolConfig::default()).unwrap();
        let result = pool.health_check().await;
        assert!(result.is_err());
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
}
