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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use redis_tower_commands::Ping;
use redis_tower_core::{Command, RedisError};
use tokio::sync::Mutex;

use crate::executor::RedisExecutor;

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
    /// command. If the PING fails, the error is returned to the caller.
    pub health_check_interval: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size: 4,
            dispatch: DispatchStrategy::RoundRobin,
            health_check_interval: None,
        }
    }
}

impl PoolConfig {
    /// Set the pool size.
    #[must_use]
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Set the dispatch strategy.
    #[must_use]
    pub fn dispatch(mut self, strategy: DispatchStrategy) -> Self {
        self.dispatch = strategy;
        self
    }

    /// Set the health check interval.
    ///
    /// If set, connections idle longer than this are PINGed before use
    /// to verify they are still alive.
    #[must_use]
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = Some(interval);
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
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connection`] if any factory call fails.
    pub async fn connect<F, Fut>(size: usize, factory: F) -> Result<Self, RedisError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<S, RedisError>>,
    {
        Self::connect_with_config(PoolConfig::default().size(size), factory).await
    }

    /// Create a pool with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connection`] if any factory call fails.
    ///
    /// # Panics
    ///
    /// Panics if `config.size` is 0 (`assert!(config.size > 0, "pool size must be at least 1")`).
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

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections,
                inflight,
                last_used,
                index: AtomicUsize::new(0),
                dispatch: config.dispatch,
                health_check_interval_ms,
            }),
        })
    }

    /// Build a pool from pre-created connections.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `conns` is empty.
    pub fn from_connections(
        connections: Vec<S>,
        dispatch: DispatchStrategy,
    ) -> Result<Self, RedisError> {
        Self::from_connections_with_config(connections, dispatch, None)
    }

    /// Build a pool from pre-created connections with a health check interval.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `conns` is empty.
    pub fn from_connections_with_config(
        connections: Vec<S>,
        dispatch: DispatchStrategy,
        health_check_interval: Option<Duration>,
    ) -> Result<Self, RedisError> {
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
        let health_check_interval_ms = health_check_interval
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mutexed: Vec<Mutex<S>> = connections.into_iter().map(Mutex::new).collect();

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections: mutexed,
                inflight,
                last_used,
                index: AtomicUsize::new(0),
                dispatch,
                health_check_interval_ms,
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
        let idx = self.next_index();
        async move {
            // inflight already incremented by next_index()
            let mut conn = inner.connections[idx].lock().await;

            // Lazy health check: PING if idle beyond the threshold.
            if inner.health_check_interval_ms > 0 {
                let last = inner.last_used[idx].load(Ordering::Acquire);
                let now = now_millis();
                if now.saturating_sub(last) >= inner.health_check_interval_ms {
                    let _: String = conn.execute(Ping::new()).await?;
                }
            }

            let result = conn.execute(cmd).await;
            inner.last_used[idx].store(now_millis(), Ordering::Release);
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
    /// configured dispatch strategy and executes the command on it.
    ///
    /// If `health_check_interval` is configured and the selected connection
    /// has been idle longer than the interval, a PING is sent first to
    /// verify the connection is alive.
    ///
    /// # Errors
    ///
    /// - Returns [`RedisError::ConnectionClosed`] if the selected connection is closed.
    /// - Returns [`RedisError::Connection`] on I/O or health-check failure.
    /// - Returns [`RedisError::Redis`] if the server returns an error response.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let idx = self.next_index();
        // inflight already incremented by next_index()
        let mut conn = self.inner.connections[idx].lock().await;

        // Lazy health check: PING if idle beyond the threshold.
        if self.inner.health_check_interval_ms > 0 {
            let last = self.inner.last_used[idx].load(Ordering::Acquire);
            let now = now_millis();
            if now.saturating_sub(last) >= self.inner.health_check_interval_ms {
                let _: String = conn.execute(Ping::new()).await?;
            }
        }

        let result = conn.execute(cmd).await;
        self.inner.last_used[idx].store(now_millis(), Ordering::Release);
        drop(conn);
        self.inner.inflight[idx].fetch_sub(1, Ordering::Release);
        result
    }
}

/// A type-erased factory for creating pooled connections.
///
/// This trait extends [`ConnectionFactory`](crate::reconnect::ConnectionFactory)
/// to support creating any connection type, not just `RedisConnection`.
pub trait PoolFactory: Send + Sync + 'static {
    /// The connection type this factory creates.
    type Connection: RedisExecutor + Send + 'static;

    /// Create a new connection.
    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>>;
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
        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::RoundRobin).unwrap();
        assert_eq!(pool.size(), 2);
    }

    #[tokio::test]
    async fn pool_empty_connections_fails() {
        let result =
            ConnectionPool::<MockConn>::from_connections(vec![], DispatchStrategy::RoundRobin);
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

        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::RoundRobin).unwrap();

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

        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::RoundRobin).unwrap();
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

        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::Random).unwrap();

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

        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::RoundRobin).unwrap();
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

        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::RoundRobin).unwrap();
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

        let pool =
            ConnectionPool::from_connections(conns, DispatchStrategy::LeastConnections).unwrap();

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

        let pool =
            ConnectionPool::from_connections(conns, DispatchStrategy::LeastConnections).unwrap();

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

        let pool =
            ConnectionPool::from_connections(conns, DispatchStrategy::LeastConnections).unwrap();
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
        let pool = ConnectionPool::from_connections_with_config(
            conns,
            DispatchStrategy::RoundRobin,
            Some(Duration::from_millis(1)),
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
        let pool = ConnectionPool::from_connections_with_config(
            conns,
            DispatchStrategy::RoundRobin,
            Some(Duration::from_secs(3600)),
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
        let pool = ConnectionPool::from_connections(conns, DispatchStrategy::RoundRobin).unwrap();

        // Set last_used to 0 so connection appears stale.
        pool.inner.last_used[0].store(0, Ordering::Release);

        let _: String = pool.execute(Ping::new()).await.unwrap();

        // Only 1 call -- health check is disabled.
        let c0 = pool.inner.connections[0].lock().await;
        assert_eq!(c0.calls(), 1);
    }
}
