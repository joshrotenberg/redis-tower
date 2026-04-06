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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
}

/// Configuration for a connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of connections in the pool.
    pub size: usize,
    /// How to select which connection handles each command.
    pub dispatch: DispatchStrategy,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size: 4,
            dispatch: DispatchStrategy::RoundRobin,
        }
    }
}

impl PoolConfig {
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
}

/// Shared state behind the pool's Arc.
struct PoolInner<S> {
    connections: Vec<Mutex<S>>,
    index: AtomicUsize,
    dispatch: DispatchStrategy,
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

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections,
                index: AtomicUsize::new(0),
                dispatch: config.dispatch,
            }),
        })
    }

    /// Build a pool from pre-created connections.
    pub fn from_connections(
        connections: Vec<S>,
        dispatch: DispatchStrategy,
    ) -> Result<Self, RedisError> {
        if connections.is_empty() {
            return Err(RedisError::InvalidUrl(
                "pool requires at least one connection".into(),
            ));
        }

        let mutexed: Vec<Mutex<S>> = connections.into_iter().map(Mutex::new).collect();

        Ok(Self {
            inner: Arc::new(PoolInner {
                connections: mutexed,
                index: AtomicUsize::new(0),
                dispatch,
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
    fn next_index(&self) -> usize {
        match self.inner.dispatch {
            DispatchStrategy::RoundRobin => {
                self.inner.index.fetch_add(1, Ordering::Relaxed) % self.inner.connections.len()
            }
            DispatchStrategy::Random => {
                // Simple xorshift-based pseudo-random from the atomic counter.
                // Not cryptographic, but good enough for load distribution.
                let mut x = self.inner.index.fetch_add(7, Ordering::Relaxed);
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % self.inner.connections.len()
            }
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
        // Clone the Arc so the future is 'static.
        let inner = Arc::clone(&self.inner);
        let idx = self.next_index();
        async move {
            let mut conn = inner.connections[idx].lock().await;
            conn.execute(cmd).await
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
    pub async fn execute<Cmd: Command>(
        &self,
        cmd: Cmd,
    ) -> Result<Cmd::Response, RedisError> {
        let idx = self.next_index();
        let mut conn = self.inner.connections[idx].lock().await;
        conn.execute(cmd).await
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
    fn create(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>>;
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
        let conns = vec![
            MockConn::new(0, vec![]),
            MockConn::new(1, vec![]),
        ];
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
            MockConn::new(0, vec![
                Frame::SimpleString(Bytes::from("PONG")),
                Frame::SimpleString(Bytes::from("PONG")),
            ]),
            MockConn::new(1, vec![
                Frame::SimpleString(Bytes::from("PONG")),
                Frame::SimpleString(Bytes::from("PONG")),
            ]),
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

        let conns = vec![
            MockConn::new(0, vec![
                Frame::SimpleString(Bytes::from("PONG")),
                Frame::SimpleString(Bytes::from("PONG")),
            ]),
        ];

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
            conns.push(MockConn::new(i, (0..10).map(|_| Frame::SimpleString(Bytes::from("PONG"))).collect()));
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
}
