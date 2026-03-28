use std::sync::Arc;

use redis_tower_core::{Command, RedisConnection, RedisError};
use tokio::sync::Mutex;

/// Simple shared Redis client for basic use cases.
///
/// Wraps a [`RedisConnection`] in `Arc<Mutex<>>` for cross-task sharing.
/// Good for quick prototyping and simple applications.
///
/// # When to use which client type
///
/// - **`RedisClient`** -- Simplest. Shared via Clone. No reconnection.
///   Good for scripts and simple applications.
/// - **`ResilientRedisClient`** -- Shared + auto-reconnection. Good for
///   long-running applications that need to survive connection drops.
/// - **`CommandAdapter<CacheService<ReconnectService>>`** -- Full Tower
///   composition. Best for production services that need middleware
///   (caching, timeouts, metrics, reconnection).
/// - **`RedisConnection`** -- Direct, exclusive access. Implements
///   `tower::Service`. Use with `tower::buffer::Buffer` for sharing.
#[derive(Clone)]
pub struct RedisClient {
    inner: Arc<Mutex<RedisConnection>>,
}

impl RedisClient {
    /// Connect to a Redis server.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect(addr).await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect using a Redis URL.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_url(url).await?;
        Ok(Self::from_connection(conn))
    }

    /// Wrap an existing connection in a shared client.
    pub fn from_connection(conn: RedisConnection) -> Self {
        Self {
            inner: Arc::new(Mutex::new(conn)),
        }
    }

    /// Execute a command against the Redis server.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let conn = self.inner.lock().await;
        conn.execute(cmd).await
    }
}
