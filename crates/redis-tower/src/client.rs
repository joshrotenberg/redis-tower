use std::sync::Arc;

use redis_tower_core::{Command, RedisConnection, RedisError};
use tokio::sync::Mutex;

/// Ergonomic Redis client that can be shared across tasks.
///
/// `RedisClient` wraps a [`RedisConnection`] in an `Arc<Mutex<>>` for
/// cross-task sharing. For proper backpressure in high-throughput scenarios,
/// wrap a `RedisConnection` with `tower::buffer::Buffer` instead.
///
/// For direct, exclusive access to a connection (no locking overhead),
/// use [`RedisConnection`] directly.
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
