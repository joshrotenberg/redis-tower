//! Batteries-included resilient Redis client.
//!
//! [`ResilientRedisClient`] combines shared access (`Arc<Mutex<>>`) with
//! automatic reconnection on connection loss. It is the recommended
//! client for long-running applications that need to survive transient
//! network failures without manual intervention.

use std::sync::Arc;

use redis_tower_core::{Command, RedisConnection, RedisError};
use tokio::sync::Mutex;

use crate::reconnect::{
    AddrConnectionFactory, ConnectionFactory, ReconnectConfig, UrlConnectionFactory,
};

/// A shared, auto-reconnecting Redis client.
///
/// Wraps a [`RedisConnection`] with automatic reconnection on connection
/// loss. Uses `Arc<Mutex<>>` for cross-task sharing.
///
/// # Concurrency
///
/// `ResilientRedisClient` is `Clone + Send + Sync`. All clones share the same
/// `Arc<Mutex<RedisConnection>>`, serializing commands one at a time.
/// Reconnection is triggered only when a command fails with a connection error
/// (`is_connection_error()` returns true); non-connection errors (WRONGTYPE,
/// etc.) are returned to the caller without triggering reconnection. After
/// `max_retries` reconnect attempts are exhausted, the error propagates to the
/// caller; the client is not permanently broken and will attempt reconnection
/// on the next command.
///
/// # Example
///
/// ```ignore
/// use redis_tower::ResilientRedisClient;
/// use redis_tower::commands::*;
///
/// let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
///
/// let c = client.clone();
/// tokio::spawn(async move {
///     c.execute(Set::new("key", "value")).await.unwrap();
/// });
/// ```
#[derive(Clone)]
pub struct ResilientRedisClient {
    conn: Arc<Mutex<RedisConnection>>,
    factory: Arc<dyn ConnectionFactory>,
    config: ReconnectConfig,
}

impl ResilientRedisClient {
    /// Connect to Redis with default reconnection settings.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::with_config(AddrConnectionFactory::new(addr), ReconnectConfig::default()).await
    }

    /// Connect via a Redis URL with default reconnection settings.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        Self::with_config(UrlConnectionFactory::new(url), ReconnectConfig::default()).await
    }

    /// Connect with a custom factory and reconnection config.
    pub async fn with_config(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        let factory: Arc<dyn ConnectionFactory> = Arc::new(factory);
        let conn = factory.connect().await?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            factory,
            config,
        })
    }

    /// Execute a command, reconnecting if the connection is lost.
    ///
    /// # Retry Safety
    ///
    /// If the command fails with a connection error, the connection is
    /// reconnected automatically, but the error is returned to the caller.
    /// If the caller retries the command, be aware that the original command
    /// may have been executed by Redis before the connection dropped.
    /// Only retry commands that are [`Command::idempotent`].
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut conn = self.conn.lock().await;
        let result = conn.execute(cmd).await;

        if let Err(ref e) = result
            && e.is_connection_error()
        {
            drop(conn);
            self.reconnect().await;
        }

        result
    }

    /// Attempt to reconnect with exponential backoff.
    async fn reconnect(&self) {
        let max = self.config.max_retries.unwrap_or(usize::MAX);
        for attempt in 0..=max {
            let delay = self.config.delay_for_attempt(attempt);
            tracing::warn!(attempt, delay = ?delay, "redis: reconnecting");
            tokio::time::sleep(delay).await;

            match self.factory.connect().await {
                Ok(new_conn) => {
                    let mut guard = self.conn.lock().await;
                    *guard = new_conn;
                    tracing::info!(attempt, "redis: reconnected successfully");
                    return;
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "redis: reconnect attempt failed");
                    continue;
                }
            }
        }
    }
}
