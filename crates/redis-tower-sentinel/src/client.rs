//! Shared sentinel-managed Redis client.

use std::sync::Arc;

use redis_tower_commands::Ping;
use redis_tower_core::{Command, RedisError};
use tokio::sync::Mutex;

use crate::connection::SentinelConnection;

/// A shared, sentinel-managed Redis client.
///
/// Wraps a [`SentinelConnection`] in `Arc<Mutex<>>` for cross-task sharing.
/// Automatically rediscovers the master on connection failure.
///
/// # Concurrency
///
/// `SentinelClient` is `Clone + Send + Sync`. All clones share the same
/// `Arc<Mutex<SentinelConnection>>`, serializing all commands through one lock.
/// For higher concurrency, use
/// [`MultiplexedSentinelClient`](crate::MultiplexedSentinelClient).
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelClient;
///
/// let client = SentinelClient::connect(
///     &["127.0.0.1:26379", "127.0.0.1:26380"],
///     "mymaster",
/// ).await?;
///
/// let c = client.clone();
/// tokio::spawn(async move {
///     c.execute(Set::new("key", "value")).await.unwrap();
/// });
/// ```
#[derive(Clone)]
pub struct SentinelClient {
    inner: Arc<Mutex<SentinelConnection>>,
}

impl SentinelClient {
    /// Connect to the master discovered via Sentinel.
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let conn = SentinelConnection::connect(sentinel_addrs, master_name).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Execute a command against the current master.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut conn = self.inner.lock().await;
        conn.execute(cmd).await
    }

    /// Force rediscovery and reconnection to the master.
    pub async fn rediscover(&self) -> Result<(), RedisError> {
        let mut conn = self.inner.lock().await;
        conn.rediscover().await
    }

    /// Send a PING to the current master.
    ///
    /// Returns `Ok(())` on success. Useful for Kubernetes readiness probes
    /// and `/health` endpoints.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        let mut conn = self.inner.lock().await;
        conn.execute(Ping::new()).await?;
        Ok(())
    }
}
