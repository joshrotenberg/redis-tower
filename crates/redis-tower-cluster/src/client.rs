//! Shared, cloneable cluster client.

use std::sync::Arc;

use redis_tower_core::{Command, RedisError};
use tokio::sync::Mutex;

use crate::connection::{ClusterConnection, ClusterConnectionBuilder, ReadPreference};

/// A shared Redis Cluster client.
///
/// Wraps a [`ClusterConnection`] in `Arc<Mutex<>>` for cross-task sharing.
///
/// # When to use this
///
/// `ClusterClient` serializes every command through a single cluster-wide
/// mutex, so throughput does not scale with concurrency. Use this for:
///
/// - Simple single-task workloads
/// - Code that needs direct access to connection-level features like
///   `MULTI`/`EXEC`
///
/// For high-concurrency sharing across many tokio tasks, prefer
/// [`MultiplexedClusterClient`](crate::MultiplexedClusterClient), which
/// maintains per-node connections with automatic pipelining and delivers
/// ~35x higher throughput under load.
///
/// # Concurrency
///
/// `ClusterClient` is `Clone + Send + Sync`. All clones share a single
/// `Arc<Mutex<ClusterConnection>>`, serializing every command through one
/// cluster-wide lock. Throughput does not scale with concurrency beyond the
/// latency of one in-flight request. For high-concurrency workloads, use
/// [`MultiplexedClusterClient`](crate::MultiplexedClusterClient), which
/// maintains per-node auto-pipelining with no global lock.
///
/// # Example
///
/// ```ignore
/// use redis_tower_cluster::ClusterClient;
/// use redis_tower::commands::*;
///
/// let client = ClusterClient::connect("127.0.0.1:7000").await?;
///
/// let c = client.clone();
/// tokio::spawn(async move {
///     c.execute(Set::new("key", "value")).await.unwrap();
/// });
/// ```
#[derive(Clone)]
pub struct ClusterClient {
    inner: Arc<Mutex<ClusterConnection>>,
}

impl ClusterClient {
    /// Connect to a cluster with default settings.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        let conn = ClusterConnection::connect(seed_addr).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Connect with host override for Docker/proxy environments.
    pub async fn connect_with_host(
        seed_addr: &str,
        host_override: &str,
    ) -> Result<Self, RedisError> {
        let conn = ClusterConnection::connect_with_host(seed_addr, host_override).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Create from a builder for full customization.
    pub async fn from_builder(builder: ClusterConnectionBuilder) -> Result<Self, RedisError> {
        let conn = builder.connect().await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Execute a command against the cluster.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut conn = self.inner.lock().await;
        conn.execute(cmd).await
    }

    /// Refresh the cluster topology.
    pub async fn refresh_topology(&self) -> Result<(), RedisError> {
        let mut conn = self.inner.lock().await;
        conn.refresh_topology().await
    }

    /// Get the current read preference.
    pub async fn read_preference(&self) -> ReadPreference {
        let conn = self.inner.lock().await;
        conn.read_preference()
    }
}
