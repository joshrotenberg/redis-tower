//! High-level Sentinel client API

use super::config::SentinelConfig;
use super::discovery::discover_replicas;
use super::make_service::SentinelMakeService;
use crate::client::RedisConnection;
use crate::types::RedisError;
use std::sync::Arc;
use tower::reconnect::Reconnect;
use tracing::debug;

/// A Redis client that uses Sentinel for automatic master discovery and failover
///
/// This client provides two key capabilities:
/// 1. Automatic master discovery with failover via Tower's `Reconnect` middleware
/// 2. Optional read-from-replica support with load balancing via Tower's `Balance` middleware
///
/// # Example
///
/// ```no_run
/// use redis_tower::sentinel::{SentinelConfig, SentinelClient};
/// use redis_tower::commands::Get;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = SentinelConfig::builder()
///     .sentinel_node("sentinel1", 26379)
///     .sentinel_node("sentinel2", 26379)
///     .sentinel_node("sentinel3", 26379)
///     .master_name("mymaster")
///     .build()?;
///
/// let client = SentinelClient::new(config);
///
/// // Writes go to master (with automatic failover)
/// let mut master = client.master();
/// let value: Option<bytes::Bytes> = master.call(Get::new("key")).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SentinelClient {
    config: Arc<SentinelConfig>,
}

impl SentinelClient {
    /// Create a new SentinelClient with the given configuration
    pub fn new(config: SentinelConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Get a service for the master with automatic failover
    ///
    /// This returns a Tower `Reconnect` service that will automatically
    /// reconnect to the new master if a failover occurs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower::sentinel::{SentinelConfig, SentinelClient};
    /// # use redis_tower::commands::{Set, Get};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = SentinelConfig::builder()
    /// #     .sentinel_node("localhost", 26379)
    /// #     .master_name("mymaster")
    /// #     .build()?;
    /// let client = SentinelClient::new(config);
    /// let mut master = client.master();
    ///
    /// // Execute commands - automatic reconnect on failover
    /// master.call(Set::new("key", "value")).await?;
    /// let value: Option<bytes::Bytes> = master.call(Get::new("key")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn master(&self) -> Reconnect<SentinelMakeService, ()> {
        let make_service = SentinelMakeService::new((*self.config).clone());
        Reconnect::new(make_service, ())
    }

    /// Discover all available replicas
    ///
    /// This queries the Sentinel nodes to find all healthy replicas.
    /// The returned addresses can be used to create replica connections.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower::sentinel::{SentinelConfig, SentinelClient};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = SentinelConfig::builder()
    /// #     .sentinel_node("localhost", 26379)
    /// #     .master_name("mymaster")
    /// #     .build()?;
    /// let client = SentinelClient::new(config);
    ///
    /// // Discover all replicas
    /// let replicas = client.discover_replicas().await?;
    /// println!("Found {} replicas", replicas.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn discover_replicas(&self) -> Result<Vec<(String, u16)>, RedisError> {
        if !self.config.read_from_replicas {
            debug!("Read-from-replicas not enabled");
            return Ok(Vec::new());
        }

        discover_replicas(&self.config).await
    }

    /// Create a connection to a specific replica
    ///
    /// This creates a direct connection to a replica at the given address.
    /// For load-balanced reads across multiple replicas, consider using
    /// a load balancing layer with multiple replica connections.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower::sentinel::{SentinelConfig, SentinelClient};
    /// # use redis_tower::commands::Get;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = SentinelConfig::builder()
    /// #     .sentinel_node("localhost", 26379)
    /// #     .master_name("mymaster")
    /// #     .read_from_replicas(true)
    /// #     .build()?;
    /// let client = SentinelClient::new(config);
    ///
    /// let replicas = client.discover_replicas().await?;
    /// if let Some((host, port)) = replicas.first() {
    ///     let mut replica = client.replica(host, *port).await?;
    ///     let value: Option<bytes::Bytes> = replica.call(Get::new("key")).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replica(&self, host: &str, port: u16) -> Result<RedisConnection, RedisError> {
        let addr = format!("{}:{}", host, port);
        let conn = RedisConnection::connect(&addr).await?;

        // Authenticate if credentials provided
        if let Some(username) = &self.config.redis_username {
            if let Some(password) = &self.config.redis_password {
                use crate::commands::AuthAcl;
                conn.execute(AuthAcl::new(username, password)).await?;
            }
        } else if let Some(password) = &self.config.redis_password {
            use crate::commands::Auth;
            conn.execute(Auth::new(password)).await?;
        }

        // Enable READONLY mode for replicas
        use crate::commands::ReadOnly;
        conn.execute(ReadOnly).await?;

        Ok(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentinel_client_creation() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .build()
            .unwrap();

        let _client = SentinelClient::new(config);
    }

    #[tokio::test]
    #[ignore] // Requires running Sentinel
    async fn test_discover_replicas() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .read_from_replicas(true)
            .build()
            .unwrap();

        let client = SentinelClient::new(config);
        let replicas = client.discover_replicas().await;

        assert!(replicas.is_ok());
    }
}
