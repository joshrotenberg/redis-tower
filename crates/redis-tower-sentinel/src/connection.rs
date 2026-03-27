//! Sentinel-managed Redis connection with automatic failover.

use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::discovery;

/// A Redis connection managed by Sentinel.
///
/// Discovers the current master via Sentinel and connects to it.
/// When a command fails with a connection error, the next call
/// rediscovers the master (which may have changed due to failover).
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelConnection;
///
/// let mut conn = SentinelConnection::connect(
///     &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
///     "mymaster",
/// ).await?;
///
/// conn.execute(Set::new("key", "value")).await?;
/// ```
pub struct SentinelConnection {
    /// Current connection to the master.
    conn: RedisConnection,
    /// Sentinel addresses for rediscovery.
    sentinel_addrs: Vec<String>,
    /// Monitored master name.
    master_name: String,
    /// Whether the connection needs rediscovery.
    needs_rediscovery: bool,
}

impl SentinelConnection {
    /// Connect to the Redis master discovered via Sentinel.
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let master_addr = discovery::discover_master(&addrs, master_name).await?;
        let conn = RedisConnection::connect(&master_addr).await?;

        Ok(Self {
            conn,
            sentinel_addrs: addrs,
            master_name: master_name.to_string(),
            needs_rediscovery: false,
        })
    }

    /// Execute a command against the current master.
    ///
    /// If the connection was marked as needing rediscovery (after a
    /// previous connection error), rediscovers the master first.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        if self.needs_rediscovery {
            self.rediscover().await?;
        }

        let result = self.conn.execute(cmd).await;
        if let Err(ref e) = result {
            if is_connection_error(e) {
                self.needs_rediscovery = true;
            }
        }
        result
    }

    /// Force rediscovery of the master and reconnect.
    pub async fn rediscover(&mut self) -> Result<(), RedisError> {
        let master_addr =
            discovery::discover_master(&self.sentinel_addrs, &self.master_name).await?;
        self.conn = RedisConnection::connect(&master_addr).await?;
        self.needs_rediscovery = false;
        Ok(())
    }

    /// Get the sentinel addresses.
    pub fn sentinel_addrs(&self) -> &[String] {
        &self.sentinel_addrs
    }

    /// Get the monitored master name.
    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// Discover current replica addresses from sentinel.
    pub async fn discover_replicas(&self) -> Result<Vec<String>, RedisError> {
        discovery::discover_replicas(&self.sentinel_addrs, &self.master_name).await
    }
}

fn is_connection_error(err: &RedisError) -> bool {
    matches!(
        err,
        RedisError::Connection(_) | RedisError::ConnectionClosed
    )
}
