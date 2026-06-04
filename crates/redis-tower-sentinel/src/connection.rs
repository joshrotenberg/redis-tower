//! Sentinel-managed Redis connection with automatic failover.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::discovery;

/// A Redis connection managed by Sentinel.
///
/// Discovers the current master via Sentinel and connects to it.
/// When a command fails with a connection error, the connection
/// immediately rediscovers the master (which may have changed due to
/// failover). The caller should retry the command.
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
    ///
    /// On a connection error during execution (which may indicate a
    /// failover), the master is rediscovered eagerly so that the next
    /// `execute()` call connects to the new master without an additional
    /// rediscovery round-trip. The current command cannot be retried here
    /// because it has already been consumed; the caller should retry if
    /// appropriate.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        if self.needs_rediscovery {
            self.rediscover().await?;
        }

        let result = self.conn.execute(cmd).await;
        if let Err(ref e) = result
            && e.is_connection_error()
        {
            // Failover may have occurred. Eagerly rediscover the new master
            // so the next execute() call connects immediately without an
            // additional rediscovery round-trip. The current command cannot
            // be retried here because it has been consumed; the caller should
            // retry if appropriate. If rediscovery fails, fall back to the
            // deferred path so the next call tries again.
            self.needs_rediscovery = self.rediscover().await.is_err();
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

impl redis_tower::RedisExecutor for SentinelConnection {
    fn execute<Cmd: redis_tower_core::Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl std::future::Future<Output = Result<Cmd::Response, redis_tower_core::RedisError>> + Send
    {
        SentinelConnection::execute(self, cmd)
    }
}

impl<Cmd: Command + 'static> tower_service::Service<Cmd> for SentinelConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <RedisConnection as tower_service::Service<Cmd>>::poll_ready(&mut self.conn, cx)
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        <RedisConnection as tower_service::Service<Cmd>>::call(&mut self.conn, cmd)
    }
}
