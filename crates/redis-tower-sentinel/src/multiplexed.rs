//! Multiplexed sentinel-managed Redis client.
//!
//! [`MultiplexedSentinelClient`] batches concurrent requests from multiple
//! tasks into Redis pipelines automatically. It uses a single TCP connection
//! to the sentinel-discovered master, with a background worker shared across
//! all clones.
//!
//! # When to use
//!
//! - Many tasks issuing independent commands concurrently against a
//!   sentinel-managed Redis deployment
//! - Read-heavy workloads where connection pool overhead is undesirable
//! - High-concurrency scenarios where [`crate::SentinelClient`]'s mutex
//!   becomes a bottleneck
//!
//! For transactions (MULTI/EXEC) or commands requiring exclusive connection
//! access, use [`crate::SentinelConnection`] directly.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower_sentinel::MultiplexedSentinelClient;
//! use redis_tower::commands::*;
//!
//! let client = MultiplexedSentinelClient::connect(
//!     &["127.0.0.1:26379"],
//!     "mymaster",
//! ).await?;
//!
//! // Clone and share across tasks -- all share the same connection.
//! let c = client.clone();
//! tokio::spawn(async move {
//!     c.execute(Set::new("key", "value")).await.unwrap();
//! });
//!
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! ```

use redis_tower::auto_pipeline::{
    AutoPipelineConfig, AutoPipelineReconnectConfig, AutoPipelineService,
};
use redis_tower::command_adapter::CommandAdapter;
use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::discovery;

/// A multiplexed sentinel-managed Redis client for high-concurrency workloads.
///
/// Wraps [`AutoPipelineService`] + [`CommandAdapter`] with sentinel discovery
/// for automatic master resolution. Clone-friendly: all clones share the same
/// background worker and TCP connection.
///
/// Concurrent requests from multiple tasks are batched into Redis pipelines
/// automatically. Single requests flush immediately with no batching delay.
#[derive(Clone)]
pub struct MultiplexedSentinelClient {
    inner: CommandAdapter<AutoPipelineService>,
}

impl MultiplexedSentinelClient {
    /// Connect to the sentinel-discovered master.
    ///
    /// Does not reconnect automatically on connection failure. For
    /// production use with failover support, use [`Self::connect_with_reconnect`].
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
            inner: CommandAdapter::new(AutoPipelineService::new(
                conn,
                AutoPipelineConfig::default(),
            )),
        })
    }

    /// Connect with automatic reconnection via sentinel discovery.
    ///
    /// On connection failure, the factory re-queries sentinel to find
    /// the current master (which may have changed due to failover).
    pub async fn connect_with_reconnect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let name = master_name.to_string();
        let factory = move || {
            let addrs = addrs.clone();
            let name = name.clone();
            async move {
                let master_addr = discovery::discover_master(&addrs, &name).await?;
                RedisConnection::connect(&master_addr).await
            }
        };
        let svc = AutoPipelineService::with_factory(
            factory,
            AutoPipelineConfig::default(),
            AutoPipelineReconnectConfig::default(),
        )
        .await?;
        Ok(Self {
            inner: CommandAdapter::new(svc),
        })
    }

    /// Execute a command against the sentinel-managed master.
    ///
    /// If other tasks are calling execute concurrently, their commands
    /// will be batched into a single Redis pipeline for efficiency.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut svc = self.inner.clone();
        std::future::poll_fn(|cx| {
            <CommandAdapter<AutoPipelineService> as tower_service::Service<Cmd>>::poll_ready(
                &mut svc, cx,
            )
        })
        .await?;
        tower_service::Service::call(&mut svc, cmd).await
    }

    /// Gracefully shut down the multiplexed sentinel client.
    ///
    /// Signals the background worker to stop accepting new requests, then
    /// waits for all in-flight requests to complete and joins the background
    /// task. If other clones of this client are still alive, this returns
    /// immediately -- the worker continues running until the last clone shuts
    /// down or is dropped.
    ///
    /// For clean application shutdown, prefer calling `shutdown()` over
    /// simply dropping the client.
    pub async fn shutdown(self) {
        self.inner.into_inner().shutdown().await;
    }
}
