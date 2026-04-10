//! Multiplexed Redis client for high-concurrency workloads.
//!
//! [`MultiplexedClient`] batches concurrent requests from multiple tasks
//! into Redis pipelines automatically. It uses a single TCP connection
//! with a background worker, similar to redis-rs's `MultiplexedConnection`.
//!
//! # When to use
//!
//! - Many tasks issuing independent commands concurrently
//! - Read-heavy workloads (GET, HGET, etc.)
//! - Situations where connection pool overhead is undesirable
//!
//! For transactions (MULTI/EXEC) or commands that require exclusive
//! connection access, use [`RedisConnection`] directly or via
//! [`ConnectionPool`](crate::pool::ConnectionPool).
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::MultiplexedClient;
//! use redis_tower::commands::*;
//!
//! let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//!
//! // Clone and share across tasks -- all use the same connection.
//! let c = client.clone();
//! tokio::spawn(async move {
//!     c.execute(Set::new("key", "value")).await.unwrap();
//! });
//!
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! ```

use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig, AutoPipelineService};
use crate::command_adapter::CommandAdapter;
use crate::reconnect::ConnectionFactory;

/// A multiplexed Redis client that batches concurrent requests.
///
/// Wraps [`AutoPipelineService`] + [`CommandAdapter`] behind a simple API.
/// Clone-friendly: all clones share the same background worker and TCP
/// connection.
///
/// Concurrent requests from multiple tasks are batched into Redis pipelines
/// automatically. Single requests flush immediately with no batching delay.
#[derive(Clone)]
pub struct MultiplexedClient {
    inner: CommandAdapter<AutoPipelineService>,
}

impl MultiplexedClient {
    /// Connect to a Redis server at `host:port`.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect(addr).await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect using a Redis URL (`redis://`, `rediss://`, `unix://`).
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_url(url).await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect and negotiate RESP3 protocol.
    pub async fn connect_resp3(addr: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_resp3(addr).await?;
        Ok(Self::from_connection(conn))
    }

    /// Wrap an existing connection in a multiplexed client.
    pub fn from_connection(conn: RedisConnection) -> Self {
        Self::from_connection_with_config(conn, AutoPipelineConfig::default())
    }

    /// Wrap an existing connection with custom pipeline configuration.
    pub fn from_connection_with_config(conn: RedisConnection, config: AutoPipelineConfig) -> Self {
        Self {
            inner: CommandAdapter::new(AutoPipelineService::new(conn, config)),
        }
    }

    /// Build a multiplexed client backed by a [`ConnectionFactory`].
    ///
    /// Unlike [`Self::connect`] / [`Self::from_connection`], the resulting
    /// client transparently reconnects when the underlying TCP connection
    /// drops, using the provided factory to build a fresh connection with
    /// exponential backoff.
    ///
    /// The factory is also the right place to replay any per-connection
    /// session setup -- AUTH, SELECT, HELLO, READONLY. Use a
    /// [`UrlConnectionFactory`](crate::reconnect::UrlConnectionFactory) for
    /// AUTH+SELECT from a URL, or implement [`ConnectionFactory`] yourself
    /// for custom init.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    /// use redis_tower::MultiplexedClient;
    /// use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
    /// use redis_tower::reconnect::{ReconnectConfig, UrlConnectionFactory};
    ///
    /// let factory = UrlConnectionFactory::new("redis://user:pass@host:6379/0");
    /// let client = MultiplexedClient::from_factory(
    ///     factory,
    ///     AutoPipelineConfig::default(),
    ///     AutoPipelineReconnectConfig::new(
    ///         ReconnectConfig::default().base_delay(Duration::from_millis(50)),
    ///     ),
    /// ).await?;
    /// ```
    pub async fn from_factory(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
    ) -> Result<Self, RedisError> {
        let svc = AutoPipelineService::with_factory(factory, config, reconnect).await?;
        Ok(Self {
            inner: CommandAdapter::new(svc),
        })
    }

    /// Execute a command.
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
}
