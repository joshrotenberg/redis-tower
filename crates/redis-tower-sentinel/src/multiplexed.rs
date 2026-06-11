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
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use tower_service::Service;

use crate::discovery;

/// A multiplexed sentinel-managed Redis client for high-concurrency workloads.
///
/// Wraps [`AutoPipelineService`] + [`CommandAdapter`] with sentinel discovery
/// for automatic master resolution. Clone-friendly: all clones share the same
/// background worker and TCP connection.
///
/// Concurrent requests from multiple tasks are batched into Redis pipelines
/// automatically. Single requests flush immediately with no batching delay.
///
/// # Middleware
///
/// The type parameter `S` is the inner Frame-level [`Service`] and defaults to
/// [`AutoPipelineService`]. Use [`from_layered`](Self::from_layered) to wrap the
/// sentinel-managed client in a Tower middleware stack (circuit breaker,
/// timeout, retry).
#[derive(Clone)]
pub struct MultiplexedSentinelClient<S = AutoPipelineService> {
    inner: CommandAdapter<S>,
}

impl MultiplexedSentinelClient<AutoPipelineService> {
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

impl<S> MultiplexedSentinelClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone,
    S::Future: Send + 'static,
{
    /// Build a sentinel client from a layered Frame-level [`Service`].
    ///
    /// The middleware injection point: wrap [`AutoPipelineService`] (or any
    /// `Service<Frame, Response = Frame, Error = RedisError>`) in a Tower stack
    /// and hand it here. Every [`execute`](Self::execute) then flows through the
    /// middleware. The caller is responsible for sentinel discovery when
    /// building the inner service; for the built-in discovery use
    /// [`connect`](Self::connect) or
    /// [`connect_with_reconnect`](Self::connect_with_reconnect).
    pub fn from_layered(service: S) -> Self {
        Self {
            inner: CommandAdapter::new(service),
        }
    }

    /// Execute a command against the sentinel-managed master.
    ///
    /// If other tasks are calling execute concurrently, their commands
    /// will be batched into a single Redis pipeline for efficiency.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut svc = self.inner.clone();
        std::future::poll_fn(|cx| <CommandAdapter<S> as Service<Cmd>>::poll_ready(&mut svc, cx))
            .await?;
        Service::call(&mut svc, cmd).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_commands::Get;
    use std::task::{Context, Poll};

    #[derive(Clone)]
    struct MockFrameService {
        reply: Frame,
    }

    impl Service<Frame> for MockFrameService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            std::future::ready(Ok(self.reply.clone()))
        }
    }

    #[tokio::test]
    async fn from_layered_routes_execute_through_injected_service() {
        let inner = MockFrameService {
            reply: Frame::BulkString(Some(Bytes::from("layered"))),
        };
        let client = MultiplexedSentinelClient::from_layered(inner);

        let client2 = client.clone();
        let val: Option<Bytes> = client2.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("layered")));
    }
}
