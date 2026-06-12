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

use std::future::Future;

use redis_tower_commands::Ping;
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tower_service::Service;

use crate::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig, AutoPipelineService};
use crate::command_adapter::CommandAdapter;
use crate::reconnect::ConnectionFactory;
use crate::transaction::TransactionExecutor;

/// A multiplexed Redis client that batches concurrent requests.
///
/// Wraps [`AutoPipelineService`] + [`CommandAdapter`] behind a simple API.
/// Clone-friendly: all clones share the same background worker and TCP
/// connection.
///
/// Concurrent requests from multiple tasks are batched into Redis pipelines
/// automatically. Single requests flush immediately with no batching delay.
///
/// # Concurrency
///
/// `MultiplexedClient` is `Clone + Send + Sync`. All clones share the same
/// background worker task and a single TCP connection. Concurrent callers from
/// any number of tasks are safe; their commands are automatically batched into
/// pipelines. For workloads requiring exclusive connection access (transactions,
/// blocking commands), use [`RedisConnection`] directly or
/// [`ConnectionPool`](crate::pool::ConnectionPool).
///
/// # Middleware
///
/// The type parameter `S` is the inner Frame-level [`Service`] and defaults to
/// [`AutoPipelineService`]. To wrap the client in Tower middleware (circuit
/// breakers, timeouts, retries), build a `Service<Frame>` stack and pass it to
/// [`from_layered`](Self::from_layered):
///
/// ```ignore
/// use std::time::Duration;
/// use tower::ServiceBuilder;
/// use redis_tower::{AutoPipelineService, AutoPipelineConfig, CommandTimeoutLayer,
///     MultiplexedClient, RedisConnection};
///
/// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let inner = ServiceBuilder::new()
///     .layer(CommandTimeoutLayer::new(Duration::from_secs(1)))
///     .service(AutoPipelineService::new(conn, AutoPipelineConfig::default()));
/// let client = MultiplexedClient::from_layered(inner);
/// ```
///
/// # Transactions
///
/// Use the [`Transaction`](crate::Transaction) type for MULTI/EXEC -- it runs
/// atomically here (the whole WATCH/MULTI/EXEC sequence is sent as one
/// contiguous pipeline via [`AutoPipelineService::call_pipeline`], so no other
/// task's commands interleave).
///
/// Do **not** drive a transaction with the raw `Multi`/`Exec` command builders
/// over [`execute`](Self::execute): each `execute` is an independent
/// auto-pipelined call, so commands from other tasks sharing this connection
/// can land between your MULTI and EXEC and corrupt the transaction. The
/// `Transaction` type exists precisely to avoid that.
#[derive(Clone)]
pub struct MultiplexedClient<S = AutoPipelineService> {
    inner: CommandAdapter<S>,
}

impl MultiplexedClient<AutoPipelineService> {
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

    /// Connect from a Redis URL with an explicit TLS config (custom root CA or
    /// mTLS client certificate).
    ///
    /// See [`RedisConnection::connect_url_with_tls`].
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub async fn connect_url_with_tls(
        url: &str,
        tls_config: &redis_tower_core::tls::TlsConfig,
    ) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_url_with_tls(url, tls_config).await?;
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

    /// Gracefully shut down the multiplexed client.
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

impl<S> MultiplexedClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone,
    S::Future: Send + 'static,
{
    /// Build a multiplexed client from a layered Frame-level [`Service`].
    ///
    /// This is the middleware injection point: wrap [`AutoPipelineService`] (or
    /// any `Service<Frame, Response = Frame, Error = RedisError>`) in a Tower
    /// stack -- circuit breaker, timeout, retry -- and hand the result here. The
    /// client adapts typed commands onto the stack, so every [`execute`] flows
    /// through your middleware.
    ///
    /// [`execute`]: Self::execute
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    /// use tower::ServiceBuilder;
    /// use redis_tower::{AutoPipelineService, AutoPipelineConfig, CommandTimeoutLayer,
    ///     MultiplexedClient, RedisConnection};
    ///
    /// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
    /// let inner = ServiceBuilder::new()
    ///     .layer(CommandTimeoutLayer::new(Duration::from_secs(1)))
    ///     .service(AutoPipelineService::new(conn, AutoPipelineConfig::default()));
    /// let client = MultiplexedClient::from_layered(inner);
    /// let pong = client.health_check().await?;
    /// ```
    pub fn from_layered(service: S) -> Self {
        Self {
            inner: CommandAdapter::new(service),
        }
    }

    /// Execute a command.
    ///
    /// If other tasks are calling execute concurrently, their commands
    /// will be batched into a single Redis pipeline for efficiency.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut svc = self.inner.clone();
        std::future::poll_fn(|cx| <CommandAdapter<S> as Service<Cmd>>::poll_ready(&mut svc, cx))
            .await?;
        Service::call(&mut svc, cmd).await
    }

    /// Send a PING to verify the connection is alive.
    ///
    /// Returns `Ok(())` on success. Useful for Kubernetes readiness probes
    /// and `/health` endpoints.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        self.execute(Ping::new()).await?;
        Ok(())
    }
}

/// Atomic MULTI/EXEC for the standard multiplexed client.
///
/// The WATCH/MULTI/commands/EXEC frames are sent as one contiguous batch via
/// [`AutoPipelineService::call_pipeline`], which guarantees the worker flushes
/// them back-to-back with no interleaving from other tasks sharing the
/// connection. This makes [`Transaction`](crate::Transaction) safe on a
/// `MultiplexedClient` despite the shared connection. Only the default
/// `AutoPipelineService`-backed client supports this (a layered client built
/// with [`from_layered`](MultiplexedClient::from_layered) has no
/// `call_pipeline`).
impl TransactionExecutor for MultiplexedClient<AutoPipelineService> {
    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
        // Assemble the full sequence: [WATCH..., MULTI, commands..., EXEC].
        let mut frames = watch_frames;
        frames.push(array(vec![bulk("MULTI")]));
        frames.extend(command_frames);
        frames.push(array(vec![bulk("EXEC")]));

        // Clone the handle so the future owns its executor; the clone shares
        // the same worker, and call_pipeline keeps the batch atomic.
        let mut svc = self.inner.clone().into_inner();
        async move {
            let mut responses = svc.call_pipeline(frames).await?;
            // The last response is EXEC's: an array of per-command results when
            // committed, or null when a WATCHed key changed (aborted).
            let exec = responses.pop().ok_or(RedisError::UnexpectedResponse {
                expected: "EXEC response",
                actual: "empty pipeline response".to_string(),
            })?;
            match exec {
                Frame::Array(Some(results)) => Ok(Some(results)),
                Frame::Array(None) | Frame::Null => Ok(None),
                Frame::Error(e) => Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned())),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "array or null",
                    actual: format!("{other:?}"),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_commands::Get;
    use std::task::{Context, Poll};
    use tower_layer::Layer;

    /// A minimal Frame-level service standing in for a real connection, used to
    /// verify the injection point without a live server.
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

    #[test]
    fn multiplexed_client_is_transaction_executor() {
        // The standard client supports atomic MULTI/EXEC via call_pipeline.
        fn assert_txn_executor<T: TransactionExecutor>() {}
        assert_txn_executor::<MultiplexedClient>();
    }

    #[tokio::test]
    async fn from_layered_routes_execute_through_injected_service() {
        let inner = MockFrameService {
            reply: Frame::BulkString(Some(Bytes::from("layered"))),
        };
        let client = MultiplexedClient::from_layered(inner);

        // Generic over the injected service, and still Clone-shareable.
        let client2 = client.clone();
        let val: Option<Bytes> = client2.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("layered")));
    }

    #[tokio::test]
    async fn from_layered_composes_a_real_tower_layer() {
        use crate::command_timeout::CommandTimeoutLayer;
        use std::time::Duration;

        // Wrap the inner service in an actual middleware layer, then inject it.
        let inner = CommandTimeoutLayer::new(Duration::from_secs(5)).layer(MockFrameService {
            reply: Frame::BulkString(Some(Bytes::from("through-timeout"))),
        });
        let client = MultiplexedClient::from_layered(inner);

        let val: Option<Bytes> = client.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("through-timeout")));
    }
}
