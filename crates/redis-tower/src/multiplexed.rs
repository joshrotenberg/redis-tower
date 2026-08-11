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
//! Direct [`Transaction`](crate::Transaction) execution submits one atomic
//! WATCH/MULTI/EXEC batch. Workflows that need separate calls while holding
//! exclusive connection state, including the closure-based `transaction`
//! helpers and blocking commands, require [`RedisConnection`] directly or via
//! [`ConnectionPool`](crate::pool::ConnectionPool).
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
//! # let _ = val;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;

use redis_tower_commands::Ping;
use redis_tower_core::{
    Command, ConnectionConfig, Frame, ProtocolVersion, RedisConnection, RedisError,
};
use redis_tower_protocol::helpers::{array, bulk};
use tower_service::Service;

use crate::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig, AutoPipelineService};
use crate::cache_layer::CacheService;
use crate::cache_state::CacheStatistics;
use crate::caching::{CachedClientConfig, connect_resp3, force_resp3};
use crate::circuit_breaker::{RedisCircuitBreakerClient, RedisCircuitBreakerConfig};
use crate::command_adapter::CommandAdapter;
use crate::pipeline::PipelineExecutor;
use crate::reconnect::{
    AddrConnectionFactory, ConnectionEventBus, ConnectionFactory, Resp3AddrConnectionFactory,
    UrlConnectionFactory,
};
use crate::retry::{RetryClient, RetryPolicy};
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
/// pipelines. Direct [`Transaction`](crate::Transaction) values are supported;
/// for workflows requiring exclusive connection access across separate calls,
/// use [`RedisConnection`] directly or [`ConnectionPool`](crate::pool::ConnectionPool).
///
/// # Blocking commands
///
/// **Never run a blocking command on a `MultiplexedClient`.** Because every
/// clone shares one connection and one pipeline worker, a blocking command
/// (`BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`/`BZPOPMAX`, or `XREAD`/`XREADGROUP`
/// with `BLOCK`) holds that worker for its entire wait, stalling every other
/// concurrent caller until it returns. Use a dedicated [`RedisConnection`] or a
/// [`ConnectionPool`](crate::pool::ConnectionPool) connection for blocking
/// work; such commands report `is_blocking() == true`.
///
/// # Middleware
///
/// The type parameter `S` is the inner Frame-level [`Service`] and defaults to
/// [`AutoPipelineService`]. To wrap the client in Tower middleware (circuit
/// breakers, timeouts, retries), build a `Service<Frame>` stack and pass it to
/// [`from_layered`](Self::from_layered):
///
/// This injection point is below [`CommandAdapter`], so the middleware sees raw
/// [`Frame`] values rather than typed command metadata. In particular, a
/// [`CommandTimeoutLayer`](crate::CommandTimeoutLayer) here supplies its static
/// timeout but cannot inspect [`WithDeadline`](redis_tower_core::WithDeadline)
/// itself. `MultiplexedClient::execute` and `CommandAdapter` enforce that typed
/// absolute deadline across readiness and the frame-level call before the
/// metadata is discarded.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
/// # let _ = client;
/// # Ok(())
/// # }
/// ```
///
/// # Transactions
///
/// Use the [`Transaction`](crate::Transaction) type for MULTI/EXEC -- it runs
/// atomically here (the whole WATCH/MULTI/EXEC sequence is sent as one
/// contiguous pipeline via [`AutoPipelineService::call_pipeline`], so no other
/// task's commands interleave).
///
/// The closure-based [`transaction()`](crate::transaction()) and
/// [`transaction_with_retries`](crate::transaction_with_retries) helpers are
/// rejected before WATCH because their read/build window spans separate queue
/// submissions. A direct [`Transaction`](crate::Transaction) may include WATCH
/// when its body is already known; read/compute/build retries require a
/// dedicated [`RedisConnection`].
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

/// A cloneable, auto-pipelined Redis client with server-assisted local caching.
///
/// This client keeps [`CacheService`] inside the typed command adapter and
/// directly above the shared [`AutoPipelineService`] worker. Cache hits
/// therefore avoid the worker queue, while misses and non-cacheable commands
/// retain the same batching and back-pressure behavior as
/// [`MultiplexedClient`].
///
/// It is a distinct newtype rather than a type alias so its tracked connection
/// constructors do not make the standard [`MultiplexedClient`] constructors
/// ambiguous.
#[derive(Clone)]
pub struct CachedMultiplexedClient {
    inner: MultiplexedClient<CacheService<AutoPipelineService>>,
}

impl MultiplexedClient<AutoPipelineService> {
    /// Connect to a Redis server at `host:port`.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect(addr).await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect with explicit transport, protocol, and RESP decode settings.
    ///
    /// This name distinguishes connection settings from
    /// [`Self::from_connection_with_config`], whose config controls the
    /// auto-pipeline worker after the connection has been established.
    pub async fn connect_with_connection_config(
        addr: &str,
        config: &redis_tower_core::ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_with_config(addr, config).await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect using a Redis URL (`redis://`, `rediss://`, `unix://`).
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_url(url).await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect from a Redis URL with explicit connection settings.
    pub async fn connect_url_with_connection_config(
        url: &str,
        config: &redis_tower_core::ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_url_with_config(url, config).await?;
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

    /// Connect from a Redis URL with explicit TLS and connection settings.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub async fn connect_url_with_tls_and_connection_config(
        url: &str,
        tls_config: &redis_tower_core::tls::TlsConfig,
        connection_config: &redis_tower_core::ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let conn =
            RedisConnection::connect_url_with_tls_and_config(url, tls_config, connection_config)
                .await?;
        Ok(Self::from_connection(conn))
    }

    /// Connect and negotiate RESP3 protocol.
    pub async fn connect_resp3(addr: &str) -> Result<Self, RedisError> {
        let conn = RedisConnection::connect_resp3(addr).await?;
        Ok(Self::from_connection(conn))
    }

    /// Create a client that connects to `host:port` on its first command.
    ///
    /// This constructor is synchronous and performs no DNS or network I/O,
    /// which makes it suitable for serverless initialization paths. The first
    /// command receives [`RedisError::ConnectionClosed`] if that deferred
    /// attempt fails. Later connection loss is handled using the default
    /// reconnect policy.
    ///
    /// # Panics
    ///
    /// Panics when called outside an entered Tokio runtime. Construction
    /// starts the lightweight request worker even though network I/O is
    /// deferred.
    pub fn connect_lazy(addr: impl Into<String>) -> Self {
        Self::from_lazy_factory(
            AddrConnectionFactory::new(addr),
            AutoPipelineConfig::default(),
            AutoPipelineReconnectConfig::default(),
        )
    }

    /// Create a URL-backed client that connects on its first command.
    ///
    /// URL authentication, database selection, TLS, and Unix-socket settings
    /// are applied by [`UrlConnectionFactory`] on the deferred connection and
    /// every reconnect.
    ///
    /// # Panics
    ///
    /// Panics when called outside an entered Tokio runtime.
    pub fn connect_url_lazy(url: impl Into<String>) -> Self {
        Self::from_lazy_factory(
            UrlConnectionFactory::new(url),
            AutoPipelineConfig::default(),
            AutoPipelineReconnectConfig::default(),
        )
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

    /// Wrap an existing connection and publish its lifecycle events.
    ///
    /// This fixed-connection form reports [`crate::ConnectionEvent::Connected`] and a
    /// later disconnect, but it does not reconnect. Subscribe to `events`
    /// before this call to observe the initial event.
    pub fn from_connection_with_events(
        conn: RedisConnection,
        config: AutoPipelineConfig,
        events: ConnectionEventBus,
    ) -> Self {
        Self {
            inner: CommandAdapter::new(AutoPipelineService::new_with_events(conn, config, events)),
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
    /// [`UrlConnectionFactory`] for
    /// AUTH+SELECT from a URL, or implement [`ConnectionFactory`] yourself
    /// for custom init.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
    /// # let _ = client;
    /// # Ok(())
    /// # }
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

    /// Build a reconnecting multiplexed client that publishes lifecycle events.
    ///
    /// Subscribe to `events` before calling this constructor to observe the
    /// initial connect result. The same bus can be cloned into a Sentinel or
    /// cluster topology manager, which can publish
    /// [`crate::ConnectionEvent::Failover`] after confirming a role change.
    pub async fn from_factory_with_events(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        let svc = AutoPipelineService::with_factory_and_events(factory, config, reconnect, events)
            .await?;
        Ok(Self {
            inner: CommandAdapter::new(svc),
        })
    }

    /// Build a reconnecting client whose factory is first called by a command.
    ///
    /// Unlike [`Self::from_factory`], this returns synchronously and does not
    /// establish a Redis connection during application startup. Connection
    /// health is initially false. A failed first attempt returns
    /// [`RedisError::ConnectionClosed`] to the triggering command, while a
    /// later command can attempt connection again. Use the event-enabled form
    /// when the underlying connection failure detail is required.
    ///
    /// # Panics
    ///
    /// Panics when called outside an entered Tokio runtime.
    pub fn from_lazy_factory(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
    ) -> Self {
        let svc = AutoPipelineService::with_lazy_factory(factory, config, reconnect);
        Self {
            inner: CommandAdapter::new(svc),
        }
    }

    /// Build a lazily connected client that publishes lifecycle events.
    ///
    /// Construction emits no event. The first deferred success publishes
    /// [`crate::ConnectionEvent::Connected`]; a deferred failure publishes
    /// [`crate::ConnectionEvent::ConnectFailed`]. Subscribe before calling
    /// this constructor to observe that first command-driven transition.
    ///
    /// # Panics
    ///
    /// Panics when called outside an entered Tokio runtime.
    pub fn from_lazy_factory_with_events(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
        events: ConnectionEventBus,
    ) -> Self {
        let svc =
            AutoPipelineService::with_lazy_factory_and_events(factory, config, reconnect, events);
        Self {
            inner: CommandAdapter::new(svc),
        }
    }

    /// Return whether the shared worker currently owns a usable connection.
    ///
    /// This is an instantaneous local snapshot and does not send `PING`. It is
    /// initially `false` for a client built with a lazy constructor, becomes
    /// `true` after its first connection succeeds, and returns to `false`
    /// before disconnect events or failed request responses are delivered.
    pub fn is_connection_healthy(&self) -> bool {
        self.inner.inner().is_connection_healthy()
    }

    /// Subscribe to connection-health transitions without forcing a connect.
    ///
    /// The initial snapshot follows [`Self::is_connection_healthy`]. The watch
    /// channel closes when the shared worker terminates.
    pub fn subscribe_connection_health(&self) -> tokio::sync::watch::Receiver<bool> {
        self.inner.inner().subscribe_connection_health()
    }

    /// Returns the current number of requests pending in the auto-pipeline
    /// queue.
    ///
    /// This is an instantaneous snapshot intended for observability and load
    /// shedding decisions. The value may change immediately after it is read.
    /// Layered clients built with [`Self::from_layered`] expose this method only
    /// when their concrete inner service remains [`AutoPipelineService`].
    pub fn queue_depth(&self) -> usize {
        self.inner.inner().queue_depth()
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

    /// Wrap this client in idempotent-aware automatic retries.
    ///
    /// Returns a [`RetryClient`] whose `execute` reissues idempotent commands
    /// on retryable errors per the [`RetryPolicy`], and never retries a
    /// non-idempotent write (so it cannot be silently duplicated). The retry
    /// wrapper shares this client's connection -- it is a cheap handle clone.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower::{MultiplexedClient, RetryPolicy};
    /// use redis_tower::commands::Get;
    ///
    /// let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
    /// let retrying = client.retry(RetryPolicy::default());
    /// let value: Option<bytes::Bytes> = retrying.execute(Get::new("key")).await?;
    /// # let _ = value;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry(&self, policy: RetryPolicy) -> RetryClient<Self> {
        RetryClient::new(self.clone(), policy)
    }
}

impl CachedMultiplexedClient {
    /// Connect a cloneable, auto-pipelined client with server-assisted caching
    /// and safe cache defaults.
    ///
    /// This opens a RESP3 data connection plus a dedicated invalidation
    /// receiver. The receiver lifecycle is owned by the cached service: if it
    /// disconnects, caching is disabled and cleared until tracking has been
    /// re-established and the data connection atomically redirects
    /// invalidations to the replacement receiver. Losing the fixed data worker
    /// instead clears the cache and closes the client so tracking state is
    /// never reconstructed implicitly.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_config(addr, CachedClientConfig::default()).await
    }

    /// Connect with explicit cache and tracking configuration.
    pub async fn connect_with_config(
        addr: &str,
        cache_config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::connect_with_pipeline_config(addr, cache_config, AutoPipelineConfig::default()).await
    }

    /// Connect a cached client with explicit transport and RESP decode
    /// settings.
    ///
    /// Client-side caching always forces RESP3, regardless of the protocol
    /// policy in `connection_config`. The remaining settings are applied to
    /// both the data connection and every invalidation receiver.
    pub async fn connect_with_connection_config(
        addr: &str,
        connection_config: &ConnectionConfig,
        cache_config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let factory =
            Resp3AddrConnectionFactory::new(addr).with_connection_config(connection_config.clone());
        Self::from_factory(factory, cache_config).await
    }

    /// Connect using a Redis URL (`redis://`, `rediss://`, or `unix://`) with
    /// safe cache defaults.
    ///
    /// URL authentication and database selection are applied independently to
    /// the data and invalidation connections.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        Self::connect_url_with_config(url, CachedClientConfig::default()).await
    }

    /// Connect using a Redis URL with explicit cache and tracking
    /// configuration.
    pub async fn connect_url_with_config(
        url: &str,
        cache_config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::connect_url_with_connection_config(url, &ConnectionConfig::new(), cache_config).await
    }

    /// Connect using a Redis URL with explicit transport and RESP decode
    /// settings.
    ///
    /// Client-side caching always forces RESP3. For custom TLS roots or mTLS,
    /// build a [`UrlConnectionFactory`] and pass it to [`Self::from_factory`].
    pub async fn connect_url_with_connection_config(
        url: &str,
        connection_config: &ConnectionConfig,
        cache_config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let connection_config = connection_config
            .clone()
            .with_protocol(ProtocolVersion::Resp3);
        let factory = UrlConnectionFactory::new(url).with_connection_config(connection_config);
        Self::from_factory(factory, cache_config).await
    }

    /// Connect a cached client with explicit auto-pipeline configuration.
    pub async fn connect_with_pipeline_config(
        addr: &str,
        cache_config: CachedClientConfig,
        pipeline_config: AutoPipelineConfig,
    ) -> Result<Self, RedisError> {
        let factory = Resp3AddrConnectionFactory::new(addr);
        Self::from_factory_with_pipeline_config(factory, cache_config, pipeline_config).await
    }

    /// Connect the data and invalidation paths through one shared factory.
    ///
    /// The factory creates the initial fixed data connection, the invalidation
    /// receiver, and any replacement receiver after tracking loss. This does
    /// not make the data worker reconnecting: use a new cached client after a
    /// data-connection failure so tracking setup cannot be silently lost.
    pub async fn from_factory(
        factory: impl ConnectionFactory,
        cache_config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::from_factory_with_pipeline_config(
            factory,
            cache_config,
            AutoPipelineConfig::default(),
        )
        .await
    }

    /// Connect through a shared factory with explicit auto-pipeline settings.
    pub async fn from_factory_with_pipeline_config(
        factory: impl ConnectionFactory,
        cache_config: CachedClientConfig,
        pipeline_config: AutoPipelineConfig,
    ) -> Result<Self, RedisError> {
        Self::from_shared_factory_with_pipeline_config(
            Arc::new(factory),
            cache_config,
            pipeline_config,
        )
        .await
    }

    async fn from_shared_factory_with_pipeline_config(
        factory: Arc<dyn ConnectionFactory>,
        cache_config: CachedClientConfig,
        pipeline_config: AutoPipelineConfig,
    ) -> Result<Self, RedisError> {
        let conn = connect_resp3(factory.as_ref()).await?;
        Self::from_connection_with_shared_factory_and_pipeline_config(
            conn,
            factory,
            cache_config,
            pipeline_config,
        )
        .await
    }

    /// Wrap an existing data connection and create invalidation receivers with
    /// `receiver_factory`.
    ///
    /// The existing connection is upgraded to RESP3 if necessary. The factory
    /// must reproduce any authentication and transport setup required for a
    /// second connection to the same Redis server.
    pub async fn from_connection_with_factory(
        conn: RedisConnection,
        receiver_factory: impl ConnectionFactory,
        cache_config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::from_connection_with_factory_and_pipeline_config(
            conn,
            receiver_factory,
            cache_config,
            AutoPipelineConfig::default(),
        )
        .await
    }

    /// Wrap an existing data connection and use explicit auto-pipeline
    /// settings.
    pub async fn from_connection_with_factory_and_pipeline_config(
        conn: RedisConnection,
        receiver_factory: impl ConnectionFactory,
        cache_config: CachedClientConfig,
        pipeline_config: AutoPipelineConfig,
    ) -> Result<Self, RedisError> {
        Self::from_connection_with_shared_factory_and_pipeline_config(
            conn,
            Arc::new(receiver_factory),
            cache_config,
            pipeline_config,
        )
        .await
    }

    async fn from_connection_with_shared_factory_and_pipeline_config(
        conn: RedisConnection,
        receiver_factory: Arc<dyn ConnectionFactory>,
        cache_config: CachedClientConfig,
        pipeline_config: AutoPipelineConfig,
    ) -> Result<Self, RedisError> {
        let conn = force_resp3(conn).await?;
        let pipeline = AutoPipelineService::new(conn, pipeline_config);
        let cache = CacheService::with_tracking(pipeline, receiver_factory, &cache_config).await?;
        Ok(Self {
            inner: MultiplexedClient::from_layered(cache),
        })
    }

    /// Return the current number of requests pending in the auto-pipeline
    /// queue.
    ///
    /// Cache hits bypass this queue. As with the standard multiplexed client,
    /// this value is an instantaneous observability snapshot.
    pub fn queue_depth(&self) -> usize {
        self.inner.inner.inner().queue_depth()
    }

    /// Return the number of entries currently held in the local cache.
    pub async fn cache_size(&self) -> usize {
        self.inner.inner.inner().cache_size().await
    }

    /// Clear every local cache entry.
    pub async fn clear_cache(&self) {
        self.inner.inner.inner().clear_cache().await;
    }

    /// Return whether the data worker and invalidation tracking are healthy and
    /// cache reads are currently active.
    ///
    /// A disconnected tracking receiver disables and clears the cache until
    /// its replacement has been connected and installed on the data worker. A
    /// disconnected fixed data worker leaves this false permanently and
    /// requires constructing a new cached client.
    pub async fn is_caching_healthy(&self) -> bool {
        self.inner.inner.inner().is_caching_healthy().await
    }

    /// Return a point-in-time snapshot of cache activity counters.
    pub async fn cache_statistics(&self) -> CacheStatistics {
        self.inner.inner.inner().cache_statistics().await
    }

    /// Execute a command through the shared cached auto-pipeline.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        self.inner.execute(cmd).await
    }

    /// Send a PING to verify the data connection is alive.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        self.inner.health_check().await
    }

    /// Protect this cached client with a Redis-aware circuit breaker.
    pub fn with_circuit_breaker(
        self,
        config: RedisCircuitBreakerConfig,
    ) -> RedisCircuitBreakerClient<CacheService<AutoPipelineService>> {
        self.inner.with_circuit_breaker(config)
    }

    /// Wrap this client in idempotent-aware automatic retries.
    pub fn retry(&self, policy: RetryPolicy) -> RetryClient<Self> {
        RetryClient::new(self.clone(), policy)
    }

    /// Gracefully stop invalidation tracking and the auto-pipeline worker.
    ///
    /// If other client clones remain, this returns immediately and their
    /// shared tracking and worker lifecycle continues. The final clone waits
    /// for both background tasks to finish.
    pub async fn shutdown(self) {
        self.inner.inner.into_inner().shutdown().await;
    }
}

impl<S> MultiplexedClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone,
    S::Future: Send + 'static,
{
    /// Protect this client's frame service with a Redis-aware circuit breaker.
    ///
    /// The returned client shares breaker state across clones and exposes an
    /// operational handle through
    /// [`RedisCircuitBreakerClient::circuit_breaker_handle`].
    pub fn with_circuit_breaker(
        self,
        config: RedisCircuitBreakerConfig,
    ) -> RedisCircuitBreakerClient<S> {
        RedisCircuitBreakerClient::new(self.inner.into_inner(), config)
    }

    /// Build a multiplexed client from a layered Frame-level [`Service`].
    ///
    /// This is the middleware injection point: wrap [`AutoPipelineService`] (or
    /// any `Service<Frame, Response = Frame, Error = RedisError>`) in a Tower
    /// stack -- circuit breaker, timeout, retry -- and hand the result here. The
    /// client adapts typed commands onto the stack, so every [`execute`] flows
    /// through your middleware.
    ///
    /// The injected service receives raw [`Frame`] requests. Typed metadata
    /// such as [`Command::deadline`] is therefore not visible inside these
    /// layers. `execute` enforces that deadline around readiness plus dispatch;
    /// use [`CommandTimeoutLayer`](crate::CommandTimeoutLayer) here for an
    /// additional static frame-level timeout, or build a typed stack outside
    /// [`ExecutorService`](crate::ExecutorService) when middleware itself must
    /// inspect command metadata.
    ///
    /// [`execute`]: Self::execute
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
    /// # let _ = pong;
    /// # Ok(())
    /// # }
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
    /// A deadline carried by [`redis_tower_core::WithDeadline`] bounds both
    /// waiting for inner readiness and the dispatched call, including clients
    /// built with [`Self::from_layered`].
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        let mut svc = self.inner.clone();
        let operation = async move {
            std::future::poll_fn(|cx| {
                <CommandAdapter<S> as Service<Cmd>>::poll_ready(&mut svc, cx)
            })
            .await?;
            Service::call(&mut svc, cmd).await
        };

        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, operation)
                .await
                .map_err(|_elapsed| RedisError::CommandTimeout)?,
            None => operation.await,
        }
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

/// Explicit pipelining for the standard multiplexed client.
///
/// All frames are submitted as one [`AutoPipelineService::call_pipeline`]
/// request, so they remain contiguous on the shared connection and their raw
/// response frames are returned in the same order. Cloning the service handle
/// keeps the returned future independent of the caller's borrow while retaining
/// `call_pipeline`'s cancellation behavior: if the future is dropped before the
/// worker flushes it, the queued batch is pruned rather than sent after its
/// caller has gone away.
///
/// Only the default `AutoPipelineService`-backed client supports this. A
/// layered client built with [`from_layered`](MultiplexedClient::from_layered)
/// has no atomic multi-frame call surface.
impl PipelineExecutor for MultiplexedClient<AutoPipelineService> {
    fn execute_pipeline(
        &mut self,
        frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Vec<Frame>, RedisError>> + Send {
        let mut svc = self.inner.clone().into_inner();
        async move { svc.call_pipeline(frames).await }
    }
}

/// Explicit pipelining for the cached multiplexed client.
///
/// The cache service conservatively clears local entries around the raw batch
/// before forwarding all frames as one worker request. This preserves
/// read-your-own-writes even though [`PipelineExecutor`] carries untyped frames
/// whose complete key effects cannot always be determined locally.
impl PipelineExecutor for CachedMultiplexedClient {
    fn execute_pipeline(
        &mut self,
        frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Vec<Frame>, RedisError>> + Send {
        let mut svc = self.inner.inner.clone().into_inner();
        async move { svc.call_pipeline(frames).await }
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
    const SUPPORTS_TRANSACTION_RETRY: bool = false;

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

/// Atomic MULTI/EXEC for the cached multiplexed client.
///
/// As with explicit pipelines, the cache service clears local entries before
/// and after this untyped batch, then submits the complete WATCH/MULTI/EXEC
/// sequence as one contiguous worker request.
impl TransactionExecutor for CachedMultiplexedClient {
    const SUPPORTS_TRANSACTION_RETRY: bool = false;

    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
        let mut frames = watch_frames;
        frames.push(array(vec![bulk("MULTI")]));
        frames.extend(command_frames);
        frames.push(array(vec![bulk("EXEC")]));

        let mut svc = self.inner.inner.clone().into_inner();
        async move {
            let mut responses = svc.call_pipeline(frames).await?;
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
    use redis_tower_core::WithDeadline;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;
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

    #[derive(Clone)]
    struct NeverReadyFrameService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<Frame> for NeverReadyFrameService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Pending
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            self.calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(Frame::Null))
        }
    }

    #[test]
    fn multiplexed_client_is_transaction_executor() {
        // The standard client supports atomic MULTI/EXEC via call_pipeline.
        fn assert_txn_executor<T: TransactionExecutor>() {}
        assert_txn_executor::<MultiplexedClient>();
    }

    #[test]
    fn multiplexed_client_is_pipeline_executor() {
        fn assert_pipeline_executor<T: PipelineExecutor>() {}
        assert_pipeline_executor::<MultiplexedClient>();
    }

    #[test]
    fn cached_multiplexed_client_preserves_shared_executor_surface() {
        fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
        fn assert_pipeline_executor<T: PipelineExecutor>() {}
        fn assert_transaction_executor<T: TransactionExecutor>() {}
        fn assert_redis_executor<T: crate::RedisExecutor>() {}

        assert_clone_send_sync::<CachedMultiplexedClient>();
        assert_pipeline_executor::<CachedMultiplexedClient>();
        assert_transaction_executor::<CachedMultiplexedClient>();
        assert_redis_executor::<CachedMultiplexedClient>();
    }

    #[cfg(unix)]
    struct QueuedConnectionFactory {
        connections: StdMutex<VecDeque<RedisConnection>>,
        calls: Arc<AtomicUsize>,
    }

    #[cfg(unix)]
    impl ConnectionFactory for QueuedConnectionFactory {
        fn connect(
            &self,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let connection = self
                .connections
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(RedisError::ConnectionClosed);
            Box::pin(async move { connection })
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cached_factory_shares_setup_and_forces_resp3_on_both_connections() {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::Framed;

        let (data_client, data_server) = tokio::net::UnixStream::pair().unwrap();
        let (receiver_client, receiver_server) = tokio::net::UnixStream::pair().unwrap();
        let data_connection =
            RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(data_client));
        let receiver_connection =
            RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(receiver_client));
        let calls = Arc::new(AtomicUsize::new(0));
        let factory: Arc<dyn ConnectionFactory> = Arc::new(QueuedConnectionFactory {
            connections: StdMutex::new(VecDeque::from([data_connection, receiver_connection])),
            calls: Arc::clone(&calls),
        });

        let data_task = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(data_server),
                redis_tower_core::RespCodec::new(),
            );
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("HELLO"), bulk("3")])
            );
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("CLIENT"), bulk("TRACKING"), bulk("OFF")])
            );
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![
                    bulk("CLIENT"),
                    bulk("TRACKING"),
                    bulk("ON"),
                    bulk("REDIRECT"),
                    bulk("42"),
                    bulk("BCAST"),
                    bulk("NOLOOP"),
                ])
            );
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();
            futures::future::pending::<()>().await;
        });
        let receiver_task = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(receiver_server),
                redis_tower_core::RespCodec::new(),
            );
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("HELLO"), bulk("3")])
            );
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("CLIENT"), bulk("ID")])
            );
            framed.send(Frame::Integer(42)).await.unwrap();
            futures::future::pending::<()>().await;
        });

        let client = CachedMultiplexedClient::from_factory(factory, CachedClientConfig::default())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(client.is_caching_healthy().await);

        client.shutdown().await;
        data_task.abort();
        receiver_task.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pipeline_executor_preserves_binary_frames_and_response_order() {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::Framed;

        let (client_stream, server_stream) = tokio::net::UnixStream::pair().unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client_stream));
        let mut client = MultiplexedClient::from_connection(conn);

        let binary_key = Bytes::from_static(b"key:\0\xff");
        let binary_value = Bytes::from_static(b"value:\0\x80\xff");
        let requests = vec![
            array(vec![
                bulk("SET"),
                Frame::BulkString(Some(binary_key.clone())),
                Frame::BulkString(Some(binary_value.clone())),
            ]),
            array(vec![
                bulk("GET"),
                Frame::BulkString(Some(binary_key.clone())),
            ]),
        ];
        let expected_requests = requests.clone();
        let responses = vec![
            Frame::SimpleString(Bytes::from_static(b"OK")),
            Frame::BulkString(Some(binary_value)),
        ];
        let expected_responses = responses.clone();

        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(server_stream),
                redis_tower_core::RespCodec::new(),
            );
            for expected in expected_requests {
                assert_eq!(framed.next().await.unwrap().unwrap(), expected);
            }
            for response in responses {
                framed.send(response).await.unwrap();
            }
        });

        let actual = PipelineExecutor::execute_pipeline(&mut client, requests)
            .await
            .unwrap();
        assert_eq!(actual, expected_responses);

        server.await.unwrap();
        client.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_pipeline_executor_request_is_pruned_before_wire() {
        use tokio::io::AsyncReadExt;

        let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client_stream));
        let batch_window = Duration::from_millis(200);
        let mut client = MultiplexedClient::from_connection_with_config(
            conn,
            AutoPipelineConfig {
                batch_window,
                ..AutoPipelineConfig::default()
            },
        );

        let request = array(vec![
            bulk("SET"),
            bulk("cancelled-pipeline"),
            bulk("must-not-land"),
        ]);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                PipelineExecutor::execute_pipeline(&mut client, vec![request]),
            )
            .await
            .is_err(),
            "pipeline unexpectedly completed without a server response"
        );

        let mut bytes = [0u8; 128];
        assert!(
            tokio::time::timeout(
                batch_window + Duration::from_millis(100),
                server_stream.read(&mut bytes),
            )
            .await
            .is_err(),
            "a cancelled multiplexed pipeline reached the Redis socket"
        );

        client.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cached_pipeline_and_transaction_writes_cannot_leave_stale_reads() {
        use futures::{SinkExt, StreamExt};
        use redis_tower_commands::Set;
        use tokio_util::codec::Framed;

        use crate::cache_layer::CacheConfig;
        use crate::{Pipeline, Transaction};

        let (client_stream, server_stream) = tokio::net::UnixStream::pair().unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client_stream));
        let pipeline = AutoPipelineService::new(conn, AutoPipelineConfig::default());
        let cache = CacheService::new(pipeline, CacheConfig::default());
        let mut client = CachedMultiplexedClient {
            inner: MultiplexedClient::from_layered(cache),
        };
        assert_eq!(client.queue_depth(), 0);

        let get = array(vec![bulk("GET"), bulk("key")]);
        let set_pipeline = array(vec![bulk("SET"), bulk("key"), bulk("pipeline")]);
        let set_transaction = array(vec![bulk("SET"), bulk("key"), bulk("transaction")]);

        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(server_stream),
                redis_tower_core::RespCodec::new(),
            );

            assert_eq!(framed.next().await.unwrap().unwrap(), get);
            framed
                .send(Frame::BulkString(Some(Bytes::from_static(b"initial"))))
                .await
                .unwrap();

            assert_eq!(framed.next().await.unwrap().unwrap(), set_pipeline);
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();

            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("GET"), bulk("key")])
            );
            framed
                .send(Frame::BulkString(Some(Bytes::from_static(b"pipeline"))))
                .await
                .unwrap();

            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("MULTI")])
            );
            assert_eq!(framed.next().await.unwrap().unwrap(), set_transaction);
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("EXEC")])
            );
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"QUEUED")))
                .await
                .unwrap();
            framed
                .send(Frame::Array(Some(vec![Frame::SimpleString(
                    Bytes::from_static(b"OK"),
                )])))
                .await
                .unwrap();

            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("GET"), bulk("key")])
            );
            framed
                .send(Frame::BulkString(Some(Bytes::from_static(b"transaction"))))
                .await
                .unwrap();
        });

        let initial: Option<Bytes> = client.execute(Get::new("key")).await.unwrap();
        assert_eq!(initial, Some(Bytes::from_static(b"initial")));
        let cached: Option<Bytes> = client.execute(Get::new("key")).await.unwrap();
        assert_eq!(cached, initial);
        assert_eq!(
            client.queue_depth(),
            0,
            "a cache hit must release its readiness reservation"
        );
        assert_eq!(client.cache_size().await, 1);
        assert!(client.is_caching_healthy().await);
        let stats = client.cache_statistics().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        Pipeline::new()
            .push(Set::new("key", "pipeline"))
            .execute(&mut client)
            .await
            .unwrap();
        let after_pipeline: Option<Bytes> = client.execute(Get::new("key")).await.unwrap();
        assert_eq!(after_pipeline, Some(Bytes::from_static(b"pipeline")));

        let _ = Transaction::new()
            .push(Set::new("key", "transaction"))
            .execute(&mut client)
            .await
            .unwrap();
        let after_transaction: Option<Bytes> = client.execute(Get::new("key")).await.unwrap();
        assert_eq!(after_transaction, Some(Bytes::from_static(b"transaction")));

        server.await.unwrap();
        client.clear_cache().await;
        assert_eq!(client.cache_size().await, 0);
        client.shutdown().await;
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

        // Wrap the inner service in an actual middleware layer, then inject it.
        let inner = CommandTimeoutLayer::new(Duration::from_secs(5)).layer(MockFrameService {
            reply: Frame::BulkString(Some(Bytes::from("through-timeout"))),
        });
        let client = MultiplexedClient::from_layered(inner);

        let val: Option<Bytes> = client.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("through-timeout")));
    }

    #[tokio::test]
    async fn typed_deadline_bounds_readiness_above_frame_layers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = MultiplexedClient::from_layered(NeverReadyFrameService {
            calls: Arc::clone(&calls),
        });

        let result = client
            .execute(WithDeadline::after(
                Get::new("k"),
                Duration::from_millis(20),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
