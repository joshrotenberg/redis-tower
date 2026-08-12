//! Auto-reconnecting connection wrapper.
//!
//! Provides [`ResilientConnection`], a Redis connection that automatically
//! reconnects with configurable exponential backoff when the underlying
//! TCP connection drops. Implements `tower::Service<Cmd>` so it can be
//! used as a drop-in replacement for [`RedisConnection`].
//!
//! # Factories
//!
//! Different factories determine what negotiation happens on each reconnect:
//!
//! - [`AddrConnectionFactory`] -- plain TCP, automatic RESP3 negotiation, no auth
//! - [`UrlConnectionFactory`] -- AUTH + SELECT from URL parameters, automatic
//!   RESP3 negotiation
//! - [`Resp3AddrConnectionFactory`] -- plain TCP, RESP3 via `HELLO 3`, no auth
//! - [`CredentialConnectionFactory`](crate::credentials::CredentialConnectionFactory)
//!   -- dynamic credentials fetched on every connection, with AUTH before the
//!   requested protocol negotiation
//!
//! For static URL credentials, configure [`UrlConnectionFactory`] with a
//! [`ConnectionConfig`] whose protocol is [`ProtocolVersion::Resp3`]. For a
//! rotating provider, use
//! [`CredentialConnectionFactory`](crate::credentials::CredentialConnectionFactory)
//! with the same protocol setting.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig, ResilientConnection};
//! use redis_tower::commands::*;
//!
//! let mut conn = ResilientConnection::new(
//!     AddrConnectionFactory::new("127.0.0.1:6379"),
//!     ReconnectConfig::default(),
//! ).await?;
//!
//! // Transparently reconnects after connection loss.
//! let val: Option<bytes::Bytes> = conn.execute(Get::new("key")).await?;
//! # let _ = val;
//! # Ok(())
//! # }
//! ```
//!
//! # Lifecycle events
//!
//! Event delivery is bounded and observational: a slow consumer reports lag
//! but never delays reconnection.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::reconnect::{
//!     AddrConnectionFactory, ConnectionEventBus, ReconnectConfig, ResilientConnection,
//! };
//!
//! let events = ConnectionEventBus::default();
//! let mut stream = events.subscribe();
//! let _connection = ResilientConnection::new_with_events(
//!     AddrConnectionFactory::new("127.0.0.1:6379"),
//!     ReconnectConfig::default(),
//!     events,
//! ).await?;
//!
//! while let Ok(event) = stream.recv().await {
//!     println!("connection event: {event:?}");
//! }
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::Stream;
use redis_tower_core::{Command, ConnectionConfig, ProtocolVersion, RedisConnection, RedisError};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Default number of connection lifecycle events retained for each subscriber.
pub const DEFAULT_CONNECTION_EVENT_CAPACITY: usize = 64;

/// Why an established Redis connection became unusable.
///
/// This is deliberately clone-friendly: error text is held in an [`Arc<str>`]
/// because a broadcast event may be observed by many subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionDisconnectReason {
    /// A connection operation returned an error that requires replacement.
    ConnectionError {
        /// Human-readable error text from the failing connection operation.
        ///
        /// This text can include endpoint names and Redis/server details. Treat
        /// it as potentially sensitive when exporting or logging events.
        error: Arc<str>,
    },
    /// A pipelined response exceeded its configured deadline.
    CommandTimeout,
    /// Redis replied `READONLY`, indicating that a formerly writable node was
    /// demoted to a replica.
    ReadOnly,
    /// A direct connection wrapper was dropped, or the last client/service
    /// handle closed and its background worker stopped cleanly. This terminal
    /// transition is distinct from any earlier outage disconnect.
    Shutdown,
}

/// A programmatic connection lifecycle notification.
///
/// Events are emitted in transition order. For example, a reconnect that fails
/// once and then succeeds produces `Disconnected`, `ReconnectAttempt`,
/// `ReconnectFailed`, `ReconnectAttempt`, and `Reconnected` in that order.
/// Slow consumers receive an explicit [`ConnectionEventRecvError::Lagged`]
/// error rather than applying backpressure to connection progress. A
/// producer's `Disconnected { reason: Shutdown }` is its terminal event, though
/// a shared bus may continue carrying events from other producers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    /// The initial connection was established.
    Connected,
    /// An initial connection attempt failed.
    ConnectFailed {
        /// Human-readable error text from the failed connection attempt.
        ///
        /// This text can include endpoint names and Redis/server details. Treat
        /// it as potentially sensitive when exporting or logging events.
        error: Arc<str>,
    },
    /// An established connection became unusable.
    Disconnected {
        /// The condition that caused the disconnect transition.
        reason: ConnectionDisconnectReason,
    },
    /// A reconnect attempt was scheduled with a backoff delay.
    ReconnectAttempt {
        /// One-based reconnect attempt number.
        attempt: usize,
        /// Delay before this attempt starts.
        delay: Duration,
    },
    /// A reconnect attempt failed.
    ReconnectFailed {
        /// One-based reconnect attempt number.
        attempt: usize,
        /// Human-readable error text from the failed attempt.
        ///
        /// This text can include endpoint names and Redis/server details. Treat
        /// it as potentially sensitive when exporting or logging events.
        error: Arc<str>,
    },
    /// A new connection was established after a disconnect.
    Reconnected {
        /// Number of attempts needed to reconnect.
        attempts: usize,
        /// Total time spent in the reconnect campaign.
        elapsed: Duration,
    },
    /// The configured reconnect budget was exhausted.
    ReconnectExhausted {
        /// Number of reconnect attempts that were made.
        attempts: usize,
    },
    /// A topology manager observed a primary endpoint change.
    ///
    /// Standalone reconnectors do not infer failover from a socket failure.
    /// The multiplexed Sentinel client publishes this automatically because it
    /// has one primary and can report a ROLE-verified previous/current address
    /// pair. This compares the exact endpoint strings returned by Sentinel; it
    /// does not establish durable Redis node identity. Textual aliases for the
    /// same server can therefore look like a failover, while a replacement at
    /// the same endpoint does not produce this event.
    /// Redis Cluster can change several slot-scoped primaries independently, so
    /// the core API intentionally does not collapse every cluster topology diff
    /// into one misleading global failover. Cluster integrations retain the
    /// explicit [`ConnectionEventBus::publish`] producer hook for transitions
    /// they can identify precisely.
    Failover {
        /// Previous primary address, when known.
        previous: Option<Arc<str>>,
        /// New primary address, when known.
        current: Option<Arc<str>>,
    },
}

/// Error returned while receiving connection lifecycle events.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionEventRecvError {
    /// The subscriber fell behind the bounded event buffer.
    #[error("connection event subscriber lagged and skipped {skipped} event(s)")]
    Lagged {
        /// Number of events skipped before the oldest retained event.
        skipped: u64,
    },
    /// Every publisher was dropped and no further events can arrive.
    #[error("connection event stream closed")]
    Closed,
}

/// Clone-friendly bounded broadcaster for connection lifecycle events.
///
/// Each subscriber has an independent cursor over the same bounded ring. A
/// slow subscriber does not block reconnect or failover progress; it receives
/// [`ConnectionEventRecvError::Lagged`] with the exact number of skipped
/// events. Publishing when there are no subscribers is a constant-time no-op.
/// Use [`publish_with`](Self::publish_with) to avoid constructing an event
/// payload in that case.
///
/// The bus does not spawn a task. Events are published synchronously at the
/// lifecycle transition that produced them.
#[derive(Debug, Clone)]
pub struct ConnectionEventBus {
    tx: broadcast::Sender<ConnectionEvent>,
}

impl ConnectionEventBus {
    /// Create an event bus with the requested bounded buffer capacity.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "connection event capacity must be non-zero");
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to events published after this call.
    ///
    /// Broadcast subscriptions do not replay earlier events. Create the stream
    /// before passing a clone of the bus to a `*_with_events` constructor when
    /// the initial [`ConnectionEvent::Connected`] or
    /// [`ConnectionEvent::ConnectFailed`] event matters.
    pub fn subscribe(&self) -> ConnectionEventStream {
        ConnectionEventStream {
            inner: BroadcastStream::new(self.tx.subscribe()),
        }
    }

    /// Return the number of active event subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Publish an event without waiting for subscribers.
    ///
    /// Returns `true` when at least one subscriber was present. A subscriber
    /// that has fallen behind observes lag independently; it never delays this
    /// call or connection progress.
    pub fn publish(&self, event: ConnectionEvent) -> bool {
        if self.tx.receiver_count() == 0 {
            return false;
        }
        self.tx.send(event).is_ok()
    }

    /// Lazily construct and publish an event when subscribers are present.
    ///
    /// Returns `false` without invoking `make_event` when no subscriber exists.
    pub fn publish_with(&self, make_event: impl FnOnce() -> ConnectionEvent) -> bool {
        if self.tx.receiver_count() == 0 {
            return false;
        }
        self.tx.send(make_event()).is_ok()
    }
}

impl Default for ConnectionEventBus {
    fn default() -> Self {
        Self::new(DEFAULT_CONNECTION_EVENT_CAPACITY)
    }
}

/// A bounded asynchronous stream of [`ConnectionEvent`] values.
///
/// The [`Stream`] implementation yields lag as an error item and ends when all
/// bus publishers are dropped. [`recv`](Self::recv) represents that terminal
/// state explicitly as [`ConnectionEventRecvError::Closed`].
#[derive(Debug)]
pub struct ConnectionEventStream {
    inner: BroadcastStream<ConnectionEvent>,
}

impl ConnectionEventStream {
    /// Receive the next lifecycle event or an explicit lag/closed error.
    pub async fn recv(&mut self) -> Result<ConnectionEvent, ConnectionEventRecvError> {
        futures::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx))
            .await
            .unwrap_or(Err(ConnectionEventRecvError::Closed))
    }
}

impl Stream for ConnectionEventStream {
    type Item = Result<ConnectionEvent, ConnectionEventRecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                Poll::Ready(Some(Err(ConnectionEventRecvError::Lagged { skipped })))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Factory for creating new Redis connections.
///
/// Used by [`ResilientConnection`] and [`ResilientRedisClient`](crate::ResilientRedisClient)
/// to establish fresh connections during initial setup and reconnection.
///
/// The `connect()` method is called on every new connection, including
/// reconnections after connection loss. This makes it the right place to
/// replay any session-level setup such as `CLIENT TRACKING ON`, `SELECT`,
/// or `AUTH` that must be re-established after a reconnect.
///
/// A blanket implementation is provided for any `Fn() -> Future<Output = Result<RedisConnection, RedisError>>`,
/// so closures work out of the box. For named factories, see
/// [`AddrConnectionFactory`], [`UrlConnectionFactory`], and
/// [`CredentialConnectionFactory`](crate::credentials::CredentialConnectionFactory).
pub trait ConnectionFactory: Send + Sync + 'static {
    /// Create a new [`RedisConnection`].
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>>;
}

impl<F, Fut> ConnectionFactory for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<RedisConnection, RedisError>> + Send + 'static,
{
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        Box::pin((self)())
    }
}

impl ConnectionFactory for Arc<dyn ConnectionFactory> {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        self.as_ref().connect()
    }
}

/// A [`ConnectionFactory`] that connects via a Redis URL string.
///
/// Supports `redis://`, `rediss://` (TLS), and `unix://` schemes.
///
/// This factory uses the [`RedisConnection`] URL connection path, including
/// its configured variant. AUTH and SELECT are therefore replayed on every
/// reconnection based on the URL parameters. Use this factory (not
/// [`AddrConnectionFactory`]) when your Redis server requires authentication
/// or a non-default database.
pub struct UrlConnectionFactory {
    url: String,
    connection_config: ConnectionConfig,
    /// Explicit TLS config applied on every (re)connect, so reconnect-with-auth
    /// works with a custom CA / mTLS instead of the URL's default TLS.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<std::sync::Arc<redis_tower_core::tls::TlsConfig>>,
}

impl UrlConnectionFactory {
    /// Create a new factory from the given Redis URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connection_config: ConnectionConfig::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        }
    }

    /// Apply connection settings to every initial connection and reconnect.
    ///
    /// This is the built-in factory path for retaining tightened RESP decode
    /// limits across resilient clients and connection pools.
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }

    /// Use an explicit TLS config (custom root CA or mTLS client certificate)
    /// for every connection this factory makes.
    ///
    /// Without this, a `rediss://` URL uses the default rustls config -- so URL
    /// connect and custom TLS were previously mutually exclusive, which made
    /// reconnect-with-auth plus a private CA impossible. With it, the factory
    /// connects via the config-aware custom-TLS URL path on every attempt.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn with_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.tls = Some(std::sync::Arc::new(tls));
        self
    }
}

impl ConnectionFactory for UrlConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let url = self.url.clone();
        let connection_config = self.connection_config.clone();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        if let Some(tls) = self.tls.clone() {
            return Box::pin(async move {
                RedisConnection::connect_url_with_tls_and_config(&url, &tls, &connection_config)
                    .await
            });
        }
        Box::pin(
            async move { RedisConnection::connect_url_with_config(&url, &connection_config).await },
        )
    }
}

impl crate::pool::PoolFactory for UrlConnectionFactory {
    type Connection = RedisConnection;

    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
        ConnectionFactory::connect(self)
    }
}

/// A [`ConnectionFactory`] that connects via a `host:port` address string.
///
/// This factory creates plain TCP connections with no authentication or
/// database selection. Its default connection config auto-negotiates RESP3
/// with RESP2 fallback. If you need AUTH or SELECT on reconnect, use
/// [`UrlConnectionFactory`] instead.
pub struct AddrConnectionFactory {
    addr: String,
    connection_config: ConnectionConfig,
}

impl AddrConnectionFactory {
    /// Create a new factory from the given `host:port` address.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            connection_config: ConnectionConfig::default(),
        }
    }

    /// Apply connection settings to every initial connection and reconnect.
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }
}

impl ConnectionFactory for AddrConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        let connection_config = self.connection_config.clone();
        Box::pin(
            async move { RedisConnection::connect_with_config(&addr, &connection_config).await },
        )
    }
}

impl crate::pool::PoolFactory for AddrConnectionFactory {
    type Connection = RedisConnection;

    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
        ConnectionFactory::connect(self)
    }
}

/// A [`ConnectionFactory`] that connects via a `host:port` address and
/// negotiates RESP3 using `HELLO 3`.
///
/// Use this when you need forced RESP3 without URL-based AUTH/SELECT. For
/// forced RESP3 with authentication, use [`UrlConnectionFactory`] with a
/// `redis://` URL and a [`ConnectionConfig`] set to [`ProtocolVersion::Resp3`].
pub struct Resp3AddrConnectionFactory {
    addr: String,
    connection_config: ConnectionConfig,
}

impl Resp3AddrConnectionFactory {
    /// Create a new factory from the given `host:port` address.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            connection_config: ConnectionConfig::default().with_protocol(ProtocolVersion::Resp3),
        }
    }

    /// Apply connection settings to every initial connection and reconnect.
    ///
    /// The factory always forces [`ProtocolVersion::Resp3`], while retaining
    /// the supplied keepalive, timeout, and RESP decode limits.
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection_config = config.with_protocol(ProtocolVersion::Resp3);
        self
    }
}

impl ConnectionFactory for Resp3AddrConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        let connection_config = self.connection_config.clone();
        Box::pin(
            async move { RedisConnection::connect_with_config(&addr, &connection_config).await },
        )
    }
}

impl crate::pool::PoolFactory for Resp3AddrConnectionFactory {
    type Connection = RedisConnection;

    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
        ConnectionFactory::connect(self)
    }
}

/// Configuration for reconnection behavior.
///
/// Controls the exponential backoff strategy used by [`ResilientConnection`]
/// and [`ResilientRedisClient`](crate::ResilientRedisClient).
///
/// # Defaults
///
/// - `max_retries`: `None` (infinite retries after the first reconnect attempt)
/// - `base_delay`: 100ms
/// - `max_delay`: 5s
/// - `jitter`: `true`
/// - `connect_timeout`: `None` (no timeout)
///
/// # Jitter
///
/// When `jitter` is enabled (the default), each backoff delay is a uniformly
/// random value in `[0, cap)` where `cap` is the un-jittered exponential
/// delay. This is the "full jitter" strategy recommended by AWS for avoiding
/// thundering-herd reconnect storms when Redis restarts and many clients
/// reconnect simultaneously.
///
/// Set `jitter: false` (via [`.jitter(false)`](Self::jitter)) to restore
/// deterministic backoff, which is useful in tests.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of retries after the first reconnection attempt.
    ///
    /// `Some(0)` still permits one reconnect attempt. `Some(n)` permits at
    /// most `n + 1` total reconnect attempts, while `None` retries forever.
    /// Initial construction is separate and is not counted in this budget.
    pub max_retries: Option<usize>,
    /// Initial delay before first reconnection attempt.
    pub base_delay: Duration,
    /// Maximum delay between attempts (caps exponential backoff).
    pub max_delay: Duration,
    /// Whether to apply full jitter to each backoff delay.
    ///
    /// Defaults to `true`. When enabled, each delay is a uniformly random
    /// value in `[0, cap)` rather than the deterministic exponential value,
    /// spreading reconnect attempts across time.
    pub jitter: bool,
    /// Per-attempt connect timeout applied to each `factory.connect()` call,
    /// including the initial call made by reconnecting constructors.
    ///
    /// When `Some`, each call to the [`ConnectionFactory`] is wrapped in
    /// `tokio::time::timeout`. If the factory does not complete within this
    /// duration the attempt is treated as a failure, and the reconnect loop
    /// waits for the next backoff delay before trying again.
    ///
    /// When `None` (the default), connection attempts run without a timeout
    /// and may block for the OS-default TCP timeout — potentially several
    /// minutes on an unreachable host.
    pub connect_timeout: Option<Duration>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: None,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            jitter: true,
            connect_timeout: None,
        }
    }
}

impl ReconnectConfig {
    /// Set the maximum number of retries after the first reconnect attempt.
    ///
    /// Passing zero allows the first reconnect attempt but no retry after it.
    /// Passing `n` allows at most `n + 1` total reconnect attempts.
    #[must_use]
    pub fn max_retries(mut self, n: usize) -> Self {
        self.max_retries = Some(n);
        self
    }

    /// Set the initial delay before the first reconnection attempt.
    #[must_use]
    pub fn base_delay(mut self, d: Duration) -> Self {
        self.base_delay = d;
        self
    }

    /// Set the maximum delay between reconnection attempts.
    ///
    /// Caps the exponential backoff so delays do not grow unbounded.
    #[must_use]
    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = d;
        self
    }

    /// Enable or disable full jitter on backoff delays.
    ///
    /// When `true` (the default), each delay is a uniformly random value in
    /// `[0, cap)` where `cap` is the un-jittered exponential delay. This
    /// spreads reconnect attempts to avoid thundering-herd storms.
    ///
    /// When `false`, delays are deterministic: `base_delay * 2^attempt`,
    /// capped at `max_delay`. Useful in tests that assert specific delay
    /// values.
    #[must_use]
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    /// Set a timeout for each individual connection attempt.
    ///
    /// When set, each call to [`ConnectionFactory::connect`] is wrapped in
    /// [`tokio::time::timeout`], including the initial factory call made by
    /// reconnecting constructors. If a reconnect factory does not complete
    /// within this duration, the attempt is counted as a failure and the loop
    /// retries after the next backoff delay.
    ///
    /// When not set (the default), connection attempts run without a timeout
    /// and may block for the OS-default TCP timeout — potentially several
    /// minutes on an unreachable host.
    #[must_use]
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = Some(d);
        self
    }

    /// Return whether the zero-based reconnect attempt is past the budget.
    pub(crate) fn attempt_exhausted(&self, attempt: usize) -> bool {
        self.max_retries.map(|max| attempt > max).unwrap_or(false)
    }

    /// Return the finite total attempt budget, when configured.
    pub(crate) fn total_attempt_budget(&self) -> Option<usize> {
        self.max_retries.map(|max| max.saturating_add(1))
    }

    pub(crate) fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let cap = self
            .base_delay
            .saturating_mul(1u32.wrapping_shl(attempt.min(31) as u32))
            .min(self.max_delay);

        if self.jitter {
            // Full jitter (AWS recommendation): uniform random in [0, cap).
            // This spreads reconnect storms when many clients back off together.
            let nanos = cap.as_nanos() as u64;
            if nanos == 0 {
                return Duration::ZERO;
            }
            Duration::from_nanos(rand::random::<u64>() % nanos)
        } else {
            cap
        }
    }
}

/// Connect through a factory while applying the configured per-attempt limit.
pub(crate) async fn connect_with_timeout(
    factory: &dyn ConnectionFactory,
    timeout: Option<Duration>,
) -> Result<RedisConnection, RedisError> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, factory.connect())
            .await
            .map_err(|_| RedisError::ConnectTimeout)?,
        None => factory.connect().await,
    }
}

type ReconnectFuture = Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>>;

pub(crate) enum ConnState {
    Connected(RedisConnection),
    WaitingToReconnect {
        attempt: usize,
        sleep: Pin<Box<tokio::time::Sleep>>,
        /// When the connection was first lost, carried across attempts so the
        /// success log can report the total reconnection duration.
        started: Instant,
    },
    Reconnecting {
        attempt: usize,
        future: ReconnectFuture,
        /// See `WaitingToReconnect::started`; carried across the transition.
        started: Instant,
    },
    Failed,
}

/// An auto-reconnecting Redis connection.
///
/// Wraps a [`ConnectionFactory`] and maintains a live connection. When a
/// command fails with a connection error, the next `poll_ready` triggers
/// reconnection with configurable exponential backoff.
///
/// # Factory Selection
///
/// The factory you choose determines what happens on reconnect:
///
/// | Factory | AUTH | SELECT | Protocol |
/// |---------|------|--------|----------|
/// | [`AddrConnectionFactory`] | No | No | Auto (RESP3 with RESP2 fallback) |
/// | [`UrlConnectionFactory`] | Yes (from URL) | Yes (from URL) | Auto (RESP3 with RESP2 fallback) |
/// | [`Resp3AddrConnectionFactory`] | No | No | Forced RESP3 |
/// | [`CredentialConnectionFactory`](crate::credentials::CredentialConnectionFactory) | Yes (from provider) | No | Configurable after AUTH |
///
/// All four named factories can retain a [`ConnectionConfig`] across every
/// reconnect via their `with_connection_config` builders.
///
/// # Custom Setup on Reconnect
///
/// Server-side state such as `CLIENT TRACKING`, pub/sub subscriptions, or
/// other session-level configuration is **not** automatically replayed on
/// reconnection. Only the setup performed inside [`ConnectionFactory::connect`]
/// runs on each new connection.
///
/// To replay custom commands after every (re)connection, implement
/// [`ConnectionFactory`] and issue the setup commands in `connect()`:
///
/// ```no_run
/// use redis_tower::reconnect::ConnectionFactory;
/// use redis_tower::commands::ClientTracking;
/// use redis_tower_core::{RedisConnection, RedisError};
/// use std::future::Future;
/// use std::pin::Pin;
///
/// struct TrackingFactory {
///     addr: String,
/// }
///
/// impl ConnectionFactory for TrackingFactory {
///     fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
///         let addr = self.addr.clone();
///         Box::pin(async move {
///             let mut conn = RedisConnection::connect_resp3(&addr).await?;
///             // CLIENT TRACKING, SELECT, or any other setup runs on every connection.
///             conn.execute(ClientTracking::on()).await?;
///             Ok(conn)
///         })
///     }
/// }
/// ```
///
/// Alternatively, use a closure factory for simple cases:
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::reconnect::{ReconnectConfig, ResilientConnection};
/// use redis_tower::commands::ClientTracking;
/// use redis_tower_core::{RedisConnection, RedisError};
///
/// let addr = "127.0.0.1:6379".to_string();
/// let conn = ResilientConnection::new(
///     move || {
///         let addr = addr.clone();
///         async move {
///             let mut c = RedisConnection::connect_resp3(&addr).await?;
///             c.execute(ClientTracking::on()).await?;
///             Ok::<_, RedisError>(c)
///         }
///     },
///     ReconnectConfig::default(),
/// ).await?;
/// # let _ = conn;
/// # Ok(())
/// # }
/// ```
///
/// # Behavior During Reconnection
///
/// The [`execute`](Self::execute) method and the `tower::Service` trait
/// behave differently when the connection is down:
///
/// - **`execute()`** -- returns [`RedisError::ConnectionClosed`] immediately
///   (fail-fast). Callers must handle the error or retry themselves.
/// - **`Service::poll_ready()`** -- drives the reconnection state machine
///   and returns `Poll::Pending` until a new connection is established.
///   Callers using the Tower `Service` trait (including via
///   `tower::buffer::Buffer`) will wait for reconnection to complete.
///   The in-flight queue is bounded by the caller's `Buffer` capacity.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig, ResilientConnection};
/// use redis_tower::commands::*;
///
/// let mut conn = ResilientConnection::new(
///     AddrConnectionFactory::new("127.0.0.1:6379"),
///     ReconnectConfig::default(),
/// ).await?;
///
/// // Reconnects automatically after connection loss.
/// let val = conn.execute(Get::new("key")).await?;
/// # let _ = val;
/// # Ok(())
/// # }
/// ```
pub struct ResilientConnection {
    pub(crate) factory: Arc<dyn ConnectionFactory>,
    pub(crate) config: ReconnectConfig,
    pub(crate) state: ConnState,
    /// Shared flag set by call futures when a connection error occurs.
    /// Checked by poll_ready on the next call cycle.
    ///
    /// NOTE: There is a one-request-delay between when a connection error
    /// occurs and when reconnection begins, because the flag is only checked
    /// in poll_ready. This is acceptable for most use cases.
    pub(crate) needs_reconnect: Arc<AtomicBool>,
    event_bus: Option<ConnectionEventBus>,
    disconnect_reported: Option<Arc<AtomicBool>>,
    shutdown_reported: Option<Arc<AtomicBool>>,
    lifecycle_lock: Option<Arc<StdMutex<()>>>,
    pub(crate) on_connect: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(crate) on_reconnect: Option<Arc<dyn Fn(usize) + Send + Sync>>,
}

impl ResilientConnection {
    /// Create a new resilient connection.
    pub async fn new(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        Self::new_inner(factory, config, None).await
    }

    /// Create a resilient connection that publishes lifecycle events.
    ///
    /// Subscribe to `events` before calling this constructor to observe the
    /// initial [`ConnectionEvent::Connected`] or
    /// [`ConnectionEvent::ConnectFailed`] event. Event consumption is never
    /// required for connection progress; lagging subscribers are skipped by
    /// the bounded broadcaster.
    pub async fn new_with_events(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::new_inner(factory, config, Some(events)).await
    }

    async fn new_inner(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        event_bus: Option<ConnectionEventBus>,
    ) -> Result<Self, RedisError> {
        let factory = Arc::new(factory);
        let result = connect_with_timeout(factory.as_ref(), config.connect_timeout).await;
        let conn = match result {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(events) = &event_bus {
                    events.publish_with(|| ConnectionEvent::ConnectFailed {
                        error: Arc::from(error.to_string()),
                    });
                }
                return Err(error);
            }
        };
        if let Some(events) = &event_bus {
            events.publish(ConnectionEvent::Connected);
        }
        let disconnect_reported = event_bus.as_ref().map(|_| Arc::new(AtomicBool::new(false)));
        let shutdown_reported = event_bus.as_ref().map(|_| Arc::new(AtomicBool::new(false)));
        let lifecycle_lock = event_bus.as_ref().map(|_| Arc::new(StdMutex::new(())));
        Ok(Self {
            factory,
            config,
            state: ConnState::Connected(conn),
            needs_reconnect: Arc::new(AtomicBool::new(false)),
            event_bus,
            disconnect_reported,
            shutdown_reported,
            lifecycle_lock,
            on_connect: None,
            on_reconnect: None,
        })
    }

    /// Set a callback fired when a connection is established.
    pub fn on_connect(mut self, f: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// Set a callback fired on each reconnection (receives attempt count).
    pub fn on_reconnect(mut self, f: impl Fn(usize) + Send + Sync + 'static) -> Self {
        self.on_reconnect = Some(Arc::new(f));
        self
    }

    /// Execute a command through the resilient connection.
    ///
    /// For direct async usage without the Tower `Service` trait.
    ///
    /// Unlike `Service::call()`, this method **fails fast**: if the connection
    /// is not in the `Connected` state (e.g., during reconnection), it returns
    /// [`RedisError::ConnectionClosed`] immediately rather than waiting for
    /// reconnection to complete.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        match &mut self.state {
            ConnState::Connected(conn) => {
                let result = conn.execute(cmd).await;
                if let Err(ref e) = result
                    && e.is_connection_error()
                {
                    self.needs_reconnect.store(true, Ordering::Release);
                    publish_disconnect_before_shutdown(
                        self.event_bus.as_ref(),
                        self.disconnect_reported.as_deref(),
                        self.shutdown_reported.as_deref(),
                        self.lifecycle_lock.as_deref(),
                        e,
                    );
                }
                result
            }
            _ => Err(RedisError::ConnectionClosed),
        }
    }

    /// Schedule the next reconnect attempt. `started` marks when the connection
    /// was first lost; it is threaded through every attempt so the eventual
    /// success log can report the total reconnection duration rather than the
    /// duration of the final attempt alone.
    fn trigger_reconnect(&mut self, attempt: usize, started: Instant) {
        if self.config.attempt_exhausted(attempt) {
            if let Some(events) = &self.event_bus {
                events.publish(ConnectionEvent::ReconnectExhausted { attempts: attempt });
            }
            self.state = ConnState::Failed;
            return;
        }
        let delay = self.config.delay_for_attempt(attempt);
        tracing::warn!(attempt, delay = ?delay, "redis: connection lost, reconnecting");
        if let Some(events) = &self.event_bus {
            events.publish(ConnectionEvent::ReconnectAttempt {
                attempt: attempt + 1,
                delay,
            });
        }
        self.state = ConnState::WaitingToReconnect {
            attempt,
            sleep: Box::pin(tokio::time::sleep(delay)),
            started,
        };
    }
}

impl Drop for ResilientConnection {
    fn drop(&mut self) {
        let _guard = self.lifecycle_lock.as_ref().map(|lock| {
            lock.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        publish_shutdown_once(self.event_bus.as_ref(), self.shutdown_reported.as_deref());
    }
}

impl<Cmd: Command> tower_service::Service<Cmd> for ResilientConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check if a previous call signaled a connection error.
        // Race window: between the flag being set in a call future and this
        // check, one additional request may be dispatched to the broken
        // connection. This is inherent to the AtomicBool design and is
        // acceptable for most use cases.
        if self.needs_reconnect.swap(false, Ordering::Acquire)
            && matches!(self.state, ConnState::Connected(_))
        {
            self.trigger_reconnect(0, Instant::now());
        }

        loop {
            match &mut self.state {
                ConnState::Connected(_) => return Poll::Ready(Ok(())),
                ConnState::Failed => {
                    return Poll::Ready(Err(RedisError::ReconnectFailed {
                        attempts: self.config.total_attempt_budget().unwrap_or(0),
                        last_error: Arc::new(RedisError::ConnectionClosed),
                    }));
                }
                ConnState::WaitingToReconnect {
                    attempt,
                    sleep,
                    started,
                } => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let attempt = *attempt;
                        let started = *started;
                        let connect_timeout = self.config.connect_timeout;
                        let factory = Arc::clone(&self.factory);
                        let future: ReconnectFuture = Box::pin(async move {
                            connect_with_timeout(factory.as_ref(), connect_timeout).await
                        });
                        self.state = ConnState::Reconnecting {
                            attempt,
                            future,
                            started,
                        };
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ConnState::Reconnecting {
                    attempt,
                    future,
                    started,
                } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(conn)) => {
                        let attempt = *attempt;
                        let elapsed = started.elapsed();
                        let elapsed_ms = elapsed.as_millis();
                        self.state = ConnState::Connected(conn);
                        if let Some(disconnect_reported) = &self.disconnect_reported {
                            disconnect_reported.store(false, Ordering::Release);
                        }
                        tracing::info!(attempt, elapsed_ms, "redis: reconnected successfully");
                        if let Some(events) = &self.event_bus {
                            events.publish(ConnectionEvent::Reconnected {
                                attempts: attempt + 1,
                                elapsed,
                            });
                        }
                        if attempt > 0
                            && let Some(ref cb) = self.on_reconnect
                        {
                            cb(attempt);
                        }
                        if let Some(ref cb) = self.on_connect {
                            cb();
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        let attempt = *attempt;
                        let started = *started;
                        tracing::warn!(attempt, error = %e, "redis: reconnect attempt failed");
                        if let Some(events) = &self.event_bus {
                            events.publish_with(|| ConnectionEvent::ReconnectFailed {
                                attempt: attempt + 1,
                                error: Arc::from(e.to_string()),
                            });
                        }
                        self.trigger_reconnect(attempt + 1, started);
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let conn = match &mut self.state {
            ConnState::Connected(conn) => conn,
            _ => return Box::pin(async { Err(RedisError::ConnectionClosed) }),
        };

        let future = <RedisConnection as tower_service::Service<Cmd>>::call(conn, cmd);
        let needs_reconnect = Arc::clone(&self.needs_reconnect);
        let event_bus = self.event_bus.clone();
        let disconnect_reported = self.disconnect_reported.clone();
        let shutdown_reported = self.shutdown_reported.clone();
        let lifecycle_lock = self.lifecycle_lock.clone();

        Box::pin(async move {
            let result = future.await;
            if let Err(ref e) = result
                && e.is_connection_error()
            {
                needs_reconnect.store(true, Ordering::Release);
                publish_disconnect_before_shutdown(
                    event_bus.as_ref(),
                    disconnect_reported.as_deref(),
                    shutdown_reported.as_deref(),
                    lifecycle_lock.as_deref(),
                    e,
                );
            }
            result
        })
    }
}

pub(crate) fn publish_disconnect_once(
    event_bus: Option<&ConnectionEventBus>,
    disconnect_reported: Option<&AtomicBool>,
    error: &RedisError,
) {
    let (Some(events), Some(disconnect_reported)) = (event_bus, disconnect_reported) else {
        return;
    };
    if disconnect_reported
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    events.publish_with(|| ConnectionEvent::Disconnected {
        reason: ConnectionDisconnectReason::ConnectionError {
            error: Arc::from(error.to_string()),
        },
    });
}

pub(crate) fn publish_disconnect_before_shutdown(
    event_bus: Option<&ConnectionEventBus>,
    disconnect_reported: Option<&AtomicBool>,
    shutdown_reported: Option<&AtomicBool>,
    lifecycle_lock: Option<&StdMutex<()>>,
    error: &RedisError,
) {
    let _guard = lifecycle_lock.map(|lock| {
        lock.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    if shutdown_reported.is_some_and(|reported| reported.load(Ordering::Acquire)) {
        return;
    }
    publish_disconnect_once(event_bus, disconnect_reported, error);
}

pub(crate) fn publish_shutdown_once(
    event_bus: Option<&ConnectionEventBus>,
    shutdown_reported: Option<&AtomicBool>,
) {
    let (Some(events), Some(shutdown_reported)) = (event_bus, shutdown_reported) else {
        return;
    };
    if shutdown_reported
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    events.publish(ConnectionEvent::Disconnected {
        reason: ConnectionDisconnectReason::Shutdown,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn event_bus_delivers_ordered_events_to_multiple_subscribers() {
        let events = ConnectionEventBus::new(8);
        let mut first = events.subscribe();
        let mut second = events.subscribe();

        let expected = [
            ConnectionEvent::Connected,
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::from_millis(25),
            },
            ConnectionEvent::Reconnected {
                attempts: 1,
                elapsed: Duration::from_millis(30),
            },
            ConnectionEvent::Failover {
                previous: Some(Arc::from("redis-a:6379")),
                current: Some(Arc::from("redis-b:6379")),
            },
        ];
        for event in expected.iter().cloned() {
            assert!(events.publish(event));
        }

        for expected_event in &expected {
            assert_eq!(first.recv().await.unwrap(), expected_event.clone());
            assert_eq!(second.recv().await.unwrap(), expected_event.clone());
        }
    }

    #[tokio::test]
    async fn event_stream_reports_lag_then_continues_with_retained_event() {
        let events = ConnectionEventBus::new(1);
        let mut stream = events.subscribe();

        events.publish(ConnectionEvent::Connected);
        events.publish(ConnectionEvent::ReconnectAttempt {
            attempt: 1,
            delay: Duration::ZERO,
        });
        events.publish(ConnectionEvent::ReconnectExhausted { attempts: 1 });

        let skipped = match stream.recv().await.unwrap_err() {
            ConnectionEventRecvError::Lagged { skipped } => skipped,
            other => panic!("expected lag error, got {other:?}"),
        };
        assert!(skipped >= 1);
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectExhausted { attempts: 1 }
        );
    }

    #[tokio::test]
    async fn event_stream_reports_closed_after_last_publisher_drops() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        drop(events);

        assert_eq!(
            stream.recv().await.unwrap_err(),
            ConnectionEventRecvError::Closed
        );
    }

    #[test]
    fn lazy_publish_skips_payload_work_without_subscribers() {
        let events = ConnectionEventBus::new(4);
        let constructed = AtomicBool::new(false);

        assert!(!events.publish_with(|| {
            constructed.store(true, Ordering::Release);
            ConnectionEvent::Connected
        }));
        assert!(!constructed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_is_distinct_from_an_outage_and_emitted_once() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let disconnect_reported = AtomicBool::new(false);
        let shutdown_reported = AtomicBool::new(false);

        publish_disconnect_once(
            Some(&events),
            Some(&disconnect_reported),
            &RedisError::ConnectionClosed,
        );
        publish_shutdown_once(Some(&events), Some(&shutdown_reported));
        publish_shutdown_once(Some(&events), Some(&shutdown_reported));

        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn initial_connect_failure_is_observable() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let factory = || async { Err::<RedisConnection, _>(RedisError::ConnectionClosed) };

        let result = ResilientConnection::new_with_events(
            factory,
            ReconnectConfig::default(),
            events.clone(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed {
                error: Arc::from("connection closed"),
            }
        );
    }

    #[tokio::test]
    async fn initial_connect_timeout_is_observable() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let factory =
            || async { futures::future::pending::<Result<RedisConnection, RedisError>>().await };

        let result = ResilientConnection::new_with_events(
            factory,
            ReconnectConfig::default().connect_timeout(Duration::from_millis(10)),
            events,
        )
        .await;
        assert!(matches!(result, Err(RedisError::ConnectTimeout)));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed {
                error: Arc::from(RedisError::ConnectTimeout.to_string()),
            }
        );
    }

    #[tokio::test]
    async fn dropping_healthy_connection_publishes_shutdown_once() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _stream = stream;
            futures::future::pending::<()>().await;
        });
        let factory = move || async move {
            let stream = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(|error| RedisError::connection(addr.to_string(), error))?;
            Ok(RedisConnection::from_stream(
                redis_tower_core::RedisStream::Tcp(stream),
            ))
        };
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let connection = ResilientConnection::new_with_events(
            factory,
            ReconnectConfig::default(),
            events.clone(),
        )
        .await
        .unwrap();
        assert_eq!(stream.recv().await.unwrap(), ConnectionEvent::Connected);

        drop(connection);
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.recv())
                .await
                .is_err()
        );

        server.abort();
    }

    #[test]
    fn named_factories_retain_connection_config() {
        let limits = redis_tower_core::RespLimits {
            max_frame_size: 4096,
            max_depth: 7,
        };
        let config = ConnectionConfig::new()
            .with_protocol(ProtocolVersion::Resp2)
            .with_resp_limits(limits);

        let addr =
            AddrConnectionFactory::new("127.0.0.1:6379").with_connection_config(config.clone());
        assert_eq!(addr.connection_config.protocol(), ProtocolVersion::Resp2);
        assert_eq!(addr.connection_config.resp_limits(), limits);

        let url = UrlConnectionFactory::new("redis://127.0.0.1:6379")
            .with_connection_config(config.clone());
        assert_eq!(url.connection_config.protocol(), ProtocolVersion::Resp2);
        assert_eq!(url.connection_config.resp_limits(), limits);

        let resp3 =
            Resp3AddrConnectionFactory::new("127.0.0.1:6379").with_connection_config(config);
        assert_eq!(resp3.connection_config.protocol(), ProtocolVersion::Resp3);
        assert_eq!(resp3.connection_config.resp_limits(), limits);
    }

    #[test]
    fn named_factories_can_replace_pooled_connections() {
        fn assert_pool_factory<F: crate::pool::PoolFactory<Connection = RedisConnection>>() {}

        assert_pool_factory::<AddrConnectionFactory>();
        assert_pool_factory::<UrlConnectionFactory>();
        assert_pool_factory::<Resp3AddrConnectionFactory>();
    }

    #[test]
    fn jitter_produces_different_delays() {
        let config = ReconnectConfig::default(); // jitter: true
        // Collect 100 samples for attempt 0 (cap = 100 ms).
        // The probability that all 100 are identical is astronomically small
        // (≈ (1/100_000_000)^99 ≈ 0), so any failure here indicates a bug.
        let delays: Vec<Duration> = (0..100).map(|_| config.delay_for_attempt(0)).collect();
        let first = delays[0];
        assert!(
            delays.iter().any(|d| *d != first),
            "all 100 jittered delays were identical — jitter may not be working"
        );
    }

    #[test]
    fn jitter_delays_are_within_cap() {
        let config = ReconnectConfig::default(); // jitter: true
        let cap = Duration::from_millis(100); // attempt 0 cap
        for _ in 0..1000 {
            let d = config.delay_for_attempt(0);
            assert!(d < cap, "jittered delay {d:?} exceeded cap {cap:?}");
        }
    }

    #[test]
    fn no_jitter_produces_deterministic_delays() {
        let config = ReconnectConfig::default().jitter(false);
        let d0 = config.delay_for_attempt(0);
        let d0b = config.delay_for_attempt(0);
        assert_eq!(d0, d0b, "delays should be identical with jitter disabled");
        assert_eq!(
            d0,
            Duration::from_millis(100),
            "attempt 0 without jitter should equal base_delay"
        );
    }

    #[test]
    fn no_jitter_exponential_backoff() {
        let config = ReconnectConfig::default().jitter(false);
        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));
    }

    #[test]
    fn no_jitter_capped_at_max_delay() {
        let config = ReconnectConfig::default().jitter(false);
        // At attempt 6: 100ms * 2^6 = 6400ms > max_delay (5000ms).
        assert_eq!(config.delay_for_attempt(6), Duration::from_secs(5));
    }

    #[test]
    fn zero_cap_returns_zero() {
        // If base_delay * 2^attempt somehow rounds to 0, we should not panic.
        let config = ReconnectConfig {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: true,
            ..Default::default()
        };
        assert_eq!(config.delay_for_attempt(0), Duration::ZERO);
    }

    #[test]
    fn reconnect_config_connect_timeout() {
        let cfg = ReconnectConfig::default().connect_timeout(Duration::from_secs(2));
        assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn reconnect_config_connect_timeout_default_is_none() {
        let cfg = ReconnectConfig::default();
        assert_eq!(cfg.connect_timeout, None);
    }

    // -- retry-limit boundary tests --
    //
    // `max_retries` counts retries after the first reconnect attempt. These
    // helpers are shared by every reconnecting surface.

    #[test]
    fn max_retries_zero_allows_one_attempt() {
        let config = ReconnectConfig::default().max_retries(0);
        assert!(!config.attempt_exhausted(0));
        assert!(config.attempt_exhausted(1));
        assert_eq!(config.total_attempt_budget(), Some(1));
    }

    #[test]
    fn max_retries_three_allows_four_total_attempts() {
        let config = ReconnectConfig::default().max_retries(3);
        assert!(!config.attempt_exhausted(3));
        assert!(config.attempt_exhausted(4));
        assert_eq!(config.total_attempt_budget(), Some(4));
    }

    #[test]
    fn max_retries_none_never_fails() {
        let config = ReconnectConfig::default(); // max_retries: None
        let attempt = 9999usize;
        assert!(!config.attempt_exhausted(attempt));
        assert_eq!(config.total_attempt_budget(), None);
    }

    // -- reconnect success log includes duration --

    use std::future::poll_fn;
    use std::sync::Mutex;
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;

    /// A tracing layer that records each event's fields as `"field=value ..."`.
    #[derive(Clone, Default)]
    struct EventCapture {
        events: Arc<Mutex<Vec<String>>>,
    }

    struct FieldCollector(String);

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!(" {}={:?}", field.name(), value));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for EventCapture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut collector = FieldCollector(String::new());
            event.record(&mut collector);
            self.events.lock().unwrap().push(collector.0);
        }
    }

    #[tokio::test]
    async fn reconnect_success_log_includes_duration() {
        use tower_service::Service;

        // A local listener whose accept loop keeps the server side of each
        // loopback connection alive. The factory just needs `connect()` to
        // succeed; no Redis protocol is exchanged.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });

        let factory = move || async move {
            let stream = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(|e| RedisError::connection(addr.to_string(), e))?;
            Ok::<_, RedisError>(RedisConnection::from_stream(
                redis_tower_core::RedisStream::Tcp(stream),
            ))
        };

        let config = ReconnectConfig {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
            ..Default::default()
        };
        let lifecycle = ConnectionEventBus::new(8);
        let mut lifecycle_stream = lifecycle.subscribe();
        let mut conn = ResilientConnection::new_with_events(factory, config, lifecycle)
            .await
            .unwrap();
        assert_eq!(
            lifecycle_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        // Force a reconnect cycle and drive the state machine to completion.
        conn.needs_reconnect.store(true, Ordering::Release);
        poll_fn(|cx| {
            <ResilientConnection as Service<redis_tower_commands::Ping>>::poll_ready(&mut conn, cx)
        })
        .await
        .expect("reconnect should succeed against the loopback listener");

        assert_eq!(
            lifecycle_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::from_millis(1),
            }
        );
        assert!(matches!(
            lifecycle_stream.recv().await.unwrap(),
            ConnectionEvent::Reconnected { attempts: 1, .. }
        ));

        let events = capture.events.lock().unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.contains("reconnected successfully") && e.contains("elapsed_ms")),
            "expected a reconnect success log carrying elapsed_ms, got: {events:?}"
        );
    }
}
