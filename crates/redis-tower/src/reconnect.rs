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
//! - [`AddrConnectionFactory`] -- plain TCP, RESP2, no auth
//! - [`UrlConnectionFactory`] -- AUTH + SELECT from URL parameters, RESP2
//! - [`Resp3AddrConnectionFactory`] -- plain TCP, RESP3 via `HELLO 3`, no auth
//!
//! For RESP3 with authentication, implement [`ConnectionFactory`] yourself.
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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Command, RedisConnection, RedisError};

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
/// [`AddrConnectionFactory`] and [`UrlConnectionFactory`].
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

/// A [`ConnectionFactory`] that connects via a Redis URL string.
///
/// Supports `redis://`, `rediss://` (TLS), and `unix://` schemes.
///
/// This factory calls [`RedisConnection::connect_url`], which runs
/// `post_connect_setup` internally. This means AUTH and SELECT are
/// replayed on every reconnection based on the URL parameters. Use
/// this factory (not [`AddrConnectionFactory`]) when your Redis server
/// requires authentication or a non-default database.
pub struct UrlConnectionFactory {
    url: String,
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
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        }
    }

    /// Use an explicit TLS config (custom root CA or mTLS client certificate)
    /// for every connection this factory makes.
    ///
    /// Without this, a `rediss://` URL uses the default rustls config -- so URL
    /// connect and custom TLS were previously mutually exclusive, which made
    /// reconnect-with-auth plus a private CA impossible. With it, the factory
    /// connects via [`RedisConnection::connect_url_with_tls`] on every attempt.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn with_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.tls = Some(std::sync::Arc::new(tls));
        self
    }
}

impl ConnectionFactory for UrlConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let url = self.url.clone();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        if let Some(tls) = self.tls.clone() {
            return Box::pin(
                async move { RedisConnection::connect_url_with_tls(&url, &tls).await },
            );
        }
        Box::pin(async move { RedisConnection::connect_url(&url).await })
    }
}

/// A [`ConnectionFactory`] that connects via a `host:port` address string.
///
/// This factory creates plain TCP connections using RESP2 with no
/// authentication or database selection. If you need AUTH, SELECT, or
/// RESP3 negotiation on reconnect, use [`UrlConnectionFactory`] or
/// [`Resp3AddrConnectionFactory`] instead.
pub struct AddrConnectionFactory {
    addr: String,
}

impl AddrConnectionFactory {
    /// Create a new factory from the given `host:port` address.
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

impl ConnectionFactory for AddrConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        Box::pin(async move { RedisConnection::connect(&addr).await })
    }
}

/// A [`ConnectionFactory`] that connects via a `host:port` address and
/// negotiates RESP3 using `HELLO 3`.
///
/// Use this when you need RESP3 protocol without URL-based AUTH/SELECT.
/// For RESP3 with authentication, use [`UrlConnectionFactory`] with a
/// `redis://` URL (which handles AUTH and SELECT) and then upgrade the
/// protocol yourself, or implement [`ConnectionFactory`] directly.
pub struct Resp3AddrConnectionFactory {
    addr: String,
}

impl Resp3AddrConnectionFactory {
    /// Create a new factory from the given `host:port` address.
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

impl ConnectionFactory for Resp3AddrConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        Box::pin(async move { RedisConnection::connect_resp3(&addr).await })
    }
}

/// Configuration for reconnection behavior.
///
/// Controls the exponential backoff strategy used by [`ResilientConnection`]
/// and [`ResilientRedisClient`](crate::ResilientRedisClient).
///
/// # Defaults
///
/// - `max_retries`: `None` (infinite)
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
    /// Maximum number of reconnection attempts. `None` means infinite.
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
    /// Per-attempt connect timeout applied to each `factory.connect()` call.
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
    /// Set the maximum number of reconnection attempts.
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
    /// [`tokio::time::timeout`]. If the factory does not complete within
    /// this duration the attempt is counted as a failure and the reconnect
    /// loop retries after the next backoff delay.
    ///
    /// When not set (the default), connection attempts run without a timeout
    /// and may block for the OS-default TCP timeout — potentially several
    /// minutes on an unreachable host.
    #[must_use]
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = Some(d);
        self
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
/// | Factory | AUTH | SELECT | RESP3 |
/// |---------|------|--------|-------|
/// | [`AddrConnectionFactory`] | No | No | No |
/// | [`UrlConnectionFactory`] | Yes (from URL) | Yes (from URL) | No |
/// | [`Resp3AddrConnectionFactory`] | No | No | Yes |
///
/// For RESP3 with authentication, implement [`ConnectionFactory`] yourself
/// or use a closure factory.
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
    pub(crate) on_connect: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(crate) on_reconnect: Option<Arc<dyn Fn(usize) + Send + Sync>>,
}

impl ResilientConnection {
    /// Create a new resilient connection.
    pub async fn new(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        let factory = Arc::new(factory);
        let conn = if let Some(t) = config.connect_timeout {
            let r = tokio::time::timeout(t, factory.connect())
                .await
                .map_err(|_| RedisError::ConnectTimeout)?;
            r?
        } else {
            factory.connect().await?
        };
        Ok(Self {
            factory,
            config,
            state: ConnState::Connected(conn),
            needs_reconnect: Arc::new(AtomicBool::new(false)),
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
        if let Some(max) = self.config.max_retries
            && attempt > max
        {
            self.state = ConnState::Failed;
            return;
        }
        let delay = self.config.delay_for_attempt(attempt);
        tracing::warn!(attempt, delay = ?delay, "redis: connection lost, reconnecting");
        self.state = ConnState::WaitingToReconnect {
            attempt,
            sleep: Box::pin(tokio::time::sleep(delay)),
            started,
        };
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
                        attempts: self.config.max_retries.unwrap_or(0),
                        last_error: Box::new(RedisError::ConnectionClosed),
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
                        let future: ReconnectFuture = if let Some(t) = connect_timeout {
                            let inner = self.factory.connect();
                            Box::pin(async move {
                                tokio::time::timeout(t, inner)
                                    .await
                                    .map_err(|_| RedisError::ConnectTimeout)?
                            })
                        } else {
                            self.factory.connect()
                        };
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
                        let elapsed_ms = started.elapsed().as_millis();
                        self.state = ConnState::Connected(conn);
                        tracing::info!(attempt, elapsed_ms, "redis: reconnected successfully");
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

        Box::pin(async move {
            let result = future.await;
            if let Err(ref e) = result
                && e.is_connection_error()
            {
                needs_reconnect.store(true, Ordering::Release);
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    // `trigger_reconnect` transitions to `ConnState::Failed` when
    // `attempt > max_retries`. `ResilientConnection::new` requires a live
    // connection, so we exercise the same predicate `trigger_reconnect` uses
    // against the config directly.

    #[test]
    fn max_retries_zero_allows_one_attempt() {
        // max_retries: Some(0) means attempt 0 is allowed but attempt 1 fails.
        let config = ReconnectConfig::default().max_retries(0);
        let should_fail =
            |attempt: usize| config.max_retries.map(|max| attempt > max).unwrap_or(false);
        assert!(!should_fail(0), "attempt 0 should be within max_retries 0");
        assert!(should_fail(1), "attempt 1 should exceed max_retries 0");
    }

    #[test]
    fn max_retries_none_never_fails() {
        let config = ReconnectConfig::default(); // max_retries: None
        let attempt = 9999usize;
        let should_fail = config.max_retries.map(|max| attempt > max).unwrap_or(false);
        assert!(!should_fail, "max_retries: None should never fail");
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
        let mut conn = ResilientConnection::new(factory, config).await.unwrap();

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

        let events = capture.events.lock().unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.contains("reconnected successfully") && e.contains("elapsed_ms")),
            "expected a reconnect success log carrying elapsed_ms, got: {events:?}"
        );
    }
}
