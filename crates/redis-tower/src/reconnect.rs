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
//! ```ignore
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
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_core::{Command, RedisConnection, RedisError};

/// Factory for creating new Redis connections.
///
/// Used by [`ResilientConnection`] and [`ResilientRedisClient`](crate::ResilientRedisClient)
/// to establish fresh connections during initial setup and reconnection.
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
}

impl UrlConnectionFactory {
    /// Create a new factory from the given Redis URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl ConnectionFactory for UrlConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let url = self.url.clone();
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
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts. `None` means infinite.
    pub max_retries: Option<usize>,
    /// Initial delay before first reconnection attempt.
    pub base_delay: Duration,
    /// Maximum delay between attempts (caps exponential backoff).
    pub max_delay: Duration,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: None,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl ReconnectConfig {
    /// Set the maximum number of reconnection attempts.
    pub fn max_retries(mut self, n: usize) -> Self {
        self.max_retries = Some(n);
        self
    }

    /// Set the initial delay before the first reconnection attempt.
    pub fn base_delay(mut self, d: Duration) -> Self {
        self.base_delay = d;
        self
    }

    /// Set the maximum delay between reconnection attempts.
    ///
    /// Caps the exponential backoff so delays do not grow unbounded.
    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = d;
        self
    }

    pub(crate) fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let delay = self
            .base_delay
            .saturating_mul(1u32.wrapping_shl(attempt.min(31) as u32));
        delay.min(self.max_delay)
    }
}

type ReconnectFuture = Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>>;

pub(crate) enum ConnState {
    Connected(RedisConnection),
    WaitingToReconnect {
        attempt: usize,
        sleep: Pin<Box<tokio::time::Sleep>>,
    },
    Reconnecting {
        attempt: usize,
        future: ReconnectFuture,
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
/// ```ignore
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
        let conn = factory.connect().await?;
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
                if let Err(ref e) = result {
                    if e.is_connection_error() {
                        self.needs_reconnect.store(true, Ordering::Release);
                    }
                }
                result
            }
            _ => Err(RedisError::ConnectionClosed),
        }
    }

    fn trigger_reconnect(&mut self, attempt: usize) {
        if let Some(max) = self.config.max_retries {
            if attempt > max {
                self.state = ConnState::Failed;
                return;
            }
        }
        let delay = self.config.delay_for_attempt(attempt);
        self.state = ConnState::WaitingToReconnect {
            attempt,
            sleep: Box::pin(tokio::time::sleep(delay)),
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
            self.trigger_reconnect(0);
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
                ConnState::WaitingToReconnect { attempt, sleep } => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let attempt = *attempt;
                        let future = self.factory.connect();
                        self.state = ConnState::Reconnecting { attempt, future };
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ConnState::Reconnecting { attempt, future } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(conn)) => {
                        let attempt = *attempt;
                        self.state = ConnState::Connected(conn);
                        if attempt > 0 {
                            if let Some(ref cb) = self.on_reconnect {
                                cb(attempt);
                            }
                        }
                        if let Some(ref cb) = self.on_connect {
                            cb();
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(_)) => {
                        let next = *attempt + 1;
                        self.trigger_reconnect(next);
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
            if let Err(ref e) = result {
                if e.is_connection_error() {
                    needs_reconnect.store(true, Ordering::Release);
                }
            }
            result
        })
    }
}
