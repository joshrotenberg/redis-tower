//! Auto-reconnecting connection wrapper.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_core::{Command, RedisConnection, RedisError};

/// Factory for creating new Redis connections.
pub trait ConnectionFactory: Send + Sync + 'static {
    /// Create a new connection.
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

/// A connection factory that connects via a URL string.
pub struct UrlConnectionFactory {
    url: String,
}

impl UrlConnectionFactory {
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

/// A connection factory that connects via an address string.
pub struct AddrConnectionFactory {
    addr: String,
}

impl AddrConnectionFactory {
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

/// Configuration for reconnection behavior.
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
    pub fn max_retries(mut self, n: usize) -> Self {
        self.max_retries = Some(n);
        self
    }

    pub fn base_delay(mut self, d: Duration) -> Self {
        self.base_delay = d;
        self
    }

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
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        match &self.state {
            ConnState::Connected(conn) => {
                let result = conn.execute(cmd).await;
                if let Err(ref e) = result {
                    if is_connection_error(e) {
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

/// Returns true if the error indicates a lost connection.
pub fn is_connection_error(err: &RedisError) -> bool {
    matches!(
        err,
        RedisError::Connection(_) | RedisError::ConnectionClosed
    )
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
                if is_connection_error(e) {
                    needs_reconnect.store(true, Ordering::Release);
                }
            }
            result
        })
    }
}
