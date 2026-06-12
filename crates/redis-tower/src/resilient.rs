//! Batteries-included resilient Redis client.
//!
//! [`ResilientRedisClient`] combines shared access (`Arc<Mutex<>>`) with
//! automatic reconnection on connection loss. It is the recommended
//! client for long-running applications that need to survive transient
//! network failures without manual intervention.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use redis_tower_commands::Ping;
use redis_tower_core::{Command, RedisConnection, RedisError};
use tokio::sync::Mutex;

use crate::reconnect::{
    AddrConnectionFactory, ConnectionFactory, ReconnectConfig, UrlConnectionFactory,
};

/// A shared, auto-reconnecting Redis client.
///
/// Wraps a [`RedisConnection`] with automatic reconnection on connection
/// loss. Uses `Arc<Mutex<>>` for cross-task sharing.
///
/// # Concurrency
///
/// `ResilientRedisClient` is `Clone + Send + Sync`. All clones share the same
/// `Arc<Mutex<RedisConnection>>`, serializing commands one at a time.
/// Reconnection is triggered only when a command fails with a connection error
/// (`is_connection_error()` returns true); non-connection errors (WRONGTYPE,
/// etc.) are returned to the caller without triggering reconnection. After
/// `max_retries` reconnect attempts are exhausted, the error propagates to the
/// caller; the client is not permanently broken and will attempt reconnection
/// on the next command.
///
/// # Example
///
/// ```ignore
/// use redis_tower::ResilientRedisClient;
/// use redis_tower::commands::*;
///
/// let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
///
/// let c = client.clone();
/// tokio::spawn(async move {
///     c.execute(Set::new("key", "value")).await.unwrap();
/// });
/// ```
#[derive(Clone)]
pub struct ResilientRedisClient {
    conn: Arc<Mutex<RedisConnection>>,
    factory: Arc<dyn ConnectionFactory>,
    config: ReconnectConfig,
    /// Single-flights reconnects across clones: a connection drop seen by many
    /// clones triggers one reconnect, not one storm per clone.
    gate: Arc<ReconnectGate>,
}

impl ResilientRedisClient {
    /// Connect to Redis with default reconnection settings.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::with_config(AddrConnectionFactory::new(addr), ReconnectConfig::default()).await
    }

    /// Connect via a Redis URL with default reconnection settings.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        Self::with_config(UrlConnectionFactory::new(url), ReconnectConfig::default()).await
    }

    /// Connect with a custom factory and reconnection config.
    pub async fn with_config(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        let factory: Arc<dyn ConnectionFactory> = Arc::new(factory);
        let conn = connect_with_timeout(&*factory, config.connect_timeout).await?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            factory,
            config,
            gate: Arc::new(ReconnectGate::new()),
        })
    }

    /// Execute a command, reconnecting if the connection is lost.
    ///
    /// # Retry Safety
    ///
    /// If the command fails with a connection error, the connection is
    /// reconnected automatically, but the error is returned to the caller.
    /// If the caller retries the command, be aware that the original command
    /// may have been executed by Redis before the connection dropped.
    /// Only retry commands that are [`Command::idempotent`].
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut conn = self.conn.lock().await;
        let result = conn.execute(cmd).await;

        if let Err(ref e) = result
            && e.is_connection_error()
        {
            drop(conn);
            self.reconnect().await;
        }

        result
    }

    /// Send a PING to verify the connection is alive.
    ///
    /// Returns `Ok(())` on success. Useful for Kubernetes readiness probes
    /// and `/health` endpoints.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        conn.execute(Ping::new()).await?;
        Ok(())
    }

    /// Attempt to reconnect, single-flighting across clones.
    async fn reconnect(&self) {
        // Snapshot the generation before taking the gate. If another clone
        // reconnects while we wait, the generation advances and we skip --
        // a shared connection drop triggers one reconnect, not one per clone.
        let seen = self.gate.generation();
        let _guard = self.gate.enter().await;
        if self.gate.generation() != seen {
            return;
        }

        if let Some(conn) = reconnect_campaign(&*self.factory, &self.config).await {
            *self.conn.lock().await = conn;
            self.gate.mark_reconnected();
        }
    }
}

/// Connect via the factory, bounding the attempt by `connect_timeout` if one is
/// configured so a black-holed connect cannot hang the reconnect loop forever.
async fn connect_with_timeout(
    factory: &dyn ConnectionFactory,
    connect_timeout: Option<Duration>,
) -> Result<RedisConnection, RedisError> {
    match connect_timeout {
        Some(t) => tokio::time::timeout(t, factory.connect())
            .await
            .map_err(|_| RedisError::ConnectTimeout)?,
        None => factory.connect().await,
    }
}

/// Run a reconnect campaign: try **immediately**, then back off before each
/// retry, up to `max_retries`. Returns the new connection, or `None` once the
/// budget is exhausted.
///
/// Attempting immediately (rather than sleeping a full backoff first) lets a
/// brief blip recover without an avoidable delay.
async fn reconnect_campaign(
    factory: &dyn ConnectionFactory,
    config: &ReconnectConfig,
) -> Option<RedisConnection> {
    let max = config.max_retries.unwrap_or(usize::MAX);
    for attempt in 0..=max {
        if attempt > 0 {
            let delay = config.delay_for_attempt(attempt - 1);
            tracing::warn!(attempt, delay = ?delay, "redis: backing off before reconnect");
            tokio::time::sleep(delay).await;
        }
        match connect_with_timeout(factory, config.connect_timeout).await {
            Ok(conn) => {
                tracing::info!(attempt, "redis: reconnected successfully");
                return Some(conn);
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "redis: reconnect attempt failed");
            }
        }
    }
    None
}

/// Single-flight coordinator for reconnects shared across clones.
///
/// A generation counter, bumped once per successful reconnect, lets a clone
/// that was waiting on the gate detect that the connection was already replaced
/// and skip its own attempt.
struct ReconnectGate {
    lock: Mutex<()>,
    generation: AtomicU64,
}

impl ReconnectGate {
    fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            generation: AtomicU64::new(0),
        }
    }

    /// The current reconnect generation.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Take the gate; only one clone holds it at a time, the rest wait.
    async fn enter(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    /// Advance the generation after a successful reconnect.
    fn mark_reconnected(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    type ConnFuture = Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>>;

    /// Factory whose `connect()` always fails immediately, counting calls.
    struct FailingFactory {
        calls: Arc<AtomicUsize>,
    }

    impl ConnectionFactory for FailingFactory {
        fn connect(&self) -> ConnFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(RedisError::ConnectionClosed) })
        }
    }

    /// Factory whose `connect()` never resolves, counting calls.
    struct HangingFactory {
        calls: Arc<AtomicUsize>,
    }

    impl ConnectionFactory for HangingFactory {
        fn connect(&self) -> ConnFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::pending())
        }
    }

    fn config(
        max_retries: usize,
        base: Duration,
        connect_timeout: Option<Duration>,
    ) -> ReconnectConfig {
        ReconnectConfig {
            max_retries: Some(max_retries),
            base_delay: base,
            jitter: false,
            connect_timeout,
            ..Default::default()
        }
    }

    // -- defect 1: connect_with_timeout --

    #[tokio::test]
    async fn connect_with_timeout_propagates_factory_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = FailingFactory {
            calls: calls.clone(),
        };
        // RedisConnection is not Debug, so match rather than unwrap_err.
        let result = connect_with_timeout(&factory, None).await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn connect_with_timeout_times_out_a_hanging_connect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = HangingFactory {
            calls: calls.clone(),
        };
        let start = Instant::now();
        let result = connect_with_timeout(&factory, Some(Duration::from_millis(30))).await;
        // Without the timeout this would hang forever.
        assert!(matches!(result, Err(RedisError::ConnectTimeout)));
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // -- defect 2: attempt immediately, before the first backoff --

    #[tokio::test]
    async fn reconnect_campaign_attempts_immediately_without_initial_sleep() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = FailingFactory {
            calls: calls.clone(),
        };
        // A 1s base delay: the old code slept it *before* the only attempt.
        let cfg = config(0, Duration::from_secs(1), None);
        let start = Instant::now();
        let result = reconnect_campaign(&factory, &cfg).await;
        assert!(result.is_none());
        // One immediate attempt, no pre-sleep.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "first attempt should not wait out the backoff"
        );
    }

    #[tokio::test]
    async fn reconnect_campaign_backs_off_between_retries() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = FailingFactory {
            calls: calls.clone(),
        };
        // 3 attempts total (initial + 2 retries) with tiny backoff.
        let cfg = config(2, Duration::from_millis(1), None);
        let result = reconnect_campaign(&factory, &cfg).await;
        assert!(result.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn reconnect_campaign_honors_connect_timeout_per_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = HangingFactory {
            calls: calls.clone(),
        };
        let cfg = config(0, Duration::from_millis(1), Some(Duration::from_millis(30)));
        let result = reconnect_campaign(&factory, &cfg).await;
        assert!(result.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // -- defect 3: single-flight gate --

    #[tokio::test]
    async fn gate_skips_when_another_clone_already_reconnected() {
        let gate = ReconnectGate::new();
        // Two clones both observe generation 0 before either reconnects.
        let seen_a = gate.generation();
        let seen_b = gate.generation();

        // Clone A reconnects: it sees no change, proceeds, and marks success.
        {
            let _g = gate.enter().await;
            assert_eq!(gate.generation(), seen_a);
            gate.mark_reconnected();
        }

        // Clone B now takes the gate: the generation advanced, so it skips.
        {
            let _g = gate.enter().await;
            assert_ne!(gate.generation(), seen_b);
        }
    }

    #[tokio::test]
    async fn gate_serializes_concurrent_holders() {
        let gate = Arc::new(ReconnectGate::new());
        let held = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let gate = gate.clone();
            let held = held.clone();
            let max_seen = max_seen.clone();
            handles.push(tokio::spawn(async move {
                let _g = gate.enter().await;
                let now = held.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                held.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // The gate is exclusive: never more than one holder at a time.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }
}
