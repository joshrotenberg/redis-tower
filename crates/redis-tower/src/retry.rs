//! Idempotent-aware automatic retries at the command altitude.
//!
//! [`RetryLayer`] is a Tower [`Layer`] that wraps any
//! command-level `Service<Cmd>` (for example
//! [`ExecutorService`] over a real client, or a
//! [`CommandAdapter`](crate::CommandAdapter)) and retries failed commands
//! according to a [`RetryPolicy`].
//!
//! The retry decision runs at the **command altitude**, above the point where
//! a typed [`Command`] is lowered to a [`Frame`](redis_tower_core::Frame), so
//! the policy can read [`Command::idempotent`]. That matters: retrying a
//! non-idempotent write (INCR, LPUSH, ...) after a connection error can
//! silently duplicate data, because the first attempt may have reached Redis
//! before the connection dropped. The default policy therefore retries only
//! when the command is idempotent **and** the error is
//! [retryable](redis_tower_core::RedisError::is_retryable).
//!
//! This is a safety guarantee neither redis-rs nor fred offer: automatic
//! retries that are on by default only for the commands where they cannot
//! corrupt data.
//!
//! # Two ways to use it
//!
//! Compose it as a Tower layer over a bridged client:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{ExecutorService, MultiplexedClient, RetryLayer, RetryPolicy};
//! use tower::ServiceBuilder;
//!
//! let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let service = ServiceBuilder::new()
//!     .layer(RetryLayer::new(RetryPolicy::default()))
//!     .service(ExecutorService::new(client));
//! # let _ = service;
//! # Ok(())
//! # }
//! ```
//!
//! Or opt in directly on a client via its `retry` combinator, which returns a
//! [`RetryClient`] with an ergonomic `execute`:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{MultiplexedClient, RetryPolicy};
//! use redis_tower::commands::Get;
//!
//! let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let retrying = client.retry(RetryPolicy::default());
//! let value: Option<bytes::Bytes> = retrying.execute(Get::new("key")).await?;
//! # let _ = value;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_core::{Command, RedisError};
use tower_layer::Layer;
use tower_service::Service;

use crate::executor::{ExecutorService, RedisExecutor};

/// Policy controlling idempotent-aware retries.
///
/// A retry happens only when [`should_retry`](Self::should_retry) returns
/// `true` and the attempt budget ([`max_retries`](Self::max_retries)) is not
/// yet exhausted. The default decision is `idempotent && err.is_retryable()`:
/// safe automatic retries for read-only / naturally-idempotent commands, and
/// no silent write duplication for the rest.
///
/// Backoff mirrors the reconnect backoff: exponential
/// `base_delay * 2^attempt`, capped at `max_delay`, with optional full jitter.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries **after** the first attempt. A value of `0`
    /// disables retrying (one attempt only). Total attempts are
    /// `max_retries + 1`.
    pub max_retries: usize,
    /// Base delay used for the first retry; doubles each subsequent retry.
    pub base_delay: Duration,
    /// Upper bound on any single backoff delay.
    pub max_delay: Duration,
    /// Whether to apply full jitter to each backoff delay.
    ///
    /// When `true` (the default), each delay is a uniformly random value in
    /// `[0, cap)` where `cap` is the un-jittered exponential delay, spreading
    /// retries to avoid thundering-herd storms. When `false`, delays are the
    /// deterministic `base_delay * 2^attempt` (useful in tests).
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Create a policy with a given retry budget and otherwise-default backoff.
    #[must_use]
    pub fn new(max_retries: usize) -> Self {
        Self {
            max_retries,
            ..Self::default()
        }
    }

    /// Set the maximum number of retries after the first attempt.
    #[must_use]
    pub fn max_retries(mut self, n: usize) -> Self {
        self.max_retries = n;
        self
    }

    /// Set the base delay used for the first retry.
    #[must_use]
    pub fn base_delay(mut self, d: Duration) -> Self {
        self.base_delay = d;
        self
    }

    /// Set the cap applied to each backoff delay.
    #[must_use]
    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = d;
        self
    }

    /// Enable or disable full jitter on backoff delays.
    #[must_use]
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    /// Decide whether a failed command should be retried.
    ///
    /// The default policy retries only when the command is idempotent and the
    /// error is [retryable](RedisError::is_retryable). This is what makes
    /// automatic retries safe by default: a non-idempotent write is never
    /// re-sent after a connection error, so it cannot be silently duplicated.
    pub fn should_retry(&self, idempotent: bool, err: &RedisError) -> bool {
        idempotent && err.is_retryable()
    }

    /// Backoff delay before the retry with 0-based index `attempt` (the first
    /// retry is `attempt = 0`).
    fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let cap = self
            .base_delay
            .saturating_mul(1u32.wrapping_shl(attempt.min(31) as u32))
            .min(self.max_delay);

        if self.jitter {
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

/// Tower [`Layer`] that wraps a command-level service with idempotent-aware
/// retries. See the [module docs](crate::retry).
#[derive(Debug, Clone)]
pub struct RetryLayer {
    policy: RetryPolicy,
}

impl RetryLayer {
    /// Create a retry layer from a [`RetryPolicy`].
    pub fn new(policy: RetryPolicy) -> Self {
        Self { policy }
    }
}

impl<S> Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryService {
            inner,
            policy: self.policy.clone(),
        }
    }
}

/// A `Service<Cmd>` that retries failed commands per a [`RetryPolicy`].
///
/// Requires `Cmd: Clone` because each attempt re-issues a fresh copy of the
/// command. Produced by [`RetryLayer`], or constructed directly with
/// [`RetryService::new`].
#[derive(Debug, Clone)]
pub struct RetryService<S> {
    inner: S,
    policy: RetryPolicy,
}

impl<S> RetryService<S> {
    /// Wrap an inner command-level service with retries.
    pub fn new(inner: S, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }
}

impl<S, Cmd> Service<Cmd> for RetryService<S>
where
    S: Service<Cmd, Response = Cmd::Response, Error = RedisError> + Clone + Send + 'static,
    S::Future: Send,
    Cmd: Command + Clone + Send + 'static,
{
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        // Clone the inner service into the future so it owns a `'static`
        // handle. For every redis-tower client this is a cheap Arc bump that
        // shares the same connection, so retries reuse the live pipeline.
        let mut inner = self.inner.clone();
        let policy = self.policy.clone();
        Box::pin(async move {
            let idempotent = cmd.idempotent();
            let mut attempt = 0usize;
            loop {
                std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
                let result = inner.call(cmd.clone()).await;
                match result {
                    Ok(response) => return Ok(response),
                    Err(err) => {
                        if attempt < policy.max_retries && policy.should_retry(idempotent, &err) {
                            let delay = policy.delay_for_attempt(attempt);
                            tracing::debug!(
                                attempt = attempt + 1,
                                max_retries = policy.max_retries,
                                delay = ?delay,
                                error = %err,
                                "redis: retrying idempotent command after retryable error"
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
        })
    }
}

/// An ergonomic retrying wrapper around a cloneable [`RedisExecutor`].
///
/// Returned by the `retry` combinator on [`MultiplexedClient`] and
/// [`ResilientRedisClient`]. It bridges the client to a command-level
/// [`Service`] via [`ExecutorService`] and applies a [`RetryService`], then
/// exposes a familiar `execute` that reissues idempotent commands on retryable
/// errors.
///
/// `execute` requires `Cmd: Clone` (every command builder derives it) because a
/// retry re-sends a copy of the command.
///
/// [`MultiplexedClient`]: crate::MultiplexedClient
/// [`ResilientRedisClient`]: crate::ResilientRedisClient
#[derive(Clone, Debug)]
pub struct RetryClient<C> {
    inner: RetryService<ExecutorService<C>>,
}

impl<C> RetryClient<C>
where
    C: RedisExecutor + Clone + Send + 'static,
{
    /// Wrap a cloneable [`RedisExecutor`] with a [`RetryPolicy`].
    pub fn new(client: C, policy: RetryPolicy) -> Self {
        Self {
            inner: RetryService::new(ExecutorService::new(client), policy),
        }
    }

    /// Execute a command, retrying per the policy.
    ///
    /// Idempotent commands are re-issued on retryable errors up to the policy
    /// budget; non-idempotent commands are never retried, so a write cannot be
    /// silently duplicated.
    pub async fn execute<Cmd>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command + Clone + Send + 'static,
    {
        let mut svc = self.inner.clone();
        std::future::poll_fn(|cx| {
            <RetryService<ExecutorService<C>> as Service<Cmd>>::poll_ready(&mut svc, cx)
        })
        .await?;
        <RetryService<ExecutorService<C>> as Service<Cmd>>::call(&mut svc, cmd).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_commands::{Get, Incr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A `RedisExecutor` that fails the first `fail_times` calls with a
    /// configurable error, then succeeds, counting every call.
    #[derive(Clone)]
    struct FlakyExecutor {
        calls: Arc<AtomicUsize>,
        fail_times: usize,
        error: Arc<dyn Fn() -> RedisError + Send + Sync>,
    }

    impl FlakyExecutor {
        fn new(fail_times: usize, error: impl Fn() -> RedisError + Send + Sync + 'static) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                fail_times,
                error: Arc::new(error),
            }
        }
    }

    impl RedisExecutor for FlakyExecutor {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let fail = n < self.fail_times;
            let err = (self.error)();
            async move {
                if fail {
                    Err(err)
                } else {
                    // On success, parse a null frame -- fine for GET
                    // (`Option<Bytes>` -> None) and INCR is never reached
                    // here because it is non-idempotent and not retried.
                    cmd.parse_response(redis_tower_core::Frame::Null)
                }
            }
        }
    }

    fn no_jitter(max_retries: usize) -> RetryPolicy {
        RetryPolicy::new(max_retries)
            .base_delay(Duration::from_millis(0))
            .jitter(false)
    }

    #[test]
    fn default_policy_retries_only_idempotent_retryable() {
        let policy = RetryPolicy::default();
        // Idempotent + retryable -> retry.
        assert!(policy.should_retry(true, &RedisError::ConnectionClosed));
        // Non-idempotent + retryable -> do not retry (write-duplication guard).
        assert!(!policy.should_retry(false, &RedisError::ConnectionClosed));
        // Idempotent + non-retryable -> do not retry.
        assert!(!policy.should_retry(true, &RedisError::Redis("WRONGTYPE".into())));
        // Neither -> do not retry.
        assert!(!policy.should_retry(false, &RedisError::Redis("WRONGTYPE".into())));
    }

    #[test]
    fn delay_grows_exponentially_and_caps() {
        let policy = RetryPolicy::new(5)
            .base_delay(Duration::from_millis(10))
            .max_delay(Duration::from_millis(35))
            .jitter(false);
        assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(10));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(20));
        // 40ms would exceed the 35ms cap.
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(35));
        assert_eq!(policy.delay_for_attempt(9), Duration::from_millis(35));
    }

    #[tokio::test]
    async fn retries_idempotent_command_until_success() {
        // Fail twice, then succeed: with a budget of 3 the third attempt wins.
        let exec = FlakyExecutor::new(2, || RedisError::ConnectionClosed);
        let counter = exec.calls.clone();
        let client = RetryClient::new(exec, no_jitter(3));

        let result: Option<bytes::Bytes> = client.execute(Get::new("k")).await.unwrap();
        assert_eq!(result, None);
        // 1 initial attempt + 2 retries = 3 calls.
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_budget_exhausted() {
        // Always fails; budget of 2 retries means 3 attempts then an error.
        let exec = FlakyExecutor::new(usize::MAX, || RedisError::ConnectionClosed);
        let counter = exec.calls.clone();
        let client = RetryClient::new(exec, no_jitter(2));

        let result = client.execute(Get::new("k")).await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_non_idempotent_command() {
        // INCR is non-idempotent: a single connection failure must not be
        // retried, or the counter could be double-incremented.
        let exec = FlakyExecutor::new(usize::MAX, || RedisError::ConnectionClosed);
        let counter = exec.calls.clone();
        let client = RetryClient::new(exec, no_jitter(5));

        let result = client.execute(Incr::new("k")).await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
        // Exactly one attempt, no retries.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_retry_non_retryable_error() {
        // A WRONGTYPE is a caller error, not a transient fault: never retried
        // even for an idempotent command.
        let exec = FlakyExecutor::new(usize::MAX, || RedisError::Redis("WRONGTYPE".into()));
        let counter = exec.calls.clone();
        let client = RetryClient::new(exec, no_jitter(5));

        let result = client.execute(Get::new("k")).await;
        assert!(matches!(result, Err(RedisError::Redis(_))));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_budget_disables_retries() {
        let exec = FlakyExecutor::new(usize::MAX, || RedisError::ConnectionClosed);
        let counter = exec.calls.clone();
        let client = RetryClient::new(exec, no_jitter(0));

        let result = client.execute(Get::new("k")).await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_layer_wraps_a_service() {
        // Exercise the Tower Layer path: RetryLayer over an ExecutorService.
        use tower_layer::Layer;

        let exec = FlakyExecutor::new(1, || RedisError::ConnectionClosed);
        let counter = exec.calls.clone();
        let mut svc = RetryLayer::new(no_jitter(3)).layer(ExecutorService::new(exec));

        std::future::poll_fn(|cx| Service::<Get>::poll_ready(&mut svc, cx))
            .await
            .unwrap();
        let result: Option<bytes::Bytes> = svc.call(Get::new("k")).await.unwrap();
        assert_eq!(result, None);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
