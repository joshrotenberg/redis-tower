//! Redis-aware adapter for `tower-resilience` circuit breaking.
//!
//! The adapter delegates state management, half-open admission, metrics, and
//! event handling to `tower-resilience-circuitbreaker`. It preserves
//! `redis-tower`'s public `RedisError` surface by mapping an upstream open
//! rejection to [`RedisError::CircuitOpen`] and passing inner errors through.
//!
//! By default only infrastructure failures count toward opening the circuit:
//! connection failures, connect timeouts, and command timeouts. Redis command
//! errors such as `WRONGTYPE`, `NOSCRIPT`, `MOVED`, and `ASK` are returned to
//! the caller without degrading circuit health.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::{
//!     MultiplexedClient, RedisCircuitBreakerConfig,
//! };
//!
//! let client = MultiplexedClient::connect("127.0.0.1:6379")
//!     .await?
//!     .with_circuit_breaker(RedisCircuitBreakerConfig {
//!         failure_threshold: 5,
//!         recovery_probe_interval: Duration::from_secs(5),
//!     });
//!
//! let health = client.circuit_breaker_handle().health_status();
//! assert_eq!(health, "healthy");
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_commands::Ping;
use redis_tower_core::{Command, Frame, RedisError};
use tower_layer::Layer;
use tower_resilience_circuitbreaker::{
    CircuitBreaker as UpstreamCircuitBreaker, CircuitBreakerError,
    CircuitBreakerHandle as UpstreamCircuitBreakerHandle,
    CircuitBreakerLayer as UpstreamCircuitBreakerLayer, FnClassifier,
};
use tower_service::Service;

use crate::executor::RedisExecutor;
use crate::retry::{RetryClient, RetryPolicy};

/// Metrics snapshot maintained by the upstream circuit breaker.
pub use tower_resilience_circuitbreaker::CircuitMetrics as RedisCircuitMetrics;
/// Observable state of a Redis circuit breaker.
pub use tower_resilience_circuitbreaker::CircuitState as RedisCircuitState;

type ClassifierFn = fn(&Result<Frame, RedisError>) -> bool;
type RedisClassifier = FnClassifier<ClassifierFn>;
type InnerLayer = UpstreamCircuitBreakerLayer<RedisClassifier>;
type InnerHandle = UpstreamCircuitBreakerHandle<RedisClassifier>;

#[derive(Debug)]
struct BreakerRequest {
    frame: Frame,
    deadline: Option<tokio::time::Instant>,
}

/// Applies a typed command's absolute deadline inside the breaker so the
/// breaker observes and classifies an in-flight timeout.
#[derive(Clone)]
struct DeadlineFrameService<S> {
    inner: S,
}

impl<S> DeadlineFrameService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<BreakerRequest> for DeadlineFrameService<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: BreakerRequest) -> Self::Future {
        if request
            .deadline
            .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
        {
            return Box::pin(async { Err(RedisError::CommandTimeout) });
        }

        let future = self.inner.call(request.frame);
        Box::pin(async move {
            match request.deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, future)
                    .await
                    .map_err(|_elapsed| RedisError::CommandTimeout)?,
                None => future.await,
            }
        })
    }
}

/// Configuration for the Redis-aware circuit breaker.
///
/// The settings retain the semantics of the original redis-tower breaker:
/// `failure_threshold` is a consecutive-failure count and a successful call
/// resets it. One call is admitted while half-open.
#[derive(Clone, Debug)]
pub struct RedisCircuitBreakerConfig {
    /// Consecutive classified failures before the circuit opens (default: 5).
    pub failure_threshold: u32,
    /// How long to remain open before allowing a recovery probe (default: 5s).
    pub recovery_probe_interval: Duration,
}

impl Default for RedisCircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_probe_interval: Duration::from_secs(5),
        }
    }
}

/// Return whether a Redis error represents an infrastructure failure.
///
/// Connection failures, connect timeouts, and command timeouts count toward
/// opening the circuit. Server responses and caller errors do not.
pub fn redis_error_is_circuit_failure(error: &RedisError) -> bool {
    error.is_connection_error()
        || matches!(
            error,
            RedisError::ConnectTimeout | RedisError::CommandTimeout
        )
        || matches!(
            error,
            RedisError::ReconnectFailed { last_error, .. }
                if redis_error_is_circuit_failure(last_error)
        )
}

fn classify_redis_result(result: &Result<Frame, RedisError>) -> bool {
    matches!(result, Err(error) if redis_error_is_circuit_failure(error))
}

/// Read-only handle for circuit state and metrics.
#[derive(Clone)]
pub struct RedisCircuitBreakerHandle {
    inner: InnerHandle,
}

impl RedisCircuitBreakerHandle {
    /// Return the current circuit state without waiting for a lock.
    pub fn state(&self) -> RedisCircuitState {
        self.inner.state()
    }

    /// Return whether the circuit is open.
    pub fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    /// Return `healthy`, `degraded`, or `unhealthy` for health endpoints.
    pub fn health_status(&self) -> &'static str {
        self.inner.health_status()
    }

    /// Return an HTTP-compatible health status (`200` or `503`).
    pub fn http_status(&self) -> u16 {
        self.inner.http_status()
    }

    /// Snapshot the circuit's call and failure metrics.
    pub async fn metrics(&self) -> RedisCircuitMetrics {
        self.inner.metrics().await
    }
}

impl fmt::Debug for RedisCircuitBreakerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisCircuitBreakerHandle")
            .field("state", &self.state())
            .finish()
    }
}

/// Tower layer backed by `tower-resilience-circuitbreaker`.
///
/// All services produced by a layer share one circuit state. The inner service
/// must be cloneable because upstream moves a clone into each call future.
#[derive(Clone)]
pub struct RedisCircuitBreakerLayer {
    inner: InnerLayer,
    handle: RedisCircuitBreakerHandle,
    config: RedisCircuitBreakerConfig,
}

impl RedisCircuitBreakerLayer {
    /// Build a Redis-aware breaker using consecutive-failure semantics.
    pub fn new(config: RedisCircuitBreakerConfig) -> Self {
        let threshold = config.failure_threshold.max(1) as usize;
        let (inner, handle) = UpstreamCircuitBreakerLayer::builder()
            .consecutive_failures(threshold)
            .wait_duration_in_open(config.recovery_probe_interval)
            .permitted_calls_in_half_open(1)
            .name("redis")
            .failure_classifier(classify_redis_result as ClassifierFn)
            .on_state_transition(|from, to| match to {
                RedisCircuitState::Open => {
                    tracing::warn!(?from, ?to, "redis circuit breaker state transition")
                }
                _ => tracing::info!(?from, ?to, "redis circuit breaker state transition"),
            })
            .build_with_handle();
        Self {
            inner,
            handle: RedisCircuitBreakerHandle { inner: handle },
            config,
        }
    }

    /// Clone a read-only handle for health checks and operational metrics.
    pub fn handle(&self) -> RedisCircuitBreakerHandle {
        self.handle.clone()
    }
}

impl fmt::Debug for RedisCircuitBreakerLayer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisCircuitBreakerLayer")
            .field("config", &self.config)
            .field("state", &self.handle.state())
            .finish()
    }
}

impl<S> Layer<S> for RedisCircuitBreakerLayer {
    type Service = RedisCircuitBreakerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        RedisCircuitBreakerService {
            inner: self.inner.layer(DeadlineFrameService::new(service)),
            handle: self.handle.clone(),
        }
    }
}

/// RedisError-preserving service produced by [`RedisCircuitBreakerLayer`].
pub struct RedisCircuitBreakerService<S> {
    inner: UpstreamCircuitBreaker<DeadlineFrameService<S>, RedisClassifier>,
    handle: RedisCircuitBreakerHandle,
}

impl<S: Clone> Clone for RedisCircuitBreakerService<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            handle: self.handle.clone(),
        }
    }
}

impl<S> RedisCircuitBreakerService<S> {
    /// Clone a read-only handle for health checks and operational metrics.
    pub fn handle(&self) -> RedisCircuitBreakerHandle {
        self.handle.clone()
    }

    fn call_with_deadline(
        &mut self,
        frame: Frame,
        deadline: Option<tokio::time::Instant>,
    ) -> Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>
    where
        S: Service<Frame, Response = Frame, Error = RedisError> + Clone + Send + 'static,
        S::Future: Send + 'static,
    {
        let future = self.inner.call(BreakerRequest { frame, deadline });
        Box::pin(async move { future.await.map_err(map_circuit_error) })
    }
}

impl<S> fmt::Debug for RedisCircuitBreakerService<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisCircuitBreakerService")
            .field("state", &self.handle.state())
            .finish_non_exhaustive()
    }
}

fn map_circuit_error(error: CircuitBreakerError<RedisError>) -> RedisError {
    match error {
        CircuitBreakerError::OpenCircuit => RedisError::CircuitOpen,
        CircuitBreakerError::Inner(error) => error,
    }
}

impl<S> Service<Frame> for RedisCircuitBreakerService<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(map_circuit_error)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        self.call_with_deadline(request, None)
    }
}

/// Typed high-level client protected by a Redis circuit breaker.
///
/// Created by `MultiplexedClient::with_circuit_breaker` or
/// `ResilientRedisClient::with_circuit_breaker`.
#[derive(Clone)]
pub struct RedisCircuitBreakerClient<S> {
    inner: RedisCircuitBreakerService<S>,
    handle: RedisCircuitBreakerHandle,
}

impl<S> RedisCircuitBreakerClient<S> {
    /// Wrap a cloneable frame service in the Redis-aware circuit breaker.
    pub fn new(service: S, config: RedisCircuitBreakerConfig) -> Self {
        let layer = RedisCircuitBreakerLayer::new(config);
        let handle = layer.handle();
        Self {
            inner: layer.layer(service),
            handle,
        }
    }

    /// Clone the operational handle for this client's shared circuit.
    pub fn circuit_breaker_handle(&self) -> RedisCircuitBreakerHandle {
        self.handle.clone()
    }
}

impl<S> RedisCircuitBreakerClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    /// Execute a typed command through the circuit breaker.
    ///
    /// A deadline carried by [`redis_tower_core::WithDeadline`] bounds both
    /// waiting for inner readiness and the dispatched call.
    pub fn execute<Cmd: Command + 'static>(
        &self,
        command: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        let deadline = command.deadline();
        let mut service = self.inner.clone();
        async move {
            let readiness = std::future::poll_fn(|cx| service.poll_ready(cx));
            match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, readiness)
                    .await
                    .map_err(|_elapsed| RedisError::CommandTimeout)?,
                None => readiness.await,
            }?;

            if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                return Err(RedisError::CommandTimeout);
            }

            let frame = command.to_frame();
            let response = service.call_with_deadline(frame, deadline).await?;
            if let Frame::Error(ref error) = response {
                return Err(RedisError::Redis(
                    String::from_utf8_lossy(error).into_owned(),
                ));
            }
            command.parse_response(response)
        }
    }

    /// Send a PING through the breaker for a dependency health check.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        self.execute(Ping::new()).await?;
        Ok(())
    }

    /// Add idempotent-aware retries outside the circuit breaker.
    pub fn retry(&self, policy: RetryPolicy) -> RetryClient<Self> {
        RetryClient::new(self.clone(), policy)
    }
}

impl<S> RedisExecutor for RedisCircuitBreakerClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    fn execute<Cmd: Command>(
        &mut self,
        command: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        RedisCircuitBreakerClient::execute(self, command)
    }
}

/// Deprecated name for [`RedisCircuitBreakerConfig`].
#[deprecated(
    since = "0.1.0",
    note = "use RedisCircuitBreakerConfig; this alias will be removed in a future release"
)]
pub type CircuitBreakerConfig = RedisCircuitBreakerConfig;

/// Deprecated name for [`RedisCircuitBreakerLayer`].
#[deprecated(
    since = "0.1.0",
    note = "use RedisCircuitBreakerLayer; this alias will be removed in a future release"
)]
pub type CircuitBreakerLayer = RedisCircuitBreakerLayer;

/// Deprecated name for [`RedisCircuitBreakerService`].
#[deprecated(
    since = "0.1.0",
    note = "use RedisCircuitBreakerService; this alias will be removed in a future release"
)]
pub type CircuitBreakerService<S> = RedisCircuitBreakerService<S>;

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::WithDeadline;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Clone)]
    struct ToggleService {
        fail: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
        user_error: bool,
    }

    impl Service<Frame> for ToggleService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.user_error {
                std::future::ready(Err(RedisError::Redis(
                    "WRONGTYPE Operation against a key".into(),
                )))
            } else if self.fail.load(Ordering::SeqCst) {
                std::future::ready(Err(RedisError::ConnectionClosed))
            } else {
                std::future::ready(Ok(Frame::SimpleString("PONG".into())))
            }
        }
    }

    #[derive(Clone)]
    struct NeverReadyService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<Frame> for NeverReadyService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            self.calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(Frame::SimpleString("PONG".into())))
        }
    }

    #[derive(Clone)]
    struct SlowService {
        calls: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl Service<Frame> for SlowService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(Frame::SimpleString("PONG".into()))
            })
        }
    }

    fn ping() -> Frame {
        Frame::Array(Some(vec![Frame::BulkString(Some("PING".into()))]))
    }

    #[tokio::test]
    async fn typed_deadline_bounds_circuit_breaker_readiness() {
        let calls = Arc::new(AtomicUsize::new(0));
        let config = RedisCircuitBreakerConfig {
            failure_threshold: 1,
            recovery_probe_interval: Duration::from_secs(5),
        };
        let client = RedisCircuitBreakerClient::new(
            NeverReadyService {
                calls: Arc::clone(&calls),
            },
            config,
        );
        let handle = client.circuit_breaker_handle();

        let result = client
            .execute(WithDeadline::after(Ping::new(), Duration::from_millis(20)))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(handle.state(), RedisCircuitState::Closed);
        assert_eq!(handle.metrics().await.total_calls, 0);
    }

    #[tokio::test]
    async fn dispatched_typed_deadline_counts_as_circuit_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = RedisCircuitBreakerClient::new(
            SlowService {
                calls: Arc::clone(&calls),
                delay: Duration::from_millis(100),
            },
            RedisCircuitBreakerConfig {
                failure_threshold: 1,
                recovery_probe_interval: Duration::from_secs(5),
            },
        );
        let handle = client.circuit_breaker_handle();

        let result = client
            .execute(WithDeadline::after(Ping::new(), Duration::from_millis(20)))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(handle.state(), RedisCircuitState::Open);
    }

    #[tokio::test]
    async fn infrastructure_failure_opens_and_maps_rejection() {
        let calls = Arc::new(AtomicUsize::new(0));
        let layer = RedisCircuitBreakerLayer::new(RedisCircuitBreakerConfig {
            failure_threshold: 1,
            recovery_probe_interval: Duration::from_secs(5),
        });
        let handle = layer.handle();
        let mut service = layer.layer(ToggleService {
            fail: Arc::new(AtomicBool::new(true)),
            calls: Arc::clone(&calls),
            user_error: false,
        });

        assert!(matches!(
            service.call(ping()).await,
            Err(RedisError::ConnectionClosed)
        ));
        assert_eq!(handle.state(), RedisCircuitState::Open);
        assert!(matches!(
            service.call(ping()).await,
            Err(RedisError::CircuitOpen)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn user_error_does_not_degrade_circuit_health() {
        let calls = Arc::new(AtomicUsize::new(0));
        let layer = RedisCircuitBreakerLayer::new(RedisCircuitBreakerConfig {
            failure_threshold: 1,
            recovery_probe_interval: Duration::from_secs(5),
        });
        let handle = layer.handle();
        let mut service = layer.layer(ToggleService {
            fail: Arc::new(AtomicBool::new(false)),
            calls: Arc::clone(&calls),
            user_error: true,
        });

        for _ in 0..3 {
            assert!(matches!(
                service.call(ping()).await,
                Err(RedisError::Redis(message)) if message.starts_with("WRONGTYPE")
            ));
        }
        assert_eq!(handle.state(), RedisCircuitState::Closed);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn successful_half_open_probe_closes_circuit() {
        let fail = Arc::new(AtomicBool::new(true));
        let layer = RedisCircuitBreakerLayer::new(RedisCircuitBreakerConfig {
            failure_threshold: 1,
            recovery_probe_interval: Duration::from_millis(20),
        });
        let handle = layer.handle();
        let mut service = layer.layer(ToggleService {
            fail: Arc::clone(&fail),
            calls: Arc::new(AtomicUsize::new(0)),
            user_error: false,
        });

        let _ = service.call(ping()).await;
        assert_eq!(handle.state(), RedisCircuitState::Open);

        fail.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(service.call(ping()).await.is_ok());
        assert_eq!(handle.state(), RedisCircuitState::Closed);
    }

    #[test]
    fn classifier_counts_only_infrastructure_failures() {
        assert!(redis_error_is_circuit_failure(
            &RedisError::ConnectionClosed
        ));
        assert!(redis_error_is_circuit_failure(&RedisError::ConnectTimeout));
        assert!(redis_error_is_circuit_failure(&RedisError::CommandTimeout));
        assert!(redis_error_is_circuit_failure(
            &RedisError::ReconnectFailed {
                attempts: 1,
                last_error: Arc::new(RedisError::ConnectTimeout),
            }
        ));
        assert!(!redis_error_is_circuit_failure(&RedisError::Redis(
            "WRONGTYPE bad value".into()
        )));
        assert!(!redis_error_is_circuit_failure(&RedisError::Redis(
            "MOVED 1 127.0.0.1:6379".into()
        )));
    }
}
