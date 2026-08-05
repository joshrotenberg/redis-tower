//! Tower layer that enforces static and per-command deadlines.
//!
//! [`CommandTimeoutLayer`] wraps any inner service and cancels calls that
//! exceed the configured duration, returning [`RedisError::CommandTimeout`].
//! Its default form remains generic over every request type. Call
//! [`CommandTimeoutLayer::with_request_deadlines`] when requests implement
//! [`RequestDeadline`] and an earlier absolute deadline carried by
//! [`WithDeadline`](redis_tower_core::WithDeadline) should shorten that static
//! limit.
//!
//! Implemented without the `tower` crate as a production dependency — uses
//! [`tokio::time::timeout`] internally together with the `tower-service` and
//! `tower-layer` crates that are already production dependencies of
//! `redis-tower`.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{RedisConnection, CommandTimeoutLayer};
//! use tower_layer::Layer;
//! use std::time::Duration;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let svc = CommandTimeoutLayer::new(Duration::from_secs(5)).layer(conn);
//! # let _ = svc;
//! # Ok(())
//! # }
//! ```
//!
//! # One end-to-end budget
//!
//! Put the timeout layer outside [`ExecutorService`](crate::ExecutorService)
//! and wrap individual commands in [`WithDeadline`](redis_tower_core::WithDeadline).
//! The same absolute instant is then visible to the outer timeout and to a
//! [`ConnectionPool`](crate::ConnectionPool) while it waits for a slot:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::{
//!     CommandTimeoutLayer, ConnectionPool, ExecutorService, RedisConnection,
//!     WithDeadline, commands::Get,
//! };
//! use tower_layer::Layer;
//! use tower_service::Service;
//!
//! let pool = ConnectionPool::connect(4, || {
//!     RedisConnection::connect("127.0.0.1:6379")
//! }).await?;
//! let mut service = CommandTimeoutLayer::new(Duration::from_secs(5))
//!     .with_request_deadlines()
//!     .layer(ExecutorService::new(pool));
//!
//! let command = WithDeadline::after(Get::new("key"), Duration::from_millis(250));
//! let _value = service.call(command).await?;
//! # Ok(())
//! # }
//! ```
//!
//! The limits combine as follows:
//!
//! - the absolute request deadline is never reset by cloning, retrying, or
//!   waiting for the pool;
//! - the deadline-aware layer's configured duration is an additional upper
//!   bound for the whole call;
//! - [`PoolConfig::acquisition_timeout`](crate::PoolConfig::acquisition_timeout)
//!   bounds only pool waiting, and the pool returns
//!   [`RedisError::PoolAcquisitionTimeout`] when that static limit wins;
//! - connection-establishment timeouts and Redis's server-side blocking-command
//!   timeouts are separate controls.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_core::{RedisError, RequestDeadline};
use tower_layer::Layer;
use tower_service::Service;

/// A Tower [`Layer`] that enforces a static per-call timeout.
///
/// The configured duration is a static upper bound measured from
/// [`Service::call`]. It intentionally accepts any request type, preserving the
/// generic behavior of this API. To also inspect typed per-request deadlines,
/// call [`with_request_deadlines`](Self::with_request_deadlines) before applying
/// the layer.
///
/// Unlike `tower::timeout::TimeoutLayer`, this implementation depends only on
/// `tower-service`, `tower-layer`, and `tokio` — all already production
/// dependencies of `redis-tower` — so the `tower` crate itself is not
/// required as a non-dev dependency.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::{RedisConnection, CommandTimeoutLayer};
/// use tower_layer::Layer;
/// use std::time::Duration;
///
/// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let svc = CommandTimeoutLayer::new(Duration::from_secs(5)).layer(conn);
/// # let _ = svc;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CommandTimeoutLayer {
    duration: Duration,
}

impl CommandTimeoutLayer {
    /// Create a layer with the given static per-command timeout.
    ///
    /// Call [`Self::with_request_deadlines`] when a request-specific absolute
    /// deadline should shorten, but never extend, this duration.
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }

    /// Opt in to deadline extraction from requests.
    ///
    /// The returned layer requires requests to implement [`RequestDeadline`]
    /// and uses the earlier of the configured duration and the request's
    /// absolute deadline. Typed [`Command`](redis_tower_core::Command) values
    /// implement that trait automatically; raw [`Frame`](redis_tower_core::Frame)
    /// requests retain the configured static timeout.
    ///
    /// Keeping this opt-in separate lets [`CommandTimeoutLayer::new`] continue
    /// to wrap arbitrary services whose request types do not carry deadline
    /// metadata.
    #[must_use]
    pub fn with_request_deadlines(self) -> RequestDeadlineTimeoutLayer {
        RequestDeadlineTimeoutLayer {
            duration: self.duration,
        }
    }
}

impl<S> Layer<S> for CommandTimeoutLayer {
    type Service = CommandTimeoutService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CommandTimeoutService {
            inner,
            duration: self.duration,
        }
    }
}

/// The service produced by [`CommandTimeoutLayer`].
///
/// Wraps the inner service's `call` future in [`tokio::time::timeout`]. If the
/// inner future does not resolve within the configured duration, the call
/// returns [`RedisError::CommandTimeout`].
#[derive(Debug, Clone)]
pub struct CommandTimeoutService<S> {
    inner: S,
    duration: Duration,
}

impl<S, Req> Service<Req> for CommandTimeoutService<S>
where
    S: Service<Req, Error = RedisError>,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let duration = self.duration;
        let fut = self.inner.call(req);
        Box::pin(async move {
            tokio::time::timeout(duration, fut)
                .await
                .map_err(|_elapsed| RedisError::CommandTimeout)?
        })
    }
}

/// Deadline-aware variant of [`CommandTimeoutLayer`].
///
/// Construct this with [`CommandTimeoutLayer::with_request_deadlines`]. It
/// preserves the configured static upper bound and shortens it when a request
/// supplies an earlier absolute [`RequestDeadline`].
#[derive(Debug, Clone)]
pub struct RequestDeadlineTimeoutLayer {
    duration: Duration,
}

impl<S> Layer<S> for RequestDeadlineTimeoutLayer {
    type Service = RequestDeadlineTimeoutService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestDeadlineTimeoutService {
            inner,
            duration: self.duration,
        }
    }
}

/// The service produced by [`RequestDeadlineTimeoutLayer`].
#[derive(Debug, Clone)]
pub struct RequestDeadlineTimeoutService<S> {
    inner: S,
    duration: Duration,
}

impl<S, Req> Service<Req> for RequestDeadlineTimeoutService<S>
where
    S: Service<Req, Error = RedisError>,
    Req: RequestDeadline,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let now = tokio::time::Instant::now();
        let static_deadline = now + self.duration;
        let deadline = req
            .request_deadline()
            .map_or(static_deadline, |request_deadline| {
                request_deadline.min(static_deadline)
            });

        // Do not call the inner service when the absolute budget was already
        // exhausted. This matters for side-effecting commands: timing out must
        // not enqueue work that the caller has already abandoned.
        if deadline <= now {
            return Box::pin(async { Err(RedisError::CommandTimeout) });
        }

        let fut = self.inner.call(req);
        Box::pin(async move {
            tokio::time::timeout_at(deadline, fut)
                .await
                .map_err(|_elapsed| RedisError::CommandTimeout)?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::{Command, Frame, WithDeadline};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tower_layer::Layer;
    use tower_service::Service;

    #[derive(Clone)]
    struct TestCommand;

    impl Command for TestCommand {
        type Response = ();

        fn to_frame(&self) -> Frame {
            Frame::Null
        }

        fn parse_response(&self, _frame: Frame) -> Result<Self::Response, RedisError> {
            Ok(())
        }

        fn name(&self) -> &str {
            "TEST"
        }
    }

    // A mock service that sleeps for a given duration before returning Ok(()).
    struct SlowService {
        delay: Duration,
        calls: Arc<AtomicUsize>,
    }

    impl<R> Service<R> for SlowService {
        type Response = ();
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: R) -> Self::Future {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let d = self.delay;
            Box::pin(async move {
                tokio::time::sleep(d).await;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn command_timeout_fires_when_slow() {
        let calls = Arc::new(AtomicUsize::new(0));
        let layer = CommandTimeoutLayer::new(Duration::from_millis(50));
        let mut svc = layer.layer(SlowService {
            delay: Duration::from_millis(200),
            calls,
        });
        let result = svc.call(()).await;
        assert!(matches!(result, Err(RedisError::CommandTimeout)));
    }

    #[tokio::test]
    async fn command_timeout_passes_when_fast() {
        let calls = Arc::new(AtomicUsize::new(0));
        let layer = CommandTimeoutLayer::new(Duration::from_millis(200));
        let mut svc = layer.layer(SlowService {
            delay: Duration::from_millis(10),
            calls,
        });
        let result = svc.call(()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn request_deadline_shortens_static_timeout() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut svc = CommandTimeoutLayer::new(Duration::from_secs(1))
            .with_request_deadlines()
            .layer(SlowService {
                delay: Duration::from_millis(200),
                calls,
            });
        let command = WithDeadline::after(TestCommand, Duration::from_millis(30));

        assert!(matches!(
            svc.call(command).await,
            Err(RedisError::CommandTimeout)
        ));
    }

    #[tokio::test]
    async fn static_timeout_can_be_earlier_than_request_deadline() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut svc = CommandTimeoutLayer::new(Duration::from_millis(30))
            .with_request_deadlines()
            .layer(SlowService {
                delay: Duration::from_millis(200),
                calls,
            });
        let command = WithDeadline::after(TestCommand, Duration::from_secs(1));

        assert!(matches!(
            svc.call(command).await,
            Err(RedisError::CommandTimeout)
        ));
    }

    #[tokio::test]
    async fn absolute_deadline_is_not_reset_before_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let command = WithDeadline::after(TestCommand, Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(35)).await;

        let mut svc = CommandTimeoutLayer::new(Duration::from_secs(1))
            .with_request_deadlines()
            .layer(SlowService {
                delay: Duration::from_millis(35),
                calls,
            });

        assert!(matches!(
            svc.call(command).await,
            Err(RedisError::CommandTimeout)
        ));
    }

    #[tokio::test]
    async fn expired_request_never_reaches_inner_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut svc = CommandTimeoutLayer::new(Duration::from_secs(1))
            .with_request_deadlines()
            .layer(SlowService {
                delay: Duration::ZERO,
                calls: Arc::clone(&calls),
            });
        let command = WithDeadline::new(
            TestCommand,
            tokio::time::Instant::now() - Duration::from_millis(1),
        );

        assert!(matches!(
            svc.call(command).await,
            Err(RedisError::CommandTimeout)
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raw_frame_requests_keep_static_timeout_support() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut svc = CommandTimeoutLayer::new(Duration::from_millis(20))
            .with_request_deadlines()
            .layer(SlowService {
                delay: Duration::from_millis(100),
                calls,
            });

        assert!(matches!(
            svc.call(Frame::Null).await,
            Err(RedisError::CommandTimeout)
        ));
    }

    #[tokio::test]
    async fn static_layer_remains_generic_over_requests_without_deadline_metadata() {
        struct CustomRequest;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut svc = CommandTimeoutLayer::new(Duration::from_millis(20)).layer(SlowService {
            delay: Duration::from_millis(100),
            calls,
        });

        assert!(matches!(
            svc.call(CustomRequest).await,
            Err(RedisError::CommandTimeout)
        ));
    }
}
