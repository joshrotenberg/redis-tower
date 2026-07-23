//! Tower layer that enforces a per-command deadline.
//!
//! [`CommandTimeoutLayer`] wraps any inner service and cancels calls that
//! exceed the configured duration, returning [`RedisError::CommandTimeout`].
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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_core::RedisError;
use tower_layer::Layer;
use tower_service::Service;

/// A Tower [`Layer`] that enforces a per-command timeout.
///
/// Wraps any inner service and cancels in-flight calls that exceed the
/// configured deadline, returning [`RedisError::CommandTimeout`].
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
    /// Create a new `CommandTimeoutLayer` with the given per-command deadline.
    pub fn new(duration: Duration) -> Self {
        Self { duration }
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
/// Wraps the inner service's `call` future in a [`tokio::time::timeout`].
/// If the inner future does not resolve before the deadline, the call returns
/// [`RedisError::CommandTimeout`].
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tower_layer::Layer;
    use tower_service::Service;

    // A mock service that sleeps for a given duration before returning Ok(())
    struct SlowService(Duration);

    impl Service<()> for SlowService {
        type Response = ();
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: ()) -> Self::Future {
            let d = self.0;
            Box::pin(async move {
                tokio::time::sleep(d).await;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn command_timeout_fires_when_slow() {
        let layer = CommandTimeoutLayer::new(Duration::from_millis(50));
        let mut svc = layer.layer(SlowService(Duration::from_millis(200)));
        let result = svc.call(()).await;
        assert!(matches!(result, Err(RedisError::CommandTimeout)));
    }

    #[tokio::test]
    async fn command_timeout_passes_when_fast() {
        let layer = CommandTimeoutLayer::new(Duration::from_millis(200));
        let mut svc = layer.layer(SlowService(Duration::from_millis(10)));
        let result = svc.call(()).await;
        assert!(result.is_ok());
    }
}
