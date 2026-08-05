//! Adapter that maps typed `Command` to raw `Frame` service.
//!
//! Wraps any `Service<Frame, Response=Frame>` and implements
//! `Service<Cmd>` for any `Cmd: Command`. Because lowering to a raw frame
//! removes typed command metadata, the adapter enforces [`Command::deadline`]
//! around the inner call before crossing that boundary.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use redis_tower_core::{Command, Frame, RedisError};
use tower_service::Service;

/// Wraps a `Service<Frame>` to provide `Service<Cmd>` for typed commands.
///
/// Converts `Cmd -> Frame` via [`Command::to_frame`], calls the inner
/// service, then converts `Frame -> Cmd::Response` via
/// [`Command::parse_response`]. This is the bridge between Frame-level
/// Tower middleware and typed command dispatch.
///
/// A deadline carried by [`redis_tower_core::WithDeadline`] is enforced around
/// the complete inner call here. Frame-level middleware inside this adapter
/// receives only a [`Frame`] and therefore cannot inspect the typed deadline
/// itself. Put metadata-aware middleware outside `CommandAdapter`; a
/// frame-level [`crate::CommandTimeoutLayer`] still provides its configured
/// static timeout.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use tower::{Service, ServiceBuilder};
/// use redis_tower::{CommandAdapter, FrameService};
/// use redis_tower::tracing_layer::TracingLayer;
/// use redis_tower::commands::Get;
///
/// let frame_svc = FrameService::connect("127.0.0.1:6379").await?;
/// let mut svc = CommandAdapter::new(
///     ServiceBuilder::new()
///         .layer(TracingLayer::new())
///         .service(frame_svc),
/// );
/// let val: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
/// # let _ = val;
/// # Ok(())
/// # }
/// ```
pub struct CommandAdapter<S> {
    inner: S,
}

impl<S: Clone> Clone for CommandAdapter<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> CommandAdapter<S> {
    /// Create a new `CommandAdapter` wrapping the given Frame-level service.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    /// Get a reference to the inner service.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner service.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consume the adapter and return the inner service.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<Cmd, S> Service<Cmd> for CommandAdapter<S>
where
    Cmd: Command + 'static,
    S: Service<Frame, Response = Frame, Error = RedisError>,
    S::Future: Send + 'static,
{
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let deadline = cmd.deadline();
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            return Box::pin(async { Err(RedisError::CommandTimeout) });
        }

        let frame = cmd.to_frame();
        let future = self.inner.call(frame);
        Box::pin(async move {
            let response = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, future)
                    .await
                    .map_err(|_elapsed| RedisError::CommandTimeout)??,
                None => future.await?,
            };
            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }
            cmd.parse_response(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::WithDeadline;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Clone)]
    struct TestCommand;

    impl Command for TestCommand {
        type Response = Frame;

        fn to_frame(&self) -> Frame {
            Frame::Null
        }

        fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
            Ok(frame)
        }

        fn name(&self) -> &str {
            "TEST"
        }
    }

    struct SlowFrameService {
        delay: Duration,
        calls: Arc<AtomicUsize>,
    }

    impl Service<Frame> for SlowFrameService {
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
                Ok(Frame::Null)
            })
        }
    }

    #[tokio::test]
    async fn adapter_enforces_typed_deadline_before_metadata_is_lowered() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = CommandAdapter::new(SlowFrameService {
            delay: Duration::from_millis(100),
            calls: Arc::clone(&calls),
        });

        let result = service
            .call(WithDeadline::after(TestCommand, Duration::from_millis(20)))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn adapter_rejects_expired_command_without_calling_frame_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = CommandAdapter::new(SlowFrameService {
            delay: Duration::ZERO,
            calls: Arc::clone(&calls),
        });

        let result = service
            .call(WithDeadline::new(
                TestCommand,
                tokio::time::Instant::now() - Duration::from_millis(1),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
