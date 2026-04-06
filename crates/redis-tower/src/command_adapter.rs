//! Adapter that maps typed `Command` to raw `Frame` service.
//!
//! Wraps any `Service<Frame, Response=Frame>` and implements
//! `Service<Cmd>` for any `Cmd: Command`.

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
/// # Example
///
/// ```ignore
/// use tower::ServiceBuilder;
/// use redis_tower::{CommandAdapter, FrameService};
/// use redis_tower::tracing_layer::TracingLayer;
/// use redis_tower::commands::*;
/// use tower::ServiceExt;
///
/// let frame_svc = FrameService::connect("127.0.0.1:6379").await?;
/// let mut svc = CommandAdapter::new(
///     ServiceBuilder::new()
///         .layer(TracingLayer::new())
///         .service(frame_svc),
/// );
/// let val: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
/// ```
pub struct CommandAdapter<S> {
    inner: S,
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
        let frame = cmd.to_frame();
        let future = self.inner.call(frame);
        Box::pin(async move {
            let response = future.await?;
            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }
            cmd.parse_response(response)
        })
    }
}
