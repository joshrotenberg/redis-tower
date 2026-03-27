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
/// Converts `Cmd -> Frame` via `to_frame()`, calls the inner service,
/// then converts `Frame -> Cmd::Response` via `parse_response()`.
pub struct CommandAdapter<S> {
    inner: S,
}

impl<S> CommandAdapter<S> {
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
