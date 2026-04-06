//! Tower layer for tracing Redis commands at the Frame level.
//!
//! Wraps a `Service<Frame, Response=Frame>` and creates a tracing span
//! for each Redis command, recording the command name and status.
//!
//! # Example
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use redis_tower::tracing_layer::TracingLayer;
//!
//! let svc = ServiceBuilder::new()
//!     .layer(TracingLayer::new())
//!     .service(frame_service);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use redis_tower_core::{Frame, RedisError};
use tower_layer::Layer;
use tower_service::Service;
use tracing::Instrument;

/// Tower `Layer` that adds tracing spans for Redis commands.
///
/// Each call creates an `info`-level span named `redis.command` with the
/// command name extracted from the request frame.
#[derive(Clone, Debug, Default)]
pub struct TracingLayer;

impl TracingLayer {
    /// Create a new tracing layer.
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingService { inner }
    }
}

/// Tower `Service` that instruments Redis commands with tracing spans.
///
/// Created by [`TracingLayer`]. Extracts the command name from the request
/// `Frame::Array` and records it in a span along with OpenTelemetry
/// semantic attributes.
#[derive(Clone, Debug)]
pub struct TracingService<S> {
    inner: S,
}

impl<S> TracingService<S> {
    /// Create a new tracing service wrapping an inner service.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

/// Extract the command name from a request frame.
///
/// Expects `Frame::Array(Some(vec![Frame::BulkString(Some(name)), ...]))`.
/// Returns `"UNKNOWN"` if the frame does not match this pattern.
fn extract_command_name(frame: &Frame) -> String {
    if let Frame::Array(Some(items)) = frame {
        if let Some(Frame::BulkString(Some(bytes))) = items.first() {
            if let Ok(name) = std::str::from_utf8(bytes) {
                return name.to_ascii_uppercase();
            }
        }
    }
    "UNKNOWN".to_string()
}

impl<S> Service<Frame> for TracingService<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError>,
    S::Future: Send + 'static,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let cmd_name = extract_command_name(&request);
        let span = tracing::info_span!(
            "redis.command",
            redis.command = %cmd_name,
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
        );

        let future = self.inner.call(request);

        Box::pin(
            async move {
                let result = future.await;
                let span = tracing::Span::current();
                match &result {
                    Ok(Frame::Error(_)) => {
                        span.record("otel.status_code", "ERROR");
                    }
                    Ok(_) => {
                        span.record("otel.status_code", "OK");
                    }
                    Err(_) => {
                        span.record("otel.status_code", "ERROR");
                    }
                }
                result
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn extracts_get_command() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("get"))),
            Frame::BulkString(Some(Bytes::from("mykey"))),
        ]));
        assert_eq!(extract_command_name(&frame), "GET");
    }

    #[test]
    fn extracts_set_command() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        assert_eq!(extract_command_name(&frame), "SET");
    }

    #[test]
    fn unknown_for_non_array() {
        let frame = Frame::SimpleString(Bytes::from("PING"));
        assert_eq!(extract_command_name(&frame), "UNKNOWN");
    }

    #[test]
    fn unknown_for_empty_array() {
        let frame = Frame::Array(Some(vec![]));
        assert_eq!(extract_command_name(&frame), "UNKNOWN");
    }

    #[test]
    fn unknown_for_null_array() {
        let frame = Frame::Array(None);
        assert_eq!(extract_command_name(&frame), "UNKNOWN");
    }
}
