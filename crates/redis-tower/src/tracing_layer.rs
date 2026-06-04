//! Tower layer for tracing Redis commands at the Frame level.
//!
//! Wraps a `Service<Frame, Response=Frame>` and creates a tracing span
//! for each Redis command, recording the command name, status, and
//! OpenTelemetry DB semantic convention attributes.
//!
//! # Example
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use redis_tower::tracing_layer::TracingLayer;
//!
//! // Basic -- no peer address:
//! let svc = ServiceBuilder::new()
//!     .layer(TracingLayer::new())
//!     .service(frame_service);
//!
//! // With server address for OTel per-node flame graphs:
//! let svc = ServiceBuilder::new()
//!     .layer(TracingLayer::with_peer("127.0.0.1:6379"))
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
/// Each call creates an `info`-level span named `redis.command` with OTel
/// DB semantic convention attributes (`db.system`, `db.statement`,
/// `server.address`). Error details are recorded as `otel.status_code` and
/// `otel.status_message`.
///
/// Use [`TracingLayer::with_peer`] to set `server.address` for per-node
/// flame graphs in cluster or sentinel topologies.
#[derive(Clone, Debug, Default)]
pub struct TracingLayer {
    server_address: Option<String>,
}

impl TracingLayer {
    /// Create a new tracing layer without a peer address.
    pub fn new() -> Self {
        Self {
            server_address: None,
        }
    }

    /// Create a new tracing layer with a `server.address` field.
    ///
    /// The address is recorded on every span as the `server.address` OTel
    /// attribute, enabling per-node flame graphs in backends like Grafana
    /// Tempo.
    pub fn with_peer(addr: impl Into<String>) -> Self {
        Self {
            server_address: Some(addr.into()),
        }
    }
}

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingService {
            inner,
            server_address: self.server_address.clone(),
        }
    }
}

/// Tower `Service` that instruments Redis commands with tracing spans.
///
/// Created by [`TracingLayer`]. Extracts the command name from the request
/// `Frame::Array` and records OTel DB semantic convention attributes on
/// each span.
#[derive(Clone, Debug)]
pub struct TracingService<S> {
    inner: S,
    server_address: Option<String>,
}

impl<S> TracingService<S> {
    /// Create a new tracing service wrapping an inner service.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            server_address: None,
        }
    }

    /// Create a new tracing service with an explicit server address.
    pub fn with_peer(inner: S, addr: impl Into<String>) -> Self {
        Self {
            inner,
            server_address: Some(addr.into()),
        }
    }
}

/// Extract the command name from a request frame.
///
/// Expects `Frame::Array(Some(vec![Frame::BulkString(Some(name)), ...]))`.
/// Returns `"UNKNOWN"` if the frame does not match this pattern.
fn extract_command_name(frame: &Frame) -> String {
    if let Frame::Array(Some(items)) = frame
        && let Some(Frame::BulkString(Some(bytes))) = items.first()
        && let Ok(name) = std::str::from_utf8(bytes)
    {
        return name.to_ascii_uppercase();
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
            db.system = "redis",
            db.statement = %cmd_name,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            server.address = tracing::field::Empty,
        );

        if let Some(ref addr) = self.server_address {
            span.record("server.address", addr.as_str());
        }

        let future = self.inner.call(request);

        Box::pin(
            async move {
                let result = future.await;
                let span = tracing::Span::current();
                match &result {
                    Ok(Frame::Error(bytes)) => {
                        span.record("otel.status_code", "ERROR");
                        span.record(
                            "otel.status_message",
                            String::from_utf8_lossy(bytes).as_ref(),
                        );
                    }
                    Ok(_) => {
                        span.record("otel.status_code", "OK");
                    }
                    Err(e) => {
                        span.record("otel.status_code", "ERROR");
                        span.record("otel.status_message", e.to_string().as_str());
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

    #[test]
    fn tracing_layer_new_has_no_peer() {
        let layer = TracingLayer::new();
        assert!(layer.server_address.is_none());
    }

    #[test]
    fn tracing_layer_with_peer_sets_address() {
        let layer = TracingLayer::with_peer("127.0.0.1:6379");
        assert_eq!(layer.server_address.as_deref(), Some("127.0.0.1:6379"));
    }

    #[test]
    fn tracing_layer_default_has_no_peer() {
        let layer = TracingLayer::default();
        assert!(layer.server_address.is_none());
    }
}
