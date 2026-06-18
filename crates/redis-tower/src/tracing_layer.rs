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
//!
//! // Warn on commands slower than 50ms (a differentiator neither
//! // redis-rs nor fred ships):
//! use std::time::Duration;
//! let svc = ServiceBuilder::new()
//!     .layer(TracingLayer::new().with_slow_command_threshold(Duration::from_millis(50)))
//!     .service(frame_service);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Frame, RedisError};
use tower_layer::Layer;
use tower_service::Service;
use tracing::Instrument;

/// Tower `Layer` that adds tracing spans for Redis commands.
///
/// Each call creates an `info`-level span carrying the **stable OpenTelemetry
/// database semantic conventions** so it lights up Redis dashboards in Grafana,
/// Datadog, and Dynatrace out of the box:
///
/// - `otel.name` set to the command (the exported span name is dynamic, e.g.
///   `GET`, not a static `redis.command`),
/// - `db.system.name = "redis"` and `db.operation.name` = the command,
/// - `server.address` / `server.port` (split from the peer address),
/// - `otel.status_code` / `otel.status_message`, and `error.type` on failure.
///
/// The deprecated `db.system` / `db.statement` attributes are emitted only when
/// [`with_legacy_attributes`](Self::with_legacy_attributes) is set, for callers
/// mid-migration.
///
/// Use [`TracingLayer::with_peer`] to set the peer address for per-node flame
/// graphs in cluster or sentinel topologies, or
/// [`with_slow_command_threshold`](Self::with_slow_command_threshold) to log a
/// `WARN` event for commands that exceed a latency budget.
#[derive(Clone, Debug, Default)]
pub struct TracingLayer {
    server_address: Option<String>,
    legacy_attributes: bool,
    slow_command_threshold: Option<Duration>,
}

impl TracingLayer {
    /// Create a new tracing layer without a peer address.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new tracing layer with a peer address.
    ///
    /// The address is split into the `server.address` and `server.port` OTel
    /// attributes on every span, enabling per-node flame graphs in backends
    /// like Grafana Tempo.
    pub fn with_peer(addr: impl Into<String>) -> Self {
        Self {
            server_address: Some(addr.into()),
            legacy_attributes: false,
            slow_command_threshold: None,
        }
    }

    /// Also emit the deprecated `db.system` / `db.statement` attributes
    /// alongside the stable conventions, for dashboards not yet migrated.
    pub fn with_legacy_attributes(mut self) -> Self {
        self.legacy_attributes = true;
        self
    }

    /// Log a `WARN`-level event for any command whose round-trip latency meets
    /// or exceeds `threshold`.
    ///
    /// The event is emitted inside the command's span and carries the command
    /// name, the measured `elapsed_ms`, and the configured `threshold_ms`. Slow
    /// commands are still recorded as normal spans; this only adds the warning.
    /// Disabled by default -- a differentiator neither `redis-rs` nor `fred`
    /// ships out of the box.
    pub fn with_slow_command_threshold(mut self, threshold: Duration) -> Self {
        self.slow_command_threshold = Some(threshold);
        self
    }
}

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingService {
            inner,
            server_address: self.server_address.clone(),
            legacy_attributes: self.legacy_attributes,
            slow_command_threshold: self.slow_command_threshold,
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
    legacy_attributes: bool,
    slow_command_threshold: Option<Duration>,
}

impl<S> TracingService<S> {
    /// Create a new tracing service wrapping an inner service.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            server_address: None,
            legacy_attributes: false,
            slow_command_threshold: None,
        }
    }

    /// Create a new tracing service with an explicit peer address.
    pub fn with_peer(inner: S, addr: impl Into<String>) -> Self {
        Self {
            inner,
            server_address: Some(addr.into()),
            legacy_attributes: false,
            slow_command_threshold: None,
        }
    }

    /// Also emit the deprecated `db.system` / `db.statement` attributes.
    pub fn with_legacy_attributes(mut self) -> Self {
        self.legacy_attributes = true;
        self
    }

    /// Log a `WARN`-level event for any command whose round-trip latency meets
    /// or exceeds `threshold`. See [`TracingLayer::with_slow_command_threshold`].
    pub fn with_slow_command_threshold(mut self, threshold: Duration) -> Self {
        self.slow_command_threshold = Some(threshold);
        self
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
        // `otel.name` drives the exported OTel span name (dynamic, per command);
        // the static macro name is what non-OTel tracing subscribers see.
        let span = tracing::info_span!(
            "redis.command",
            otel.name = %cmd_name,
            otel.kind = "client",
            db.system.name = "redis",
            db.operation.name = %cmd_name,
            server.address = tracing::field::Empty,
            server.port = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
            error.type = tracing::field::Empty,
            // Convenience field kept for existing filters.
            redis.command = %cmd_name,
            // Deprecated; recorded only with legacy compat enabled.
            db.system = tracing::field::Empty,
            db.statement = tracing::field::Empty,
        );

        if let Some(ref addr) = self.server_address {
            let (host, port) = split_server_addr(addr);
            span.record("server.address", host);
            if let Some(port) = port {
                span.record("server.port", i64::from(port));
            }
        }

        if self.legacy_attributes {
            span.record("db.system", "redis");
            span.record("db.statement", cmd_name.as_str());
        }

        let slow_threshold = self.slow_command_threshold;
        let started = Instant::now();
        let cmd_for_log = cmd_name.clone();
        let future = self.inner.call(request);

        Box::pin(
            async move {
                let result = future.await;
                let span = tracing::Span::current();
                let elapsed = started.elapsed();
                if is_slow(slow_threshold, elapsed) {
                    let threshold_ms = slow_threshold.map_or(0, |t| t.as_millis() as u64);
                    tracing::warn!(
                        command = %cmd_for_log,
                        elapsed_ms = elapsed.as_millis() as u64,
                        threshold_ms,
                        "Redis command exceeded slow-command threshold"
                    );
                }
                match &result {
                    Ok(Frame::Error(bytes)) => {
                        let msg = String::from_utf8_lossy(bytes);
                        span.record("otel.status_code", "ERROR");
                        span.record("otel.status_message", msg.as_ref());
                        // The Redis error prefix (WRONGTYPE, MOVED, ...).
                        span.record("error.type", msg.split_whitespace().next().unwrap_or("ERR"));
                    }
                    Ok(_) => {
                        span.record("otel.status_code", "OK");
                    }
                    Err(e) => {
                        span.record("otel.status_code", "ERROR");
                        span.record("otel.status_message", e.to_string().as_str());
                        span.record("error.type", error_type(e));
                    }
                }
                result
            }
            .instrument(span),
        )
    }
}

/// Split a `host:port` peer address into `(host, port)` for the OTel
/// `server.address` / `server.port` attributes. IPv6 addresses (which contain
/// colons) split on the last colon; an address with no port yields `None`.
fn split_server_addr(addr: &str) -> (&str, Option<u16>) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<u16>().ok()),
        None => (addr, None),
    }
}

/// The OTel `error.type` for a [`RedisError`]: the server error prefix for a
/// Redis error (`WRONGTYPE`, `MOVED`, ...), otherwise a stable category.
fn error_type(err: &RedisError) -> &str {
    match err {
        RedisError::Redis(msg) => msg.split_whitespace().next().unwrap_or("ERR"),
        RedisError::Connection(_) | RedisError::ConnectionClosed => "CONNECTION",
        RedisError::Protocol(_) => "PROTOCOL",
        RedisError::CommandTimeout => "COMMAND_TIMEOUT",
        RedisError::ConnectTimeout => "CONNECT_TIMEOUT",
        RedisError::CircuitOpen => "CIRCUIT_OPEN",
        RedisError::PoolAcquisitionTimeout { .. } => "POOL_TIMEOUT",
        RedisError::QueueFull => "QUEUE_FULL",
        _ => "CLIENT_ERROR",
    }
}

/// Whether a command's `elapsed` round-trip duration crosses the configured
/// slow-command `threshold`. Returns `false` when no threshold is set, so a
/// disabled threshold never flags a command as slow.
fn is_slow(threshold: Option<Duration>, elapsed: Duration) -> bool {
    threshold.is_some_and(|t| elapsed >= t)
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
    fn split_server_addr_ipv4() {
        assert_eq!(
            split_server_addr("127.0.0.1:6379"),
            ("127.0.0.1", Some(6379))
        );
    }

    #[test]
    fn split_server_addr_ipv6_splits_on_last_colon() {
        assert_eq!(split_server_addr("::1:6380"), ("::1", Some(6380)));
    }

    #[test]
    fn split_server_addr_no_port() {
        assert_eq!(split_server_addr("localhost"), ("localhost", None));
    }

    #[test]
    fn split_server_addr_bad_port() {
        // Host kept, port dropped when it doesn't parse.
        assert_eq!(split_server_addr("host:notaport"), ("host", None));
    }

    #[test]
    fn error_type_uses_redis_prefix() {
        let err = RedisError::Redis("WRONGTYPE Operation against a key".into());
        assert_eq!(error_type(&err), "WRONGTYPE");
        let moved = RedisError::Redis("MOVED 3999 127.0.0.1:6381".into());
        assert_eq!(error_type(&moved), "MOVED");
    }

    #[test]
    fn error_type_categorizes_client_errors() {
        assert_eq!(error_type(&RedisError::ConnectionClosed), "CONNECTION");
        assert_eq!(error_type(&RedisError::CommandTimeout), "COMMAND_TIMEOUT");
        assert_eq!(error_type(&RedisError::CircuitOpen), "CIRCUIT_OPEN");
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

    #[test]
    fn is_slow_false_without_threshold() {
        assert!(!is_slow(None, Duration::from_secs(10)));
    }

    #[test]
    fn is_slow_true_when_elapsed_exceeds_threshold() {
        assert!(is_slow(
            Some(Duration::from_millis(50)),
            Duration::from_millis(75)
        ));
    }

    #[test]
    fn is_slow_true_when_elapsed_equals_threshold() {
        assert!(is_slow(
            Some(Duration::from_millis(50)),
            Duration::from_millis(50)
        ));
    }

    #[test]
    fn is_slow_false_when_elapsed_below_threshold() {
        assert!(!is_slow(
            Some(Duration::from_millis(50)),
            Duration::from_millis(49)
        ));
    }

    #[test]
    fn layer_with_slow_command_threshold_sets_field() {
        let layer = TracingLayer::new().with_slow_command_threshold(Duration::from_millis(100));
        assert_eq!(
            layer.slow_command_threshold,
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn layer_without_slow_command_threshold_is_none() {
        assert!(TracingLayer::new().slow_command_threshold.is_none());
    }

    #[test]
    fn service_with_slow_command_threshold_sets_field() {
        let svc = TracingService::new(()).with_slow_command_threshold(Duration::from_millis(100));
        assert_eq!(svc.slow_command_threshold, Some(Duration::from_millis(100)));
    }

    #[test]
    fn layer_propagates_slow_command_threshold_to_service() {
        let layer = TracingLayer::new().with_slow_command_threshold(Duration::from_millis(25));
        let svc = layer.layer(());
        assert_eq!(svc.slow_command_threshold, Some(Duration::from_millis(25)));
    }
}
