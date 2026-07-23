//! Tower layer for collecting per-command metrics at the Frame level.
//!
//! Framework-agnostic: users implement [`MetricsRecorder`] for their
//! metrics backend (prometheus, metrics crate, etc.).
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use redis_tower::metrics_layer::{MetricsLayer, MetricsRecorder, ErrorKind};
//!
//! struct MyRecorder;
//!
//! impl MetricsRecorder for MyRecorder {
//!     fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
//!         match error {
//!             None => println!("{command} took {duration:?} (ok)"),
//!             Some(kind) => println!("{command} took {duration:?} (error: {kind:?})"),
//!         }
//!     }
//! }
//!
//! let layer = MetricsLayer::new(MyRecorder);
//! # let _ = layer;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Frame, RedisError};
use tower_service::Service;

/// Categorizes a Redis error for metrics labeling.
///
/// Used as the `error` argument to [`MetricsRecorder::command_completed`].
/// Each variant corresponds to a meaningful alert/dashboard category.
///
/// # Categories
///
/// - `Connection` — transport-level failures: IO errors, closed connections.
///   These are transient and typically require reconnection.
/// - `Timeout` — pool acquisition timeout; indicates pool exhaustion.
/// - `WrongType` — Redis `WRONGTYPE` error; indicates an application bug.
/// - `CircuitOpen` — circuit breaker is open; request rejected without
///   touching Redis.
/// - `QueueFull` — the auto-pipeline channel is full; caller should shed load.
/// - `Auth` — authentication failure (`NOAUTH`, `WRONGPASS`).
/// - `Other` — all other errors (generic Redis errors, protocol errors, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transport-level error (IO error, connection closed).
    Connection,
    /// Pool acquisition timed out.
    Timeout,
    /// Redis `WRONGTYPE` error.
    WrongType,
    /// Circuit breaker is open.
    CircuitOpen,
    /// Auto-pipeline queue is full.
    QueueFull,
    /// Authentication error (`NOAUTH`, `WRONGPASS`).
    Auth,
    /// All other errors.
    Other,
}

impl ErrorKind {
    /// Map a [`RedisError`] to the most specific [`ErrorKind`].
    pub fn from_error(e: &RedisError) -> Self {
        match e {
            RedisError::Connection { .. } | RedisError::ConnectionClosed => ErrorKind::Connection,
            RedisError::PoolAcquisitionTimeout { .. } => ErrorKind::Timeout,
            RedisError::CircuitOpen => ErrorKind::CircuitOpen,
            RedisError::QueueFull => ErrorKind::QueueFull,
            RedisError::Redis(msg) => {
                if msg.starts_with("WRONGTYPE") {
                    ErrorKind::WrongType
                } else if msg.starts_with("NOAUTH") || msg.starts_with("WRONGPASS") {
                    ErrorKind::Auth
                } else {
                    ErrorKind::Other
                }
            }
            _ => ErrorKind::Other,
        }
    }

    /// Classify a Redis-level error frame (i.e. `Frame::Error`) by its
    /// error prefix.
    fn from_frame_error(bytes: &bytes::Bytes) -> Self {
        let msg = std::str::from_utf8(bytes).unwrap_or("");
        if msg.starts_with("WRONGTYPE") {
            ErrorKind::WrongType
        } else if msg.starts_with("NOAUTH") || msg.starts_with("WRONGPASS") {
            ErrorKind::Auth
        } else {
            ErrorKind::Other
        }
    }
}

/// Receives metric events for each Redis command. Users implement this
/// trait to integrate with their metrics framework (prometheus, metrics
/// crate, OpenTelemetry, etc.).
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Called after each command completes.
    ///
    /// - `command`: the Redis command name (e.g. `"GET"`, `"SET"`), or
    ///   `"UNKNOWN"` if it could not be extracted from the frame.
    /// - `duration`: wall-clock time from call to completion.
    /// - `error`: `None` on success; `Some(kind)` on failure. The kind
    ///   enables labeled counters such as
    ///   `redis_command_errors_total{command="GET",kind="connection"}`.
    ///
    /// Note: `Frame::Error` responses (Redis-level errors such as
    /// `WRONGTYPE`) are classified as failures, not successes.
    fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>);

    /// Called after each auto-pipeline flush.
    ///
    /// `batch_size` is the number of frames sent in that flush. A histogram
    /// of this value reveals whether pipelining is effective (`batch_size >
    /// 1`) or whether individual callers flush immediately (`batch_size ==
    /// 1`).
    ///
    /// Default implementation is a no-op so existing implementors are not
    /// affected.
    fn pipeline_flushed(&self, batch_size: usize) {
        let _ = batch_size;
    }
}

/// Tower `Layer` that produces [`MetricsService`] wrappers.
///
/// Wraps each inner service with a [`MetricsService`] that records
/// per-command latency and error category via the provided
/// [`MetricsRecorder`].
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use tower::ServiceBuilder;
/// use redis_tower::FrameService;
/// use redis_tower::metrics_layer::MetricsLayer;
/// #
/// # use std::time::Duration;
/// # use redis_tower::metrics_layer::{ErrorKind, MetricsRecorder};
/// # struct MyRecorder;
/// # impl MetricsRecorder for MyRecorder {
/// #     fn command_completed(&self, command: &str, duration: Duration, error: Option<ErrorKind>) {
/// #         let _ = (command, duration, error);
/// #     }
/// # }
///
/// let frame_service = FrameService::connect("127.0.0.1:6379").await?;
/// let layer = MetricsLayer::new(MyRecorder);
/// let svc = ServiceBuilder::new()
///     .layer(layer)
///     .service(frame_service);
/// # let _ = svc;
/// # Ok(())
/// # }
/// ```
pub struct MetricsLayer<R> {
    recorder: Arc<R>,
}

impl<R: MetricsRecorder> MetricsLayer<R> {
    /// Create a new metrics layer with the given recorder.
    pub fn new(recorder: R) -> Self {
        Self {
            recorder: Arc::new(recorder),
        }
    }
}

impl<R: MetricsRecorder, S> tower_layer::Layer<S> for MetricsLayer<R> {
    type Service = MetricsService<S, R>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            recorder: Arc::clone(&self.recorder),
        }
    }
}

/// Tower `Service` that records per-command metrics via a [`MetricsRecorder`].
///
/// Created by [`MetricsLayer`] or directly via [`MetricsService::new`].
/// Extracts the command name from each request frame and reports the
/// command name, wall-clock duration, and error category after each call
/// completes.
pub struct MetricsService<S, R> {
    inner: S,
    recorder: Arc<R>,
}

impl<S, R> MetricsService<S, R> {
    /// Create a new metrics service wrapping an inner Frame service.
    pub fn new(inner: S, recorder: Arc<R>) -> Self {
        Self { inner, recorder }
    }
}

/// Extract the command name from a Redis command frame.
///
/// Expects `Frame::Array(Some(vec))` where the first element is
/// `Frame::BulkString(Some(bytes))`.
fn extract_command_name(frame: &Frame) -> Option<&str> {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return None,
    };
    match &items[0] {
        Frame::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
        _ => None,
    }
}

impl<S, R> Service<Frame> for MetricsService<S, R>
where
    S: Service<Frame, Response = Frame, Error = RedisError>,
    S::Future: Send + 'static,
    R: MetricsRecorder,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let command_name = extract_command_name(&request)
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase();
        let start = Instant::now();
        let recorder = Arc::clone(&self.recorder);
        let future = self.inner.call(request);

        Box::pin(async move {
            let result = future.await;
            let elapsed = start.elapsed();
            let error_kind = match &result {
                Ok(Frame::Error(bytes)) => Some(ErrorKind::from_frame_error(bytes)),
                Ok(_) => None,
                Err(e) => Some(ErrorKind::from_error(e)),
            };
            recorder.command_completed(&command_name, elapsed, error_kind);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn extract_name_from_get() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
        ]));
        assert_eq!(extract_command_name(&frame), Some("GET"));
    }

    #[test]
    fn extract_name_from_set() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        assert_eq!(extract_command_name(&frame), Some("SET"));
    }

    #[test]
    fn extract_name_empty_array() {
        let frame = Frame::Array(Some(vec![]));
        assert_eq!(extract_command_name(&frame), None);
    }

    #[test]
    fn extract_name_null_frame() {
        assert_eq!(extract_command_name(&Frame::Null), None);
    }

    #[test]
    fn extract_name_none_array() {
        let frame = Frame::Array(None);
        assert_eq!(extract_command_name(&frame), None);
    }

    #[test]
    fn error_kind_from_error_connection() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::ConnectionClosed),
            ErrorKind::Connection
        );
    }

    #[test]
    fn error_kind_from_error_timeout() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::PoolAcquisitionTimeout {
                waited: std::time::Duration::from_millis(50),
                pool_size: 4,
            }),
            ErrorKind::Timeout
        );
    }

    #[test]
    fn error_kind_from_error_wrongtype() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into()
            )),
            ErrorKind::WrongType
        );
    }

    #[test]
    fn error_kind_from_error_circuit_open() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::CircuitOpen),
            ErrorKind::CircuitOpen
        );
    }

    #[test]
    fn error_kind_from_error_queue_full() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::QueueFull),
            ErrorKind::QueueFull
        );
    }

    #[test]
    fn error_kind_from_error_auth_noauth() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis("NOAUTH Authentication required".into())),
            ErrorKind::Auth
        );
    }

    #[test]
    fn error_kind_from_error_auth_wrongpass() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis(
                "WRONGPASS invalid username-password pair".into()
            )),
            ErrorKind::Auth
        );
    }

    #[test]
    fn error_kind_from_error_other() {
        assert_eq!(
            ErrorKind::from_error(&RedisError::Redis("ERR unknown command".into())),
            ErrorKind::Other
        );
    }

    #[test]
    fn error_kind_from_frame_error_wrongtype() {
        assert_eq!(
            ErrorKind::from_frame_error(&Bytes::from("WRONGTYPE value is not a set")),
            ErrorKind::WrongType
        );
    }

    #[test]
    fn error_kind_from_frame_error_other() {
        assert_eq!(
            ErrorKind::from_frame_error(&Bytes::from("ERR syntax error")),
            ErrorKind::Other
        );
    }

    struct TestRecorder {
        error_kind: Mutex<Option<Option<ErrorKind>>>,
        duration_ns: AtomicU64,
    }

    impl TestRecorder {
        fn new() -> Self {
            Self {
                error_kind: Mutex::new(None),
                duration_ns: AtomicU64::new(0),
            }
        }

        fn was_called(&self) -> bool {
            self.error_kind.lock().unwrap().is_some()
        }

        fn recorded_error(&self) -> Option<ErrorKind> {
            self.error_kind.lock().unwrap().flatten()
        }
    }

    impl MetricsRecorder for TestRecorder {
        fn command_completed(&self, _command: &str, duration: Duration, error: Option<ErrorKind>) {
            *self.error_kind.lock().unwrap() = Some(error);
            self.duration_ns
                .store(duration.as_nanos() as u64, Ordering::SeqCst);
        }
    }

    /// A mock service that always returns SimpleString "OK".
    struct OkService;

    impl Service<Frame> for OkService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            Box::pin(async { Ok(Frame::SimpleString("OK".into())) })
        }
    }

    /// A mock service that always returns a transport error.
    struct ErrService;

    impl Service<Frame> for ErrService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            Box::pin(async { Err(RedisError::ConnectionClosed) })
        }
    }

    /// A mock service that returns a Frame::Error (Redis-level error).
    struct FrameErrService {
        msg: &'static str,
    }

    impl Service<Frame> for FrameErrService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            let msg = self.msg;
            Box::pin(async move { Ok(Frame::Error(Bytes::from(msg))) })
        }
    }

    #[tokio::test]
    async fn records_success() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(OkService, Arc::clone(&recorder));

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_ok());
        assert!(recorder.was_called());
        assert_eq!(recorder.recorded_error(), None);
        assert!(recorder.duration_ns.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn records_failure_connection_closed() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(ErrService, Arc::clone(&recorder));

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("val"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_err());
        assert!(recorder.was_called());
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::Connection));
    }

    #[tokio::test]
    async fn frame_error_is_classified_as_failure() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(
            FrameErrService {
                msg: "WRONGTYPE Operation against a key holding the wrong kind of value",
            },
            Arc::clone(&recorder),
        );

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SADD"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("member"))),
        ]));

        // The service returns Ok(Frame::Error(...)) -- a Redis-level error,
        // not a transport error. The Result is Ok.
        let result = svc.call(frame).await;
        assert!(result.is_ok());
        assert!(recorder.was_called());
        // But the recorder should classify it as a WrongType failure.
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::WrongType));
    }

    #[tokio::test]
    async fn frame_error_generic_classified_as_other() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(
            FrameErrService {
                msg: "ERR syntax error",
            },
            Arc::clone(&recorder),
        );

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_ok());
        assert_eq!(recorder.recorded_error(), Some(ErrorKind::Other));
    }
}
