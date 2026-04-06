//! Tower layer for collecting per-command metrics at the Frame level.
//!
//! Framework-agnostic: users implement [`MetricsRecorder`] for their
//! metrics backend (prometheus, metrics crate, etc.).
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use std::time::Duration;
//! use redis_tower::metrics_layer::{MetricsLayer, MetricsRecorder};
//!
//! struct MyRecorder;
//!
//! impl MetricsRecorder for MyRecorder {
//!     fn command_completed(&self, command: &str, duration: Duration, success: bool) {
//!         println!("{command} took {duration:?} (ok={success})");
//!     }
//! }
//!
//! let layer = MetricsLayer::new(MyRecorder);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Frame, RedisError};
use tower_service::Service;

/// Receives metric events. Users implement this for their metrics framework.
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Called after each command completes.
    ///
    /// - `command`: the Redis command name (e.g. "GET", "SET"), or "UNKNOWN"
    ///   if it could not be extracted from the frame.
    /// - `duration`: wall-clock time from call to completion.
    /// - `success`: whether the inner service returned `Ok`.
    fn command_completed(&self, command: &str, duration: Duration, success: bool);
}

/// Tower `Layer` that produces [`MetricsService`] wrappers.
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
            let success = result.is_ok();
            recorder.command_completed(&command_name, elapsed, success);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

    struct TestRecorder {
        called: AtomicBool,
        success: AtomicBool,
        duration_ns: AtomicU64,
    }

    impl TestRecorder {
        fn new() -> Self {
            Self {
                called: AtomicBool::new(false),
                success: AtomicBool::new(false),
                duration_ns: AtomicU64::new(0),
            }
        }
    }

    impl MetricsRecorder for TestRecorder {
        fn command_completed(&self, _command: &str, duration: Duration, success: bool) {
            self.called.store(true, Ordering::SeqCst);
            self.success.store(success, Ordering::SeqCst);
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

    /// A mock service that always returns an error.
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
        assert!(recorder.called.load(Ordering::SeqCst));
        assert!(recorder.success.load(Ordering::SeqCst));
        assert!(recorder.duration_ns.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn records_failure() {
        let recorder = Arc::new(TestRecorder::new());
        let mut svc = MetricsService::new(ErrService, Arc::clone(&recorder));

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("val"))),
        ]));

        let result = svc.call(frame).await;
        assert!(result.is_err());
        assert!(recorder.called.load(Ordering::SeqCst));
        assert!(!recorder.success.load(Ordering::SeqCst));
    }
}
