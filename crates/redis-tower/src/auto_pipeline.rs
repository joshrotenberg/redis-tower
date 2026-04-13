//! Auto-pipelining middleware for transparent batching of concurrent requests.
//!
//! When multiple tasks issue commands concurrently, instead of sending them
//! one at a time, [`AutoPipelineService`] collects them into a batch and
//! sends them as a single Redis pipeline. Each caller gets back their
//! individual response.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::{AutoPipelineConfig, AutoPipelineService, CommandAdapter, RedisConnection};
//! use redis_tower::commands::*;
//! use tower::Service;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut svc = CommandAdapter::new(AutoPipelineService::new(conn, AutoPipelineConfig::default()));
//! let value: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use redis_tower_core::{Frame, RedisConnection, RedisError};
use tokio::sync::{mpsc, oneshot};
use tower_service::Service;
use tracing::warn;

use crate::reconnect::{ConnectionFactory, ReconnectConfig};

/// Configuration for the auto-pipelining service.
#[derive(Debug, Clone)]
pub struct AutoPipelineConfig {
    /// Maximum commands to batch before sending. Default: 100.
    pub max_batch_size: usize,
    /// Time to wait for more commands after draining the immediate queue.
    ///
    /// Default: 0 (no wait -- flushes immediately, only batches requests
    /// that arrive concurrently). Set to 1-2ms for write-heavy workloads
    /// where batching reduces round-trips.
    pub batch_window: Duration,
}

impl Default for AutoPipelineConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_window: Duration::ZERO,
        }
    }
}

/// Reconnect policy for a factory-backed [`AutoPipelineService`].
///
/// Applies only when the service is constructed via
/// [`AutoPipelineService::with_factory`]. The plain [`AutoPipelineService::new`]
/// path owns a single pre-built connection and does not reconnect.
#[derive(Debug, Clone, Default)]
pub struct AutoPipelineReconnectConfig {
    /// Backoff parameters for reconnection attempts.
    pub reconnect: ReconnectConfig,
}

impl AutoPipelineReconnectConfig {
    /// Create a new reconnect config with the given backoff parameters.
    pub fn new(reconnect: ReconnectConfig) -> Self {
        Self { reconnect }
    }
}

/// Source of the connection owned by the background worker.
enum ConnSource {
    /// Fixed pre-built connection: no reconnect on failure.
    Fixed,
    /// Factory-backed connection: rebuild on failure using the factory.
    Factory {
        factory: Arc<dyn ConnectionFactory>,
        reconnect: ReconnectConfig,
    },
}

/// A request sent through the channel to the background worker.
///
/// Most callers send a `Single` frame. Callers that need multiple frames to
/// land on the wire contiguously (without other tasks' commands interleaving)
/// -- for example ASKING followed by a migrated command during a cluster
/// resharding -- send a `Multi` request instead. The worker guarantees that
/// all frames inside one `Multi` are flushed back-to-back in the same
/// `execute_pipeline` call.
enum WorkerRequest {
    Single {
        frame: Frame,
        response_tx: oneshot::Sender<Result<Frame, RedisError>>,
    },
    Multi {
        frames: Vec<Frame>,
        response_tx: oneshot::Sender<Result<Vec<Frame>, RedisError>>,
    },
}

impl WorkerRequest {
    fn frame_count(&self) -> usize {
        match self {
            WorkerRequest::Single { .. } => 1,
            WorkerRequest::Multi { frames, .. } => frames.len(),
        }
    }

    fn fail(self, err: RedisError) {
        match self {
            WorkerRequest::Single { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            WorkerRequest::Multi { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
        }
    }
}

/// A `Service<Frame>` that transparently batches concurrent requests into
/// Redis pipelines for better throughput.
///
/// Uses a channel-based approach similar to Tower's `Buffer`:
///
/// 1. `call()` sends the request `Frame` plus a oneshot sender through a channel
/// 2. A background task collects requests for up to `batch_window` duration
///    or `max_batch_size` requests
/// 3. The background task sends all collected frames via `execute_pipeline`
/// 4. Each response is routed back via the corresponding oneshot sender
///
/// Compose with [`CommandAdapter`](crate::CommandAdapter) for typed commands:
///
/// ```ignore
/// let svc = CommandAdapter::new(AutoPipelineService::new(conn, config));
/// ```
pub struct AutoPipelineService {
    tx: mpsc::Sender<WorkerRequest>,
    _worker: Arc<tokio::task::JoinHandle<()>>,
}

impl AutoPipelineService {
    /// Create a new auto-pipelining service wrapping the given connection.
    ///
    /// The connection is moved into a background task that exclusively owns it.
    /// All requests are sent through a channel and batched automatically.
    ///
    /// The service does **not** reconnect if the connection fails -- every
    /// subsequent request returns [`RedisError::ConnectionClosed`]. Wrap this
    /// in a reconnect layer, or use [`Self::with_factory`] to build a
    /// service that rebuilds its own connection on failure.
    pub fn new(conn: RedisConnection, config: AutoPipelineConfig) -> Self {
        Self::from_parts(conn, config, ConnSource::Fixed)
    }

    /// Create a new auto-pipelining service that rebuilds its connection on
    /// failure using the provided [`ConnectionFactory`].
    ///
    /// On pipeline execution failure, in-flight requests receive
    /// [`RedisError::ConnectionClosed`], then the worker reconnects via the
    /// factory with exponential backoff governed by `reconnect`. Subsequent
    /// requests are served by the new connection.
    ///
    /// The factory is also the right place to replay session setup
    /// (AUTH, SELECT, HELLO, READONLY) on every reconnect -- see
    /// [`UrlConnectionFactory`](crate::reconnect::UrlConnectionFactory)
    /// for a ready-made AUTH+SELECT factory.
    pub async fn with_factory(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
    ) -> Result<Self, RedisError> {
        let factory: Arc<dyn ConnectionFactory> = Arc::new(factory);
        let conn = factory.connect().await?;
        let source = ConnSource::Factory {
            factory,
            reconnect: reconnect.reconnect,
        };
        Ok(Self::from_parts(conn, config, source))
    }

    fn from_parts(conn: RedisConnection, config: AutoPipelineConfig, source: ConnSource) -> Self {
        let (tx, rx) = mpsc::channel(config.max_batch_size * 2);
        let worker = tokio::spawn(pipeline_worker(rx, conn, config, source));
        Self {
            tx,
            _worker: Arc::new(worker),
        }
    }

    /// Send multiple frames through the service as a single atomic batch.
    ///
    /// The worker guarantees that all frames in the supplied slice are
    /// flushed back-to-back in one [`RedisConnection::execute_pipeline`]
    /// call, with no interleaving from other concurrent callers. This is
    /// what you want for sequences like `ASKING` + the migrated command
    /// during cluster resharding, where ordering on a single connection
    /// matters.
    ///
    /// Returns one response frame per input frame, in order. If any frame's
    /// response is an error, the overall call still returns the full
    /// response vector -- error inspection is left to the caller.
    pub async fn call_pipeline(&mut self, frames: Vec<Frame>) -> Result<Vec<Frame>, RedisError> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(WorkerRequest::Multi {
                frames,
                response_tx: resp_tx,
            })
            .await
            .map_err(|_| RedisError::ConnectionClosed)?;
        resp_rx.await.map_err(|_| RedisError::ConnectionClosed)?
    }
}

/// Background task that collects requests and executes them as pipelines.
///
/// Batch size is measured in *frames*, not requests, so a single `Multi`
/// request carrying N frames counts as N toward `max_batch_size`. This keeps
/// the effective flush size stable regardless of how many frames individual
/// callers send.
async fn pipeline_worker(
    mut rx: mpsc::Receiver<WorkerRequest>,
    mut conn: RedisConnection,
    config: AutoPipelineConfig,
    source: ConnSource,
) {
    loop {
        // Wait for the first request (blocks until one arrives).
        let first = match rx.recv().await {
            Some(req) => req,
            None => break, // channel closed, all senders dropped
        };

        let mut frame_count = first.frame_count();
        let mut batch: Vec<WorkerRequest> = vec![first];

        // Drain any immediately-available requests without waiting.
        // This handles the high-concurrency case where multiple requests
        // arrive between flushes.
        while frame_count < config.max_batch_size {
            match rx.try_recv() {
                Ok(req) => {
                    frame_count += req.frame_count();
                    batch.push(req);
                }
                Err(_) => break,
            }
        }

        // If we haven't filled the batch and the window is non-zero,
        // wait briefly for more requests to arrive.
        if frame_count < config.max_batch_size && !config.batch_window.is_zero() {
            let deadline = tokio::time::Instant::now() + config.batch_window;
            loop {
                if frame_count >= config.max_batch_size {
                    break;
                }
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(req)) => {
                        frame_count += req.frame_count();
                        batch.push(req);
                    }
                    Ok(None) => {
                        // Channel closed -- flush remaining and exit.
                        let _ = flush_batch(&mut conn, batch).await;
                        return;
                    }
                    Err(_) => break, // timeout
                }
            }
        }

        if flush_batch(&mut conn, batch).await.is_err() {
            // Pipeline execution failed. Either give up (Fixed source) or
            // reconnect via factory and keep serving.
            match &source {
                ConnSource::Fixed => {
                    // Current behavior: leave the worker running on the dead
                    // connection so any future batches also fail-fast and
                    // upstream retry layers can notice.
                }
                ConnSource::Factory { factory, reconnect } => {
                    match reconnect_with_backoff(factory.as_ref(), reconnect).await {
                        Some(new_conn) => {
                            conn = new_conn;
                        }
                        None => {
                            // Max retries exhausted. Drain any queued
                            // requests with errors and exit the worker --
                            // subsequent callers will see ConnectionClosed
                            // from poll_ready because the channel closes.
                            while let Ok(req) = rx.try_recv() {
                                req.fail(RedisError::ConnectionClosed);
                            }
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Send a batch of requests as a pipeline and route responses back.
///
/// Frames from all requests are flattened into a single `execute_pipeline`
/// call in request order, preserving within-request contiguity: every frame
/// from a given `Multi` request appears consecutively on the wire, with no
/// other caller's frames in between. Responses are partitioned back to the
/// originating request.
///
/// Returns `Ok(())` on success and `Err(())` on pipeline failure so the
/// worker can decide whether to reconnect. All individual response channels
/// are always notified before this returns.
async fn flush_batch(conn: &mut RedisConnection, batch: Vec<WorkerRequest>) -> Result<(), ()> {
    // Flatten all frames in order.
    let total_frames: usize = batch.iter().map(|r| r.frame_count()).sum();
    let mut frames: Vec<Frame> = Vec::with_capacity(total_frames);
    for req in &batch {
        match req {
            WorkerRequest::Single { frame, .. } => frames.push(frame.clone()),
            WorkerRequest::Multi { frames: fs, .. } => frames.extend(fs.iter().cloned()),
        }
    }

    match conn.execute_pipeline(frames).await {
        Ok(responses) => {
            let mut iter = responses.into_iter();
            for req in batch {
                match req {
                    WorkerRequest::Single { response_tx, .. } => {
                        if let Some(resp) = iter.next() {
                            let _ = response_tx.send(Ok(resp));
                        } else {
                            let _ = response_tx.send(Err(RedisError::ConnectionClosed));
                        }
                    }
                    WorkerRequest::Multi {
                        frames: fs,
                        response_tx,
                    } => {
                        let count = fs.len();
                        let collected: Vec<Frame> = iter.by_ref().take(count).collect();
                        if collected.len() == count {
                            let _ = response_tx.send(Ok(collected));
                        } else {
                            let _ = response_tx.send(Err(RedisError::ConnectionClosed));
                        }
                    }
                }
            }
            Ok(())
        }
        Err(_) => {
            for req in batch {
                req.fail(RedisError::ConnectionClosed);
            }
            Err(())
        }
    }
}

/// Reconnect with exponential backoff. Returns `None` if `max_retries` is
/// exhausted.
async fn reconnect_with_backoff(
    factory: &dyn ConnectionFactory,
    config: &ReconnectConfig,
) -> Option<RedisConnection> {
    let mut attempt: usize = 0;
    loop {
        if let Some(max) = config.max_retries
            && attempt >= max
        {
            warn!(
                attempts = attempt,
                "auto_pipeline: reconnect attempts exhausted"
            );
            return None;
        }
        let delay = config.delay_for_attempt(attempt);
        tokio::time::sleep(delay).await;
        attempt += 1;
        match factory.connect().await {
            Ok(conn) => {
                return Some(conn);
            }
            Err(e) => {
                warn!(attempt, error = %e, "auto_pipeline: reconnect attempt failed");
                continue;
            }
        }
    }
}

impl Service<Frame> for AutoPipelineService {
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.tx.is_closed() {
            Poll::Ready(Err(RedisError::ConnectionClosed))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, frame: Frame) -> Self::Future {
        let (resp_tx, resp_rx) = oneshot::channel();
        let tx = self.tx.clone();
        Box::pin(async move {
            tx.send(WorkerRequest::Single {
                frame,
                response_tx: resp_tx,
            })
            .await
            .map_err(|_| RedisError::ConnectionClosed)?;
            resp_rx.await.map_err(|_| RedisError::ConnectionClosed)?
        })
    }
}

impl Clone for AutoPipelineService {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _worker: Arc::clone(&self._worker),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let config = AutoPipelineConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.batch_window, Duration::ZERO);
    }

    #[test]
    fn config_custom() {
        let config = AutoPipelineConfig {
            max_batch_size: 50,
            batch_window: Duration::from_micros(500),
        };
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.batch_window, Duration::from_micros(500));
    }

    #[tokio::test]
    async fn closed_channel_error_is_retryable() {
        // When the background worker is gone (connection death), the error
        // must be retryable so upstream retry layers can reconnect.
        let (tx, rx) = mpsc::channel::<WorkerRequest>(1);
        drop(rx);
        let mut svc = AutoPipelineService {
            tx,
            _worker: Arc::new(tokio::spawn(async {})),
        };

        let frame = Frame::SimpleString(b"PING"[..].into());
        let err = svc.call(frame).await.unwrap_err();
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn closed_channel_returns_error() {
        // Create a service with a channel that we immediately close.
        let (tx, rx) = mpsc::channel::<WorkerRequest>(1);
        drop(rx); // close the receiver
        let mut svc = AutoPipelineService {
            tx,
            _worker: Arc::new(tokio::spawn(async {})),
        };

        // poll_ready should report closed.
        let ready = futures::future::poll_fn(|cx| svc.poll_ready(cx)).await;
        assert!(ready.is_err());

        // call should also fail.
        let frame = Frame::SimpleString(b"PING"[..].into());
        let result = svc.call(frame).await;
        assert!(result.is_err());
    }
}
