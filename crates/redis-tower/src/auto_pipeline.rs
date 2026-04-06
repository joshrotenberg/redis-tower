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

/// Configuration for the auto-pipelining service.
#[derive(Debug, Clone)]
pub struct AutoPipelineConfig {
    /// Maximum commands to batch before sending. Default: 100.
    pub max_batch_size: usize,
    /// Time to wait for more commands before flushing. Default: 1ms.
    pub batch_window: Duration,
}

impl Default for AutoPipelineConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_window: Duration::from_millis(1),
        }
    }
}

/// A request sent through the channel to the background worker.
struct PipelineRequest {
    frame: Frame,
    response_tx: oneshot::Sender<Result<Frame, RedisError>>,
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
    tx: mpsc::Sender<PipelineRequest>,
    _worker: Arc<tokio::task::JoinHandle<()>>,
}

impl AutoPipelineService {
    /// Create a new auto-pipelining service wrapping the given connection.
    ///
    /// The connection is moved into a background task that exclusively owns it.
    /// All requests are sent through a channel and batched automatically.
    pub fn new(conn: RedisConnection, config: AutoPipelineConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.max_batch_size * 2);
        let worker = tokio::spawn(pipeline_worker(rx, conn, config));
        Self {
            tx,
            _worker: Arc::new(worker),
        }
    }
}

/// Background task that collects requests and executes them as pipelines.
async fn pipeline_worker(
    mut rx: mpsc::Receiver<PipelineRequest>,
    mut conn: RedisConnection,
    config: AutoPipelineConfig,
) {
    loop {
        // Wait for the first request.
        let first = match rx.recv().await {
            Some(req) => req,
            None => break, // channel closed, all senders dropped
        };

        let mut batch = vec![first];

        // Collect more requests within the batch window.
        let deadline = tokio::time::Instant::now() + config.batch_window;
        loop {
            if batch.len() >= config.max_batch_size {
                break;
            }
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(req)) => batch.push(req),
                Ok(None) => {
                    // Channel closed -- flush remaining batch and exit.
                    flush_batch(&mut conn, batch).await;
                    return;
                }
                Err(_) => break, // timeout reached
            }
        }

        flush_batch(&mut conn, batch).await;
    }
}

/// Send a batch of requests as a pipeline and route responses back.
async fn flush_batch(conn: &mut RedisConnection, batch: Vec<PipelineRequest>) {
    let frames: Vec<Frame> = batch.iter().map(|r| r.frame.clone()).collect();
    match conn.execute_pipeline(frames).await {
        Ok(responses) => {
            for (req, resp) in batch.into_iter().zip(responses) {
                let _ = req.response_tx.send(Ok(resp));
            }
        }
        Err(e) => {
            let msg = e.to_string();
            for req in batch {
                let _ = req.response_tx.send(Err(RedisError::Connection(
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, msg.clone()),
                )));
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
            tx.send(PipelineRequest {
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
        assert_eq!(config.batch_window, Duration::from_millis(1));
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
    async fn closed_channel_returns_error() {
        // Create a service with a channel that we immediately close.
        let (tx, rx) = mpsc::channel::<PipelineRequest>(1);
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
