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
use tokio_util::sync::PollSender;
use tower_service::Service;
use tracing::warn;

use crate::metrics_layer::MetricsRecorder;
use crate::reconnect::{ConnectionFactory, ReconnectConfig};

/// Configuration for the auto-pipelining service.
#[derive(Clone)]
pub struct AutoPipelineConfig {
    /// Maximum commands to batch before sending. Default: 100.
    pub max_batch_size: usize,
    /// Time to wait for more commands after draining the immediate queue.
    ///
    /// Default: 0 (no wait -- flushes immediately, only batches requests
    /// that arrive concurrently). Set to 1-2ms for write-heavy workloads
    /// where batching reduces round-trips.
    pub batch_window: Duration,
    /// Capacity of the internal command queue (number of pending requests
    /// that can be buffered).
    ///
    /// When the queue is full, the default behavior is back-pressure: a new
    /// request awaits a free slot (its `poll_ready` returns `Pending`). Set
    /// [`shed_load_on_full`](Self::shed_load_on_full) to fail fast with
    /// `RedisError::QueueFull` instead.
    ///
    /// Default: 1024.
    pub queue_capacity: usize,
    /// Fail fast instead of applying back-pressure when the queue is full.
    ///
    /// When `false` (default), a caller awaits a free slot before its request
    /// is accepted -- real back-pressure that paces producers to the worker's
    /// drain rate. When `true`, a full queue makes the call return
    /// `RedisError::QueueFull` immediately (load shedding), which suits callers
    /// that prefer to reject rather than wait.
    ///
    /// Default: `false`.
    pub shed_load_on_full: bool,
    /// Maximum time to wait for a batch's responses before treating the
    /// connection as failed.
    ///
    /// `None` (default) means no response deadline: a hung or black-holed node
    /// can stall this worker's whole queue until OS TCP keepalive eventually
    /// fires (minutes). Set a value so a stuck node is detected promptly -- the
    /// in-flight batch fails with [`RedisError::CommandTimeout`] and the worker
    /// discards the connection (factory-backed clients then reconnect with
    /// backoff, so a new connection serves subsequent requests).
    ///
    /// The deadline covers a whole pipelined batch's round-trip, so size it
    /// above your slowest legitimate command (a long `BLPOP`/`WAIT`/`DEBUG
    /// SLEEP` will trip it).
    ///
    /// Default: `None`.
    pub response_timeout: Option<Duration>,
    /// Optional metrics recorder for worker-level observability.
    ///
    /// When set, the background worker calls
    /// [`MetricsRecorder::pipeline_flushed`] after each batch flush, reporting
    /// how many frames went out together. This is the one signal only the
    /// worker can see -- a histogram of it shows whether auto-pipelining is
    /// actually batching (`> 1`) or every caller flushes alone (`== 1`).
    ///
    /// For per-command latency/error metrics and tracing spans, wrap the
    /// client in [`MetricsLayer`](crate::MetricsLayer) /
    /// [`TracingLayer`](crate::TracingLayer) via
    /// [`MultiplexedClient::from_layered`](crate::MultiplexedClient::from_layered) --
    /// those compose at the `Service<Frame>` layer where per-command timing is
    /// available.
    ///
    /// Default: `None`.
    pub metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
    /// Treat a `READONLY` reply as a signal that the connection points at a
    /// replica and the worker should reconnect via its factory.
    ///
    /// Off by default. A factory-backed Sentinel client enables this: when a
    /// master is demoted to a replica (`REPLICAOF`) with TCP intact, writes
    /// come back `READONLY` rather than as a connection error, so without this
    /// the worker would keep serving the demoted node forever. With it on, the
    /// caller still receives the `READONLY` error for the current command and
    /// the worker reconnects (re-querying Sentinel) so the next batch lands on
    /// the new master. Standalone and cluster clients leave this off.
    ///
    /// Default: `false`.
    pub reconnect_on_readonly: bool,
}

impl std::fmt::Debug for AutoPipelineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoPipelineConfig")
            .field("max_batch_size", &self.max_batch_size)
            .field("batch_window", &self.batch_window)
            .field("queue_capacity", &self.queue_capacity)
            .field("shed_load_on_full", &self.shed_load_on_full)
            .field("response_timeout", &self.response_timeout)
            .field(
                "metrics_recorder",
                &self.metrics_recorder.as_ref().map(|_| "<recorder>"),
            )
            .field("reconnect_on_readonly", &self.reconnect_on_readonly)
            .finish()
    }
}

impl Default for AutoPipelineConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_window: Duration::ZERO,
            queue_capacity: 1024,
            shed_load_on_full: false,
            response_timeout: None,
            metrics_recorder: None,
            reconnect_on_readonly: false,
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
    /// Reservation-based view of the same channel, used for the back-pressure
    /// path: `poll_ready` reserves a slot via [`PollSender::poll_reserve`] and
    /// `call` fills it with [`PollSender::send_item`].
    poll_tx: PollSender<WorkerRequest>,
    /// When `true`, `call` uses `try_send` and a full queue yields `QueueFull`
    /// instead of awaiting capacity. Mirrors
    /// [`AutoPipelineConfig::shed_load_on_full`].
    shed_load: bool,
    worker: Arc<WorkerHandle>,
}

/// Wrapper around the background task's [`JoinHandle`](tokio::task::JoinHandle)
/// that emits a warning when dropped without being cleanly shut down.
///
/// Stores the handle in an `Option` so it can be `take()`n by [`shutdown()`]
/// without conflicting with the `Drop` impl.
struct WorkerHandle {
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl WorkerHandle {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Returns `true` if the background task has already finished.
    fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .map(|h| h.is_finished())
            .unwrap_or(true)
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        // If the task has not yet finished when the last Arc<WorkerHandle>
        // is dropped, the JoinHandle is being abandoned. Any in-flight
        // requests in the pipeline worker may be silently dropped.
        if !self.is_finished() {
            warn!(
                "AutoPipelineService dropped without calling shutdown(); \
                 background worker may still have queued requests in flight"
            );
        }
    }
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
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let poll_tx = PollSender::new(tx.clone());
        let shed_load = config.shed_load_on_full;
        let handle = tokio::spawn(pipeline_worker(rx, conn, config, source));
        Self {
            tx,
            poll_tx,
            shed_load,
            worker: Arc::new(WorkerHandle::new(handle)),
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
        let request = WorkerRequest::Multi {
            frames,
            response_tx: resp_tx,
        };
        if self.shed_load {
            self.tx.try_send(request).map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => RedisError::QueueFull,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => RedisError::ConnectionClosed,
            })?;
        } else {
            // Back-pressure: await a free slot rather than failing fast.
            self.tx
                .reserve()
                .await
                .map_err(|_| RedisError::ConnectionClosed)?
                .send(request);
        }
        resp_rx.await.map_err(|_| RedisError::ConnectionClosed)?
    }

    /// Gracefully shut down the pipeline service.
    ///
    /// Drops this instance's sender half, signalling the background worker
    /// that no more requests will arrive from this handle. If this is the
    /// last live clone (i.e. the last `tx` sender and the last `Arc`
    /// reference to the worker handle), waits for the worker to drain the
    /// remaining queue and exit cleanly.
    ///
    /// If other clones are still alive, returns immediately -- the worker
    /// continues running until the last clone shuts down or is dropped.
    ///
    /// For clean application shutdown, prefer calling `shutdown()` over
    /// simply dropping the service.
    pub async fn shutdown(self) {
        // Drop both sender handles first: decrements the sender count. When all
        // senders are gone the worker's `recv()` returns `None` and the worker
        // exits after flushing any remaining batch. `poll_tx` holds its own
        // clone of the sender, so it must be dropped too or the worker await
        // below would hang.
        drop(self.tx);
        drop(self.poll_tx);
        // Attempt to take sole ownership of the WorkerHandle Arc. Succeeds
        // only when we hold the last reference (all other clones have already
        // been dropped or shut down), in which case we await the worker to
        // ensure the final batch is flushed before we return.
        if let Ok(mut worker_handle) = Arc::try_unwrap(self.worker)
            && let Some(handle) = worker_handle.handle.take()
        {
            let _ = handle.await;
        }
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
                        let _ = flush_batch(
                            &mut conn,
                            batch,
                            config.response_timeout,
                            config.metrics_recorder.as_ref(),
                            config.reconnect_on_readonly,
                        )
                        .await;
                        return;
                    }
                    Err(_) => break, // timeout
                }
            }
        }

        if flush_batch(
            &mut conn,
            batch,
            config.response_timeout,
            config.metrics_recorder.as_ref(),
            config.reconnect_on_readonly,
        )
        .await
        .is_err()
        {
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

/// True if `frame` is a `READONLY` error reply (write attempted against a
/// replica). Used to detect a demoted Sentinel master.
fn is_readonly_frame(frame: &Frame) -> bool {
    matches!(frame, Frame::Error(e) if e.len() >= 8 && e[..8].eq_ignore_ascii_case(b"READONLY"))
}

/// Send a batch of requests as a pipeline and route responses back.
///
/// Frames from all requests are flattened into a single `execute_pipeline`
/// call in request order, preserving within-request contiguity: every frame
/// from a given `Multi` request appears consecutively on the wire, with no
/// other caller's frames in between. Responses are partitioned back to the
/// originating request.
///
/// Takes ownership of `batch` in a single pass to move frames directly into
/// the pipeline vec, avoiding per-Frame clones. A `Responder` enum tracks
/// response routing alongside the frame vec so both the success and error
/// paths can notify all senders without a second iteration.
///
/// Returns `Ok(())` on success and `Err(())` on pipeline failure so the
/// worker can decide whether to reconnect. All individual response channels
/// are always notified before this returns.
async fn flush_batch(
    conn: &mut RedisConnection,
    batch: Vec<WorkerRequest>,
    response_timeout: Option<Duration>,
    recorder: Option<&Arc<dyn MetricsRecorder>>,
    reconnect_on_readonly: bool,
) -> Result<(), ()> {
    // Owned single-pass: move frames out of each request directly into the
    // pipeline vec, and collect response senders into a parallel `responders`
    // vec. This eliminates the per-Frame clone that the previous two-pass
    // implementation (borrow to build frames, then own to route responses)
    // required.
    let total_frames: usize = batch.iter().map(|r| r.frame_count()).sum();

    // Report the batch size -- the one observability signal only the worker can
    // see (per-command metrics belong to the composed MetricsLayer instead).
    if let Some(recorder) = recorder {
        recorder.pipeline_flushed(total_frames);
    }

    let mut frames: Vec<Frame> = Vec::with_capacity(total_frames);

    enum Responder {
        Single(oneshot::Sender<Result<Frame, RedisError>>),
        Multi(usize, oneshot::Sender<Result<Vec<Frame>, RedisError>>),
    }
    let mut responders: Vec<Responder> = Vec::with_capacity(batch.len());

    for req in batch {
        match req {
            WorkerRequest::Single { frame, response_tx } => {
                frames.push(frame);
                responders.push(Responder::Single(response_tx));
            }
            WorkerRequest::Multi {
                frames: fs,
                response_tx,
            } => {
                let count = fs.len();
                frames.extend(fs);
                responders.push(Responder::Multi(count, response_tx));
            }
        }
    }

    let exec_result = match response_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, conn.execute_pipeline(frames)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                // Response timeout: the connection has written commands whose
                // replies were never read, so its state is now unknown. Fail
                // the whole batch and signal the worker (via Err) to discard
                // and reconnect the connection.
                for responder in responders {
                    match responder {
                        Responder::Single(tx) => {
                            let _ = tx.send(Err(RedisError::CommandTimeout));
                        }
                        Responder::Multi(_, tx) => {
                            let _ = tx.send(Err(RedisError::CommandTimeout));
                        }
                    }
                }
                return Err(());
            }
        },
        None => conn.execute_pipeline(frames).await,
    };
    match exec_result {
        Ok(responses) => {
            // A READONLY reply means the connection points at a replica (e.g. a
            // Sentinel master demoted via REPLICAOF). Detect it before routing
            // so the caller still gets the error, then signal the worker to
            // reconnect via the factory onto a real master.
            let saw_readonly = reconnect_on_readonly && responses.iter().any(is_readonly_frame);

            let mut iter = responses.into_iter();
            for responder in responders {
                match responder {
                    Responder::Single(tx) => {
                        let _ = tx.send(iter.next().ok_or(RedisError::ConnectionClosed));
                    }
                    Responder::Multi(count, tx) => {
                        let collected: Vec<Frame> = iter.by_ref().take(count).collect();
                        if collected.len() == count {
                            let _ = tx.send(Ok(collected));
                        } else {
                            let _ = tx.send(Err(RedisError::ConnectionClosed));
                        }
                    }
                }
            }
            if saw_readonly { Err(()) } else { Ok(()) }
        }
        Err(_) => {
            for responder in responders {
                match responder {
                    Responder::Single(tx) => {
                        let _ = tx.send(Err(RedisError::ConnectionClosed));
                    }
                    Responder::Multi(_, tx) => {
                        let _ = tx.send(Err(RedisError::ConnectionClosed));
                    }
                }
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

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.shed_load {
            // Load-shedding: readiness only reflects channel liveness; `call`
            // does the (possibly failing) `try_send`.
            return if self.tx.is_closed() {
                Poll::Ready(Err(RedisError::ConnectionClosed))
            } else {
                Poll::Ready(Ok(()))
            };
        }
        // Back-pressure: reserve a queue slot, pending until one is free.
        self.poll_tx
            .poll_reserve(cx)
            .map_err(|_| RedisError::ConnectionClosed)
    }

    fn call(&mut self, frame: Frame) -> Self::Future {
        let (resp_tx, resp_rx) = oneshot::channel();
        let request = WorkerRequest::Single {
            frame,
            response_tx: resp_tx,
        };

        if self.shed_load {
            let tx = self.tx.clone();
            return Box::pin(async move {
                tx.try_send(request).map_err(|e| match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => RedisError::QueueFull,
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        RedisError::ConnectionClosed
                    }
                })?;
                resp_rx.await.map_err(|_| RedisError::ConnectionClosed)?
            });
        }

        // Back-pressure: fill the slot reserved by `poll_ready`. Per the Tower
        // contract, `poll_ready` must have returned `Ready(Ok)` first.
        let send_result = self
            .poll_tx
            .send_item(request)
            .map_err(|_| RedisError::ConnectionClosed);
        Box::pin(async move {
            send_result?;
            resp_rx.await.map_err(|_| RedisError::ConnectionClosed)?
        })
    }
}

impl Clone for AutoPipelineService {
    fn clone(&self) -> Self {
        // Build a fresh PollSender so the clone starts with no reservation held.
        Self {
            tx: self.tx.clone(),
            poll_tx: PollSender::new(self.tx.clone()),
            shed_load: self.shed_load,
            worker: Arc::clone(&self.worker),
        }
    }
}

impl AutoPipelineService {
    /// Returns the current number of requests pending in the internal queue.
    ///
    /// This is an instantaneous snapshot; the value may change immediately
    /// after reading. Use it for observability (metrics, health checks).
    pub fn queue_depth(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
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
        assert_eq!(config.queue_capacity, 1024);
        assert!(
            !config.shed_load_on_full,
            "back-pressure is the default; load shedding is opt-in"
        );
        assert!(
            config.response_timeout.is_none(),
            "no response deadline by default"
        );
        assert!(
            config.metrics_recorder.is_none(),
            "no metrics recorder by default"
        );
    }

    #[test]
    fn config_custom() {
        let config = AutoPipelineConfig {
            max_batch_size: 50,
            batch_window: Duration::from_micros(500),
            queue_capacity: 512,
            shed_load_on_full: true,
            response_timeout: Some(Duration::from_millis(250)),
            metrics_recorder: None,
            reconnect_on_readonly: false,
        };
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.batch_window, Duration::from_micros(500));
        assert_eq!(config.queue_capacity, 512);
        assert!(config.shed_load_on_full);
        assert_eq!(config.response_timeout, Some(Duration::from_millis(250)));
        assert!(!config.reconnect_on_readonly);
    }

    #[test]
    fn is_readonly_frame_detects_readonly_errors_only() {
        use bytes::Bytes;
        assert!(is_readonly_frame(&Frame::Error(Bytes::from(
            "READONLY You can't write against a read only replica."
        ))));
        // Case-insensitive on the prefix.
        assert!(is_readonly_frame(&Frame::Error(Bytes::from(
            "readonly nope"
        ))));
        // Other errors and non-error frames are not READONLY.
        assert!(!is_readonly_frame(&Frame::Error(Bytes::from(
            "WRONGTYPE Operation against a key"
        ))));
        assert!(!is_readonly_frame(&Frame::Error(Bytes::from("READ"))));
        assert!(!is_readonly_frame(&Frame::SimpleString(Bytes::from("OK"))));
    }

    fn make_test_svc(
        tx: mpsc::Sender<WorkerRequest>,
        handle: tokio::task::JoinHandle<()>,
        shed_load: bool,
    ) -> AutoPipelineService {
        AutoPipelineService {
            poll_tx: PollSender::new(tx.clone()),
            tx,
            shed_load,
            worker: Arc::new(WorkerHandle::new(handle)),
        }
    }

    #[tokio::test]
    async fn closed_channel_error_is_retryable() {
        // When the background worker is gone (connection death), the error
        // must be retryable so upstream retry layers can reconnect.
        let (tx, rx) = mpsc::channel::<WorkerRequest>(1);
        drop(rx);
        let mut svc = make_test_svc(tx, tokio::spawn(async {}), true);

        let frame = Frame::SimpleString(b"PING"[..].into());
        let err = svc.call(frame).await.unwrap_err();
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn closed_channel_returns_error() {
        // Create a service with a channel that we immediately close.
        let (tx, rx) = mpsc::channel::<WorkerRequest>(1);
        drop(rx); // close the receiver
        let mut svc = make_test_svc(tx, tokio::spawn(async {}), true);

        // poll_ready should report closed.
        let ready = futures::future::poll_fn(|cx| svc.poll_ready(cx)).await;
        assert!(ready.is_err());

        // call should also fail.
        let frame = Frame::SimpleString(b"PING"[..].into());
        let result = svc.call(frame).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn shutdown_last_clone_awaits_worker() {
        // Create a service whose worker exits immediately (no connection, no
        // requests -- the channel closes as soon as tx is dropped).
        let (tx, rx) = mpsc::channel::<WorkerRequest>(1);
        let handle = tokio::spawn(async move {
            // Simulate a worker that drains and exits when the channel closes.
            let mut rx = rx;
            while rx.recv().await.is_some() {}
        });
        let svc = make_test_svc(tx, handle, false);

        // shutdown() on the sole instance should drop tx, succeed in
        // Arc::try_unwrap, and await the worker to completion.
        svc.shutdown().await;
        // If we reach here the worker has exited cleanly.
    }

    #[tokio::test]
    async fn shutdown_non_last_clone_returns_immediately() {
        // When another clone is alive, Arc::try_unwrap fails and shutdown()
        // returns immediately without awaiting the worker.
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(1);
        // Spawn a worker that never exits on its own.
        let handle = tokio::spawn(futures::future::pending::<()>());
        let svc = make_test_svc(tx, handle, false);

        // Keep a second clone alive so Arc::try_unwrap will fail.
        let _clone = svc.clone();

        // shutdown() on this clone should return immediately (not hang).
        svc.shutdown().await;

        // _clone still holds the Arc; worker is still running.
        // Drop the clone to let the worker task get cleaned up.
    }

    #[tokio::test]
    async fn queue_full_returns_queue_full_error() {
        // Fill the channel (capacity 1), then verify the next call returns QueueFull.
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(1);
        // Fill the one slot without receiving.
        let (dummy_tx, _dummy_rx) = oneshot::channel();
        tx.try_send(WorkerRequest::Single {
            frame: Frame::SimpleString(b"PING"[..].into()),
            response_tx: dummy_tx,
        })
        .unwrap();

        let mut svc = make_test_svc(tx, tokio::spawn(async {}), true);

        let frame = Frame::SimpleString(b"PING"[..].into());
        let err = svc.call(frame).await.unwrap_err();
        assert!(
            matches!(err, RedisError::QueueFull),
            "expected QueueFull, got {err:?}"
        );
    }

    #[tokio::test]
    async fn backpressure_poll_ready_pends_when_full() {
        // Default (back-pressure) mode: a full queue makes poll_ready pend
        // rather than returning QueueFull.
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(1);
        // Occupy the single slot without draining it.
        let (dummy_tx, _dummy_rx) = oneshot::channel();
        tx.try_send(WorkerRequest::Single {
            frame: Frame::SimpleString(b"PING"[..].into()),
            response_tx: dummy_tx,
        })
        .unwrap();

        let mut svc = make_test_svc(tx, tokio::spawn(async {}), false);
        let pending = std::future::poll_fn(|cx| Poll::Ready(svc.poll_ready(cx).is_pending())).await;
        assert!(
            pending,
            "poll_ready must pend (back-pressure) when the queue is full"
        );
    }

    #[tokio::test]
    async fn backpressure_poll_ready_errors_when_closed() {
        // A closed channel surfaces as a (retryable) connection error from
        // poll_ready in back-pressure mode -- not a pend.
        let (tx, rx) = mpsc::channel::<WorkerRequest>(1);
        drop(rx);
        let mut svc = make_test_svc(tx, tokio::spawn(async {}), false);
        let ready = std::future::poll_fn(|cx| svc.poll_ready(cx)).await;
        let err = ready.unwrap_err();
        assert!(
            err.is_retryable(),
            "closed-channel error should be retryable"
        );
    }

    #[tokio::test]
    async fn queue_full_not_retryable() {
        assert!(!RedisError::QueueFull.is_retryable());
    }

    #[tokio::test]
    async fn queue_full_not_connection_error() {
        assert!(!RedisError::QueueFull.is_connection_error());
    }

    #[test]
    fn config_queue_capacity_default() {
        let config = AutoPipelineConfig::default();
        assert_eq!(config.queue_capacity, 1024);
    }

    #[tokio::test]
    async fn queue_depth_zero_when_empty() {
        // A fresh channel with nothing sent should report depth 0.
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(64);
        let svc = make_test_svc(tx, tokio::spawn(async {}), false);
        assert_eq!(svc.queue_depth(), 0);
    }

    #[tokio::test]
    async fn queue_depth_increases_with_pending_requests() {
        // With no receiver draining, each enqueued request raises the depth.
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(10);
        let svc = make_test_svc(tx.clone(), tokio::spawn(async {}), false);

        assert_eq!(svc.queue_depth(), 0);

        // Manually enqueue a request to simulate a queued, unconsumed item.
        let (dummy_tx, _dummy_rx) = oneshot::channel();
        tx.try_send(WorkerRequest::Single {
            frame: Frame::SimpleString(b"PING"[..].into()),
            response_tx: dummy_tx,
        })
        .unwrap();

        assert_eq!(svc.queue_depth(), 1);
    }
}
