//! Auto-pipelining middleware for transparent batching of concurrent requests.
//!
//! When multiple tasks issue commands concurrently, instead of sending them
//! one at a time, [`AutoPipelineService`] collects them into a batch and
//! sends them as a single Redis pipeline. Each caller gets back their
//! individual response.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{AutoPipelineConfig, AutoPipelineService, CommandAdapter, RedisConnection};
//! use redis_tower::commands::*;
//! use tower::Service;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut svc = CommandAdapter::new(AutoPipelineService::new(conn, AutoPipelineConfig::default()));
//! let value: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
//! # let _ = value;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Frame, RedisConnection, RedisError};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::PollSender;
use tower_service::Service;
use tracing::warn;

use crate::metrics_layer::MetricsRecorder;
use crate::reconnect::{
    ConnectionDisconnectReason, ConnectionEvent, ConnectionEventBus, ConnectionFactory,
    ReconnectConfig, connect_with_timeout,
};

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
/// [`AutoPipelineService::with_factory`] or
/// [`AutoPipelineService::with_lazy_factory`]. The plain
/// [`AutoPipelineService::new`] path owns a single pre-built connection and
/// does not reconnect.
#[derive(Debug, Clone, Default)]
pub struct AutoPipelineReconnectConfig {
    /// Backoff parameters for reconnection attempts.
    ///
    /// Its [`ReconnectConfig::connect_timeout`] also bounds the initial
    /// factory call made by eager and lazy `with_factory` constructors.
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

/// Why the pipeline worker discarded its current connection.
///
/// The owned Redis error is kept until event publication so formatting it is
/// skipped entirely when lifecycle events have no subscribers.
enum PipelineFailure {
    Connection(RedisError),
    CommandTimeout,
    ReadOnly,
}

impl PipelineFailure {
    fn event_reason(&self) -> ConnectionDisconnectReason {
        match self {
            Self::Connection(error) => ConnectionDisconnectReason::ConnectionError {
                error: Arc::from(error.to_string()),
            },
            Self::CommandTimeout => ConnectionDisconnectReason::CommandTimeout,
            Self::ReadOnly => ConnectionDisconnectReason::ReadOnly,
        }
    }
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

    /// Return whether the caller has dropped its response future.
    ///
    /// The worker checks this immediately before flattening a batch for the
    /// wire. Requests cancelled while waiting in `batch_window` must not be
    /// executed after their caller deadline has already expired.
    fn response_is_closed(&self) -> bool {
        match self {
            WorkerRequest::Single { response_tx, .. } => response_tx.is_closed(),
            WorkerRequest::Multi { response_tx, .. } => response_tx.is_closed(),
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
/// ```no_run
/// # use redis_tower::{AutoPipelineConfig, AutoPipelineService, CommandAdapter, RedisConnection};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// # let config = AutoPipelineConfig::default();
/// let svc = CommandAdapter::new(AutoPipelineService::new(conn, config));
/// # let _ = svc;
/// # Ok(())
/// # }
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
    /// Counts only public service handles. Dropping the final lease wakes and
    /// cancels a worker that may be inside reconnect backoff or a factory call.
    lease: WorkerLease,
    /// Shared connection-health view. The sender is owned only by the worker,
    /// so this channel closes when that worker terminates even if service
    /// clones remain alive.
    connection_health: watch::Receiver<bool>,
    worker: Arc<WorkerHandle>,
}

struct WorkerControl {
    state: Mutex<WorkerControlState>,
    shutdown_tx: watch::Sender<bool>,
    event_bus: Option<ConnectionEventBus>,
}

struct WorkerControlState {
    handles: usize,
    worker_running: bool,
    shutdown_published: bool,
}

impl WorkerControl {
    fn new(shutdown_tx: watch::Sender<bool>, event_bus: Option<ConnectionEventBus>) -> Self {
        Self {
            state: Mutex::new(WorkerControlState {
                handles: 1,
                worker_running: true,
                shutdown_published: false,
            }),
            shutdown_tx,
            event_bus,
        }
    }

    fn add_handle(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.handles = state
            .handles
            .checked_add(1)
            .expect("AutoPipelineService handle count overflowed");
    }

    fn release_handle(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.handles = state
            .handles
            .checked_sub(1)
            .expect("AutoPipelineService handle count underflowed");
        if state.handles == 0 {
            self.shutdown_tx.send_replace(true);
            if !state.worker_running {
                self.publish_shutdown_locked(&mut state);
            }
        }
    }

    fn worker_finished(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.worker_running = false;
        if state.handles == 0 {
            self.publish_shutdown_locked(&mut state);
        }
    }

    fn publish_shutdown_locked(&self, state: &mut WorkerControlState) {
        if state.shutdown_published {
            return;
        }
        state.shutdown_published = true;
        if let Some(events) = &self.event_bus {
            events.publish(ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            });
        }
    }

    /// Run `f` while holding the same lock used by final-handle release.
    /// This linearizes lifecycle events before shutdown or suppresses them
    /// after shutdown, so `Shutdown` remains the terminal event.
    fn with_active_handle<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.handles == 0 { None } else { Some(f()) }
    }
}

struct WorkerRunGuard {
    control: Arc<WorkerControl>,
}

impl Drop for WorkerRunGuard {
    fn drop(&mut self) {
        self.control.worker_finished();
    }
}

struct WorkerLifecycle {
    control: Arc<WorkerControl>,
    shutdown: watch::Receiver<bool>,
    connection_health: ConnectionHealthPublisher,
    _run_guard: WorkerRunGuard,
}

/// Worker-owned publisher for the current data-connection health.
///
/// Dropping the worker always leaves observers with a final `false` snapshot
/// before closing the watch channel.
struct ConnectionHealthPublisher {
    tx: watch::Sender<bool>,
}

impl ConnectionHealthPublisher {
    fn new(tx: watch::Sender<bool>) -> Self {
        Self { tx }
    }

    fn set(&self, healthy: bool) {
        self.tx.send_if_modified(|current| {
            let changed = *current != healthy;
            *current = healthy;
            changed
        });
    }
}

impl Drop for ConnectionHealthPublisher {
    fn drop(&mut self) {
        self.set(false);
    }
}

struct WorkerLease {
    control: Arc<WorkerControl>,
}

impl Clone for WorkerLease {
    fn clone(&self) -> Self {
        self.control.add_handle();
        Self {
            control: Arc::clone(&self.control),
        }
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.control.release_handle();
    }
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
        Self::from_parts(Some(conn), config, ConnSource::Fixed, None)
    }

    /// Create a non-reconnecting auto-pipeline service with lifecycle events.
    ///
    /// The bus receives [`ConnectionEvent::Connected`] immediately and one
    /// [`ConnectionEvent::Disconnected`] transition if the fixed connection
    /// later fails. This constructor does not add reconnection behavior; use
    /// [`Self::with_factory_and_events`] for that.
    pub fn new_with_events(
        conn: RedisConnection,
        config: AutoPipelineConfig,
        events: ConnectionEventBus,
    ) -> Self {
        Self::from_parts(Some(conn), config, ConnSource::Fixed, Some(events))
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
        Self::with_factory_inner(factory, config, reconnect, None).await
    }

    /// Create a factory-backed auto-pipeline service with lifecycle events.
    ///
    /// Subscribe before calling this constructor to observe the initial
    /// connect result. Disconnect and reconnect transitions are published by
    /// the existing pipeline worker; event delivery does not spawn a separate
    /// task and never blocks that worker.
    pub async fn with_factory_and_events(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::with_factory_inner(factory, config, reconnect, Some(events)).await
    }

    async fn with_factory_inner(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
        event_bus: Option<ConnectionEventBus>,
    ) -> Result<Self, RedisError> {
        let factory: Arc<dyn ConnectionFactory> = Arc::new(factory);
        let conn = match connect_with_timeout(factory.as_ref(), reconnect.reconnect.connect_timeout)
            .await
        {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(events) = &event_bus {
                    events.publish_with(|| ConnectionEvent::ConnectFailed {
                        error: Arc::from(error.to_string()),
                    });
                }
                return Err(error);
            }
        };
        let source = ConnSource::Factory {
            factory,
            reconnect: reconnect.reconnect,
        };
        Ok(Self::from_parts(Some(conn), config, source, event_bus))
    }

    /// Create a factory-backed service without opening a connection yet.
    ///
    /// Construction is synchronous and performs no network I/O. The first
    /// accepted request invokes the factory and waits for that connection
    /// attempt before it is sent. If the attempt fails, that request receives
    /// [`RedisError::ConnectionClosed`]; the worker remains alive and the next
    /// request makes a fresh attempt. Once connected, failures use `reconnect`
    /// in exactly the same way as [`Self::with_factory`].
    ///
    /// Connection health starts as `false` and becomes `true` immediately
    /// before the first [`ConnectionEvent::Connected`] event is published.
    /// With an event bus, a failed deferred attempt publishes
    /// [`ConnectionEvent::ConnectFailed`]. No `Connected` event is emitted
    /// merely by constructing the service.
    ///
    /// # Panics
    ///
    /// Panics when called outside an entered Tokio runtime because the
    /// lightweight request worker is spawned during construction.
    pub fn with_lazy_factory(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
    ) -> Self {
        Self::with_lazy_factory_inner(factory, config, reconnect, None)
    }

    /// Create a lazily connected factory-backed service with lifecycle events.
    ///
    /// Subscribe to `events` before this call to observe the first deferred
    /// connection result. Construction itself publishes no connection event.
    ///
    /// # Panics
    ///
    /// Panics when called outside an entered Tokio runtime.
    pub fn with_lazy_factory_and_events(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
        events: ConnectionEventBus,
    ) -> Self {
        Self::with_lazy_factory_inner(factory, config, reconnect, Some(events))
    }

    fn with_lazy_factory_inner(
        factory: impl ConnectionFactory,
        config: AutoPipelineConfig,
        reconnect: AutoPipelineReconnectConfig,
        event_bus: Option<ConnectionEventBus>,
    ) -> Self {
        let source = ConnSource::Factory {
            factory: Arc::new(factory),
            reconnect: reconnect.reconnect,
        };
        Self::from_parts(None, config, source, event_bus)
    }

    fn from_parts(
        conn: Option<RedisConnection>,
        config: AutoPipelineConfig,
        source: ConnSource,
        event_bus: Option<ConnectionEventBus>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let poll_tx = PollSender::new(tx.clone());
        let shed_load = config.shed_load_on_full;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let initially_connected = conn.is_some();
        let (connection_health_tx, connection_health) = watch::channel(initially_connected);
        let control = Arc::new(WorkerControl::new(shutdown_tx, event_bus.clone()));
        let lease = WorkerLease {
            control: Arc::clone(&control),
        };
        if initially_connected && let Some(events) = &event_bus {
            events.publish(ConnectionEvent::Connected);
        }
        let lifecycle = WorkerLifecycle {
            control: Arc::clone(&control),
            shutdown: shutdown_rx,
            connection_health: ConnectionHealthPublisher::new(connection_health_tx),
            _run_guard: WorkerRunGuard {
                control: Arc::clone(&control),
            },
        };
        let runtime = tokio::runtime::Handle::try_current()
            .expect("AutoPipelineService must be constructed inside an entered Tokio runtime");
        let handle = runtime.spawn(pipeline_worker(
            rx, conn, config, source, event_bus, lifecycle,
        ));
        Self {
            tx,
            poll_tx,
            shed_load,
            lease,
            connection_health,
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

    /// Send multiple frames through the queue slot reserved by
    /// [`Service::poll_ready`] as one atomic worker request.
    ///
    /// This is the multi-frame counterpart to [`Service::call`]. It exists for
    /// frame-level middleware that must prepend connection-local setup to one
    /// command without allowing another caller to interleave between them. In
    /// particular, client-side caching in opt-in mode uses it for `CLIENT
    /// CACHING YES` followed by the cacheable read.
    ///
    /// In back-pressure mode the caller must first drive `poll_ready` to
    /// `Ready(Ok(()))`; this method consumes that exact reservation. In
    /// load-shedding mode `poll_ready` does not reserve capacity and this
    /// method follows [`Service::call`] by using `try_send`.
    pub(crate) fn call_reserved_pipeline(
        &mut self,
        frames: Vec<Frame>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Frame>, RedisError>> + Send>> {
        if frames.is_empty() {
            // Release a back-pressure reservation even though there is no
            // worker request to fill it with.
            self.release_reservation();
            return Box::pin(async { Ok(Vec::new()) });
        }

        let (resp_tx, resp_rx) = oneshot::channel();
        let request = WorkerRequest::Multi {
            frames,
            response_tx: resp_tx,
        };

        let send_result = if self.shed_load {
            self.tx.try_send(request).map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => RedisError::QueueFull,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => RedisError::ConnectionClosed,
            })
        } else {
            self.poll_tx
                .send_item(request)
                .map_err(|_| RedisError::ConnectionClosed)
        };

        Box::pin(async move {
            send_result?;
            resp_rx.await.map_err(|_| RedisError::ConnectionClosed)?
        })
    }

    /// Release a queue slot previously reserved by [`Service::poll_ready`]
    /// without submitting a worker request.
    ///
    /// Frame middleware uses this when it satisfies a request locally after
    /// readiness was already established. Client-side cache hits are the
    /// canonical case: they must return the cached response while restoring
    /// the auto-pipeline queue's full capacity.
    pub(crate) fn release_reservation(&mut self) {
        let _ = self.poll_tx.abort_send();
    }

    /// Gracefully shut down the pipeline service.
    ///
    /// Drops this instance's sender half. If this is the last live service
    /// clone, it also signals the background worker to close its receiver,
    /// reject any late sends, drain requests that were already accepted, and
    /// exit cleanly. The method waits for that final worker exit.
    ///
    /// If other clones are still alive, returns immediately -- the worker
    /// continues running until the last clone shuts down or is dropped.
    /// When lifecycle events are configured, the service publishes exactly one
    /// [`ConnectionEvent::Disconnected`] with
    /// [`ConnectionDisconnectReason::Shutdown`] after the worker observes that
    /// the final public handle closed. An earlier unexpected `Disconnected`
    /// event does not suppress this distinct terminal transition. `shutdown()`
    /// waits for normal queued work to drain and for this event to be emitted.
    /// If the worker is reconnecting, the final handle cancels its backoff or
    /// in-progress factory call before the worker exits.
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
        // The final public service lease signals the worker independently of
        // the request channel, so reconnect backoff and hanging factories are
        // cancellation-safe too.
        drop(self.lease);
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

    /// Returns `true` while the background worker task is still running.
    ///
    /// A factory-backed worker exits only after exhausting its reconnect budget
    /// (or on a clean [`shutdown`](Self::shutdown)); once it has, this service
    /// can no longer serve requests. A cluster client uses this to detect a
    /// per-node worker that gave up on a dead address so it can replace the
    /// service during a topology refresh.
    pub fn is_alive(&self) -> bool {
        !self.worker.is_finished()
    }

    /// Return the worker's current data-connection health snapshot.
    ///
    /// This is public only so sibling workspace clients can fail closed when
    /// connection-local server state is lost. In particular, a cluster cache
    /// supervisor must not serve hits while any tracked node is disconnected.
    #[doc(hidden)]
    pub fn is_connection_healthy(&self) -> bool {
        *self.connection_health.borrow()
    }

    /// Subscribe to data-connection health transitions.
    ///
    /// The initial value is `true` for a service given an established
    /// connection and `false` for a lazily connected service. Failures publish
    /// `false` before request responders or disconnect events are notified;
    /// initial connection and factory-backed reconnection publish `true` only
    /// after a connection is installed. The channel closes when the worker
    /// terminates. This is public only for sibling workspace clients that own
    /// a coordinated lifecycle across multiple data connections.
    #[doc(hidden)]
    pub fn subscribe_connection_health(&self) -> watch::Receiver<bool> {
        self.connection_health.clone()
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
    mut conn: Option<RedisConnection>,
    config: AutoPipelineConfig,
    source: ConnSource,
    event_bus: Option<ConnectionEventBus>,
    lifecycle: WorkerLifecycle,
) {
    let WorkerLifecycle {
        control,
        mut shutdown,
        connection_health,
        _run_guard,
    } = lifecycle;
    let mut outage_reported = false;
    let mut shutting_down = false;
    loop {
        // Wait for the first request or final-handle shutdown. Closing the
        // receiver rejects any sender retained outside a public service
        // handle while still allowing already-buffered requests to drain.
        let first = if shutting_down {
            rx.recv().await
        } else {
            loop {
                tokio::select! {
                    biased;
                    () = wait_for_shutdown(&mut shutdown) => {
                        shutting_down = true;
                        rx.close();
                        break rx.recv().await;
                    }
                    request = rx.recv() => break request,
                    idle_result = read_idle_push_if_connected(&mut conn) => match idle_result {
                        Ok(()) => {
                            // A push was routed to its subscriber. Re-enter the
                            // biased select so queued requests retain priority
                            // over further unsolicited traffic.
                        }
                        Err(error) => {
                            connection_health.set(false);
                            let failure = PipelineFailure::Connection(error);
                            publish_worker_outage(
                                event_bus.as_ref(),
                                &mut outage_reported,
                                failure.event_reason(),
                                &control,
                            );
                            match &source {
                                ConnSource::Fixed => {
                                    // An idle failure proves this fixed
                                    // transport cannot serve future work.
                                    // Close the queue and reject anything that
                                    // raced with the socket notification.
                                    rx.close();
                                    while let Ok(request) = rx.try_recv() {
                                        request.fail(RedisError::ConnectionClosed);
                                    }
                                    return;
                                }
                                ConnSource::Factory { factory, reconnect } => {
                                    if !reconnect_worker_connection(
                                        conn.as_mut().expect("idle failure requires a connection"),
                                        &mut rx,
                                        factory.as_ref(),
                                        reconnect,
                                        event_bus.as_ref(),
                                        &mut shutdown,
                                        &control,
                                        &connection_health,
                                        &mut outage_reported,
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                    },
                }
            }
        };
        let first = match first {
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
        let mut channel_closed = false;
        if frame_count < config.max_batch_size && !config.batch_window.is_zero() {
            let deadline = tokio::time::Instant::now() + config.batch_window;
            loop {
                if frame_count >= config.max_batch_size {
                    break;
                }

                if shutting_down {
                    match rx.try_recv() {
                        Ok(req) => {
                            frame_count += req.frame_count();
                            batch.push(req);
                            continue;
                        }
                        Err(_) => break,
                    }
                }

                tokio::select! {
                    biased;
                    () = wait_for_shutdown(&mut shutdown) => {
                        shutting_down = true;
                        rx.close();
                    }
                    request = rx.recv() => match request {
                        Some(req) => {
                            frame_count += req.frame_count();
                            batch.push(req);
                        }
                        None => {
                            channel_closed = true;
                            break;
                        }
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
        }

        if conn.is_none() {
            let ConnSource::Factory { factory, reconnect } = &source else {
                unreachable!("a fixed worker always starts with a connection");
            };
            let connect = connect_with_timeout(factory.as_ref(), reconnect.connect_timeout);
            tokio::pin!(connect);
            let connect_result = tokio::select! {
                biased;
                () = wait_for_shutdown(&mut shutdown) => {
                    shutting_down = true;
                    rx.close();
                    fail_requests(batch);
                    continue;
                }
                result = &mut connect => result,
            };
            match connect_result {
                Ok(new_conn) => {
                    let connected = control.with_active_handle(|| {
                        connection_health.set(true);
                        if let Some(events) = &event_bus {
                            events.publish(ConnectionEvent::Connected);
                        }
                    });
                    if connected.is_none() {
                        fail_requests(batch);
                        return;
                    }
                    conn = Some(new_conn);
                }
                Err(error) => {
                    control.with_active_handle(|| {
                        if let Some(events) = &event_bus {
                            events.publish_with(|| ConnectionEvent::ConnectFailed {
                                error: Arc::from(error.to_string()),
                            });
                        }
                    });
                    fail_requests(batch);
                    continue;
                }
            }
        }

        let flush_result = flush_batch(
            conn.as_mut().expect("connection established before flush"),
            batch,
            config.response_timeout,
            config.metrics_recorder.as_ref(),
            config.reconnect_on_readonly,
            &connection_health,
        )
        .await;

        if channel_closed {
            if let Err(failure) = flush_result {
                publish_worker_outage(
                    event_bus.as_ref(),
                    &mut outage_reported,
                    failure.event_reason(),
                    &control,
                );
            }
            return;
        }

        if let Err(failure) = flush_result {
            publish_worker_outage(
                event_bus.as_ref(),
                &mut outage_reported,
                failure.event_reason(),
                &control,
            );
            // Pipeline execution failed. Either give up (Fixed source) or
            // reconnect via factory and keep serving.
            match &source {
                ConnSource::Fixed => {
                    // Current behavior: leave the worker running on the dead
                    // connection so any future batches also fail-fast and
                    // upstream retry layers can notice.
                }
                ConnSource::Factory { factory, reconnect } => {
                    if !reconnect_worker_connection(
                        conn.as_mut().expect("failed flush requires a connection"),
                        &mut rx,
                        factory.as_ref(),
                        reconnect,
                        event_bus.as_ref(),
                        &mut shutdown,
                        &control,
                        &connection_health,
                        &mut outage_reported,
                    )
                    .await
                    {
                        return;
                    }
                }
            }
        }
    }
}

/// Wait for an unsolicited push only while a connection exists.
///
/// A lazy worker has no socket to poll before its first command, so this stays
/// pending and lets the request/shutdown branches of the surrounding select
/// drive progress.
async fn read_idle_push_if_connected(conn: &mut Option<RedisConnection>) -> Result<(), RedisError> {
    match conn {
        Some(conn) => conn.read_idle_push().await,
        None => futures::future::pending().await,
    }
}

fn fail_requests(requests: Vec<WorkerRequest>) {
    for request in requests {
        request.fail(RedisError::ConnectionClosed);
    }
}

/// Replace a failed factory-backed worker connection.
///
/// Returns `true` when a replacement was installed and the worker should
/// continue. All terminal outcomes fail requests queued during reconnect and
/// return `false`.
#[allow(clippy::too_many_arguments)]
async fn reconnect_worker_connection(
    conn: &mut RedisConnection,
    rx: &mut mpsc::Receiver<WorkerRequest>,
    factory: &dyn ConnectionFactory,
    reconnect: &ReconnectConfig,
    event_bus: Option<&ConnectionEventBus>,
    shutdown: &mut watch::Receiver<bool>,
    control: &WorkerControl,
    connection_health: &ConnectionHealthPublisher,
    outage_reported: &mut bool,
) -> bool {
    let started = Instant::now();
    match reconnect_with_backoff(factory, reconnect, event_bus, shutdown, control).await {
        ReconnectOutcome::Connected(new_conn, attempts) => {
            let reconnected = control.with_active_handle(|| {
                *conn = new_conn;
                // Publish health before the lifecycle event so observers can
                // rely on the event/state ordering in either direction.
                connection_health.set(true);
                *outage_reported = false;
                if let Some(events) = event_bus {
                    events.publish(ConnectionEvent::Reconnected {
                        attempts,
                        elapsed: started.elapsed(),
                    });
                }
            });
            if reconnected.is_some() {
                true
            } else {
                fail_queued_requests(rx);
                false
            }
        }
        ReconnectOutcome::Exhausted | ReconnectOutcome::Shutdown => {
            fail_queued_requests(rx);
            false
        }
    }
}

fn fail_queued_requests(rx: &mut mpsc::Receiver<WorkerRequest>) {
    rx.close();
    while let Ok(request) = rx.try_recv() {
        request.fail(RedisError::ConnectionClosed);
    }
}

fn publish_worker_outage(
    event_bus: Option<&ConnectionEventBus>,
    outage_reported: &mut bool,
    reason: ConnectionDisconnectReason,
    control: &WorkerControl,
) {
    if *outage_reported {
        return;
    }
    let active = control.with_active_handle(|| {
        if let Some(events) = event_bus {
            events.publish(ConnectionEvent::Disconnected { reason });
        }
    });
    if active.is_some() {
        *outage_reported = true;
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
/// Returns `Ok(())` on success and a typed [`PipelineFailure`] on connection
/// loss, timeout, or `READONLY` so the worker can publish the cause and decide
/// whether to reconnect. All individual response channels are always notified
/// before this returns.
async fn flush_batch(
    conn: &mut RedisConnection,
    batch: Vec<WorkerRequest>,
    response_timeout: Option<Duration>,
    recorder: Option<&Arc<dyn MetricsRecorder>>,
    reconnect_on_readonly: bool,
    connection_health: &ConnectionHealthPublisher,
) -> Result<(), PipelineFailure> {
    // A request can be accepted into the worker queue and then cancelled while
    // this worker waits out `batch_window`. `timeout_at` drops the response
    // receiver at the command deadline; prune those requests at the last
    // batching boundary before frames are moved into the wire pipeline.
    let mut batch = batch;
    batch.retain(|request| !request.response_is_closed());
    if batch.is_empty() {
        return Ok(());
    }

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
                connection_health.set(false);
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
                return Err(PipelineFailure::CommandTimeout);
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

            if saw_readonly {
                connection_health.set(false);
            }

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
            if saw_readonly {
                Err(PipelineFailure::ReadOnly)
            } else {
                Ok(())
            }
        }
        Err(error) => {
            connection_health.set(false);
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
            Err(PipelineFailure::Connection(error))
        }
    }
}

enum ReconnectOutcome {
    Connected(RedisConnection, usize),
    Exhausted,
    Shutdown,
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

/// Reconnect with exponential backoff until connected, exhausted, or the last
/// public service handle requests worker shutdown.
async fn reconnect_with_backoff(
    factory: &dyn ConnectionFactory,
    config: &ReconnectConfig,
    event_bus: Option<&ConnectionEventBus>,
    shutdown: &mut watch::Receiver<bool>,
    control: &WorkerControl,
) -> ReconnectOutcome {
    let mut attempt: usize = 0;
    loop {
        if *shutdown.borrow() {
            return ReconnectOutcome::Shutdown;
        }
        if config.attempt_exhausted(attempt) {
            warn!(
                attempts = attempt,
                "auto_pipeline: reconnect attempts exhausted"
            );
            let active = control.with_active_handle(|| {
                if let Some(events) = event_bus {
                    events.publish(ConnectionEvent::ReconnectExhausted { attempts: attempt });
                }
            });
            if active.is_none() {
                return ReconnectOutcome::Shutdown;
            }
            return ReconnectOutcome::Exhausted;
        }
        let delay = config.delay_for_attempt(attempt);
        let active = control.with_active_handle(|| {
            if let Some(events) = event_bus {
                events.publish(ConnectionEvent::ReconnectAttempt {
                    attempt: attempt + 1,
                    delay,
                });
            }
        });
        if active.is_none() {
            return ReconnectOutcome::Shutdown;
        }
        if !delay.is_zero() {
            tokio::select! {
                biased;
                () = wait_for_shutdown(shutdown) => return ReconnectOutcome::Shutdown,
                () = tokio::time::sleep(delay) => {}
            }
        }
        attempt += 1;
        let connect = connect_with_timeout(factory, config.connect_timeout);
        tokio::pin!(connect);
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return ReconnectOutcome::Shutdown,
            result = &mut connect => result,
        };
        match result {
            Ok(conn) => {
                return ReconnectOutcome::Connected(conn, attempt);
            }
            Err(e) => {
                warn!(attempt, error = %e, "auto_pipeline: reconnect attempt failed");
                let active = control.with_active_handle(|| {
                    if let Some(events) = event_bus {
                        events.publish_with(|| ConnectionEvent::ReconnectFailed {
                            attempt,
                            error: Arc::from(e.to_string()),
                        });
                    }
                });
                if active.is_none() {
                    return ReconnectOutcome::Shutdown;
                }
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
            // Accept or reject load-shed work synchronously. A returned but
            // unpolled response future must not retain a hidden channel sender
            // that can keep the worker alive after the final service handle is
            // shut down.
            let send_result = self.tx.try_send(request).map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => RedisError::QueueFull,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => RedisError::ConnectionClosed,
            });
            return Box::pin(async move {
                send_result?;
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
            lease: self.lease.clone(),
            connection_health: self.connection_health.clone(),
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
    fn lazy_factory_requires_an_entered_tokio_runtime() {
        let result = std::panic::catch_unwind(|| {
            AutoPipelineService::with_lazy_factory(
                || async { Err::<RedisConnection, _>(RedisError::ConnectionClosed) },
                AutoPipelineConfig::default(),
                AutoPipelineReconnectConfig::default(),
            )
        });
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lazy_factory_defers_connection_and_retries_after_initial_failure() {
        use futures::{SinkExt, StreamExt};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio_util::codec::Framed;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Tcp(stream),
                redis_tower_core::RespCodec::new(),
            );
            let request = framed
                .next()
                .await
                .expect("client closed before request")
                .expect("client sent an invalid request");
            assert_eq!(
                request,
                Frame::SimpleString(bytes::Bytes::from_static(b"PING"))
            );
            framed
                .send(Frame::SimpleString(bytes::Bytes::from_static(b"PONG")))
                .await
                .unwrap();
            let _ = framed.next().await;
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let factory = move || {
            let factory_calls = Arc::clone(&factory_calls);
            async move {
                if factory_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                    return Err(RedisError::ConnectionClosed);
                }
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| RedisError::connection(addr.to_string(), error))?;
                Ok(RedisConnection::from_stream(
                    redis_tower_core::RedisStream::Tcp(stream),
                ))
            }
        };

        let events = ConnectionEventBus::new(8);
        let mut event_stream = events.subscribe();
        let mut service = AutoPipelineService::with_lazy_factory_and_events(
            factory,
            AutoPipelineConfig::default(),
            AutoPipelineReconnectConfig::default(),
            events,
        );

        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert!(!service.is_connection_healthy());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), event_stream.recv())
                .await
                .is_err(),
            "lazy construction must not publish a connection event"
        );

        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        assert!(matches!(
            service
                .call(Frame::SimpleString(bytes::Bytes::from_static(b"FIRST")))
                .await,
            Err(RedisError::ConnectionClosed)
        ));
        assert!(!service.is_connection_healthy());
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed { .. }
        ));

        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        assert_eq!(
            service
                .call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")))
                .await
                .unwrap(),
            Frame::SimpleString(bytes::Bytes::from_static(b"PONG"))
        );
        assert!(service.is_connection_healthy());
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );
        assert_eq!(calls.load(Ordering::Acquire), 2);

        service.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lazy_factory_shutdown_cancels_first_connection_attempt() {
        use tokio::sync::Notify;

        let entered = Arc::new(Notify::new());
        let factory_entered = Arc::clone(&entered);
        let factory = move || {
            let factory_entered = Arc::clone(&factory_entered);
            async move {
                factory_entered.notify_one();
                futures::future::pending::<Result<RedisConnection, RedisError>>().await
            }
        };
        let mut service = AutoPipelineService::with_lazy_factory(
            factory,
            AutoPipelineConfig {
                shed_load_on_full: true,
                ..AutoPipelineConfig::default()
            },
            AutoPipelineReconnectConfig::default(),
        );

        let response = service.call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")));
        entered.notified().await;
        tokio::time::timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("shutdown must cancel a deferred connection attempt");
        assert!(matches!(response.await, Err(RedisError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn initial_factory_timeout_publishes_connect_failed() {
        let factory =
            || async { futures::future::pending::<Result<RedisConnection, RedisError>>().await };
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let reconnect = AutoPipelineReconnectConfig::new(
            ReconnectConfig::default().connect_timeout(Duration::from_millis(10)),
        );

        let result = AutoPipelineService::with_factory_and_events(
            factory,
            AutoPipelineConfig::default(),
            reconnect,
            events,
        )
        .await;
        assert!(matches!(result, Err(RedisError::ConnectTimeout)));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed {
                error: Arc::from(RedisError::ConnectTimeout.to_string()),
            }
        );
    }

    #[tokio::test]
    async fn reconnect_factory_timeout_is_counted_and_exhausted() {
        let factory =
            || async { futures::future::pending::<Result<RedisConnection, RedisError>>().await };
        let config = ReconnectConfig {
            max_retries: Some(0),
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            connect_timeout: Some(Duration::from_millis(10)),
        };
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        let control = WorkerControl::new(shutdown_tx, None);

        assert!(matches!(
            reconnect_with_backoff(&factory, &config, Some(&events), &mut shutdown, &control,)
                .await,
            ReconnectOutcome::Exhausted
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed {
                attempt: 1,
                error: Arc::from(RedisError::ConnectTimeout.to_string()),
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectExhausted { attempts: 1 }
        );
    }

    #[tokio::test]
    async fn reconnect_exhaustion_is_published_after_final_failure() {
        let factory = || async { Err::<RedisConnection, _>(RedisError::ConnectionClosed) };
        let config = ReconnectConfig {
            max_retries: Some(2),
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            connect_timeout: None,
        };
        let events = ConnectionEventBus::new(8);
        let mut stream = events.subscribe();
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        let control = WorkerControl::new(shutdown_tx, None);

        assert!(matches!(
            reconnect_with_backoff(&factory, &config, Some(&events), &mut shutdown, &control,)
                .await,
            ReconnectOutcome::Exhausted
        ));

        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed { attempt: 1, .. }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 2,
                delay: Duration::ZERO,
            }
        );
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed { attempt: 2, .. }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 3,
                delay: Duration::ZERO,
            }
        );
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed { attempt: 3, .. }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectExhausted { attempts: 3 }
        );
    }

    #[tokio::test]
    async fn factory_worker_detects_idle_disconnect_and_reconnects_in_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::{Notify, oneshot};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (first_closed_tx, first_closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let _ = first_closed_tx.send(());

            let (second, _) = listener.accept().await.unwrap();
            let _second = second;
            futures::future::pending::<()>().await;
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let allow_reconnect = Arc::new(Notify::new());
        let factory_calls = Arc::clone(&calls);
        let factory_allow_reconnect = Arc::clone(&allow_reconnect);
        let factory = move || {
            let factory_calls = Arc::clone(&factory_calls);
            let factory_allow_reconnect = Arc::clone(&factory_allow_reconnect);
            async move {
                let call = factory_calls.fetch_add(1, Ordering::AcqRel);
                if call == 1 {
                    return Err(RedisError::ConnectionClosed);
                }
                if call == 2 {
                    factory_allow_reconnect.notified().await;
                }
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| RedisError::connection(addr.to_string(), error))?;
                Ok(RedisConnection::from_stream(
                    redis_tower_core::RedisStream::Tcp(stream),
                ))
            }
        };

        let events = ConnectionEventBus::new(16);
        let mut event_stream = events.subscribe();
        let reconnect = AutoPipelineReconnectConfig::new(ReconnectConfig {
            max_retries: Some(3),
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            connect_timeout: None,
        });
        let service = AutoPipelineService::with_factory_and_events(
            factory,
            AutoPipelineConfig::default(),
            reconnect,
            events,
        )
        .await
        .unwrap();
        let mut health = service.subscribe_connection_health();
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        first_closed_rx.await.unwrap();
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed {
                attempt: 1,
                error: Arc::from("connection closed"),
            }
        );
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 2,
                delay: Duration::ZERO,
            }
        );
        health.changed().await.unwrap();
        assert!(!*health.borrow_and_update());
        assert!(!service.is_connection_healthy());

        allow_reconnect.notify_one();
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Reconnected { attempts: 2, .. }
        ));
        health.changed().await.unwrap();
        assert!(*health.borrow_and_update());
        assert!(service.is_connection_healthy());
        assert_eq!(calls.load(Ordering::Acquire), 3);

        service.shutdown().await;
        server.abort();
    }

    #[tokio::test]
    async fn final_handle_shutdown_cancels_hanging_unlimited_reconnect() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::{Notify, oneshot};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (first_closed_tx, first_closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let _ = first_closed_tx.send(());
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let reconnect_entered = Arc::new(Notify::new());
        let factory_calls = Arc::clone(&calls);
        let factory_entered = Arc::clone(&reconnect_entered);
        let factory = move || {
            let factory_calls = Arc::clone(&factory_calls);
            let factory_entered = Arc::clone(&factory_entered);
            async move {
                if factory_calls.fetch_add(1, Ordering::AcqRel) > 0 {
                    factory_entered.notify_one();
                    return futures::future::pending::<Result<RedisConnection, RedisError>>().await;
                }
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| RedisError::connection(addr.to_string(), error))?;
                Ok(RedisConnection::from_stream(
                    redis_tower_core::RedisStream::Tcp(stream),
                ))
            }
        };

        let events = ConnectionEventBus::new(8);
        let mut event_stream = events.subscribe();
        let reconnect = AutoPipelineReconnectConfig::new(ReconnectConfig {
            max_retries: None,
            base_delay: Duration::ZERO,
            max_delay: Duration::from_secs(5),
            jitter: false,
            connect_timeout: None,
        });
        let service = AutoPipelineService::with_factory_and_events(
            factory,
            AutoPipelineConfig::default(),
            reconnect,
            events,
        )
        .await
        .unwrap();
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        first_closed_rx.await.unwrap();
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        reconnect_entered.notified().await;

        tokio::time::timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("final-handle shutdown must cancel a hanging reconnect factory");
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );
        assert_eq!(
            event_stream.recv().await.unwrap_err(),
            crate::reconnect::ConnectionEventRecvError::Closed
        );
        assert_eq!(calls.load(Ordering::Acquire), 2);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn exhausted_worker_still_publishes_shutdown_on_final_handle() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::oneshot;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (first_closed_tx, first_closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let _ = first_closed_tx.send(());
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let factory = move || {
            let factory_calls = Arc::clone(&factory_calls);
            async move {
                if factory_calls.fetch_add(1, Ordering::AcqRel) > 0 {
                    return Err(RedisError::ConnectionClosed);
                }
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| RedisError::connection(addr.to_string(), error))?;
                Ok(RedisConnection::from_stream(
                    redis_tower_core::RedisStream::Tcp(stream),
                ))
            }
        };

        let events = ConnectionEventBus::new(8);
        let mut event_stream = events.subscribe();
        let reconnect = AutoPipelineReconnectConfig::new(ReconnectConfig {
            max_retries: Some(0),
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            connect_timeout: None,
        });
        let service = AutoPipelineService::with_factory_and_events(
            factory,
            AutoPipelineConfig::default(),
            reconnect,
            events,
        )
        .await
        .unwrap();
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        first_closed_rx.await.unwrap();
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt { attempt: 1, .. }
        ));
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed { attempt: 1, .. }
        ));
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectExhausted { attempts: 1 }
        );

        tokio::time::timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("shutdown should join an already exhausted worker");
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );
        assert_eq!(
            event_stream.recv().await.unwrap_err(),
            crate::reconnect::ConnectionEventRecvError::Closed
        );
        assert_eq!(calls.load(Ordering::Acquire), 2);

        server.await.unwrap();
    }

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

    #[cfg(unix)]
    #[tokio::test]
    async fn fixed_worker_detects_idle_close_and_closes_health_and_queue() {
        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client));
        let events = ConnectionEventBus::new(4);
        let mut event_stream = events.subscribe();
        let mut service =
            AutoPipelineService::new_with_events(conn, AutoPipelineConfig::default(), events);
        let mut health = service.subscribe_connection_health();

        assert!(service.is_connection_healthy());
        assert!(*health.borrow());
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        drop(server);
        let disconnected = tokio::time::timeout(Duration::from_secs(1), event_stream.recv())
            .await
            .expect("idle close was not detected")
            .unwrap();
        assert!(matches!(
            disconnected,
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert!(
            !service.is_connection_healthy(),
            "health must be false before Disconnected is observable"
        );

        health
            .changed()
            .await
            .expect("the final unhealthy snapshot must be observable");
        assert!(!*health.borrow_and_update());
        assert!(
            health.changed().await.is_err(),
            "terminal fixed-worker loss must close the health channel"
        );
        assert!(
            futures::future::poll_fn(|cx| service.poll_ready(cx))
                .await
                .is_err(),
            "terminal idle loss must close the request queue"
        );

        service.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pipeline_failure_publishes_unhealthy_before_failing_responder() {
        use futures::StreamExt;
        use tokio_util::codec::Framed;

        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(server),
                redis_tower_core::RespCodec::new(),
            );
            framed
                .next()
                .await
                .expect("client closed before dispatch")
                .expect("client sent an invalid request");
            // Closing after reading the request makes this an in-flight
            // transport failure rather than an idle-close race.
        });

        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client));
        let events = ConnectionEventBus::new(4);
        let mut event_stream = events.subscribe();
        let mut service =
            AutoPipelineService::new_with_events(conn, AutoPipelineConfig::default(), events);
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        assert!(
            service
                .call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")))
                .await
                .is_err()
        );
        assert!(
            !service.is_connection_healthy(),
            "the response future observed failure before shared health changed"
        );
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));

        service.shutdown().await;
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn response_timeout_publishes_unhealthy_before_failing_responder() {
        use futures::StreamExt;
        use tokio_util::codec::Framed;

        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(server),
                redis_tower_core::RespCodec::new(),
            );
            framed
                .next()
                .await
                .expect("client closed before dispatch")
                .expect("client sent an invalid request");
            // Deliberately withhold the response until the client's deadline
            // quarantines and closes the transport.
            let _ = framed.next().await;
        });

        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client));
        let events = ConnectionEventBus::new(4);
        let mut event_stream = events.subscribe();
        let config = AutoPipelineConfig {
            response_timeout: Some(Duration::from_millis(10)),
            ..AutoPipelineConfig::default()
        };
        let mut service = AutoPipelineService::new_with_events(conn, config, events);
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        assert!(matches!(
            service
                .call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")))
                .await,
            Err(RedisError::CommandTimeout)
        ));
        assert!(
            !service.is_connection_healthy(),
            "the timeout responder was notified before shared health changed"
        );
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::CommandTimeout,
            }
        );

        service.shutdown().await;
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn idle_push_is_routed_without_delaying_the_next_request() {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::Framed;

        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let mut conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client));
        let mut pushes = conn.subscribe_pushes();
        let expected_push = Frame::Push(vec![
            Frame::SimpleString(bytes::Bytes::from_static(b"invalidate")),
            Frame::Array(Some(vec![Frame::SimpleString(bytes::Bytes::from_static(
                b"key",
            ))])),
        ]);
        let server_push = expected_push.clone();
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(server),
                redis_tower_core::RespCodec::new(),
            );
            framed.send(server_push).await.unwrap();
            framed
                .next()
                .await
                .expect("client closed before request")
                .expect("client sent an invalid request");
            framed
                .send(Frame::SimpleString(bytes::Bytes::from_static(b"PONG")))
                .await
                .unwrap();
            // Keep the socket open until the worker's graceful shutdown drops
            // its side, proving the health assertion below is not racy.
            let _ = framed.next().await;
        });

        let mut service = AutoPipelineService::new(conn, AutoPipelineConfig::default());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pushes.recv())
                .await
                .expect("idle worker did not route the push")
                .unwrap(),
            expected_push
        );
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        assert_eq!(
            service
                .call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")))
                .await
                .unwrap(),
            Frame::SimpleString(bytes::Bytes::from_static(b"PONG"))
        );
        assert!(service.is_connection_healthy());

        service.shutdown().await;
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_single_and_multi_requests_are_pruned_before_wire() {
        use tokio::io::AsyncReadExt;

        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client));
        let batch_window = Duration::from_millis(200);
        let mut service = AutoPipelineService::new(
            conn,
            AutoPipelineConfig {
                batch_window,
                ..AutoPipelineConfig::default()
            },
        );

        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let single = service.call(Frame::SimpleString(bytes::Bytes::from_static(
            b"CANCELLED-SINGLE",
        )));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), single)
                .await
                .is_err(),
            "single request unexpectedly completed without a server response"
        );

        let multi = service.call_pipeline(vec![
            Frame::SimpleString(bytes::Bytes::from_static(b"CANCELLED-MULTI-1")),
            Frame::SimpleString(bytes::Bytes::from_static(b"CANCELLED-MULTI-2")),
        ]);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), multi)
                .await
                .is_err(),
            "multi request unexpectedly completed without a server response"
        );

        // Both response receivers are closed well before the batch window
        // ends. The worker must drop both requests rather than flushing their
        // frames after the caller deadlines have expired.
        let mut bytes = [0u8; 256];
        assert!(
            tokio::time::timeout(
                batch_window + Duration::from_millis(100),
                server.read(&mut bytes)
            )
            .await
            .is_err(),
            "a cancelled queued request reached the Redis socket"
        );

        service.shutdown().await;
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
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (_connection_health_tx, connection_health) = watch::channel(true);
        let control = Arc::new(WorkerControl::new(shutdown_tx, None));
        AutoPipelineService {
            poll_tx: PollSender::new(tx.clone()),
            tx,
            shed_load,
            lease: WorkerLease { control },
            connection_health,
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
    async fn shutdown_drains_an_unpolled_shed_load_call() {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::Framed;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request = Frame::Array(Some(vec![Frame::BulkString(Some(
            bytes::Bytes::from_static(b"PING"),
        ))]));
        let expected_request = request.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Tcp(stream),
                redis_tower_core::RespCodec::new(),
            );
            assert_eq!(framed.next().await.unwrap().unwrap(), expected_request);
            framed
                .send(Frame::SimpleString(bytes::Bytes::from_static(b"PONG")))
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(stream));
        let events = ConnectionEventBus::new(4);
        let mut event_stream = events.subscribe();
        let config = AutoPipelineConfig {
            // A long window makes it deterministic that final shutdown, not
            // the timer, causes the already-accepted request to flush.
            batch_window: Duration::from_secs(30),
            shed_load_on_full: true,
            ..AutoPipelineConfig::default()
        };
        let mut service = AutoPipelineService::new_with_events(conn, config, events);
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        // Intentionally retain this future without polling it. `call` must
        // have accepted the request synchronously without hiding a sender in
        // the response future, and shutdown must drain that queued request.
        let response = service.call(request);
        tokio::time::timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("an unpolled shed-load call must not keep shutdown alive");
        assert_eq!(
            response.await.unwrap(),
            Frame::SimpleString(bytes::Bytes::from_static(b"PONG"))
        );
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );
        assert_eq!(
            event_stream.recv().await.unwrap_err(),
            crate::reconnect::ConnectionEventRecvError::Closed
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn final_shutdown_closes_an_idle_worker_with_a_retained_sender() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut byte = [0_u8; 1];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(stream));
        let service = AutoPipelineService::new(conn, AutoPipelineConfig::default());

        // Model a sender retained by an already-returned future or adapter.
        // Final public-handle shutdown must wake the idle worker independently
        // of sender count and close the receiver against any late work.
        let retained_sender = service.tx.clone();
        tokio::time::timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("final shutdown must wake an idle worker");
        assert!(retained_sender.is_closed());
        drop(retained_sender);

        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("worker shutdown must close its Redis connection")
            .unwrap();
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
    async fn shutdown_publishes_one_intentional_disconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _stream = stream;
            futures::future::pending::<()>().await;
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(stream));
        let events = ConnectionEventBus::new(4);
        let mut event_stream = events.subscribe();
        let service =
            AutoPipelineService::new_with_events(conn, AutoPipelineConfig::default(), events);
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        service.shutdown().await;
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );

        server.abort();
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
    async fn is_alive_tracks_worker_exit() {
        // A running worker reports alive.
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(1);
        let alive = make_test_svc(tx, tokio::spawn(futures::future::pending::<()>()), false);
        assert!(alive.is_alive());

        // A worker that has returned reports not-alive once the task finishes.
        let (tx2, _rx2) = mpsc::channel::<WorkerRequest>(1);
        let dead = make_test_svc(tx2, tokio::spawn(async {}), false);
        // Let the empty worker task complete before checking.
        for _ in 0..10 {
            if !dead.is_alive() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!dead.is_alive());
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
    async fn reserved_pipeline_consumes_poll_ready_slot_as_one_request() {
        let (tx, mut rx) = mpsc::channel::<WorkerRequest>(1);
        let mut svc = make_test_svc(tx, tokio::spawn(async {}), false);

        futures::future::poll_fn(|cx| svc.poll_ready(cx))
            .await
            .unwrap();

        let frames = vec![
            Frame::SimpleString(b"CLIENT CACHING YES"[..].into()),
            Frame::SimpleString(b"GET key"[..].into()),
        ];
        let response = svc.call_reserved_pipeline(frames.clone());

        let request = rx.recv().await.expect("reserved request should be queued");
        match request {
            WorkerRequest::Multi {
                frames: actual,
                response_tx,
            } => {
                assert_eq!(actual, frames);
                response_tx
                    .send(Ok(vec![
                        Frame::SimpleString(b"OK"[..].into()),
                        Frame::BulkString(Some(b"value"[..].into())),
                    ]))
                    .unwrap();
            }
            WorkerRequest::Single { .. } => panic!("reserved pipeline was split into a single"),
        }

        assert_eq!(response.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn local_response_can_release_poll_ready_reservation() {
        let (tx, _rx) = mpsc::channel::<WorkerRequest>(1);
        let mut svc = make_test_svc(tx.clone(), tokio::spawn(async {}), false);

        assert_eq!(tx.capacity(), 1);
        futures::future::poll_fn(|cx| svc.poll_ready(cx))
            .await
            .unwrap();
        assert_eq!(tx.capacity(), 0, "poll_ready should reserve capacity");

        svc.release_reservation();
        assert_eq!(
            tx.capacity(),
            1,
            "a locally satisfied request must restore queue capacity"
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
