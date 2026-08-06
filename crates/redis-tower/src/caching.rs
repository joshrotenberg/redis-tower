//! Client-side caching with automatic invalidation.
//!
//! [`CachedClient`] keeps a RESP3 data connection and a separate invalidation
//! receiver. Redis redirects invalidations to that receiver. If the receiver
//! is lost, the local cache is cleared and disabled before reconnection; it is
//! enabled again only after a new receiver ID has been installed on the data
//! connection. Losing the fixed data connection clears and permanently closes
//! caching for that client, which must then be rebuilt.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use redis_tower_commands::{ClientId, ClientTracking};
use redis_tower_core::{
    Command, ConnectionConfig, Frame, ProtocolVersion, RedisConnection, RedisError,
};
use tokio::sync::{RwLock, mpsc, watch};

use crate::auto_pipeline::AutoPipelineConfig;
use crate::cache_state::{
    CacheState, CacheStatistics, DEFAULT_MAX_ENTRIES, DEFAULT_TTL, managed_cache_state_command,
    parse_invalidation,
};
use crate::metrics_layer::MetricsRecorder;
use crate::multiplexed::CachedMultiplexedClient;
use crate::reconnect::{ConnectionFactory, Resp3AddrConnectionFactory, UrlConnectionFactory};

/// Redis server-assisted tracking mode used by a cached client.
///
/// The default is broadcast mode without prefix filters. This preserves the
/// behavior of [`CachedClient::connect`] while allowing applications to limit
/// invalidation traffic or opt in one cacheable command at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheTrackingMode {
    /// Receive invalidations for every key matching one of `prefixes`.
    /// An empty prefix list broadcasts invalidations for all keys.
    Broadcast {
        /// Binary-safe key prefixes supplied to `CLIENT TRACKING`.
        prefixes: Vec<Bytes>,
    },
    /// Redis' default tracking mode: only keys read through this connection
    /// are tracked.
    ServerDefault,
    /// Track only cacheable reads preceded by `CLIENT CACHING YES`.
    OptIn,
}

impl Default for CacheTrackingMode {
    fn default() -> Self {
        Self::Broadcast {
            prefixes: Vec::new(),
        }
    }
}

impl CacheTrackingMode {
    /// Broadcast invalidations for every key.
    pub fn broadcast() -> Self {
        Self::default()
    }

    /// Broadcast invalidations only for the supplied binary-safe prefixes.
    pub fn broadcast_with_prefixes(prefixes: impl IntoIterator<Item = impl Into<Bytes>>) -> Self {
        Self::Broadcast {
            prefixes: prefixes.into_iter().map(Into::into).collect(),
        }
    }

    /// Whether cacheable reads need an atomic `CLIENT CACHING YES` prefix.
    #[doc(hidden)]
    pub fn is_opt_in(&self) -> bool {
        matches!(self, Self::OptIn)
    }

    /// Build the connection-local tracking command for a receiver connection.
    #[doc(hidden)]
    pub fn tracking_command(&self, receiver_id: i64) -> ClientTracking {
        let command = ClientTracking::on().redirect(receiver_id).noloop();
        match self {
            Self::Broadcast { prefixes } => prefixes
                .iter()
                .fold(command.bcast(), |command, prefix| command.prefix(prefix)),
            Self::ServerDefault => command,
            Self::OptIn => command.optin(),
        }
    }
}

/// Configuration for [`CachedClient`] and cloneable cached multiplexed clients.
#[derive(Clone)]
pub struct CachedClientConfig {
    /// Maximum number of local entries (`0` means unbounded).
    pub max_entries: usize,
    /// Client-side freshness deadline. `None` disables the TTL backstop.
    pub client_ttl: Option<Duration>,
    /// Redis server-assisted tracking mode.
    pub tracking_mode: CacheTrackingMode,
    /// Optional bounded-cardinality cache metrics sink.
    pub metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl fmt::Debug for CachedClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedClientConfig")
            .field("max_entries", &self.max_entries)
            .field("client_ttl", &self.client_ttl)
            .field("tracking_mode", &self.tracking_mode)
            .field(
                "metrics_recorder",
                &self.metrics_recorder.as_ref().map(|_| "<recorder>"),
            )
            .finish()
    }
}

impl Default for CachedClientConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_ENTRIES,
            client_ttl: Some(DEFAULT_TTL),
            tracking_mode: CacheTrackingMode::default(),
            metrics_recorder: None,
        }
    }
}

impl CachedClientConfig {
    /// Create a configuration with safe bounded defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of local entries (`0` means unbounded).
    pub fn max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries;
        self
    }

    /// Set the client-side freshness deadline.
    pub fn client_ttl(mut self, client_ttl: Option<Duration>) -> Self {
        self.client_ttl = client_ttl;
        self
    }

    /// Select the Redis server-assisted tracking mode.
    pub fn tracking_mode(mut self, tracking_mode: CacheTrackingMode) -> Self {
        self.tracking_mode = tracking_mode;
        self
    }

    /// Emit cache events through `metrics_recorder`.
    pub fn metrics_recorder(mut self, metrics_recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(metrics_recorder);
        self
    }

    /// Construct the shared cache state described by this configuration.
    ///
    /// This is public only for sibling workspace client implementations.
    #[doc(hidden)]
    pub fn new_state(&self) -> CacheState {
        CacheState::new_with_recorder(
            self.max_entries,
            self.client_ttl,
            self.metrics_recorder.clone(),
        )
    }
}

#[doc(hidden)]
pub fn validate_cached_user_command(frame: &Frame) -> Result<(), RedisError> {
    let Some(command) = managed_cache_state_command(frame) else {
        return Ok(());
    };
    Err(RedisError::Redis(format!(
        "ERR {command} is managed internally by the cached client; use a dedicated uncached connection"
    )))
}

/// A serialized Redis client with local caching and automatic invalidation.
///
/// This compatibility facade uses the same cache service and connection actor
/// as [`CachedMultiplexedClient`], configured not to combine independent caller
/// requests in one worker batch. Atomic setup-plus-command sequences remain
/// contiguous. Clones share the data worker, cache, statistics, and
/// invalidation supervisor.
#[derive(Clone)]
pub struct CachedClient {
    inner: CachedMultiplexedClient,
}

impl CachedClient {
    /// Connect with safe defaults (broadcast invalidations, 30-second client
    /// TTL, and at most 10,000 entries).
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_config(addr, CachedClientConfig::default()).await
    }

    /// Connect with explicit cache and tracking configuration.
    pub async fn connect_with_config(
        addr: &str,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::from_factory(Resp3AddrConnectionFactory::new(addr), config).await
    }

    /// Connect to `host:port` with explicit transport and RESP decode settings.
    ///
    /// Client-side caching always forces RESP3, regardless of the protocol
    /// policy in `connection_config`. The remaining settings are applied to
    /// both the data connection and every invalidation receiver.
    pub async fn connect_with_connection_config(
        addr: &str,
        connection_config: &ConnectionConfig,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let factory =
            Resp3AddrConnectionFactory::new(addr).with_connection_config(connection_config.clone());
        Self::from_factory(factory, config).await
    }

    /// Connect using a Redis URL (`redis://`, `rediss://`, or `unix://`) with
    /// safe cache defaults.
    ///
    /// URL authentication and database selection are applied independently to
    /// the data and invalidation connections.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        Self::connect_url_with_config(url, CachedClientConfig::default()).await
    }

    /// Connect using a Redis URL with explicit cache and tracking
    /// configuration.
    pub async fn connect_url_with_config(
        url: &str,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::connect_url_with_connection_config(url, &ConnectionConfig::new(), config).await
    }

    /// Connect using a Redis URL with explicit transport and RESP decode
    /// settings.
    ///
    /// Client-side caching always forces RESP3. For custom TLS roots or mTLS,
    /// build a [`UrlConnectionFactory`] and pass it to [`Self::from_factory`].
    pub async fn connect_url_with_connection_config(
        url: &str,
        connection_config: &ConnectionConfig,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let connection_config = connection_config
            .clone()
            .with_protocol(ProtocolVersion::Resp3);
        let factory = UrlConnectionFactory::new(url).with_connection_config(connection_config);
        Self::from_factory(factory, config).await
    }

    /// Connect the data and invalidation paths through one shared factory.
    ///
    /// The first connection becomes the fixed data connection. The same
    /// factory creates the invalidation receiver and any replacement receiver
    /// after tracking loss. Every returned connection is forced to RESP3.
    pub async fn from_factory(
        factory: impl ConnectionFactory,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let inner = CachedMultiplexedClient::from_factory_with_pipeline_config(
            factory,
            config,
            serialized_pipeline_config(),
        )
        .await?;
        Ok(Self { inner })
    }

    /// Wrap an existing data connection and create invalidation receivers with
    /// `receiver_factory`.
    ///
    /// The existing connection is upgraded to RESP3 if necessary. The factory
    /// must reproduce any authentication and transport setup required for a
    /// second connection to the same Redis server.
    pub async fn from_connection_with_factory(
        conn: RedisConnection,
        receiver_factory: impl ConnectionFactory,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let inner = CachedMultiplexedClient::from_connection_with_factory_and_pipeline_config(
            conn,
            receiver_factory,
            config,
            serialized_pipeline_config(),
        )
        .await?;
        Ok(Self { inner })
    }

    /// Execute a command. Cacheable reads may be served locally.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        self.inner.execute(cmd).await
    }

    /// Get the number of entries in the cache.
    pub async fn cache_size(&self) -> usize {
        self.inner.cache_size().await
    }

    /// Clear the local cache.
    pub async fn clear_cache(&self) {
        self.inner.clear_cache().await;
    }

    /// Return aggregate hit/miss/invalidation/eviction counters.
    pub async fn cache_statistics(&self) -> CacheStatistics {
        self.inner.cache_statistics().await
    }

    /// Whether both the data worker and invalidation tracking are healthy and
    /// cache reads are active.
    pub async fn is_caching_healthy(&self) -> bool {
        self.inner.is_caching_healthy().await
    }

    /// Gracefully stop the invalidation supervisor and data worker.
    ///
    /// If other client clones remain, this returns immediately and their
    /// shared lifecycle continues running. The final clone waits for both
    /// background tasks to stop before returning.
    pub async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

fn serialized_pipeline_config() -> AutoPipelineConfig {
    AutoPipelineConfig {
        max_batch_size: 1,
        batch_window: Duration::ZERO,
        ..AutoPipelineConfig::default()
    }
}

pub(crate) type TrackingStream = Pin<
    Box<dyn Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>> + Send + 'static>,
>;

pub(crate) type TrackingConfigurator = Arc<
    dyn Fn(i64) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send + 'static>>
        + Send
        + Sync,
>;

/// Shared fail-closed gate for the auto-pipeline data connection.
///
/// The private receiver lets request paths notice an unseen `false -> true`
/// sequence even when Tokio's watch channel coalesces it to a latest value of
/// `true`. Any unseen transition closes the gate synchronously. Only the
/// tracking supervisor reopens it, after clearing the cache and reinstalling
/// the REDIRECT configuration on the current data connection.
#[derive(Clone)]
pub(crate) struct DataConnectionHealth {
    inner: Arc<DataConnectionHealthInner>,
}

struct DataConnectionHealthInner {
    receiver: StdMutex<watch::Receiver<bool>>,
    usable: AtomicBool,
}

impl DataConnectionHealth {
    pub(crate) fn new(receiver: watch::Receiver<bool>) -> Self {
        let usable = *receiver.borrow();
        Self {
            inner: Arc::new(DataConnectionHealthInner {
                receiver: StdMutex::new(receiver),
                usable: AtomicBool::new(usable),
            }),
        }
    }

    pub(crate) fn is_usable(&self) -> bool {
        if !self.inner.usable.load(Ordering::Acquire) {
            return false;
        }

        let mut receiver = self
            .inner
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match receiver.has_changed() {
            Ok(true) => {
                let _latest = *receiver.borrow_and_update();
                self.inner.usable.store(false, Ordering::Release);
                false
            }
            Ok(false) if *receiver.borrow() => self.inner.usable.load(Ordering::Acquire),
            Ok(false) | Err(_) => {
                self.inner.usable.store(false, Ordering::Release);
                false
            }
        }
    }

    pub(crate) fn mark_unusable(&self) {
        self.inner.usable.store(false, Ordering::Release);
    }

    /// Consume the current healthy version immediately before configuring
    /// tracking while leaving cache use disabled.
    pub(crate) fn begin_reconfigure(&self) -> bool {
        self.mark_unusable();
        let mut receiver = self
            .inner
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if receiver.has_changed().is_err() {
            return false;
        }
        *receiver.borrow_and_update()
    }

    /// Reopen the gate only when no data-connection transition occurred after
    /// [`begin_reconfigure`](Self::begin_reconfigure).
    pub(crate) fn mark_reconfigured(&self) -> bool {
        let receiver = self
            .inner
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(receiver.has_changed(), Ok(false)) {
            self.inner.usable.store(false, Ordering::Release);
            return false;
        }
        let healthy = *receiver.borrow();
        self.inner.usable.store(healthy, Ordering::Release);
        healthy
    }
}

/// Fails client-side caching closed while a possible mutation is in flight.
///
/// Normal completion calls [`finish`](Self::finish) after post-invalidation.
/// Cancellation drops the guard, disables/clears cache state, and never keeps
/// polling the Redis command merely to perform cleanup.
pub(crate) struct CacheMutationGuard {
    cache: Arc<RwLock<CacheState>>,
    mutations_in_flight: Arc<AtomicUsize>,
    recovery: Option<TrackingRecovery>,
    armed: bool,
}

impl CacheMutationGuard {
    pub(crate) fn new(
        cache: Arc<RwLock<CacheState>>,
        mutations_in_flight: Arc<AtomicUsize>,
        recovery: Option<TrackingRecovery>,
    ) -> Self {
        mutations_in_flight.fetch_add(1, Ordering::AcqRel);
        Self {
            cache,
            mutations_in_flight,
            recovery,
            armed: true,
        }
    }

    pub(crate) fn finish(mut self) {
        self.armed = false;
        self.mutations_in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for CacheMutationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        if let Ok(mut cache) = self.cache.try_write() {
            cache.disable();
            if self
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.request(&self.mutations_in_flight))
            {
                return;
            }
            self.mutations_in_flight.fetch_sub(1, Ordering::AcqRel);
            return;
        }

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let cache = Arc::clone(&self.cache);
            let mutations_in_flight = Arc::clone(&self.mutations_in_flight);
            let recovery = self.recovery.clone();
            runtime.spawn(async move {
                cache.write().await.disable();
                if recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.request(&mutations_in_flight))
                {
                    return;
                }
                mutations_in_flight.fetch_sub(1, Ordering::AcqRel);
            });
        } else if self
            .recovery
            .as_ref()
            .is_some_and(|recovery| recovery.request(&self.mutations_in_flight))
        {
            // The supervisor owns a runtime and will disable before recovery.
        }
        // Without a runtime, retain the gate and fail closed.
    }
}

struct RecoveryRequest {
    mutations_in_flight: Arc<AtomicUsize>,
}

/// Requests a new invalidation receiver plus an ordered tracking reconfigure.
#[derive(Clone)]
pub(crate) struct TrackingRecovery {
    tx: mpsc::UnboundedSender<RecoveryRequest>,
}

impl TrackingRecovery {
    fn request(&self, mutations_in_flight: &Arc<AtomicUsize>) -> bool {
        self.tx
            .send(RecoveryRequest {
                mutations_in_flight: Arc::clone(mutations_in_flight),
            })
            .is_ok()
    }
}

/// Force an existing connection into RESP3, which client-side caching requires.
pub(crate) async fn force_resp3(
    mut connection: RedisConnection,
) -> Result<RedisConnection, RedisError> {
    if !connection.is_resp3() {
        connection.hello(3).await?;
    }
    debug_assert!(connection.is_resp3());
    Ok(connection)
}

/// Connect through `factory` and force the resulting connection into RESP3.
pub(crate) async fn connect_resp3(
    factory: &dyn ConnectionFactory,
) -> Result<RedisConnection, RedisError> {
    force_resp3(factory.connect().await?).await
}

/// Connect a RESP3 invalidation receiver and return its Redis client ID.
async fn connect_tracking_receiver(
    factory: &dyn ConnectionFactory,
) -> Result<(i64, TrackingStream), RedisError> {
    let mut receiver = connect_resp3(factory).await?;
    let receiver_id = receiver.execute(ClientId::new()).await?;
    let framed = receiver.into_framed()?;
    let (_sink, stream) = framed.split();
    Ok((receiver_id, Box::pin(stream)))
}

/// A shared lease for a background invalidation task.
///
/// The task receives a shutdown signal when the final clone is dropped. It
/// intentionally does not capture this `Arc`, avoiding a task/owner cycle.
#[derive(Clone)]
pub(crate) struct InvalidationLifecycle {
    inner: Arc<InvalidationLifecycleInner>,
}

struct InvalidationLifecycleInner {
    shutdown: watch::Sender<bool>,
    recovery: Option<TrackingRecovery>,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for InvalidationLifecycleInner {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

impl InvalidationLifecycle {
    fn new(
        shutdown: watch::Sender<bool>,
        recovery: Option<TrackingRecovery>,
        task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            inner: Arc::new(InvalidationLifecycleInner {
                shutdown,
                recovery,
                task: StdMutex::new(Some(task)),
            }),
        }
    }

    pub(crate) fn recovery(&self) -> Option<TrackingRecovery> {
        self.inner.recovery.clone()
    }

    /// Stop and join the task when this is the final service/client lease.
    pub(crate) async fn shutdown(self) {
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }
        self.inner.shutdown.send_replace(true);
        let task = self
            .inner
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// Start a receiver supervisor, install its REDIRECT ID on the data service,
/// and keep both connected for the lifetime of the returned lease.
pub(crate) async fn start_tracking_supervisor(
    receiver_factory: Arc<dyn ConnectionFactory>,
    cache: Arc<RwLock<CacheState>>,
    configure: TrackingConfigurator,
    mut data_health: watch::Receiver<bool>,
    connection_health: DataConnectionHealth,
) -> Result<InvalidationLifecycle, RedisError> {
    let (receiver_id, initial_stream) =
        connect_tracking_receiver(receiver_factory.as_ref()).await?;
    if !*data_health.borrow() || !connection_health.begin_reconfigure() {
        cache.write().await.disable();
        return Err(RedisError::ConnectionClosed);
    }
    configure(receiver_id).await?;
    if data_health.has_changed().unwrap_or(true)
        || !*data_health.borrow()
        || !connection_health.mark_reconfigured()
    {
        connection_health.mark_unusable();
        cache.write().await.disable();
        return Err(RedisError::ConnectionClosed);
    }

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (recovery_tx, mut recovery_rx) = mpsc::unbounded_channel();
    let recovery = TrackingRecovery { tx: recovery_tx };
    let task = tokio::spawn(async move {
        let mut stream = initial_stream;
        let mut pending_recoveries = Vec::new();
        loop {
            let tracking_lost = loop {
                if !*data_health.borrow() || !connection_health.is_usable() {
                    break true;
                }
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break false;
                        }
                    }
                    changed = data_health.changed() => {
                        match changed {
                            Err(_) => {
                                connection_health.mark_unusable();
                                break false;
                            }
                            Ok(()) => {
                                // A watch receiver can coalesce false -> true.
                                // Every observed version change is therefore an
                                // outage until tracking has been reconfigured.
                                connection_health.mark_unusable();
                                break true;
                            }
                        }
                    }
                    request = recovery_rx.recv() => {
                        if let Some(request) = request {
                            pending_recoveries.push(request);
                        }
                        break true;
                    }
                    item = stream.next() => {
                        match item {
                            Some(Ok(frame)) => {
                                if let Some(keys) = parse_invalidation(&frame) {
                                    let mut state = cache.write().await;
                                    if keys.is_empty() {
                                        state.clear();
                                    } else {
                                        for key in keys {
                                            state.invalidate(&key);
                                        }
                                    }
                                }
                            }
                            Some(Err(_)) | None => break true,
                        }
                    }
                }
            };

            connection_health.mark_unusable();
            cache.write().await.disable();
            if !tracking_lost {
                return;
            }

            let mut backoff = Duration::from_millis(100);
            'reconnect: loop {
                while !*data_health.borrow() {
                    tokio::select! {
                        biased;
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                return;
                            }
                        }
                        changed = data_health.changed() => {
                            if changed.is_err() {
                                connection_health.mark_unusable();
                                return;
                            }
                            connection_health.mark_unusable();
                        }
                        request = recovery_rx.recv() => {
                            if let Some(request) = request {
                                pending_recoveries.push(request);
                            }
                        }
                    }
                }

                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    changed = data_health.changed() => {
                        if changed.is_err() {
                            connection_health.mark_unusable();
                            return;
                        }
                        connection_health.mark_unusable();
                        continue 'reconnect;
                    }
                    request = recovery_rx.recv() => {
                        if let Some(request) = request {
                            pending_recoveries.push(request);
                        }
                        continue 'reconnect;
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }

                let receiver = tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => return,
                    changed = data_health.changed() => {
                        if changed.is_err() {
                            connection_health.mark_unusable();
                            return;
                        }
                        connection_health.mark_unusable();
                        continue 'reconnect;
                    }
                    request = recovery_rx.recv() => {
                        if let Some(request) = request {
                            pending_recoveries.push(request);
                        }
                        continue 'reconnect;
                    }
                    receiver = connect_tracking_receiver(receiver_factory.as_ref()) => receiver,
                };

                match receiver {
                    Ok((receiver_id, new_stream)) => {
                        if !*data_health.borrow() || !connection_health.begin_reconfigure() {
                            continue 'reconnect;
                        }
                        let barrier_recovery_count = pending_recoveries.len();
                        let configured = tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => return,
                            changed = data_health.changed() => {
                                if changed.is_err() {
                                    connection_health.mark_unusable();
                                    return;
                                }
                                connection_health.mark_unusable();
                                continue 'reconnect;
                            }
                            request = recovery_rx.recv() => {
                                if let Some(request) = request {
                                    pending_recoveries.push(request);
                                }
                                continue 'reconnect;
                            }
                            configured = configure(receiver_id) => configured,
                        };
                        match configured {
                            Ok(()) => {
                                while let Ok(request) = recovery_rx.try_recv() {
                                    pending_recoveries.push(request);
                                }
                                if pending_recoveries.len() > barrier_recovery_count {
                                    // Queue one final configure after every
                                    // canceled mutation observed during recovery.
                                    continue 'reconnect;
                                }
                                if data_health.has_changed().unwrap_or(true)
                                    || !*data_health.borrow()
                                {
                                    connection_health.mark_unusable();
                                    continue 'reconnect;
                                }
                                if !connection_health.mark_reconfigured() {
                                    continue 'reconnect;
                                }
                                stream = new_stream;
                                cache.write().await.enable();
                                for request in pending_recoveries.drain(..) {
                                    request.mutations_in_flight.fetch_sub(1, Ordering::AcqRel);
                                }
                                break;
                            }
                            Err(error) => {
                                tracing::warn!(%error, "failed to reinstall Redis cache tracking redirect");
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to reconnect Redis cache invalidation receiver");
                    }
                }
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    });

    Ok(InvalidationLifecycle::new(
        shutdown_tx,
        Some(recovery.clone()),
        task,
    ))
}

/// Own a one-shot invalidation stream until its last cache-service clone drops.
pub(crate) fn own_invalidation_stream(
    cache: Arc<RwLock<CacheState>>,
    mut stream: TrackingStream,
) -> InvalidationLifecycle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                item = stream.next() => match item {
                    Some(Ok(frame)) => {
                        if let Some(keys) = parse_invalidation(&frame) {
                            let mut state = cache.write().await;
                            if keys.is_empty() {
                                state.clear();
                            } else {
                                for key in keys {
                                    state.invalidate(&key);
                                }
                            }
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        }
        cache.write().await.disable();
    });
    InvalidationLifecycle::new(shutdown_tx, None, task)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn existing_resp2_connection_is_forced_to_resp3() {
        use futures::{SinkExt, StreamExt};
        use redis_tower_protocol::helpers::{array, bulk};
        use tokio_util::codec::Framed;

        let (client_stream, server_stream) = tokio::net::UnixStream::pair().unwrap();
        let connection =
            RedisConnection::from_stream(redis_tower_core::RedisStream::Unix(client_stream));
        assert!(!connection.is_resp3());

        let server = tokio::spawn(async move {
            let mut framed = Framed::new(
                redis_tower_core::RedisStream::Unix(server_stream),
                redis_tower_core::RespCodec::new(),
            );
            assert_eq!(
                framed.next().await.unwrap().unwrap(),
                array(vec![bulk("HELLO"), bulk("3")])
            );
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();
        });

        let connection = force_resp3(connection).await.unwrap();
        assert!(connection.is_resp3());
        server.await.unwrap();
    }

    #[test]
    fn compatibility_client_disables_worker_batching() {
        let config = serialized_pipeline_config();
        assert_eq!(config.max_batch_size, 1);
        assert!(config.batch_window.is_zero());
    }

    #[test]
    fn data_replacement_during_tracking_setup_keeps_gate_closed() {
        let (health_tx, health_rx) = watch::channel(true);
        let health = DataConnectionHealth::new(health_rx);

        assert!(health.begin_reconfigure());
        health_tx.send_replace(false);
        health_tx.send_replace(true);

        assert!(!health.mark_reconfigured());
        assert!(!health.is_usable());

        assert!(health.begin_reconfigure());
        assert!(health.mark_reconfigured());
        assert!(health.is_usable());
    }

    #[tokio::test]
    async fn canceled_mutation_requests_ordered_tracking_recovery() {
        let cache = Arc::new(RwLock::new(CacheState::default()));
        let mutations = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let recovery = TrackingRecovery { tx };

        let guard =
            CacheMutationGuard::new(Arc::clone(&cache), Arc::clone(&mutations), Some(recovery));
        drop(guard);

        assert!(!cache.read().await.is_enabled());
        assert_eq!(mutations.load(Ordering::Acquire), 1);
        let request = rx.recv().await.expect("recovery request");

        // The supervisor releases the gate only after its same-connection
        // tracking configure command has completed as an ordering barrier.
        cache.write().await.enable();
        request.mutations_in_flight.fetch_sub(1, Ordering::AcqRel);
        assert_eq!(mutations.load(Ordering::Acquire), 0);
        assert!(cache.read().await.is_enabled());
    }
}
