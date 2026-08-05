//! Tower client-side cache middleware.
//!
//! [`CacheLayer`] builds cloneable [`CacheService`] values. Production users
//! should attach an invalidation stream or use a tracked cached client; a
//! cache without invalidations can serve stale data until its client TTL.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use redis_tower_commands::{ClientCaching, ClientTracking};
use redis_tower_core::{Command, Frame, FrameService, RedisError};
use tokio::sync::RwLock;
use tower_layer::Layer;
use tower_service::Service;

use crate::auto_pipeline::AutoPipelineService;
use crate::cache_state::{
    CacheState, CacheStatistics, DEFAULT_MAX_ENTRIES, DEFAULT_TTL, command_may_mutate,
    extract_cache_entry, parse_invalidation,
};
use crate::caching::{
    CacheMutationGuard, CachedClientConfig, DataConnectionHealth, InvalidationLifecycle,
    TrackingConfigurator, TrackingRecovery, own_invalidation_stream, start_tracking_supervisor,
    validate_cached_user_command,
};
use crate::metrics_layer::MetricsRecorder;
use crate::reconnect::ConnectionFactory;
use crate::reconnect_layer::ReconnectService;

type BoxRedisFuture<T> = Pin<Box<dyn Future<Output = Result<T, RedisError>> + Send>>;
type OptInDispatch<S> = fn(&mut S, Frame) -> BoxRedisFuture<Frame>;

/// Releases readiness acquired by an inner service when the cache answers a
/// request locally.
///
/// Tower establishes readiness before the request is available to
/// [`Service::call`]. An inner service may reserve bounded capacity when
/// [`Service::poll_ready`] succeeds, but a cache hit does not invoke the inner
/// service's `call`. Implementations must therefore release any capacity held
/// by the most recent successful readiness check. The method must also be safe
/// when readiness did not acquire a reservation, such as a load-shedding
/// service whose `poll_ready` only checks liveness.
///
/// There is deliberately no blanket no-op implementation: silently assuming
/// that an arbitrary Tower service has reservation-free readiness can leak
/// capacity and eventually stall the service. Custom backends should implement
/// this trait explicitly. Keep [`CacheLayer`] directly above such a backend and
/// place other middleware outside the cache unless that middleware also
/// implements and correctly propagates this contract.
pub trait ReleaseReadiness {
    /// Give back capacity acquired by the latest successful `poll_ready` when
    /// no corresponding inner `call` will be made.
    fn release_readiness(&mut self);
}

impl ReleaseReadiness for AutoPipelineService {
    fn release_readiness(&mut self) {
        self.release_reservation();
    }
}

impl ReleaseReadiness for FrameService {
    fn release_readiness(&mut self) {
        // FrameService::poll_ready only checks its owned transport's sink. It
        // does not acquire a separate permit that must be returned.
    }
}

impl ReleaseReadiness for ReconnectService {
    fn release_readiness(&mut self) {
        // ReconnectService delegates readiness to FrameService, whose readiness
        // probe does not acquire a separate permit.
    }
}

/// Size and freshness bounds for a [`CacheService`].
///
/// # Defaults
///
/// - `max_size`: 10,000 entries
/// - `ttl`: 30 seconds
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of cached entries. `0` means unbounded.
    pub max_size: usize,
    /// Per-entry client freshness deadline. `None` disables the deadline.
    pub ttl: Option<Duration>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_ENTRIES,
            ttl: Some(DEFAULT_TTL),
        }
    }
}

/// A Tower layer that shares one local cache and invalidation lifecycle across
/// every service it creates.
///
/// The wrapped service must implement [`ReleaseReadiness`]. Put this cache
/// directly above that backend, then put tracing, metrics, timeout, and other
/// middleware outside the cache. If custom middleware sits inside the cache,
/// it must propagate the readiness-release contract to its own inner service.
#[derive(Clone)]
pub struct CacheLayer {
    cache: Arc<RwLock<CacheState>>,
    mutations_in_flight: Arc<AtomicUsize>,
    tracked_prefixes: Option<Arc<Vec<Bytes>>>,
    recovery: Option<TrackingRecovery>,
    invalidation: Option<InvalidationLifecycle>,
}

impl CacheLayer {
    /// Create a layer **without server invalidations**.
    ///
    /// # Staleness warning
    ///
    /// This constructor cannot guarantee cache coherence. Entries may remain
    /// stale until `ttl` expires. Prefer a tracked cached client or
    /// [`with_invalidation_stream`](Self::with_invalidation_stream).
    pub fn new(config: CacheConfig) -> Self {
        Self::without_invalidation(config)
    }

    /// Explicitly create a layer without an invalidation source.
    pub fn without_invalidation(config: CacheConfig) -> Self {
        warn_invalidation_free();
        Self {
            cache: Arc::new(RwLock::new(CacheState::new(config.max_size, config.ttl))),
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            tracked_prefixes: None,
            recovery: None,
            invalidation: None,
        }
    }

    /// Create an invalidation-free layer with a cache metrics recorder.
    pub fn without_invalidation_with_recorder(
        config: CacheConfig,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> Self {
        warn_invalidation_free();
        Self {
            cache: Arc::new(RwLock::new(CacheState::new_with_recorder(
                config.max_size,
                config.ttl,
                Some(recorder),
            ))),
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            tracked_prefixes: None,
            recovery: None,
            invalidation: None,
        }
    }

    /// Create a layer that owns `push_stream` until the final layer/service
    /// clone is dropped. Stream termination clears and disables the cache.
    ///
    /// # Panics
    ///
    /// Panics when called outside an active Tokio runtime because ownership of
    /// the stream is maintained by a spawned task.
    pub fn with_invalidation_stream<T>(config: CacheConfig, push_stream: T) -> Self
    where
        T: Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>> + Send + 'static,
    {
        let cache = Arc::new(RwLock::new(CacheState::new(config.max_size, config.ttl)));
        let invalidation = own_invalidation_stream(Arc::clone(&cache), Box::pin(push_stream));
        Self {
            cache,
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            tracked_prefixes: None,
            recovery: None,
            invalidation: Some(invalidation),
        }
    }

    /// Create a recorder-enabled layer that owns its invalidation stream.
    ///
    /// This is the production counterpart to
    /// [`without_invalidation_with_recorder`](Self::without_invalidation_with_recorder):
    /// cache events are forwarded to `recorder`, and stream termination fails
    /// the cache closed.
    ///
    /// # Panics
    ///
    /// Panics when called outside an active Tokio runtime because ownership of
    /// the stream is maintained by a spawned task.
    pub fn with_invalidation_stream_and_recorder<T>(
        config: CacheConfig,
        push_stream: T,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> Self
    where
        T: Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>> + Send + 'static,
    {
        let cache = Arc::new(RwLock::new(CacheState::new_with_recorder(
            config.max_size,
            config.ttl,
            Some(recorder),
        )));
        let invalidation = own_invalidation_stream(Arc::clone(&cache), Box::pin(push_stream));
        Self {
            cache,
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            tracked_prefixes: None,
            recovery: None,
            invalidation: Some(invalidation),
        }
    }

    /// Get the shared cache state used by services built from this layer.
    pub fn cache(&self) -> &Arc<RwLock<CacheState>> {
        &self.cache
    }
}

impl<S: ReleaseReadiness> Layer<S> for CacheLayer {
    type Service = CacheService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CacheService {
            inner,
            cache: Arc::clone(&self.cache),
            mutations_in_flight: Arc::clone(&self.mutations_in_flight),
            data_health: None,
            tracked_prefixes: self.tracked_prefixes.clone(),
            recovery: self.recovery.clone(),
            invalidation: self.invalidation.clone(),
            opt_in_dispatch: None,
        }
    }
}

/// Tower `Service` that caches Frame responses for cacheable read commands.
///
/// The local response cache supports `GET`, `HGET`, `HGETALL`, `LRANGE`,
/// `SMEMBERS`, `ZRANGE`, and `TYPE`. Other commands pass through to the inner
/// service; known mutations synchronously invalidate affected entries.
///
/// Clones share cache state and the owned invalidation lifecycle. Cacheable
/// misses use per-key epochs so a response racing an invalidation is never
/// inserted. Non-cacheable commands are classified and locally invalidated
/// both before and after dispatch.
///
/// The inner service must implement [`ReleaseReadiness`] because a cache hit
/// returns without calling it after Tower readiness has already been acquired.
pub struct CacheService<S> {
    inner: S,
    cache: Arc<RwLock<CacheState>>,
    mutations_in_flight: Arc<AtomicUsize>,
    data_health: Option<DataConnectionHealth>,
    tracked_prefixes: Option<Arc<Vec<Bytes>>>,
    recovery: Option<TrackingRecovery>,
    invalidation: Option<InvalidationLifecycle>,
    opt_in_dispatch: Option<OptInDispatch<S>>,
}

impl<S: Clone> Clone for CacheService<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: Arc::clone(&self.cache),
            mutations_in_flight: Arc::clone(&self.mutations_in_flight),
            data_health: self.data_health.clone(),
            tracked_prefixes: self.tracked_prefixes.clone(),
            recovery: self.recovery.clone(),
            invalidation: self.invalidation.clone(),
            opt_in_dispatch: self.opt_in_dispatch,
        }
    }
}

impl<S> CacheService<S> {
    /// Wrap `inner` **without server invalidations**.
    ///
    /// # Staleness warning
    ///
    /// This compatibility constructor cannot guarantee cache coherence.
    /// Entries may remain stale until their TTL. Prefer a tracked cached
    /// client, [`CacheLayer::with_invalidation_stream`], or
    /// [`with_invalidation_stream`](Self::with_invalidation_stream).
    pub fn new(inner: S, config: CacheConfig) -> Self {
        Self::without_invalidation(inner, config)
    }

    /// Explicitly wrap `inner` without an invalidation source.
    pub fn without_invalidation(inner: S, config: CacheConfig) -> Self {
        warn_invalidation_free();
        Self {
            inner,
            cache: Arc::new(RwLock::new(CacheState::new(config.max_size, config.ttl))),
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            data_health: None,
            tracked_prefixes: None,
            recovery: None,
            invalidation: None,
            opt_in_dispatch: None,
        }
    }

    /// Create with existing shared cache state and no invalidation source.
    ///
    /// The caller is responsible for driving invalidations and failing the
    /// cache closed if that source ends. Prefer
    /// [`with_invalidation_stream`](Self::with_invalidation_stream), or pass
    /// this cache to [`spawn_invalidation_task`] and retain its task handle.
    pub fn with_cache(inner: S, cache: Arc<RwLock<CacheState>>) -> Self {
        warn_invalidation_free();
        Self {
            inner,
            cache,
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            data_health: None,
            tracked_prefixes: None,
            recovery: None,
            invalidation: None,
            opt_in_dispatch: None,
        }
    }

    /// Wrap an inner service and own its invalidation stream across clones.
    ///
    /// # Panics
    ///
    /// Panics when called outside an active Tokio runtime because ownership of
    /// the stream is maintained by a spawned task.
    pub fn with_invalidation_stream<T>(inner: S, config: CacheConfig, push_stream: T) -> Self
    where
        T: Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>> + Send + 'static,
    {
        let cache = Arc::new(RwLock::new(CacheState::new(config.max_size, config.ttl)));
        let invalidation = own_invalidation_stream(Arc::clone(&cache), Box::pin(push_stream));
        Self {
            inner,
            cache,
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            data_health: None,
            tracked_prefixes: None,
            recovery: None,
            invalidation: Some(invalidation),
            opt_in_dispatch: None,
        }
    }

    /// Wrap an inner service with an owned invalidation stream and cache-event
    /// recorder.
    ///
    /// # Panics
    ///
    /// Panics when called outside an active Tokio runtime because ownership of
    /// the stream is maintained by a spawned task.
    pub fn with_invalidation_stream_and_recorder<T>(
        inner: S,
        config: CacheConfig,
        push_stream: T,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> Self
    where
        T: Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>> + Send + 'static,
    {
        let cache = Arc::new(RwLock::new(CacheState::new_with_recorder(
            config.max_size,
            config.ttl,
            Some(recorder),
        )));
        let invalidation = own_invalidation_stream(Arc::clone(&cache), Box::pin(push_stream));
        Self {
            inner,
            cache,
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            data_health: None,
            tracked_prefixes: None,
            recovery: None,
            invalidation: Some(invalidation),
            opt_in_dispatch: None,
        }
    }
}

impl<S> CacheService<S> {
    /// Get the wrapped service.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Mutably access the wrapped service.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consume this middleware and return the wrapped service.
    ///
    /// Dropping the service also drops this clone's invalidation lease.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Get the shared cache state.
    pub fn cache(&self) -> &Arc<RwLock<CacheState>> {
        &self.cache
    }

    /// Get the current number of entries.
    pub async fn cache_size(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Clear all local entries.
    pub async fn clear_cache(&self) {
        self.cache.write().await.clear();
    }

    /// Return aggregate cache statistics.
    pub async fn cache_statistics(&self) -> CacheStatistics {
        self.cache.read().await.statistics()
    }

    /// Whether cache use is enabled, no mutation is in flight, and any managed
    /// data worker remains healthy.
    pub async fn is_caching_healthy(&self) -> bool {
        self.data_connection_is_healthy()
            && self.mutations_in_flight.load(Ordering::Acquire) == 0
            && self.cache.read().await.is_enabled()
    }

    fn data_connection_is_healthy(&self) -> bool {
        self.data_health
            .as_ref()
            .is_none_or(DataConnectionHealth::is_usable)
    }
}

impl CacheService<AutoPipelineService> {
    /// Attach a reconnecting RESP3 invalidation receiver to an auto-pipeline.
    pub(crate) async fn with_tracking(
        inner: AutoPipelineService,
        receiver_factory: Arc<dyn ConnectionFactory>,
        config: &CachedClientConfig,
    ) -> Result<Self, RedisError> {
        let cache = Arc::new(RwLock::new(config.new_state()));
        if !inner.is_connection_healthy() {
            cache.write().await.disable();
            return Err(RedisError::ConnectionClosed);
        }
        let data_health_rx = inner.subscribe_connection_health();
        let data_health = DataConnectionHealth::new(data_health_rx.clone());
        let tracking_inner = inner.clone();
        let tracking_mode = config.tracking_mode.clone();
        let configure: TrackingConfigurator = Arc::new(move |receiver_id| {
            let mut inner = tracking_inner.clone();
            let reset = ClientTracking::off();
            let command = tracking_mode.tracking_command(receiver_id);
            Box::pin(async move {
                let mut responses = inner
                    .call_pipeline(vec![reset.to_frame(), command.to_frame()])
                    .await?
                    .into_iter();
                reset.parse_response(responses.next().ok_or(RedisError::ConnectionClosed)?)?;
                command.parse_response(responses.next().ok_or(RedisError::ConnectionClosed)?)
            })
        });
        let invalidation = start_tracking_supervisor(
            receiver_factory,
            Arc::clone(&cache),
            configure,
            data_health_rx,
            data_health.clone(),
        )
        .await?;
        let recovery = invalidation.recovery();

        Ok(Self {
            inner,
            cache,
            mutations_in_flight: Arc::new(AtomicUsize::new(0)),
            data_health: Some(data_health),
            tracked_prefixes: match &config.tracking_mode {
                crate::caching::CacheTrackingMode::Broadcast { prefixes } => {
                    Some(Arc::new(prefixes.clone()))
                }
                crate::caching::CacheTrackingMode::ServerDefault
                | crate::caching::CacheTrackingMode::OptIn => None,
            },
            recovery,
            invalidation: Some(invalidation),
            opt_in_dispatch: config
                .tracking_mode
                .is_opt_in()
                .then_some(dispatch_opt_in_auto_pipeline),
        })
    }

    /// Return the number of requests currently queued in the worker.
    pub fn queue_depth(&self) -> usize {
        self.inner.queue_depth()
    }

    /// Submit an explicit batch atomically and conservatively clear the cache
    /// before and after dispatch.
    pub async fn call_pipeline(&mut self, frames: Vec<Frame>) -> Result<Vec<Frame>, RedisError> {
        for frame in &frames {
            validate_cached_user_command(frame)?;
        }
        let guard = CacheMutationGuard::new(
            Arc::clone(&self.cache),
            Arc::clone(&self.mutations_in_flight),
            self.recovery.clone(),
        );
        self.cache.write().await.clear();
        let result = self.inner.call_pipeline(frames).await;
        self.cache.write().await.clear();
        guard.finish();
        result
    }

    /// Stop tracking before shutting down the final auto-pipeline worker.
    pub async fn shutdown(self) {
        let Self {
            inner,
            invalidation,
            ..
        } = self;
        if let Some(invalidation) = invalidation {
            invalidation.shutdown().await;
        }
        inner.shutdown().await;
    }
}

fn dispatch_opt_in_auto_pipeline(
    inner: &mut AutoPipelineService,
    request: Frame,
) -> BoxRedisFuture<Frame> {
    let caching = ClientCaching::new(true);
    let future = inner.call_reserved_pipeline(vec![caching.to_frame(), request]);
    Box::pin(async move {
        let mut responses = future.await?.into_iter();
        let setup = responses.next().ok_or(RedisError::ConnectionClosed)?;
        caching.parse_response(setup)?;
        responses.next().ok_or(RedisError::ConnectionClosed)
    })
}

impl<S> Service<Frame> for CacheService<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + ReleaseReadiness,
    S::Future: Send + 'static,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = BoxRedisFuture<Frame>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        if let Err(error) = validate_cached_user_command(&request) {
            self.inner.release_readiness();
            return Box::pin(async move { Err(error) });
        }

        let extracted_entry = extract_cache_entry(&request);
        let entry = extracted_entry.clone().filter(|(_, redis_key)| {
            self.tracked_prefixes.as_ref().is_none_or(|prefixes| {
                prefixes.is_empty() || prefixes.iter().any(|prefix| redis_key.starts_with(prefix))
            })
        });

        if extracted_entry.is_some() && entry.is_none() {
            // This is a cacheable read outside a configured BCAST prefix.
            // Redis will not invalidate it, so consume readiness and pass it
            // through without perturbing valid-prefix cache traffic.
            return Box::pin(self.inner.call(request));
        }

        if extracted_entry.is_none() && !command_may_mutate(&request) {
            return Box::pin(self.inner.call(request));
        }

        let mut epoch = None;

        if self.mutations_in_flight.load(Ordering::Acquire) == 0
            && let Some((ref cache_key, ref redis_key)) = entry
            && let Ok(cache) = self.cache.try_read()
            // Linearize the health decision after acquiring cache state so a
            // previously observed `true` cannot authorize a later stale hit.
            && self.data_connection_is_healthy()
        {
            if let Some(cached) = cache.get(cache_key) {
                let response = cached.clone();
                drop(cache);
                self.inner.release_readiness();
                return Box::pin(async move { Ok(response) });
            }
            epoch = cache.snapshot_epoch(redis_key);
        }

        let request_for_invalidation = request.clone();
        let mutation_guard = if entry.is_none() {
            // Gate cache hits synchronously even if the short state write lock
            // is momentarily held by the invalidation receiver.
            let guard = CacheMutationGuard::new(
                Arc::clone(&self.cache),
                Arc::clone(&self.mutations_in_flight),
                self.recovery.clone(),
            );
            if let Ok(mut cache) = self.cache.try_write() {
                cache.invalidate_for_command(&request_for_invalidation);
            }
            Some(guard)
        } else {
            None
        };

        let future = if entry.is_some() {
            if let Some(dispatch) = self.opt_in_dispatch {
                dispatch(&mut self.inner, request)
            } else {
                Box::pin(self.inner.call(request))
            }
        } else {
            Box::pin(self.inner.call(request))
        };
        let cache = Arc::clone(&self.cache);

        if entry.is_none() {
            Box::pin(complete_mutation(
                future,
                cache,
                mutation_guard.expect("mutation entry must have a guard"),
                request_for_invalidation,
            ))
        } else {
            let data_health = self.data_health.clone();
            Box::pin(async move {
                let result = future.await;
                if let (Some((cache_key, redis_key)), Ok(response)) = (&entry, &result)
                    && !matches!(response, Frame::Error(_))
                    && let Some(epoch) = epoch
                    && data_health
                        .as_ref()
                        .is_none_or(DataConnectionHealth::is_usable)
                {
                    cache.write().await.insert_if_current(
                        cache_key.clone(),
                        redis_key.clone(),
                        response.clone(),
                        epoch,
                    );
                }
                result
            })
        }
    }
}

async fn complete_mutation(
    future: BoxRedisFuture<Frame>,
    cache: Arc<RwLock<CacheState>>,
    guard: CacheMutationGuard,
    request: Frame,
) -> Result<Frame, RedisError> {
    let result = future.await;
    cache.write().await.invalidate_for_command(&request);
    guard.finish();
    result
}

fn warn_invalidation_free() {
    static WARN_ONCE: Once = Once::new();
    WARN_ONCE.call_once(|| {
        tracing::warn!(
            "client-side cache created without Redis invalidations; entries may be stale until TTL"
        );
    });
}

/// Spawn a legacy one-shot invalidation task.
///
/// Prefer constructors that own the lifecycle across service clones. This
/// compatibility helper disables and clears the cache when `push_rx` ends, but
/// the caller remains responsible for retaining and stopping the task.
pub fn spawn_invalidation_task(
    cache: Arc<RwLock<CacheState>>,
    mut push_rx: impl Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>>
    + Unpin
    + Send
    + 'static,
) -> tokio::task::JoinHandle<()> {
    use futures::StreamExt;
    tokio::spawn(async move {
        while let Some(Ok(frame)) = push_rx.next().await {
            if let Some(keys) = parse_invalidation(&frame) {
                let mut cache = cache.write().await;
                if keys.is_empty() {
                    cache.clear();
                } else {
                    for key in keys {
                        cache.invalidate(&key);
                    }
                }
            }
        }
        cache.write().await.disable();
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics_layer::{CacheEvent, ErrorKind};
    use bytes::Bytes;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    #[derive(Clone)]
    struct CountingService {
        call_count: Arc<Mutex<usize>>,
        response: Frame,
    }

    #[derive(Default)]
    struct RecordingCacheMetrics {
        hits: AtomicU64,
        misses: AtomicU64,
    }

    impl MetricsRecorder for RecordingCacheMetrics {
        fn command_completed(
            &self,
            _command: &str,
            _duration: Duration,
            _error: Option<ErrorKind>,
        ) {
        }

        fn cache_event(&self, event: CacheEvent, count: u64) {
            let counter = match event {
                CacheEvent::Hit => Some(&self.hits),
                CacheEvent::Miss => Some(&self.misses),
                CacheEvent::Invalidation | CacheEvent::Eviction => None,
            };
            if let Some(counter) = counter {
                counter.fetch_add(count, Ordering::Relaxed);
            }
        }
    }

    impl Service<Frame> for CountingService {
        type Response = Frame;
        type Error = RedisError;
        type Future = BoxRedisFuture<Frame>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            let count = Arc::clone(&self.call_count);
            let response = self.response.clone();
            Box::pin(async move {
                *count.lock().unwrap() += 1;
                Ok(response)
            })
        }
    }

    impl ReleaseReadiness for CountingService {
        fn release_readiness(&mut self) {}
    }

    #[derive(Clone)]
    struct GatedReadService {
        get_calls: Arc<AtomicUsize>,
        first_get_started: Arc<tokio::sync::Semaphore>,
        release_first_get: Arc<tokio::sync::Semaphore>,
    }

    impl Service<Frame> for GatedReadService {
        type Response = Frame;
        type Error = RedisError;
        type Future = BoxRedisFuture<Frame>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Frame) -> Self::Future {
            if !is_command(&request, b"GET") {
                return Box::pin(async { Ok(Frame::SimpleString(Bytes::from_static(b"OK"))) });
            }

            let call_index = self.get_calls.fetch_add(1, Ordering::SeqCst);
            let first_get_started = Arc::clone(&self.first_get_started);
            let release_first_get = Arc::clone(&self.release_first_get);
            Box::pin(async move {
                if call_index == 0 {
                    first_get_started.add_permits(1);
                    release_first_get
                        .acquire_owned()
                        .await
                        .expect("first GET release semaphore must remain open")
                        .forget();
                    Ok(Frame::BulkString(Some(Bytes::from_static(b"stale"))))
                } else {
                    Ok(Frame::BulkString(Some(Bytes::from_static(b"fresh"))))
                }
            })
        }
    }

    impl ReleaseReadiness for GatedReadService {
        fn release_readiness(&mut self) {}
    }

    #[derive(Clone)]
    struct CancellableReadOnlyService {
        get_calls: Arc<AtomicUsize>,
        read_only_started: Arc<tokio::sync::Semaphore>,
    }

    impl Service<Frame> for CancellableReadOnlyService {
        type Response = Frame;
        type Error = RedisError;
        type Future = BoxRedisFuture<Frame>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Frame) -> Self::Future {
            if is_command(&request, b"GET") {
                self.get_calls.fetch_add(1, Ordering::SeqCst);
                return Box::pin(async {
                    Ok(Frame::BulkString(Some(Bytes::from_static(b"value"))))
                });
            }

            let read_only_started = Arc::clone(&self.read_only_started);
            Box::pin(async move {
                read_only_started.add_permits(1);
                std::future::pending::<Result<Frame, RedisError>>().await
            })
        }
    }

    impl ReleaseReadiness for CancellableReadOnlyService {
        fn release_readiness(&mut self) {}
    }

    #[derive(Default)]
    struct CapacityOneState {
        reserved: bool,
        calls: usize,
        releases: usize,
    }

    struct CapacityOneService {
        state: Arc<Mutex<CapacityOneState>>,
    }

    impl Service<Frame> for CapacityOneService {
        type Response = Frame;
        type Error = RedisError;
        type Future = BoxRedisFuture<Frame>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().unwrap();
            if state.reserved {
                Poll::Pending
            } else {
                state.reserved = true;
                Poll::Ready(Ok(()))
            }
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            let mut state = self.state.lock().unwrap();
            assert!(
                state.reserved,
                "call must consume capacity acquired by poll_ready"
            );
            state.reserved = false;
            state.calls += 1;
            Box::pin(async { Ok(Frame::BulkString(Some(Bytes::from_static(b"value")))) })
        }
    }

    impl ReleaseReadiness for CapacityOneService {
        fn release_readiness(&mut self) {
            let mut state = self.state.lock().unwrap();
            assert!(
                state.reserved,
                "a cache hit must follow successful poll_ready"
            );
            state.reserved = false;
            state.releases += 1;
        }
    }

    fn frame(parts: &[&str]) -> Frame {
        Frame::Array(Some(
            parts
                .iter()
                .map(|part| Frame::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
                .collect(),
        ))
    }

    fn is_command(request: &Frame, expected: &[u8]) -> bool {
        let Frame::Array(Some(parts)) = request else {
            return false;
        };
        let Some(Frame::BulkString(Some(command))) = parts.first() else {
            return false;
        };
        command.as_ref().eq_ignore_ascii_case(expected)
    }

    async fn wait_for_permit(semaphore: &Arc<tokio::sync::Semaphore>) {
        tokio::time::timeout(Duration::from_secs(1), semaphore.acquire())
            .await
            .expect("service future was not polled")
            .expect("service semaphore must remain open")
            .forget();
    }

    #[tokio::test]
    async fn cloned_service_shares_entries() {
        let calls = Arc::new(Mutex::new(0));
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"value"))),
        };
        let mut first = CacheService::new(inner, CacheConfig::default());
        let mut second = first.clone();

        first.call(frame(&["GET", "key"])).await.unwrap();
        second.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn unhealthy_data_connection_bypasses_an_existing_cache_hit() {
        let calls = Arc::new(Mutex::new(0));
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"stale"))),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        let (health_tx, health_rx) = tokio::sync::watch::channel(true);
        service.data_health = Some(DataConnectionHealth::new(health_rx));

        assert_eq!(
            service.call(frame(&["GET", "key"])).await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"stale")))
        );
        assert_eq!(service.cache_size().await, 1);

        service.inner.response = Frame::BulkString(Some(Bytes::from_static(b"fresh")));
        health_tx.send_replace(false);

        assert_eq!(
            service.call(frame(&["GET", "key"])).await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"fresh")))
        );
        assert_eq!(*calls.lock().unwrap(), 2);
        assert!(!service.is_caching_healthy().await);
    }

    #[tokio::test]
    async fn read_started_while_data_connection_is_unhealthy_is_not_cached() {
        let calls = Arc::new(Mutex::new(0));
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"value"))),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        let (_health_tx, health_rx) = tokio::sync::watch::channel(false);
        service.data_health = Some(DataConnectionHealth::new(health_rx));

        service.call(frame(&["GET", "key"])).await.unwrap();

        assert_eq!(service.cache_size().await, 0);
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn cache_hit_releases_capacity_one_readiness_reservation() {
        let state = Arc::new(Mutex::new(CapacityOneState::default()));
        let inner = CapacityOneService {
            state: Arc::clone(&state),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());

        std::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        service.call(frame(&["GET", "cached"])).await.unwrap();

        std::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        service.call(frame(&["GET", "cached"])).await.unwrap();

        tokio::time::timeout(
            Duration::from_millis(100),
            std::future::poll_fn(|cx| service.poll_ready(cx)),
        )
        .await
        .expect("cache hit must restore the only readiness permit")
        .unwrap();
        service.call(frame(&["GET", "other"])).await.unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.calls, 2);
        assert_eq!(state.releases, 1);
    }

    #[tokio::test]
    async fn managed_connection_state_is_rejected_and_releases_readiness() {
        let state = Arc::new(Mutex::new(CapacityOneState::default()));
        let inner = CapacityOneService {
            state: Arc::clone(&state),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());

        std::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let error = service
            .call(frame(&["CLIENT", "TRACKING", "OFF"]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("managed internally"));

        tokio::time::timeout(
            Duration::from_millis(100),
            std::future::poll_fn(|cx| service.poll_ready(cx)),
        )
        .await
        .expect("rejected command must return the readiness permit")
        .unwrap();
        service.call(frame(&["GET", "key"])).await.unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.calls, 1);
        assert_eq!(state.releases, 1);
    }

    #[tokio::test]
    async fn owned_invalidation_stream_can_emit_cache_metrics() {
        let calls = Arc::new(Mutex::new(0));
        let recorder = Arc::new(RecordingCacheMetrics::default());
        let metrics: Arc<dyn MetricsRecorder> = recorder.clone();
        let stream =
            futures::stream::pending::<Result<Frame, redis_tower_protocol::ProtocolError>>();
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"value"))),
        };
        let mut service = CacheService::with_invalidation_stream_and_recorder(
            inner,
            CacheConfig::default(),
            stream,
            metrics,
        );

        service.call(frame(&["GET", "key"])).await.unwrap();
        service.call(frame(&["GET", "key"])).await.unwrap();

        assert_eq!(recorder.misses.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.hits.load(Ordering::Relaxed), 1);
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn local_write_invalidation_rejects_racing_insert() {
        let calls = Arc::new(Mutex::new(0));
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"value"))),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        service.call(frame(&["GET", "key"])).await.unwrap();
        service.call(frame(&["SET", "key", "new"])).await.unwrap();
        assert_eq!(service.cache_size().await, 0);
    }

    #[tokio::test]
    async fn late_get_response_is_not_cached_after_local_write() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let first_get_started = Arc::new(tokio::sync::Semaphore::new(0));
        let release_first_get = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = GatedReadService {
            get_calls: Arc::clone(&get_calls),
            first_get_started: Arc::clone(&first_get_started),
            release_first_get: Arc::clone(&release_first_get),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        let mut reader = service.clone();

        let stale_read =
            tokio::spawn(async move { reader.call(frame(&["GET", "key"])).await.unwrap() });
        wait_for_permit(&first_get_started).await;

        service.call(frame(&["SET", "key", "new"])).await.unwrap();
        release_first_get.add_permits(1);
        assert_eq!(
            stale_read.await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"stale")))
        );
        assert_eq!(service.cache_size().await, 0);

        assert_eq!(
            service.call(frame(&["GET", "key"])).await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"fresh")))
        );
        service.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn read_started_while_disabled_is_not_cached_after_reenable() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let first_get_started = Arc::new(tokio::sync::Semaphore::new(0));
        let release_first_get = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = GatedReadService {
            get_calls: Arc::clone(&get_calls),
            first_get_started: Arc::clone(&first_get_started),
            release_first_get: Arc::clone(&release_first_get),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        service.cache.write().await.disable();
        let mut reader = service.clone();

        let untracked_read =
            tokio::spawn(async move { reader.call(frame(&["GET", "key"])).await.unwrap() });
        wait_for_permit(&first_get_started).await;

        service.cache.write().await.enable();
        release_first_get.add_permits(1);
        assert_eq!(
            untracked_read.await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"stale")))
        );
        assert_eq!(service.cache_size().await, 0);

        assert_eq!(
            service.call(frame(&["GET", "key"])).await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"fresh")))
        );
        service.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn coalesced_data_health_outage_rejects_an_old_read() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let first_get_started = Arc::new(tokio::sync::Semaphore::new(0));
        let release_first_get = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = GatedReadService {
            get_calls: Arc::clone(&get_calls),
            first_get_started: Arc::clone(&first_get_started),
            release_first_get: Arc::clone(&release_first_get),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        let (health_tx, health_rx) = tokio::sync::watch::channel(true);
        service.data_health = Some(DataConnectionHealth::new(health_rx));
        let mut reader = service.clone();

        let old_connection_read =
            tokio::spawn(async move { reader.call(frame(&["GET", "key"])).await.unwrap() });
        wait_for_permit(&first_get_started).await;

        health_tx.send_replace(false);
        health_tx.send_replace(true);
        release_first_get.add_permits(1);

        assert_eq!(
            old_connection_read.await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"stale")))
        );
        assert_eq!(service.cache_size().await, 0);
        assert!(!service.is_caching_healthy().await);

        service.cache.write().await.disable();
        assert!(
            service
                .data_health
                .as_ref()
                .expect("data-health gate")
                .begin_reconfigure()
        );
        assert!(
            service
                .data_health
                .as_ref()
                .expect("data-health gate")
                .mark_reconfigured()
        );
        service.cache.write().await.enable();

        assert_eq!(
            service.call(frame(&["GET", "key"])).await.unwrap(),
            Frame::BulkString(Some(Bytes::from_static(b"fresh")))
        );
        service.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn broadcast_prefixes_bypass_keys_redis_will_not_invalidate() {
        let calls = Arc::new(Mutex::new(0));
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"value"))),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        service.tracked_prefixes = Some(Arc::new(vec![Bytes::from_static(b"tracked:")]));

        service.call(frame(&["GET", "outside:key"])).await.unwrap();
        service.call(frame(&["GET", "outside:key"])).await.unwrap();

        assert_eq!(*calls.lock().unwrap(), 2);
        assert_eq!(service.cache_size().await, 0);
    }

    #[tokio::test]
    async fn dropping_unpolled_mutation_fails_cache_closed() {
        let calls = Arc::new(Mutex::new(0));
        let inner = CountingService {
            call_count: Arc::clone(&calls),
            response: Frame::BulkString(Some(Bytes::from_static(b"value"))),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());
        service.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(service.cache_size().await, 1);

        let mutation = service.call(frame(&["SET", "key", "new"]));
        drop(mutation);

        assert!(!service.is_caching_healthy().await);
        assert_eq!(service.cache_size().await, 0);
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "dropping the future must not keep polling the command"
        );
    }

    #[tokio::test]
    async fn cancelling_dispatched_read_only_commands_keeps_cache_healthy() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let read_only_started = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = CancellableReadOnlyService {
            get_calls: Arc::clone(&get_calls),
            read_only_started: Arc::clone(&read_only_started),
        };
        let mut service = CacheService::new(inner, CacheConfig::default());

        service.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(service.cache_size().await, 1);

        for request in [frame(&["PING"]), frame(&["TTL", "key"])] {
            let mut dispatched = service.clone();
            let task = tokio::spawn(async move { dispatched.call(request).await });
            wait_for_permit(&read_only_started).await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }

        assert_eq!(service.cache_size().await, 1);
        assert!(service.is_caching_healthy().await);
        service.call(frame(&["GET", "key"])).await.unwrap();
        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidation_task_disables_cache_when_stream_ends() {
        let cache = Arc::new(RwLock::new(CacheState::default()));
        let stream =
            futures::stream::iter(Vec::<Result<Frame, redis_tower_protocol::ProtocolError>>::new());
        spawn_invalidation_task(Arc::clone(&cache), stream)
            .await
            .unwrap();
        assert!(!cache.read().await.is_enabled());
    }

    #[test]
    fn cache_layer_is_a_real_tower_layer() {
        fn assert_layer<L: Layer<CountingService>>() {}
        assert_layer::<CacheLayer>();
    }
}
