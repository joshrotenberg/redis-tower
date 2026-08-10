//! Cluster-wide server-assisted client-side caching.
//!
//! A cached cluster client owns one shared [`CacheState`] above the router and
//! one RESP3 invalidation receiver per current master. Cache reads are enabled
//! only while every master has a healthy data connection, a live receiver, and
//! `CLIENT TRACKING ... REDIRECT` installed on that data connection. Any loss
//! closes the synchronous safety gate before recovery starts and clears the
//! shared cache.
//! Cluster caching requires a finite [`CachedClientConfig::client_ttl`] as a
//! freshness backstop for slot ownership changes that this client has not yet
//! observed.
//!
//! The module deliberately talks to [`MultiplexedClusterClient`] through the
//! crate-private [`ClusterCacheBackend`] seam. The router owns redirect wire
//! ordering and its private node services; this module owns cache races,
//! invalidation coverage, and lifecycle.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt, future::join_all};
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::cache_support::{
    command_may_mutate, extract_cache_entry, parse_invalidation, validate_cached_user_command,
};
use redis_tower::credentials::CredentialProvider;
use redis_tower::metrics_layer::MetricsRecorder;
use redis_tower::{
    CacheState, CacheStatistics, CacheTrackingMode, CachedClientConfig, PipelineExecutor,
    RedisExecutor,
};
use redis_tower_commands::{ClientId, ClientTracking};
use redis_tower_core::{Command, ConnectionConfig, Frame, ProtocolVersion, RedisError, RespLimits};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::time::Instant;

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
use redis_tower_core::tls::TlsConfig;

use crate::connection::{ClusterNodeConnector, ReadPreference};
use crate::multiplexed::{MultiplexedClusterClient, MultiplexedClusterClientBuilder};
use crate::slot::slot_for_key;
use crate::topology::ClusterTopology;
use crate::topology::changes::{ChangeContinuity, TopologyChange, TopologyRevision};

pub(crate) type CacheFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type TrackingStream = Pin<
    Box<dyn Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>> + Send + 'static>,
>;

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
const FINITE_CLIENT_TTL_REQUIRED: &str =
    "ERR cluster client-side caching requires a finite client_ttl";

fn validate_cluster_cache_config(config: &CachedClientConfig) -> Result<(), RedisError> {
    if config.client_ttl.is_none() {
        return Err(RedisError::Redis(FINITE_CLIENT_TTL_REQUIRED.to_string()));
    }
    Ok(())
}

/// Metadata the cache wrapper preserves when handing a raw frame to the
/// private cluster router.
///
/// The router applies the deadline to the complete redirect loop. Name,
/// idempotency, and blocking metadata preserve the original `Command` surface
/// for observation and any future retry policy; the current router does not
/// replay connection errors.
pub(crate) struct ClusterCacheRequest {
    pub(crate) frame: Frame,
    pub(crate) command_name: String,
    pub(crate) deadline: Option<Instant>,
    pub(crate) idempotent: bool,
    pub(crate) is_blocking: bool,
}

/// Wire prefix required for a routed cache request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheDispatchMode {
    /// Send only the user command.
    Plain,
    /// Atomically send `CLIENT CACHING YES` immediately before the command.
    /// ASK retries deliberately fall back to plain dispatch because Redis's
    /// one-shot `ASKING` and `CLIENT CACHING YES` flags consume one another.
    /// The cache safety gate is already closed before following an ASK, so the
    /// migrated response cannot populate the cache.
    OptIn,
}

/// One desired topology master and the resources needed to track it.
///
/// `data_health` is `None` when the topology names a master for which the
/// multiplexed client could not build a data service. Such a snapshot is
/// intentionally incomplete and cannot enable caching.
pub(crate) struct ClusterCacheMaster {
    pub(crate) addr: String,
    pub(crate) connector: ClusterNodeConnector,
    pub(crate) data_health: Option<watch::Receiver<bool>>,
}

/// An atomic snapshot of desired master coverage.
pub(crate) struct ClusterCacheNodeSnapshot {
    pub(crate) revision: TopologyRevision,
    pub(crate) masters: Vec<ClusterCacheMaster>,
}

impl ClusterCacheNodeSnapshot {
    fn master_addrs(&self) -> Vec<String> {
        let mut addrs = self
            .masters
            .iter()
            .map(|master| master.addr.clone())
            .collect::<Vec<_>>();
        addrs.sort();
        addrs.dedup();
        addrs
    }
}

/// Private operations the cache runtime needs from the cluster router.
///
/// This trait is implemented in `multiplexed.rs`, where private node services
/// and redirect machinery are available. All node-local pipelines must use a
/// reserved/atomic worker submission so connection-local setup cannot
/// interleave with user traffic.
pub(crate) trait ClusterCacheBackend: Clone + Send + Sync + 'static {
    /// Snapshot the topology revision, every desired master, its connector,
    /// and (when present) its data-service health receiver under one router
    /// read lock.
    fn cache_node_snapshot(&self) -> CacheFuture<'_, Result<ClusterCacheNodeSnapshot, RedisError>>;

    /// Register the synchronous fail-closed callbacks used immediately before
    /// committing a topology change and when the router observes node loss.
    fn install_cache_hooks(
        &self,
        hooks: ClusterCacheHooks,
    ) -> CacheFuture<'_, Result<(), RedisError>>;

    /// Execute one raw request through the normal redirect/retry loop.
    /// `OptIn` atomically prefixes the initial command. Any fail-closed
    /// redirect/retry attempt is downgraded to `Plain`; ASK uses its own
    /// atomic `[ASKING, command]` pipeline.
    fn execute_cache_request(
        &self,
        request: ClusterCacheRequest,
        mode: CacheDispatchMode,
    ) -> CacheFuture<'_, Result<Frame, RedisError>>;

    /// Send a reserved pipeline to one exact master, bypassing slot routing.
    /// Used for atomic `CLIENT TRACKING OFF` followed by `ON REDIRECT`.
    fn execute_cache_node_pipeline(
        &self,
        addr: &str,
        frames: Vec<Frame>,
    ) -> CacheFuture<'_, Result<Vec<Frame>, RedisError>>;

    /// Execute an explicit user pipeline through the ordinary cluster pipeline
    /// implementation. The cache wrapper clears before and after this call.
    fn execute_cache_pipeline(
        &self,
        frames: Vec<Frame>,
    ) -> CacheFuture<'_, Result<Vec<Frame>, RedisError>>;
}

/// Builder for a [`CachedMultiplexedClusterClient`].
///
/// It wraps the ordinary multiplexed builder, forces RESP3 after every
/// `connection_config` update, and rejects replica read preferences because
/// the first cluster-cache implementation installs invalidation coverage only
/// on masters.
pub struct CachedMultiplexedClusterClientBuilder {
    inner: MultiplexedClusterClientBuilder,
    cache_config: CachedClientConfig,
    unsupported_read_preference: Option<ReadPreference>,
}

impl CachedMultiplexedClusterClientBuilder {
    pub(crate) fn new(seed_addr: impl Into<String>) -> Self {
        Self {
            inner: MultiplexedClusterClient::builder(seed_addr).protocol(ProtocolVersion::Resp3),
            cache_config: CachedClientConfig::default(),
            unsupported_read_preference: None,
        }
    }

    /// Configure local cache bounds, freshness, tracking mode, and metrics.
    ///
    /// Unlike standalone cached clients, Cluster requires a finite
    /// [`CachedClientConfig::client_ttl`]. [`Self::connect`] rejects `None`
    /// before opening the seed connection.
    pub fn cache_config(mut self, config: CachedClientConfig) -> Self {
        self.cache_config = config;
        self
    }

    /// Set the host override for Docker/proxy environments.
    pub fn host_override(mut self, host: impl Into<String>) -> Self {
        self.inner = self.inner.host_override(host);
        self
    }

    /// Map internal cluster addresses to externally reachable addresses.
    pub fn address_map(mut self, map: std::collections::HashMap<String, String>) -> Self {
        self.inner = self.inner.address_map(map);
        self
    }

    /// Select a read preference.
    ///
    /// The initial cached cluster implementation supports masters only. A
    /// non-master value is remembered and returned as an error by `connect`,
    /// preserving the usual fluent builder shape without silently changing the
    /// requested routing policy.
    pub fn read_preference(mut self, preference: ReadPreference) -> Self {
        self.unsupported_read_preference =
            (preference != ReadPreference::Master).then_some(preference);
        self.inner = self.inner.read_preference(preference);
        self
    }

    /// Set the maximum number of redirects/retries followed per command.
    pub fn max_redirects(mut self, max: usize) -> Self {
        self.inner = self.inner.max_redirects(max);
        self
    }

    /// Configure each master's auto-pipeline worker.
    pub fn pipeline_config(mut self, config: AutoPipelineConfig) -> Self {
        self.inner = self.inner.pipeline_config(config);
        self
    }

    /// Record cluster command, redirect, refresh, and worker metrics.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.inner = self.inner.metrics_recorder(recorder);
        self
    }

    /// Include bounded master-address labels in cluster metrics.
    pub fn include_node_in_metrics(mut self, include: bool) -> Self {
        self.inner = self.inner.include_node_in_metrics(include);
        self
    }

    /// Configure per-master worker reconnect behavior.
    pub fn reconnect_config(mut self, config: AutoPipelineReconnectConfig) -> Self {
        self.inner = self.inner.reconnect_config(config);
        self
    }

    /// Authenticate seed, data, reconnect, and invalidation connections.
    pub fn credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.inner = self.inner.credentials(provider);
        self
    }

    /// Configure transport, timeouts, and decode limits for every connection.
    /// RESP3 is forced regardless of the supplied protocol preference.
    pub fn connection_config(mut self, config: ConnectionConfig) -> Self {
        self.inner = self
            .inner
            .connection_config(config.with_protocol(ProtocolVersion::Resp3));
        self
    }

    /// Configure RESP decode limits for all cluster connections.
    pub fn resp_limits(mut self, limits: RespLimits) -> Self {
        self.inner = self
            .inner
            .resp_limits(limits)
            .protocol(ProtocolVersion::Resp3);
        self
    }

    /// Enable TLS for seed, data, reconnect, and invalidation connections.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.inner = self.inner.tls(tls);
        self
    }

    /// Connect every data node, install per-master invalidation coverage, and
    /// enable the shared cache only after coverage is complete.
    pub async fn connect(self) -> Result<CachedMultiplexedClusterClient, RedisError> {
        validate_cluster_cache_config(&self.cache_config)?;

        if let Some(preference) = self.unsupported_read_preference {
            return Err(RedisError::Redis(format!(
                "ERR cluster client-side caching requires ReadPreference::Master; got {preference:?}"
            )));
        }

        let client = self
            .inner
            .protocol(ProtocolVersion::Resp3)
            .connect()
            .await?;
        match CachedMultiplexedClusterClient::start(client.clone(), self.cache_config).await {
            Ok(cached) => Ok(cached),
            Err(error) => {
                client.shutdown().await;
                Err(error)
            }
        }
    }
}

/// Cloneable, high-concurrency Redis Cluster client with one coherent local
/// cache and automatic per-master invalidation coverage.
#[derive(Clone)]
pub struct CachedMultiplexedClusterClient {
    inner: MultiplexedClusterClient,
    cache: Arc<RwLock<CacheState>>,
    policy: Arc<CachePolicy>,
    mutations_in_flight: Arc<AtomicUsize>,
    gate: Arc<CacheSafetyGate>,
    control: CacheControl,
    lifecycle: CacheLifecycle,
}

struct CachePolicy {
    tracking_mode: CacheTrackingMode,
    coverage_health: Arc<CoverageHealth>,
}

/// Unconsumed copies of every current master's data-worker health receiver.
///
/// `NodeMonitor` owns a separate receiver clone for asynchronous recovery, but
/// cache hits and fills consult these copies synchronously. Leaving their
/// versions unconsumed makes even a coalesced false -> true reconnect visible:
/// the replacement connection has not had CLIENT TRACKING reinstalled yet.
#[derive(Default)]
struct CoverageHealth {
    receivers: StdRwLock<Vec<watch::Receiver<bool>>>,
    #[cfg(test)]
    test_senders: StdMutex<Vec<watch::Sender<bool>>>,
}

impl CoverageHealth {
    fn install(&self, receivers: Vec<watch::Receiver<bool>>) {
        *self
            .receivers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = receivers;
    }

    fn is_usable(&self) -> bool {
        let receivers = self
            .receivers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !receivers.is_empty()
            && receivers.iter().all(|health| {
                !health.has_changed().unwrap_or(true)
                    && *health.borrow()
                    && !health.has_changed().unwrap_or(true)
            })
    }

    fn ensure_usable(&self, control: &CacheControl) -> bool {
        if self.is_usable() {
            true
        } else {
            control.node_lost_if_open(None);
            false
        }
    }

    #[cfg(test)]
    fn assumed_healthy() -> Arc<Self> {
        let health = Arc::new(Self::default());
        let (sender, receiver) = watch::channel(true);
        health.install(vec![receiver]);
        health.test_senders.lock().unwrap().push(sender);
        health
    }
}

impl CachedMultiplexedClusterClient {
    /// Create a cached cluster builder with safe bounded cache defaults.
    pub fn builder(seed_addr: impl Into<String>) -> CachedMultiplexedClusterClientBuilder {
        CachedMultiplexedClusterClientBuilder::new(seed_addr)
    }

    /// Connect using safe cache defaults.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        Self::builder(seed_addr).connect().await
    }

    /// Connect with explicit cache configuration.
    ///
    /// Cluster caching rejects a configuration whose `client_ttl` is `None`;
    /// a finite TTL bounds stale entries after unobserved slot-owner changes.
    pub async fn connect_with_config(
        seed_addr: &str,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        Self::builder(seed_addr)
            .cache_config(config)
            .connect()
            .await
    }

    async fn start(
        inner: MultiplexedClusterClient,
        config: CachedClientConfig,
    ) -> Result<Self, RedisError> {
        // Keep this validation at the runtime boundary as well as the public
        // builder so future internal constructors cannot bypass the invariant.
        validate_cluster_cache_config(&config)?;

        if inner.read_preference().await != ReadPreference::Master {
            return Err(RedisError::Redis(
                "ERR cluster client-side caching requires ReadPreference::Master".to_string(),
            ));
        }

        let cache = Arc::new(RwLock::new(config.new_state()));
        cache.write().await.suspend();
        let gate = Arc::new(CacheSafetyGate::new_closed());
        let mutations_in_flight = Arc::new(AtomicUsize::new(0));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let control = CacheControl {
            gate: Arc::clone(&gate),
            tx: control_tx,
            node_loss_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        inner
            .install_cache_hooks(ClusterCacheHooks {
                control: control.clone(),
            })
            .await?;

        let expected_gate_generation = gate.generation();
        let snapshot = inner.cache_node_snapshot().await?;
        let observed_revision = snapshot.revision;
        let candidate = prepare_coverage(&inner, snapshot, &config.tracking_mode).await?;
        let verification = inner.cache_node_snapshot().await?;
        if !candidate.matches(&verification) {
            return Err(RedisError::ConnectionClosed);
        }

        let coverage_health = Arc::new(CoverageHealth::default());
        let coverage = candidate.spawn(Arc::clone(&cache), control.clone(), &coverage_health);
        if !coverage_health.is_usable() || !coverage.monitors_running() {
            shutdown_coverage_fail_closed(&cache, coverage).await;
            return Err(RedisError::ConnectionClosed);
        }
        cache.write().await.resume();
        if !gate.open_if_generation(expected_gate_generation) {
            shutdown_coverage_fail_closed(&cache, coverage).await;
            return Err(RedisError::ConnectionClosed);
        }
        if !coverage_health.ensure_usable(&control) || !coverage.monitors_running() {
            control.node_lost(None);
            shutdown_coverage_fail_closed(&cache, coverage).await;
            return Err(RedisError::ConnectionClosed);
        }

        let policy = Arc::new(CachePolicy {
            tracking_mode: config.tracking_mode,
            coverage_health,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_cache_supervisor(
            inner.clone(),
            Arc::clone(&cache),
            Arc::clone(&gate),
            Arc::clone(&mutations_in_flight),
            Arc::clone(&policy),
            control.clone(),
            control_rx,
            shutdown_rx,
            coverage,
            observed_revision,
            expected_gate_generation,
        ));
        let lifecycle = CacheLifecycle::new(shutdown_tx, task);

        Ok(Self {
            inner,
            cache,
            policy,
            mutations_in_flight,
            gate,
            control,
            lifecycle,
        })
    }

    /// Execute a typed Redis command through the shared cluster cache.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            return Err(RedisError::CommandTimeout);
        }
        let request = ClusterCacheRequest {
            frame: cmd.to_frame(),
            command_name: cmd.name().to_string(),
            deadline,
            idempotent: cmd.idempotent(),
            is_blocking: cmd.is_blocking(),
        };
        let frame = execute_cached_request(
            &self.inner,
            &self.cache,
            &self.policy,
            &self.mutations_in_flight,
            &self.gate,
            &self.control,
            request,
        )
        .await?;
        cmd.parse_response(frame)
    }

    /// Refresh topology and per-master services. The router's topology hook
    /// closes cache use synchronously; the supervisor re-enables it only after
    /// the new master set has complete invalidation coverage.
    pub async fn refresh_topology(&self) -> Result<(), RedisError> {
        self.inner.refresh_topology().await
    }

    /// Get a snapshot of the current cluster topology.
    pub async fn topology(&self) -> ClusterTopology {
        self.inner.topology().await
    }

    /// Number of entries in the one shared cluster cache.
    pub async fn cache_size(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Clear every local cache entry.
    pub async fn clear_cache(&self) {
        self.cache.write().await.clear();
    }

    /// Return aggregate cluster-cache hit/miss/invalidation/eviction counters.
    pub async fn cache_statistics(&self) -> CacheStatistics {
        self.cache.read().await.statistics()
    }

    /// Whether every current master is covered, no mutation is in flight, and
    /// cache reads/fills are enabled.
    pub async fn is_caching_healthy(&self) -> bool {
        self.gate.is_open()
            && self.policy.coverage_health.ensure_usable(&self.control)
            && self.mutations_in_flight.load(Ordering::Acquire) == 0
            && self.lifecycle.is_running()
            && self.cache.read().await.is_enabled()
    }

    /// Stop invalidation receivers before draining the final cluster client.
    /// Earlier clones return immediately and leave their shared lifecycle live.
    pub async fn shutdown(self) {
        let Self {
            inner, lifecycle, ..
        } = self;
        if lifecycle.shutdown().await {
            inner.shutdown().await;
        }
    }
}

impl RedisExecutor for CachedMultiplexedClusterClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        let client = self.clone();
        async move { client.execute(cmd).await }
    }
}

impl PipelineExecutor for CachedMultiplexedClusterClient {
    fn execute_pipeline(
        &mut self,
        frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Vec<Frame>, RedisError>> + Send {
        let client = self.clone();
        async move {
            execute_cached_pipeline(
                &client.inner,
                &client.cache,
                &client.mutations_in_flight,
                &client.control,
                frames,
            )
            .await
        }
    }
}

async fn execute_cached_request<B: ClusterCacheBackend>(
    backend: &B,
    cache: &Arc<RwLock<CacheState>>,
    policy: &CachePolicy,
    mutations_in_flight: &Arc<AtomicUsize>,
    gate: &Arc<CacheSafetyGate>,
    control: &CacheControl,
    request: ClusterCacheRequest,
) -> Result<Frame, RedisError> {
    validate_cached_user_command(&request.frame)?;

    let extracted = extract_cache_entry(&request.frame);
    let entry = extracted
        .clone()
        .filter(|(_, redis_key)| key_matches_tracking_mode(&policy.tracking_mode, redis_key));

    // A cacheable read outside configured BCAST prefixes is a plain
    // passthrough. Treating it as a mutation would unnecessarily clear valid
    // in-prefix entries, while caching it would be unsafe because Redis will
    // never invalidate it.
    if extracted.is_some() && entry.is_none() {
        return dispatch_and_fail_closed_on_loss(
            backend,
            control,
            request,
            CacheDispatchMode::Plain,
        )
        .await;
    }

    let partition = entry.as_ref().map(|(_, redis_key)| slot_for_key(redis_key));
    let mut epoch = None;
    if mutations_in_flight.load(Ordering::Acquire) == 0
        && gate.is_open()
        && policy.coverage_health.ensure_usable(control)
        && let Some(((cache_key, redis_key), partition)) = entry.as_ref().zip(partition)
        && let Ok(state) = cache.try_read()
        // Linearize safety after acquiring cache state. A concurrent failure
        // closes the atomic gate before its asynchronous state cleanup.
        && gate.is_open()
        && policy.coverage_health.ensure_usable(control)
    {
        if let Some(response) = state.get_in_partition(cache_key, partition) {
            let response = response.clone();
            if gate.is_open() && policy.coverage_health.ensure_usable(control) {
                return Ok(response);
            }
        }
        if gate.is_open() && policy.coverage_health.ensure_usable(control) {
            epoch = state.snapshot_epoch_in_partition(redis_key, partition);
        }
    }

    if entry.is_none() && !command_may_mutate(&request.frame) {
        return dispatch_and_fail_closed_on_loss(
            backend,
            control,
            request,
            CacheDispatchMode::Plain,
        )
        .await;
    }

    if entry.is_none() {
        let request_frame = request.frame.clone();
        let guard = CacheMutationGuard::new(
            Arc::clone(cache),
            Arc::clone(mutations_in_flight),
            control.clone(),
        );
        if let Ok(mut state) = cache.try_write() {
            state.invalidate_for_command(&request_frame);
        }
        let result =
            dispatch_and_fail_closed_on_loss(backend, control, request, CacheDispatchMode::Plain)
                .await;
        cache.write().await.invalidate_for_command(&request_frame);
        guard.finish();
        return result;
    }

    let mode = if policy.tracking_mode.is_opt_in() && epoch.is_some() {
        CacheDispatchMode::OptIn
    } else {
        CacheDispatchMode::Plain
    };
    let result = dispatch_and_fail_closed_on_loss(backend, control, request, mode).await;
    if let (Some((cache_key, redis_key)), Some(partition), Some(epoch), Ok(response)) =
        (entry, partition, epoch, &result)
        && !matches!(response, Frame::Error(_))
        && gate.is_open()
        && policy.coverage_health.ensure_usable(control)
        && mutations_in_flight.load(Ordering::Acquire) == 0
    {
        let mut state = cache.write().await;
        if gate.is_open()
            && policy.coverage_health.ensure_usable(control)
            && mutations_in_flight.load(Ordering::Acquire) == 0
        {
            state.insert_if_current_in_partition(
                cache_key,
                redis_key,
                response.clone(),
                partition,
                epoch,
            );
        }
    }
    result
}

async fn dispatch_and_fail_closed_on_loss<B: ClusterCacheBackend>(
    backend: &B,
    control: &CacheControl,
    request: ClusterCacheRequest,
    mode: CacheDispatchMode,
) -> Result<Frame, RedisError> {
    let result = backend.execute_cache_request(request, mode).await;
    if result
        .as_ref()
        .err()
        .is_some_and(cache_dispatch_requires_recovery)
    {
        control.node_lost(None);
    }
    result
}

fn cache_dispatch_requires_recovery(error: &RedisError) -> bool {
    error.is_connection_error() || matches!(error, RedisError::CommandTimeout)
}

fn key_matches_tracking_mode(mode: &CacheTrackingMode, redis_key: &[u8]) -> bool {
    match mode {
        CacheTrackingMode::Broadcast { prefixes } => {
            prefixes.is_empty() || prefixes.iter().any(|prefix| redis_key.starts_with(prefix))
        }
        CacheTrackingMode::ServerDefault | CacheTrackingMode::OptIn => true,
    }
}

async fn execute_cached_pipeline<B: ClusterCacheBackend>(
    backend: &B,
    cache: &Arc<RwLock<CacheState>>,
    mutations_in_flight: &Arc<AtomicUsize>,
    control: &CacheControl,
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, RedisError> {
    for frame in &frames {
        validate_cached_user_command(frame)?;
    }
    let guard = CacheMutationGuard::new(
        Arc::clone(cache),
        Arc::clone(mutations_in_flight),
        control.clone(),
    );
    cache.write().await.clear();
    let result = backend.execute_cache_pipeline(frames).await;
    cache.write().await.clear();
    if result
        .as_ref()
        .err()
        .is_some_and(cache_dispatch_requires_recovery)
    {
        // Close the gate before releasing the mutation count. Otherwise a
        // concurrent read could begin in the gap after an ambiguous pipeline
        // failure but before recovery is requested.
        control.node_lost(None);
    }
    guard.finish();
    result
}

/// Single atomic word combining a generation and an open bit.
///
/// `close` increments the generation and clears the bit. Recovery can only
/// reopen with a compare-and-swap against the generation captured before it
/// began, so a concurrent close can never be overwritten by stale recovery.
struct CacheSafetyGate {
    state: AtomicU64,
}

impl CacheSafetyGate {
    fn new_closed() -> Self {
        Self {
            state: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn new_open() -> Self {
        Self {
            state: AtomicU64::new(1),
        }
    }

    fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) & 1 == 1
    }

    fn generation(&self) -> u64 {
        self.state.load(Ordering::Acquire) >> 1
    }

    fn close(&self) -> u64 {
        let previous = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let generation = (state >> 1)
                    .checked_add(1)
                    .expect("cluster cache safety generation overflowed");
                Some(generation << 1)
            })
            .expect("cache safety close update is infallible");
        (previous >> 1) + 1
    }

    fn open_if_generation(&self, generation: u64) -> bool {
        self.state
            .compare_exchange(
                generation << 1,
                (generation << 1) | 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

#[derive(Clone)]
struct CacheControl {
    gate: Arc<CacheSafetyGate>,
    tx: mpsc::UnboundedSender<CacheControlEvent>,
    node_loss_pending: Arc<std::sync::atomic::AtomicBool>,
}

impl CacheControl {
    fn topology_changed(&self, change: Arc<TopologyChange>) {
        let generation = self.gate.close();
        let _ = self
            .tx
            .send(CacheControlEvent::TopologyChanged { generation, change });
    }

    fn node_lost(&self, addr: Option<String>) {
        // One loss is enough to keep the global gate closed through a complete
        // coverage rebuild. Coalesce outage traffic until recovery explicitly
        // rearms this signal immediately before its final validation/open CAS.
        if self.node_loss_pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let generation = self.gate.close();
        let _ = self
            .tx
            .send(CacheControlEvent::NodeLost { generation, addr });
    }

    fn node_lost_if_open(&self, addr: Option<String>) {
        if self.gate.is_open() {
            self.node_lost(addr);
        }
    }

    fn rearm_node_loss(&self) {
        self.node_loss_pending.store(false, Ordering::Release);
    }

    fn mutation_cancelled(&self) {
        let generation = self.gate.close();
        let _ = self
            .tx
            .send(CacheControlEvent::MutationCancelled { generation });
    }

    fn mutation_finished(&self) {
        if !self.gate.is_open() {
            let _ = self.tx.send(CacheControlEvent::MutationFinished);
        }
    }
}

/// Synchronous hooks stored by the cluster router.
#[derive(Clone)]
pub(crate) struct ClusterCacheHooks {
    control: CacheControl,
}

impl ClusterCacheHooks {
    /// Call immediately before committing a full refresh or MOVED slot patch.
    pub(crate) fn topology_changing(&self, change: Arc<TopologyChange>) {
        self.control.topology_changed(change);
    }

    /// Call as soon as router/node code observes a data connection failure.
    pub(crate) fn node_connection_lost(&self, addr: impl Into<String>) {
        self.control.node_lost(Some(addr.into()));
    }
}

enum CacheControlEvent {
    TopologyChanged {
        generation: u64,
        change: Arc<TopologyChange>,
    },
    NodeLost {
        generation: u64,
        addr: Option<String>,
    },
    MutationCancelled {
        generation: u64,
    },
    MutationFinished,
}

impl CacheControlEvent {
    fn gate_generation(&self) -> Option<u64> {
        match self {
            Self::TopologyChanged { generation, .. }
            | Self::NodeLost { generation, .. }
            | Self::MutationCancelled { generation } => Some(*generation),
            Self::MutationFinished => None,
        }
    }
}

struct CacheMutationGuard {
    cache: Arc<RwLock<CacheState>>,
    mutations_in_flight: Arc<AtomicUsize>,
    control: CacheControl,
    armed: bool,
}

impl CacheMutationGuard {
    fn new(
        cache: Arc<RwLock<CacheState>>,
        mutations_in_flight: Arc<AtomicUsize>,
        control: CacheControl,
    ) -> Self {
        mutations_in_flight.fetch_add(1, Ordering::AcqRel);
        Self {
            cache,
            mutations_in_flight,
            control,
            armed: true,
        }
    }

    fn finish(mut self) {
        self.armed = false;
        self.mutations_in_flight.fetch_sub(1, Ordering::AcqRel);
        self.control.mutation_finished();
    }
}

impl Drop for CacheMutationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        // Close synchronously before attempting the asynchronous state write.
        self.control.mutation_cancelled();
        if let Ok(mut cache) = self.cache.try_write() {
            cache.disable();
        } else if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let cache = Arc::clone(&self.cache);
            runtime.spawn(async move {
                cache.write().await.disable();
            });
        }
        // The supervisor decrements this guard's in-flight count only after a
        // fresh receiver/configuration barrier. Until then every cache hit is
        // blocked even if asynchronous state cleanup has not acquired its lock.
    }
}

struct PreparedNode {
    addr: String,
    health: watch::Receiver<bool>,
    stream: TrackingStream,
}

struct CoverageCandidate {
    revision: TopologyRevision,
    master_addrs: Vec<String>,
    nodes: Vec<PreparedNode>,
}

impl CoverageCandidate {
    fn matches(&self, snapshot: &ClusterCacheNodeSnapshot) -> bool {
        self.revision == snapshot.revision && self.master_addrs == snapshot.master_addrs()
    }

    fn spawn(
        self,
        cache: Arc<RwLock<CacheState>>,
        control: CacheControl,
        coverage_health: &CoverageHealth,
    ) -> Coverage {
        coverage_health.install(self.nodes.iter().map(|node| node.health.clone()).collect());
        let monitors = self
            .nodes
            .into_iter()
            .map(|node| NodeMonitor::spawn(node, Arc::clone(&cache), control.clone()))
            .collect();
        Coverage { monitors }
    }
}

struct Coverage {
    monitors: Vec<NodeMonitor>,
}

impl Coverage {
    fn monitors_running(&self) -> bool {
        !self.monitors.is_empty()
            && self
                .monitors
                .iter()
                .all(|monitor| !monitor.task.is_finished())
    }

    async fn shutdown(self) {
        for monitor in &self.monitors {
            monitor.stop.send_replace(true);
        }
        for monitor in self.monitors {
            let _ = monitor.task.await;
        }
    }
}

/// Disable and clear shared state before stopping receiver coverage.
///
/// Every current recovery path rebuilds tracking on every master with
/// `CLIENT TRACKING OFF` followed by `ON`. Entries cannot survive that global
/// receiver gap: an external write made after the old monitors stop and before
/// the replacements are armed would otherwise have no invalidation path.
async fn shutdown_coverage_fail_closed(cache: &Arc<RwLock<CacheState>>, coverage: Coverage) {
    cache.write().await.disable();
    coverage.shutdown().await;
}

struct NodeMonitor {
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl NodeMonitor {
    fn spawn(node: PreparedNode, cache: Arc<RwLock<CacheState>>, control: CacheControl) -> Self {
        let (stop, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let PreparedNode {
                addr,
                mut health,
                mut stream,
            } = node;
            loop {
                tokio::select! {
                    biased;
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return;
                        }
                    }
                    changed = health.changed() => {
                        // A watch version change is a loss even when false ->
                        // true was coalesced. The new data connection has not
                        // had this receiver ID installed yet.
                        let _ = changed;
                        control.node_lost(Some(addr.clone()));
                        return;
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
                        Some(Err(_)) | None => {
                            control.node_lost(Some(addr.clone()));
                            return;
                        }
                    },
                }
            }
        });
        Self { stop, task }
    }
}

async fn prepare_coverage<B: ClusterCacheBackend>(
    backend: &B,
    snapshot: ClusterCacheNodeSnapshot,
    mode: &CacheTrackingMode,
) -> Result<CoverageCandidate, RedisError> {
    if snapshot.masters.is_empty() {
        return Err(RedisError::Redis(
            "ERR cluster client-side caching requires at least one master".to_string(),
        ));
    }
    let master_addrs = snapshot.master_addrs();
    if master_addrs.len() != snapshot.masters.len() {
        return Err(RedisError::Redis(
            "ERR duplicate master in cluster cache coverage snapshot".to_string(),
        ));
    }

    let prepared = join_all(
        snapshot
            .masters
            .into_iter()
            .map(|master| prepare_node(backend, master, mode)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    Ok(CoverageCandidate {
        revision: snapshot.revision,
        master_addrs,
        nodes: prepared,
    })
}

async fn prepare_node<B: ClusterCacheBackend>(
    backend: &B,
    master: ClusterCacheMaster,
    mode: &CacheTrackingMode,
) -> Result<PreparedNode, RedisError> {
    let ClusterCacheMaster {
        addr,
        connector,
        data_health,
    } = master;
    let mut health = data_health.ok_or_else(|| {
        RedisError::Redis(format!(
            "ERR no connected data service for cluster cache master {addr}"
        ))
    })?;
    if !*health.borrow() {
        return Err(RedisError::ConnectionClosed);
    }
    let _ = *health.borrow_and_update();

    let (receiver_id, stream) = connect_tracking_receiver(&connector, &addr).await?;
    if health.has_changed().unwrap_or(true) || !*health.borrow() {
        return Err(RedisError::ConnectionClosed);
    }

    let off = ClientTracking::off();
    let on = mode.tracking_command(receiver_id);
    let responses = backend
        .execute_cache_node_pipeline(&addr, vec![off.to_frame(), on.to_frame()])
        .await?;
    if responses.len() != 2 {
        return Err(RedisError::UnexpectedResponse {
            expected: "CLIENT TRACKING OFF and ON responses",
            actual: format!("{} responses", responses.len()),
        });
    }
    let mut responses = responses.into_iter();
    off.parse_response(responses.next().ok_or(RedisError::ConnectionClosed)?)?;
    on.parse_response(responses.next().ok_or(RedisError::ConnectionClosed)?)?;

    if health.has_changed().unwrap_or(true) || !*health.borrow() {
        return Err(RedisError::ConnectionClosed);
    }
    Ok(PreparedNode {
        addr,
        health,
        stream,
    })
}

async fn connect_tracking_receiver(
    connector: &ClusterNodeConnector,
    addr: &str,
) -> Result<(i64, TrackingStream), RedisError> {
    let mut connection = connector.connect(addr, false).await?;
    if !connection.is_resp3() {
        connection
            .negotiate_protocol(ProtocolVersion::Resp3)
            .await?;
    }
    let receiver_id = connection.execute(ClientId::new()).await?;
    let framed = connection.into_framed()?;
    let (_sink, stream) = framed.split();
    Ok((receiver_id, Box::pin(stream)))
}

fn queue_control_event(pending: &mut BTreeMap<u64, CacheControlEvent>, event: CacheControlEvent) {
    if let Some(generation) = event.gate_generation() {
        let replaced = pending.insert(generation, event);
        debug_assert!(replaced.is_none(), "gate generations are unique");
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_control_events_through_current_generation(
    gate: &CacheSafetyGate,
    events: &mut mpsc::UnboundedReceiver<CacheControlEvent>,
    pending: &mut BTreeMap<u64, CacheControlEvent>,
    applied_generation: &mut u64,
    shutdown: &mut watch::Receiver<bool>,
    cache: &Arc<RwLock<CacheState>>,
    observed_revision: &mut TopologyRevision,
    pending_cancelled_mutations: &mut usize,
) -> bool {
    loop {
        let next_generation = applied_generation
            .checked_add(1)
            .expect("cluster cache applied generation overflowed");
        if let Some(event) = pending.remove(&next_generation) {
            apply_control_event(event, cache, observed_revision, pending_cancelled_mutations).await;
            *applied_generation = next_generation;
            continue;
        }

        let current_generation = gate.generation();
        if *applied_generation == current_generation {
            return true;
        }
        debug_assert!(*applied_generation < current_generation);

        let event = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return false;
                }
                continue;
            }
            event = events.recv() => event,
        };
        let Some(event) = event else {
            return false;
        };
        queue_control_event(pending, event);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_cache_supervisor<B: ClusterCacheBackend>(
    backend: B,
    cache: Arc<RwLock<CacheState>>,
    gate: Arc<CacheSafetyGate>,
    mutations_in_flight: Arc<AtomicUsize>,
    policy: Arc<CachePolicy>,
    control: CacheControl,
    mut events: mpsc::UnboundedReceiver<CacheControlEvent>,
    mut shutdown: watch::Receiver<bool>,
    mut coverage: Coverage,
    mut observed_revision: TopologyRevision,
    mut applied_gate_generation: u64,
) {
    let running = SupervisorRunningGuard {
        gate: Arc::clone(&gate),
        cache: Arc::clone(&cache),
    };
    let mut pending_cancelled_mutations = 0usize;
    let mut pending_control_events = BTreeMap::new();
    let mut reconnect_backoff = INITIAL_RECONNECT_BACKOFF;

    loop {
        let event = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    shutdown_coverage_fail_closed(&cache, coverage).await;
                    drop(running);
                    return;
                }
                continue;
            }
            event = events.recv() => match event {
                Some(event) => event,
                None => {
                    shutdown_coverage_fail_closed(&cache, coverage).await;
                    drop(running);
                    return;
                }
            },
        };

        if matches!(event, CacheControlEvent::MutationFinished) && gate.is_open() {
            continue;
        }
        queue_control_event(&mut pending_control_events, event);
        shutdown_coverage_fail_closed(&cache, coverage).await;
        if !apply_control_events_through_current_generation(
            &gate,
            &mut events,
            &mut pending_control_events,
            &mut applied_gate_generation,
            &mut shutdown,
            &cache,
            &mut observed_revision,
            &mut pending_cancelled_mutations,
        )
        .await
        {
            drop(running);
            return;
        }

        'recover: loop {
            if *shutdown.borrow() {
                drop(running);
                return;
            }
            if !apply_control_events_through_current_generation(
                &gate,
                &mut events,
                &mut pending_control_events,
                &mut applied_gate_generation,
                &mut shutdown,
                &cache,
                &mut observed_revision,
                &mut pending_cancelled_mutations,
            )
            .await
            {
                drop(running);
                return;
            }
            let expected_gate_generation = gate.generation();
            debug_assert_eq!(expected_gate_generation, applied_gate_generation);
            let snapshot_result = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        drop(running);
                        return;
                    }
                    continue;
                }
                snapshot = backend.cache_node_snapshot() => snapshot,
            };
            let snapshot = match snapshot_result {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(%error, "failed to snapshot cluster cache coverage");
                    wait_before_retry(
                        reconnect_backoff,
                        &mut shutdown,
                        &mut events,
                        &mut pending_control_events,
                    )
                    .await;
                    reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                    continue;
                }
            };
            if snapshot.revision != observed_revision {
                // Missing the detailed event means targeted invalidation is no
                // longer provably sufficient. Clear and adopt the snapshot.
                cache.write().await.disable();
                observed_revision = snapshot.revision;
            }

            let candidate_result = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        drop(running);
                        return;
                    }
                    continue;
                }
                candidate = prepare_coverage(&backend, snapshot, &policy.tracking_mode) => candidate,
            };
            let candidate = match candidate_result {
                Ok(candidate) => candidate,
                Err(error) => {
                    tracing::warn!(%error, "failed to restore cluster cache coverage");
                    cache.write().await.disable();
                    wait_before_retry(
                        reconnect_backoff,
                        &mut shutdown,
                        &mut events,
                        &mut pending_control_events,
                    )
                    .await;
                    reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                    continue;
                }
            };
            let verification_result = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        drop(running);
                        return;
                    }
                    continue;
                }
                snapshot = backend.cache_node_snapshot() => snapshot,
            };
            let verification = match verification_result {
                Ok(snapshot) => snapshot,
                Err(_) => continue,
            };
            if !candidate.matches(&verification) || gate.generation() != expected_gate_generation {
                continue;
            }

            let candidate_revision = candidate.revision;
            let next_coverage =
                candidate.spawn(Arc::clone(&cache), control.clone(), &policy.coverage_health);
            if !policy.coverage_health.is_usable() || !next_coverage.monitors_running() {
                shutdown_coverage_fail_closed(&cache, next_coverage).await;
                continue;
            }

            if pending_cancelled_mutations > 0 {
                let previous =
                    mutations_in_flight.fetch_sub(pending_cancelled_mutations, Ordering::AcqRel);
                debug_assert!(previous >= pending_cancelled_mutations);
                pending_cancelled_mutations = 0;
            }

            while mutations_in_flight.load(Ordering::Acquire) != 0 {
                let event = tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            shutdown_coverage_fail_closed(&cache, next_coverage).await;
                            drop(running);
                            return;
                        }
                        continue;
                    }
                    event = events.recv() => event,
                };
                let Some(event) = event else {
                    shutdown_coverage_fail_closed(&cache, next_coverage).await;
                    drop(running);
                    return;
                };
                if matches!(event, CacheControlEvent::MutationFinished) {
                    continue;
                }
                queue_control_event(&mut pending_control_events, event);
                shutdown_coverage_fail_closed(&cache, next_coverage).await;
                continue 'recover;
            }

            if !policy.coverage_health.is_usable() || !next_coverage.monitors_running() {
                shutdown_coverage_fail_closed(&cache, next_coverage).await;
                continue;
            }
            control.rearm_node_loss();
            if !policy.coverage_health.is_usable() || !next_coverage.monitors_running() {
                control.node_lost(None);
                shutdown_coverage_fail_closed(&cache, next_coverage).await;
                continue;
            }
            cache.write().await.resume();
            if !gate.open_if_generation(expected_gate_generation) {
                shutdown_coverage_fail_closed(&cache, next_coverage).await;
                continue;
            }
            if !policy.coverage_health.ensure_usable(&control) || !next_coverage.monitors_running()
            {
                control.node_lost(None);
                shutdown_coverage_fail_closed(&cache, next_coverage).await;
                continue;
            }
            observed_revision = candidate_revision;
            coverage = next_coverage;
            reconnect_backoff = INITIAL_RECONNECT_BACKOFF;
            break 'recover;
        }
    }
}

async fn apply_control_event(
    event: CacheControlEvent,
    cache: &Arc<RwLock<CacheState>>,
    observed_revision: &mut TopologyRevision,
    pending_cancelled_mutations: &mut usize,
) {
    match event {
        CacheControlEvent::TopologyChanged { change, .. } => {
            let mut state = cache.write().await;
            state.suspend();
            match change.continuity_after(*observed_revision) {
                ChangeContinuity::AlreadyApplied => {}
                ChangeContinuity::Contiguous => {
                    for slot in &change.diff.changed_slots {
                        state.invalidate_partition(slot.slot);
                    }
                    *observed_revision = change.revision;
                }
                ChangeContinuity::Gap => {
                    state.clear();
                    *observed_revision = change.revision;
                }
            }
        }
        CacheControlEvent::NodeLost { addr, .. } => {
            if let Some(addr) = addr {
                tracing::warn!(%addr, "cluster cache coverage lost");
            }
            cache.write().await.disable();
        }
        CacheControlEvent::MutationCancelled { .. } => {
            *pending_cancelled_mutations = pending_cancelled_mutations.saturating_add(1);
            cache.write().await.disable();
        }
        CacheControlEvent::MutationFinished => {}
    }
}

async fn wait_before_retry(
    delay: Duration,
    shutdown: &mut watch::Receiver<bool>,
    events: &mut mpsc::UnboundedReceiver<CacheControlEvent>,
    pending: &mut BTreeMap<u64, CacheControlEvent>,
) {
    tokio::select! {
        biased;
        _ = shutdown.changed() => {}
        event = events.recv() => {
            if let Some(event) = event {
                queue_control_event(pending, event);
            }
        }
        _ = tokio::time::sleep(delay) => {}
    }
}

struct SupervisorRunningGuard {
    gate: Arc<CacheSafetyGate>,
    cache: Arc<RwLock<CacheState>>,
}

impl Drop for SupervisorRunningGuard {
    fn drop(&mut self) {
        self.gate.close();
        if let Ok(mut cache) = self.cache.try_write() {
            cache.disable();
        } else if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let cache = Arc::clone(&self.cache);
            runtime.spawn(async move {
                cache.write().await.disable();
            });
        }
    }
}

struct CacheLifecycle {
    inner: Arc<CacheLifecycleInner>,
    owns_client: bool,
}

struct CacheLifecycleInner {
    shutdown: watch::Sender<bool>,
    clients: AtomicUsize,
    running: std::sync::atomic::AtomicBool,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Clone for CacheLifecycle {
    fn clone(&self) -> Self {
        self.inner.clients.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            owns_client: true,
        }
    }
}

impl Drop for CacheLifecycle {
    fn drop(&mut self) {
        if self.release_client() {
            self.inner.shutdown.send_replace(true);
        }
    }
}

impl Drop for CacheLifecycleInner {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

impl CacheLifecycle {
    fn new(shutdown: watch::Sender<bool>, task: tokio::task::JoinHandle<()>) -> Self {
        Self {
            inner: Arc::new(CacheLifecycleInner {
                shutdown,
                clients: AtomicUsize::new(1),
                running: std::sync::atomic::AtomicBool::new(true),
                task: StdMutex::new(Some(task)),
            }),
            owns_client: true,
        }
    }

    fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::Acquire)
            && self
                .inner
                .task
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(|task| !task.is_finished())
    }

    fn release_client(&mut self) -> bool {
        if !self.owns_client {
            return false;
        }
        self.owns_client = false;
        self.inner.clients.fetch_sub(1, Ordering::AcqRel) == 1
    }

    async fn shutdown(mut self) -> bool {
        if !self.release_client() {
            return false;
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
        self.inner.running.store(false, Ordering::Release);
        true
    }
}

// Match `MultiplexedClusterClient`'s typed Tower surface. Routing is
// asynchronous, so readiness reports lifecycle only; the selected node worker
// still enforces actual queue capacity.
impl<Cmd: Command + 'static> tower_service::Service<Cmd> for CachedMultiplexedClusterClient {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = CacheFuture<'static, Result<Cmd::Response, RedisError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.lifecycle.is_running() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(RedisError::ConnectionClosed))
        }
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let client = self.clone();
        Box::pin(async move { client.execute(cmd).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_protocol::helpers::{array, bulk};
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct FakeBackend {
        calls: Arc<AtomicUsize>,
        modes: Arc<StdMutex<Vec<CacheDispatchMode>>>,
        response: Frame,
        block: Option<Arc<Notify>>,
        started: Option<Arc<Notify>>,
        command_timeout: bool,
    }

    impl FakeBackend {
        fn responding(response: Frame) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                modes: Arc::new(StdMutex::new(Vec::new())),
                response,
                block: None,
                started: None,
                command_timeout: false,
            }
        }
    }

    impl ClusterCacheBackend for FakeBackend {
        fn cache_node_snapshot(
            &self,
        ) -> CacheFuture<'_, Result<ClusterCacheNodeSnapshot, RedisError>> {
            Box::pin(async { panic!("unused by request-path unit tests") })
        }

        fn install_cache_hooks(
            &self,
            _hooks: ClusterCacheHooks,
        ) -> CacheFuture<'_, Result<(), RedisError>> {
            Box::pin(async { Ok(()) })
        }

        fn execute_cache_request(
            &self,
            _request: ClusterCacheRequest,
            mode: CacheDispatchMode,
        ) -> CacheFuture<'_, Result<Frame, RedisError>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.modes.lock().unwrap().push(mode);
            let response = self.response.clone();
            let block = self.block.clone();
            let started = self.started.clone();
            let command_timeout = self.command_timeout;
            Box::pin(async move {
                if let Some(started) = started {
                    started.notify_one();
                }
                if let Some(block) = block {
                    block.notified().await;
                }
                if command_timeout {
                    Err(RedisError::CommandTimeout)
                } else {
                    Ok(response)
                }
            })
        }

        fn execute_cache_node_pipeline(
            &self,
            _addr: &str,
            _frames: Vec<Frame>,
        ) -> CacheFuture<'_, Result<Vec<Frame>, RedisError>> {
            Box::pin(async { panic!("unused by request-path unit tests") })
        }

        fn execute_cache_pipeline(
            &self,
            _frames: Vec<Frame>,
        ) -> CacheFuture<'_, Result<Vec<Frame>, RedisError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn get(key: &str) -> Frame {
        array(vec![bulk("GET"), bulk(key)])
    }

    fn set(key: &str, value: &str) -> Frame {
        array(vec![bulk("SET"), bulk(key), bulk(value)])
    }

    fn request(frame: Frame) -> ClusterCacheRequest {
        ClusterCacheRequest {
            frame,
            command_name: "TEST".to_string(),
            deadline: None,
            idempotent: false,
            is_blocking: false,
        }
    }

    #[test]
    fn cached_client_preserves_typed_tower_service_surface() {
        fn assert_service<S, Req>()
        where
            S: tower_service::Service<Req>,
        {
        }

        assert_service::<CachedMultiplexedClusterClient, redis_tower_commands::Get>();
    }

    #[tokio::test]
    async fn builder_rejects_missing_client_ttl_before_trying_seed() {
        let result = CachedMultiplexedClusterClient::builder("not a valid seed address")
            .cache_config(CachedClientConfig::new().client_ttl(None))
            .connect()
            .await;
        let Err(error) = result else {
            panic!("a cluster cache without a finite client TTL was accepted");
        };

        assert!(
            matches!(
                error,
                RedisError::Redis(ref message) if message == FINITE_CLIENT_TTL_REQUIRED
            ),
            "configuration validation should run before seed parsing or connection: {error}"
        );
    }

    type RuntimeParts = (
        Arc<RwLock<CacheState>>,
        CachePolicy,
        Arc<AtomicUsize>,
        Arc<CacheSafetyGate>,
        CacheControl,
        mpsc::UnboundedReceiver<CacheControlEvent>,
    );

    fn runtime_parts(mode: CacheTrackingMode) -> RuntimeParts {
        let config = CachedClientConfig::default().tracking_mode(mode.clone());
        let cache = Arc::new(RwLock::new(config.new_state()));
        let mutations = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(CacheSafetyGate::new_open());
        let (tx, rx) = mpsc::unbounded_channel();
        let control = CacheControl {
            gate: Arc::clone(&gate),
            tx,
            node_loss_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (
            cache,
            CachePolicy {
                tracking_mode: mode,
                coverage_health: CoverageHealth::assumed_healthy(),
            },
            mutations,
            gate,
            control,
            rx,
        )
    }

    #[test]
    fn safety_gate_cannot_be_reopened_by_stale_recovery() {
        let gate = CacheSafetyGate::new_closed();
        let first = gate.generation();
        assert!(gate.open_if_generation(first));
        gate.close();
        assert!(!gate.open_if_generation(first));
        assert!(!gate.is_open());
        let second = gate.generation();
        assert!(gate.open_if_generation(second));
    }

    #[test]
    fn node_loss_events_coalesce_until_recovery_rearms_them() {
        let (_cache, _policy, _mutations, gate, control, mut events) =
            runtime_parts(CacheTrackingMode::ServerDefault);
        let initial_generation = gate.generation();

        control.node_lost(Some("first".to_string()));
        control.node_lost(Some("duplicate".to_string()));
        assert_eq!(gate.generation(), initial_generation + 1);
        assert!(matches!(
            events.try_recv(),
            Ok(CacheControlEvent::NodeLost { addr: Some(addr), .. }) if addr == "first"
        ));
        assert!(events.try_recv().is_err());

        control.rearm_node_loss();
        control.node_lost(Some("after-rearm".to_string()));
        assert_eq!(gate.generation(), initial_generation + 2);
        assert!(matches!(
            events.try_recv(),
            Ok(CacheControlEvent::NodeLost { addr: Some(addr), .. }) if addr == "after-rearm"
        ));
    }

    #[tokio::test]
    async fn concurrent_final_shutdowns_elect_one_supervisor_joiner() {
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let task = tokio::spawn(async move {
            while shutdown_rx.changed().await.is_ok() {
                if *shutdown_rx.borrow() {
                    task_stopped.store(true, Ordering::Release);
                    return;
                }
            }
        });
        let first = CacheLifecycle::new(shutdown, task);
        let second = first.clone();

        let (first_joined, second_joined) = tokio::join!(first.shutdown(), second.shutdown());

        assert_ne!(first_joined, second_joined);
        assert!(stopped.load(Ordering::Acquire));
    }

    #[test]
    fn broadcast_prefix_filter_is_binary_safe() {
        let mode = CacheTrackingMode::broadcast_with_prefixes([
            Bytes::from_static(b"tenant:\0"),
            Bytes::from_static(b"public:"),
        ]);
        assert!(key_matches_tracking_mode(&mode, b"tenant:\0key"));
        assert!(key_matches_tracking_mode(&mode, b"public:key"));
        assert!(!key_matches_tracking_mode(&mode, b"tenant:key"));
    }

    #[tokio::test]
    async fn cache_miss_then_partition_scoped_hit() {
        let backend = FakeBackend::responding(Frame::BulkString(Some(Bytes::from_static(b"v"))));
        let (cache, policy, mutations, gate, control, _events) =
            runtime_parts(CacheTrackingMode::ServerDefault);

        let first = execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(get("key")),
        )
        .await
        .unwrap();
        let second = execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(get("key")),
        )
        .await
        .unwrap();

        assert_eq!(first, second);
        assert_eq!(backend.calls.load(Ordering::Relaxed), 1);
        assert_eq!(cache.read().await.len(), 1);
    }

    #[tokio::test]
    async fn opt_in_cache_miss_uses_atomic_dispatch_mode() {
        let backend = FakeBackend::responding(Frame::BulkString(Some(Bytes::from_static(b"v"))));
        let (cache, policy, mutations, gate, control, _events) =
            runtime_parts(CacheTrackingMode::OptIn);

        execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(get("key")),
        )
        .await
        .unwrap();

        assert_eq!(
            *backend.modes.lock().unwrap(),
            vec![CacheDispatchMode::OptIn]
        );
    }

    #[tokio::test]
    async fn opt_in_request_bypasses_setup_while_cache_gate_is_closed() {
        let backend = FakeBackend::responding(Frame::BulkString(Some(Bytes::from_static(b"v"))));
        let (cache, policy, mutations, gate, control, _events) =
            runtime_parts(CacheTrackingMode::OptIn);
        gate.close();

        execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(get("key")),
        )
        .await
        .unwrap();

        assert_eq!(
            *backend.modes.lock().unwrap(),
            vec![CacheDispatchMode::Plain]
        );
    }

    #[tokio::test]
    async fn unseen_data_health_change_synchronously_bypasses_a_cache_hit() {
        let backend =
            FakeBackend::responding(Frame::BulkString(Some(Bytes::from_static(b"fresh"))));
        let (cache, policy, mutations, gate, control, mut events) =
            runtime_parts(CacheTrackingMode::ServerDefault);
        let frame = get("key");
        let (cache_key, redis_key) = extract_cache_entry(&frame).unwrap();
        let partition = slot_for_key(&redis_key);
        let epoch = cache
            .read()
            .await
            .snapshot_epoch_in_partition(&redis_key, partition)
            .unwrap();
        assert!(cache.write().await.insert_if_current_in_partition(
            cache_key,
            redis_key,
            Frame::BulkString(Some(Bytes::from_static(b"stale"))),
            partition,
            epoch,
        ));
        policy.coverage_health.test_senders.lock().unwrap()[0].send_replace(false);

        let response = execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(frame),
        )
        .await
        .unwrap();

        assert_eq!(
            response,
            Frame::BulkString(Some(Bytes::from_static(b"fresh")))
        );
        assert_eq!(backend.calls.load(Ordering::Relaxed), 1);
        assert!(!gate.is_open());
        assert!(matches!(
            events.try_recv(),
            Ok(CacheControlEvent::NodeLost { .. })
        ));
    }

    #[tokio::test]
    async fn write_pre_and_post_invalidation_prevents_a_stale_hit() {
        let backend = FakeBackend::responding(Frame::BulkString(Some(Bytes::from_static(b"v"))));
        let (cache, policy, mutations, gate, control, _events) =
            runtime_parts(CacheTrackingMode::ServerDefault);

        execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(get("key")),
        )
        .await
        .unwrap();
        execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(set("key", "new")),
        )
        .await
        .unwrap();
        execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(get("key")),
        )
        .await
        .unwrap();

        assert_eq!(backend.calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn cancelled_mutation_closes_gate_and_requests_recovery() {
        let started = Arc::new(Notify::new());
        let backend = FakeBackend {
            calls: Arc::new(AtomicUsize::new(0)),
            modes: Arc::new(StdMutex::new(Vec::new())),
            response: Frame::SimpleString(Bytes::from_static(b"OK")),
            block: Some(Arc::new(Notify::new())),
            started: Some(Arc::clone(&started)),
            command_timeout: false,
        };
        let (cache, policy, mutations, gate, control, mut events) =
            runtime_parts(CacheTrackingMode::ServerDefault);

        let mut future = Box::pin(execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(set("key", "value")),
        ));
        tokio::select! {
            _ = started.notified() => {}
            result = &mut future => panic!("blocked mutation unexpectedly completed: {result:?}"),
        }
        drop(future);
        tokio::task::yield_now().await;

        assert!(!gate.is_open());
        assert_eq!(mutations.load(Ordering::Acquire), 1);
        assert!(matches!(
            events.recv().await,
            Some(CacheControlEvent::MutationCancelled { .. })
        ));
    }

    #[tokio::test]
    async fn timed_out_noloop_mutation_requests_fail_closed_recovery() {
        let mut backend = FakeBackend::responding(Frame::SimpleString(Bytes::from_static(b"OK")));
        backend.command_timeout = true;
        let (cache, policy, mutations, gate, control, mut events) =
            runtime_parts(CacheTrackingMode::ServerDefault);

        let result = execute_cached_request(
            &backend,
            &cache,
            &policy,
            &mutations,
            &gate,
            &control,
            request(set("key", "value")),
        )
        .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert!(!gate.is_open());
        assert_eq!(mutations.load(Ordering::Acquire), 0);
        assert!(matches!(
            events.recv().await,
            Some(CacheControlEvent::NodeLost { .. })
        ));
    }

    #[tokio::test]
    async fn out_of_prefix_read_bypasses_cache_without_clearing_it() {
        let backend = FakeBackend::responding(Frame::BulkString(Some(Bytes::from_static(b"v"))));
        let (cache, policy, mutations, gate, control, _events) =
            runtime_parts(CacheTrackingMode::broadcast_with_prefixes([
                Bytes::from_static(b"allowed:"),
            ]));

        for _ in 0..2 {
            execute_cached_request(
                &backend,
                &cache,
                &policy,
                &mutations,
                &gate,
                &control,
                request(get("outside:key")),
            )
            .await
            .unwrap();
        }

        assert_eq!(backend.calls.load(Ordering::Relaxed), 2);
        assert!(cache.read().await.is_empty());
    }

    #[tokio::test]
    async fn global_receiver_handover_clears_unchanged_slot_entries() {
        use crate::topology::changes::TopologyChangeTracker;
        use crate::topology::{ClusterTopology, NodeAddr, SlotRange};

        let cache = Arc::new(RwLock::new(CacheState::default()));
        let request = get("unchanged:key");
        let (cache_key, redis_key) = extract_cache_entry(&request).unwrap();
        let partition = slot_for_key(&redis_key);
        assert_ne!(partition, 0, "test key must be outside the changed slot");
        let epoch = cache
            .read()
            .await
            .snapshot_epoch_in_partition(&redis_key, partition)
            .unwrap();
        assert!(cache.write().await.insert_if_current_in_partition(
            cache_key.clone(),
            redis_key.clone(),
            Frame::BulkString(Some(Bytes::from_static(b"old"))),
            partition,
            epoch,
        ));

        shutdown_coverage_fail_closed(
            &cache,
            Coverage {
                monitors: Vec::new(),
            },
        )
        .await;

        let owner = |port| NodeAddr {
            host: "127.0.0.1".to_string(),
            port,
        };
        let old = ClusterTopology::new(vec![SlotRange {
            start: 0,
            end: 16_383,
            master: owner(7000),
            replicas: Vec::new(),
        }]);
        let new = ClusterTopology::new(vec![
            SlotRange {
                start: 0,
                end: 0,
                master: owner(7001),
                replicas: Vec::new(),
            },
            SlotRange {
                start: 1,
                end: 16_383,
                master: owner(7000),
                replicas: Vec::new(),
            },
        ]);
        let mut tracker = TopologyChangeTracker::new();
        let change = tracker.record(&old, &new).unwrap();
        let mut observed_revision = TopologyRevision::INITIAL;
        let mut pending_cancelled_mutations = 0;
        apply_control_event(
            CacheControlEvent::TopologyChanged {
                generation: 1,
                change: Arc::clone(&change),
            },
            &cache,
            &mut observed_revision,
            &mut pending_cancelled_mutations,
        )
        .await;

        let mut state = cache.write().await;
        assert!(!state.is_enabled());
        assert!(state.is_empty());
        state.resume();
        assert!(state.get_in_partition(&cache_key, partition).is_none());
        assert!(!state.insert_if_current_in_partition(
            cache_key,
            redis_key,
            Frame::BulkString(Some(Bytes::from_static(b"late"))),
            partition,
            epoch,
        ));
        assert_eq!(observed_revision, change.revision);
    }

    #[tokio::test]
    async fn recovery_applies_out_of_order_gate_events_before_reopen() {
        let cache = Arc::new(RwLock::new(CacheState::default()));
        let gate = CacheSafetyGate::new_open();
        let first_generation = gate.close();
        let second_generation = gate.close();
        let (tx, mut events) = mpsc::unbounded_channel();
        tx.send(CacheControlEvent::MutationCancelled {
            generation: second_generation,
        })
        .unwrap();
        tx.send(CacheControlEvent::NodeLost {
            generation: first_generation,
            addr: None,
        })
        .unwrap();

        let first_event = events.recv().await.unwrap();
        let mut pending = BTreeMap::new();
        queue_control_event(&mut pending, first_event);
        let mut applied_generation = 0;
        let mut observed_revision = TopologyRevision::INITIAL;
        let mut pending_cancelled_mutations = 0;
        let (_shutdown_tx, mut shutdown) = watch::channel(false);

        assert!(
            apply_control_events_through_current_generation(
                &gate,
                &mut events,
                &mut pending,
                &mut applied_generation,
                &mut shutdown,
                &cache,
                &mut observed_revision,
                &mut pending_cancelled_mutations,
            )
            .await
        );
        assert_eq!(applied_generation, second_generation);
        assert_eq!(pending_cancelled_mutations, 1);
        assert!(cache.read().await.is_empty());
        assert!(gate.open_if_generation(applied_generation));
    }

    #[test]
    fn topology_hook_closes_gate_before_delivery() {
        use crate::topology::changes::TopologyChangeTracker;
        use crate::topology::{ClusterTopology, NodeAddr, SlotRange};

        let topology = |port| {
            ClusterTopology::new(vec![SlotRange {
                start: 0,
                end: 16_383,
                master: NodeAddr {
                    host: "127.0.0.1".to_string(),
                    port,
                },
                replicas: Vec::new(),
            }])
        };
        let old = topology(7000);
        let new = topology(7001);
        let mut tracker = TopologyChangeTracker::new();
        let change = tracker.record(&old, &new).unwrap();
        let gate = Arc::new(CacheSafetyGate::new_open());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let hooks = ClusterCacheHooks {
            control: CacheControl {
                gate: Arc::clone(&gate),
                tx,
                node_loss_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        };

        hooks.topology_changing(change);

        assert!(!gate.is_open());
        assert!(matches!(
            rx.try_recv(),
            Ok(CacheControlEvent::TopologyChanged { .. })
        ));
    }
}
