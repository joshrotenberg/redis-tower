//! Multiplexed Redis Cluster client.
//!
//! [`MultiplexedClusterClient`] is the high-concurrency sibling of
//! [`ClusterConnection`](crate::ClusterConnection). Where `ClusterConnection`
//! owns one synchronous [`RedisConnection`](redis_tower_core::RedisConnection)
//! per node and is wrapped in a single cluster-wide mutex by
//! [`ClusterClient`](crate::ClusterClient), this type owns a per-node
//! [`AutoPipelineService`] backed by [`MultiplexedClient::from_factory`].
//! That means:
//!
//! - Concurrent requests from multiple tasks are batched into Redis pipelines
//!   automatically (per node).
//! - No global mutex -- slot routing is a short read-lock lookup.
//! - Startup connects to every node concurrently (bounded), so a large
//!   cluster does not pay one connect round trip per node before the client
//!   is usable.
//! - Each per-node connection transparently reconnects on failure via a
//!   [`ConnectionFactory`], with configurable backoff.
//! - Factories replay per-node session setup (AUTH, protocol negotiation,
//!   READONLY) in the same order on every reconnect.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower_cluster::MultiplexedClusterClient;
//! use redis_tower_commands::Set;
//!
//! let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
//!
//! // Clone freely across tasks -- all share one worker per node.
//! let c = client.clone();
//! tokio::spawn(async move {
//!     c.execute(Set::new("key", "value")).await.unwrap();
//! });
//! # Ok(())
//! # }
//! ```
//!
//! # Redirect handling
//!
//! MOVED and ASK redirects are handled transparently. ASK is dispatched as
//! an atomic `[ASKING, cmd]` pipeline via
//! [`AutoPipelineService::call_pipeline`], so the ASKING connection state
//! set by the first frame is always consumed by our migrated command and
//! not by another in-flight request from a concurrent task.
//!
//! Redirects are emitted as structured warning events and topology refreshes
//! emit lifecycle events. Configure
//! [`MultiplexedClusterClientBuilder::metrics_recorder`] to record logical
//! command, redirect, refresh, and pipeline measurements. Per-node command
//! labels are opt-in through
//! [`MultiplexedClusterClientBuilder::include_node_in_metrics`] and bounded to
//! 64 concrete addresses plus `_OTHER`.
//!
//! # Transactions
//!
//! This client does **not** support MULTI/EXEC. Keyless transaction commands
//! route to the default node while the queued commands route by their own
//! keys, so a transaction would scatter across nodes and not execute
//! atomically. Use [`ClusterConnection`](crate::ClusterConnection) or
//! [`ClusterClient`](crate::ClusterClient) with [`redis_tower::Transaction`];
//! those executors validate that every key shares one hash slot and pin the
//! complete WATCH/MULTI/EXEC exchange to its owning master.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock as StdRwLock, Weak};
use std::time::{Duration, Instant};

use futures::stream::{StreamExt, TryStreamExt};
use redis_tower::AutoPipelineService;
use redis_tower::PipelineExecutor;
use redis_tower::RedisExecutor;
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::credentials::{
    CredentialProvider, CredentialReauthenticationHandle, Credentials, StreamingCredentialProvider,
    spawn_credential_reauthentication as spawn_reauthentication_task,
};
use redis_tower::metrics_layer::{
    ClusterRedirectKind, ClusterTopologyRefreshOutcome, ErrorKind, MetricsRecorder,
};
use redis_tower::reconnect::{ConnectionFactory, ReconnectConfig};
use redis_tower_commands::ClientCaching;
#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
use redis_tower_core::tls::TlsConfig;
use redis_tower_core::{
    Command, ConnectionConfig, Frame, ProtocolVersion, RedisConnection, RedisError, RespLimits,
};
use redis_tower_protocol::helpers::{array, bulk};
use tokio::sync::{Notify, RwLock};
use tower_service::Service;

use crate::caching::{
    CacheDispatchMode, ClusterCacheBackend, ClusterCacheHooks, ClusterCacheMaster,
    ClusterCacheNodeSnapshot, ClusterCacheRequest,
};
use crate::connection::{
    ClusterNodeConnector, MAX_REDIRECTS, ReadPreference, ReadRoutingStrategy, Redirect,
    RoundRobinRouting, TRANSIENT_RETRY_BACKOFF, TransientError, parse_cluster_url, parse_redirect,
    remap_topology, remap_topology_with_map, replica_unavailable, strict_replica_read_slot,
};
use crate::key_extractor;
use crate::slot::slot_for_key;
use crate::topology::changes::{TopologyChange, TopologyChangeTracker, TopologySnapshot};
use crate::topology::{ClusterTopology, NodeAddr, discover_topology};

/// A high-concurrency, multiplexed Redis Cluster client.
///
/// See the crate module-level docs (`redis_tower_cluster::multiplexed`) for
/// an overview.
pub struct MultiplexedClusterClient {
    inner: Arc<RwLock<Inner>>,
    cache_hooks: Arc<StdRwLock<Option<ClusterCacheHooks>>>,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
    include_node_in_metrics: bool,
    node_metric_labels: Arc<BoundedNodeMetricLabels>,
    /// Rate-limits and single-flights background self-healing refreshes shared
    /// across clones, so a node failure seen by many concurrent commands
    /// triggers one refresh, not a storm.
    refresh_gate: Arc<RefreshGate>,
}

impl Clone for MultiplexedClusterClient {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cache_hooks: Arc::clone(&self.cache_hooks),
            metrics_recorder: self.metrics_recorder.clone(),
            include_node_in_metrics: self.include_node_in_metrics,
            node_metric_labels: Arc::clone(&self.node_metric_labels),
            refresh_gate: Arc::clone(&self.refresh_gate),
        }
    }
}

/// Maximum number of distinct node-address labels emitted by one client.
///
/// Node labels are disabled by default. When enabled, addresses beyond this
/// cap are folded into `_OTHER`, keeping long-running metrics cardinality
/// bounded even while a cluster repeatedly changes membership.
const MAX_NODE_METRIC_LABELS: usize = 64;

#[derive(Default)]
struct BoundedNodeMetricLabels {
    seen: StdRwLock<HashSet<String>>,
}

impl BoundedNodeMetricLabels {
    fn label<'a>(&self, node: &'a str) -> NodeMetricLabel<'a> {
        {
            let seen = self.seen.read().unwrap();
            if seen.contains(node) {
                return NodeMetricLabel::Node(node);
            }
            if seen.len() >= MAX_NODE_METRIC_LABELS {
                return NodeMetricLabel::Other;
            }
        }

        let mut seen = self.seen.write().unwrap();
        // Another command may have registered this node between our read and
        // write locks.
        if seen.contains(node) {
            return NodeMetricLabel::Node(node);
        }
        if seen.len() < MAX_NODE_METRIC_LABELS {
            seen.insert(node.to_string());
            NodeMetricLabel::Node(node)
        } else {
            NodeMetricLabel::Other
        }
    }
}

enum NodeMetricLabel<'a> {
    Node(&'a str),
    Other,
}

impl NodeMetricLabel<'_> {
    fn as_str(&self) -> &str {
        match self {
            Self::Node(node) => node,
            Self::Other => "_OTHER",
        }
    }
}

/// Coordinates background topology refreshes: single-flight (only one at a
/// time) and rate-limited (at most one per `min_interval`).
struct RefreshGate {
    in_flight: AtomicBool,
    last_start: Mutex<Option<Instant>>,
    min_interval: Duration,
    idle: Notify,
}

/// Releases a claimed refresh gate even if its task is cancelled or unwinds.
struct RefreshPermit {
    gate: Arc<RefreshGate>,
}

/// Owns a background refresh's internal client clone and gate permit in the
/// shutdown-safe order, including task cancellation and panic unwinding.
struct RefreshTaskGuard {
    client: Option<MultiplexedClusterClient>,
    permit: Option<RefreshPermit>,
}

impl RefreshTaskGuard {
    fn new(client: MultiplexedClusterClient, permit: RefreshPermit) -> Self {
        Self {
            client: Some(client),
            permit: Some(permit),
        }
    }

    fn client(&self) -> &MultiplexedClusterClient {
        self.client.as_ref().expect("refresh client is present")
    }
}

impl Drop for RefreshTaskGuard {
    fn drop(&mut self) {
        // `RefreshPermit` publishes idle, so the Arc-owning client clone must
        // disappear first. Option::take makes the order explicit.
        drop(self.client.take());
        drop(self.permit.take());
    }
}

impl Drop for RefreshPermit {
    fn drop(&mut self) {
        self.gate.finish();
    }
}

impl RefreshGate {
    fn new(min_interval: Duration) -> Self {
        Self {
            in_flight: AtomicBool::new(false),
            last_start: Mutex::new(None),
            min_interval,
            idle: Notify::new(),
        }
    }

    /// Try to claim the right to start a refresh.
    ///
    /// The returned permit releases the single-flight gate on drop, including
    /// task cancellation and panic unwinding. `None` means a refresh is already
    /// in flight or one ran too recently.
    fn try_begin(self: &Arc<Self>) -> Option<RefreshPermit> {
        // Single-flight: bail if another refresh is already running.
        if self.in_flight.swap(true, Ordering::AcqRel) {
            return None;
        }
        // Rate-limit: bail if we refreshed within the last `min_interval`.
        let mut last = self.last_start.lock().unwrap();
        if let Some(t) = *last
            && t.elapsed() < self.min_interval
        {
            self.finish();
            return None;
        }
        *last = Some(Instant::now());
        Some(RefreshPermit {
            gate: Arc::clone(self),
        })
    }

    fn finish(&self) {
        self.in_flight.store(false, Ordering::Release);
        self.idle.notify_waiters();
    }

    async fn wait_until_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.in_flight.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// Minimum interval between background self-healing refreshes.
const REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(1000);

/// Per-node reconnect attempts before the worker gives up and surfaces
/// `ConnectionClosed`.
///
/// Bounded -- unlike the standalone default of unbounded retries -- so a dead
/// node lets the cluster client self-heal: the worker stops looping on the dead
/// address, the resulting `ConnectionClosed` triggers a topology refresh, and
/// the refresh routes to the promoted replica's (different) address. With
/// unbounded retries the worker would loop on the dead address forever and no
/// refresh would ever fire.
const NODE_RECONNECT_MAX_RETRIES: usize = 3;

/// Maximum number of per-node connections opened concurrently when building
/// node services (startup and topology refresh).
///
/// Connecting to every node serially costs one TCP -- and, under TLS, one full
/// handshake -- round trip per node, so a large cluster pays N round trips
/// before the client is usable. Building them concurrently collapses that to
/// roughly one, but an unbounded fan-out on a 100-node cluster would open 100
/// sockets in the same instant. This caps the in-flight burst.
const MAX_CONCURRENT_CONNECTS: usize = 16;

/// Default per-node reconnect policy: bounded retries over the standard backoff.
fn default_node_reconnect() -> AutoPipelineReconnectConfig {
    AutoPipelineReconnectConfig::new(ReconnectConfig {
        max_retries: Some(NODE_RECONNECT_MAX_RETRIES),
        ..ReconnectConfig::default()
    })
}

struct Inner {
    topology: ClusterTopology,
    topology_changes: TopologyChangeTracker,
    masters: HashMap<String, AutoPipelineService>,
    replicas: HashMap<String, AutoPipelineService>,
    default_node: String,
    host_override: Option<String>,
    address_map: Option<HashMap<String, String>>,
    read_preference: ReadPreference,
    read_routing: Arc<dyn ReadRoutingStrategy>,
    max_redirects: usize,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    credentials: Option<Arc<dyn CredentialProvider>>,
    connection_config: ConnectionConfig,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<Arc<TlsConfig>>,
}

/// The narrow slice of cluster state needed by dedicated Pub/Sub sessions.
///
/// Pub/Sub sockets do not use the auto-pipeline workers, so retaining a full
/// [`MultiplexedClusterClient`] clone would unnecessarily keep every worker
/// alive and change the client's last-owner shutdown semantics. The weak
/// handle lets sharded sessions observe topology while their originating
/// client is alive without becoming another owner of that client.
#[derive(Clone)]
pub(crate) struct ClusterPubSubBackend {
    inner: Weak<RwLock<Inner>>,
    cache_hooks: Weak<StdRwLock<Option<ClusterCacheHooks>>>,
    connector: ClusterNodeConnector,
    reconnect: ReconnectConfig,
    max_redirects: usize,
}

impl ClusterPubSubBackend {
    fn inner(&self) -> Result<Arc<RwLock<Inner>>, RedisError> {
        self.inner.upgrade().ok_or(RedisError::ConnectionClosed)
    }

    pub(crate) fn reconnect_config(&self) -> &ReconnectConfig {
        &self.reconnect
    }

    pub(crate) fn max_redirects(&self) -> usize {
        self.max_redirects
    }

    pub(crate) fn fixed_factory(&self, node: NodeAddr) -> NodeConnectionFactory {
        NodeConnectionFactory {
            addr: node.addr_string(),
            readonly: false,
            connector: self.connector.clone(),
        }
    }

    /// Connect a dedicated Pub/Sub socket while honoring the client's
    /// configured per-attempt connect timeout.
    pub(crate) async fn connect_node(
        &self,
        node: &NodeAddr,
    ) -> Result<RedisConnection, RedisError> {
        let factory = self.fixed_factory(node.clone());
        match self.reconnect.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, factory.connect())
                .await
                .map_err(|_| RedisError::ConnectTimeout)?,
            None => factory.connect().await,
        }
    }

    pub(crate) async fn validate_member(&self, node: &NodeAddr) -> Result<(), RedisError> {
        let inner = self.inner()?;
        let inner = inner.read().await;
        let is_member = inner
            .topology
            .master_addrs()
            .into_iter()
            .chain(inner.topology.replica_addrs())
            .any(|candidate| candidate == node);
        if is_member {
            Ok(())
        } else {
            Err(RedisError::Redis(format!(
                "cluster Pub/Sub node {node} is not a member of the current topology"
            )))
        }
    }

    /// Atomically capture a topology revision and subscribe to all later
    /// revisions. Holding one read lock closes the snapshot/subscribe race.
    pub(crate) async fn topology_state(
        &self,
    ) -> Result<
        (
            TopologySnapshot,
            tokio::sync::watch::Receiver<Option<Arc<TopologyChange>>>,
        ),
        RedisError,
    > {
        let inner = self.inner()?;
        let inner = inner.read().await;
        let changes = inner.topology_changes.subscribe();
        let snapshot = inner.topology_changes.snapshot(&inner.topology);
        Ok((snapshot, changes))
    }

    pub(crate) async fn topology_snapshot(&self) -> Result<TopologySnapshot, RedisError> {
        let inner = self.inner()?;
        let inner = inner.read().await;
        Ok(inner.topology_changes.snapshot(&inner.topology))
    }

    /// Apply the same address-map/host-override policy used by ordinary
    /// command redirects to an address advertised in a Pub/Sub MOVED reply.
    pub(crate) async fn remap_redirect(&self, addr: &str) -> Result<NodeAddr, RedisError> {
        let inner = self.inner()?;
        let inner = inner.read().await;
        let remapped = if let Some(mapped) = inner
            .address_map
            .as_ref()
            .and_then(|address_map| address_map.get(addr))
        {
            mapped.clone()
        } else if let Some(host) = &inner.host_override {
            match addr.rsplit_once(':') {
                Some((_old_host, port)) => format!("{host}:{port}"),
                None => addr.to_string(),
            }
        } else {
            addr.to_string()
        };

        NodeAddr::parse(&remapped).ok_or_else(|| {
            RedisError::Redis(format!(
                "cluster Pub/Sub redirect returned an invalid node address: {remapped}"
            ))
        })
    }

    /// Commit an authoritative Pub/Sub MOVED reply into the shared routing
    /// snapshot and notify every topology-derived consumer before the new
    /// owner becomes visible.
    pub(crate) async fn commit_moved(&self, slot: u16, target: NodeAddr) -> Result<(), RedisError> {
        self.ensure_master(&target).await?;
        let inner = self.inner()?;
        let mut inner = inner.write().await;
        let previous_topology = inner.topology.clone();
        let mut current_topology = previous_topology.clone();
        current_topology.reassign_slot(slot, target);
        let change = inner
            .topology_changes
            .record(&previous_topology, &current_topology);
        if let Some(change) = change
            && let Some(cache_hooks) = self.cache_hooks.upgrade()
            && let Some(hooks) = cache_hooks.read().unwrap().as_ref().cloned()
        {
            hooks.topology_changing(change);
        }
        inner.topology = current_topology;
        Ok(())
    }

    /// Ensure command routing can use a Pub/Sub MOVED target before exposing
    /// that target as the shared slot owner. A failover target may previously
    /// have existed only in the replica service map, or may be entirely new.
    async fn ensure_master(&self, target: &NodeAddr) -> Result<(), RedisError> {
        let inner = self.inner()?;
        let addr = target.addr_string();
        let (pipeline_config, reconnect_config) = {
            let inner = inner.read().await;
            if inner.masters.contains_key(&addr) {
                return Ok(());
            }
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
            )
        };

        let service = build_node_service(
            &addr,
            false,
            pipeline_config,
            reconnect_config,
            self.connector.clone(),
        )
        .await?;
        inner.write().await.masters.entry(addr).or_insert(service);
        Ok(())
    }
}

/// Builder for configuring a [`MultiplexedClusterClient`].
pub struct MultiplexedClusterClientBuilder {
    seed_addr: String,
    host_override: Option<String>,
    address_map: Option<HashMap<String, String>>,
    read_preference: ReadPreference,
    read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
    max_redirects: usize,
    pipeline_config: AutoPipelineConfig,
    include_node_in_metrics: bool,
    reconnect_config: AutoPipelineReconnectConfig,
    credentials: Option<Arc<dyn CredentialProvider>>,
    connection_config: ConnectionConfig,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<Arc<TlsConfig>>,
}

impl MultiplexedClusterClientBuilder {
    /// Set the host override for Docker/proxy environments.
    pub fn host_override(mut self, host: impl Into<String>) -> Self {
        self.host_override = Some(host.into());
        self
    }

    /// Map internal cluster addresses to external addresses for NAT/Kubernetes
    /// environments. Keys are `"internal_host:port"`, values are
    /// `"external_host:port"`.
    pub fn address_map(mut self, map: HashMap<String, String>) -> Self {
        self.address_map = Some(map);
        self
    }

    /// Set the read preference for keyed, read-only commands.
    ///
    /// [`ReadPreference::Replica`] is strict: when no live, connected replica
    /// worker is available for the command's slot, the command returns an
    /// error instead of being sent to the master.
    /// [`ReadPreference::PreferReplica`] falls back to the master in that case.
    /// Writes always use the master.
    pub fn read_preference(mut self, pref: ReadPreference) -> Self {
        self.read_preference = pref;
        self
    }

    /// Set a custom read routing strategy for replica selection.
    pub fn read_routing(mut self, strategy: impl ReadRoutingStrategy) -> Self {
        self.read_routing = Some(Arc::new(strategy));
        self
    }

    /// Set the maximum number of MOVED/ASK redirects to follow for a single
    /// command before giving up with an error.
    ///
    /// The initial node attempt is always made, including when `max` is zero.
    /// Each followed redirect is another round-trip, so this bounds the worst
    /// case latency of one command during a resharding. Transient cluster
    /// replies share the same follow-up budget. Defaults to 5.
    pub fn max_redirects(mut self, max: usize) -> Self {
        self.max_redirects = max;
        self
    }

    /// Override the auto-pipeline batching config used for each per-node worker.
    pub fn pipeline_config(mut self, config: AutoPipelineConfig) -> Self {
        self.pipeline_config = config;
        self
    }

    /// Record command, redirect, topology-refresh, and pipeline metrics.
    ///
    /// The recorder is shared by the cluster router and every per-node
    /// auto-pipeline worker. Node-address labels remain disabled unless
    /// [`Self::include_node_in_metrics`] is also enabled.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.pipeline_config.metrics_recorder = Some(recorder);
        self
    }

    /// Include the final cluster node address in command metrics.
    ///
    /// Disabled by default because node addresses add metric series. When
    /// enabled, one client emits up to 64 concrete node-address labels plus
    /// `_OTHER`; later addresses are folded into `_OTHER` so cardinality stays
    /// bounded across repeated topology changes.
    pub fn include_node_in_metrics(mut self, include: bool) -> Self {
        self.include_node_in_metrics = include;
        self
    }

    /// Override the reconnect config used for each per-node worker.
    pub fn reconnect_config(mut self, config: AutoPipelineReconnectConfig) -> Self {
        self.reconnect_config = config;
        self
    }

    /// Authenticate every per-node connection using the given credential
    /// provider.
    ///
    /// The provider is consulted on the initial connection and on every
    /// reconnect (for example after a node failover), so credential
    /// rotation flows through transparently without any additional wiring:
    /// the node factory fetches fresh credentials from the provider each
    /// time it has to rebuild a connection.
    pub fn credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.credentials = Some(Arc::new(provider));
        self
    }

    /// Set the connection settings used for every cluster node.
    ///
    /// The complete configuration is retained for seed discovery, masters,
    /// replicas, redirect-created connections, topology refreshes, and worker
    /// reconnects. Authentication completes before the configured protocol is
    /// negotiated, allowing RESP3 to work with protected clusters.
    pub fn connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }

    /// Set the RESP protocol negotiation policy for every cluster node.
    pub fn protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.connection_config = self.connection_config.with_protocol(protocol);
        self
    }

    /// Set RESP decode limits for every cluster connection.
    ///
    /// The limits apply before any handshake or authentication frames are
    /// decoded and are retained for seed discovery, masters, replicas,
    /// redirect-created connections, topology refreshes, and reconnects.
    pub fn resp_limits(mut self, limits: RespLimits) -> Self {
        self.connection_config = self.connection_config.with_resp_limits(limits);
        self
    }

    /// Enable TLS for every per-node connection, including the seed
    /// connection used for topology discovery.
    ///
    /// The hostname used for SNI / certificate verification is derived
    /// from each node's address (`host` portion of `host:port`). If your
    /// cluster reports internal IPs that don't match your certificate,
    /// combine this with [`Self::host_override`] to remap all nodes to a
    /// canonical hostname, or use
    /// [`TlsConfig::danger_accept_invalid_hostnames`].
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[cfg(feature = "tls-rustls")]
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower_cluster::MultiplexedClusterClient;
    /// use redis_tower_core::tls::TlsConfig;
    ///
    /// let client = MultiplexedClusterClient::builder("redis.example.com:7000")
    ///     .tls(TlsConfig::default_rustls())
    ///     .connect()
    ///     .await?;
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(Arc::new(tls));
        self
    }

    /// Connect to the cluster.
    pub async fn connect(self) -> Result<MultiplexedClusterClient, RedisError> {
        MultiplexedClusterClient::connect_inner(
            &self.seed_addr,
            self.host_override,
            self.address_map,
            self.read_preference,
            self.read_routing,
            self.max_redirects,
            self.pipeline_config,
            self.include_node_in_metrics,
            self.reconnect_config,
            self.credentials,
            self.connection_config,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            self.tls,
        )
        .await
    }
}

impl MultiplexedClusterClient {
    /// Re-authenticate every established master and replica worker.
    ///
    /// Each `AUTH` is serialized through its node's auto-pipeline worker. A
    /// worker that rejects the replacement credential is removed from routing
    /// so the next command reconnects through the configured provider.
    pub async fn reauthenticate_all(&self, credentials: &Credentials) -> Result<(), RedisError> {
        // Gate routing and topology replacement for the duration of the pass.
        // Besides preventing mixed old/new routing after this method returns,
        // this ensures an AUTH failure cannot remove a newer worker that a
        // concurrent topology refresh installed at the same address.
        let mut inner = self.inner.write().await;
        let services: Vec<(String, bool, AutoPipelineService)> = inner
            .masters
            .iter()
            .map(|(address, service)| (address.clone(), false, service.clone()))
            .chain(
                inner
                    .replicas
                    .iter()
                    .map(|(address, service)| (address.clone(), true, service.clone())),
            )
            .collect();

        let auth = credentials.auth_command();
        let frame = auth.to_frame();
        let mut failed = Vec::new();
        let mut first_error = None;
        for (address, replica, mut service) in services {
            let result = match call_service(&mut service, frame.clone()).await {
                Ok(Frame::SimpleString(value)) if &value[..] == b"OK" => Ok(()),
                Ok(Frame::Error(error)) => Err(RedisError::Redis(
                    String::from_utf8_lossy(&error).into_owned(),
                )),
                Ok(other) => Err(RedisError::UnexpectedResponse {
                    expected: "OK",
                    actual: format!("{other:?}"),
                }),
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                failed.push((address, replica));
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        for (address, replica) in failed {
            if replica {
                inner.replicas.remove(&address);
            } else {
                inner.masters.remove(&address);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Apply each pushed credential to all current cluster node workers.
    ///
    /// Dropping the returned handle stops future reauthentication. Configure
    /// this client with the same provider so new nodes and reconnects also use
    /// the provider's current credentials.
    pub fn spawn_credential_reauthentication(
        &self,
        provider: Arc<dyn StreamingCredentialProvider>,
    ) -> CredentialReauthenticationHandle {
        let client = self.clone();
        spawn_reauthentication_task(provider, move |credentials| {
            let client = client.clone();
            async move { client.reauthenticate_all(&credentials).await }
        })
    }

    /// Connect to a cluster using a seed node address.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        Self::connect_inner(
            seed_addr,
            None,
            None,
            ReadPreference::Master,
            None,
            MAX_REDIRECTS,
            AutoPipelineConfig::default(),
            false,
            default_node_reconnect(),
            None,
            ConnectionConfig::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
        .await
    }

    /// Connect to a cluster, remapping all node hosts to `host_override`.
    pub async fn connect_with_host(
        seed_addr: &str,
        host_override: &str,
    ) -> Result<Self, RedisError> {
        Self::connect_inner(
            seed_addr,
            Some(host_override.to_string()),
            None,
            ReadPreference::Master,
            None,
            MAX_REDIRECTS,
            AutoPipelineConfig::default(),
            false,
            default_node_reconnect(),
            None,
            ConnectionConfig::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
        .await
    }

    /// Connect to a cluster from a Redis URL.
    ///
    /// Parses `redis://[user:pass@]host:port` / `rediss://...`, wiring AUTH
    /// credentials and TLS (rustls -- system roots with a webpki-roots fallback)
    /// from the URL. See
    /// [`ClusterConnection::connect_url`](crate::ClusterConnection::connect_url)
    /// for the URL semantics; use [`builder`](Self::builder) for a custom TLS
    /// config or host override.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let (seed, credentials, tls) = parse_cluster_url(url)?;
        let mut builder = Self::builder(seed);
        if let Some(creds) = credentials {
            builder = builder.credentials(creds);
        }
        if tls {
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            {
                builder = builder.tls(crate::connection::default_url_tls());
            }
            #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
            {
                return Err(crate::connection::tls_feature_required());
            }
        }
        builder.connect().await
    }

    /// Create a builder for configuring the client.
    pub fn builder(seed_addr: impl Into<String>) -> MultiplexedClusterClientBuilder {
        MultiplexedClusterClientBuilder {
            seed_addr: seed_addr.into(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: None,
            max_redirects: MAX_REDIRECTS,
            pipeline_config: AutoPipelineConfig::default(),
            include_node_in_metrics: false,
            reconnect_config: default_node_reconnect(),
            credentials: None,
            connection_config: ConnectionConfig::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_inner(
        seed_addr: &str,
        host_override: Option<String>,
        address_map: Option<HashMap<String, String>>,
        read_preference: ReadPreference,
        read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
        max_redirects: usize,
        pipeline_config: AutoPipelineConfig,
        include_node_in_metrics: bool,
        reconnect_config: AutoPipelineReconnectConfig,
        credentials: Option<Arc<dyn CredentialProvider>>,
        connection_config: ConnectionConfig,
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<Arc<TlsConfig>>,
    ) -> Result<Self, RedisError> {
        let metrics_recorder = pipeline_config.metrics_recorder.clone();
        let connector = ClusterNodeConnector::new(
            connection_config.clone(),
            credentials.clone(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls.clone(),
        );
        // Discover topology via a short-lived raw connection. Authenticate
        // before CLUSTER SLOTS so the discovery itself works against an
        // ACL-protected cluster.
        let mut seed_conn = connector.connect(seed_addr, false).await?;
        let mut topology = discover_topology(&mut seed_conn).await?;
        drop(seed_conn);

        if let Some(ref map) = address_map {
            remap_topology_with_map(&mut topology, map);
        }
        if let Some(ref host) = host_override {
            remap_topology(&mut topology, host);
        }

        // Connect to all masters through factory-backed auto-pipeline services.
        // The handshakes run concurrently (bounded by MAX_CONCURRENT_CONNECTS);
        // a large cluster would otherwise serialize one connect round trip per
        // node before `connect` returns.
        let master_addrs = dedup_addrs(topology.master_addrs());
        // Taken from the address list, not from completion order, so the
        // default node stays the first master in topology order no matter
        // which handshake finishes first.
        let mut default_node = master_addrs.first().cloned().unwrap_or_default();
        let mut masters: HashMap<String, AutoPipelineService> = build_node_services(
            master_addrs,
            /* readonly = */ false,
            pipeline_config.clone(),
            reconnect_config.clone(),
            connector.clone(),
        )
        .await?;

        // Connect to replicas if the read preference uses them.
        let replicas: HashMap<String, AutoPipelineService> =
            if read_preference == ReadPreference::Master {
                HashMap::new()
            } else {
                build_node_services(
                    dedup_addrs(topology.replica_addrs()),
                    /* readonly = */ true,
                    pipeline_config.clone(),
                    reconnect_config.clone(),
                    connector.clone(),
                )
                .await?
            };

        if default_node.is_empty() {
            // No masters discovered -- fall back to the seed addr via a fresh
            // factory-backed service so keyless commands still route somewhere.
            let svc = build_node_service(
                seed_addr,
                false,
                pipeline_config.clone(),
                reconnect_config.clone(),
                connector,
            )
            .await?;
            masters.insert(seed_addr.to_string(), svc);
            default_node = seed_addr.to_string();
        }

        let read_routing = read_routing.unwrap_or_else(|| Arc::new(RoundRobinRouting::new()));

        Ok(Self {
            inner: Arc::new(RwLock::new(Inner {
                topology,
                topology_changes: TopologyChangeTracker::new(),
                masters,
                replicas,
                default_node,
                host_override,
                address_map,
                read_preference,
                read_routing,
                max_redirects,
                pipeline_config,
                reconnect_config,
                credentials,
                connection_config,
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                tls,
            })),
            cache_hooks: Arc::new(StdRwLock::new(None)),
            metrics_recorder,
            include_node_in_metrics,
            node_metric_labels: Arc::new(BoundedNodeMetricLabels::default()),
            refresh_gate: Arc::new(RefreshGate::new(REFRESH_MIN_INTERVAL)),
        })
    }

    /// Execute a command, routing it to the correct cluster node.
    ///
    /// Handles MOVED and ASK redirects transparently. ASK is handled by
    /// sending `ASKING` + the migrated command as an atomic pipeline through
    /// the target node, preserving single-connection ordering during
    /// live resharding. A deadline carried by
    /// [`redis_tower_core::WithDeadline`] bounds the complete routing,
    /// readiness, redirect, and retry operation.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        self.execute_with_dispatch(cmd, CacheDispatchMode::Plain)
            .await
    }

    async fn execute_with_dispatch<Cmd: Command>(
        &self,
        cmd: Cmd,
        dispatch_mode: CacheDispatchMode,
    ) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        let observation = self
            .metrics_recorder
            .as_ref()
            .map(|_| (cmd.name().to_ascii_uppercase(), Instant::now()));
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            if let Some((command, started)) = observation {
                self.record_command_completion(
                    &command,
                    started.elapsed(),
                    Some(ErrorKind::from_error(&RedisError::CommandTimeout)),
                    None,
                );
            }
            return Err(RedisError::CommandTimeout);
        }
        let observe_metrics = observation.is_some();
        let cmd_frame = cmd.to_frame();
        let mut last_node = None;

        let operation = self.execute_routed(
            cmd,
            cmd_frame,
            dispatch_mode,
            observe_metrics,
            &mut last_node,
        );
        let result = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, operation).await {
                Ok(result) => result,
                Err(_elapsed) => Err(RedisError::CommandTimeout),
            },
            None => operation.await,
        };
        if let Some((command, started)) = observation {
            let error = result.as_ref().err().map(ErrorKind::from_error);
            self.record_command_completion(
                &command,
                started.elapsed(),
                error,
                last_node.as_deref(),
            );
        }
        result
    }

    /// Execute raw pipeline frames across their owning cluster nodes.
    ///
    /// Slot extraction is completed for the entire input before any request is
    /// sent. Frames are then pinned to their owning masters from one topology
    /// snapshot, grouped by concrete node, dispatched concurrently, and
    /// restored to submission order. Explicit pipelines intentionally ignore
    /// replica read preference so dependent commands in one slot retain their
    /// wire order. A node-batch transport error is returned for the whole
    /// operation because Redis may have applied an unknown prefix of that
    /// batch.
    async fn execute_cluster_pipeline(&self, frames: Vec<Frame>) -> Result<Vec<Frame>, RedisError> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }

        // Validate every known command before the first wire write and retain
        // its authoritative routing slot. Unknown/custom commands deliberately
        // preserve the legacy first-argument fallback: pipelines are
        // non-atomic, unlike transactions, so compatibility is preferable to
        // rejecting an otherwise routable extension command.
        let routing_slots = frames
            .iter()
            .map(|frame| {
                key_extractor::pipeline_routing_slot(frame)
                    .map_err(|error| RedisError::Redis(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut groups: HashMap<String, (AutoPipelineService, Vec<(usize, Frame)>)> =
            HashMap::new();
        {
            // Resolve the complete routing plan while holding one read guard.
            // A concurrent topology refresh therefore cannot split two
            // same-slot entries across old and new owners. Keyed frames fail
            // closed when the snapshot has no usable owner; silently falling
            // back to the default node would defeat preflight and ordering.
            let inner = self.inner.read().await;
            for (index, (frame, slot)) in frames
                .iter()
                .cloned()
                .zip(routing_slots.iter().copied())
                .enumerate()
            {
                let (addr, service) = match slot {
                    Some(slot) => {
                        let owner = inner.topology.master_for_slot(slot).ok_or_else(|| {
                            RedisError::Redis(format!(
                                "no cluster master owns pipeline slot {slot}"
                            ))
                        })?;
                        let addr = owner.addr_string();
                        let service = inner.masters.get(&addr).cloned().ok_or_else(|| {
                            RedisError::Redis(format!(
                                "no connected master service for pipeline slot {slot} ({addr})"
                            ))
                        })?;
                        (addr, service)
                    }
                    None => {
                        let addr = inner.default_node.clone();
                        let service = inner.masters.get(&addr).cloned().ok_or_else(|| {
                            RedisError::Redis(format!(
                                "no connected default master service for pipeline ({addr})"
                            ))
                        })?;
                        (addr, service)
                    }
                };
                groups
                    .entry(addr)
                    .or_insert_with(|| (service, Vec::new()))
                    .1
                    .push((index, frame));
            }
        }

        // Await every submitted node batch even if one fails. Returning early
        // would cancel sibling futures after some of them may already have
        // reached Redis, making the ambiguity wider and harder to reason about.
        let batches = groups.into_iter().map(|(addr, (mut service, entries))| {
            let client = self.clone();
            async move {
                let batch_frames = entries
                    .iter()
                    .map(|(_, frame)| frame.clone())
                    .collect::<Vec<_>>();
                let response = service.call_pipeline(batch_frames).await;
                if let Err(error) = &response {
                    client.observe_node_error(&addr, error);
                }
                (entries, response)
            }
        });
        let batches = futures::future::join_all(batches).await;

        let mut ordered = vec![None; frames.len()];
        for (entries, response) in batches {
            let responses = response?;
            if responses.len() != entries.len() {
                return Err(RedisError::UnexpectedResponse {
                    expected: "one pipeline response per command",
                    actual: format!(
                        "received {} responses for {} commands",
                        responses.len(),
                        entries.len()
                    ),
                });
            }
            for ((index, _), response) in entries.into_iter().zip(responses) {
                ordered[index] = Some(response);
            }
        }

        // Successful entries are final. Only entries that explicitly asked us
        // to redirect are retried, each as its own raw frame so a successful
        // neighbor in the original node batch can never be replayed.
        let mut redirected_by_slot: BTreeMap<u16, Vec<(usize, Frame, Redirect)>> = BTreeMap::new();
        for (index, response) in ordered.iter().enumerate() {
            let Some(redirect) = response.as_ref().and_then(parse_redirect) else {
                continue;
            };
            let redirect_slot = match &redirect {
                Redirect::Moved { slot, .. } | Redirect::Ask { slot, .. } => *slot,
            };
            // Prefer the preflighted command slot. A keyless/custom command
            // without one can still acquire an authoritative slot from Redis's
            // redirect response.
            let ordering_slot = routing_slots[index].unwrap_or(redirect_slot);
            redirected_by_slot.entry(ordering_slot).or_default().push((
                index,
                frames[index].clone(),
                redirect,
            ));
        }

        // Redirects are exceptional, so preserve correctness over maximizing
        // their fan-out: entries from one slot are followed sequentially in
        // submission order, while independent slots still make progress
        // concurrently. This never replays an entry that already succeeded.
        let redirected = redirected_by_slot.into_values().map(|entries| {
            let client = self.clone();
            async move {
                let mut completed = Vec::with_capacity(entries.len());
                for (index, frame, redirect) in entries {
                    let response = client.follow_pipeline_redirect(frame, redirect).await?;
                    completed.push((index, response));
                }
                Ok::<_, RedisError>(completed)
            }
        });
        let redirected = futures::future::join_all(redirected).await;
        let mut redirect_error = None;
        for slot_result in redirected {
            match slot_result {
                Ok(completed) => {
                    for (index, response) in completed {
                        ordered[index] = Some(response);
                    }
                }
                Err(error) => {
                    if redirect_error.is_none() {
                        redirect_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = redirect_error {
            return Err(error);
        }

        ordered
            .into_iter()
            .map(|response| response.ok_or(RedisError::ConnectionClosed))
            .collect()
    }

    /// Follow a redirect already returned by a node pipeline for one frame.
    ///
    /// The original node attempt has already returned the first redirect.
    /// MOVED and ASK replies are followed exactly as for
    /// [`execute`](Self::execute), while every other Redis error is returned as
    /// that command's raw response and is not retried.
    async fn follow_pipeline_redirect(
        &self,
        frame: Frame,
        mut redirect: Redirect,
    ) -> Result<Frame, RedisError> {
        let max_redirects = self.inner.read().await.max_redirects;
        self.record_pipeline_redirect(&redirect);

        // The grouped node call is outside this loop. Each iteration follows
        // exactly one redirect, so max_redirects=1 permits one replay.
        for redirect_number in 1..=max_redirects {
            let attempt = redirect_number.saturating_add(1);
            let response = match redirect {
                Redirect::Moved { slot, addr } => {
                    self.notify_cache_node_loss(&addr);
                    let addr = self.remap_addr(&addr).await;
                    tracing::warn!(
                        command = "PIPELINE",
                        kind = "MOVED",
                        slot,
                        attempt,
                        to_addr = %addr,
                        "cluster: pipelined command redirected"
                    );
                    self.ensure_master(&addr).await?;
                    self.update_slot_owner(slot, &addr).await;
                    self.trigger_refresh();
                    let mut target = self.master_service(&addr).await?;
                    self.dispatch_to_target(
                        &mut target,
                        frame.clone(),
                        CacheDispatchMode::Plain,
                        false,
                    )
                    .await?
                }
                Redirect::Ask { slot, addr } => {
                    self.notify_cache_node_loss(&addr);
                    let addr = self.remap_addr(&addr).await;
                    tracing::warn!(
                        command = "PIPELINE",
                        kind = "ASK",
                        slot,
                        attempt,
                        to_addr = %addr,
                        "cluster: pipelined command redirected"
                    );
                    self.ensure_master(&addr).await?;
                    let mut target = self.master_service(&addr).await?;
                    self.dispatch_to_target(
                        &mut target,
                        frame.clone(),
                        CacheDispatchMode::Plain,
                        true,
                    )
                    .await?
                }
            };

            match parse_redirect(&response) {
                Some(next) => {
                    self.record_pipeline_redirect(&next);
                    redirect = next;
                }
                None => return Ok(response),
            }
        }

        Err(RedisError::Redis(format!(
            "too many redirects ({max_redirects})"
        )))
    }

    async fn dispatch_to_target(
        &self,
        target: &mut Target,
        frame: Frame,
        mode: CacheDispatchMode,
        asking: bool,
    ) -> Result<Frame, RedisError> {
        let result = async {
            match (asking, mode) {
                (false, CacheDispatchMode::Plain) => call_service(&mut target.svc, frame).await,
                (true, CacheDispatchMode::Plain) => {
                    let responses = target
                        .svc
                        .call_pipeline(vec![array(vec![bulk("ASKING")]), frame])
                        .await?;
                    parse_prefixed_response(responses, true, false)
                }
                (false, CacheDispatchMode::OptIn) => {
                    let caching = ClientCaching::new(true);
                    let responses = target
                        .svc
                        .call_pipeline(vec![caching.to_frame(), frame])
                        .await?;
                    parse_prefixed_response(responses, false, true)
                }
                (true, CacheDispatchMode::OptIn) => {
                    // ASKING and CLIENT CACHING YES are both one-shot flags.
                    // Sending either setup command after the other consumes
                    // the first flag before the keyed command runs. The ASK
                    // hook has already closed the cache gate synchronously,
                    // so bypass cache opt-in for this migrated read.
                    let responses = target
                        .svc
                        .call_pipeline(vec![array(vec![bulk("ASKING")]), frame])
                        .await?;
                    parse_prefixed_response(responses, true, false)
                }
            }
        }
        .await;

        if let Err(error) = &result {
            self.observe_node_error(&target.addr, error);
        }
        result
    }

    async fn execute_routed<Cmd: Command>(
        &self,
        cmd: Cmd,
        cmd_frame: Frame,
        dispatch_mode: CacheDispatchMode,
        observe_metrics: bool,
        last_node: &mut Option<String>,
    ) -> Result<Cmd::Response, RedisError> {
        let strict_replica_slot = {
            let inner = self.inner.read().await;
            strict_replica_read_slot(inner.read_preference, &cmd_frame)
        };
        // Initial routing.
        let mut target = self.route_command(&cmd_frame).await?;
        let max_redirects = self.inner.read().await.max_redirects;
        let mut send_asking = false;
        let mut effective_dispatch_mode = dispatch_mode;
        let mut followups_used = 0usize;

        loop {
            if observe_metrics {
                *last_node = Some(target.addr.clone());
            }
            let response = self
                .dispatch_to_target(
                    &mut target,
                    cmd_frame.clone(),
                    effective_dispatch_mode,
                    send_asking,
                )
                .await?;
            let attempt = followups_used.saturating_add(1);

            match parse_redirect(&response) {
                Some(Redirect::Moved { slot, addr }) => {
                    // A MOVED reply proves the routing snapshot is stale. Close
                    // cache use synchronously before any address remapping or
                    // node connection await can let another task serve a hit.
                    self.notify_cache_node_loss(&target.addr);
                    effective_dispatch_mode = CacheDispatchMode::Plain;
                    let addr = self.remap_addr(&addr).await;
                    tracing::warn!(
                        command = cmd.name(),
                        kind = "MOVED",
                        slot,
                        attempt,
                        from_addr = %target.addr,
                        to_addr = %addr,
                        "cluster: command redirected"
                    );
                    self.record_redirect(ClusterRedirectKind::Moved);
                    if let Some(request_slot) = strict_replica_slot {
                        // MOVED identifies a master. Retain its authoritative
                        // ownership update, but never replay a strict replica
                        // read against that master.
                        self.update_slot_owner(slot, &addr).await;
                        return Err(replica_unavailable(request_slot));
                    }
                    if followups_used >= max_redirects {
                        break;
                    }
                    followups_used += 1;
                    self.ensure_master(&addr).await?;
                    self.update_slot_owner(slot, &addr).await;
                    // Patch the single moved slot immediately, and schedule a
                    // rate-limited full refresh: during a live resharding many
                    // slots migrate, and one refresh reconciles them all.
                    self.trigger_refresh();
                    target = self.master_service(&addr).await?;
                    send_asking = false;
                    continue;
                }
                Some(Redirect::Ask { slot, addr }) => {
                    // ASK does not change the committed owner, but it still
                    // marks an active migration window in which cached data is
                    // unsafe until coverage is rebuilt.
                    self.notify_cache_node_loss(&target.addr);
                    effective_dispatch_mode = CacheDispatchMode::Plain;
                    let addr = self.remap_addr(&addr).await;
                    tracing::warn!(
                        command = cmd.name(),
                        kind = "ASK",
                        slot,
                        attempt,
                        from_addr = %target.addr,
                        to_addr = %addr,
                        "cluster: command redirected"
                    );
                    self.record_redirect(ClusterRedirectKind::Ask);
                    if let Some(request_slot) = strict_replica_slot {
                        // ASKING plus the command would run on the migrating
                        // master. Strict replica reads fail instead.
                        return Err(replica_unavailable(request_slot));
                    }
                    if followups_used >= max_redirects {
                        break;
                    }
                    followups_used += 1;
                    self.ensure_master(&addr).await?;
                    target = self.master_service(&addr).await?;
                    send_asking = true;
                    continue;
                }
                None => {
                    // Transient cluster errors: retry within the redirect
                    // budget rather than surfacing on first occurrence.
                    if let Some(transient) = TransientError::from_frame(&response) {
                        // TRYAGAIN/CLUSTERDOWN/LOADING all mean the node or slot
                        // is in a transitional state. A cached wrapper must
                        // fail closed before any retry/backoff await.
                        self.notify_cache_node_loss(&target.addr);
                        effective_dispatch_mode = CacheDispatchMode::Plain;
                        if followups_used >= max_redirects {
                            break;
                        }
                        followups_used += 1;
                        if transient == TransientError::ClusterDown {
                            // The cluster view may be stale (failover in
                            // progress). Schedule a gated background refresh --
                            // not an inline one per retry, which would storm the
                            // cluster with reconnects and stall its election --
                            // then re-route and retry after a backoff.
                            self.trigger_refresh();
                            target = self.route_command(&cmd_frame).await?;
                            send_asking = false;
                        }
                        tracing::debug!(?transient, "transient cluster error; retrying");
                        tokio::time::sleep(TRANSIENT_RETRY_BACKOFF).await;
                        continue;
                    }
                    if let Frame::Error(ref e) = response {
                        return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
                    }
                    return cmd.parse_response(response);
                }
            }
        }

        Err(RedisError::Redis(format!(
            "too many redirects ({max_redirects})"
        )))
    }

    fn record_command_completion(
        &self,
        command: &str,
        duration: Duration,
        error: Option<ErrorKind>,
        node: Option<&str>,
    ) {
        let Some(recorder) = &self.metrics_recorder else {
            return;
        };

        if self.include_node_in_metrics
            && let Some(node) = node
        {
            let node = self.node_metric_labels.label(node);
            recorder.command_completed_on_node(command, duration, error, Some(node.as_str()));
        } else {
            recorder.command_completed_on_node(command, duration, error, None);
        }
    }

    fn notify_cache_node_loss(&self, addr: &str) {
        if let Some(hooks) = self.cache_hooks.read().unwrap().as_ref().cloned() {
            hooks.node_connection_lost(addr.to_string());
        }
    }

    fn observe_node_error(&self, addr: &str, error: &RedisError) {
        // Admission failures happen before a wire write and do not disturb
        // connection-local tracking. Other service/setup failures conservatively
        // invalidate coverage, including malformed prefix responses.
        if cache_error_requires_node_loss(error) {
            self.notify_cache_node_loss(addr);
        }
        if error.is_connection_error() {
            self.trigger_refresh();
        }
    }

    fn record_redirect(&self, kind: ClusterRedirectKind) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.cluster_redirected(kind);
        }
    }

    fn record_pipeline_redirect(&self, redirect: &Redirect) {
        self.record_redirect(match redirect {
            Redirect::Moved { .. } => ClusterRedirectKind::Moved,
            Redirect::Ask { .. } => ClusterRedirectKind::Ask,
        });
    }

    fn record_topology_refresh(&self, duration: Duration, outcome: ClusterTopologyRefreshOutcome) {
        if let Some(recorder) = &self.metrics_recorder {
            recorder.cluster_topology_refresh_completed(duration, outcome);
        }
    }

    /// Refresh the cluster topology from a connected master.
    ///
    /// Self-healing: re-runs `CLUSTER SLOTS` (against the first node that
    /// answers, so a dead seed is skipped) and reconciles the per-node
    /// services against the result. New nodes get a service; a node whose
    /// worker has exited -- it gave up reconnecting to a dead address -- is
    /// rebuilt at the same address; a node absent from the new topology is
    /// pruned and drained. Live, still-present nodes are left untouched.
    pub async fn refresh_topology(&self) -> Result<(), RedisError> {
        let started = Instant::now();
        tracing::info!("cluster: topology refresh started");
        let result = self.refresh_topology_inner().await;
        let duration = started.elapsed();
        let outcome = match &result {
            Ok(stats) if stats.outcome() == ClusterTopologyRefreshOutcome::Partial => {
                tracing::warn!(
                    duration_ms = duration.as_secs_f64() * 1000.0,
                    outcome = "partial",
                    masters = stats.master_count,
                    replicas = stats.replica_count,
                    services_built = stats.services_built,
                    services_skipped = stats.services_skipped,
                    services_pruned = stats.services_pruned,
                    "cluster: topology refresh partially completed"
                );
                ClusterTopologyRefreshOutcome::Partial
            }
            Ok(stats) => {
                tracing::info!(
                    duration_ms = duration.as_secs_f64() * 1000.0,
                    outcome = "success",
                    masters = stats.master_count,
                    replicas = stats.replica_count,
                    services_built = stats.services_built,
                    services_skipped = stats.services_skipped,
                    services_pruned = stats.services_pruned,
                    "cluster: topology refresh completed"
                );
                ClusterTopologyRefreshOutcome::Success
            }
            Err(error) => {
                tracing::warn!(
                    duration_ms = duration.as_secs_f64() * 1000.0,
                    outcome = "error",
                    error = %error,
                    "cluster: topology refresh failed"
                );
                ClusterTopologyRefreshOutcome::Error
            }
        };
        self.record_topology_refresh(duration, outcome);
        result.map(|_| ())
    }

    async fn refresh_topology_inner(&self) -> Result<TopologyRefreshStats, RedisError> {
        // Snapshot what we need from the inner state, then release the lock
        // before doing network I/O.
        let (
            pipeline_config,
            reconnect_config,
            host_override,
            address_map,
            read_preference,
            credentials,
            connection_config,
        ) = {
            let inner = self.inner.read().await;
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
                inner.host_override.clone(),
                inner.address_map.clone(),
                inner.read_preference,
                inner.credentials.clone(),
                inner.connection_config.clone(),
            )
        };
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        let tls: Option<Arc<TlsConfig>> = self.inner.read().await.tls.clone();
        let connector = ClusterNodeConnector::new(
            connection_config,
            credentials,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls,
        );

        // Run CLUSTER SLOTS against the first node that answers. The previous
        // seed (`masters.keys().next()`) could be the node that just died, so
        // try every node we know about -- masters first, then replicas.
        let seeds: Vec<String> = {
            let inner = self.inner.read().await;
            inner
                .masters
                .keys()
                .chain(inner.replicas.keys())
                .cloned()
                .collect()
        };
        let mut discovered = None;
        let mut last_err = RedisError::ConnectionClosed;
        for seed in &seeds {
            match discover_from_seed(seed, &connector).await {
                Ok(t) => {
                    discovered = Some(t);
                    break;
                }
                Err(e) => {
                    self.observe_node_error(seed, &e);
                    tracing::debug!(seed, error = %e, "cluster: seed unreachable during topology refresh");
                    last_err = e;
                }
            }
        }
        let mut topology = discovered.ok_or(last_err)?;

        if let Some(ref map) = address_map {
            remap_topology_with_map(&mut topology, map);
        }
        if let Some(ref host) = host_override {
            remap_topology(&mut topology, host);
        }

        // Desired per-node addresses from the fresh topology.
        let master_desired: Vec<String> = topology
            .master_addrs()
            .iter()
            .map(|a| a.addr_string())
            .collect();
        let replica_desired: Vec<String> = if read_preference != ReadPreference::Master {
            topology
                .replica_addrs()
                .iter()
                .map(|a| a.addr_string())
                .collect()
        } else {
            Vec::new()
        };
        let master_count = master_desired.len();
        let replica_count = replica_desired.len();

        // Diff against current services and their liveness (read lock, no I/O).
        let (master_diff, replica_diff) = {
            let inner = self.inner.read().await;
            let master_live: HashMap<String, bool> = inner
                .masters
                .iter()
                .map(|(addr, svc)| (addr.clone(), svc.is_alive()))
                .collect();
            let replica_live: HashMap<String, bool> = inner
                .replicas
                .iter()
                .map(|(addr, svc)| (addr.clone(), svc.is_alive()))
                .collect();
            (
                diff_node_services(&master_desired, &master_live),
                diff_node_services(&replica_desired, &replica_live),
            )
        };

        // A dead/replaced master or a membership transition invalidates the
        // data-connection-local TRACKING state. Close the synchronous cache
        // gate as soon as refresh planning observes it, before network work.
        for addr in master_diff
            .to_build
            .iter()
            .chain(master_diff.to_prune.iter())
        {
            self.notify_cache_node_loss(addr);
        }

        // Build (re)placement services without holding the write lock, with the
        // handshakes overlapping (bounded by MAX_CONCURRENT_CONNECTS). A node
        // that is unreachable right now (e.g. a master still listed by CLUSTER
        // SLOTS mid-failover) is skipped, not fatal: committing the reachable
        // nodes and pruning departed ones still makes progress, and the next
        // refresh picks up the rest once it settles.
        let built_masters = build_node_services_best_effort(
            master_diff.to_build.clone(),
            false,
            pipeline_config.clone(),
            reconnect_config.clone(),
            connector.clone(),
        )
        .await;
        let built_replicas = build_node_services_best_effort(
            replica_diff.to_build.clone(),
            true,
            pipeline_config.clone(),
            reconnect_config.clone(),
            connector,
        )
        .await;
        let services_requested = master_diff.to_build.len() + replica_diff.to_build.len();
        let services_built = built_masters.len() + built_replicas.len();
        let services_skipped = services_requested.saturating_sub(services_built);
        let services_pruned = master_diff.to_prune.len() + replica_diff.to_prune.len();

        // Commit under the write lock; collect replaced/pruned services to drain
        // after the lock is released.
        let mut to_drain: Vec<AutoPipelineService> = Vec::new();
        {
            let mut inner = self.inner.write().await;
            let previous_topology = inner.topology.clone();
            let current_topology = topology;
            let change = inner
                .topology_changes
                .record(&previous_topology, &current_topology);
            let hooks = self.cache_hooks.read().unwrap().as_ref().cloned();
            if let (Some(hooks), Some(change)) = (&hooks, change) {
                // The old topology and old services are still committed here.
                hooks.topology_changing(change);
            }
            if let Some(hooks) = &hooks {
                // Re-close immediately before every master service swap. This
                // covers same-topology refreshes where the ownership diff is
                // empty but a replacement worker has no TRACKING state yet.
                for (addr, _) in &built_masters {
                    hooks.node_connection_lost(addr.clone());
                }
                for addr in &master_diff.to_prune {
                    hooks.node_connection_lost(addr.clone());
                }
            }
            inner.topology = current_topology;
            for (addr, svc) in built_masters {
                if let Some(old) = inner.masters.insert(addr, svc) {
                    to_drain.push(old);
                }
            }
            for (addr, svc) in built_replicas {
                if let Some(old) = inner.replicas.insert(addr, svc) {
                    to_drain.push(old);
                }
            }
            for addr in &master_diff.to_prune {
                if let Some(svc) = inner.masters.remove(addr) {
                    to_drain.push(svc);
                }
            }
            for addr in &replica_diff.to_prune {
                if let Some(svc) = inner.replicas.remove(addr) {
                    to_drain.push(svc);
                }
            }
        }

        // Drain replaced/pruned services outside the lock: an alive service
        // flushes its in-flight batch; a dead one returns immediately.
        for svc in to_drain {
            svc.shutdown().await;
        }
        Ok(TopologyRefreshStats {
            master_count,
            replica_count,
            services_built,
            services_skipped,
            services_pruned,
        })
    }

    /// Spawn a rate-limited, single-flight background topology refresh.
    ///
    /// Called when a node failure is observed (a connection error, or a MOVED
    /// during resharding). Returns immediately: the failing command still
    /// surfaces its error to the caller, while the refresh heals the topology
    /// -- replacing the dead node's service and pruning departed nodes -- so
    /// subsequent commands route to the live cluster instead of looping on the
    /// dead address forever. The [`RefreshGate`] collapses concurrent triggers
    /// into a single refresh.
    fn trigger_refresh(&self) {
        let Some(permit) = self.refresh_gate.try_begin() else {
            return;
        };
        let client = self.clone();
        tokio::spawn(async move {
            let guard = RefreshTaskGuard::new(client, permit);
            let _ = guard.client().refresh_topology().await;
        });
    }

    /// Get a snapshot of the current cluster topology.
    pub async fn topology(&self) -> ClusterTopology {
        self.inner.read().await.topology.clone()
    }

    /// Build the weak, dedicated-connection backend used by cluster Pub/Sub.
    pub(crate) async fn pubsub_backend(&self) -> ClusterPubSubBackend {
        let inner = self.inner.read().await;
        let connector = ClusterNodeConnector::new(
            inner.connection_config.clone(),
            inner.credentials.clone(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            inner.tls.clone(),
        );
        ClusterPubSubBackend {
            inner: Arc::downgrade(&self.inner),
            cache_hooks: Arc::downgrade(&self.cache_hooks),
            connector,
            reconnect: inner.reconnect_config.reconnect.clone(),
            max_redirects: inner.max_redirects,
        }
    }

    /// Snapshot the topology together with its monotonic ownership revision.
    #[allow(dead_code)] // Consumed by the stacked cluster-caching runtime.
    pub(crate) async fn topology_snapshot(&self) -> TopologySnapshot {
        let inner = self.inner.read().await;
        inner.topology_changes.snapshot(&inner.topology)
    }

    /// Subscribe to committed master-membership and slot-owner changes.
    #[allow(dead_code)] // Consumed by the stacked cluster-caching runtime.
    pub(crate) async fn subscribe_topology_changes(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<Arc<TopologyChange>>> {
        self.inner.read().await.topology_changes.subscribe()
    }

    /// Get the current read preference.
    pub async fn read_preference(&self) -> ReadPreference {
        self.inner.read().await.read_preference
    }

    /// Snapshot the addresses of every master node this client currently holds
    /// a live service for, in a stable (sorted) order.
    ///
    /// Taken from the service map rather than from the topology, so a master
    /// listed by `CLUSTER SLOTS` that has no service -- pruned, or skipped by a
    /// best-effort refresh -- is not reported as scannable.
    pub(crate) async fn master_service_addrs(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        let mut addrs: Vec<String> = inner.masters.keys().cloned().collect();
        // `masters` is a HashMap, so its iteration order varies run to run.
        // Sorting makes any per-node traversal reproducible.
        addrs.sort();
        addrs
    }

    /// Whether this client currently holds a master service for `addr`.
    ///
    /// Lets the cluster-wide scan tell a node that has left this client's master
    /// set -- pruned by a topology refresh, because `CLUSTER SLOTS` no longer
    /// lists it as owning any -- from a node that is present and failing. The
    /// first is a membership change to be skipped, the second a scan failure to
    /// be surfaced.
    pub(crate) async fn holds_master(&self, addr: &str) -> bool {
        self.inner.read().await.masters.contains_key(addr)
    }

    /// Execute a command against one specific master node, bypassing slot
    /// routing and the redirect loop.
    ///
    /// This exists for commands that are node-scoped rather than key-scoped:
    /// `SCAN` iterates the keyspace of the node it is sent to, so a cluster-wide
    /// scan has to address each master in turn. Slot routing cannot express
    /// that -- `SCAN` carries no key, so [`execute`](Self::execute) sends it to
    /// the default node and returns that one node's keys.
    ///
    /// No redirect handling: a MOVED reply surfaces as an error rather than
    /// being followed, because following it would defeat the point of pinning
    /// the command to a node.
    pub(crate) async fn execute_on_node<Cmd: Command>(
        &self,
        addr: &str,
        cmd: Cmd,
    ) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        let observation = self
            .metrics_recorder
            .as_ref()
            .map(|_| (cmd.name().to_ascii_uppercase(), Instant::now()));
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            if let Some((command, started)) = observation {
                self.record_command_completion(
                    &command,
                    started.elapsed(),
                    Some(ErrorKind::from_error(&RedisError::CommandTimeout)),
                    Some(addr),
                );
            }
            return Err(RedisError::CommandTimeout);
        }
        let operation = self.execute_on_node_inner(addr, cmd);
        let result = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, operation).await {
                Ok(result) => result,
                Err(_elapsed) => Err(RedisError::CommandTimeout),
            },
            None => operation.await,
        };
        if let Some((command, started)) = observation {
            let error = result.as_ref().err().map(ErrorKind::from_error);
            self.record_command_completion(&command, started.elapsed(), error, Some(addr));
        }
        result
    }

    async fn execute_on_node_inner<Cmd: Command>(
        &self,
        addr: &str,
        cmd: Cmd,
    ) -> Result<Cmd::Response, RedisError> {
        let mut target = self.master_service(addr).await?;
        let response = match call_service(&mut target.svc, cmd.to_frame()).await {
            Ok(r) => r,
            Err(e) => {
                self.observe_node_error(addr, &e);
                return Err(e);
            }
        };
        if let Frame::Error(ref e) = response {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }
        cmd.parse_response(response)
    }

    /// Gracefully shut down the cluster client, draining every per-node worker.
    ///
    /// Signals each master and replica [`AutoPipelineService`] to stop
    /// accepting new requests, then waits for their in-flight batches to flush
    /// and the background workers to exit. This is the cluster analogue of
    /// [`MultiplexedClient::shutdown`](redis_tower::MultiplexedClient::shutdown)
    /// and the SIGTERM drain path for a cluster deployment: without it,
    /// dropping the client abandons the per-node workers and any requests still
    /// queued in them are silently dropped.
    ///
    /// Only the last live clone drains the workers. If other clones of this
    /// client are still alive (they share one worker set through an `Arc`),
    /// this returns immediately and the workers keep running until the final
    /// clone shuts down or is dropped -- mirroring
    /// [`MultiplexedClient::shutdown`](redis_tower::MultiplexedClient::shutdown).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower_cluster::MultiplexedClusterClient;
    ///
    /// let cluster = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
    ///
    /// // On SIGTERM: stop accepting new work, then drain the cluster client.
    /// // Wire the wait to your runtime's signal handler, such as
    /// // `tokio::signal::ctrl_c` (tokio's `signal` feature).
    /// cluster.shutdown().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self) {
        // A background refresh owns an internal client clone. Its guard drops
        // that clone before publishing idle, so wait before deciding whether
        // any remaining Arc owner is a real public clone.
        self.refresh_gate.wait_until_idle().await;
        // Only the last clone owns the workers outright; earlier clones return
        // immediately so a shutdown from one task does not strand the others.
        if Arc::strong_count(&self.inner) > 1 {
            return;
        }
        // Take the per-node services out from under the lock, then drain them
        // outside it. An alive service flushes its in-flight batch and joins
        // its worker; a dead one returns immediately.
        let (masters, replicas) = {
            let mut inner = self.inner.write().await;
            (
                std::mem::take(&mut inner.masters),
                std::mem::take(&mut inner.replicas),
            )
        };
        for svc in masters.into_values().chain(replicas.into_values()) {
            svc.shutdown().await;
        }
    }

    // -- internals --

    /// Resolve the command to a target service, honoring read preference.
    async fn route_command(&self, frame: &Frame) -> Result<Target, RedisError> {
        let inner = self.inner.read().await;

        if let Some(key) = key_extractor::extract_key(frame) {
            let slot = slot_for_key(key);

            // Read-only commands with replica preference: try a replica first.
            if inner.read_preference != ReadPreference::Master
                && key_extractor::is_readonly_command(frame)
            {
                if let Some(addr) = pick_replica(&inner, slot)
                    && let Some(svc) = inner.replicas.get(&addr)
                    && replica_service_is_usable(svc)
                {
                    let svc = svc.clone();
                    return Ok(Target { svc, addr });
                }
                if inner.read_preference == ReadPreference::Replica {
                    return Err(replica_unavailable(slot));
                }
                // PreferReplica falls through to master.
            }

            if let Some(addr_node) = inner.topology.master_for_slot(slot) {
                let addr_str = addr_node.addr_string();
                if let Some(svc) = inner.masters.get(&addr_str) {
                    return Ok(Target {
                        svc: svc.clone(),
                        addr: addr_str,
                    });
                }
            }
        }

        // Keyless command or no route: fall back to default node.
        let default = inner.default_node.clone();
        let svc = inner
            .masters
            .get(&default)
            .cloned()
            .ok_or(RedisError::ConnectionClosed)?;
        Ok(Target { svc, addr: default })
    }

    async fn master_service(&self, addr: &str) -> Result<Target, RedisError> {
        let inner = self.inner.read().await;
        let svc = inner
            .masters
            .get(addr)
            .cloned()
            .ok_or_else(|| RedisError::Redis(format!("no service for node {addr}")))?;
        Ok(Target {
            svc,
            addr: addr.to_string(),
        })
    }

    async fn ensure_master(&self, addr: &str) -> Result<(), RedisError> {
        {
            let inner = self.inner.read().await;
            if inner.masters.contains_key(addr) {
                return Ok(());
            }
        }
        // Build the new service without holding any lock across connect.
        let (pipeline_config, reconnect_config, credentials, connection_config) = {
            let inner = self.inner.read().await;
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
                inner.credentials.clone(),
                inner.connection_config.clone(),
            )
        };
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        let tls: Option<Arc<TlsConfig>> = self.inner.read().await.tls.clone();
        let connector = ClusterNodeConnector::new(
            connection_config,
            credentials,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls,
        );
        let svc =
            match build_node_service(addr, false, pipeline_config, reconnect_config, connector)
                .await
            {
                Ok(service) => service,
                Err(error) => {
                    self.observe_node_error(addr, &error);
                    return Err(error);
                }
            };
        let mut inner = self.inner.write().await;
        inner.masters.entry(addr.to_string()).or_insert(svc);
        Ok(())
    }

    /// Patch a single slot's owner after a MOVED, splitting its containing
    /// range so the rest of the range keeps its owner. See
    /// [`ClusterTopology::reassign_slot`].
    async fn update_slot_owner(&self, slot: u16, addr: &str) {
        if let Some((host, port_str)) = addr.rsplit_once(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            let mut inner = self.inner.write().await;
            let previous_topology = inner.topology.clone();
            let mut current_topology = previous_topology.clone();
            current_topology.reassign_slot(
                slot,
                NodeAddr {
                    host: host.to_string(),
                    port,
                },
            );
            let change = inner
                .topology_changes
                .record(&previous_topology, &current_topology);
            if let Some(change) = change
                && let Some(hooks) = self.cache_hooks.read().unwrap().as_ref().cloned()
            {
                // Close cache use while the old owner is still committed.
                hooks.topology_changing(change);
            }
            inner.topology = current_topology;
        }
    }

    async fn remap_addr(&self, addr: &str) -> String {
        let inner = self.inner.read().await;
        if let Some(ref map) = inner.address_map
            && let Some(mapped) = map.get(addr)
        {
            return mapped.clone();
        }
        if let Some(ref host) = inner.host_override
            && let Some((_old_host, port)) = addr.rsplit_once(':')
        {
            return format!("{host}:{port}");
        }
        addr.to_string()
    }
}

fn cache_error_requires_node_loss(error: &RedisError) -> bool {
    !matches!(
        error,
        RedisError::QueueFull | RedisError::CircuitOpen | RedisError::PoolAcquisitionTimeout { .. }
    )
}

/// Cluster-aware implementation of redis-tower's generic pipeline executor.
///
/// Commands are validated, master-pinned from one topology snapshot, and
/// grouped by concrete node. Submission order is preserved within a node;
/// different node batches run concurrently and have no total execution order.
/// Raw responses are restored to submission order. Only entries that return
/// MOVED or ASK are sent again; a successful command is never replayed because
/// a neighbor in its batch was redirected. Redirect replay starts after the
/// original batches complete, so migration can weaken same-slot execution
/// order when an earlier entry redirects but a later one succeeds.
/// Cancellation after dispatch is ambiguous because some node batches may
/// already have executed.
impl PipelineExecutor for MultiplexedClusterClient {
    fn execute_pipeline(
        &mut self,
        frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Vec<Frame>, RedisError>> + Send {
        let client = self.clone();
        async move { client.execute_cluster_pipeline(frames).await }
    }
}

struct ClusterCacheCommand {
    request: ClusterCacheRequest,
}

impl Command for ClusterCacheCommand {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        self.request.frame.clone()
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        &self.request.command_name
    }

    fn idempotent(&self) -> bool {
        self.request.idempotent
    }

    fn is_blocking(&self) -> bool {
        self.request.is_blocking
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.request.deadline
    }
}

impl ClusterCacheBackend for MultiplexedClusterClient {
    fn cache_node_snapshot(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ClusterCacheNodeSnapshot, RedisError>> + Send + '_>>
    {
        Box::pin(async move {
            let inner = self.inner.read().await;
            let connector = ClusterNodeConnector::new(
                inner.connection_config.clone(),
                inner.credentials.clone(),
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                inner.tls.clone(),
            );
            let masters = dedup_addrs(inner.topology.master_addrs())
                .into_iter()
                .map(|addr| ClusterCacheMaster {
                    data_health: inner
                        .masters
                        .get(&addr)
                        .map(AutoPipelineService::subscribe_connection_health),
                    connector: connector.clone(),
                    addr,
                })
                .collect();
            Ok(ClusterCacheNodeSnapshot {
                revision: inner.topology_changes.revision(),
                masters,
            })
        })
    }

    fn install_cache_hooks(
        &self,
        hooks: ClusterCacheHooks,
    ) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send + '_>> {
        Box::pin(async move {
            // Serialize installation with topology commits so the following
            // atomic snapshot cannot miss a transition between the two steps.
            let _inner = self.inner.write().await;
            *self.cache_hooks.write().unwrap() = Some(hooks);
            Ok(())
        })
    }

    fn execute_cache_request(
        &self,
        request: ClusterCacheRequest,
        mode: CacheDispatchMode,
    ) -> Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send + '_>> {
        Box::pin(async move {
            self.execute_with_dispatch(ClusterCacheCommand { request }, mode)
                .await
        })
    }

    fn execute_cache_node_pipeline(
        &self,
        addr: &str,
        frames: Vec<Frame>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Frame>, RedisError>> + Send + '_>> {
        let addr = addr.to_string();
        Box::pin(async move {
            let mut target = self.master_service(&addr).await?;
            let result = target.svc.call_pipeline(frames).await;
            if let Err(error) = &result {
                self.observe_node_error(&addr, error);
            }
            result
        })
    }

    fn execute_cache_pipeline(
        &self,
        frames: Vec<Frame>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Frame>, RedisError>> + Send + '_>> {
        Box::pin(async move { self.execute_cluster_pipeline(frames).await })
    }
}

struct Target {
    svc: AutoPipelineService,
    addr: String,
}

fn parse_prefixed_response(
    responses: Vec<Frame>,
    asking: bool,
    caching: bool,
) -> Result<Frame, RedisError> {
    let mut responses = responses.into_iter();
    if asking {
        parse_setup_ok(
            responses.next().ok_or(RedisError::ConnectionClosed)?,
            "ASKING OK response",
        )?;
    }
    if caching {
        let command = ClientCaching::new(true);
        let response = responses.next().ok_or(RedisError::ConnectionClosed)?;
        if let Frame::Error(error) = response {
            return Err(RedisError::Redis(
                String::from_utf8_lossy(&error).into_owned(),
            ));
        }
        command.parse_response(response)?;
    }
    responses.next().ok_or(RedisError::ConnectionClosed)
}

fn parse_setup_ok(frame: Frame, expected: &'static str) -> Result<(), RedisError> {
    match frame {
        Frame::SimpleString(value) if value.as_ref() == b"OK" => Ok(()),
        Frame::Error(error) => Err(RedisError::Redis(
            String::from_utf8_lossy(&error).into_owned(),
        )),
        other => Err(RedisError::UnexpectedResponse {
            expected,
            actual: format!("{other:?}"),
        }),
    }
}

fn pick_replica(inner: &Inner, slot: u16) -> Option<String> {
    let replicas = inner.topology.replicas_for_slot(slot).unwrap_or(&[]);
    let is_usable = |replica: &NodeAddr| {
        inner
            .replicas
            .get(&replica.addr_string())
            .is_some_and(replica_service_is_usable)
    };
    let available = if replicas.iter().all(&is_usable) {
        Cow::Borrowed(replicas)
    } else {
        // A best-effort topology refresh can retain an advertised replica
        // whose worker is absent, dead, or reconnecting. Never expose those
        // unusable entries to a caller-provided routing strategy.
        Cow::Owned(
            replicas
                .iter()
                .filter(|replica| is_usable(replica))
                .cloned()
                .collect(),
        )
    };
    inner
        .read_routing
        .select_replica(slot, &available)
        .map(NodeAddr::addr_string)
}

fn replica_service_is_usable(service: &AutoPipelineService) -> bool {
    service.is_alive() && service.is_connection_healthy()
}

/// Counts reported when a topology refresh completes successfully.
#[derive(Debug, PartialEq, Eq)]
struct TopologyRefreshStats {
    master_count: usize,
    replica_count: usize,
    services_built: usize,
    services_skipped: usize,
    services_pruned: usize,
}

impl TopologyRefreshStats {
    fn outcome(&self) -> ClusterTopologyRefreshOutcome {
        if self.services_skipped == 0 {
            ClusterTopologyRefreshOutcome::Success
        } else {
            ClusterTopologyRefreshOutcome::Partial
        }
    }
}

/// How a per-node service map should change to match a freshly discovered
/// topology.
#[derive(Debug, Default, PartialEq, Eq)]
struct ServiceDiff {
    /// Addresses needing a freshly built service: a new node, or one whose
    /// worker has exited (gave up reconnecting to a dead address).
    to_build: Vec<String>,
    /// Addresses present now but absent from the new topology -- drain and drop.
    to_prune: Vec<String>,
}

/// Compute the [`ServiceDiff`] for a set of desired node addresses against the
/// current services, keyed by address with their liveness (`is_alive`).
///
/// A desired address is (re)built when it is absent or its current worker is
/// dead; an alive desired address is kept. A current address absent from the
/// desired set is pruned. Pure so the self-heal policy is unit-testable without
/// a live cluster.
fn diff_node_services(desired: &[String], current: &HashMap<String, bool>) -> ServiceDiff {
    let desired_set: HashSet<&str> = desired.iter().map(String::as_str).collect();

    let mut to_build = Vec::new();
    for addr in desired {
        // Build when absent or dead; an alive entry (`Some(true)`) is kept.
        if current.get(addr).copied() != Some(true) {
            to_build.push(addr.clone());
        }
    }

    let mut to_prune = Vec::new();
    for addr in current.keys() {
        if !desired_set.contains(addr.as_str()) {
            to_prune.push(addr.clone());
        }
    }

    ServiceDiff { to_build, to_prune }
}

/// Connect to a seed node, authenticate if needed, and run `CLUSTER SLOTS`.
///
/// Used by [`MultiplexedClusterClient::refresh_topology`] to try each known
/// node in turn until one answers, so a refresh survives the seed itself
/// having died.
async fn discover_from_seed(
    seed_addr: &str,
    connector: &ClusterNodeConnector,
) -> Result<ClusterTopology, RedisError> {
    let mut conn = connector.connect(seed_addr, false).await?;
    discover_topology(&mut conn).await
}

/// Collect node addresses as strings, dropping duplicates and preserving
/// first-seen order.
///
/// A master that owns several slot ranges appears once per range in
/// `CLUSTER SLOTS`, and one service per node is enough. Order is preserved so
/// the caller can take the first entry as the default node deterministically.
fn dedup_addrs(addrs: Vec<&NodeAddr>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let addr_str = addr.addr_string();
        if seen.insert(addr_str.clone()) {
            out.push(addr_str);
        }
    }
    out
}

/// Build one [`AutoPipelineService`] per address, running up to
/// [`MAX_CONCURRENT_CONNECTS`] handshakes at a time.
///
/// Fails on the first node that fails to connect; the remaining in-flight
/// connects are dropped. Because the connects overlap, the error surfaced when
/// several nodes are unreachable is whichever failed first in time, not
/// whichever comes first in `addrs`.
async fn build_node_services(
    addrs: Vec<String>,
    readonly: bool,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    connector: ClusterNodeConnector,
) -> Result<HashMap<String, AutoPipelineService>, RedisError> {
    futures::stream::iter(addrs.into_iter().map(|addr| {
        let pipeline_config = pipeline_config.clone();
        let reconnect_config = reconnect_config.clone();
        let connector = connector.clone();
        async move {
            let svc = build_node_service(
                &addr,
                readonly,
                pipeline_config,
                reconnect_config,
                connector,
            )
            .await?;
            Ok((addr, svc))
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_CONNECTS)
    .try_collect()
    .await
}

/// Build one [`AutoPipelineService`] per address with the same bounded fan-out
/// as [`build_node_services`], but keeping only the nodes that answered.
///
/// This is the topology-refresh counterpart: an address that fails to connect
/// is logged at debug and dropped instead of failing the batch. A node can be
/// legitimately unreachable mid-failover while still listed by `CLUSTER SLOTS`,
/// and committing the reachable nodes makes progress that the next refresh
/// builds on. Startup ([`build_node_services`]) wants the opposite -- a node it
/// cannot reach is a failed connect -- so the two cannot share one helper.
///
/// The returned pairs are in completion order, not `addrs` order. Callers must
/// not derive anything order-dependent from them.
async fn build_node_services_best_effort(
    addrs: Vec<String>,
    readonly: bool,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    connector: ClusterNodeConnector,
) -> Vec<(String, AutoPipelineService)> {
    let role = if readonly { "replica" } else { "master" };
    futures::stream::iter(addrs.into_iter().map(|addr| {
        let pipeline_config = pipeline_config.clone();
        let reconnect_config = reconnect_config.clone();
        let connector = connector.clone();
        async move {
            match build_node_service(
                &addr,
                readonly,
                pipeline_config,
                reconnect_config,
                connector,
            )
            .await
            {
                Ok(svc) => Some((addr, svc)),
                Err(e) => {
                    tracing::debug!(
                        addr,
                        role,
                        error = %e,
                        "cluster: node unreachable during refresh; skipping"
                    );
                    None
                }
            }
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_CONNECTS)
    .filter_map(|built| async move { built })
    .collect()
    .await
}

async fn build_node_service(
    addr: &str,
    readonly: bool,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    connector: ClusterNodeConnector,
) -> Result<AutoPipelineService, RedisError> {
    let factory = NodeConnectionFactory {
        addr: addr.to_string(),
        readonly,
        connector,
    };
    AutoPipelineService::with_factory(factory, pipeline_config, reconnect_config).await
}

/// A [`ConnectionFactory`] that connects to a single node and optionally
/// authenticates and/or sends READONLY before yielding the connection.
///
/// Order on each (re)connect:
/// 1. Open TCP (or TLS if configured) to `addr`.
/// 2. If `credentials` is set, fetch fresh credentials from the provider
///    and send AUTH. Fetching on every reconnect means credential rotation
///    flows through automatically.
/// 3. Negotiate the configured RESP protocol.
/// 4. If `readonly` is set (replica node), send READONLY so reads to this
///    connection succeed.
pub(crate) struct NodeConnectionFactory {
    addr: String,
    readonly: bool,
    connector: ClusterNodeConnector,
}

impl ConnectionFactory for NodeConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        let readonly = self.readonly;
        let connector = self.connector.clone();
        Box::pin(async move { connector.connect(&addr, readonly).await })
    }
}

/// Fetch credentials from the provider and send AUTH on the given connection.
/// Send a single frame through an [`AutoPipelineService`] and await the
/// response. Mirrors what `MultiplexedClient::execute` does internally, but
/// stays at the frame level so the cluster routing code can reuse the same
/// service across redirects without needing `Command: Clone`.
async fn call_service(svc: &mut AutoPipelineService, frame: Frame) -> Result<Frame, RedisError> {
    std::future::poll_fn(|cx| <AutoPipelineService as Service<Frame>>::poll_ready(svc, cx)).await?;
    <AutoPipelineService as Service<Frame>>::call(svc, frame).await
}

// Tower `Service<Cmd>` impl. `execute` takes `&self` because multiple tasks
// share one client via `Clone`, so we bridge to the `&mut self` Service API
// by cloning the client into the call future. poll_ready is always Ready:
// per-node worker readiness is implicit (the worker owns the connection and
// the client's channels are bounded).
impl<Cmd: Command + 'static> tower_service::Service<Cmd> for MultiplexedClusterClient {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = std::pin::Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.execute(cmd).await })
    }
}

impl std::fmt::Debug for MultiplexedClusterClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexedClusterClient").finish()
    }
}

/// `MultiplexedClusterClient` is a [`RedisExecutor`], so it composes with
/// generic code (and [`ConnectionPool`](redis_tower::ConnectionPool)) that
/// accepts `impl RedisExecutor` rather than a concrete client type. `execute`
/// already takes `&self`; the trait's `&mut self` contract is satisfied
/// trivially.
impl RedisExecutor for MultiplexedClusterClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        MultiplexedClusterClient::execute(self, cmd)
    }
}

#[cfg(test)]
mod redis_executor_tests {
    use super::*;

    fn assert_redis_executor<T: RedisExecutor>() {}

    #[test]
    fn cluster_client_implements_redis_executor() {
        assert_redis_executor::<MultiplexedClusterClient>();
    }
}

#[cfg(test)]
mod diff_tests {
    use super::*;

    fn current(entries: &[(&str, bool)]) -> HashMap<String, bool> {
        entries
            .iter()
            .map(|(a, alive)| (a.to_string(), *alive))
            .collect()
    }

    fn desired(addrs: &[&str]) -> Vec<String> {
        addrs.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn builds_new_nodes_and_keeps_alive_ones() {
        let diff = diff_node_services(&desired(&["a", "b"]), &current(&[("a", true)]));
        assert_eq!(diff.to_build, vec!["b".to_string()]); // a alive -> kept
        assert!(diff.to_prune.is_empty());
    }

    #[test]
    fn rebuilds_dead_service_at_unchanged_address() {
        // The kill-a-master case: address unchanged, but its worker exited.
        let diff = diff_node_services(&desired(&["a"]), &current(&[("a", false)]));
        assert_eq!(diff.to_build, vec!["a".to_string()]);
        assert!(diff.to_prune.is_empty());
    }

    #[test]
    fn prunes_departed_nodes() {
        let mut diff = diff_node_services(
            &desired(&["a"]),
            &current(&[("a", true), ("gone", true), ("gone2", false)]),
        );
        assert!(diff.to_build.is_empty());
        diff.to_prune.sort();
        assert_eq!(diff.to_prune, vec!["gone".to_string(), "gone2".to_string()]);
    }

    #[test]
    fn empty_desired_prunes_everything() {
        let diff = diff_node_services(&[], &current(&[("a", true), ("b", false)]));
        assert!(diff.to_build.is_empty());
        assert_eq!(diff.to_prune.len(), 2);
    }

    #[test]
    fn fresh_topology_builds_all() {
        let diff = diff_node_services(&desired(&["a", "b", "c"]), &HashMap::new());
        assert_eq!(diff.to_build.len(), 3);
        assert!(diff.to_prune.is_empty());
    }

    #[test]
    fn skipped_node_builds_make_a_refresh_partial() {
        let complete = TopologyRefreshStats {
            master_count: 3,
            replica_count: 0,
            services_built: 1,
            services_skipped: 0,
            services_pruned: 0,
        };
        assert_eq!(complete.outcome(), ClusterTopologyRefreshOutcome::Success);

        let partial = TopologyRefreshStats {
            services_skipped: 1,
            ..complete
        };
        assert_eq!(partial.outcome(), ClusterTopologyRefreshOutcome::Partial);
    }

    #[test]
    fn refresh_gate_single_flights() {
        let gate = Arc::new(RefreshGate::new(Duration::from_millis(0)));
        let permit = gate.try_begin().expect("first caller claims the refresh");
        assert!(
            gate.try_begin().is_none(),
            "second is denied while one is in flight"
        );
        drop(permit);
        assert!(
            gate.try_begin().is_some(),
            "after finish (no rate limit) a new one starts"
        );
    }

    #[test]
    fn refresh_gate_rate_limits() {
        let gate = Arc::new(RefreshGate::new(Duration::from_secs(60)));
        let permit = gate.try_begin().unwrap();
        drop(permit);
        // Not in flight, but within the min interval -> denied.
        assert!(gate.try_begin().is_none());
    }

    #[tokio::test]
    async fn refresh_gate_rejected_claim_leaves_idle_waiters_released() {
        let gate = Arc::new(RefreshGate::new(Duration::from_secs(60)));
        drop(gate.try_begin().unwrap());
        assert!(gate.try_begin().is_none());

        tokio::time::timeout(Duration::from_millis(50), gate.wait_until_idle())
            .await
            .expect("rate-limited claim left the refresh gate stuck in flight");
    }
}

#[cfg(test)]
mod parallel_connect_tests {
    use super::*;
    use futures::SinkExt;
    use redis_tower::credentials::Credentials;
    use redis_tower_core::RedisStream;
    use redis_tower_protocol::RespCodec;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::Framed;

    /// How long each fake node stalls before answering its first command.
    /// Long enough that serialized connects cannot overlap by accident.
    const HANDSHAKE_STALL: Duration = Duration::from_millis(250);

    fn connector(config: ConnectionConfig) -> ClusterNodeConnector {
        ClusterNodeConnector::new(
            config,
            None,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
    }

    fn command_parts(frame: Frame) -> Vec<bytes::Bytes> {
        let Frame::Array(Some(parts)) = frame else {
            panic!("expected command array, got {frame:?}");
        };
        parts
            .into_iter()
            .map(|part| match part {
                Frame::BulkString(Some(value)) => value,
                other => panic!("expected bulk-string command part, got {other:?}"),
            })
            .collect()
    }

    #[derive(Default)]
    struct ConcurrencyProbe {
        in_handshake: AtomicUsize,
        peak: AtomicUsize,
    }

    impl ConcurrencyProbe {
        fn enter(&self) {
            let now = self.in_handshake.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
        }

        fn leave(&self) {
            self.in_handshake.fetch_sub(1, Ordering::SeqCst);
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }
    }

    /// Spawn a fake node that answers every command with an error frame.
    ///
    /// `RedisConnection::connect` opens with CLIENT SETINFO twice and HELLO 3,
    /// all of which tolerate an error reply (SETINFO is best-effort and a
    /// failed HELLO is the RESP2 fallback), so `-ERR` is enough for the
    /// connect to succeed without implementing Redis.
    ///
    /// Each accepted connection counts itself as in-handshake for
    /// [`HANDSHAKE_STALL`] before it replies to anything, so the probe's peak
    /// is the number of connects that were genuinely in flight at once.
    async fn spawn_fake_node(probe: Arc<ConcurrencyProbe>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let probe = probe.clone();
                tokio::spawn(async move {
                    probe.enter();
                    tokio::time::sleep(HANDSHAKE_STALL).await;
                    probe.leave();
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 || sock.write_all(b"-ERR fake node\r\n").await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    async fn build_against_fakes(count: usize) -> (HashMap<String, AutoPipelineService>, usize) {
        let probe = Arc::new(ConcurrencyProbe::default());
        let mut addrs = Vec::with_capacity(count);
        for _ in 0..count {
            addrs.push(spawn_fake_node(probe.clone()).await);
        }
        let services = build_node_services(
            addrs,
            false,
            AutoPipelineConfig::default(),
            default_node_reconnect(),
            connector(ConnectionConfig::default()),
        )
        .await
        .expect("every fake node should connect");
        (services, probe.peak())
    }

    #[tokio::test]
    async fn node_factory_applies_resp_limits_to_each_connection() {
        let addr = spawn_fake_node(Arc::new(ConcurrencyProbe::default())).await;
        let limits = RespLimits {
            max_frame_size: 4096,
            max_depth: 9,
        };
        let factory = NodeConnectionFactory {
            addr,
            readonly: false,
            connector: connector(ConnectionConfig::new().with_resp_limits(limits)),
        };

        let conn = factory.connect().await.expect("fake node should connect");
        let framed = conn.into_framed().expect("connection should be idle");
        assert_eq!(framed.codec().limits(), limits);
    }

    #[tokio::test]
    async fn node_factory_refetches_credentials_and_negotiates_after_auth() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let fetches = Arc::new(AtomicUsize::new(0));
        let provider_fetches = Arc::clone(&fetches);
        let provider = move || {
            let generation = provider_fetches.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(Credentials::new("alice", format!("secret-{generation}"))) }
        };
        let factory = NodeConnectionFactory {
            addr,
            readonly: false,
            connector: ClusterNodeConnector::new(
                ConnectionConfig::new().with_protocol(ProtocolVersion::Resp3),
                Some(Arc::new(provider)),
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                None,
            ),
        };

        let server_task = tokio::spawn(async move {
            for generation in 1..=2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut framed = Framed::new(RedisStream::Tcp(socket), RespCodec::new());

                for _ in 0..2 {
                    let command = command_parts(framed.next().await.unwrap().unwrap());
                    assert_eq!(command[0].as_ref(), b"CLIENT");
                    assert_eq!(command[1].as_ref(), b"SETINFO");
                    framed
                        .send(Frame::SimpleString(b"OK"[..].into()))
                        .await
                        .unwrap();
                }

                let auth = command_parts(framed.next().await.unwrap().unwrap());
                assert_eq!(auth[0].as_ref(), b"AUTH");
                assert_eq!(auth[1].as_ref(), b"alice");
                assert_eq!(auth[2].as_ref(), format!("secret-{generation}").as_bytes());
                framed
                    .send(Frame::SimpleString(b"OK"[..].into()))
                    .await
                    .unwrap();

                let hello = command_parts(framed.next().await.unwrap().unwrap());
                assert_eq!(hello[0].as_ref(), b"HELLO");
                assert_eq!(hello[1].as_ref(), b"3");
                framed
                    .send(Frame::SimpleString(b"OK"[..].into()))
                    .await
                    .unwrap();
            }
        });

        let first = factory.connect().await.unwrap();
        assert!(first.is_resp3());
        drop(first);
        let second = factory.connect().await.unwrap();
        assert!(second.is_resp3());
        drop(second);

        server_task.await.unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    /// The regression #458 guards: node connects must overlap. Serialized
    /// connects can only ever have one handshake in flight, so a peak above
    /// one proves the fan-out, and the cap proves it stays bounded.
    #[tokio::test(flavor = "multi_thread")]
    async fn node_connects_run_concurrently_within_the_bound() {
        let node_count = MAX_CONCURRENT_CONNECTS + 4;
        let (services, peak) = build_against_fakes(node_count).await;

        assert_eq!(
            services.len(),
            node_count,
            "every node should get a service"
        );
        assert!(
            peak > 1,
            "connects were serialized: peak in-flight handshakes was {peak}"
        );
        assert!(
            peak <= MAX_CONCURRENT_CONNECTS,
            "fan-out exceeded the bound: peak {peak} > {MAX_CONCURRENT_CONNECTS}"
        );
    }

    /// Serialized connects would cost `node_count * HANDSHAKE_STALL`; the
    /// bounded fan-out costs one stall per wave. Generous margin so the
    /// assertion is about the shape of the work, not CI timing noise.
    #[tokio::test(flavor = "multi_thread")]
    async fn connecting_many_nodes_beats_the_serial_cost() {
        let node_count = 8;
        let started = Instant::now();
        let (services, _) = build_against_fakes(node_count).await;
        let elapsed = started.elapsed();

        assert_eq!(services.len(), node_count);
        let serial_cost = HANDSHAKE_STALL * node_count as u32;
        assert!(
            elapsed < serial_cost / 2,
            "took {elapsed:?}, which is not meaningfully better than the \
             serial cost of {serial_cost:?}"
        );
    }

    /// An address nothing is listening on. Port 1 on the loopback is refused
    /// immediately, so this is a fast connect failure rather than a timeout.
    const UNREACHABLE: &str = "127.0.0.1:1";

    async fn build_best_effort_against_fakes(
        reachable: usize,
        unreachable: usize,
    ) -> (Vec<(String, AutoPipelineService)>, Vec<String>, usize) {
        let probe = Arc::new(ConcurrencyProbe::default());
        let mut addrs = Vec::with_capacity(reachable + unreachable);
        for _ in 0..reachable {
            addrs.push(spawn_fake_node(probe.clone()).await);
        }
        let live: Vec<String> = addrs.clone();
        for _ in 0..unreachable {
            addrs.push(UNREACHABLE.to_string());
        }
        let built = build_node_services_best_effort(
            addrs,
            false,
            AutoPipelineConfig::default(),
            default_node_reconnect(),
            connector(ConnectionConfig::default()),
        )
        .await;
        (built, live, probe.peak())
    }

    /// The refresh-path half of the #458 fan-out (#631). Serialized connects
    /// can only ever have one handshake in flight, so a peak above one proves
    /// the overlap and the cap proves it stays bounded.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_node_connects_run_concurrently_within_the_bound() {
        let node_count = MAX_CONCURRENT_CONNECTS + 4;
        let (built, _, peak) = build_best_effort_against_fakes(node_count, 0).await;

        assert_eq!(built.len(), node_count, "every node should get a service");
        assert!(
            peak > 1,
            "connects were serialized: peak in-flight handshakes was {peak}"
        );
        assert!(
            peak <= MAX_CONCURRENT_CONNECTS,
            "fan-out exceeded the bound: peak {peak} > {MAX_CONCURRENT_CONNECTS}"
        );
    }

    /// Refresh is best-effort, unlike startup: a node that will not answer
    /// right now (a master still listed by CLUSTER SLOTS mid-failover) must be
    /// dropped from the batch, not fail it. This is what a naive swap to
    /// `build_node_services`, which is fail-fast, would break.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_skips_unreachable_nodes_instead_of_failing() {
        let (built, live, _) = build_best_effort_against_fakes(3, 2).await;

        let mut got: Vec<String> = built.into_iter().map(|(addr, _)| addr).collect();
        got.sort();
        let mut want = live;
        want.sort();
        assert_eq!(
            got, want,
            "the reachable nodes should all be built, and only those"
        );
    }

    /// The fail-fast sibling still fails fast: the same unreachable address
    /// that refresh skips must abort a startup build.
    #[tokio::test(flavor = "multi_thread")]
    async fn startup_build_still_fails_on_an_unreachable_node() {
        let probe = Arc::new(ConcurrencyProbe::default());
        let addrs = vec![
            spawn_fake_node(probe.clone()).await,
            UNREACHABLE.to_string(),
        ];
        let result = build_node_services(
            addrs,
            false,
            AutoPipelineConfig::default(),
            default_node_reconnect(),
            connector(ConnectionConfig::default()),
        )
        .await;

        assert!(
            result.is_err(),
            "startup must not silently drop a node it cannot reach"
        );
    }

    #[test]
    fn dedup_addrs_keeps_first_seen_order() {
        let node = |host: &str| NodeAddr {
            host: host.to_string(),
            port: 6379,
        };
        let nodes = [
            node("10.0.0.2"),
            node("10.0.0.1"),
            // Same master, second slot range -- one service is enough.
            node("10.0.0.2"),
        ];
        let deduped = dedup_addrs(nodes.iter().collect());
        assert_eq!(
            deduped,
            vec!["10.0.0.2:6379".to_string(), "10.0.0.1:6379".to_string()],
            "duplicates dropped, first-seen order preserved so the default \
             node stays deterministic"
        );
    }
}

#[cfg(test)]
mod observability_tests {
    use super::*;
    use bytes::Bytes;
    use futures::SinkExt;
    use redis_tower::metrics_layer::{ClusterRedirectKind, ErrorKind, MetricsRecorder};
    use redis_tower_commands::{Get, SPublish, Set};
    use redis_tower_core::WithDeadline;
    use redis_tower_protocol::RespCodec;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::Framed;

    #[derive(Debug, PartialEq, Eq)]
    struct CommandCompletion {
        command: String,
        error: Option<ErrorKind>,
        node: Option<String>,
    }

    struct NameProbeCommand {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Command for NameProbeCommand {
        type Response = Frame;

        fn to_frame(&self) -> Frame {
            array(vec![bulk("GET"), bulk("key")])
        }

        fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
            Ok(frame)
        }

        fn name(&self) -> &str {
            self.calls.fetch_add(1, Ordering::Relaxed);
            "GET"
        }
    }

    #[derive(Default)]
    struct RecordingMetrics {
        completions: Mutex<Vec<CommandCompletion>>,
        redirects: Mutex<Vec<ClusterRedirectKind>>,
        refreshes: Mutex<Vec<(Duration, ClusterTopologyRefreshOutcome)>>,
    }

    impl MetricsRecorder for RecordingMetrics {
        fn command_completed(&self, command: &str, _duration: Duration, error: Option<ErrorKind>) {
            self.completions.lock().unwrap().push(CommandCompletion {
                command: command.to_string(),
                error,
                node: None,
            });
        }

        fn command_completed_on_node(
            &self,
            command: &str,
            _duration: Duration,
            error: Option<ErrorKind>,
            node: Option<&str>,
        ) {
            self.completions.lock().unwrap().push(CommandCompletion {
                command: command.to_string(),
                error,
                node: node.map(str::to_owned),
            });
        }

        fn cluster_redirected(&self, kind: ClusterRedirectKind) {
            self.redirects.lock().unwrap().push(kind);
        }

        fn cluster_topology_refresh_completed(
            &self,
            duration: Duration,
            outcome: ClusterTopologyRefreshOutcome,
        ) {
            self.refreshes.lock().unwrap().push((duration, outcome));
        }
    }

    struct RecordingRouting {
        calls: Arc<AtomicUsize>,
        candidates: Arc<Mutex<Vec<NodeAddr>>>,
    }

    impl ReadRoutingStrategy for RecordingRouting {
        fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            replicas.clone_into(&mut self.candidates.lock().unwrap());
            replicas.first()
        }
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn command_parts(frame: Frame) -> Vec<Bytes> {
        let Frame::Array(Some(parts)) = frame else {
            panic!("expected command array, got {frame:?}");
        };
        parts
            .into_iter()
            .map(|part| match part {
                Frame::BulkString(Some(value)) => value,
                other => panic!("expected bulk-string command part, got {other:?}"),
            })
            .collect()
    }

    /// Build an auto-pipeline service on one end of a loopback TCP connection
    /// and script the server end to answer its first request.
    ///
    /// `expected_markers` lets the ASK test prove that both ASKING and the
    /// logical command reached the same connection before the fake replies.
    async fn scripted_service(
        expected_markers: Vec<&'static [u8]>,
        response: Vec<u8>,
    ) -> (String, AutoPipelineService, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let server_task = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let n = server.read(&mut buf).await.unwrap();
                assert!(n > 0, "scripted node closed before receiving its request");
                request.extend_from_slice(&buf[..n]);
                if expected_markers
                    .iter()
                    .all(|marker| contains_bytes(&request, marker))
                {
                    break;
                }
            }
            server.write_all(&response).await.unwrap();
        });

        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(client));
        let service = AutoPipelineService::new(conn, AutoPipelineConfig::default());
        (addr.to_string(), service, server_task)
    }

    async fn framed_service(
        expected: Vec<Frame>,
        responses: Vec<Frame>,
    ) -> (String, AutoPipelineService, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        drop(listener);

        let server_task = tokio::spawn(async move {
            let mut framed =
                Framed::new(redis_tower_core::RedisStream::Tcp(server), RespCodec::new());
            for expected in expected {
                let actual = framed.next().await.unwrap().unwrap();
                assert_eq!(actual, expected);
            }
            for response in responses {
                framed.send(response).await.unwrap();
            }
        });

        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(client));
        let service = AutoPipelineService::new(conn, AutoPipelineConfig::default());
        (addr.to_string(), service, server_task)
    }

    /// Spawn a target that accepts the standard connection bootstrap and one
    /// SPUBLISH, proving a Pub/Sub MOVED target is immediately usable by the
    /// ordinary command router.
    async fn pubsub_moved_target(
        expected_channel: &'static str,
    ) -> (NodeAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut framed =
                Framed::new(redis_tower_core::RedisStream::Tcp(socket), RespCodec::new());

            for expected in [b"CLIENT".as_slice(), b"CLIENT", b"HELLO"] {
                let command = command_parts(framed.next().await.unwrap().unwrap());
                assert_eq!(command[0].as_ref(), expected);
                // SETINFO is best-effort and a failed HELLO makes Auto fall
                // back to RESP2, which keeps this fake node intentionally
                // small while exercising the real connector path.
                framed
                    .send(Frame::Error(Bytes::from_static(b"ERR unsupported")))
                    .await
                    .unwrap();
            }

            let command = command_parts(framed.next().await.unwrap().unwrap());
            assert_eq!(command[0].as_ref(), b"SPUBLISH");
            assert_eq!(command[1].as_ref(), expected_channel.as_bytes());
            framed.send(Frame::Integer(1)).await.unwrap();
        });
        (node_addr(&addr.to_string()), task)
    }

    fn cache_request(frame: Frame, command_name: &str) -> ClusterCacheRequest {
        ClusterCacheRequest {
            frame,
            command_name: command_name.to_string(),
            deadline: None,
            idempotent: true,
            is_blocking: false,
        }
    }

    /// Build a service whose peer records any wire write and responds with an
    /// error so a routing regression fails promptly rather than hanging.
    async fn quiet_service() -> (
        String,
        AutoPipelineService,
        Arc<AtomicBool>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let saw_wire = Arc::new(AtomicBool::new(false));
        let server_saw_wire = Arc::clone(&saw_wire);
        let server_task = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            match tokio::time::timeout(Duration::from_secs(2), server.read(&mut buf)).await {
                Ok(Ok(0)) | Err(_) => {}
                Ok(Ok(_)) => {
                    server_saw_wire.store(true, Ordering::Relaxed);
                    let _ = server.write_all(b"-ERR unexpected wire write\r\n").await;
                }
                Ok(Err(_)) => {}
            }
        });

        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(client));
        let service = AutoPipelineService::new(conn, AutoPipelineConfig::default());
        (addr.to_string(), service, saw_wire, server_task)
    }

    async fn dead_service() -> (String, AutoPipelineService) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let conn = RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(client));
        let service = AutoPipelineService::new(conn, AutoPipelineConfig::default());
        let server_task = tokio::spawn(async move {
            let mut request = [0u8; 256];
            assert!(server.read(&mut request).await.unwrap() > 0);
            // Closing without a response terminates the fixed worker.
        });
        let mut probe = service.clone();
        let _ = call_service(
            &mut probe,
            array(vec![bulk("GET"), bulk("dead-worker-probe")]),
        )
        .await;
        server_task.await.unwrap();
        drop(probe);
        tokio::time::timeout(Duration::from_secs(1), async {
            while service.is_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixed-connection worker did not exit after transport failure");
        assert!(!service.is_connection_healthy());
        (addr.to_string(), service)
    }

    async fn reconnecting_unhealthy_service() -> (String, AutoPipelineService) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let initial = Arc::new(Mutex::new(Some(RedisConnection::from_stream(
            redis_tower_core::RedisStream::Tcp(client),
        ))));
        let factory = {
            let initial = Arc::clone(&initial);
            move || {
                let connection = initial.lock().unwrap().take();
                async move {
                    match connection {
                        Some(connection) => Ok(connection),
                        None => std::future::pending().await,
                    }
                }
            }
        };
        let reconnect = AutoPipelineReconnectConfig::new(ReconnectConfig {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            ..ReconnectConfig::default()
        });
        let service =
            AutoPipelineService::with_factory(factory, AutoPipelineConfig::default(), reconnect)
                .await
                .unwrap();
        let server_task = tokio::spawn(async move {
            let mut request = [0u8; 256];
            assert!(server.read(&mut request).await.unwrap() > 0);
            // Closing without a response leaves the live worker reconnecting.
        });
        let mut probe = service.clone();
        let _ = call_service(
            &mut probe,
            array(vec![bulk("GET"), bulk("unhealthy-worker-probe")]),
        )
        .await;
        server_task.await.unwrap();
        drop(probe);
        assert!(service.is_alive());
        assert!(!service.is_connection_healthy());
        (addr.to_string(), service)
    }

    fn node_addr(addr: &str) -> NodeAddr {
        let addr: std::net::SocketAddr = addr.parse().unwrap();
        NodeAddr {
            host: addr.ip().to_string(),
            port: addr.port(),
        }
    }

    fn test_client(
        default_node: String,
        topology: ClusterTopology,
        masters: HashMap<String, AutoPipelineService>,
        recorder: Arc<RecordingMetrics>,
        include_node_in_metrics: bool,
    ) -> MultiplexedClusterClient {
        let recorder: Arc<dyn MetricsRecorder> = recorder;
        MultiplexedClusterClient {
            inner: Arc::new(RwLock::new(Inner {
                topology,
                topology_changes: TopologyChangeTracker::new(),
                masters,
                replicas: HashMap::new(),
                default_node,
                host_override: None,
                address_map: None,
                read_preference: ReadPreference::Master,
                read_routing: Arc::new(RoundRobinRouting::new()),
                max_redirects: 3,
                pipeline_config: AutoPipelineConfig::default(),
                reconnect_config: default_node_reconnect(),
                credentials: None,
                connection_config: ConnectionConfig::default(),
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                tls: None,
            })),
            cache_hooks: Arc::new(StdRwLock::new(None)),
            metrics_recorder: Some(recorder),
            include_node_in_metrics,
            node_metric_labels: Arc::new(BoundedNodeMetricLabels::default()),
            refresh_gate: Arc::new(RefreshGate::new(REFRESH_MIN_INTERVAL)),
        }
    }

    fn redirect_client(
        initial_addr: String,
        initial_service: AutoPipelineService,
        target_addr: String,
        target_service: AutoPipelineService,
        recorder: Arc<RecordingMetrics>,
    ) -> MultiplexedClusterClient {
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&initial_addr),
            replicas: Vec::new(),
        }]);
        let masters = HashMap::from([
            (initial_addr.clone(), initial_service),
            (target_addr, target_service),
        ]);
        test_client(initial_addr, topology, masters, recorder, true)
    }

    #[tokio::test]
    async fn pubsub_moved_commits_each_later_authoritative_owner() {
        const CHANNEL: &str = "{pubsub-moved}:events";
        let slot = slot_for_key(CHANNEL.as_bytes());
        let first = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let (second, second_server) = pubsub_moved_target(CHANNEL).await;
        let (third, third_server) = pubsub_moved_target(CHANNEL).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: first.clone(),
            replicas: Vec::new(),
        }]);
        let client = test_client(
            first.addr_string(),
            topology,
            HashMap::new(),
            Arc::new(RecordingMetrics::default()),
            false,
        );
        let backend = client.pubsub_backend().await;
        let (_snapshot, mut changes) = backend.topology_state().await.unwrap();

        backend.commit_moved(slot, second.clone()).await.unwrap();
        changes.changed().await.unwrap();
        assert_eq!(
            client.topology().await.master_for_slot(slot),
            Some(&second),
            "the first Pub/Sub MOVED target must become the shared owner"
        );
        assert_eq!(
            client
                .execute(SPublish::new(CHANNEL, "second"))
                .await
                .unwrap(),
            1
        );
        second_server.await.unwrap();

        backend.commit_moved(slot, third.clone()).await.unwrap();
        changes.changed().await.unwrap();
        assert_eq!(
            client.topology().await.master_for_slot(slot),
            Some(&third),
            "a later authoritative owner must supersede the earlier MOVED"
        );
        assert_eq!(
            client
                .execute(SPublish::new(CHANNEL, "third"))
                .await
                .unwrap(),
            1
        );
        third_server.await.unwrap();

        client.shutdown().await;
    }

    #[tokio::test]
    async fn already_expired_command_skips_routing_and_records_timeout() {
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            String::new(),
            ClusterTopology::new(Vec::new()),
            HashMap::new(),
            Arc::clone(&recorder),
            true,
        );

        let result = client
            .execute(WithDeadline::new(
                Get::new("key"),
                tokio::time::Instant::now() - Duration::from_millis(1),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: Some(ErrorKind::Other),
                node: None,
            }]
        );

        let pinned_result = client
            .execute_on_node(
                "127.0.0.1:7000",
                WithDeadline::new(
                    Get::new("key"),
                    tokio::time::Instant::now() - Duration::from_millis(1),
                ),
            )
            .await;
        assert!(matches!(pinned_result, Err(RedisError::CommandTimeout)));
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![
                CommandCompletion {
                    command: "GET".to_string(),
                    error: Some(ErrorKind::Other),
                    node: None,
                },
                CommandCompletion {
                    command: "GET".to_string(),
                    error: Some(ErrorKind::Other),
                    node: Some("127.0.0.1:7000".to_string()),
                },
            ]
        );
        client.shutdown().await;
    }

    #[tokio::test]
    async fn command_deadline_bounds_routing_lock_and_records_timeout() {
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            String::new(),
            ClusterTopology::new(Vec::new()),
            HashMap::new(),
            Arc::clone(&recorder),
            false,
        );
        let inner_lock = client.inner.write().await;

        let result = client
            .execute(WithDeadline::after(
                Get::new("key"),
                Duration::from_millis(25),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: Some(ErrorKind::Other),
                node: None,
            }]
        );
        drop(inner_lock);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn pinned_node_deadline_bounds_lookup_and_records_timeout() {
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            String::new(),
            ClusterTopology::new(Vec::new()),
            HashMap::new(),
            Arc::clone(&recorder),
            true,
        );
        let inner_lock = client.inner.write().await;

        let result = client
            .execute_on_node(
                "127.0.0.1:7000",
                WithDeadline::after(Get::new("key"), Duration::from_millis(25)),
            )
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: Some(ErrorKind::Other),
                node: Some("127.0.0.1:7000".to_string()),
            }]
        );
        drop(inner_lock);
        client.shutdown().await;
    }

    #[test]
    fn bounded_node_metric_labels_cap_at_64_and_preserve_seen_nodes() {
        let labels = BoundedNodeMetricLabels::default();

        for i in 0..MAX_NODE_METRIC_LABELS {
            let node = format!("127.0.0.1:{}", 7000 + i);
            assert_eq!(labels.label(&node).as_str(), node);
        }

        assert_eq!(labels.label("127.0.0.1:8000").as_str(), "_OTHER");
        assert_eq!(labels.label("127.0.0.1:8001").as_str(), "_OTHER");
        assert_eq!(labels.label("127.0.0.1:7000").as_str(), "127.0.0.1:7000");
        assert_eq!(labels.seen.read().unwrap().len(), MAX_NODE_METRIC_LABELS);
    }

    #[test]
    fn builder_disables_node_metrics_by_default_and_can_enable_them() {
        let default = MultiplexedClusterClient::builder("127.0.0.1:7000");
        assert!(!default.include_node_in_metrics);

        let enabled =
            MultiplexedClusterClient::builder("127.0.0.1:7000").include_node_in_metrics(true);
        assert!(enabled.include_node_in_metrics);

        let disabled_again = enabled.include_node_in_metrics(false);
        assert!(!disabled_again.include_node_in_metrics);
    }

    #[test]
    fn builder_defaults_and_sets_resp_limits() {
        let default = MultiplexedClusterClient::builder("127.0.0.1:7000");
        assert_eq!(
            default.connection_config.resp_limits(),
            RespLimits::default()
        );
        assert_eq!(default.connection_config.protocol(), ProtocolVersion::Auto);

        let limits = RespLimits {
            max_frame_size: 2048,
            max_depth: 12,
        };
        let configured = MultiplexedClusterClient::builder("127.0.0.1:7000").resp_limits(limits);
        assert_eq!(configured.connection_config.resp_limits(), limits);
    }

    #[test]
    fn builder_retains_full_connection_config_and_composable_overrides() {
        let limits = RespLimits {
            max_frame_size: 8192,
            max_depth: 11,
        };
        let config = ConnectionConfig::new()
            .with_connect_timeout(Some(Duration::from_secs(4)))
            .with_protocol(ProtocolVersion::Resp2)
            .with_resp_limits(limits);

        let configured = MultiplexedClusterClient::builder("127.0.0.1:7000")
            .connection_config(config)
            .protocol(ProtocolVersion::Resp3);

        assert_eq!(
            configured.connection_config.connect_timeout(),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            configured.connection_config.protocol(),
            ProtocolVersion::Resp3
        );
        assert_eq!(configured.connection_config.resp_limits(), limits);
    }

    #[test]
    fn cache_command_preserves_raw_request_metadata() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let frame = array(vec![bulk("BLPOP"), bulk("queue"), bulk("0")]);
        let command = ClusterCacheCommand {
            request: ClusterCacheRequest {
                frame: frame.clone(),
                command_name: "BLPOP".to_string(),
                deadline: Some(deadline),
                idempotent: false,
                is_blocking: true,
            },
        };

        assert_eq!(command.to_frame(), frame);
        assert_eq!(command.name(), "BLPOP");
        assert_eq!(command.deadline(), Some(deadline));
        assert!(!command.idempotent());
        assert!(command.is_blocking());
    }

    #[test]
    fn cache_coverage_ignores_pre_dispatch_admission_errors() {
        assert!(!cache_error_requires_node_loss(&RedisError::QueueFull));
        assert!(!cache_error_requires_node_loss(&RedisError::CircuitOpen));
        assert!(!cache_error_requires_node_loss(
            &RedisError::PoolAcquisitionTimeout {
                waited: Duration::from_millis(1),
                pool_size: 1,
            }
        ));
        assert!(cache_error_requires_node_loss(&RedisError::CommandTimeout));
        assert!(cache_error_requires_node_loss(
            &RedisError::ConnectionClosed
        ));
    }

    #[tokio::test]
    async fn cache_opt_in_prefix_is_atomic_on_the_routed_master() {
        let get = array(vec![bulk("GET"), bulk("cache-key")]);
        let value = Frame::BulkString(Some(Bytes::from_static(b"value")));
        let (addr, service, server) = framed_service(
            vec![ClientCaching::new(true).to_frame(), get.clone()],
            vec![Frame::SimpleString(b"OK"[..].into()), value.clone()],
        )
        .await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr, service)]),
            Arc::new(RecordingMetrics::default()),
            false,
        );

        let response = client
            .execute_cache_request(cache_request(get, "GET"), CacheDispatchMode::OptIn)
            .await
            .unwrap();
        assert_eq!(response, value);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cache_opt_in_ask_retry_bypasses_the_mutually_exclusive_cache_flag() {
        let key = "cache-ask-key";
        let get = array(vec![bulk("GET"), bulk(key)]);
        let slot = slot_for_key(key.as_bytes());
        let value = Frame::BulkString(Some(Bytes::from_static(b"migrated")));
        let (target_addr, target_service, target_server) = framed_service(
            vec![array(vec![bulk("ASKING")]), get.clone()],
            vec![Frame::SimpleString(b"OK"[..].into()), value.clone()],
        )
        .await;
        let ask = Frame::Error(format!("ASK {slot} {target_addr}").into_bytes().into());
        let (initial_addr, initial_service, initial_server) = framed_service(
            vec![ClientCaching::new(true).to_frame(), get.clone()],
            vec![Frame::SimpleString(b"OK"[..].into()), ask],
        )
        .await;
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr,
            target_service,
            Arc::new(RecordingMetrics::default()),
        );

        let response = client
            .execute_cache_request(cache_request(get, "GET"), CacheDispatchMode::OptIn)
            .await
            .unwrap();
        assert_eq!(response, value);
        initial_server.await.unwrap();
        target_server.await.unwrap();
    }

    #[tokio::test]
    async fn cache_opt_in_moved_retry_bypasses_setup_on_the_new_master() {
        let key = "cache-moved-key";
        let get = array(vec![bulk("GET"), bulk(key)]);
        let slot = slot_for_key(key.as_bytes());
        let value = Frame::BulkString(Some(Bytes::from_static(b"moved")));
        let (target_addr, target_service, target_server) =
            framed_service(vec![get.clone()], vec![value.clone()]).await;
        let moved = Frame::Error(format!("MOVED {slot} {target_addr}").into_bytes().into());
        let (initial_addr, initial_service, initial_server) = framed_service(
            vec![ClientCaching::new(true).to_frame(), get.clone()],
            vec![Frame::SimpleString(b"OK"[..].into()), moved],
        )
        .await;
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr,
            target_service,
            Arc::new(RecordingMetrics::default()),
        );

        let response = client
            .execute_cache_request(cache_request(get, "GET"), CacheDispatchMode::OptIn)
            .await
            .unwrap();
        assert_eq!(response, value);
        initial_server.await.unwrap();
        target_server.await.unwrap();
    }

    #[tokio::test]
    async fn cache_node_pipeline_targets_the_exact_master() {
        let (default_addr, default_service, default_saw_wire, default_server) =
            quiet_service().await;
        let off = array(vec![bulk("CLIENT"), bulk("TRACKING"), bulk("OFF")]);
        let on = array(vec![
            bulk("CLIENT"),
            bulk("TRACKING"),
            bulk("ON"),
            bulk("REDIRECT"),
            bulk("42"),
        ]);
        let (target_addr, target_service, target_server) = framed_service(
            vec![off.clone(), on.clone()],
            vec![
                Frame::SimpleString(b"OK"[..].into()),
                Frame::SimpleString(b"OK"[..].into()),
            ],
        )
        .await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&default_addr),
            replicas: Vec::new(),
        }]);
        let client = test_client(
            default_addr.clone(),
            topology,
            HashMap::from([
                (default_addr, default_service),
                (target_addr.clone(), target_service),
            ]),
            Arc::new(RecordingMetrics::default()),
            false,
        );

        let responses = client
            .execute_cache_node_pipeline(&target_addr, vec![off, on])
            .await
            .unwrap();
        assert_eq!(responses.len(), 2);
        assert!(!default_saw_wire.load(Ordering::Relaxed));
        drop(client);
        target_server.await.unwrap();
        default_server.await.unwrap();
    }

    #[tokio::test]
    async fn final_shutdown_waits_for_internal_refresh_clone_before_draining() {
        let (addr, service, _saw_wire, server) = quiet_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr, service)]),
            Arc::new(RecordingMetrics::default()),
            false,
        );
        let permit = client.refresh_gate.try_begin().unwrap();
        let internal_refresh = RefreshTaskGuard::new(client.clone(), permit);
        let (release, hold) = tokio::sync::oneshot::channel();
        let refresh = tokio::spawn(async move {
            let _ = hold.await;
            drop(internal_refresh);
        });
        let shutdown = tokio::spawn(client.shutdown());
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());

        release.send(()).unwrap();
        refresh.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .expect("final shutdown did not wake after refresh released its clone")
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cache_snapshot_atomically_includes_desired_and_missing_masters() {
        let (connected_addr, service, _saw_wire, connected_server) = quiet_service().await;
        let missing = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 9,
        };
        let topology = ClusterTopology::new(vec![
            crate::topology::SlotRange {
                start: 0,
                end: 8_000,
                master: node_addr(&connected_addr),
                replicas: Vec::new(),
            },
            crate::topology::SlotRange {
                start: 8_001,
                end: 16_383,
                master: missing.clone(),
                replicas: Vec::new(),
            },
        ]);
        let client = test_client(
            connected_addr.clone(),
            topology,
            HashMap::from([(connected_addr.clone(), service)]),
            Arc::new(RecordingMetrics::default()),
            false,
        );

        let snapshot = client.cache_node_snapshot().await.unwrap();
        assert_eq!(snapshot.revision.get(), 0);
        assert_eq!(snapshot.masters.len(), 2);
        assert!(
            snapshot
                .masters
                .iter()
                .find(|master| master.addr == connected_addr)
                .unwrap()
                .data_health
                .is_some()
        );
        assert!(
            snapshot
                .masters
                .iter()
                .find(|master| master.addr == missing.addr_string())
                .unwrap()
                .data_health
                .is_none()
        );
        drop(snapshot);
        drop(client);
        connected_server.await.unwrap();
    }

    #[tokio::test]
    async fn no_recorder_keeps_command_observation_off_the_hot_path() {
        let (addr, service, server) =
            scripted_service(vec![b"GET"], b"$5\r\nvalue\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let mut client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr, service)]),
            recorder,
            false,
        );
        client.metrics_recorder = None;
        let name_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let result = client
            .execute(NameProbeCommand {
                calls: Arc::clone(&name_calls),
            })
            .await;
        client.shutdown().await;
        server.await.unwrap();

        assert!(matches!(result, Ok(Frame::BulkString(Some(_)))));
        assert_eq!(name_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn no_recorder_keeps_pinned_node_observation_off_the_hot_path() {
        let (addr, service, server) =
            scripted_service(vec![b"GET"], b"$5\r\nvalue\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let mut client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr.clone(), service)]),
            recorder,
            false,
        );
        client.metrics_recorder = None;
        let name_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let result = client
            .execute_on_node(
                &addr,
                NameProbeCommand {
                    calls: Arc::clone(&name_calls),
                },
            )
            .await;
        client.shutdown().await;
        server.await.unwrap();

        assert!(matches!(result, Ok(Frame::BulkString(Some(_)))));
        assert_eq!(name_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn pinned_node_command_records_one_completion_on_that_node() {
        let (addr, service, server) =
            scripted_service(vec![b"GET"], b"$5\r\nvalue\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr.clone(), service)]),
            Arc::clone(&recorder),
            true,
        );

        let result = client.execute_on_node(&addr, Get::new("key")).await;
        client.shutdown().await;
        server.await.unwrap();

        assert_eq!(result.unwrap(), Some(Bytes::from_static(b"value")));
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: None,
                node: Some(addr),
            }]
        );
    }

    #[tokio::test]
    async fn moved_redirect_records_one_hook_and_one_completion_on_the_final_node() {
        let (target_addr, target_service, target_server) =
            scripted_service(vec![b"GET"], b"$5\r\nvalue\r\n".to_vec()).await;
        let moved = format!("-MOVED 42 {target_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) =
            scripted_service(vec![b"GET"], moved).await;
        let recorder = Arc::new(RecordingMetrics::default());
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr.clone(),
            target_service,
            Arc::clone(&recorder),
        );
        client.inner.write().await.max_redirects = 1;

        // MOVED normally schedules a background topology refresh. Hold the
        // gate so this focused test has no unrelated network task racing it.
        let refresh_permit = client.refresh_gate.try_begin().unwrap();
        let result = client.execute(Get::new("key")).await;
        drop(refresh_permit);
        client.shutdown().await;
        initial_server.await.unwrap();
        target_server.await.unwrap();

        assert_eq!(result.unwrap(), Some(Bytes::from_static(b"value")));
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Moved]
        );
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: None,
                node: Some(target_addr),
            }]
        );
    }

    #[tokio::test]
    async fn ask_redirect_records_one_hook_and_one_completion_on_the_final_node() {
        let (target_addr, target_service, target_server) =
            scripted_service(vec![b"ASKING", b"GET"], b"+OK\r\n$5\r\nvalue\r\n".to_vec()).await;
        let ask = format!("-ASK 42 {target_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) =
            scripted_service(vec![b"GET"], ask).await;
        let recorder = Arc::new(RecordingMetrics::default());
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr.clone(),
            target_service,
            Arc::clone(&recorder),
        );

        let result = client.execute(Get::new("key")).await;
        client.shutdown().await;
        initial_server.await.unwrap();
        target_server.await.unwrap();

        assert_eq!(result.unwrap(), Some(Bytes::from_static(b"value")));
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Ask]
        );
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: None,
                node: Some(target_addr),
            }]
        );
    }

    #[tokio::test]
    async fn redirect_budget_does_not_follow_moved_after_one_ask() {
        let slot = 42;
        let (final_addr, final_service, final_saw_wire, final_server) = quiet_service().await;
        let moved = format!("+OK\r\n-MOVED {slot} {final_addr}\r\n").into_bytes();
        let (ask_addr, ask_service, ask_server) =
            scripted_service(vec![b"ASKING", b"GET"], moved).await;
        let ask = format!("-ASK {slot} {ask_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) =
            scripted_service(vec![b"GET"], ask).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&initial_addr),
            replicas: Vec::new(),
        }]);
        let masters = HashMap::from([
            (initial_addr.clone(), initial_service),
            (ask_addr, ask_service),
            (final_addr, final_service),
        ]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            initial_addr,
            topology,
            masters,
            Arc::clone(&recorder),
            false,
        );
        client.inner.write().await.max_redirects = 1;

        let error = client
            .execute(Get::new("key"))
            .await
            .expect_err("one redirect budget followed ASK and MOVED");
        assert!(error.to_string().contains("too many redirects (1)"));
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Ask, ClusterRedirectKind::Moved]
        );

        client.shutdown().await;
        initial_server.await.unwrap();
        ask_server.await.unwrap();
        final_server.await.unwrap();
        assert!(
            !final_saw_wire.load(Ordering::Relaxed),
            "budget exhaustion connected to or wrote the second redirect target"
        );
    }

    #[tokio::test]
    async fn redirect_budget_follows_repeated_ask_atomically() {
        let slot = 42;
        let (final_addr, final_service, final_server) =
            scripted_service(vec![b"ASKING", b"GET"], b"+OK\r\n$5\r\nvalue\r\n".to_vec()).await;
        let second_ask = format!("+OK\r\n-ASK {slot} {final_addr}\r\n").into_bytes();
        let (middle_addr, middle_service, middle_server) =
            scripted_service(vec![b"ASKING", b"GET"], second_ask).await;
        let first_ask = format!("-ASK {slot} {middle_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) =
            scripted_service(vec![b"GET"], first_ask).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&initial_addr),
            replicas: Vec::new(),
        }]);
        let masters = HashMap::from([
            (initial_addr.clone(), initial_service),
            (middle_addr, middle_service),
            (final_addr.clone(), final_service),
        ]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(initial_addr, topology, masters, Arc::clone(&recorder), true);
        client.inner.write().await.max_redirects = 2;

        assert_eq!(
            client.execute(Get::new("key")).await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Ask, ClusterRedirectKind::Ask]
        );
        assert_eq!(
            recorder.completions.lock().unwrap().last().unwrap().node,
            Some(final_addr)
        );

        client.shutdown().await;
        initial_server.await.unwrap();
        middle_server.await.unwrap();
        final_server.await.unwrap();
    }

    #[tokio::test]
    async fn strict_replica_without_replica_never_reaches_multiplexed_master() {
        let (master_addr, master_service, master_saw_wire, master_server) = quiet_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&master_addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        client.inner.write().await.read_preference = ReadPreference::Replica;
        let slot = slot_for_key(b"foo");

        let error = client
            .execute(Get::new("foo"))
            .await
            .expect_err("strict Replica unexpectedly fell back to the master");

        assert!(
            error.to_string().contains(&format!(
                "ReadPreference::Replica requires a connected replica for slot {slot}"
            )),
            "unexpected routing error: {error}"
        );
        client.shutdown().await;
        master_server.await.unwrap();
        assert!(!master_saw_wire.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn strict_replica_filters_dead_worker_before_routing() {
        let (master_addr, master_service, master_saw_wire, master_server) = quiet_service().await;
        let (replica_addr, replica_service) = dead_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&master_addr),
            replicas: vec![node_addr(&replica_addr)],
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        let routing_calls = Arc::new(AtomicUsize::new(0));
        let routing_candidates = Arc::new(Mutex::new(Vec::new()));
        {
            let mut inner = client.inner.write().await;
            inner.read_preference = ReadPreference::Replica;
            inner.read_routing = Arc::new(RecordingRouting {
                calls: Arc::clone(&routing_calls),
                candidates: Arc::clone(&routing_candidates),
            });
            inner.replicas.insert(replica_addr, replica_service);
        }

        let error = client
            .execute(Get::new("foo"))
            .await
            .expect_err("strict Replica routed to a dead worker or the master");
        assert!(error.to_string().contains("requires a connected replica"));
        assert_eq!(routing_calls.load(Ordering::SeqCst), 1);
        assert!(routing_candidates.lock().unwrap().is_empty());
        client.shutdown().await;
        master_server.await.unwrap();
        assert!(!master_saw_wire.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn prefer_replica_without_replica_falls_back_to_multiplexed_master() {
        let (master_addr, master_service, master_server) =
            scripted_service(vec![b"GET"], b"$5\r\nvalue\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&master_addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        client.inner.write().await.read_preference = ReadPreference::PreferReplica;

        assert_eq!(
            client.execute(Get::new("foo")).await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        client.shutdown().await;
        master_server.await.unwrap();
    }

    #[tokio::test]
    async fn prefer_replica_filters_reconnecting_worker_and_uses_master() {
        let (master_addr, master_service, master_server) =
            scripted_service(vec![b"GET"], b"$5\r\nvalue\r\n".to_vec()).await;
        let (replica_addr, replica_service) = reconnecting_unhealthy_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&master_addr),
            replicas: vec![node_addr(&replica_addr)],
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        let routing_calls = Arc::new(AtomicUsize::new(0));
        let routing_candidates = Arc::new(Mutex::new(Vec::new()));
        {
            let mut inner = client.inner.write().await;
            inner.read_preference = ReadPreference::PreferReplica;
            inner.read_routing = Arc::new(RecordingRouting {
                calls: Arc::clone(&routing_calls),
                candidates: Arc::clone(&routing_candidates),
            });
            inner.replicas.insert(replica_addr, replica_service);
        }

        assert_eq!(
            client.execute(Get::new("foo")).await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(routing_calls.load(Ordering::SeqCst), 1);
        assert!(routing_candidates.lock().unwrap().is_empty());
        client.shutdown().await;
        master_server.await.unwrap();
    }

    #[tokio::test]
    async fn strict_replica_skips_unconnected_topology_replica_on_multiplexed_client() {
        let (master_addr, master_service, master_saw_wire, master_server) = quiet_service().await;
        let (replica_addr, replica_service, replica_server) =
            scripted_service(vec![b"GET"], b"$7\r\nreplica\r\n".to_vec()).await;
        let connected_replica = node_addr(&replica_addr);
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&master_addr),
            replicas: vec![
                NodeAddr {
                    host: "127.0.0.1".to_string(),
                    port: 1,
                },
                connected_replica.clone(),
            ],
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        let routing_calls = Arc::new(AtomicUsize::new(0));
        let routing_candidates = Arc::new(Mutex::new(Vec::new()));
        {
            let mut inner = client.inner.write().await;
            inner.read_preference = ReadPreference::Replica;
            inner.read_routing = Arc::new(RecordingRouting {
                calls: Arc::clone(&routing_calls),
                candidates: Arc::clone(&routing_candidates),
            });
            inner.replicas.insert(replica_addr, replica_service);
        }

        assert_eq!(
            client.execute(Get::new("foo")).await.unwrap(),
            Some(Bytes::from_static(b"replica"))
        );
        client.shutdown().await;
        replica_server.await.unwrap();
        master_server.await.unwrap();
        assert!(!master_saw_wire.load(Ordering::Relaxed));
        assert_eq!(routing_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*routing_candidates.lock().unwrap(), vec![connected_replica]);
    }

    #[tokio::test]
    async fn strict_replica_redirects_never_reach_multiplexed_master() {
        for redirect_kind in ["MOVED", "ASK"] {
            let (master_addr, master_service, master_saw_wire, master_server) =
                quiet_service().await;
            let slot = slot_for_key(b"foo");
            let redirect = format!("-{redirect_kind} {slot} {master_addr}\r\n").into_bytes();
            let (replica_addr, replica_service, replica_server) =
                scripted_service(vec![b"GET"], redirect).await;
            let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
                start: 0,
                end: 16_383,
                master: node_addr(&master_addr),
                replicas: vec![node_addr(&replica_addr)],
            }]);
            let recorder = Arc::new(RecordingMetrics::default());
            let client = test_client(
                master_addr.clone(),
                topology,
                HashMap::from([(master_addr, master_service)]),
                recorder,
                false,
            );
            {
                let mut inner = client.inner.write().await;
                inner.read_preference = ReadPreference::Replica;
                inner.replicas.insert(replica_addr, replica_service);
            }

            let error = client
                .execute(Get::new("foo"))
                .await
                .expect_err("strict Replica followed a redirect to the master");
            assert!(
                error.to_string().contains(&format!(
                    "ReadPreference::Replica requires a connected replica for slot {slot}"
                )),
                "unexpected {redirect_kind} routing error: {error}"
            );
            client.shutdown().await;
            replica_server.await.unwrap();
            master_server.await.unwrap();
            assert!(!master_saw_wire.load(Ordering::Relaxed));
        }
    }

    #[tokio::test]
    async fn strict_replica_still_routes_multiplexed_writes_to_master() {
        let (master_addr, master_service, master_server) =
            scripted_service(vec![b"SET"], b"+OK\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&master_addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        client.inner.write().await.read_preference = ReadPreference::Replica;

        client.execute(Set::new("foo", "value")).await.unwrap();
        client.shutdown().await;
        master_server.await.unwrap();
    }

    fn get_frame(key: &'static str) -> Frame {
        array(vec![bulk("GET"), bulk(key)])
    }

    #[tokio::test]
    async fn cluster_pipeline_groups_by_node_and_restores_submission_order() {
        let key_a1 = "{pipeline-a}:one";
        let key_b = "{pipeline-b}:one";
        let key_a2 = "{pipeline-a}:two";
        let slot_a = slot_for_key(key_a1.as_bytes());
        let slot_b = slot_for_key(key_b.as_bytes());
        assert_ne!(slot_a, slot_b);

        let (addr_a, service_a, server_a) = scripted_service(
            vec![key_a1.as_bytes(), key_a2.as_bytes()],
            b"+a-one\r\n+a-two\r\n".to_vec(),
        )
        .await;
        let (addr_b, service_b, server_b) =
            scripted_service(vec![key_b.as_bytes()], b"+b-one\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![
            crate::topology::SlotRange {
                start: slot_a,
                end: slot_a,
                master: node_addr(&addr_a),
                replicas: Vec::new(),
            },
            crate::topology::SlotRange {
                start: slot_b,
                end: slot_b,
                master: node_addr(&addr_b),
                replicas: Vec::new(),
            },
        ]);
        let masters = HashMap::from([(addr_a.clone(), service_a), (addr_b.clone(), service_b)]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(addr_a, topology, masters, recorder, false);
        let mut executor = client.clone();

        let responses = PipelineExecutor::execute_pipeline(
            &mut executor,
            vec![get_frame(key_a1), get_frame(key_b), get_frame(key_a2)],
        )
        .await
        .unwrap();

        assert_eq!(
            responses,
            vec![
                Frame::SimpleString(Bytes::from_static(b"a-one")),
                Frame::SimpleString(Bytes::from_static(b"b-one")),
                Frame::SimpleString(Bytes::from_static(b"a-two")),
            ]
        );

        drop(executor);
        client.shutdown().await;
        server_a.await.unwrap();
        server_b.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_pipeline_accepts_unknown_commands_with_legacy_first_key_routing() {
        let key = "{custom-pipeline}:key";
        let slot = slot_for_key(key.as_bytes());
        let (addr, service, server) = scripted_service(
            vec![b"CUSTOM.CMD", key.as_bytes()],
            b"+custom-ok\r\n".to_vec(),
        )
        .await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: slot,
            end: slot,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr, service)]),
            recorder,
            false,
        );
        let mut executor = client.clone();

        let responses = PipelineExecutor::execute_pipeline(
            &mut executor,
            vec![array(vec![bulk("CUSTOM.CMD"), bulk(key), bulk("arg")])],
        )
        .await
        .unwrap();
        assert_eq!(
            responses,
            vec![Frame::SimpleString(Bytes::from_static(b"custom-ok"))]
        );

        drop(executor);
        client.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_pipeline_routes_known_non_argv1_layout_from_authoritative_slot() {
        let key_a = "{zintercard-pipeline}:a";
        let key_b = "{zintercard-pipeline}:b";
        let slot = slot_for_key(key_a.as_bytes());
        let (owner_addr, owner_service, owner_server) = scripted_service(
            vec![b"ZINTERCARD", key_a.as_bytes(), key_b.as_bytes()],
            b":2\r\n".to_vec(),
        )
        .await;
        let (default_addr, default_service, default_saw_wire, default_server) =
            quiet_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: slot,
            end: slot,
            master: node_addr(&owner_addr),
            replicas: Vec::new(),
        }]);
        let masters = HashMap::from([
            (owner_addr, owner_service),
            (default_addr.clone(), default_service),
        ]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(default_addr, topology, masters, recorder, false);
        let mut executor = client.clone();

        let responses = PipelineExecutor::execute_pipeline(
            &mut executor,
            vec![array(vec![
                bulk("ZINTERCARD"),
                bulk("2"),
                bulk(key_a),
                bulk(key_b),
            ])],
        )
        .await
        .unwrap();
        assert_eq!(responses, vec![Frame::Integer(2)]);

        drop(executor);
        client.shutdown().await;
        owner_server.await.unwrap();
        default_server.await.unwrap();
        assert!(!default_saw_wire.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn cluster_pipeline_pins_dependent_same_slot_commands_to_master() {
        let key = "{master-pipeline}:key";
        let slot = slot_for_key(key.as_bytes());
        let (master_addr, master_service, master_server) = scripted_service(
            vec![b"SET", b"GET", key.as_bytes()],
            b"+OK\r\n$5\r\nvalue\r\n".to_vec(),
        )
        .await;
        let (replica_addr, replica_service, replica_saw_wire, replica_server) =
            quiet_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: slot,
            end: slot,
            master: node_addr(&master_addr),
            replicas: vec![node_addr(&replica_addr)],
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            master_addr.clone(),
            topology,
            HashMap::from([(master_addr, master_service)]),
            recorder,
            false,
        );
        {
            let mut inner = client.inner.write().await;
            inner.read_preference = ReadPreference::PreferReplica;
            inner.replicas.insert(replica_addr, replica_service);
        }

        let results = crate::ClusterPipeline::new()
            .push(Set::new(key, "value"))
            .push(Get::new(key))
            .execute(&client)
            .await
            .unwrap();
        assert_eq!(results.get::<Option<Bytes>>(0).unwrap(), &None);
        assert_eq!(
            results.get::<Option<Bytes>>(1).unwrap().as_deref(),
            Some(&b"value"[..])
        );

        client.shutdown().await;
        master_server.await.unwrap();
        replica_server.await.unwrap();
        assert!(!replica_saw_wire.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn cluster_pipeline_rejects_known_crossslot_frame_before_any_wire_write() {
        let (addr, service, saw_wire, server) = quiet_service().await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&addr),
            replicas: Vec::new(),
        }]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            addr.clone(),
            topology,
            HashMap::from([(addr, service)]),
            recorder,
            false,
        );
        let mut executor = client.clone();

        let result = PipelineExecutor::execute_pipeline(
            &mut executor,
            vec![
                array(vec![bulk("SET"), bulk("{safe}:key"), bulk("value")]),
                array(vec![bulk("MGET"), bulk("{a}:key"), bulk("{b}:key")]),
            ],
        )
        .await;
        assert!(matches!(
            result,
            Err(RedisError::Redis(ref message)) if message.starts_with("CROSSSLOT")
        ));

        drop(executor);
        client.shutdown().await;
        server.await.unwrap();
        assert!(!saw_wire.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn cluster_pipeline_retries_only_the_moved_entry() {
        let stable_key = "pipeline-stable";
        let moved_key = "pipeline-moved";
        let moved_slot = slot_for_key(moved_key.as_bytes());
        let (target_addr, target_service, target_server) =
            scripted_service(vec![moved_key.as_bytes()], b"$7\r\nretried\r\n".to_vec()).await;
        let initial_response =
            format!("+stable\r\n-MOVED {moved_slot} {target_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) = scripted_service(
            vec![stable_key.as_bytes(), moved_key.as_bytes()],
            initial_response,
        )
        .await;
        let recorder = Arc::new(RecordingMetrics::default());
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr,
            target_service,
            Arc::clone(&recorder),
        );
        client.inner.write().await.max_redirects = 1;
        let refresh_permit = client.refresh_gate.try_begin().unwrap();
        let mut executor = client.clone();

        let responses = tokio::time::timeout(
            Duration::from_secs(2),
            PipelineExecutor::execute_pipeline(
                &mut executor,
                vec![get_frame(stable_key), get_frame(moved_key)],
            ),
        )
        .await
        .expect("pipeline hung after replaying more than the redirected entry")
        .unwrap();

        assert_eq!(
            responses,
            vec![
                Frame::SimpleString(Bytes::from_static(b"stable")),
                Frame::BulkString(Some(Bytes::from_static(b"retried"))),
            ]
        );
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Moved]
        );

        drop(executor);
        drop(refresh_permit);
        client.shutdown().await;
        initial_server.await.unwrap();
        target_server.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_pipeline_follows_same_slot_redirects_in_submission_order() {
        let key = "{ordered-redirect}:key";
        let slot = slot_for_key(key.as_bytes());
        let (second_started_tx, second_started_rx) = tokio::sync::oneshot::channel();

        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first_listener.local_addr().unwrap();
        let first_client = tokio::net::TcpStream::connect(first_addr).await.unwrap();
        let (mut first_server_stream, _) = first_listener.accept().await.unwrap();
        drop(first_listener);
        let first_server = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !contains_bytes(&request, b"SET") {
                let n = first_server_stream.read(&mut buf).await.unwrap();
                assert!(n > 0, "first redirect target closed before SET");
                request.extend_from_slice(&buf[..n]);
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(300), second_started_rx)
                    .await
                    .is_err(),
                "the second same-slot redirect ran before the first completed"
            );
            first_server_stream.write_all(b"+OK\r\n").await.unwrap();
        });
        let first_conn =
            RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(first_client));
        let first_service = AutoPipelineService::new(first_conn, AutoPipelineConfig::default());

        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_addr = second_listener.local_addr().unwrap();
        let second_client = tokio::net::TcpStream::connect(second_addr).await.unwrap();
        let (mut second_server_stream, _) = second_listener.accept().await.unwrap();
        drop(second_listener);
        let second_server = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !contains_bytes(&request, b"GET") {
                let n = second_server_stream.read(&mut buf).await.unwrap();
                assert!(n > 0, "second redirect target closed before GET");
                request.extend_from_slice(&buf[..n]);
            }
            let _ = second_started_tx.send(());
            second_server_stream
                .write_all(b"$5\r\nvalue\r\n")
                .await
                .unwrap();
        });
        let second_conn =
            RedisConnection::from_stream(redis_tower_core::RedisStream::Tcp(second_client));
        let second_service = AutoPipelineService::new(second_conn, AutoPipelineConfig::default());

        let initial_response =
            format!("-MOVED {slot} {first_addr}\r\n-MOVED {slot} {second_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) =
            scripted_service(vec![b"SET", b"GET"], initial_response).await;
        let topology = ClusterTopology::new(vec![crate::topology::SlotRange {
            start: 0,
            end: 16_383,
            master: node_addr(&initial_addr),
            replicas: Vec::new(),
        }]);
        let masters = HashMap::from([
            (initial_addr.clone(), initial_service),
            (first_addr.to_string(), first_service),
            (second_addr.to_string(), second_service),
        ]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            initial_addr,
            topology,
            masters,
            Arc::clone(&recorder),
            false,
        );
        let refresh_permit = client.refresh_gate.try_begin().unwrap();
        let mut executor = client.clone();

        let responses = tokio::time::timeout(
            Duration::from_secs(3),
            PipelineExecutor::execute_pipeline(
                &mut executor,
                vec![
                    array(vec![bulk("SET"), bulk(key), bulk("value")]),
                    array(vec![bulk("GET"), bulk(key)]),
                ],
            ),
        )
        .await
        .expect("ordered redirect pipeline timed out")
        .unwrap();
        assert_eq!(
            responses,
            vec![
                Frame::SimpleString(Bytes::from_static(b"OK")),
                Frame::BulkString(Some(Bytes::from_static(b"value"))),
            ]
        );
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Moved, ClusterRedirectKind::Moved]
        );

        drop(executor);
        drop(refresh_permit);
        client.shutdown().await;
        initial_server.await.unwrap();
        first_server.await.unwrap();
        second_server.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_pipeline_ask_retry_keeps_asking_with_affected_entry() {
        let stable_key = "pipeline-ask-stable";
        let ask_key = "pipeline-ask-moved";
        let ask_slot = slot_for_key(ask_key.as_bytes());
        let (target_addr, target_service, target_server) = scripted_service(
            vec![b"ASKING", ask_key.as_bytes()],
            b"+OK\r\n$7\r\nretried\r\n".to_vec(),
        )
        .await;
        let initial_response = format!("+stable\r\n-ASK {ask_slot} {target_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) = scripted_service(
            vec![stable_key.as_bytes(), ask_key.as_bytes()],
            initial_response,
        )
        .await;
        let recorder = Arc::new(RecordingMetrics::default());
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr,
            target_service,
            Arc::clone(&recorder),
        );
        let mut executor = client.clone();

        let responses = PipelineExecutor::execute_pipeline(
            &mut executor,
            vec![get_frame(stable_key), get_frame(ask_key)],
        )
        .await
        .unwrap();

        assert_eq!(
            responses,
            vec![
                Frame::SimpleString(Bytes::from_static(b"stable")),
                Frame::BulkString(Some(Bytes::from_static(b"retried"))),
            ]
        );
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Ask]
        );

        drop(executor);
        client.shutdown().await;
        initial_server.await.unwrap();
        target_server.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_pipeline_node_transport_failure_is_global_and_not_retried() {
        let key_ok = "{pipeline-ok}:key";
        let key_failed = "{pipeline-failed}:key";
        let slot_ok = slot_for_key(key_ok.as_bytes());
        let slot_failed = slot_for_key(key_failed.as_bytes());
        assert_ne!(slot_ok, slot_failed);

        let (addr_ok, service_ok, server_ok) =
            scripted_service(vec![key_ok.as_bytes()], b"+ok\r\n".to_vec()).await;
        // This fake accepts the batch and closes without a response. Redis may
        // have applied a write before such a close, so the client must not
        // replay this node group or return the successful sibling results as a
        // complete pipeline outcome.
        let (addr_failed, service_failed, server_failed) =
            scripted_service(vec![key_failed.as_bytes()], Vec::new()).await;
        let topology = ClusterTopology::new(vec![
            crate::topology::SlotRange {
                start: slot_ok,
                end: slot_ok,
                master: node_addr(&addr_ok),
                replicas: Vec::new(),
            },
            crate::topology::SlotRange {
                start: slot_failed,
                end: slot_failed,
                master: node_addr(&addr_failed),
                replicas: Vec::new(),
            },
        ]);
        let masters = HashMap::from([(addr_ok.clone(), service_ok), (addr_failed, service_failed)]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(addr_ok, topology, masters, recorder, false);
        let mut executor = client.clone();

        let result = PipelineExecutor::execute_pipeline(
            &mut executor,
            vec![get_frame(key_ok), get_frame(key_failed)],
        )
        .await;
        assert!(result.is_err());

        drop(executor);
        client.shutdown().await;
        server_ok.await.unwrap();
        server_failed.await.unwrap();
    }

    #[tokio::test]
    async fn split_mget_is_binary_safe_and_restores_input_order() {
        let key_a1: &'static [u8] = b"{split-a}:\0\xff-one";
        let key_b: &'static [u8] = b"{split-b}:\x80";
        let key_a2: &'static [u8] = b"{split-a}:\0\xff-two";
        let slot_a = slot_for_key(key_a1);
        let slot_b = slot_for_key(key_b);
        assert_ne!(slot_a, slot_b);

        let (addr_a, service_a, server_a) = scripted_service(
            vec![key_a1, key_a2],
            b"*2\r\n$2\r\na1\r\n$2\r\na2\r\n".to_vec(),
        )
        .await;
        let (addr_b, service_b, server_b) =
            scripted_service(vec![key_b], b"*1\r\n$2\r\nb1\r\n".to_vec()).await;
        let topology = ClusterTopology::new(vec![
            crate::topology::SlotRange {
                start: slot_a,
                end: slot_a,
                master: node_addr(&addr_a),
                replicas: Vec::new(),
            },
            crate::topology::SlotRange {
                start: slot_b,
                end: slot_b,
                master: node_addr(&addr_b),
                replicas: Vec::new(),
            },
        ]);
        let masters = HashMap::from([(addr_a.clone(), service_a), (addr_b.clone(), service_b)]);
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(addr_a, topology, masters, recorder, false);

        let values = client.mget_split([key_a1, key_b, key_a2]).await.unwrap();
        assert_eq!(
            values,
            vec![
                Some(Bytes::from_static(b"a1")),
                Some(Bytes::from_static(b"b1")),
                Some(Bytes::from_static(b"a2")),
            ]
        );

        client.shutdown().await;
        server_a.await.unwrap();
        server_b.await.unwrap();
    }

    #[tokio::test]
    async fn repeated_ask_is_counted_when_budget_is_exhausted() {
        let repeated_ask = b"+OK\r\n-ASK 42 127.0.0.1:9999\r\n".to_vec();
        let (target_addr, target_service, target_server) =
            scripted_service(vec![b"ASKING", b"GET"], repeated_ask).await;
        let ask = format!("-ASK 42 {target_addr}\r\n").into_bytes();
        let (initial_addr, initial_service, initial_server) =
            scripted_service(vec![b"GET"], ask).await;
        let recorder = Arc::new(RecordingMetrics::default());
        let client = redirect_client(
            initial_addr,
            initial_service,
            target_addr.clone(),
            target_service,
            Arc::clone(&recorder),
        );
        client.inner.write().await.max_redirects = 1;

        let result = client.execute(Get::new("key")).await;
        client.shutdown().await;
        initial_server.await.unwrap();
        target_server.await.unwrap();

        assert!(matches!(result, Err(RedisError::Redis(_))));
        assert_eq!(
            *recorder.redirects.lock().unwrap(),
            vec![ClusterRedirectKind::Ask, ClusterRedirectKind::Ask]
        );
        assert_eq!(
            *recorder.completions.lock().unwrap(),
            vec![CommandCompletion {
                command: "GET".to_string(),
                error: Some(ErrorKind::Other),
                node: Some(target_addr),
            }]
        );
    }

    #[tokio::test]
    async fn failed_topology_refresh_records_one_failure_hook() {
        let recorder = Arc::new(RecordingMetrics::default());
        let client = test_client(
            String::new(),
            ClusterTopology::new(Vec::new()),
            HashMap::new(),
            Arc::clone(&recorder),
            false,
        );

        let result = client.refresh_topology().await;
        client.shutdown().await;

        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
        let refreshes = recorder.refreshes.lock().unwrap();
        assert_eq!(refreshes.len(), 1);
        assert_eq!(refreshes[0].1, ClusterTopologyRefreshOutcome::Error);
    }
}
