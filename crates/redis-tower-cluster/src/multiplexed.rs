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
//! - Each per-node connection transparently reconnects on failure via a
//!   [`ConnectionFactory`], with configurable backoff.
//! - Factories are the place to replay per-node session setup (AUTH, READONLY).
//!
//! # Example
//!
//! ```ignore
//! use redis_tower_cluster::MultiplexedClusterClient;
//! use redis_tower::commands::*;
//!
//! let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
//!
//! // Clone freely across tasks -- all share one worker per node.
//! let c = client.clone();
//! tokio::spawn(async move {
//!     c.execute(Set::new("key", "value")).await.unwrap();
//! });
//! ```
//!
//! # Redirect handling
//!
//! MOVED and ASK redirects are handled transparently. ASK is dispatched as
//! an atomic `[ASKING, cmd]` pipeline via
//! [`AutoPipelineService::call_pipeline`], so the ASKING connection state
//! set by the first frame is always consumed by our migrated command and
//! not by another in-flight request from a concurrent task.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use redis_tower::AutoPipelineService;
use redis_tower::RedisExecutor;
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::credentials::CredentialProvider;
use redis_tower::reconnect::ConnectionFactory;
use redis_tower_commands::Auth;
#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
use redis_tower_core::tls::TlsConfig;
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tokio::sync::RwLock;
use tower_service::Service;

use crate::connection::{
    MAX_REDIRECTS, ReadPreference, ReadRoutingStrategy, Redirect, RoundRobinRouting,
    parse_redirect, remap_topology, remap_topology_with_map,
};
use crate::key_extractor;
use crate::slot::slot_for_key;
use crate::topology::{ClusterTopology, NodeAddr, discover_topology};

/// A high-concurrency, multiplexed Redis Cluster client.
///
/// See the crate module-level docs (`redis_tower_cluster::multiplexed`) for
/// an overview.
pub struct MultiplexedClusterClient {
    inner: Arc<RwLock<Inner>>,
}

impl Clone for MultiplexedClusterClient {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner {
    topology: ClusterTopology,
    masters: HashMap<String, AutoPipelineService>,
    replicas: HashMap<String, AutoPipelineService>,
    default_node: String,
    host_override: Option<String>,
    address_map: Option<HashMap<String, String>>,
    read_preference: ReadPreference,
    read_routing: Arc<dyn ReadRoutingStrategy>,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    credentials: Option<Arc<dyn CredentialProvider>>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<Arc<TlsConfig>>,
}

/// Builder for configuring a [`MultiplexedClusterClient`].
pub struct MultiplexedClusterClientBuilder {
    seed_addr: String,
    host_override: Option<String>,
    address_map: Option<HashMap<String, String>>,
    read_preference: ReadPreference,
    read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    credentials: Option<Arc<dyn CredentialProvider>>,
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

    /// Set the read preference.
    pub fn read_preference(mut self, pref: ReadPreference) -> Self {
        self.read_preference = pref;
        self
    }

    /// Set a custom read routing strategy for replica selection.
    pub fn read_routing(mut self, strategy: impl ReadRoutingStrategy) -> Self {
        self.read_routing = Some(Arc::new(strategy));
        self
    }

    /// Override the auto-pipeline batching config used for each per-node worker.
    pub fn pipeline_config(mut self, config: AutoPipelineConfig) -> Self {
        self.pipeline_config = config;
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
    /// ```ignore
    /// use redis_tower_cluster::MultiplexedClusterClient;
    /// use redis_tower_core::tls::TlsConfig;
    ///
    /// let client = MultiplexedClusterClient::builder("redis.example.com:7000")
    ///     .tls(TlsConfig::default_rustls())
    ///     .connect()
    ///     .await?;
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
            self.pipeline_config,
            self.reconnect_config,
            self.credentials,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            self.tls,
        )
        .await
    }
}

impl MultiplexedClusterClient {
    /// Connect to a cluster using a seed node address.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        Self::connect_inner(
            seed_addr,
            None,
            None,
            ReadPreference::Master,
            None,
            AutoPipelineConfig::default(),
            AutoPipelineReconnectConfig::default(),
            None,
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
            AutoPipelineConfig::default(),
            AutoPipelineReconnectConfig::default(),
            None,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
        .await
    }

    /// Create a builder for configuring the client.
    pub fn builder(seed_addr: impl Into<String>) -> MultiplexedClusterClientBuilder {
        MultiplexedClusterClientBuilder {
            seed_addr: seed_addr.into(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: None,
            pipeline_config: AutoPipelineConfig::default(),
            reconnect_config: AutoPipelineReconnectConfig::default(),
            credentials: None,
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
        pipeline_config: AutoPipelineConfig,
        reconnect_config: AutoPipelineReconnectConfig,
        credentials: Option<Arc<dyn CredentialProvider>>,
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<Arc<TlsConfig>>,
    ) -> Result<Self, RedisError> {
        // Discover topology via a short-lived raw connection. Authenticate
        // before CLUSTER SLOTS so the discovery itself works against an
        // ACL-protected cluster.
        let mut seed_conn = connect_node(
            seed_addr,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls.as_deref(),
        )
        .await?;
        if let Some(ref provider) = credentials {
            authenticate(&mut seed_conn, provider.as_ref()).await?;
        }
        let mut topology = discover_topology(&mut seed_conn).await?;
        drop(seed_conn);

        if let Some(ref map) = address_map {
            remap_topology_with_map(&mut topology, map);
        }
        if let Some(ref host) = host_override {
            remap_topology(&mut topology, host);
        }

        let mut masters: HashMap<String, AutoPipelineService> = HashMap::new();
        let mut default_node = String::new();

        // Connect to all masters through factory-backed auto-pipeline services.
        for addr in topology.master_addrs() {
            let addr_str = addr.addr_string();
            if masters.contains_key(&addr_str) {
                continue;
            }
            let svc = build_node_service(
                &addr_str,
                /* readonly = */ false,
                pipeline_config.clone(),
                reconnect_config.clone(),
                credentials.clone(),
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                tls.clone(),
            )
            .await?;
            if default_node.is_empty() {
                default_node.clone_from(&addr_str);
            }
            masters.insert(addr_str, svc);
        }

        // Connect to replicas if the read preference uses them.
        let mut replicas: HashMap<String, AutoPipelineService> = HashMap::new();
        if read_preference != ReadPreference::Master {
            for addr in topology.replica_addrs() {
                let addr_str = addr.addr_string();
                if replicas.contains_key(&addr_str) {
                    continue;
                }
                let svc = build_node_service(
                    &addr_str,
                    /* readonly = */ true,
                    pipeline_config.clone(),
                    reconnect_config.clone(),
                    credentials.clone(),
                    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                    tls.clone(),
                )
                .await?;
                replicas.insert(addr_str, svc);
            }
        }

        if default_node.is_empty() {
            // No masters discovered -- fall back to the seed addr via a fresh
            // factory-backed service so keyless commands still route somewhere.
            let svc = build_node_service(
                seed_addr,
                false,
                pipeline_config.clone(),
                reconnect_config.clone(),
                credentials.clone(),
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                tls.clone(),
            )
            .await?;
            masters.insert(seed_addr.to_string(), svc);
            default_node = seed_addr.to_string();
        }

        let read_routing = read_routing.unwrap_or_else(|| Arc::new(RoundRobinRouting::new()));

        Ok(Self {
            inner: Arc::new(RwLock::new(Inner {
                topology,
                masters,
                replicas,
                default_node,
                host_override,
                address_map,
                read_preference,
                read_routing,
                pipeline_config,
                reconnect_config,
                credentials,
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                tls,
            })),
        })
    }

    /// Execute a command, routing it to the correct cluster node.
    ///
    /// Handles MOVED and ASK redirects transparently. ASK is handled by
    /// sending `ASKING` + the migrated command as an atomic pipeline through
    /// the target node, preserving single-connection ordering during
    /// live resharding.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let cmd_frame = cmd.to_frame();

        // Initial routing.
        let mut target = self.route_command(&cmd_frame).await?;

        for _ in 0..MAX_REDIRECTS {
            let response = call_service(&mut target.svc, cmd_frame.clone()).await?;

            match parse_redirect(&response) {
                Some(Redirect::Moved { slot, addr }) => {
                    let addr = self.remap_addr(&addr).await;
                    self.ensure_master(&addr).await?;
                    self.update_slot_owner(slot, &addr).await;
                    target = self.master_service(&addr).await?;
                    continue;
                }
                Some(Redirect::Ask { addr }) => {
                    let addr = self.remap_addr(&addr).await;
                    self.ensure_master(&addr).await?;
                    let mut ask_target = self.master_service(&addr).await?;
                    // Atomic [ASKING, cmd] via call_pipeline. The worker
                    // guarantees contiguous emission on the wire, so the
                    // ASKING state set on the connection is consumed by
                    // our cmd and not some other in-flight request.
                    let asking_frame = array(vec![bulk("ASKING")]);
                    let responses = ask_target
                        .svc
                        .call_pipeline(vec![asking_frame, cmd_frame.clone()])
                        .await?;
                    let cmd_response = responses
                        .into_iter()
                        .nth(1)
                        .ok_or(RedisError::ConnectionClosed)?;
                    // If ASKING + cmd returned MOVED, fall through the
                    // redirect loop to handle it as a MOVED from this node.
                    if let Some(Redirect::Moved { slot, addr }) = parse_redirect(&cmd_response) {
                        let addr = self.remap_addr(&addr).await;
                        self.ensure_master(&addr).await?;
                        self.update_slot_owner(slot, &addr).await;
                        target = self.master_service(&addr).await?;
                        continue;
                    }
                    if let Frame::Error(ref e) = cmd_response {
                        return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
                    }
                    return cmd.parse_response(cmd_response);
                }
                None => {
                    if let Frame::Error(ref e) = response {
                        return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
                    }
                    return cmd.parse_response(response);
                }
            }
        }

        Err(RedisError::Redis(format!(
            "too many redirects ({MAX_REDIRECTS})"
        )))
    }

    /// Refresh the cluster topology from a connected master.
    ///
    /// Discovers any new masters/replicas and spins up per-node services
    /// for them. Existing services for still-present nodes are preserved.
    pub async fn refresh_topology(&self) -> Result<(), RedisError> {
        // Snapshot what we need from the inner state, then release the lock
        // before doing network I/O.
        let (
            pipeline_config,
            reconnect_config,
            host_override,
            address_map,
            read_preference,
            credentials,
        ) = {
            let inner = self.inner.read().await;
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
                inner.host_override.clone(),
                inner.address_map.clone(),
                inner.read_preference,
                inner.credentials.clone(),
            )
        };
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        let tls: Option<Arc<TlsConfig>> = self.inner.read().await.tls.clone();

        // Use a short-lived raw connection to an existing master to run
        // CLUSTER SLOTS. We pick any master addr we know about.
        let seed_addr = {
            let inner = self.inner.read().await;
            inner
                .masters
                .keys()
                .next()
                .cloned()
                .ok_or(RedisError::ConnectionClosed)?
        };
        let mut seed_conn = connect_node(
            &seed_addr,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls.as_deref(),
        )
        .await?;
        if let Some(ref provider) = credentials {
            authenticate(&mut seed_conn, provider.as_ref()).await?;
        }
        let mut topology = discover_topology(&mut seed_conn).await?;
        drop(seed_conn);

        if let Some(ref map) = address_map {
            remap_topology_with_map(&mut topology, map);
        }
        if let Some(ref host) = host_override {
            remap_topology(&mut topology, host);
        }

        // Build services for any new nodes without holding the write lock
        // across connect.
        let mut new_masters: Vec<(String, AutoPipelineService)> = Vec::new();
        {
            let inner = self.inner.read().await;
            for addr in topology.master_addrs() {
                let addr_str = addr.addr_string();
                if !inner.masters.contains_key(&addr_str) {
                    let svc = build_node_service(
                        &addr_str,
                        false,
                        pipeline_config.clone(),
                        reconnect_config.clone(),
                        credentials.clone(),
                        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                        tls.clone(),
                    )
                    .await?;
                    new_masters.push((addr_str, svc));
                }
            }
        }

        let mut new_replicas: Vec<(String, AutoPipelineService)> = Vec::new();
        if read_preference != ReadPreference::Master {
            let inner = self.inner.read().await;
            for addr in topology.replica_addrs() {
                let addr_str = addr.addr_string();
                if !inner.replicas.contains_key(&addr_str) {
                    let svc = build_node_service(
                        &addr_str,
                        true,
                        pipeline_config.clone(),
                        reconnect_config.clone(),
                        credentials.clone(),
                        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                        tls.clone(),
                    )
                    .await?;
                    new_replicas.push((addr_str, svc));
                }
            }
        }

        // Commit the new topology and new services under the write lock.
        let mut inner = self.inner.write().await;
        inner.topology = topology;
        for (addr, svc) in new_masters {
            inner.masters.insert(addr, svc);
        }
        for (addr, svc) in new_replicas {
            inner.replicas.insert(addr, svc);
        }
        Ok(())
    }

    /// Get a snapshot of the current cluster topology.
    pub async fn topology(&self) -> ClusterTopology {
        self.inner.read().await.topology.clone()
    }

    /// Get the current read preference.
    pub async fn read_preference(&self) -> ReadPreference {
        self.inner.read().await.read_preference
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
                && let Some(addr) = pick_replica(&inner, slot)
                && let Some(svc) = inner.replicas.get(&addr)
            {
                return Ok(Target {
                    svc: svc.clone(),
                    _addr: addr,
                });
            }

            if let Some(addr_node) = inner.topology.master_for_slot(slot) {
                let addr_str = addr_node.addr_string();
                if let Some(svc) = inner.masters.get(&addr_str) {
                    return Ok(Target {
                        svc: svc.clone(),
                        _addr: addr_str,
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
        Ok(Target {
            svc,
            _addr: default,
        })
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
            _addr: addr.to_string(),
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
        let (pipeline_config, reconnect_config, credentials) = {
            let inner = self.inner.read().await;
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
                inner.credentials.clone(),
            )
        };
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        let tls: Option<Arc<TlsConfig>> = self.inner.read().await.tls.clone();
        let svc = build_node_service(
            addr,
            false,
            pipeline_config,
            reconnect_config,
            credentials,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls,
        )
        .await?;
        let mut inner = self.inner.write().await;
        inner.masters.entry(addr.to_string()).or_insert(svc);
        Ok(())
    }

    async fn update_slot_owner(&self, slot: u16, addr: &str) {
        let mut inner = self.inner.write().await;
        if let Some((host, port_str)) = addr.rsplit_once(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            for range in &mut inner.topology.slot_ranges {
                if slot >= range.start && slot <= range.end {
                    range.master = NodeAddr {
                        host: host.to_string(),
                        port,
                    };
                    return;
                }
            }
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

struct Target {
    svc: AutoPipelineService,
    _addr: String,
}

fn pick_replica(inner: &Inner, slot: u16) -> Option<String> {
    let replicas = inner.topology.replicas_for_slot(slot)?;
    if replicas.is_empty() {
        return None;
    }
    let selected = inner.read_routing.select_replica(slot, replicas)?;
    Some(selected.addr_string())
}

/// Build a per-node [`AutoPipelineService`] backed by a reconnecting factory.
async fn build_node_service(
    addr: &str,
    readonly: bool,
    pipeline_config: AutoPipelineConfig,
    reconnect_config: AutoPipelineReconnectConfig,
    credentials: Option<Arc<dyn CredentialProvider>>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<Arc<TlsConfig>>,
) -> Result<AutoPipelineService, RedisError> {
    let factory = NodeConnectionFactory {
        addr: addr.to_string(),
        readonly,
        credentials,
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        tls,
    };
    AutoPipelineService::with_factory(factory, pipeline_config, reconnect_config).await
}

/// Open a raw [`RedisConnection`] to `addr`, using TLS if configured.
///
/// The TLS hostname is taken from the host portion of `addr` (the part
/// before the final `:`). For TLS peers that report internal IPs, combine
/// with [`MultiplexedClusterClientBuilder::host_override`] so the SNI
/// hostname matches the certificate.
async fn connect_node(
    addr: &str,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<&TlsConfig>,
) -> Result<RedisConnection, RedisError> {
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    if let Some(tls) = tls {
        let hostname = addr
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(addr)
            .to_string();
        return RedisConnection::connect_tls(addr, &hostname, tls).await;
    }
    RedisConnection::connect(addr).await
}

/// A [`ConnectionFactory`] that connects to a single node and optionally
/// authenticates and/or sends READONLY before yielding the connection.
///
/// Order on each (re)connect:
/// 1. Open TCP (or TLS if configured) to `addr`.
/// 2. If `credentials` is set, fetch fresh credentials from the provider
///    and send AUTH. Fetching on every reconnect means credential rotation
///    flows through automatically.
/// 3. If `readonly` is set (replica node), send READONLY so reads to this
///    connection succeed.
struct NodeConnectionFactory {
    addr: String,
    readonly: bool,
    credentials: Option<Arc<dyn CredentialProvider>>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<Arc<TlsConfig>>,
}

impl ConnectionFactory for NodeConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        let readonly = self.readonly;
        let credentials = self.credentials.clone();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        let tls = self.tls.clone();
        Box::pin(async move {
            let mut conn = connect_node(
                &addr,
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                tls.as_deref(),
            )
            .await?;
            if let Some(provider) = credentials {
                authenticate(&mut conn, provider.as_ref()).await?;
            }
            if readonly {
                let responses = conn
                    .execute_pipeline(vec![array(vec![bulk("READONLY")])])
                    .await?;
                if let Some(Frame::Error(ref e)) = responses.into_iter().next() {
                    return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
                }
            }
            Ok(conn)
        })
    }
}

/// Fetch credentials from the provider and send AUTH on the given connection.
async fn authenticate(
    conn: &mut RedisConnection,
    provider: &dyn CredentialProvider,
) -> Result<(), RedisError> {
    let creds = provider.get_credentials().await?;
    let auth_cmd = match creds.username.as_deref() {
        Some(user) => Auth::credentials(user, &creds.password),
        None => Auth::password(&creds.password),
    };
    let responses = conn.execute_pipeline(vec![auth_cmd.to_frame()]).await?;
    match responses.into_iter().next() {
        Some(Frame::SimpleString(s)) if &s[..] == b"OK" => Ok(()),
        Some(Frame::Error(e)) => Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned())),
        Some(other) => Err(RedisError::UnexpectedResponse {
            expected: "OK",
            actual: format!("{other:?}"),
        }),
        None => Err(RedisError::ConnectionClosed),
    }
}

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
