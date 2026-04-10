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
//! # Current limitations
//!
//! - **ASK redirects** during slot migrations are not handled atomically.
//!   Because [`AutoPipelineService`] does not yet support sending multiple
//!   frames as a single atomic batch, we cannot reliably send
//!   `ASKING` followed by the migrated command on the same worker slot.
//!   Instead, ASK is treated like MOVED: the topology is refreshed and the
//!   command is retried against whichever node the refreshed topology
//!   reports. This is correct after migration completes but may
//!   oscillate (and eventually error with "too many redirects") during an
//!   active migration. If you need zero-error operation through resharding,
//!   use [`ClusterConnection`](crate::ClusterConnection) for now.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use redis_tower::AutoPipelineService;
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::reconnect::ConnectionFactory;
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
    ) -> Result<Self, RedisError> {
        // Discover topology via a short-lived raw connection.
        let mut seed_conn = RedisConnection::connect(seed_addr).await?;
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
            })),
        })
    }

    /// Execute a command, routing it to the correct cluster node.
    ///
    /// Handles MOVED redirects transparently by refreshing the topology
    /// slot mapping and retrying against the new owner. ASK redirects are
    /// currently treated like MOVED (see module docs for limitations).
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
                    // Without atomic multi-frame support we cannot send
                    // ASKING + cmd on the same worker slot. Refresh topology
                    // so a subsequent call uses the new owner, and retry
                    // against the ASK target (which may itself MOVED back --
                    // we rely on MAX_REDIRECTS to bound the loop).
                    let addr = self.remap_addr(&addr).await;
                    self.ensure_master(&addr).await?;
                    target = self.master_service(&addr).await?;
                    continue;
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
        let (pipeline_config, reconnect_config, host_override, address_map, read_preference) = {
            let inner = self.inner.read().await;
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
                inner.host_override.clone(),
                inner.address_map.clone(),
                inner.read_preference,
            )
        };

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
        let mut seed_conn = RedisConnection::connect(&seed_addr).await?;
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
        let (pipeline_config, reconnect_config) = {
            let inner = self.inner.read().await;
            (
                inner.pipeline_config.clone(),
                inner.reconnect_config.clone(),
            )
        };
        let svc = build_node_service(addr, false, pipeline_config, reconnect_config).await?;
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
) -> Result<AutoPipelineService, RedisError> {
    let factory = NodeConnectionFactory {
        addr: addr.to_string(),
        readonly,
    };
    AutoPipelineService::with_factory(factory, pipeline_config, reconnect_config).await
}

/// A [`ConnectionFactory`] that connects to a single node and optionally
/// sends READONLY before yielding the connection.
struct NodeConnectionFactory {
    addr: String,
    readonly: bool,
}

impl ConnectionFactory for NodeConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let addr = self.addr.clone();
        let readonly = self.readonly;
        Box::pin(async move {
            let mut conn = RedisConnection::connect(&addr).await?;
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

/// Send a single frame through an [`AutoPipelineService`] and await the
/// response. Mirrors what `MultiplexedClient::execute` does internally, but
/// stays at the frame level so the cluster routing code can reuse the same
/// service across redirects without needing `Command: Clone`.
async fn call_service(svc: &mut AutoPipelineService, frame: Frame) -> Result<Frame, RedisError> {
    std::future::poll_fn(|cx| <AutoPipelineService as Service<Frame>>::poll_ready(svc, cx)).await?;
    <AutoPipelineService as Service<Frame>>::call(svc, frame).await
}

// Note: Tower `Service<Cmd>` impl for `MultiplexedClusterClient` is deferred
// to PR 3. The current `execute` path requires `&self`, which doesn't match
// Tower's `&mut self` poll_ready/call cleanly for a cloneable shared client.
// A wrapping adapter is straightforward but adds scope.

impl std::fmt::Debug for MultiplexedClusterClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexedClusterClient").finish()
    }
}
