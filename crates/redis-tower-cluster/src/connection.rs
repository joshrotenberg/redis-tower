//! Cluster-aware Redis connection that routes commands by slot.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use redis_tower::credentials::{CredentialProvider, StaticCredentials};
use redis_tower_commands::Auth;
use redis_tower_core::{
    Command, ConnectionConfig, Frame, RedisConnection, RedisError, RespLimits, parse_redis_url,
};
use redis_tower_protocol::helpers::{array, bulk};

use crate::key_extractor;
use crate::slot::slot_for_key;
use crate::topology::{ClusterTopology, NodeAddr, discover_topology};

/// Strategy for selecting which replica to read from.
///
/// Implement this trait to provide custom replica selection logic.
/// Built-in implementations include [`RoundRobinRouting`], [`RandomRouting`],
/// and [`FirstReplicaRouting`].
pub trait ReadRoutingStrategy: Send + Sync + 'static {
    /// Select a replica address for the given slot.
    ///
    /// `replicas` is the list of available replica addresses for the slot.
    /// Return the selected address, or `None` to fall back to the master.
    fn select_replica<'a>(&self, slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr>;
}

/// Round-robin across replicas (default).
///
/// Distributes reads evenly across all available replicas for a slot
/// by cycling through them in order.
pub struct RoundRobinRouting {
    counter: AtomicUsize,
}

impl RoundRobinRouting {
    /// Create a new round-robin routing strategy.
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinRouting {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRoutingStrategy for RoundRobinRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % replicas.len();
        Some(&replicas[idx])
    }
}

/// Pseudo-random replica selection.
///
/// Uses an atomic counter with a time-based seed to approximate random
/// distribution without requiring an external RNG dependency.
pub struct RandomRouting {
    counter: AtomicUsize,
}

impl RandomRouting {
    /// Create a new random routing strategy.
    pub fn new() -> Self {
        // Seed from the current time for a pseudo-random starting point.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        Self {
            counter: AtomicUsize::new(seed),
        }
    }
}

impl Default for RandomRouting {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRoutingStrategy for RandomRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }
        // Mix the counter value to spread selections across replicas.
        let val = self.counter.fetch_add(7919, Ordering::Relaxed);
        let idx = val % replicas.len();
        Some(&replicas[idx])
    }
}

/// Always pick the first replica.
///
/// Useful for testing or when replicas are ordered by preference
/// (e.g., closest datacenter first).
pub struct FirstReplicaRouting;

impl ReadRoutingStrategy for FirstReplicaRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        replicas.first()
    }
}

/// Maximum number of redirects to follow before giving up.
pub(crate) const MAX_REDIRECTS: usize = 5;

/// Backoff applied before retrying a transient cluster error (TRYAGAIN,
/// CLUSTERDOWN, LOADING). Retries share the redirect budget, so the total
/// transient wait is bounded by `max_redirects * TRANSIENT_RETRY_BACKOFF`.
pub(crate) const TRANSIENT_RETRY_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(50);

/// Read routing preference for cluster commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadPreference {
    /// Always read from the master (default).
    #[default]
    Master,
    /// Read from a replica if available.
    Replica,
    /// Prefer replica, fall back to master.
    PreferReplica,
}

/// A Redis Cluster connection that routes commands to the correct node.
///
/// Discovers the cluster topology via CLUSTER SLOTS on the seed node,
/// then maintains connections to masters (and optionally replicas).
///
/// Handles MOVED and ASK redirects automatically. Supports read
/// preference for routing read-only commands to replicas.
///
/// # Concurrency
///
/// `ClusterConnection` requires exclusive (`&mut self`) access for all
/// operations. It is NOT `Clone`. Share it via
/// [`ClusterClient`](crate::client::ClusterClient)
/// (`Arc<Mutex<ClusterConnection>>`) or use it directly in a single task.
/// For high-concurrency sharing, use
/// [`MultiplexedClusterClient`](crate::multiplexed::MultiplexedClusterClient).
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_cluster::{ClusterConnection, ReadPreference};
/// use redis_tower_commands::{Get, Set};
///
/// let mut cluster = ClusterConnection::builder("127.0.0.1:7000")
///     .read_preference(ReadPreference::PreferReplica)
///     .connect()
///     .await?;
///
/// cluster.execute(Set::new("key", "value")).await?;
/// let val = cluster.execute(Get::new("key")).await?; // routed to replica
/// # let _ = val;
/// # Ok(())
/// # }
/// ```
pub struct ClusterConnection {
    /// Connections to nodes, keyed by "host:port".
    nodes: HashMap<String, RedisConnection>,
    /// Current cluster topology.
    topology: ClusterTopology,
    /// Address of a node to use for keyless commands.
    default_node: String,
    /// Host override for Docker/proxy environments.
    host_override: Option<String>,
    /// Per-address mapping for NAT/Kubernetes environments.
    /// Keys are "internal_host:port", values are "external_host:port".
    address_map: Option<HashMap<String, String>>,
    /// Read routing preference.
    read_preference: ReadPreference,
    /// Strategy for selecting which replica to read from.
    read_routing: Arc<dyn ReadRoutingStrategy>,
    /// Maximum MOVED/ASK redirects to follow for a single command.
    max_redirects: usize,
    /// Credential provider for authenticating each node connection.
    credentials: Option<Arc<dyn CredentialProvider>>,
    /// Decode limits applied to every cluster node connection.
    resp_limits: RespLimits,
    /// TLS configuration for node connections.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<Arc<redis_tower_core::tls::TlsConfig>>,
}

/// Builder for configuring a `ClusterConnection`.
pub struct ClusterConnectionBuilder {
    seed_addr: String,
    host_override: Option<String>,
    address_map: Option<HashMap<String, String>>,
    read_preference: ReadPreference,
    read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
    max_redirects: usize,
    credentials: Option<Arc<dyn CredentialProvider>>,
    resp_limits: RespLimits,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<Arc<redis_tower_core::tls::TlsConfig>>,
}

impl ClusterConnectionBuilder {
    /// Set the host override for Docker/proxy environments.
    pub fn host_override(mut self, host: impl Into<String>) -> Self {
        self.host_override = Some(host.into());
        self
    }

    /// Map internal cluster addresses to external addresses.
    ///
    /// In NAT/Kubernetes environments, cluster nodes report internal IPs
    /// that aren't reachable from the client. This mapping translates
    /// internal addresses to external ones.
    ///
    /// Keys are `"internal_host:port"` and values are `"external_host:port"`.
    /// The address map is checked before `host_override`, so explicit
    /// per-address mappings take priority.
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
    ///
    /// When a read-only command is routed to a replica (based on
    /// [`ReadPreference`]), this strategy determines which replica to use.
    /// If not set, defaults to [`RoundRobinRouting`].
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

    /// Authenticate every node connection (seed, masters, replicas, and
    /// reconnects) using the given credential provider.
    ///
    /// Required for ACL-protected or password-protected clusters (most Cloud
    /// and Enterprise deployments). The provider is consulted on every
    /// connection, so credential rotation flows through automatically. Use
    /// [`StaticCredentials`](redis_tower::credentials::StaticCredentials) for a
    /// fixed username/password.
    pub fn credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.credentials = Some(Arc::new(provider));
        self
    }

    /// Set RESP decode limits for every cluster connection.
    ///
    /// The limits apply before any handshake or authentication frames are
    /// decoded and are retained for seed discovery, masters, replicas,
    /// redirect-created connections, and topology refreshes.
    pub fn resp_limits(mut self, limits: RespLimits) -> Self {
        self.resp_limits = limits;
        self
    }

    /// Set the TLS configuration for cluster connections.
    ///
    /// When set, all connections to cluster nodes (seed, masters, and
    /// replicas) will use TLS. The hostname for SNI verification is
    /// derived from each node's address.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.tls = Some(Arc::new(tls));
        self
    }

    /// Connect to the cluster.
    pub async fn connect(self) -> Result<ClusterConnection, RedisError> {
        ClusterConnection::connect_inner(
            &self.seed_addr,
            self.host_override,
            self.address_map,
            self.read_preference,
            self.read_routing,
            self.max_redirects,
            self.credentials,
            self.resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            self.tls,
        )
        .await
    }
}

/// Parsed redirect from a MOVED or ASK error.
#[derive(Debug)]
pub(crate) enum Redirect {
    Moved { slot: u16, addr: String },
    Ask { slot: u16, addr: String },
}

impl ClusterConnection {
    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        Self {
            nodes: HashMap::new(),
            topology: ClusterTopology::new(Vec::new()),
            default_node: String::new(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        }
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
            None,
            RespLimits::default(),
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
            None,
            RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
        .await
    }

    /// Connect to a cluster from a Redis URL.
    ///
    /// `redis://[user:pass@]host:port` connects in the clear; `rediss://...`
    /// enables TLS (rustls -- system roots with a webpki-roots fallback, so it
    /// validates against managed Redis out of the box). A username/password in
    /// the URL authenticates every node connection (ACL user, or legacy
    /// password-only for `redis://:pass@`). For a custom TLS config or host
    /// override, use [`builder`](Self::builder).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower_cluster::ClusterConnection;
    ///
    /// let cluster =
    ///     ClusterConnection::connect_url("rediss://default:secret@cluster.example.com:6379")
    ///         .await?;
    /// # let _ = cluster;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let (seed, credentials, tls) = parse_cluster_url(url)?;
        let mut builder = Self::builder(seed);
        if let Some(creds) = credentials {
            builder = builder.credentials(creds);
        }
        if tls {
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            {
                builder = builder.tls(default_url_tls());
            }
            #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
            {
                return Err(tls_feature_required());
            }
        }
        builder.connect().await
    }

    /// Create a builder for configuring the connection.
    pub fn builder(seed_addr: impl Into<String>) -> ClusterConnectionBuilder {
        ClusterConnectionBuilder {
            seed_addr: seed_addr.into(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: None,
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
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
        credentials: Option<Arc<dyn CredentialProvider>>,
        resp_limits: RespLimits,
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<
            Arc<redis_tower_core::tls::TlsConfig>,
        >,
    ) -> Result<Self, RedisError> {
        let mut seed_conn = connect_node(
            seed_addr,
            credentials.as_ref(),
            resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls.as_deref(),
        )
        .await?;
        let mut topology = discover_topology(&mut seed_conn).await?;

        if let Some(ref map) = address_map {
            remap_topology_with_map(&mut topology, map);
        }
        if let Some(ref host) = host_override {
            remap_topology(&mut topology, host);
        }

        let mut nodes = HashMap::new();
        let mut default_node = String::new();

        // Connect to all masters.
        for addr in topology.master_addrs() {
            let addr_str = addr.addr_string();
            if let std::collections::hash_map::Entry::Vacant(e) = nodes.entry(addr_str.clone()) {
                let conn = connect_node(
                    &addr_str,
                    credentials.as_ref(),
                    resp_limits,
                    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                    tls.as_deref(),
                )
                .await?;
                if default_node.is_empty() {
                    default_node.clone_from(&addr_str);
                }
                e.insert(conn);
            }
        }

        // Connect to replicas if read preference uses them.
        if read_preference != ReadPreference::Master {
            for addr in topology.replica_addrs() {
                let addr_str = addr.addr_string();
                if let std::collections::hash_map::Entry::Vacant(e) = nodes.entry(addr_str.clone())
                {
                    let mut conn = connect_node(
                        &addr_str,
                        credentials.as_ref(),
                        resp_limits,
                        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                        tls.as_deref(),
                    )
                    .await?;
                    // Send READONLY to enable reads on this replica.
                    conn.execute_pipeline(vec![array(vec![bulk("READONLY")])])
                        .await?;
                    e.insert(conn);
                }
            }
        }

        if default_node.is_empty() {
            nodes.insert(seed_addr.to_string(), seed_conn);
            default_node = seed_addr.to_string();
        }

        let read_routing = read_routing.unwrap_or_else(|| Arc::new(RoundRobinRouting::new()));

        Ok(Self {
            nodes,
            topology,
            default_node,
            host_override,
            address_map,
            read_preference,
            read_routing,
            max_redirects,
            credentials,
            resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls,
        })
    }

    /// Execute a command, routing it to the correct cluster node.
    ///
    /// Handles MOVED and ASK redirects transparently. A deadline carried by
    /// [`redis_tower_core::WithDeadline`] bounds the complete routing,
    /// redirect, reconnect, and retry operation.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            return Err(RedisError::CommandTimeout);
        }
        let operation = self.execute_routed(cmd);

        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, operation)
                .await
                .map_err(|_elapsed| RedisError::CommandTimeout)?,
            None => operation.await,
        }
    }

    /// Execute after the caller has captured and arranged enforcement of the
    /// command deadline. Keeping this separate lets [`crate::ClusterClient`]
    /// include acquisition of its cluster-wide mutex in the same timeout.
    pub(crate) async fn execute_routed<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> Result<Cmd::Response, RedisError> {
        let cmd_frame = cmd.to_frame();
        let initial_node = self.route_command(&cmd_frame).to_string();

        let mut target_node = initial_node;
        let mut send_asking = false;
        let mut followups_used = 0usize;

        loop {
            // A previous command deadline may have quarantined this node's
            // socket while a reply was outstanding. Reconnect lazily before
            // routing the next command to it.
            self.ensure_connection(&target_node).await?;
            let response = if send_asking {
                // ASKING is connection-local and one-shot, so keep it in the
                // same checked-out exchange as the migrated command.
                self.execute_pipeline_on_node(
                    &target_node,
                    vec![array(vec![bulk("ASKING")]), cmd_frame.clone()],
                )
                .await?
                .into_iter()
                .nth(1)
                .ok_or(RedisError::ConnectionClosed)?
            } else {
                self.execute_pipeline_on_node(&target_node, vec![cmd_frame.clone()])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or(RedisError::ConnectionClosed)?
            };

            match parse_redirect(&response) {
                Some(Redirect::Moved { slot, addr }) => {
                    tracing::debug!(slot, from_addr = %target_node, to_addr = %addr, kind = "MOVED", "cluster redirect");
                    if followups_used >= self.max_redirects {
                        break;
                    }
                    followups_used += 1;
                    let addr = self.remap_addr(&addr);
                    self.ensure_connection(&addr).await?;
                    self.update_slot_owner(slot, &addr);
                    target_node = addr;
                    send_asking = false;
                    continue;
                }
                Some(Redirect::Ask { slot, addr }) => {
                    tracing::debug!(slot, to_addr = %addr, kind = "ASK", "cluster redirect");
                    if followups_used >= self.max_redirects {
                        break;
                    }
                    followups_used += 1;
                    let addr = self.remap_addr(&addr);
                    target_node = addr;
                    send_asking = true;
                    continue;
                }
                None => {
                    // Transient cluster errors: retry within the redirect
                    // budget rather than surfacing on first occurrence.
                    if let Some(transient) = TransientError::from_frame(&response) {
                        if followups_used >= self.max_redirects {
                            break;
                        }
                        followups_used += 1;
                        if transient == TransientError::ClusterDown {
                            // The cluster view may be stale (election / moved
                            // slots); refresh best-effort and re-route the key.
                            let _ = self.refresh_topology().await;
                            target_node = self.route_command(&cmd_frame).to_string();
                            send_asking = false;
                        }
                        tracing::debug!(?transient, node = %target_node, "transient cluster error; retrying");
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
            "too many redirects ({})",
            self.max_redirects
        )))
    }

    /// Run a pipeline on one node while making cancellation quarantine-safe.
    ///
    /// The connection is removed from the reusable node map before the first
    /// wire await and returned only after every response is read successfully.
    /// Dropping this future (for example when a command deadline expires), or
    /// encountering an I/O/protocol error, therefore drops the socket instead
    /// of leaving an unread response for a successor.
    async fn execute_pipeline_on_node(
        &mut self,
        addr: &str,
        frames: Vec<Frame>,
    ) -> Result<Vec<Frame>, RedisError> {
        let mut conn = self
            .nodes
            .remove(addr)
            .ok_or_else(|| RedisError::Redis(format!("no connection for node {addr}")))?;
        match conn.execute_pipeline(frames).await {
            Ok(responses) => {
                self.nodes.insert(addr.to_string(), conn);
                Ok(responses)
            }
            Err(error) => Err(error),
        }
    }

    /// Run a complete WATCH/MULTI/EXEC exchange on one master connection.
    ///
    /// As with [`execute_pipeline_on_node`](Self::execute_pipeline_on_node),
    /// the connection is removed from the reusable map before the first wire
    /// await. Cancellation or an incomplete exchange therefore quarantines
    /// the socket instead of leaving transaction state behind for a successor.
    async fn execute_transaction_on_node(
        &mut self,
        addr: &str,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> Result<Option<Vec<Frame>>, RedisError> {
        let mut conn = self
            .nodes
            .remove(addr)
            .ok_or_else(|| RedisError::Redis(format!("no connection for node {addr}")))?;
        match conn.execute_transaction(watch_frames, command_frames).await {
            Ok(responses) => {
                self.nodes.insert(addr.to_string(), conn);
                Ok(responses)
            }
            Err(error) => Err(error),
        }
    }

    /// Resolve a hash slot to its master. Only a genuinely keyless transaction
    /// may use the default node; a keyed transaction fails closed when the
    /// current topology cannot prove who owns its slot.
    fn transaction_node(&self, slot: Option<u16>) -> Result<String, RedisError> {
        match slot {
            Some(slot) => self
                .topology
                .master_for_slot(slot)
                .map(NodeAddr::addr_string)
                .ok_or_else(|| {
                    RedisError::Redis(format!(
                        "CLUSTERDOWN no known master owns transaction slot {slot}"
                    ))
                }),
            None => Ok(self.default_node.clone()),
        }
    }

    /// Determine which node should handle a command based on its key
    /// and read preference.
    fn route_command(&self, frame: &Frame) -> &str {
        if let Some(key) = key_extractor::extract_key(frame) {
            let slot = slot_for_key(key);

            // For read-only commands with replica preference, try a replica.
            if self.read_preference != ReadPreference::Master
                && key_extractor::is_readonly_command(frame)
            {
                if let Some(addr) = self.pick_replica(slot) {
                    return addr;
                }
                // PreferReplica falls through to master.
                if self.read_preference == ReadPreference::Replica {
                    // Strict Replica mode but no replica found -- fall through to master.
                }
            }

            if let Some(addr) = self.topology.master_for_slot(slot) {
                let addr_str = addr.addr_string();
                for node_key in self.nodes.keys() {
                    if *node_key == addr_str {
                        return node_key;
                    }
                }
            }
        }
        &self.default_node
    }

    /// Pick a replica for a given slot using the configured read routing strategy.
    fn pick_replica(&self, slot: u16) -> Option<&str> {
        let replicas = self.topology.replicas_for_slot(slot)?;
        if replicas.is_empty() {
            return None;
        }
        let selected = self.read_routing.select_replica(slot, replicas)?;
        let addr_str = selected.addr_string();
        self.nodes
            .keys()
            .find(|k| **k == addr_str)
            .map(|v| v.as_str())
    }

    /// Remap an address using the address map or host override.
    ///
    /// The address map is checked first for an exact match. If no match
    /// is found, the host override is applied (replacing the host but
    /// keeping the port).
    fn remap_addr(&self, addr: &str) -> String {
        if let Some(ref map) = self.address_map
            && let Some(mapped) = map.get(addr)
        {
            return mapped.clone();
        }
        if let Some(ref host) = self.host_override
            && let Some((_old_host, port)) = addr.rsplit_once(':')
        {
            return format!("{host}:{port}");
        }
        addr.to_string()
    }

    /// Ensure we have a connection to the given address.
    async fn ensure_connection(&mut self, addr: &str) -> Result<(), RedisError> {
        if !self.nodes.contains_key(addr) {
            let conn = connect_node(
                addr,
                self.credentials.as_ref(),
                self.resp_limits,
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                self.tls.as_deref(),
            )
            .await?;
            self.nodes.insert(addr.to_string(), conn);
        }
        Ok(())
    }

    /// Update the topology to assign a single slot to a new node (after MOVED).
    ///
    /// Patches only the named slot, splitting its containing range if needed,
    /// so a single-slot MOVED during resharding does not steal the whole range.
    /// See [`ClusterTopology::reassign_slot`].
    fn update_slot_owner(&mut self, slot: u16, addr: &str) {
        if let Some((host, port_str)) = addr.rsplit_once(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            self.topology.reassign_slot(
                slot,
                NodeAddr {
                    host: host.to_string(),
                    port,
                },
            );
        }
    }

    /// Get the current cluster topology.
    pub fn topology(&self) -> &ClusterTopology {
        &self.topology
    }

    /// Get the current read preference.
    pub fn read_preference(&self) -> ReadPreference {
        self.read_preference
    }

    /// Refresh the cluster topology from a connected node.
    pub async fn refresh_topology(&mut self) -> Result<(), RedisError> {
        let seed_addr = self
            .nodes
            .keys()
            .next()
            .cloned()
            .ok_or(RedisError::ConnectionClosed)?;
        let mut conn = self
            .nodes
            .remove(&seed_addr)
            .ok_or(RedisError::ConnectionClosed)?;
        let mut topology = match discover_topology(&mut conn).await {
            Ok(topology) => {
                self.nodes.insert(seed_addr, conn);
                topology
            }
            Err(error) => return Err(error),
        };

        if let Some(ref map) = self.address_map {
            remap_topology_with_map(&mut topology, map);
        }
        if let Some(ref host) = self.host_override {
            remap_topology(&mut topology, host);
        }

        for addr in topology.master_addrs() {
            let addr_str = addr.addr_string();
            if let std::collections::hash_map::Entry::Vacant(e) = self.nodes.entry(addr_str.clone())
            {
                let conn = connect_node(
                    &addr_str,
                    self.credentials.as_ref(),
                    self.resp_limits,
                    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                    self.tls.as_deref(),
                )
                .await?;
                e.insert(conn);
            }
        }

        if self.read_preference != ReadPreference::Master {
            for addr in topology.replica_addrs() {
                let addr_str = addr.addr_string();
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.nodes.entry(addr_str.clone())
                {
                    let mut conn = connect_node(
                        &addr_str,
                        self.credentials.as_ref(),
                        self.resp_limits,
                        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                        self.tls.as_deref(),
                    )
                    .await?;
                    conn.execute_pipeline(vec![array(vec![bulk("READONLY")])])
                        .await?;
                    e.insert(conn);
                }
            }
        }

        self.topology = topology;
        Ok(())
    }
}

impl redis_tower::RedisExecutor for ClusterConnection {
    fn execute<Cmd: redis_tower_core::Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl std::future::Future<Output = Result<Cmd::Response, redis_tower_core::RedisError>> + Send
    {
        ClusterConnection::execute(self, cmd)
    }
}

/// Slot-pinned WATCH/MULTI/EXEC for the exclusive cluster connection.
///
/// Every key is validated before a node connection is touched. Transactions
/// with keys from different hash slots are rejected client-side, while same-
/// slot transactions are sent in their entirety to that slot's master. The
/// exchange is never replayed after an ambiguous transport failure or cluster
/// redirect. [`redis_tower::Transaction::watch`] can protect a body that is
/// already known in the same preflighted, pinned exchange. The closure-based
/// `redis_tower::transaction` helpers are intentionally unavailable: their
/// standalone WATCH/read/build sequence cannot reserve one cluster node
/// connection safely, so read/compute/build optimistic locking is unsupported.
impl redis_tower::TransactionExecutor for ClusterConnection {
    const SUPPORTS_TRANSACTION_RETRY: bool = false;

    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
        let mut transaction_frames =
            Vec::with_capacity(watch_frames.len().saturating_add(command_frames.len()));
        transaction_frames.extend(watch_frames.iter().cloned());
        transaction_frames.extend(command_frames.iter().cloned());
        let slot = key_extractor::common_slot(&transaction_frames)
            .map_err(|error| RedisError::Redis(error.to_string()));

        async move {
            let slot = slot?;
            let node = self.transaction_node(slot)?;
            self.ensure_connection(&node).await?;
            let result = self
                .execute_transaction_on_node(&node, watch_frames, command_frames)
                .await;

            // A transaction is never replayed: after WATCH or MULTI reaches
            // Redis, a retry could violate optimistic-locking or duplicate an
            // applied write. A MOVED response is nevertheless authoritative
            // topology feedback, so patch that one slot for a freshly built
            // future transaction before surfacing the original error.
            if let Err(RedisError::Redis(message)) = &result {
                let redirect = Frame::Error(message.clone().into());
                if let Some(Redirect::Moved { slot, addr }) = parse_redirect(&redirect) {
                    let addr = self.remap_addr(&addr);
                    self.update_slot_owner(slot, &addr);
                }
            }

            result
        }
    }
}

// Note: Service::call routes to the correct node but does NOT handle
// MOVED/ASK redirects. Use `execute()` for full redirect handling.
// The Service impl enables Tower middleware composition (caching, timeouts).
impl<Cmd: Command + 'static> tower_service::Service<Cmd> for ClusterConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    /// Cluster poll_ready returns Ready because the target node is not known
    /// until `call` inspects the command's key. Per-node readiness is checked
    /// implicitly when the inner connection's call is invoked.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let cmd_frame = cmd.to_frame();
        let node_addr = self.route_command(&cmd_frame).to_string();

        match self.nodes.get_mut(&node_addr) {
            Some(conn) => <RedisConnection as tower_service::Service<Cmd>>::call(conn, cmd),
            None => Box::pin(async { Err(RedisError::ConnectionClosed) }),
        }
    }
}

/// Parse a MOVED or ASK redirect from an error frame.
pub(crate) fn parse_redirect(frame: &Frame) -> Option<Redirect> {
    let error_msg = match frame {
        Frame::Error(e) => String::from_utf8_lossy(e),
        _ => return None,
    };

    let parts: Vec<&str> = error_msg.splitn(3, ' ').collect();
    if parts.len() != 3 {
        return None;
    }

    match parts[0] {
        "MOVED" => {
            let slot = parts[1].parse::<u16>().ok()?;
            Some(Redirect::Moved {
                slot,
                addr: parts[2].to_string(),
            })
        }
        "ASK" => {
            let slot = parts[1].parse::<u16>().ok()?;
            Some(Redirect::Ask {
                slot,
                addr: parts[2].to_string(),
            })
        }
        _ => None,
    }
}

/// A transient cluster error worth retrying within the redirect budget.
///
/// Unlike a hard command error (WRONGTYPE, etc.), these reflect a momentary
/// cluster state -- a slot mid-migration, an election window, or a node still
/// loading -- that typically clears on a short retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransientError {
    /// `TRYAGAIN`: a multi-key command spans a slot that is mid-migration.
    TryAgain,
    /// `CLUSTERDOWN`: the cluster cannot currently serve the request (election
    /// window, or a slot no node is serving). Worth a topology refresh.
    ClusterDown,
    /// `LOADING`: the target node is still loading its dataset into memory.
    Loading,
}

impl TransientError {
    /// Classify an error frame as a transient cluster error, if it is one.
    pub(crate) fn from_frame(frame: &Frame) -> Option<Self> {
        let Frame::Error(e) = frame else {
            return None;
        };
        let msg = String::from_utf8_lossy(e);
        match msg.split(' ').next() {
            Some("TRYAGAIN") => Some(Self::TryAgain),
            Some("CLUSTERDOWN") => Some(Self::ClusterDown),
            Some("LOADING") => Some(Self::Loading),
            _ => None,
        }
    }
}

/// Connect to a single cluster node, using TLS if configured.
async fn connect_node(
    addr: &str,
    credentials: Option<&Arc<dyn CredentialProvider>>,
    resp_limits: RespLimits,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<
        &redis_tower_core::tls::TlsConfig,
    >,
) -> Result<RedisConnection, RedisError> {
    let connection_config = ConnectionConfig::new().with_resp_limits(resp_limits);
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    let mut conn = match tls {
        Some(tls) => {
            let hostname = addr
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(addr)
                .to_string();
            RedisConnection::connect_tls_with_config(addr, &hostname, tls, &connection_config)
                .await?
        }
        None => RedisConnection::connect_with_config(addr, &connection_config).await?,
    };
    #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
    let mut conn = RedisConnection::connect_with_config(addr, &connection_config).await?;

    if let Some(provider) = credentials {
        authenticate(&mut conn, provider.as_ref()).await?;
    }
    Ok(conn)
}

/// Authenticate a freshly opened node connection using the credential provider.
///
/// Shared by every cluster node connection (seed, masters, replicas, and
/// reconnects), so an ACL-protected Cloud/Enterprise cluster is reachable.
pub(crate) async fn authenticate(
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

/// Parse a Redis URL into a cluster seed address and an optional credential
/// provider, shared by both clients' `connect_url`. Returns the TLS flag so the
/// caller can wire it through its own (cfg-gated) builder.
///
/// `redis://[user:pass@]host:port` -> AUTH credentials from the URL;
/// `rediss://` sets `tls = true`. Unix-socket URLs are rejected (a cluster
/// needs reachable TCP seed nodes).
pub(crate) fn parse_cluster_url(
    url: &str,
) -> Result<(String, Option<StaticCredentials>, bool), RedisError> {
    let parsed = parse_redis_url(url)?;
    if parsed.unix {
        return Err(RedisError::InvalidUrl(
            "unix socket URLs are not supported for cluster connections".to_string(),
        ));
    }
    let seed = format!("{}:{}", parsed.host, parsed.port);
    let credentials = parsed.password.map(|password| match parsed.username {
        // ACL user (`redis://user:pass@`) vs legacy password-only
        // (`redis://:pass@`, the `requirepass` case).
        Some(user) if !user.is_empty() => StaticCredentials::new(user, password),
        _ => StaticCredentials::password(password),
    });
    Ok((seed, credentials, parsed.tls))
}

/// Build the default TLS config for a `rediss://` URL: rustls when available
/// (validating against the system roots with a webpki-roots fallback -- the
/// modern, Cloud-friendly default), otherwise native-tls. For a custom config,
/// use the builder's `.tls()`.
#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
pub(crate) fn default_url_tls() -> redis_tower_core::tls::TlsConfig {
    #[cfg(feature = "tls-rustls")]
    {
        redis_tower_core::tls::TlsConfig::default_rustls()
    }
    #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
    {
        redis_tower_core::tls::TlsConfig::default_native_tls()
    }
}

/// Error for a `rediss://` URL when no TLS feature is enabled.
#[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
pub(crate) fn tls_feature_required() -> RedisError {
    RedisError::InvalidUrl(
        "rediss:// requires the `tls-rustls` or `tls-native-tls` feature".to_string(),
    )
}

/// Remap all node addresses in a topology to use a specific host.
pub(crate) fn remap_topology(topology: &mut ClusterTopology, host: &str) {
    for range in topology.slot_ranges_mut() {
        range.master.host = host.to_string();
        for replica in &mut range.replicas {
            replica.host = host.to_string();
        }
    }
}

/// Remap node addresses in a topology using an address map.
///
/// Each key in `map` is `"internal_host:port"` and the value is
/// `"external_host:port"`. Only matching addresses are remapped.
pub(crate) fn remap_topology_with_map(
    topology: &mut ClusterTopology,
    map: &HashMap<String, String>,
) {
    for range in topology.slot_ranges_mut() {
        remap_node_addr(&mut range.master, map);
        for replica in &mut range.replicas {
            remap_node_addr(replica, map);
        }
    }
}

/// Remap a single `NodeAddr` if it matches an entry in the address map.
fn remap_node_addr(node: &mut NodeAddr, map: &HashMap<String, String>) {
    let key = node.addr_string();
    if let Some(mapped) = map.get(&key)
        && let Some((host, port_str)) = mapped.rsplit_once(':')
        && let Ok(port) = port_str.parse::<u16>()
    {
        node.host = host.to_string();
        node.port = port;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::SlotRange;
    use bytes::Bytes;
    use redis_tower_commands::{Get, Ping};
    use redis_tower_core::{RedisStream, WithDeadline};
    use tokio::io::AsyncReadExt;
    #[cfg(unix)]
    use tokio::io::AsyncWriteExt;

    #[cfg(unix)]
    fn topology_for_addr(addr: &str, start: u16, end: u16) -> ClusterTopology {
        let (host, port) = addr.rsplit_once(':').unwrap();
        ClusterTopology::new(vec![SlotRange {
            start,
            end,
            master: NodeAddr {
                host: host.to_string(),
                port: port.parse().unwrap(),
            },
            replicas: Vec::new(),
        }])
    }

    #[cfg(unix)]
    fn cluster_over_stream(
        addr: &str,
        client: tokio::net::UnixStream,
        topology: ClusterTopology,
    ) -> ClusterConnection {
        let mut cluster = ClusterConnection::empty_for_test();
        cluster.default_node = addr.to_string();
        cluster.topology = topology;
        cluster.nodes.insert(
            addr.to_string(),
            RedisConnection::from_stream(RedisStream::Unix(client)),
        );
        cluster
    }

    #[cfg(unix)]
    async fn assert_no_wire_request(server: &mut tokio::net::UnixStream) {
        let mut bytes = [0u8; 128];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(25),
                server.read(&mut bytes),
            )
            .await
            .is_err(),
            "transaction preflight wrote to the cluster node"
        );
    }

    #[cfg(unix)]
    async fn read_until_contains(server: &mut tokio::net::UnixStream, needle: &[u8]) {
        let mut received = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let mut bytes = [0u8; 256];
                let count = server.read(&mut bytes).await.unwrap();
                assert!(count > 0, "connection closed before {needle:?} arrived");
                received.extend_from_slice(&bytes[..count]);
                if received
                    .windows(needle.len())
                    .any(|window| window == needle)
                {
                    return;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {needle:?}"));
    }

    #[tokio::test]
    async fn already_expired_command_never_reaches_direct_node() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(socket_addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let addr = socket_addr.to_string();
        let mut cluster = ClusterConnection::empty_for_test();
        cluster.default_node = addr.clone();
        cluster.nodes.insert(
            addr.clone(),
            RedisConnection::from_stream(RedisStream::Tcp(client)),
        );

        let result = cluster
            .execute(WithDeadline::new(
                Ping::new(),
                tokio::time::Instant::now() - std::time::Duration::from_millis(1),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert!(
            cluster.nodes.contains_key(&addr),
            "an expired command must not check out its node connection"
        );
        let mut bytes = [0u8; 128];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(25),
                server.read(&mut bytes),
            )
            .await
            .is_err(),
            "an already-expired command reached the cluster node"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn zero_redirect_budget_still_makes_the_initial_attempt() {
        let addr = "127.0.0.1:7000";
        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(addr, client, topology_for_addr(addr, 0, 16_383));
        cluster.max_redirects = 0;

        let server_task = tokio::spawn(async move {
            read_until_contains(&mut server, b"PING").await;
            server.write_all(b"+PONG\r\n").await.unwrap();
        });

        assert_eq!(cluster.execute(Ping::new()).await.unwrap(), "PONG");
        server_task.await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn one_redirect_budget_follows_one_moved_reply() {
        let source_addr = "127.0.0.1:7000";
        let target_addr = "127.0.0.1:7001";
        let (source_client, mut source_server) = tokio::net::UnixStream::pair().unwrap();
        let (target_client, mut target_server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(
            source_addr,
            source_client,
            topology_for_addr(source_addr, 0, 16_383),
        );
        cluster.nodes.insert(
            target_addr.to_string(),
            RedisConnection::from_stream(RedisStream::Unix(target_client)),
        );
        cluster.max_redirects = 1;
        let slot = slot_for_key(b"foo");

        let source_task = tokio::spawn(async move {
            read_until_contains(&mut source_server, b"GET").await;
            source_server
                .write_all(format!("-MOVED {slot} {target_addr}\r\n").as_bytes())
                .await
                .unwrap();
        });
        let target_task = tokio::spawn(async move {
            read_until_contains(&mut target_server, b"GET").await;
            target_server.write_all(b"$5\r\nvalue\r\n").await.unwrap();
        });

        assert_eq!(
            cluster.execute(Get::new("foo")).await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        source_task.await.unwrap();
        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn deadline_quarantines_direct_node_with_unread_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(socket_addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let server_task = tokio::spawn(async move {
            let mut request = [0u8; 128];
            let received = server.read(&mut request).await.unwrap();
            assert!(received > 0, "client never wrote the timed command");

            // The timeout must drop, rather than return, the checked-out node
            // connection. That makes the peer observe EOF without replying.
            assert_eq!(server.read(&mut request).await.unwrap(), 0);
        });

        let addr = socket_addr.to_string();
        let mut cluster = ClusterConnection::empty_for_test();
        cluster.default_node = addr.clone();
        cluster.nodes.insert(
            addr.clone(),
            RedisConnection::from_stream(RedisStream::Tcp(client)),
        );

        let result = cluster
            .execute(WithDeadline::after(
                Ping::new(),
                std::time::Duration::from_millis(25),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert!(
            !cluster.nodes.contains_key(&addr),
            "a socket with an unread response must not return to the node map"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), server_task)
            .await
            .expect("quarantined socket did not close")
            .unwrap();
    }

    #[tokio::test]
    async fn io_error_quarantines_direct_node_with_partial_exchange() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(socket_addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        let server_task = tokio::spawn(async move {
            let mut request = [0u8; 128];
            let received = server.read(&mut request).await.unwrap();
            assert!(received > 0, "client never wrote the command");
            // Close without replying, simulating an exchange that may have
            // been applied server-side but cannot leave this socket aligned.
        });

        let addr = socket_addr.to_string();
        let mut cluster = ClusterConnection::empty_for_test();
        cluster.default_node = addr.clone();
        cluster.nodes.insert(
            addr.clone(),
            RedisConnection::from_stream(RedisStream::Tcp(client)),
        );

        let result = cluster.execute(Ping::new()).await;

        assert!(result.is_err());
        assert!(
            !cluster.nodes.contains_key(&addr),
            "a socket from a failed exchange must not return to the node map"
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn transaction_rejects_watch_body_cross_slot_before_wire_io() {
        let addr = "127.0.0.1:7000";
        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(addr, client, topology_for_addr(addr, 0, 16_383));

        let result = <ClusterConnection as redis_tower::TransactionExecutor>::execute_transaction(
            &mut cluster,
            vec![array(vec![bulk("WATCH"), bulk("foo")])],
            vec![array(vec![bulk("SET"), bulk("hello"), bulk("value")])],
        )
        .await;

        let error = result.expect_err("mixed-slot transaction unexpectedly executed");
        assert!(error.to_string().contains("CROSSSLOT"));
        assert!(cluster.nodes.contains_key(addr));
        assert_no_wire_request(&mut server).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn keyed_transaction_fails_closed_when_slot_is_unmapped() {
        let addr = "127.0.0.1:7000";
        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(addr, client, topology_for_addr(addr, 0, 0));
        let slot = slot_for_key(b"foo");
        assert_ne!(slot, 0);

        let result = <ClusterConnection as redis_tower::TransactionExecutor>::execute_transaction(
            &mut cluster,
            Vec::new(),
            vec![array(vec![bulk("SET"), bulk("foo"), bulk("value")])],
        )
        .await;

        let error = result.expect_err("unmapped transaction fell back to the default node");
        assert!(
            error.to_string().contains(&format!(
                "CLUSTERDOWN no known master owns transaction slot {slot}"
            )),
            "unexpected unmapped-slot error: {error}"
        );
        assert!(cluster.nodes.contains_key(addr));
        assert_no_wire_request(&mut server).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn transaction_moved_updates_topology_without_replay() {
        let source_addr = "127.0.0.1:7000";
        let target_addr = "127.0.0.1:7001";
        let (source_client, mut source_server) = tokio::net::UnixStream::pair().unwrap();
        let (target_client, mut target_server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(
            source_addr,
            source_client,
            topology_for_addr(source_addr, 0, 16_383),
        );
        cluster.nodes.insert(
            target_addr.to_string(),
            RedisConnection::from_stream(RedisStream::Unix(target_client)),
        );
        let slot = slot_for_key(b"foo");
        let moved = format!("-MOVED {slot} {target_addr}\r\n");

        let server_task = tokio::spawn(async move {
            read_until_contains(&mut source_server, b"MULTI").await;
            source_server.write_all(b"+OK\r\n").await.unwrap();
            read_until_contains(&mut source_server, b"SET").await;
            source_server.write_all(moved.as_bytes()).await.unwrap();
            read_until_contains(&mut source_server, b"DISCARD").await;
            source_server.write_all(b"+OK\r\n").await.unwrap();
        });

        let result = <ClusterConnection as redis_tower::TransactionExecutor>::execute_transaction(
            &mut cluster,
            Vec::new(),
            vec![array(vec![bulk("SET"), bulk("foo"), bulk("value")])],
        )
        .await;

        let error = result.expect_err("MOVED transaction unexpectedly succeeded");
        assert!(error.to_string().contains("MOVED"));
        assert_eq!(
            cluster
                .topology
                .master_for_slot(slot)
                .expect("MOVED slot was not retained")
                .addr_string(),
            target_addr
        );
        assert!(
            !cluster.nodes.contains_key(source_addr),
            "errored transaction connection returned to the reusable node map"
        );
        server_task.await.unwrap();
        assert_no_wire_request(&mut target_server).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn cancelled_transaction_quarantines_checked_out_node() {
        let addr = "127.0.0.1:7000";
        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(addr, client, topology_for_addr(addr, 0, 16_383));
        let server_task = tokio::spawn(async move {
            read_until_contains(&mut server, b"MULTI").await;
            let mut bytes = [0u8; 128];
            assert_eq!(
                server.read(&mut bytes).await.unwrap(),
                0,
                "cancelled transaction left its socket reusable"
            );
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            <ClusterConnection as redis_tower::TransactionExecutor>::execute_transaction(
                &mut cluster,
                Vec::new(),
                vec![array(vec![bulk("SET"), bulk("foo"), bulk("value")])],
            ),
        )
        .await;

        assert!(
            result.is_err(),
            "transaction unexpectedly received a response"
        );
        assert!(
            !cluster.nodes.contains_key(addr),
            "cancelled transaction returned its checked-out node to the map"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), server_task)
            .await
            .expect("quarantined transaction socket did not close")
            .unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn cluster_transaction_helper_rejects_before_watch_or_build() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let addr = "127.0.0.1:7000";
        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let mut cluster = cluster_over_stream(addr, client, topology_for_addr(addr, 0, 16_383));
        let build_called = Arc::new(AtomicBool::new(false));
        let build_flag = Arc::clone(&build_called);

        let result = redis_tower::transaction_with_retries(
            &mut cluster,
            ["foo"],
            0,
            async move |_conn: &mut ClusterConnection| {
                build_flag.store(true, Ordering::SeqCst);
                Ok(redis_tower::Transaction::new())
            },
        )
        .await;

        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("cluster accepted closure-based transaction helper"),
        };
        assert!(error.to_string().contains("transaction_with_retries()"));
        assert!(error.to_string().contains("Transaction::watch()"));
        assert!(!build_called.load(Ordering::SeqCst));
        assert!(cluster.nodes.contains_key(addr));
        assert_no_wire_request(&mut server).await;
    }

    #[test]
    fn parse_moved_redirect() {
        let frame = Frame::Error(Bytes::from("MOVED 3999 127.0.0.1:7001"));
        match parse_redirect(&frame) {
            Some(Redirect::Moved { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:7001");
            }
            other => panic!("expected Moved, got {other:?}"),
        }
    }

    #[test]
    fn parse_ask_redirect() {
        let frame = Frame::Error(Bytes::from("ASK 3999 127.0.0.1:7002"));
        match parse_redirect(&frame) {
            Some(Redirect::Ask { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:7002");
            }
            other => panic!("expected Ask, got {other:?}"),
        }
    }

    #[test]
    fn parse_non_redirect_error() {
        let frame = Frame::Error(Bytes::from("ERR unknown command"));
        assert!(parse_redirect(&frame).is_none());
    }

    #[test]
    fn parse_non_error_frame() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        assert!(parse_redirect(&frame).is_none());
    }

    // -- transient cluster error classification --

    #[test]
    fn transient_error_classifies_tryagain() {
        let frame = Frame::Error(Bytes::from(
            "TRYAGAIN Multiple keys request during rehashing",
        ));
        assert_eq!(
            TransientError::from_frame(&frame),
            Some(TransientError::TryAgain)
        );
    }

    #[test]
    fn transient_error_classifies_clusterdown() {
        let frame = Frame::Error(Bytes::from("CLUSTERDOWN The cluster is down"));
        assert_eq!(
            TransientError::from_frame(&frame),
            Some(TransientError::ClusterDown)
        );
    }

    #[test]
    fn transient_error_classifies_loading() {
        let frame = Frame::Error(Bytes::from(
            "LOADING Redis is loading the dataset in memory",
        ));
        assert_eq!(
            TransientError::from_frame(&frame),
            Some(TransientError::Loading)
        );
    }

    #[test]
    fn transient_error_ignores_hard_errors_and_redirects() {
        // Hard command errors are not transient.
        assert_eq!(
            TransientError::from_frame(&Frame::Error(Bytes::from("WRONGTYPE bad op"))),
            None
        );
        // MOVED/ASK are redirects, handled separately -- not transient retries.
        assert_eq!(
            TransientError::from_frame(&Frame::Error(Bytes::from("MOVED 3999 127.0.0.1:7001"))),
            None
        );
        // Non-error frames are never transient.
        assert_eq!(
            TransientError::from_frame(&Frame::SimpleString(Bytes::from("OK"))),
            None
        );
    }

    #[test]
    fn read_preference_default() {
        assert_eq!(ReadPreference::default(), ReadPreference::Master);
    }

    // -- remap_topology tests --

    fn make_topology() -> ClusterTopology {
        ClusterTopology::new(vec![
            SlotRange {
                start: 0,
                end: 5460,
                master: NodeAddr {
                    host: "10.0.0.1".to_string(),
                    port: 7000,
                },
                replicas: vec![NodeAddr {
                    host: "10.0.0.4".to_string(),
                    port: 7003,
                }],
            },
            SlotRange {
                start: 5461,
                end: 10922,
                master: NodeAddr {
                    host: "10.0.0.2".to_string(),
                    port: 7001,
                },
                replicas: vec![],
            },
            SlotRange {
                start: 10923,
                end: 16383,
                master: NodeAddr {
                    host: "10.0.0.3".to_string(),
                    port: 7002,
                },
                replicas: vec![NodeAddr {
                    host: "10.0.0.5".to_string(),
                    port: 7004,
                }],
            },
        ])
    }

    #[test]
    fn remap_topology_changes_all_hosts() {
        let mut topo = make_topology();
        remap_topology(&mut topo, "127.0.0.1");

        for range in topo.slot_ranges() {
            assert_eq!(range.master.host, "127.0.0.1");
            for replica in &range.replicas {
                assert_eq!(replica.host, "127.0.0.1");
            }
        }
    }

    #[test]
    fn remap_topology_preserves_ports() {
        let mut topo = make_topology();
        remap_topology(&mut topo, "localhost");

        assert_eq!(topo.slot_ranges()[0].master.port, 7000);
        assert_eq!(topo.slot_ranges()[1].master.port, 7001);
        assert_eq!(topo.slot_ranges()[2].master.port, 7002);
    }

    // -- remap_addr tests --

    #[test]
    fn remap_addr_with_override() {
        let conn = ClusterConnection {
            nodes: HashMap::new(),
            topology: make_topology(),
            default_node: String::new(),
            host_override: Some("127.0.0.1".to_string()),
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        };
        assert_eq!(conn.remap_addr("10.0.0.1:7000"), "127.0.0.1:7000");
    }

    #[test]
    fn remap_addr_without_override() {
        let conn = ClusterConnection {
            nodes: HashMap::new(),
            topology: make_topology(),
            default_node: String::new(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        };
        assert_eq!(conn.remap_addr("10.0.0.1:7000"), "10.0.0.1:7000");
    }

    #[test]
    fn remap_addr_with_address_map() {
        let mut map = HashMap::new();
        map.insert(
            "10.0.0.1:7000".to_string(),
            "ext1.example.com:17000".to_string(),
        );
        map.insert(
            "10.0.0.2:7001".to_string(),
            "ext2.example.com:17001".to_string(),
        );

        let conn = ClusterConnection {
            nodes: HashMap::new(),
            topology: make_topology(),
            default_node: String::new(),
            host_override: None,
            address_map: Some(map),
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        };

        // Mapped address returns the external address.
        assert_eq!(conn.remap_addr("10.0.0.1:7000"), "ext1.example.com:17000");
        assert_eq!(conn.remap_addr("10.0.0.2:7001"), "ext2.example.com:17001");
        // Unmapped address is returned as-is.
        assert_eq!(conn.remap_addr("10.0.0.3:7002"), "10.0.0.3:7002");
    }

    #[test]
    fn remap_addr_address_map_takes_priority_over_host_override() {
        let mut map = HashMap::new();
        map.insert(
            "10.0.0.1:7000".to_string(),
            "ext1.example.com:17000".to_string(),
        );

        let conn = ClusterConnection {
            nodes: HashMap::new(),
            topology: make_topology(),
            default_node: String::new(),
            host_override: Some("127.0.0.1".to_string()),
            address_map: Some(map),
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        };

        // Address in the map uses the map (takes priority).
        assert_eq!(conn.remap_addr("10.0.0.1:7000"), "ext1.example.com:17000");
        // Address not in the map falls back to host_override.
        assert_eq!(conn.remap_addr("10.0.0.2:7001"), "127.0.0.1:7001");
    }

    #[test]
    fn remap_topology_with_map_changes_matched_addresses() {
        let mut topo = make_topology();
        let mut map = HashMap::new();
        map.insert(
            "10.0.0.1:7000".to_string(),
            "ext1.example.com:17000".to_string(),
        );
        map.insert(
            "10.0.0.4:7003".to_string(),
            "ext4.example.com:17003".to_string(),
        );

        remap_topology_with_map(&mut topo, &map);

        // Matched master is remapped.
        assert_eq!(topo.slot_ranges()[0].master.host, "ext1.example.com");
        assert_eq!(topo.slot_ranges()[0].master.port, 17000);
        // Matched replica is remapped.
        assert_eq!(topo.slot_ranges()[0].replicas[0].host, "ext4.example.com");
        assert_eq!(topo.slot_ranges()[0].replicas[0].port, 17003);
        // Unmatched addresses are unchanged.
        assert_eq!(topo.slot_ranges()[1].master.host, "10.0.0.2");
        assert_eq!(topo.slot_ranges()[1].master.port, 7001);
    }

    // -- route_command tests --

    #[test]
    fn route_to_correct_master() {
        // Verify topology lookup routes to the right master based on slot.
        let topo = ClusterTopology::new(vec![
            SlotRange {
                start: 0,
                end: 5460,
                master: NodeAddr {
                    host: "127.0.0.1".to_string(),
                    port: 7000,
                },
                replicas: vec![],
            },
            SlotRange {
                start: 5461,
                end: 16383,
                master: NodeAddr {
                    host: "127.0.0.1".to_string(),
                    port: 7001,
                },
                replicas: vec![],
            },
        ]);

        // We need real entries in the map for routing to match.
        // Use a trick: insert with empty connections isn't possible,
        // but we can verify the routing logic by checking the addr_string.
        // For a proper unit test, let's just verify the topology lookup:
        let slot = slot_for_key(b"foo"); // 12182
        let master = topo.master_for_slot(slot).unwrap();
        assert_eq!(master.port, 7001); // slot 12182 > 5460

        let slot2 = slot_for_key(b"hello"); // 866
        let master2 = topo.master_for_slot(slot2).unwrap();
        assert_eq!(master2.port, 7000); // slot 866 < 5460
    }

    // -- update_slot_owner tests --

    #[test]
    fn update_slot_owner_changes_master() {
        let mut conn = ClusterConnection {
            nodes: HashMap::new(),
            topology: make_topology(),
            default_node: "10.0.0.1:7000".to_string(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        };

        // Slot 100 is in range 0-5460, owned by 10.0.0.1:7000.
        assert_eq!(conn.topology.master_for_slot(100).unwrap().port, 7000);

        // Simulate a MOVED redirect.
        conn.update_slot_owner(100, "10.0.0.9:9000");
        assert_eq!(conn.topology.master_for_slot(100).unwrap().host, "10.0.0.9");
        assert_eq!(conn.topology.master_for_slot(100).unwrap().port, 9000);
    }

    #[test]
    fn update_slot_owner_moves_only_the_named_slot() {
        let mut conn = ClusterConnection {
            nodes: HashMap::new(),
            topology: make_topology(),
            default_node: "10.0.0.1:7000".to_string(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
            max_redirects: MAX_REDIRECTS,
            credentials: None,
            resp_limits: RespLimits::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        };

        // A single-slot MOVED must not steal the rest of the 0-5460 range --
        // that was the redirect ping-pong bug during live resharding.
        conn.update_slot_owner(100, "10.0.0.9:9000");
        assert_eq!(conn.topology.master_for_slot(100).unwrap().port, 9000);
        assert_eq!(conn.topology.master_for_slot(99).unwrap().port, 7000);
        assert_eq!(conn.topology.master_for_slot(101).unwrap().port, 7000);
        assert_eq!(conn.topology.master_for_slot(0).unwrap().port, 7000);
        assert_eq!(conn.topology.master_for_slot(5460).unwrap().port, 7000);
    }

    #[test]
    fn builder_defaults_max_redirects() {
        let builder = ClusterConnection::builder("127.0.0.1:7000");
        assert_eq!(builder.max_redirects, MAX_REDIRECTS);
    }

    #[test]
    fn builder_sets_max_redirects() {
        let builder = ClusterConnection::builder("127.0.0.1:7000").max_redirects(10);
        assert_eq!(builder.max_redirects, 10);
    }

    #[test]
    fn builder_defaults_resp_limits() {
        let builder = ClusterConnection::builder("127.0.0.1:7000");
        assert_eq!(builder.resp_limits, RespLimits::default());
    }

    #[test]
    fn builder_sets_resp_limits() {
        let limits = RespLimits {
            max_frame_size: 1024,
            max_depth: 8,
        };
        let builder = ClusterConnection::builder("127.0.0.1:7000").resp_limits(limits);
        assert_eq!(builder.resp_limits, limits);
    }

    // -- connect_url parsing --

    #[tokio::test]
    async fn parse_cluster_url_acl_user_and_tls() {
        let (seed, creds, tls) =
            parse_cluster_url("rediss://alice:s3cret@cluster.example.com:7000").unwrap();
        assert_eq!(seed, "cluster.example.com:7000");
        assert!(tls);
        let creds = creds.unwrap().get_credentials().await.unwrap();
        assert_eq!(creds.username.as_deref(), Some("alice"));
        assert_eq!(creds.password, "s3cret");
    }

    #[tokio::test]
    async fn parse_cluster_url_password_only_is_legacy_auth() {
        // redis://:pass@ -- the `requirepass` case: AUTH with no username.
        let (seed, creds, tls) = parse_cluster_url("redis://:hunter2@127.0.0.1:6379").unwrap();
        assert_eq!(seed, "127.0.0.1:6379");
        assert!(!tls);
        let creds = creds.unwrap().get_credentials().await.unwrap();
        assert_eq!(creds.username, None);
        assert_eq!(creds.password, "hunter2");
    }

    #[test]
    fn parse_cluster_url_no_auth() {
        let (seed, creds, tls) = parse_cluster_url("redis://127.0.0.1:6379").unwrap();
        assert_eq!(seed, "127.0.0.1:6379");
        assert!(creds.is_none());
        assert!(!tls);
    }

    #[test]
    fn parse_cluster_url_rejects_unix_socket() {
        assert!(parse_cluster_url("unix:///tmp/redis.sock").is_err());
    }

    // -- redirect edge cases --

    #[test]
    fn parse_moved_with_ipv6() {
        let frame = Frame::Error(Bytes::from("MOVED 3999 ::1:7001"));
        // This won't parse correctly with rsplit_once(':') but let's verify behavior.
        match parse_redirect(&frame) {
            Some(Redirect::Moved { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "::1:7001");
            }
            _ => panic!("should parse as Moved"),
        }
    }

    #[test]
    fn parse_moved_invalid_slot() {
        let frame = Frame::Error(Bytes::from("MOVED notanumber 127.0.0.1:7001"));
        assert!(parse_redirect(&frame).is_none());
    }

    #[test]
    fn parse_ask_invalid_slot() {
        let frame = Frame::Error(Bytes::from("ASK notanumber 127.0.0.1:7001"));
        assert!(parse_redirect(&frame).is_none());
    }

    #[test]
    fn parse_redirect_too_few_parts() {
        let frame = Frame::Error(Bytes::from("MOVED"));
        assert!(parse_redirect(&frame).is_none());
    }

    #[test]
    fn read_preference_variants() {
        assert_ne!(ReadPreference::Master, ReadPreference::Replica);
        assert_ne!(ReadPreference::Replica, ReadPreference::PreferReplica);
        assert_ne!(ReadPreference::Master, ReadPreference::PreferReplica);
    }

    // -- ReadRoutingStrategy tests --

    fn make_replicas() -> Vec<NodeAddr> {
        vec![
            NodeAddr {
                host: "10.0.0.1".to_string(),
                port: 7001,
            },
            NodeAddr {
                host: "10.0.0.2".to_string(),
                port: 7002,
            },
            NodeAddr {
                host: "10.0.0.3".to_string(),
                port: 7003,
            },
        ]
    }

    #[test]
    fn round_robin_distributes_across_replicas() {
        let strategy = RoundRobinRouting::new();
        let replicas = make_replicas();

        let first = strategy.select_replica(0, &replicas).unwrap();
        let second = strategy.select_replica(0, &replicas).unwrap();
        let third = strategy.select_replica(0, &replicas).unwrap();
        let fourth = strategy.select_replica(0, &replicas).unwrap();

        assert_eq!(first.port, 7001);
        assert_eq!(second.port, 7002);
        assert_eq!(third.port, 7003);
        // Wraps around.
        assert_eq!(fourth.port, 7001);
    }

    #[test]
    fn round_robin_returns_none_for_empty_replicas() {
        let strategy = RoundRobinRouting::new();
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn random_routing_returns_valid_replica() {
        let strategy = RandomRouting::new();
        let replicas = make_replicas();

        // Call many times and verify all results are valid replicas.
        for _ in 0..100 {
            let selected = strategy.select_replica(0, &replicas).unwrap();
            assert!(
                replicas.contains(selected),
                "selected replica not in list: {selected:?}"
            );
        }
    }

    #[test]
    fn random_routing_returns_none_for_empty_replicas() {
        let strategy = RandomRouting::new();
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn first_replica_always_returns_first() {
        let strategy = FirstReplicaRouting;
        let replicas = make_replicas();

        for _ in 0..10 {
            let selected = strategy.select_replica(0, &replicas).unwrap();
            assert_eq!(selected.port, 7001);
            assert_eq!(selected.host, "10.0.0.1");
        }
    }

    #[test]
    fn first_replica_returns_none_for_empty_replicas() {
        let strategy = FirstReplicaRouting;
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn builder_accepts_custom_strategy() {
        // Verify the builder compiles and stores a custom strategy.
        let builder = ClusterConnection::builder("127.0.0.1:7000")
            .read_preference(ReadPreference::PreferReplica)
            .read_routing(FirstReplicaRouting);

        assert!(builder.read_routing.is_some());
        assert_eq!(builder.read_preference, ReadPreference::PreferReplica);
    }

    #[test]
    fn builder_defaults_to_no_custom_strategy() {
        let builder = ClusterConnection::builder("127.0.0.1:7000");

        assert!(builder.read_routing.is_none());
        assert_eq!(builder.read_preference, ReadPreference::Master);
    }

    /// A custom strategy for testing that always returns the last replica.
    struct LastReplicaRouting;

    impl ReadRoutingStrategy for LastReplicaRouting {
        fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
            replicas.last()
        }
    }

    #[test]
    fn custom_strategy_is_usable() {
        let strategy = LastReplicaRouting;
        let replicas = make_replicas();

        let selected = strategy.select_replica(0, &replicas).unwrap();
        assert_eq!(selected.port, 7003);
    }
}
