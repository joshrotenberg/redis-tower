//! Cluster-aware Redis connection that routes commands by slot.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
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

/// Maximum number of redirects before giving up.
pub(crate) const MAX_REDIRECTS: usize = 5;

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
/// # Example
///
/// ```ignore
/// use redis_tower_cluster::{ClusterConnection, ReadPreference};
/// use redis_tower::commands::*;
///
/// let mut cluster = ClusterConnection::builder("127.0.0.1:7000")
///     .read_preference(ReadPreference::PreferReplica)
///     .connect()
///     .await?;
///
/// cluster.execute(Set::new("key", "value")).await?;
/// let val = cluster.execute(Get::new("key")).await?; // routed to replica
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
}

/// Builder for configuring a `ClusterConnection`.
pub struct ClusterConnectionBuilder {
    seed_addr: String,
    host_override: Option<String>,
    address_map: Option<HashMap<String, String>>,
    read_preference: ReadPreference,
    read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
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

    /// Connect to the cluster.
    pub async fn connect(self) -> Result<ClusterConnection, RedisError> {
        ClusterConnection::connect_inner(
            &self.seed_addr,
            self.host_override,
            self.address_map,
            self.read_preference,
            self.read_routing,
        )
        .await
    }
}

/// Parsed redirect from a MOVED or ASK error.
#[derive(Debug)]
pub(crate) enum Redirect {
    Moved { slot: u16, addr: String },
    Ask { addr: String },
}

impl ClusterConnection {
    /// Connect to a cluster using a seed node address.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        Self::connect_inner(seed_addr, None, None, ReadPreference::Master, None).await
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
        )
        .await
    }

    /// Create a builder for configuring the connection.
    pub fn builder(seed_addr: impl Into<String>) -> ClusterConnectionBuilder {
        ClusterConnectionBuilder {
            seed_addr: seed_addr.into(),
            host_override: None,
            address_map: None,
            read_preference: ReadPreference::Master,
            read_routing: None,
        }
    }

    async fn connect_inner(
        seed_addr: &str,
        host_override: Option<String>,
        address_map: Option<HashMap<String, String>>,
        read_preference: ReadPreference,
        read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
    ) -> Result<Self, RedisError> {
        let mut seed_conn = RedisConnection::connect(seed_addr).await?;
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
                let conn = RedisConnection::connect(&addr_str).await?;
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
                    let mut conn = RedisConnection::connect(&addr_str).await?;
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
        })
    }

    /// Execute a command, routing it to the correct cluster node.
    ///
    /// Handles MOVED and ASK redirects transparently.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let cmd_frame = cmd.to_frame();
        let initial_node = self.route_command(&cmd_frame).to_string();

        let mut target_node = initial_node;

        for _ in 0..MAX_REDIRECTS {
            let conn = self.nodes.get_mut(&target_node).ok_or_else(|| {
                RedisError::Redis(format!("no connection for node {target_node}"))
            })?;

            let responses = conn.execute_pipeline(vec![cmd_frame.clone()]).await?;
            let response = responses
                .into_iter()
                .next()
                .ok_or(RedisError::ConnectionClosed)?;

            match parse_redirect(&response) {
                Some(Redirect::Moved { slot, addr }) => {
                    let addr = self.remap_addr(&addr);
                    self.ensure_connection(&addr).await?;
                    self.update_slot_owner(slot, &addr);
                    target_node = addr;
                    continue;
                }
                Some(Redirect::Ask { addr }) => {
                    let addr = self.remap_addr(&addr);
                    self.ensure_connection(&addr).await?;
                    let asking_conn = self.nodes.get_mut(&addr).ok_or_else(|| {
                        RedisError::Redis(format!("no connection for ASK node {addr}"))
                    })?;
                    asking_conn
                        .execute_pipeline(vec![array(vec![bulk("ASKING")])])
                        .await?;
                    let responses = asking_conn
                        .execute_pipeline(vec![cmd_frame.clone()])
                        .await?;
                    let response = responses
                        .into_iter()
                        .next()
                        .ok_or(RedisError::ConnectionClosed)?;

                    if let Frame::Error(ref e) = response {
                        return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
                    }
                    return cmd.parse_response(response);
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
            let conn = RedisConnection::connect(addr).await?;
            self.nodes.insert(addr.to_string(), conn);
        }
        Ok(())
    }

    /// Update the topology to assign a slot to a new node (after MOVED).
    fn update_slot_owner(&mut self, slot: u16, addr: &str) {
        if let Some((host, port_str)) = addr.rsplit_once(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            for range in &mut self.topology.slot_ranges {
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
        let conn = self
            .nodes
            .values_mut()
            .next()
            .ok_or(RedisError::ConnectionClosed)?;

        let mut topology = discover_topology(conn).await?;

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
                let conn = RedisConnection::connect(&addr_str).await?;
                e.insert(conn);
            }
        }

        if self.read_preference != ReadPreference::Master {
            for addr in topology.replica_addrs() {
                let addr_str = addr.addr_string();
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.nodes.entry(addr_str.clone())
                {
                    let mut conn = RedisConnection::connect(&addr_str).await?;
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
        "ASK" => Some(Redirect::Ask {
            addr: parts[2].to_string(),
        }),
        _ => None,
    }
}

/// Remap all node addresses in a topology to use a specific host.
pub(crate) fn remap_topology(topology: &mut ClusterTopology, host: &str) {
    for range in &mut topology.slot_ranges {
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
    for range in &mut topology.slot_ranges {
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
            Some(Redirect::Ask { addr }) => {
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

    #[test]
    fn read_preference_default() {
        assert_eq!(ReadPreference::default(), ReadPreference::Master);
    }

    // -- remap_topology tests --

    fn make_topology() -> ClusterTopology {
        ClusterTopology {
            slot_ranges: vec![
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
            ],
        }
    }

    #[test]
    fn remap_topology_changes_all_hosts() {
        let mut topo = make_topology();
        remap_topology(&mut topo, "127.0.0.1");

        for range in &topo.slot_ranges {
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

        assert_eq!(topo.slot_ranges[0].master.port, 7000);
        assert_eq!(topo.slot_ranges[1].master.port, 7001);
        assert_eq!(topo.slot_ranges[2].master.port, 7002);
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
        assert_eq!(topo.slot_ranges[0].master.host, "ext1.example.com");
        assert_eq!(topo.slot_ranges[0].master.port, 17000);
        // Matched replica is remapped.
        assert_eq!(topo.slot_ranges[0].replicas[0].host, "ext4.example.com");
        assert_eq!(topo.slot_ranges[0].replicas[0].port, 17003);
        // Unmatched addresses are unchanged.
        assert_eq!(topo.slot_ranges[1].master.host, "10.0.0.2");
        assert_eq!(topo.slot_ranges[1].master.port, 7001);
    }

    // -- route_command tests --

    #[test]
    fn route_to_correct_master() {
        // Verify topology lookup routes to the right master based on slot.
        let topo = ClusterTopology {
            slot_ranges: vec![
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
            ],
        };

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
        };

        // Slot 100 is in range 0-5460, owned by 10.0.0.1:7000.
        assert_eq!(conn.topology.master_for_slot(100).unwrap().port, 7000);

        // Simulate a MOVED redirect.
        conn.update_slot_owner(100, "10.0.0.9:9000");
        assert_eq!(conn.topology.master_for_slot(100).unwrap().host, "10.0.0.9");
        assert_eq!(conn.topology.master_for_slot(100).unwrap().port, 9000);
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
