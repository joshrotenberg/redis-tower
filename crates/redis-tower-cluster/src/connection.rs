//! Cluster-aware Redis connection that routes commands by slot.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::key_extractor;
use crate::slot::slot_for_key;
use crate::topology::{ClusterTopology, NodeAddr, discover_topology};

/// Maximum number of redirects before giving up.
const MAX_REDIRECTS: usize = 5;

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
    /// Read routing preference.
    read_preference: ReadPreference,
    /// Round-robin counter for distributing reads across replicas.
    replica_counter: AtomicUsize,
}

/// Builder for configuring a `ClusterConnection`.
pub struct ClusterConnectionBuilder {
    seed_addr: String,
    host_override: Option<String>,
    read_preference: ReadPreference,
}

impl ClusterConnectionBuilder {
    /// Set the host override for Docker/proxy environments.
    pub fn host_override(mut self, host: impl Into<String>) -> Self {
        self.host_override = Some(host.into());
        self
    }

    /// Set the read preference.
    pub fn read_preference(mut self, pref: ReadPreference) -> Self {
        self.read_preference = pref;
        self
    }

    /// Connect to the cluster.
    pub async fn connect(self) -> Result<ClusterConnection, RedisError> {
        ClusterConnection::connect_inner(&self.seed_addr, self.host_override, self.read_preference)
            .await
    }
}

/// Parsed redirect from a MOVED or ASK error.
#[derive(Debug)]
enum Redirect {
    Moved { slot: u16, addr: String },
    Ask { addr: String },
}

impl ClusterConnection {
    /// Connect to a cluster using a seed node address.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        Self::connect_inner(seed_addr, None, ReadPreference::Master).await
    }

    /// Connect to a cluster, remapping all node hosts to `host_override`.
    pub async fn connect_with_host(
        seed_addr: &str,
        host_override: &str,
    ) -> Result<Self, RedisError> {
        Self::connect_inner(
            seed_addr,
            Some(host_override.to_string()),
            ReadPreference::Master,
        )
        .await
    }

    /// Create a builder for configuring the connection.
    pub fn builder(seed_addr: impl Into<String>) -> ClusterConnectionBuilder {
        ClusterConnectionBuilder {
            seed_addr: seed_addr.into(),
            host_override: None,
            read_preference: ReadPreference::Master,
        }
    }

    async fn connect_inner(
        seed_addr: &str,
        host_override: Option<String>,
        read_preference: ReadPreference,
    ) -> Result<Self, RedisError> {
        let seed_conn = RedisConnection::connect(seed_addr).await?;
        let mut topology = discover_topology(&seed_conn).await?;

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
                    let conn = RedisConnection::connect(&addr_str).await?;
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

        Ok(Self {
            nodes,
            topology,
            default_node,
            host_override,
            read_preference,
            replica_counter: AtomicUsize::new(0),
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
            let conn = self.nodes.get(&target_node).ok_or_else(|| {
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
                    let asking_conn = self.nodes.get(&addr).ok_or_else(|| {
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

    /// Pick a replica for a given slot, round-robin across available replicas.
    fn pick_replica(&self, slot: u16) -> Option<&str> {
        let replicas = self.topology.replicas_for_slot(slot)?;
        if replicas.is_empty() {
            return None;
        }
        let idx = self.replica_counter.fetch_add(1, Ordering::Relaxed) % replicas.len();
        let addr_str = replicas[idx].addr_string();
        self.nodes
            .keys()
            .find(|k| **k == addr_str)
            .map(|v| v.as_str())
    }

    /// Remap an address using the host override if set.
    fn remap_addr(&self, addr: &str) -> String {
        if let Some(ref host) = self.host_override {
            if let Some((_old_host, port)) = addr.rsplit_once(':') {
                return format!("{host}:{port}");
            }
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
        if let Some((host, port_str)) = addr.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
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
            .values()
            .next()
            .ok_or(RedisError::ConnectionClosed)?;

        let mut topology = discover_topology(conn).await?;

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
                    let conn = RedisConnection::connect(&addr_str).await?;
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

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let cmd_frame = cmd.to_frame();
        let node_addr = self.route_command(&cmd_frame).to_string();

        // Get the node's inner Arc (execute_pipeline takes &self, backed by Arc<Mutex<>>).
        let node = self.nodes.get(&node_addr).map(|c| c.framed_arc());

        Box::pin(async move {
            use futures::SinkExt;
            use tokio_stream::StreamExt;

            let framed_arc = node.ok_or(RedisError::ConnectionClosed)?;
            let mut framed = framed_arc.lock().await;
            framed.send(cmd_frame).await.map_err(RedisError::from)?;
            let response = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;

            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }

            cmd.parse_response(response)
        })
    }
}

/// Parse a MOVED or ASK redirect from an error frame.
fn parse_redirect(frame: &Frame) -> Option<Redirect> {
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
fn remap_topology(topology: &mut ClusterTopology, host: &str) {
    for range in &mut topology.slot_ranges {
        range.master.host = host.to_string();
        for replica in &mut range.replicas {
            replica.host = host.to_string();
        }
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
            read_preference: ReadPreference::Master,
            replica_counter: AtomicUsize::new(0),
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
            read_preference: ReadPreference::Master,
            replica_counter: AtomicUsize::new(0),
        };
        assert_eq!(conn.remap_addr("10.0.0.1:7000"), "10.0.0.1:7000");
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
            read_preference: ReadPreference::Master,
            replica_counter: AtomicUsize::new(0),
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
}
