//! Cluster-aware Redis connection that routes commands by slot.

use std::collections::HashMap;

use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::key_extractor::extract_key;
use crate::slot::slot_for_key;
use crate::topology::{ClusterTopology, NodeAddr, discover_topology};

/// Maximum number of redirects before giving up.
const MAX_REDIRECTS: usize = 5;

/// A Redis Cluster connection that routes commands to the correct node.
///
/// Discovers the cluster topology via CLUSTER SLOTS on the seed node,
/// then maintains one connection per master. Commands are routed based
/// on the key's hash slot.
///
/// Handles MOVED and ASK redirects automatically:
/// - **MOVED**: Updates the slot map, connects to the new node if needed,
///   and retries the command.
/// - **ASK**: Sends ASKING to the indicated node, then retries the command
///   (single-shot, no slot map update).
///
/// # Example
///
/// ```ignore
/// use redis_tower_cluster::ClusterConnection;
/// use redis_tower::commands::*;
///
/// let cluster = ClusterConnection::connect("127.0.0.1:7000").await?;
/// cluster.execute(Set::new("key", "value")).await?;
/// let val = cluster.execute(Get::new("key")).await?;
/// ```
pub struct ClusterConnection {
    /// Connections to master nodes, keyed by "host:port".
    nodes: HashMap<String, RedisConnection>,
    /// Current cluster topology (with remapped addresses).
    topology: ClusterTopology,
    /// Address of a node to use for keyless commands.
    default_node: String,
    /// Host override for connecting to nodes (e.g., "127.0.0.1" for Docker).
    host_override: Option<String>,
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
        Self::connect_inner(seed_addr, None).await
    }

    /// Connect to a cluster, remapping all node hosts to `host_override`.
    pub async fn connect_with_host(
        seed_addr: &str,
        host_override: &str,
    ) -> Result<Self, RedisError> {
        Self::connect_inner(seed_addr, Some(host_override.to_string())).await
    }

    async fn connect_inner(
        seed_addr: &str,
        host_override: Option<String>,
    ) -> Result<Self, RedisError> {
        let seed_conn = RedisConnection::connect(seed_addr).await?;
        let mut topology = discover_topology(&seed_conn).await?;

        if let Some(ref host) = host_override {
            remap_topology(&mut topology, host);
        }

        let mut nodes = HashMap::new();
        let mut default_node = String::new();

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

        if default_node.is_empty() {
            nodes.insert(seed_addr.to_string(), seed_conn);
            default_node = seed_addr.to_string();
        }

        Ok(Self {
            nodes,
            topology,
            default_node,
            host_override,
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
                    // Send ASKING before the retry.
                    let asking_conn = self.nodes.get(&addr).ok_or_else(|| {
                        RedisError::Redis(format!("no connection for ASK node {addr}"))
                    })?;
                    asking_conn
                        .execute_pipeline(vec![array(vec![bulk("ASKING")])])
                        .await?;
                    // Retry the command on the ASK target.
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
                    // Normal response (or a non-redirect error).
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

    /// Determine which node should handle a command based on its key.
    fn route_command(&self, frame: &Frame) -> &str {
        if let Some(key) = extract_key(frame) {
            let slot = slot_for_key(key);
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

        self.topology = topology;
        Ok(())
    }
}

/// Parse a MOVED or ASK redirect from an error frame.
///
/// Format: `MOVED <slot> <host>:<port>` or `ASK <slot> <host>:<port>`
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
}
