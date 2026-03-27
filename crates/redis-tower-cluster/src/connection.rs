//! Cluster-aware Redis connection that routes commands by slot.

use std::collections::HashMap;

use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::key_extractor::extract_key;
use crate::slot::slot_for_key;
use crate::topology::{ClusterTopology, discover_topology};

/// A Redis Cluster connection that routes commands to the correct node.
///
/// Discovers the cluster topology via CLUSTER SLOTS on the seed node,
/// then maintains one connection per master. Commands are routed based
/// on the key's hash slot.
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

impl ClusterConnection {
    /// Connect to a cluster using a seed node address.
    ///
    /// Discovers the topology via CLUSTER SLOTS and connects to all masters.
    /// Node addresses from the topology are used as-is.
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        Self::connect_inner(seed_addr, None).await
    }

    /// Connect to a cluster, remapping all node hosts to `host_override`.
    ///
    /// Use this when connecting through Docker or a proxy where the cluster
    /// nodes announce internal IPs but you need to connect via localhost.
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

        // Remap node addresses if host override is set.
        if let Some(ref host) = host_override {
            for range in &mut topology.slot_ranges {
                range.master.host = host.clone();
                for replica in &mut range.replicas {
                    replica.host = host.clone();
                }
            }
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
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let frame = cmd.to_frame();
        let node_addr = self.route_command(&frame);

        let conn = self
            .nodes
            .get(node_addr)
            .ok_or_else(|| RedisError::Redis(format!("no connection for node {node_addr}")))?;

        conn.execute(cmd).await
    }

    /// Determine which node should handle a command based on its key.
    fn route_command(&self, frame: &redis_tower_core::Frame) -> &str {
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
            for range in &mut topology.slot_ranges {
                range.master.host = host.clone();
                for replica in &mut range.replicas {
                    replica.host = host.clone();
                }
            }
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
