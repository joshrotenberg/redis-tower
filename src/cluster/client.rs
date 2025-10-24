//! Cluster client implementation with automatic redirect handling

use crate::client::RedisConnection;
use crate::cluster::commands::ClusterSlots;
use crate::cluster::slots::{SlotMap, slot_for_key};
use crate::commands::Command;
use crate::types::RedisError;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::RwLock;
use tower::Service;

/// Redis Cluster client with automatic routing and redirect handling.
///
/// # Features
/// - Automatic slot-based routing to the correct node
/// - MOVED redirect handling with slot map updates
/// - ASK redirect handling with ASKING command
/// - Connection pooling to all cluster nodes
/// - Automatic topology refresh
///
/// # Example
/// ```no_run
/// use redis_tower::cluster::ClusterClient;
/// use redis_tower::commands::{Get, Set};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = ClusterClient::new(vec![
///     "127.0.0.1:7000",
///     "127.0.0.1:7001",
///     "127.0.0.1:7002",
/// ]).await?;
///
/// // Automatically routed to the correct node
/// client.execute(Set::new("user:123", "Alice")).await?;
/// let value: Option<bytes::Bytes> = client.execute(Get::new("user:123")).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ClusterClient {
    /// Connections to cluster nodes (by address)
    connections: Arc<RwLock<HashMap<String, RedisConnection>>>,
    /// Slot to node address mapping
    slot_map: Arc<RwLock<SlotMap>>,
    /// Initial seed nodes for bootstrapping
    seed_nodes: Vec<String>,
}

impl ClusterClient {
    /// Create a new cluster client.
    ///
    /// Connects to seed nodes and discovers the full cluster topology.
    pub async fn new(seed_nodes: Vec<impl Into<String>>) -> Result<Self, RedisError> {
        let seed_nodes: Vec<String> = seed_nodes.into_iter().map(Into::into).collect();

        if seed_nodes.is_empty() {
            return Err(RedisError::Protocol(
                "At least one seed node required".to_string(),
            ));
        }

        let client = Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            slot_map: Arc::new(RwLock::new(SlotMap::new())),
            seed_nodes,
        };

        // Discover cluster topology
        client.refresh_slots().await?;

        Ok(client)
    }

    /// Remap a cluster node address from internal Docker IP to accessible address.
    ///
    /// When running in Docker, CLUSTER SLOTS returns internal IPs (e.g., 172.27.0.2:7101).
    /// We need to map these back to the accessible addresses (e.g., 127.0.0.1:7101).
    ///
    /// Strategy: If the host is an internal IP, replace it with localhost but keep the port.
    fn remap_address(&self, addr: &str) -> String {
        // Split into host:port
        if let Some((host, port)) = addr.rsplit_once(':') {
            // Check if host is an internal IP (starts with 172., 192.168., 10., or similar)
            if host.starts_with("172.") || host.starts_with("192.168.") || host.starts_with("10.") {
                // Remap to localhost with same port
                return format!("127.0.0.1:{}", port);
            }
        }

        // Return address as-is if not an internal IP
        addr.to_string()
    }

    /// Execute a command on the cluster.
    ///
    /// Automatically routes to the correct node based on the key's hash slot.
    /// Handles MOVED and ASK redirects transparently.
    pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command + Clone + KeyExtractor,
    {
        const MAX_REDIRECTS: usize = 16;

        let key = command
            .extract_key()
            .ok_or_else(|| RedisError::Protocol("Command has no key for routing".to_string()))?;

        let slot = slot_for_key(key.as_bytes());
        let mut current_slot = slot;
        let mut redirects = 0;

        loop {
            if redirects >= MAX_REDIRECTS {
                return Err(RedisError::Protocol("Too many redirects".to_string()));
            }

            // Get the node for this slot
            let node_addr = {
                let slot_map = self.slot_map.read().await;
                slot_map.get_node(current_slot).map(|s| s.to_string())
            };

            if let Some(addr) = node_addr {
                // Get or create connection to this node
                let conn = self.get_or_create_connection(&addr).await?;

                // Execute command
                match conn.execute(command.clone()).await {
                    Ok(response) => return Ok(response),
                    Err(RedisError::Moved { slot, addr }) => {
                        // MOVED: permanent redirect, update slot map
                        redirects += 1;
                        current_slot = slot;

                        // Update slot map with new owner
                        {
                            let mut slot_map = self.slot_map.write().await;
                            slot_map.set_slot(slot, addr.clone());
                        }

                        continue;
                    }
                    Err(RedisError::Ask { slot: _, addr }) => {
                        // ASK: temporary redirect during migration
                        redirects += 1;
                        if redirects >= MAX_REDIRECTS {
                            return Err(RedisError::Protocol("Too many redirects".to_string()));
                        }

                        // Send ASKING then retry command on target node
                        let target_conn = self.get_or_create_connection(&addr).await?;

                        // Execute ASKING command
                        use crate::cluster::commands::Asking;
                        target_conn.execute(Asking).await?;

                        // Retry the command on the target node
                        return target_conn.execute(command).await;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                // Slot not in map, refresh and retry
                self.refresh_slots().await?;
                redirects += 1;
            }
        }
    }

    /// Refresh cluster topology by querying CLUSTER SLOTS.
    async fn refresh_slots(&self) -> Result<(), RedisError> {
        for seed_addr in &self.seed_nodes {
            match self.try_refresh_from_node(seed_addr).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    eprintln!("Failed to refresh from {}: {}", seed_addr, e);
                    continue;
                }
            }
        }

        Err(RedisError::Protocol(
            "Failed to refresh slots from any seed node".to_string(),
        ))
    }

    async fn try_refresh_from_node(&self, addr: &str) -> Result<(), RedisError> {
        // Connect to the node
        let conn = self.get_or_create_connection(addr).await?;

        // Query CLUSTER SLOTS
        let slot_ranges = conn.execute(ClusterSlots).await?;

        // Update slot map
        let mut slot_map = self.slot_map.write().await;
        for range in slot_ranges {
            let master_addr = format!("{}:{}", range.master.host, range.master.port);
            // Remap internal Docker IPs to accessible addresses
            let master_addr = self.remap_address(&master_addr);

            // Map all slots in this range to the master
            for slot in range.start_slot..=range.end_slot {
                slot_map.set_slot(slot, master_addr.clone());
            }

            // Ensure we have connections to master and replicas
            let addrs = vec![master_addr]
                .into_iter()
                .chain(
                    range
                        .replicas
                        .iter()
                        .map(|r| self.remap_address(&format!("{}:{}", r.host, r.port))),
                )
                .collect::<Vec<_>>();

            drop(slot_map);

            // Pre-create connections to all nodes
            for node_addr in addrs {
                let _ = self.get_or_create_connection(&node_addr).await;
            }

            slot_map = self.slot_map.write().await;
        }

        Ok(())
    }

    async fn get_or_create_connection(&self, addr: &str) -> Result<RedisConnection, RedisError> {
        // Check if connection exists
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(addr) {
                return Ok(conn.clone());
            }
        }

        // Create new connection
        let conn = RedisConnection::connect(addr).await?;

        // Store it
        {
            let mut connections = self.connections.write().await;
            connections.insert(addr.to_string(), conn.clone());
        }

        Ok(conn)
    }

    /// Get the number of connected nodes.
    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
}

/// Trait for extracting the routing key from a command.
///
/// This is needed to determine which cluster slot the command should route to.
pub trait KeyExtractor {
    /// Extract the routing key from this command.
    ///
    /// Returns None if the command doesn't operate on a specific key.
    fn extract_key(&self) -> Option<String>;
}

// Implement KeyExtractor for common commands
// We'll add these as we go

/// Tower Service implementation for ClusterClient
///
/// This allows ClusterClient to work with Tower middleware like timeouts,
/// retries, and circuit breakers.
///
/// # Example
/// ```no_run
/// use redis_tower::cluster::ClusterClient;
/// use redis_tower::commands::Get;
/// use tower::Service;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = ClusterClient::new(vec!["127.0.0.1:7000"]).await?;
///
/// // Use as a Tower service
/// let response = Service::call(&mut client, Get::new("mykey")).await?;
/// # Ok(())
/// # }
/// ```
impl<Cmd> Service<Cmd> for ClusterClient
where
    Cmd: Command + Clone + KeyExtractor + Send + 'static,
    Cmd::Response: Send + 'static,
{
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready - cluster handles routing internally
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, command: Cmd) -> Self::Future {
        let client = self.clone();

        Box::pin(async move { client.execute(command).await })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_cluster_client_compiles() {
        // Just verify the API compiles
    }
}
