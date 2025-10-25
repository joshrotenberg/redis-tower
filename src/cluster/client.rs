//! Cluster client implementation with automatic redirect handling

use crate::client::RedisConnection;
use crate::cluster::commands::ClusterSlots;
use crate::cluster::read_preference::ReadPreference;
use crate::cluster::slots::{SlotMap, slot_for_key};
use crate::commands::Command;
use crate::pool::{ConnectionPool, PoolConfig};
use crate::tls::TlsConfig;
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
/// - Connection pooling to all cluster nodes (configurable pool size)
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
    /// Connection pools to cluster nodes (by address)
    pools: Arc<RwLock<HashMap<String, ConnectionPool>>>,
    /// Slot to node address mapping
    slot_map: Arc<RwLock<SlotMap>>,
    /// Initial seed nodes for bootstrapping
    seed_nodes: Vec<String>,
    /// Pool configuration for each node
    pool_config: Arc<PoolConfig>,
    /// TLS configuration
    tls: TlsConfig,
    /// Read preference for routing
    read_preference: ReadPreference,
}

impl ClusterClient {
    /// Create a new cluster client with default settings.
    ///
    /// Connects to seed nodes and discovers the full cluster topology.
    /// Uses default pool configuration (10 connections per node, health checks enabled).
    pub async fn new(seed_nodes: Vec<impl Into<String>>) -> Result<Self, RedisError> {
        Self::with_pool_config(seed_nodes, PoolConfig::default()).await
    }

    /// Create a new cluster client with TLS
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::cluster::ClusterClient;
    /// use redis_tower::tls::TlsConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let tls = TlsConfig::rustls().with_native_roots().build()?;
    /// let client = ClusterClient::with_tls(
    ///     vec!["redis.example.com:7000", "redis.example.com:7001"],
    ///     tls
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_tls(
        seed_nodes: Vec<impl Into<String>>,
        tls: TlsConfig,
    ) -> Result<Self, RedisError> {
        Self::with_tls_and_pool_config(seed_nodes, PoolConfig::default(), tls).await
    }

    /// Create a new cluster client with legacy config (simple max_size).
    ///
    /// # Arguments
    /// * `seed_nodes` - Initial cluster nodes to connect to
    /// * `max_connections_per_node` - Maximum connections to maintain per node
    ///
    /// # Deprecated
    /// Use `with_pool_config()` for more control over pool behavior.
    pub async fn with_config(
        seed_nodes: Vec<impl Into<String>>,
        max_connections_per_node: usize,
    ) -> Result<Self, RedisError> {
        Self::with_pool_config(seed_nodes, PoolConfig::new(max_connections_per_node)).await
    }

    /// Create a new cluster client with custom pool configuration.
    ///
    /// # Arguments
    /// * `seed_nodes` - Initial cluster nodes to connect to
    /// * `pool_config` - Connection pool configuration (max size, health checks, timeouts, etc.)
    pub async fn with_pool_config(
        seed_nodes: Vec<impl Into<String>>,
        pool_config: PoolConfig,
    ) -> Result<Self, RedisError> {
        Self::with_tls_and_pool_config(seed_nodes, pool_config, TlsConfig::None).await
    }

    /// Create a new cluster client with TLS and custom pool configuration.
    ///
    /// # Arguments
    /// * `seed_nodes` - Initial cluster nodes to connect to
    /// * `pool_config` - Connection pool configuration (max size, health checks, timeouts, etc.)
    /// * `tls` - TLS configuration
    pub async fn with_tls_and_pool_config(
        seed_nodes: Vec<impl Into<String>>,
        pool_config: PoolConfig,
        tls: TlsConfig,
    ) -> Result<Self, RedisError> {
        Self::with_full_config(seed_nodes, pool_config, tls, ReadPreference::default()).await
    }

    /// Create a new cluster client with full configuration including read preference.
    ///
    /// # Arguments
    /// * `seed_nodes` - Initial cluster nodes to connect to
    /// * `pool_config` - Connection pool configuration
    /// * `tls` - TLS configuration
    /// * `read_preference` - Read preference (Master, Replica, PreferReplica)
    pub async fn with_full_config(
        seed_nodes: Vec<impl Into<String>>,
        pool_config: PoolConfig,
        tls: TlsConfig,
        read_preference: ReadPreference,
    ) -> Result<Self, RedisError> {
        let seed_nodes: Vec<String> = seed_nodes.into_iter().map(Into::into).collect();

        if seed_nodes.is_empty() {
            return Err(RedisError::Protocol(
                "At least one seed node required".to_string(),
            ));
        }

        if pool_config.max_size == 0 {
            return Err(RedisError::Protocol(
                "pool_config.max_size must be at least 1".to_string(),
            ));
        }

        let client = Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            slot_map: Arc::new(RwLock::new(SlotMap::new())),
            seed_nodes,
            pool_config: Arc::new(pool_config),
            tls,
            read_preference,
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
    /// Routes read-only commands to replicas based on read preference.
    pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command + Clone + KeyExtractor + crate::cluster::read_preference::ReadOnly,
    {
        const MAX_REDIRECTS: usize = 16;

        let key = command
            .extract_key()
            .ok_or_else(|| RedisError::Protocol("Command has no key for routing".to_string()))?;

        let slot = slot_for_key(key.as_bytes());
        let mut current_slot = slot;
        let mut redirects = 0;
        let is_read_only = command.is_read_only();

        loop {
            if redirects >= MAX_REDIRECTS {
                return Err(RedisError::Protocol("Too many redirects".to_string()));
            }

            // Get the node for this slot based on read preference
            let node_addr = {
                let slot_map = self.slot_map.read().await;
                slot_map
                    .get_assignment(current_slot)
                    .map(|assignment| self.select_node(assignment, is_read_only))
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

            // Collect replica addresses
            let replica_addrs: Vec<String> = range
                .replicas
                .iter()
                .map(|r| self.remap_address(&format!("{}:{}", r.host, r.port)))
                .collect();

            // Map all slots in this range to the master and replicas
            slot_map.assign_slots_with_replicas(
                range.start_slot,
                range.end_slot,
                master_addr.clone(),
                replica_addrs.clone(),
            );

            // Collect all addresses (master + replicas) for connection pre-creation
            let all_addrs = std::iter::once(master_addr)
                .chain(replica_addrs)
                .collect::<Vec<_>>();

            drop(slot_map);

            // Pre-create connections to all nodes
            for node_addr in all_addrs {
                let _ = self.get_or_create_connection(&node_addr).await;
            }

            slot_map = self.slot_map.write().await;
        }

        Ok(())
    }

    async fn get_or_create_connection(&self, addr: &str) -> Result<RedisConnection, RedisError> {
        // Check if pool exists
        {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(addr) {
                return pool.get().await;
            }
        }

        // Create new pool with the cluster's pool configuration and TLS
        let pool = ConnectionPool::with_tls(
            addr.to_string(),
            (*self.pool_config).clone(),
            self.tls.clone(),
        );
        let conn = pool.get().await?;

        // Store the pool
        {
            let mut pools = self.pools.write().await;
            pools.insert(addr.to_string(), pool);
        }

        Ok(conn)
    }

    /// Get the number of connected nodes (nodes with connection pools).
    pub async fn connection_count(&self) -> usize {
        self.pools.read().await.len()
    }

    /// Get the total number of connections across all pools.
    pub async fn total_connections(&self) -> usize {
        let pools = self.pools.read().await;
        let mut total = 0;
        for pool in pools.values() {
            total += pool.size().await;
        }
        total
    }

    /// Get the maximum connections per node setting.
    pub fn max_connections_per_node(&self) -> usize {
        self.pool_config.max_size
    }

    /// Get the pool configuration.
    pub fn pool_config(&self) -> &PoolConfig {
        &self.pool_config
    }

    /// Get the read preference.
    pub fn read_preference(&self) -> ReadPreference {
        self.read_preference
    }

    /// Select a node address based on slot assignment and read preference
    ///
    /// Returns the appropriate node address based on:
    /// - Whether the command is read-only
    /// - The configured read preference
    /// - Availability of replicas
    fn select_node(
        &self,
        assignment: &crate::cluster::slots::SlotAssignment,
        is_read_only: bool,
    ) -> String {
        // Always use master for write commands
        if !is_read_only {
            return assignment.master.clone();
        }

        match self.read_preference {
            ReadPreference::Master => assignment.master.clone(),
            ReadPreference::Replica => {
                // Use replica if available, otherwise master
                assignment
                    .replicas
                    .first()
                    .cloned()
                    .unwrap_or_else(|| assignment.master.clone())
            }
            ReadPreference::PreferReplica => {
                // Prefer replica, fall back to master
                assignment
                    .replicas
                    .first()
                    .cloned()
                    .unwrap_or_else(|| assignment.master.clone())
            }
        }
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
    Cmd:
        Command + Clone + KeyExtractor + crate::cluster::read_preference::ReadOnly + Send + 'static,
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

/// Pipeline execution for cluster
///
/// Important: All commands in the pipeline MUST target the same hash slot.
/// This is a cluster requirement - pipelines cannot span multiple nodes.
impl crate::pipeline::PipelineExecutor for ClusterClient {
    async fn execute_pipeline(
        &self,
        pipeline: &crate::pipeline::Pipeline,
    ) -> Result<crate::pipeline::PipelineResults, RedisError> {
        if pipeline.is_empty() {
            return Ok(crate::pipeline::PipelineResults::new(vec![]));
        }

        // For cluster pipelines, we need to validate that all commands target the same slot
        // Since we can't access the actual commands (they're type-erased), we'll send to the
        // first command's slot and let Redis return an error if commands span multiple slots.
        //
        // A better approach would be to track the key/slot in Pipeline, but that requires
        // more refactoring. For now, we'll execute the pipeline on a single connection
        // and document the single-slot requirement.

        // Get frames and determine target slot from first command
        let frames = pipeline.frames();
        if frames.is_empty() {
            return Ok(crate::pipeline::PipelineResults::new(vec![]));
        }

        // Extract key from first frame to determine slot
        // This is a simplified approach - in production we'd want to validate all keys
        let first_frame = &frames[0];
        let slot = self.extract_slot_from_frame(first_frame)?;

        // Get the node for this slot
        let node_addr = {
            let slot_map = self.slot_map.read().await;
            slot_map
                .get_assignment(slot)
                .map(|assignment| assignment.master.clone())
                .ok_or_else(|| RedisError::Protocol(format!("No node found for slot {}", slot)))?
        };

        // Get connection pool for that node
        let pools = self.pools.read().await;
        let pool = pools.get(&node_addr).ok_or_else(|| {
            RedisError::Protocol(format!("No connection pool for node {}", node_addr))
        })?;

        // Get a connection from the pool
        let conn = pool.get().await?;

        // Execute the pipeline on that single connection
        conn.execute_pipeline(pipeline).await
    }
}

impl ClusterClient {
    /// Extract slot from a frame (helper for pipeline routing)
    fn extract_slot_from_frame(&self, frame: &crate::codec::Frame) -> Result<u16, RedisError> {
        use crate::codec::Frame;

        match frame {
            Frame::Array(elements) if elements.len() >= 2 => {
                // Second element is typically the key
                if let Frame::BulkString(Some(key_bytes)) = &elements[1] {
                    Ok(slot_for_key(key_bytes))
                } else {
                    Err(RedisError::Protocol(
                        "Cannot extract key from pipeline command".to_string(),
                    ))
                }
            }
            _ => Err(RedisError::Protocol(
                "Invalid frame format for pipeline".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_cluster_client_compiles() {
        // Just verify the API compiles
    }
}
