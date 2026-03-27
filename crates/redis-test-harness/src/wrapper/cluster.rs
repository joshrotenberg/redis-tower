//! Redis Cluster lifecycle management built on `RedisServer`.

use std::io;
use std::time::Duration;

use super::cli::RedisCli;
use super::server::{RedisServer, RedisServerHandle};

/// Builder for a Redis Cluster.
///
/// # Example
///
/// ```no_run
/// use redis_test_harness::wrapper::cluster::RedisCluster;
///
/// let cluster = RedisCluster::builder()
///     .masters(3)
///     .replicas_per_master(1)
///     .base_port(7000)
///     .start()
///     .unwrap();
///
/// assert!(cluster.is_healthy());
/// // Stopped automatically on Drop.
/// ```
pub struct RedisClusterBuilder {
    masters: u16,
    replicas_per_master: u16,
    base_port: u16,
    bind: String,
    redis_server_bin: String,
    redis_cli_bin: String,
}

impl RedisClusterBuilder {
    pub fn masters(mut self, n: u16) -> Self {
        self.masters = n;
        self
    }

    pub fn replicas_per_master(mut self, n: u16) -> Self {
        self.replicas_per_master = n;
        self
    }

    pub fn base_port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }

    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.redis_server_bin = bin.into();
        self
    }

    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.redis_cli_bin = bin.into();
        self
    }

    fn total_nodes(&self) -> u16 {
        self.masters * (1 + self.replicas_per_master)
    }

    fn ports(&self) -> impl Iterator<Item = u16> {
        let base = self.base_port;
        let total = self.total_nodes();
        (0..total).map(move |i| base + i)
    }

    /// Start all nodes and form the cluster.
    pub fn start(self) -> io::Result<RedisClusterHandle> {
        // Stop any leftover nodes from previous runs.
        for port in self.ports() {
            RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(port)
                .shutdown();
        }
        std::thread::sleep(Duration::from_millis(500));

        // Start each node.
        let mut nodes = Vec::new();
        for port in self.ports() {
            let handle = RedisServer::new()
                .port(port)
                .bind(&self.bind)
                .dir(format!("/tmp/redis-cluster-wrapper/node-{port}"))
                .cluster_enabled(true)
                .cluster_node_timeout(5000)
                .redis_server_bin(&self.redis_server_bin)
                .redis_cli_bin(&self.redis_cli_bin)
                .start()?;
            nodes.push(handle);
        }

        // Form the cluster.
        let node_addrs: Vec<String> = nodes.iter().map(|n| n.addr()).collect();
        let cli = RedisCli::new()
            .bin(&self.redis_cli_bin)
            .host(&self.bind)
            .port(self.base_port);
        cli.cluster_create(&node_addrs, self.replicas_per_master)?;

        // Wait for convergence.
        std::thread::sleep(Duration::from_secs(2));

        Ok(RedisClusterHandle {
            nodes,
            bind: self.bind,
            base_port: self.base_port,
            redis_cli_bin: self.redis_cli_bin,
        })
    }
}

/// A running Redis Cluster. Stops all nodes on Drop.
pub struct RedisClusterHandle {
    nodes: Vec<RedisServerHandle>,
    bind: String,
    base_port: u16,
    redis_cli_bin: String,
}

/// Convenience constructor.
pub struct RedisCluster;

impl RedisCluster {
    /// Create a new cluster builder with defaults (3 masters, 0 replicas, port 7000).
    pub fn builder() -> RedisClusterBuilder {
        RedisClusterBuilder {
            masters: 3,
            replicas_per_master: 0,
            base_port: 7000,
            bind: "127.0.0.1".into(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
        }
    }
}

impl RedisClusterHandle {
    /// The seed address (first node).
    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind, self.base_port)
    }

    /// All node addresses.
    pub fn node_addrs(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.addr()).collect()
    }

    /// Check if all nodes are alive.
    pub fn all_alive(&self) -> bool {
        self.nodes.iter().all(|n| n.is_alive())
    }

    /// Check CLUSTER INFO for state=ok and all slots assigned.
    pub fn is_healthy(&self) -> bool {
        for node in &self.nodes {
            if let Ok(info) = node.run(&["CLUSTER", "INFO"]) {
                if info.contains("cluster_state:ok") && info.contains("cluster_slots_ok:16384") {
                    return true;
                }
            }
        }
        false
    }

    /// Wait until the cluster is healthy or timeout.
    pub fn wait_for_healthy(&self, timeout: Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.is_healthy() {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cluster did not become healthy in time",
                ));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Get a `RedisCli` for the seed node.
    pub fn cli(&self) -> RedisCli {
        RedisCli::new()
            .bin(&self.redis_cli_bin)
            .host(&self.bind)
            .port(self.base_port)
    }
}

impl Drop for RedisClusterHandle {
    fn drop(&mut self) {
        // RedisServerHandle::drop() handles each node.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = RedisCluster::builder();
        assert_eq!(b.masters, 3);
        assert_eq!(b.replicas_per_master, 0);
        assert_eq!(b.base_port, 7000);
        assert_eq!(b.total_nodes(), 3);
    }

    #[test]
    fn builder_with_replicas() {
        let b = RedisCluster::builder().masters(3).replicas_per_master(1);
        assert_eq!(b.total_nodes(), 6);
        let ports: Vec<u16> = b.ports().collect();
        assert_eq!(ports, vec![7000, 7001, 7002, 7003, 7004, 7005]);
    }
}
