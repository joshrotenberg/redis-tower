//! Redis Cluster lifecycle management.
//!
//! Delegates to [`crate::wrapper::cluster`] for the actual process management.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crate::wrapper::cluster::{RedisCluster as WrapperCluster, RedisClusterHandle};

/// Configuration for the cluster as a whole.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub base_port: u16,
    pub masters: u16,
    pub replicas_per_master: u16,
    pub bind: String,
    pub redis_server_bin: String,
    pub redis_cli_bin: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            base_port: 7000,
            masters: 3,
            replicas_per_master: 1,
            bind: "127.0.0.1".into(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
        }
    }
}

impl ClusterConfig {
    pub fn total_nodes(&self) -> u16 {
        self.masters * (1 + self.replicas_per_master)
    }

    pub fn ports(&self) -> impl Iterator<Item = u16> {
        let base = self.base_port;
        let total = self.total_nodes();
        (0..total).map(move |i| base + i)
    }
}

/// Status returned by `poke()`.
#[derive(Debug)]
pub struct ClusterStatus {
    pub cluster_state: String,
    pub cluster_slots_assigned: u64,
    pub cluster_slots_ok: u64,
    pub cluster_known_nodes: u64,
    pub cluster_size: u64,
    pub raw: String,
}

/// Represents a running (or stopped) Redis Cluster.
pub struct RedisCluster {
    config: ClusterConfig,
    #[allow(dead_code)]
    handle: Option<RedisClusterHandle>,
}

impl RedisCluster {
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            handle: None,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(ClusterConfig::default())
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn start(&self) -> io::Result<()> {
        // Use the wrapper to start. We can't store the handle in &self,
        // so we leak it (it lives for the duration of the test suite via OnceLock).
        let handle = WrapperCluster::builder()
            .masters(self.config.masters)
            .replicas_per_master(self.config.replicas_per_master)
            .base_port(self.config.base_port)
            .bind(&self.config.bind)
            .redis_server_bin(&self.config.redis_server_bin)
            .redis_cli_bin(&self.config.redis_cli_bin)
            .start()?;

        // Leak the handle so it lives for the static lifetime (OnceLock pattern).
        // The Drop impl on RedisServerHandle will never run, but we call stop()
        // explicitly in our own Drop.
        std::mem::forget(handle);
        Ok(())
    }

    pub fn stop(&self) -> io::Result<()> {
        use crate::wrapper::cli::RedisCli;
        for port in self.config.ports() {
            RedisCli::new()
                .bin(&self.config.redis_cli_bin)
                .host(&self.config.bind)
                .port(port)
                .shutdown();
        }
        Ok(())
    }

    pub fn poke(&self) -> io::Result<ClusterStatus> {
        use crate::wrapper::cli::RedisCli;
        for port in self.config.ports() {
            let cli = RedisCli::new()
                .bin(&self.config.redis_cli_bin)
                .host(&self.config.bind)
                .port(port);
            if let Ok(raw) = cli.run(&["CLUSTER", "INFO"]) {
                return Ok(parse_cluster_info(&raw));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable cluster nodes",
        ))
    }

    pub fn wait_for_healthy(&self, timeout: Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        loop {
            if let Ok(status) = self.poke() {
                if status.cluster_state == "ok" && status.cluster_slots_ok == 16384 {
                    return Ok(());
                }
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
}

impl Drop for RedisCluster {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn parse_cluster_info(raw: &str) -> ClusterStatus {
    let mut map: HashMap<String, String> = HashMap::new();
    for line in raw.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once(':') {
            map.insert(k.to_string(), v.trim().to_string());
        }
    }

    let get_u64 = |key: &str| -> u64 { map.get(key).and_then(|v| v.parse().ok()).unwrap_or(0) };

    ClusterStatus {
        cluster_state: map
            .get("cluster_state")
            .cloned()
            .unwrap_or_else(|| "unknown".into()),
        cluster_slots_assigned: get_u64("cluster_slots_assigned"),
        cluster_slots_ok: get_u64("cluster_slots_ok"),
        cluster_known_nodes: get_u64("cluster_known_nodes"),
        cluster_size: get_u64("cluster_size"),
        raw: raw.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_defaults() {
        let cfg = ClusterConfig::default();
        assert_eq!(cfg.total_nodes(), 6);
        let ports: Vec<u16> = cfg.ports().collect();
        assert_eq!(ports, vec![7000, 7001, 7002, 7003, 7004, 7005]);
    }

    #[test]
    fn test_cluster_config_no_replicas() {
        let cfg = ClusterConfig {
            masters: 3,
            replicas_per_master: 0,
            ..Default::default()
        };
        assert_eq!(cfg.total_nodes(), 3);
    }

    #[test]
    fn test_parse_cluster_info() {
        let raw = "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:6\r\ncluster_size:3\r\n";
        let status = parse_cluster_info(raw);
        assert_eq!(status.cluster_state, "ok");
        assert_eq!(status.cluster_slots_assigned, 16384);
        assert_eq!(status.cluster_slots_ok, 16384);
        assert_eq!(status.cluster_known_nodes, 6);
        assert_eq!(status.cluster_size, 3);
    }
}
