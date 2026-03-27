//! Redis Cluster lifecycle management.
//!
//! Generates per-node configs and shells out to `redis-server` / `redis-cli`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;

use crate::util;

/// Configuration for the cluster as a whole.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Base port; nodes use base_port, base_port+1, etc.
    pub base_port: u16,
    /// Number of master nodes.
    pub masters: u16,
    /// Number of replicas per master.
    pub replicas_per_master: u16,
    /// Bind address for all nodes.
    pub bind: String,
    /// Working directory root — each node gets a subdirectory.
    pub work_dir: PathBuf,
    /// Path to `redis-server` binary.
    pub redis_server_bin: String,
    /// Path to `redis-cli` binary.
    pub redis_cli_bin: String,
    /// Additional redis.conf directives applied to every node.
    pub extra_config: HashMap<String, String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            base_port: 7000,
            masters: 3,
            replicas_per_master: 1,
            bind: "127.0.0.1".into(),
            work_dir: PathBuf::from("/tmp/redis-cluster"),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
            extra_config: HashMap::new(),
        }
    }
}

impl ClusterConfig {
    /// Total number of nodes (masters + all replicas).
    pub fn total_nodes(&self) -> u16 {
        self.masters * (1 + self.replicas_per_master)
    }

    /// Iterator over all node ports.
    pub fn ports(&self) -> impl Iterator<Item = u16> {
        let base = self.base_port;
        let total = self.total_nodes();
        (0..total).map(move |i| base + i)
    }
}

/// Represents a running (or stopped) Redis Cluster.
#[derive(Debug)]
pub struct RedisCluster {
    config: ClusterConfig,
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

/// Per-node status.
#[derive(Debug)]
pub struct NodeStatus {
    pub port: u16,
    pub alive: bool,
    pub role: Option<String>,
    pub cluster_info: Option<String>,
}

impl RedisCluster {
    pub fn new(config: ClusterConfig) -> Self {
        Self { config }
    }

    /// 3 masters, 1 replica each, ports 7000-7005.
    pub fn with_defaults() -> Self {
        Self::new(ClusterConfig::default())
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    fn write_node_config(&self, port: u16) -> io::Result<PathBuf> {
        let node_dir = self.config.work_dir.join(format!("node-{port}"));
        fs::create_dir_all(&node_dir)?;

        let conf_path = node_dir.join("redis.conf");
        let mut conf = format!(
            r#"port {port}
bind {bind}
daemonize yes
pidfile {node_dir}/redis.pid
logfile {node_dir}/redis.log
dir {node_dir}
dbfilename dump-{port}.rdb
appendonly yes
appendfilename appendonly-{port}.aof
cluster-enabled yes
cluster-config-file nodes-{port}.conf
cluster-node-timeout 5000
"#,
            port = port,
            bind = self.config.bind,
            node_dir = node_dir.display(),
        );

        for (key, value) in &self.config.extra_config {
            conf.push_str(&format!("{key} {value}\n"));
        }

        fs::write(&conf_path, conf)?;
        Ok(conf_path)
    }

    /// Start all nodes and form the cluster.
    pub fn start(&self) -> io::Result<()> {
        if self.config.work_dir.exists() {
            fs::remove_dir_all(&self.config.work_dir)?;
        }
        fs::create_dir_all(&self.config.work_dir)?;

        let timeout = Duration::from_secs(10);

        for port in self.config.ports() {
            let conf_path = self.write_node_config(port)?;
            util::start_redis_server(&self.config.redis_server_bin, &conf_path)?;
        }

        // Wait for all nodes to accept connections
        for port in self.config.ports() {
            util::wait_for_ping(&self.config.redis_cli_bin, &self.config.bind, port, timeout)?;
        }

        // Form the cluster
        self.cluster_create()?;

        // Let the cluster converge
        thread::sleep(Duration::from_secs(2));

        Ok(())
    }

    fn cluster_create(&self) -> io::Result<()> {
        let mut args: Vec<String> = vec!["--cluster".into(), "create".into()];

        for port in self.config.ports() {
            args.push(format!("{}:{}", self.config.bind, port));
        }

        if self.config.replicas_per_master > 0 {
            args.push("--cluster-replicas".into());
            args.push(self.config.replicas_per_master.to_string());
        }

        args.push("--cluster-yes".into());

        let output = Command::new(&self.config.redis_cli_bin)
            .args(&args)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(io::Error::other(format!(
                "cluster create failed:\nstdout: {stdout}\nstderr: {stderr}"
            )));
        }

        Ok(())
    }

    /// Stop all nodes via SHUTDOWN NOSAVE.
    pub fn stop(&self) -> io::Result<()> {
        for port in self.config.ports() {
            util::shutdown_node(&self.config.redis_cli_bin, &self.config.bind, port);
        }
        Ok(())
    }

    pub fn restart(&self) -> io::Result<()> {
        let _ = self.stop();
        thread::sleep(Duration::from_secs(1));
        self.start()
    }

    /// Query first reachable node for CLUSTER INFO.
    pub fn poke(&self) -> io::Result<ClusterStatus> {
        for port in self.config.ports() {
            if let Ok(raw) = util::redis_cli(
                &self.config.redis_cli_bin,
                &self.config.bind,
                port,
                &["CLUSTER", "INFO"],
            ) {
                return Ok(parse_cluster_info(&raw));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable cluster nodes",
        ))
    }

    /// Check every node individually.
    pub fn poke_all(&self) -> Vec<NodeStatus> {
        self.config
            .ports()
            .map(|port| {
                let alive = util::redis_cli(
                    &self.config.redis_cli_bin,
                    &self.config.bind,
                    port,
                    &["PING"],
                )
                .map(|r| r.trim() == "PONG")
                .unwrap_or(false);

                let (role, cluster_info) = if alive {
                    let role = util::redis_cli(
                        &self.config.redis_cli_bin,
                        &self.config.bind,
                        port,
                        &["ROLE"],
                    )
                    .ok()
                    .and_then(|r| r.lines().next().map(|s| s.to_string()));

                    let info = util::redis_cli(
                        &self.config.redis_cli_bin,
                        &self.config.bind,
                        port,
                        &["CLUSTER", "INFO"],
                    )
                    .ok();

                    (role, info)
                } else {
                    (None, None)
                };

                NodeStatus {
                    port,
                    alive,
                    role,
                    cluster_info,
                }
            })
            .collect()
    }

    /// Wait until CLUSTER INFO reports state=ok with all slots assigned.
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
            thread::sleep(Duration::from_millis(500));
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
