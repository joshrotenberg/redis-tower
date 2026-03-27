//! Manage a Redis Sentinel topology: one master, N replicas, M sentinels.
//!
//! Designed as a test harness helper. Generates configs and shells out to
//! `redis-server` — no client library dependencies.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use crate::util;

// ── Config ──────────────────────────────────────────────────────────────────

/// Configuration for the sentinel topology.
#[derive(Debug, Clone)]
pub struct SentinelConfig {
    /// The monitored master name (used in sentinel.conf as the group name).
    pub master_name: String,
    /// Port for the master instance.
    pub master_port: u16,
    /// Number of replica instances.
    pub num_replicas: u16,
    /// Base port for replicas (replicas use base, base+1, ...).
    pub replica_base_port: u16,
    /// Number of sentinel processes.
    pub num_sentinels: u16,
    /// Base port for sentinels.
    pub sentinel_base_port: u16,
    /// Quorum — number of sentinels that must agree on a failover.
    pub quorum: u16,
    /// Bind address for all processes.
    pub bind: String,
    /// Working directory root.
    pub work_dir: PathBuf,
    /// Path to `redis-server` binary.
    pub redis_server_bin: String,
    /// Path to `redis-cli` binary.
    pub redis_cli_bin: String,
    /// Sentinel down-after-milliseconds (how quickly sentinel considers a node down).
    pub down_after_ms: u64,
    /// Sentinel failover-timeout in milliseconds.
    pub failover_timeout_ms: u64,
    /// Extra redis.conf directives for the master and replicas.
    pub extra_data_config: HashMap<String, String>,
    /// Extra sentinel.conf directives.
    pub extra_sentinel_config: HashMap<String, String>,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            master_name: "mymaster".into(),
            master_port: 6380,
            num_replicas: 2,
            replica_base_port: 6381,
            num_sentinels: 3,
            sentinel_base_port: 26379,
            quorum: 2,
            bind: "127.0.0.1".into(),
            work_dir: PathBuf::from("/tmp/redis-sentinel"),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
            down_after_ms: 5000,
            failover_timeout_ms: 10000,
            extra_data_config: HashMap::new(),
            extra_sentinel_config: HashMap::new(),
        }
    }
}

impl SentinelConfig {
    /// Iterator over all replica ports.
    pub fn replica_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.replica_base_port;
        let n = self.num_replicas;
        (0..n).map(move |i| base + i)
    }

    /// Iterator over all sentinel ports.
    pub fn sentinel_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.sentinel_base_port;
        let n = self.num_sentinels;
        (0..n).map(move |i| base + i)
    }

    /// All data node ports (master + replicas).
    pub fn data_ports(&self) -> Vec<u16> {
        let mut ports = vec![self.master_port];
        ports.extend(self.replica_ports());
        ports
    }

    /// All ports (data + sentinels).
    pub fn all_ports(&self) -> Vec<u16> {
        let mut ports = self.data_ports();
        ports.extend(self.sentinel_ports());
        ports
    }
}

// ── Sentinel Manager ────────────────────────────────────────────────────────

/// Manages the full sentinel topology lifecycle.
#[derive(Debug)]
pub struct RedisSentinel {
    config: SentinelConfig,
}

/// Status from `SENTINEL MASTER <name>`.
#[derive(Debug)]
pub struct SentinelMasterStatus {
    pub name: String,
    pub ip: String,
    pub port: u16,
    pub flags: String,
    pub num_slaves: u64,
    pub num_sentinels: u64,
    pub quorum: u64,
    pub raw: HashMap<String, String>,
}

/// Per-node liveness.
#[derive(Debug)]
pub struct NodeStatus {
    pub port: u16,
    pub kind: NodeKind,
    pub alive: bool,
    pub role: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Master,
    Replica,
    Sentinel,
}

impl RedisSentinel {
    pub fn new(config: SentinelConfig) -> Self {
        Self { config }
    }

    pub fn with_defaults() -> Self {
        Self::new(SentinelConfig::default())
    }

    pub fn config(&self) -> &SentinelConfig {
        &self.config
    }

    // ── Config generation ───────────────────────────────────────────────

    fn write_master_config(&self) -> io::Result<PathBuf> {
        let dir = self.config.work_dir.join("master");
        fs::create_dir_all(&dir)?;
        let conf_path = dir.join("redis.conf");

        let mut conf = format!(
            r#"port {port}
bind {bind}
daemonize yes
pidfile {dir}/redis.pid
logfile {dir}/redis.log
dir {dir}
dbfilename dump.rdb
appendonly yes
"#,
            port = self.config.master_port,
            bind = self.config.bind,
            dir = dir.display(),
        );

        for (k, v) in &self.config.extra_data_config {
            conf.push_str(&format!("{k} {v}\n"));
        }

        fs::write(&conf_path, conf)?;
        Ok(conf_path)
    }

    fn write_replica_config(&self, port: u16) -> io::Result<PathBuf> {
        let dir = self.config.work_dir.join(format!("replica-{port}"));
        fs::create_dir_all(&dir)?;
        let conf_path = dir.join("redis.conf");

        let mut conf = format!(
            r#"port {port}
bind {bind}
daemonize yes
pidfile {dir}/redis.pid
logfile {dir}/redis.log
dir {dir}
dbfilename dump.rdb
appendonly yes
replicaof {master_host} {master_port}
"#,
            port = port,
            bind = self.config.bind,
            dir = dir.display(),
            master_host = self.config.bind,
            master_port = self.config.master_port,
        );

        for (k, v) in &self.config.extra_data_config {
            conf.push_str(&format!("{k} {v}\n"));
        }

        fs::write(&conf_path, conf)?;
        Ok(conf_path)
    }

    fn write_sentinel_config(&self, port: u16) -> io::Result<PathBuf> {
        let dir = self.config.work_dir.join(format!("sentinel-{port}"));
        fs::create_dir_all(&dir)?;
        let conf_path = dir.join("sentinel.conf");

        let mut conf = format!(
            r#"port {port}
bind {bind}
daemonize yes
pidfile {dir}/sentinel.pid
logfile {dir}/sentinel.log
dir {dir}

sentinel monitor {name} {master_host} {master_port} {quorum}
sentinel down-after-milliseconds {name} {down_after}
sentinel failover-timeout {name} {failover_timeout}
sentinel parallel-syncs {name} 1
"#,
            port = port,
            bind = self.config.bind,
            dir = dir.display(),
            name = self.config.master_name,
            master_host = self.config.bind,
            master_port = self.config.master_port,
            quorum = self.config.quorum,
            down_after = self.config.down_after_ms,
            failover_timeout = self.config.failover_timeout_ms,
        );

        for (k, v) in &self.config.extra_sentinel_config {
            conf.push_str(&format!("{k} {v}\n"));
        }

        fs::write(&conf_path, conf)?;
        Ok(conf_path)
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Start the full topology: master → replicas → sentinels.
    pub fn start(&self) -> io::Result<()> {
        // Clean slate
        if self.config.work_dir.exists() {
            fs::remove_dir_all(&self.config.work_dir)?;
        }
        fs::create_dir_all(&self.config.work_dir)?;

        let timeout = Duration::from_secs(10);
        let cli = &self.config.redis_cli_bin;
        let host = &self.config.bind;

        // 1. Master
        let conf = self.write_master_config()?;
        util::start_redis_server(&self.config.redis_server_bin, &conf)?;
        util::wait_for_ping(cli, host, self.config.master_port, timeout)?;

        // 2. Replicas
        for port in self.config.replica_ports() {
            let conf = self.write_replica_config(port)?;
            util::start_redis_server(&self.config.redis_server_bin, &conf)?;
            util::wait_for_ping(cli, host, port, timeout)?;
        }

        // Small delay for replication to link up
        thread::sleep(Duration::from_secs(1));

        // 3. Sentinels (launched via `redis-server <conf> --sentinel`)
        for port in self.config.sentinel_ports() {
            let conf = self.write_sentinel_config(port)?;
            let status = std::process::Command::new(&self.config.redis_server_bin)
                .arg(&conf)
                .arg("--sentinel")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()?;

            if !status.success() {
                return Err(io::Error::other(format!(
                    "sentinel failed to start on port {port}"
                )));
            }
            util::wait_for_ping(cli, host, port, timeout)?;
        }

        // Wait for sentinels to discover the topology
        thread::sleep(Duration::from_secs(2));

        Ok(())
    }

    /// Stop everything: sentinels first, then replicas, then master.
    pub fn stop(&self) -> io::Result<()> {
        let cli = &self.config.redis_cli_bin;
        let host = &self.config.bind;

        // Sentinels first
        for port in self.config.sentinel_ports() {
            util::shutdown_node(cli, host, port);
        }
        // Replicas
        for port in self.config.replica_ports() {
            util::shutdown_node(cli, host, port);
        }
        // Master last
        util::shutdown_node(cli, host, self.config.master_port);

        Ok(())
    }

    /// Restart = stop + start.
    pub fn restart(&self) -> io::Result<()> {
        let _ = self.stop();
        thread::sleep(Duration::from_secs(1));
        self.start()
    }

    // ── Poking ──────────────────────────────────────────────────────────

    /// Query the first reachable sentinel for master status.
    pub fn poke(&self) -> io::Result<SentinelMasterStatus> {
        let cli = &self.config.redis_cli_bin;
        let host = &self.config.bind;

        for port in self.config.sentinel_ports() {
            if let Ok(raw) = util::redis_cli(
                cli,
                host,
                port,
                &["SENTINEL", "MASTER", &self.config.master_name],
            ) {
                return Ok(parse_sentinel_master(&raw, &self.config.master_name));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable sentinel",
        ))
    }

    /// Check every node individually.
    pub fn poke_all(&self) -> Vec<NodeStatus> {
        let cli = &self.config.redis_cli_bin;
        let host = &self.config.bind;

        let mut results = Vec::new();

        // Master
        results.push(probe_node(
            cli,
            host,
            self.config.master_port,
            NodeKind::Master,
        ));

        // Replicas
        for port in self.config.replica_ports() {
            results.push(probe_node(cli, host, port, NodeKind::Replica));
        }

        // Sentinels
        for port in self.config.sentinel_ports() {
            results.push(probe_node(cli, host, port, NodeKind::Sentinel));
        }

        results
    }

    /// Trigger a manual failover via sentinel.
    pub fn trigger_failover(&self) -> io::Result<()> {
        let cli = &self.config.redis_cli_bin;
        let host = &self.config.bind;

        for port in self.config.sentinel_ports() {
            if let Ok(resp) = util::redis_cli(
                cli,
                host,
                port,
                &["SENTINEL", "FAILOVER", &self.config.master_name],
            ) {
                if resp.trim() == "OK" {
                    return Ok(());
                }
            }
        }

        Err(io::Error::other("failover command failed on all sentinels"))
    }

    /// Wait until sentinels report the master as healthy (flags = "master").
    pub fn wait_for_healthy(&self, timeout: Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        loop {
            if let Ok(status) = self.poke() {
                if status.flags == "master"
                    && status.num_slaves >= self.config.num_replicas as u64
                    && status.num_sentinels >= self.config.num_sentinels as u64
                {
                    return Ok(());
                }
            }
            if start.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "sentinel topology did not become healthy in time",
                ));
            }
            thread::sleep(Duration::from_millis(500));
        }
    }

    /// Kill just the master process (for failover testing).
    pub fn kill_master(&self) {
        util::shutdown_node(
            &self.config.redis_cli_bin,
            &self.config.bind,
            self.config.master_port,
        );
    }
}

impl Drop for RedisSentinel {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

// ── Parsing helpers ─────────────────────────────────────────────────────────

/// Parse the flat key-value list from `SENTINEL MASTER <name>`.
/// Redis returns alternating key/value lines.
fn parse_sentinel_master(raw: &str, master_name: &str) -> SentinelMasterStatus {
    let lines: Vec<&str> = raw.lines().map(|l| l.trim()).collect();
    let mut map = HashMap::new();

    let mut i = 0;
    while i + 1 < lines.len() {
        map.insert(lines[i].to_string(), lines[i + 1].to_string());
        i += 2;
    }

    let get_u64 = |key: &str| -> u64 { map.get(key).and_then(|v| v.parse().ok()).unwrap_or(0) };

    SentinelMasterStatus {
        name: map
            .get("name")
            .cloned()
            .unwrap_or_else(|| master_name.to_string()),
        ip: map.get("ip").cloned().unwrap_or_else(|| "unknown".into()),
        port: map.get("port").and_then(|v| v.parse().ok()).unwrap_or(0),
        flags: map
            .get("flags")
            .cloned()
            .unwrap_or_else(|| "unknown".into()),
        num_slaves: get_u64("num-slaves"),
        num_sentinels: get_u64("num-other-sentinels") + 1,
        quorum: get_u64("quorum"),
        raw: map,
    }
}

fn probe_node(cli: &str, host: &str, port: u16, kind: NodeKind) -> NodeStatus {
    let alive = util::redis_cli(cli, host, port, &["PING"])
        .map(|r| r.trim() == "PONG")
        .unwrap_or(false);

    let role = if alive {
        util::redis_cli(cli, host, port, &["ROLE"])
            .ok()
            .and_then(|r| r.lines().next().map(|s| s.to_string()))
    } else {
        None
    };

    NodeStatus {
        port,
        kind,
        alive,
        role,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentinel_config_defaults() {
        let cfg = SentinelConfig::default();
        assert_eq!(cfg.master_port, 6380);
        let replicas: Vec<u16> = cfg.replica_ports().collect();
        assert_eq!(replicas, vec![6381, 6382]);
        let sentinels: Vec<u16> = cfg.sentinel_ports().collect();
        assert_eq!(sentinels, vec![26379, 26380, 26381]);
        assert_eq!(cfg.all_ports().len(), 6); // 1 master + 2 replicas + 3 sentinels
    }

    #[test]
    fn test_parse_sentinel_master() {
        let raw = "\
name\n\
mymaster\n\
ip\n\
127.0.0.1\n\
port\n\
6380\n\
flags\n\
master\n\
num-slaves\n\
2\n\
num-other-sentinels\n\
2\n\
quorum\n\
2\n\
";
        let status = parse_sentinel_master(raw, "mymaster");
        assert_eq!(status.name, "mymaster");
        assert_eq!(status.port, 6380);
        assert_eq!(status.flags, "master");
        assert_eq!(status.num_slaves, 2);
        assert_eq!(status.num_sentinels, 3);
        assert_eq!(status.quorum, 2);
    }
}
