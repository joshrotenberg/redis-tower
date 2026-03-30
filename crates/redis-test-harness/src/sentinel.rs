//! Redis Sentinel topology management.
//!
//! Uses direct `std::process::Command` for process management (sync-safe for
//! `OnceLock::get_or_init`).

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Configuration for the sentinel topology.
#[derive(Debug, Clone)]
pub struct SentinelConfig {
    pub master_name: String,
    pub master_port: u16,
    pub num_replicas: u16,
    pub replica_base_port: u16,
    pub num_sentinels: u16,
    pub sentinel_base_port: u16,
    pub quorum: u16,
    pub bind: String,
    pub work_dir: PathBuf,
    pub redis_server_bin: String,
    pub redis_cli_bin: String,
    pub down_after_ms: u64,
    pub failover_timeout_ms: u64,
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
        }
    }
}

impl SentinelConfig {
    pub fn replica_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.replica_base_port;
        let n = self.num_replicas;
        (0..n).map(move |i| base + i)
    }

    pub fn sentinel_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.sentinel_base_port;
        let n = self.num_sentinels;
        (0..n).map(move |i| base + i)
    }

    pub fn all_ports(&self) -> Vec<u16> {
        let mut ports = vec![self.master_port];
        ports.extend(self.replica_ports());
        ports.extend(self.sentinel_ports());
        ports
    }
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

/// Manages the full sentinel topology lifecycle.
pub struct RedisSentinel {
    config: SentinelConfig,
    started: bool,
}

impl RedisSentinel {
    pub fn new(config: SentinelConfig) -> Self {
        Self {
            config,
            started: false,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(SentinelConfig::default())
    }

    pub fn config(&self) -> &SentinelConfig {
        &self.config
    }

    /// Start the full sentinel topology (sync).
    pub fn start(&mut self) -> io::Result<()> {
        let dir = &self.config.work_dir;
        if dir.exists() {
            let _ = std::fs::remove_dir_all(dir);
        }
        std::fs::create_dir_all(dir)?;

        // 1. Start the master.
        self.start_redis_node(self.config.master_port, None)?;

        // 2. Start replicas with replicaof.
        for port in self.config.replica_ports() {
            self.start_redis_node(
                port,
                Some((self.config.bind.clone(), self.config.master_port)),
            )?;
        }

        // 3. Wait for PING on master and all replicas.
        self.wait_for_ping(self.config.master_port, Duration::from_secs(10))?;
        for port in self.config.replica_ports() {
            self.wait_for_ping(port, Duration::from_secs(10))?;
        }

        // 4. Start sentinels.
        for port in self.config.sentinel_ports() {
            self.start_sentinel_node(port)?;
        }

        // 5. Wait for PING on all sentinels.
        for port in self.config.sentinel_ports() {
            self.wait_for_ping(port, Duration::from_secs(10))?;
        }

        self.started = true;
        Ok(())
    }

    pub fn stop(&self) -> io::Result<()> {
        if self.started {
            for port in self.config.all_ports() {
                let _ = Command::new(&self.config.redis_cli_bin)
                    .args([
                        "-h",
                        &self.config.bind,
                        "-p",
                        &port.to_string(),
                        "SHUTDOWN",
                        "NOSAVE",
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
        Ok(())
    }

    pub fn poke(&self) -> io::Result<SentinelMasterStatus> {
        for port in self.config.sentinel_ports() {
            let output = Command::new(&self.config.redis_cli_bin)
                .args([
                    "-h",
                    &self.config.bind,
                    "-p",
                    &port.to_string(),
                    "SENTINEL",
                    "MASTER",
                    &self.config.master_name,
                ])
                .output();
            if let Ok(out) = output {
                if out.status.success() {
                    let raw = String::from_utf8_lossy(&out.stdout).to_string();
                    return Ok(parse_sentinel_master(&raw, &self.config.master_name));
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable sentinel",
        ))
    }

    pub fn wait_for_healthy(&self, timeout: Duration) -> io::Result<()> {
        let start = Instant::now();
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
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Start a regular redis-server node (master or replica).
    fn start_redis_node(&self, port: u16, replicaof: Option<(String, u16)>) -> io::Result<()> {
        let node_dir = self.config.work_dir.join(format!("redis-{port}"));
        std::fs::create_dir_all(&node_dir)?;

        let conf_path = node_dir.join("redis.conf");
        let mut conf = format!(
            "port {port}\nbind {bind}\ndaemonize yes\n\
             pidfile {ndir}/redis.pid\nlogfile {ndir}/redis.log\n\
             dir {ndir}\nsave \"\"\nprotected-mode no\n",
            bind = self.config.bind,
            ndir = node_dir.display(),
        );
        if let Some((host, master_port)) = replicaof {
            conf.push_str(&format!("replicaof {host} {master_port}\n"));
        }
        std::fs::write(&conf_path, &conf)?;

        let status = Command::new(&self.config.redis_server_bin)
            .arg(&conf_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !status.success() {
            return Err(io::Error::other(format!(
                "redis-server failed to start on port {port}"
            )));
        }
        Ok(())
    }

    /// Write sentinel.conf and start a sentinel process.
    fn start_sentinel_node(&self, port: u16) -> io::Result<()> {
        let node_dir = self.config.work_dir.join(format!("sentinel-{port}"));
        std::fs::create_dir_all(&node_dir)?;

        let conf_path = node_dir.join("sentinel.conf");
        let conf = format!(
            "port {port}\nbind {bind}\ndaemonize yes\n\
             pidfile {ndir}/sentinel.pid\nlogfile {ndir}/sentinel.log\n\
             dir {ndir}\nprotected-mode no\n\
             sentinel monitor {name} {bind} {master_port} {quorum}\n\
             sentinel down-after-milliseconds {name} {down_after}\n\
             sentinel failover-timeout {name} {failover_timeout}\n\
             sentinel parallel-syncs {name} 1\n",
            bind = self.config.bind,
            ndir = node_dir.display(),
            name = self.config.master_name,
            master_port = self.config.master_port,
            quorum = self.config.quorum,
            down_after = self.config.down_after_ms,
            failover_timeout = self.config.failover_timeout_ms,
        );
        std::fs::write(&conf_path, &conf)?;

        let status = Command::new(&self.config.redis_server_bin)
            .args([conf_path.as_os_str(), std::ffi::OsStr::new("--sentinel")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !status.success() {
            return Err(io::Error::other(format!(
                "sentinel failed to start on port {port}"
            )));
        }
        Ok(())
    }

    fn wait_for_ping(&self, port: u16, timeout: Duration) -> io::Result<()> {
        let start = Instant::now();
        loop {
            let output = Command::new(&self.config.redis_cli_bin)
                .args(["-h", &self.config.bind, "-p", &port.to_string(), "PING"])
                .output();
            if let Ok(out) = output {
                if out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "PONG" {
                    return Ok(());
                }
            }
            if start.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("node on port {port} did not respond in time"),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for RedisSentinel {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

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
        assert_eq!(cfg.all_ports().len(), 6);
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
