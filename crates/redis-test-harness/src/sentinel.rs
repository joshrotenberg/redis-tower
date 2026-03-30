//! Redis Sentinel topology management.
//!
//! Delegates to [`crate::wrapper::sentinel`] for the actual process management.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crate::wrapper::cli::RedisCli;
use crate::wrapper::sentinel::{RedisSentinel as WrapperSentinel, RedisSentinelHandle};

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
    #[allow(dead_code)]
    handle: Option<RedisSentinelHandle>,
}

impl RedisSentinel {
    pub fn new(config: SentinelConfig) -> Self {
        Self {
            config,
            handle: None,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(SentinelConfig::default())
    }

    pub fn config(&self) -> &SentinelConfig {
        &self.config
    }

    pub fn start(&self) -> io::Result<()> {
        let handle = WrapperSentinel::builder()
            .master_name(&self.config.master_name)
            .master_port(self.config.master_port)
            .replicas(self.config.num_replicas)
            .replica_base_port(self.config.replica_base_port)
            .sentinels(self.config.num_sentinels)
            .sentinel_base_port(self.config.sentinel_base_port)
            .quorum(self.config.quorum)
            .bind(&self.config.bind)
            .down_after_ms(self.config.down_after_ms)
            .failover_timeout_ms(self.config.failover_timeout_ms)
            .redis_server_bin(&self.config.redis_server_bin)
            .redis_cli_bin(&self.config.redis_cli_bin)
            .start()
            .map_err(io::Error::other)?;

        // Leak for OnceLock static pattern.
        std::mem::forget(handle);
        Ok(())
    }

    pub fn stop(&self) -> io::Result<()> {
        for port in self.config.all_ports() {
            RedisCli::new()
                .bin(&self.config.redis_cli_bin)
                .host(&self.config.bind)
                .port(port)
                .shutdown();
        }
        Ok(())
    }

    pub fn poke(&self) -> io::Result<SentinelMasterStatus> {
        for port in self.config.sentinel_ports() {
            let cli = RedisCli::new()
                .bin(&self.config.redis_cli_bin)
                .host(&self.config.bind)
                .port(port);
            if let Ok(raw) = cli.run(&["SENTINEL", "MASTER", &self.config.master_name]) {
                return Ok(parse_sentinel_master(&raw, &self.config.master_name));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable sentinel",
        ))
    }

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
            std::thread::sleep(Duration::from_millis(500));
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
