//! Redis Sentinel topology management built on `RedisServer`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use super::cli::RedisCli;
use super::server::{RedisServer, RedisServerHandle};

/// Builder for a Redis Sentinel topology.
///
/// # Example
///
/// ```no_run
/// use redis_test_harness::wrapper::sentinel::RedisSentinel;
///
/// let sentinel = RedisSentinel::builder()
///     .master_name("mymaster")
///     .master_port(6390)
///     .replicas(2)
///     .sentinels(3)
///     .start()
///     .unwrap();
///
/// assert!(sentinel.is_healthy());
/// ```
pub struct RedisSentinelBuilder {
    master_name: String,
    master_port: u16,
    num_replicas: u16,
    replica_base_port: u16,
    num_sentinels: u16,
    sentinel_base_port: u16,
    quorum: u16,
    bind: String,
    down_after_ms: u64,
    failover_timeout_ms: u64,
    redis_server_bin: String,
    redis_cli_bin: String,
}

impl RedisSentinelBuilder {
    pub fn master_name(mut self, name: impl Into<String>) -> Self {
        self.master_name = name.into();
        self
    }

    pub fn master_port(mut self, port: u16) -> Self {
        self.master_port = port;
        self
    }

    pub fn replicas(mut self, n: u16) -> Self {
        self.num_replicas = n;
        self
    }

    pub fn replica_base_port(mut self, port: u16) -> Self {
        self.replica_base_port = port;
        self
    }

    pub fn sentinels(mut self, n: u16) -> Self {
        self.num_sentinels = n;
        self
    }

    pub fn sentinel_base_port(mut self, port: u16) -> Self {
        self.sentinel_base_port = port;
        self
    }

    pub fn quorum(mut self, q: u16) -> Self {
        self.quorum = q;
        self
    }

    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    pub fn down_after_ms(mut self, ms: u64) -> Self {
        self.down_after_ms = ms;
        self
    }

    pub fn failover_timeout_ms(mut self, ms: u64) -> Self {
        self.failover_timeout_ms = ms;
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

    fn replica_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.replica_base_port;
        let n = self.num_replicas;
        (0..n).map(move |i| base + i)
    }

    fn sentinel_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.sentinel_base_port;
        let n = self.num_sentinels;
        (0..n).map(move |i| base + i)
    }

    /// Start the full topology: master, replicas, sentinels.
    pub fn start(self) -> io::Result<RedisSentinelHandle> {
        // Kill leftover processes.
        let cli_for_shutdown = |port: u16| {
            RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(port)
                .shutdown();
        };
        cli_for_shutdown(self.master_port);
        for port in self.replica_ports() {
            cli_for_shutdown(port);
        }
        for port in self.sentinel_ports() {
            cli_for_shutdown(port);
        }
        std::thread::sleep(Duration::from_millis(500));

        let base_dir = PathBuf::from("/tmp/redis-sentinel-wrapper");
        if base_dir.exists() {
            let _ = fs::remove_dir_all(&base_dir);
        }

        // 1. Start master.
        let master = RedisServer::new()
            .port(self.master_port)
            .bind(&self.bind)
            .dir(base_dir.join("master"))
            .appendonly(true)
            .redis_server_bin(&self.redis_server_bin)
            .redis_cli_bin(&self.redis_cli_bin)
            .start()?;

        // 2. Start replicas.
        let mut replicas = Vec::new();
        for port in self.replica_ports() {
            let replica = RedisServer::new()
                .port(port)
                .bind(&self.bind)
                .dir(base_dir.join(format!("replica-{port}")))
                .appendonly(true)
                .extra("replicaof", format!("{} {}", self.bind, self.master_port))
                .redis_server_bin(&self.redis_server_bin)
                .redis_cli_bin(&self.redis_cli_bin)
                .start()?;
            replicas.push(replica);
        }

        // Let replication link up.
        std::thread::sleep(Duration::from_secs(1));

        // 3. Start sentinels.
        let mut sentinel_handles = Vec::new();
        for port in self.sentinel_ports() {
            let dir = base_dir.join(format!("sentinel-{port}"));
            fs::create_dir_all(&dir)?;
            let conf_path = dir.join("sentinel.conf");
            let conf = format!(
                "port {port}\n\
                 bind {bind}\n\
                 daemonize yes\n\
                 pidfile {dir}/sentinel.pid\n\
                 logfile {dir}/sentinel.log\n\
                 dir {dir}\n\
                 sentinel monitor {name} {master_host} {master_port} {quorum}\n\
                 sentinel down-after-milliseconds {name} {down_after}\n\
                 sentinel failover-timeout {name} {failover_timeout}\n\
                 sentinel parallel-syncs {name} 1\n",
                port = port,
                bind = self.bind,
                dir = dir.display(),
                name = self.master_name,
                master_host = self.bind,
                master_port = self.master_port,
                quorum = self.quorum,
                down_after = self.down_after_ms,
                failover_timeout = self.failover_timeout_ms,
            );
            fs::write(&conf_path, conf)?;

            let status = Command::new(&self.redis_server_bin)
                .arg(&conf_path)
                .arg("--sentinel")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            if !status.success() {
                return Err(io::Error::other(format!(
                    "sentinel failed to start on port {port}"
                )));
            }

            let cli = RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(port);
            cli.wait_for_ready(Duration::from_secs(10))?;
            sentinel_handles.push((port, cli));
        }

        // Wait for sentinels to discover each other.
        std::thread::sleep(Duration::from_secs(2));

        Ok(RedisSentinelHandle {
            master,
            replicas,
            sentinel_ports: sentinel_handles.iter().map(|(p, _)| *p).collect(),
            master_name: self.master_name,
            bind: self.bind,
            redis_cli_bin: self.redis_cli_bin,
            num_sentinels: self.num_sentinels,
            num_replicas: self.num_replicas,
        })
    }
}

/// A running Redis Sentinel topology. Stops everything on Drop.
pub struct RedisSentinelHandle {
    master: RedisServerHandle,
    #[allow(dead_code)] // Kept alive for Drop cleanup
    replicas: Vec<RedisServerHandle>,
    sentinel_ports: Vec<u16>,
    master_name: String,
    bind: String,
    redis_cli_bin: String,
    num_sentinels: u16,
    num_replicas: u16,
}

/// Convenience constructor.
pub struct RedisSentinel;

impl RedisSentinel {
    /// Create a new sentinel builder with defaults.
    pub fn builder() -> RedisSentinelBuilder {
        RedisSentinelBuilder {
            master_name: "mymaster".into(),
            master_port: 6390,
            num_replicas: 2,
            replica_base_port: 6391,
            num_sentinels: 3,
            sentinel_base_port: 26389,
            quorum: 2,
            bind: "127.0.0.1".into(),
            down_after_ms: 5000,
            failover_timeout_ms: 10000,
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
        }
    }
}

impl RedisSentinelHandle {
    /// The master's address.
    pub fn master_addr(&self) -> String {
        self.master.addr()
    }

    /// All sentinel addresses.
    pub fn sentinel_addrs(&self) -> Vec<String> {
        self.sentinel_ports
            .iter()
            .map(|p| format!("{}:{}", self.bind, p))
            .collect()
    }

    /// The monitored master name.
    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// Query a sentinel for the current master status.
    pub fn poke(&self) -> io::Result<HashMap<String, String>> {
        for port in &self.sentinel_ports {
            let cli = RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(*port);
            if let Ok(raw) = cli.run(&["SENTINEL", "MASTER", &self.master_name]) {
                return Ok(parse_flat_kv(&raw));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable sentinel",
        ))
    }

    /// Check if the topology is healthy.
    pub fn is_healthy(&self) -> bool {
        if let Ok(info) = self.poke() {
            let flags = info.get("flags").map(|s| s.as_str()).unwrap_or("");
            let num_slaves: u64 = info
                .get("num-slaves")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let num_sentinels: u64 = info
                .get("num-other-sentinels")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
                + 1;
            flags == "master"
                && num_slaves >= self.num_replicas as u64
                && num_sentinels >= self.num_sentinels as u64
        } else {
            false
        }
    }

    /// Wait until the topology is healthy or timeout.
    pub fn wait_for_healthy(&self, timeout: Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.is_healthy() {
                return Ok(());
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

    /// Stop everything.
    pub fn stop(&self) {
        // Sentinels first.
        for port in &self.sentinel_ports {
            RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(*port)
                .shutdown();
        }
        // Replicas and master stopped by their handles' Drop.
    }
}

impl Drop for RedisSentinelHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Parse alternating key/value lines from sentinel output.
fn parse_flat_kv(raw: &str) -> HashMap<String, String> {
    let lines: Vec<&str> = raw.lines().map(|l| l.trim()).collect();
    let mut map = HashMap::new();
    let mut i = 0;
    while i + 1 < lines.len() {
        map.insert(lines[i].to_string(), lines[i + 1].to_string());
        i += 2;
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = RedisSentinel::builder();
        assert_eq!(b.master_port, 6390);
        assert_eq!(b.num_replicas, 2);
        assert_eq!(b.num_sentinels, 3);
        assert_eq!(b.quorum, 2);
    }

    #[test]
    fn builder_chain() {
        let b = RedisSentinel::builder()
            .master_name("custom")
            .master_port(6500)
            .replicas(1)
            .sentinels(5)
            .quorum(3);
        assert_eq!(b.master_name, "custom");
        assert_eq!(b.master_port, 6500);
        assert_eq!(b.num_replicas, 1);
        assert_eq!(b.num_sentinels, 5);
        assert_eq!(b.quorum, 3);
    }

    #[test]
    fn parse_sentinel_output() {
        let raw = "name\nmymaster\nip\n127.0.0.1\nport\n6380\n";
        let map = parse_flat_kv(raw);
        assert_eq!(map.get("name").unwrap(), "mymaster");
        assert_eq!(map.get("ip").unwrap(), "127.0.0.1");
        assert_eq!(map.get("port").unwrap(), "6380");
    }
}
