//! Standalone Redis server lifecycle management.
//!
//! Starts a single `redis-server` process for integration testing.
//! Cleanup on `Drop`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use crate::util;

/// Configuration for a standalone Redis server.
#[derive(Debug, Clone)]
pub struct StandaloneConfig {
    /// Port to listen on.
    pub port: u16,
    /// Bind address.
    pub bind: String,
    /// Working directory.
    pub work_dir: PathBuf,
    /// Path to `redis-server` binary.
    pub redis_server_bin: String,
    /// Path to `redis-cli` binary.
    pub redis_cli_bin: String,
    /// Additional redis.conf directives.
    pub extra_config: HashMap<String, String>,
}

impl Default for StandaloneConfig {
    fn default() -> Self {
        Self {
            port: 6399,
            bind: "127.0.0.1".into(),
            work_dir: PathBuf::from("/tmp/redis-standalone"),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
            extra_config: HashMap::new(),
        }
    }
}

/// A running standalone Redis server.
#[derive(Debug)]
pub struct RedisStandalone {
    config: StandaloneConfig,
}

impl RedisStandalone {
    pub fn new(config: StandaloneConfig) -> Self {
        Self { config }
    }

    /// Start with defaults (port 6399).
    pub fn with_defaults() -> Self {
        Self::new(StandaloneConfig::default())
    }

    pub fn config(&self) -> &StandaloneConfig {
        &self.config
    }

    /// The address string "host:port".
    pub fn addr(&self) -> String {
        format!("{}:{}", self.config.bind, self.config.port)
    }

    /// Start the server.
    pub fn start(&self) -> io::Result<()> {
        if self.config.work_dir.exists() {
            fs::remove_dir_all(&self.config.work_dir)?;
        }
        fs::create_dir_all(&self.config.work_dir)?;

        let conf_path = self.write_config()?;
        util::start_redis_server(&self.config.redis_server_bin, &conf_path)?;
        util::wait_for_ping(
            &self.config.redis_cli_bin,
            &self.config.bind,
            self.config.port,
            Duration::from_secs(10),
        )?;

        Ok(())
    }

    /// Stop the server via SHUTDOWN NOSAVE.
    pub fn stop(&self) -> io::Result<()> {
        util::shutdown_node(
            &self.config.redis_cli_bin,
            &self.config.bind,
            self.config.port,
        );
        Ok(())
    }

    /// Check if the server is alive.
    pub fn is_alive(&self) -> bool {
        util::redis_cli(
            &self.config.redis_cli_bin,
            &self.config.bind,
            self.config.port,
            &["PING"],
        )
        .map(|r| r.trim() == "PONG")
        .unwrap_or(false)
    }

    fn write_config(&self) -> io::Result<PathBuf> {
        let dir = &self.config.work_dir;
        let conf_path = dir.join("redis.conf");

        let mut conf = format!(
            r#"port {port}
bind {bind}
daemonize yes
pidfile {dir}/redis.pid
logfile {dir}/redis.log
dir {dir}
dbfilename dump.rdb
save ""
protected-mode no
"#,
            port = self.config.port,
            bind = self.config.bind,
            dir = dir.display(),
        );

        for (key, value) in &self.config.extra_config {
            conf.push_str(&format!("{key} {value}\n"));
        }

        fs::write(&conf_path, conf)?;
        Ok(conf_path)
    }
}

impl Drop for RedisStandalone {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standalone_config_defaults() {
        let cfg = StandaloneConfig::default();
        assert_eq!(cfg.port, 6399);
        assert_eq!(cfg.bind, "127.0.0.1");
    }

    #[test]
    fn test_standalone_addr() {
        let s = RedisStandalone::with_defaults();
        assert_eq!(s.addr(), "127.0.0.1:6399");
    }
}
