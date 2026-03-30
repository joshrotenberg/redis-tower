//! Standalone Redis server for integration testing.
//!
//! Uses direct `std::process::Command` for process management (sync-safe for
//! `OnceLock::get_or_init`).

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Configuration for a standalone Redis server.
#[derive(Debug, Clone)]
pub struct StandaloneConfig {
    pub port: u16,
    pub bind: String,
    pub work_dir: PathBuf,
    pub redis_server_bin: String,
    pub redis_cli_bin: String,
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
pub struct RedisStandalone {
    config: StandaloneConfig,
    started: bool,
}

impl RedisStandalone {
    pub fn new(config: StandaloneConfig) -> Self {
        Self {
            config,
            started: false,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(StandaloneConfig::default())
    }

    pub fn config(&self) -> &StandaloneConfig {
        &self.config
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.config.bind, self.config.port)
    }

    /// Start the server (sync -- suitable for OnceLock::get_or_init).
    pub fn start(&mut self) -> io::Result<()> {
        let dir = &self.config.work_dir;
        if dir.exists() {
            let _ = std::fs::remove_dir_all(dir);
        }
        std::fs::create_dir_all(dir)?;

        let conf_path = dir.join("redis.conf");
        let mut conf = format!(
            "port {port}\nbind {bind}\ndaemonize yes\n\
             pidfile {dir}/redis.pid\nlogfile {dir}/redis.log\n\
             dir {dir}\nsave \"\"\nprotected-mode no\n",
            port = self.config.port,
            bind = self.config.bind,
            dir = dir.display(),
        );
        for (k, v) in &self.config.extra_config {
            conf.push_str(&format!("{k} {v}\n"));
        }
        std::fs::write(&conf_path, conf)?;

        let status = Command::new(&self.config.redis_server_bin)
            .arg(&conf_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !status.success() {
            return Err(io::Error::other("redis-server failed to start"));
        }

        // Wait for PING.
        let start = Instant::now();
        loop {
            let output = Command::new(&self.config.redis_cli_bin)
                .args([
                    "-h",
                    &self.config.bind,
                    "-p",
                    &self.config.port.to_string(),
                    "PING",
                ])
                .output();
            if let Ok(out) = output {
                if out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "PONG" {
                    break;
                }
            }
            if start.elapsed() > Duration::from_secs(10) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "redis-server did not respond in time",
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        self.started = true;
        Ok(())
    }

    pub fn stop(&self) -> io::Result<()> {
        if self.started {
            let _ = Command::new(&self.config.redis_cli_bin)
                .args([
                    "-h",
                    &self.config.bind,
                    "-p",
                    &self.config.port.to_string(),
                    "SHUTDOWN",
                    "NOSAVE",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        Ok(())
    }

    pub fn is_alive(&self) -> bool {
        if !self.started {
            return false;
        }
        Command::new(&self.config.redis_cli_bin)
            .args([
                "-h",
                &self.config.bind,
                "-p",
                &self.config.port.to_string(),
                "PING",
            ])
            .output()
            .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "PONG")
            .unwrap_or(false)
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
