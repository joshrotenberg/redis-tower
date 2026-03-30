//! Standalone Redis server for integration testing.
//!
//! Thin wrapper around [`crate::wrapper::server::RedisServer`].

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use crate::wrapper::server::{RedisServer, RedisServerHandle};

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
    handle: Option<RedisServerHandle>,
}

impl RedisStandalone {
    pub fn new(config: StandaloneConfig) -> Self {
        Self {
            config,
            handle: None,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(StandaloneConfig::default())
    }

    pub fn config(&self) -> &StandaloneConfig {
        &self.config
    }

    pub fn addr(&self) -> String {
        if let Some(ref h) = self.handle {
            h.addr()
        } else {
            format!("{}:{}", self.config.bind, self.config.port)
        }
    }

    pub fn start(&mut self) -> io::Result<()> {
        let mut builder = RedisServer::new()
            .port(self.config.port)
            .bind(&self.config.bind)
            .dir(&self.config.work_dir)
            .redis_server_bin(&self.config.redis_server_bin)
            .redis_cli_bin(&self.config.redis_cli_bin);

        for (k, v) in &self.config.extra_config {
            builder = builder.extra(k, v);
        }

        self.handle = Some(builder.start().map_err(io::Error::other)?);
        Ok(())
    }

    pub fn stop(&self) -> io::Result<()> {
        if let Some(ref h) = self.handle {
            h.stop();
        }
        Ok(())
    }

    pub fn is_alive(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| h.is_alive())
    }
}

impl Drop for RedisStandalone {
    fn drop(&mut self) {
        // RedisServerHandle::drop handles cleanup.
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
