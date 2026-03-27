//! Type-safe wrapper for `redis-server` with builder pattern.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use super::cli::RedisCli;

/// Builder and lifecycle manager for a single `redis-server` process.
///
/// # Example
///
/// ```no_run
/// use redis_test_harness::wrapper::server::RedisServer;
///
/// let server = RedisServer::new()
///     .port(6400)
///     .bind("127.0.0.1")
///     .save(false)
///     .start()
///     .unwrap();
///
/// assert!(server.is_alive());
/// // Stopped automatically on Drop.
/// ```
#[derive(Debug, Clone)]
pub struct RedisServerConfig {
    pub port: u16,
    pub bind: String,
    pub dir: PathBuf,
    pub daemonize: bool,
    pub save: bool,
    pub appendonly: bool,
    pub protected_mode: bool,
    pub loglevel: LogLevel,
    pub password: Option<String>,
    pub cluster_enabled: bool,
    pub cluster_node_timeout: Option<u64>,
    pub extra: HashMap<String, String>,
    pub redis_server_bin: String,
    pub redis_cli_bin: String,
}

/// Redis log level.
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

impl LogLevel {
    fn as_str(&self) -> &str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Verbose => "verbose",
            LogLevel::Notice => "notice",
            LogLevel::Warning => "warning",
        }
    }
}

impl Default for RedisServerConfig {
    fn default() -> Self {
        Self {
            port: 6379,
            bind: "127.0.0.1".into(),
            dir: PathBuf::from("/tmp/redis-server-wrapper"),
            daemonize: true,
            save: false,
            appendonly: false,
            protected_mode: false,
            loglevel: LogLevel::Notice,
            password: None,
            cluster_enabled: false,
            cluster_node_timeout: None,
            extra: HashMap::new(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
        }
    }
}

/// Builder for a Redis server.
pub struct RedisServer {
    config: RedisServerConfig,
}

impl RedisServer {
    pub fn new() -> Self {
        Self {
            config: RedisServerConfig::default(),
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.config.bind = bind.into();
        self
    }

    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.dir = dir.into();
        self
    }

    pub fn save(mut self, save: bool) -> Self {
        self.config.save = save;
        self
    }

    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.config.appendonly = appendonly;
        self
    }

    pub fn protected_mode(mut self, protected: bool) -> Self {
        self.config.protected_mode = protected;
        self
    }

    pub fn loglevel(mut self, level: LogLevel) -> Self {
        self.config.loglevel = level;
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = Some(password.into());
        self
    }

    pub fn cluster_enabled(mut self, enabled: bool) -> Self {
        self.config.cluster_enabled = enabled;
        self
    }

    pub fn cluster_node_timeout(mut self, ms: u64) -> Self {
        self.config.cluster_node_timeout = Some(ms);
        self
    }

    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.config.redis_server_bin = bin.into();
        self
    }

    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.config.redis_cli_bin = bin.into();
        self
    }

    /// Set an arbitrary config directive.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.extra.insert(key.into(), value.into());
        self
    }

    /// Start the server. Returns a handle that stops the server on Drop.
    pub fn start(self) -> io::Result<RedisServerHandle> {
        let node_dir = self.config.dir.join(format!("node-{}", self.config.port));
        fs::create_dir_all(&node_dir)?;

        let conf_path = node_dir.join("redis.conf");
        let conf_content = self.generate_config(&node_dir);
        fs::write(&conf_path, conf_content)?;

        let status = Command::new(&self.config.redis_server_bin)
            .arg(&conf_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !status.success() {
            return Err(io::Error::other(format!(
                "redis-server failed to start on port {}",
                self.config.port
            )));
        }

        let cli = RedisCli::new()
            .bin(&self.config.redis_cli_bin)
            .host(&self.config.bind)
            .port(self.config.port);

        cli.wait_for_ready(Duration::from_secs(10))?;

        Ok(RedisServerHandle {
            config: self.config,
            cli,
        })
    }

    fn generate_config(&self, node_dir: &std::path::Path) -> String {
        let mut conf = format!(
            "port {port}\n\
             bind {bind}\n\
             daemonize {daemonize}\n\
             pidfile {dir}/redis.pid\n\
             logfile {dir}/redis.log\n\
             dir {dir}\n\
             loglevel {loglevel}\n\
             protected-mode {protected}\n",
            port = self.config.port,
            bind = self.config.bind,
            daemonize = if self.config.daemonize { "yes" } else { "no" },
            dir = node_dir.display(),
            loglevel = self.config.loglevel.as_str(),
            protected = if self.config.protected_mode {
                "yes"
            } else {
                "no"
            },
        );

        if !self.config.save {
            conf.push_str("save \"\"\n");
        }

        if self.config.appendonly {
            conf.push_str("appendonly yes\n");
        }

        if let Some(ref pw) = self.config.password {
            conf.push_str(&format!("requirepass {pw}\n"));
        }

        if self.config.cluster_enabled {
            conf.push_str("cluster-enabled yes\n");
            conf.push_str(&format!(
                "cluster-config-file {}/nodes.conf\n",
                node_dir.display()
            ));
            if let Some(timeout) = self.config.cluster_node_timeout {
                conf.push_str(&format!("cluster-node-timeout {timeout}\n"));
            }
        }

        for (key, value) in &self.config.extra {
            conf.push_str(&format!("{key} {value}\n"));
        }

        conf
    }
}

impl Default for RedisServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to a running Redis server. Stops the server on Drop.
pub struct RedisServerHandle {
    config: RedisServerConfig,
    cli: RedisCli,
}

impl RedisServerHandle {
    /// The server's address as "host:port".
    pub fn addr(&self) -> String {
        format!("{}:{}", self.config.bind, self.config.port)
    }

    /// The server's port.
    pub fn port(&self) -> u16 {
        self.config.port
    }

    /// The server's bind address.
    pub fn host(&self) -> &str {
        &self.config.bind
    }

    /// Check if the server is alive via PING.
    pub fn is_alive(&self) -> bool {
        self.cli.ping()
    }

    /// Get a `RedisCli` configured for this server.
    pub fn cli(&self) -> &RedisCli {
        &self.cli
    }

    /// Run a redis-cli command against this server.
    pub fn run(&self, args: &[&str]) -> io::Result<String> {
        self.cli.run(args)
    }

    /// Stop the server via SHUTDOWN NOSAVE.
    pub fn stop(&self) {
        self.cli.shutdown();
    }

    /// Wait until the server is ready (PING -> PONG).
    pub fn wait_for_ready(&self, timeout: Duration) -> io::Result<()> {
        self.cli.wait_for_ready(timeout)
    }
}

impl Drop for RedisServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let s = RedisServer::new();
        assert_eq!(s.config.port, 6379);
        assert_eq!(s.config.bind, "127.0.0.1");
        assert!(!s.config.save);
    }

    #[test]
    fn builder_chain() {
        let s = RedisServer::new()
            .port(6400)
            .bind("0.0.0.0")
            .save(true)
            .appendonly(true)
            .password("secret")
            .loglevel(LogLevel::Warning)
            .extra("maxmemory", "100mb");

        assert_eq!(s.config.port, 6400);
        assert_eq!(s.config.bind, "0.0.0.0");
        assert!(s.config.save);
        assert!(s.config.appendonly);
        assert_eq!(s.config.password.as_deref(), Some("secret"));
        assert_eq!(s.config.extra.get("maxmemory").unwrap(), "100mb");
    }

    #[test]
    fn cluster_config() {
        let s = RedisServer::new()
            .port(7000)
            .cluster_enabled(true)
            .cluster_node_timeout(5000);

        assert!(s.config.cluster_enabled);
        assert_eq!(s.config.cluster_node_timeout, Some(5000));
    }
}
