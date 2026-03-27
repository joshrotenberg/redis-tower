//! Type-safe wrapper for the `redis-cli` command.

use std::io;
use std::process::{Command, Output, Stdio};

/// Builder for executing `redis-cli` commands.
#[derive(Debug, Clone)]
pub struct RedisCli {
    bin: String,
    host: String,
    port: u16,
    password: Option<String>,
}

impl RedisCli {
    /// Create a new `redis-cli` builder with defaults (localhost:6379).
    pub fn new() -> Self {
        Self {
            bin: "redis-cli".into(),
            host: "127.0.0.1".into(),
            port: 6379,
            password: None,
        }
    }

    /// Set the `redis-cli` binary path.
    pub fn bin(mut self, bin: impl Into<String>) -> Self {
        self.bin = bin.into();
        self
    }

    /// Set the host to connect to.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the port to connect to.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the password for AUTH.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Run a command and return stdout on success.
    pub fn run(&self, args: &[&str]) -> io::Result<String> {
        let output = self.raw_output(args)?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(io::Error::other(format!(
                "redis-cli {}:{} failed: {stderr}",
                self.host, self.port
            )))
        }
    }

    /// Run a command, ignoring output. Used for fire-and-forget (SHUTDOWN).
    pub fn fire_and_forget(&self, args: &[&str]) {
        let _ = Command::new(&self.bin)
            .args(self.base_args())
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    /// Send PING and return true if PONG is received.
    pub fn ping(&self) -> bool {
        self.run(&["PING"])
            .map(|r| r.trim() == "PONG")
            .unwrap_or(false)
    }

    /// Send SHUTDOWN NOSAVE. Best-effort.
    pub fn shutdown(&self) {
        self.fire_and_forget(&["SHUTDOWN", "NOSAVE"]);
    }

    /// Wait until the server responds to PING or timeout expires.
    pub fn wait_for_ready(&self, timeout: std::time::Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.ping() {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{}:{} did not respond within {timeout:?}",
                        self.host, self.port
                    ),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }

    /// Run `redis-cli --cluster create ...` to form a cluster.
    pub fn cluster_create(
        &self,
        node_addrs: &[String],
        replicas_per_master: u16,
    ) -> io::Result<()> {
        let mut args: Vec<String> = vec!["--cluster".into(), "create".into()];
        args.extend(node_addrs.iter().cloned());
        if replicas_per_master > 0 {
            args.push("--cluster-replicas".into());
            args.push(replicas_per_master.to_string());
        }
        args.push("--cluster-yes".into());

        let str_args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let output = Command::new(&self.bin).args(&str_args).output()?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            Err(io::Error::other(format!(
                "cluster create failed:\nstdout: {stdout}\nstderr: {stderr}"
            )))
        }
    }

    fn base_args(&self) -> Vec<String> {
        let mut args = vec![
            "-h".to_string(),
            self.host.clone(),
            "-p".to_string(),
            self.port.to_string(),
        ];
        if let Some(ref pw) = self.password {
            args.push("-a".to_string());
            args.push(pw.clone());
        }
        args
    }

    fn raw_output(&self, args: &[&str]) -> io::Result<Output> {
        Command::new(&self.bin)
            .args(self.base_args())
            .args(args)
            .output()
    }
}

impl Default for RedisCli {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cli = RedisCli::new();
        assert_eq!(cli.host, "127.0.0.1");
        assert_eq!(cli.port, 6379);
    }

    #[test]
    fn builder_chain() {
        let cli = RedisCli::new()
            .host("10.0.0.1")
            .port(6380)
            .password("secret")
            .bin("/usr/local/bin/redis-cli");
        assert_eq!(cli.host, "10.0.0.1");
        assert_eq!(cli.port, 6380);
        assert_eq!(cli.password.as_deref(), Some("secret"));
        assert_eq!(cli.bin, "/usr/local/bin/redis-cli");
    }
}
