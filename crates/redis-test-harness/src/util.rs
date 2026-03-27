//! Shared helpers for shelling out to redis-server / redis-cli.

use std::io;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Run `redis-cli -h <host> -p <port> <args...>` and return stdout on success.
pub fn redis_cli(bin: &str, host: &str, port: u16, args: &[&str]) -> io::Result<String> {
    let output = Command::new(bin)
        .args(["-h", host, "-p", &port.to_string()])
        .args(args)
        .output()?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(io::Error::other(format!(
            "redis-cli (port {port}) failed: {stderr}"
        )))
    }
}

/// Run `redis-cli` fire-and-forget (ignore output/errors). Used for SHUTDOWN.
pub fn redis_cli_fire_and_forget(bin: &str, host: &str, port: u16, args: &[&str]) {
    let _ = Command::new(bin)
        .args(["-h", host, "-p", &port.to_string()])
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Start a redis-server with the given config file path (daemonized via config).
pub fn start_redis_server(bin: &str, conf_path: &std::path::Path) -> io::Result<()> {
    let status = Command::new(bin)
        .arg(conf_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;

    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "redis-server failed to start with config {}",
            conf_path.display()
        )))
    }
}

/// Poll a node with PING until it responds PONG or the timeout expires.
pub fn wait_for_ping(cli_bin: &str, host: &str, port: u16, timeout: Duration) -> io::Result<()> {
    let start = Instant::now();
    loop {
        if let Ok(resp) = redis_cli(cli_bin, host, port, &["PING"]) {
            if resp.trim() == "PONG" {
                return Ok(());
            }
        }
        if start.elapsed() > timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("node on port {port} did not respond within {timeout:?}"),
            ));
        }
        thread::sleep(Duration::from_millis(250));
    }
}

/// Send SHUTDOWN NOSAVE to a node. Best-effort.
pub fn shutdown_node(cli_bin: &str, host: &str, port: u16) {
    redis_cli_fire_and_forget(cli_bin, host, port, &["SHUTDOWN", "NOSAVE"]);
}
