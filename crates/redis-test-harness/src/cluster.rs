//! Redis Cluster lifecycle management.
//!
//! Uses direct `std::process::Command` for process management (sync-safe for
//! `OnceLock::get_or_init`).

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// TLS configuration for a cluster.
///
/// When present, the cluster nodes listen on `tls-port` instead of the
/// plaintext `port`. Self-signed certificates can be generated
/// automatically via [`generate_tls_certs`].
#[derive(Debug, Clone)]
pub struct TlsClusterConfig {
    /// Path to the CA certificate file (PEM).
    pub ca_cert_file: PathBuf,
    /// Path to the server certificate file (PEM).
    pub cert_file: PathBuf,
    /// Path to the server private key file (PEM).
    pub key_file: PathBuf,
}

/// Generate self-signed CA and server certificates for testing.
///
/// Writes `ca.crt`, `server.crt`, and `server.key` into `dir` and
/// returns a [`TlsClusterConfig`] pointing at them.
pub fn generate_tls_certs(dir: &std::path::Path) -> io::Result<TlsClusterConfig> {
    use rcgen::{CertificateParams, Issuer, KeyPair};

    std::fs::create_dir_all(dir)?;

    // Generate CA key pair and self-signed CA cert.
    let ca_key = KeyPair::generate().map_err(|e| io::Error::other(e.to_string()))?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())
        .map_err(|e| io::Error::other(e.to_string()))?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "redis-test-ca");
    let ca_cert = ca_params
        .clone()
        .self_signed(&ca_key)
        .map_err(|e| io::Error::other(e.to_string()))?;
    let ca_issuer = Issuer::new(ca_params, &ca_key);

    // Generate server key pair and cert signed by the CA.
    let server_key = KeyPair::generate().map_err(|e| io::Error::other(e.to_string()))?;
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .map_err(|e| io::Error::other(e.to_string()))?;
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    // Add 127.0.0.1 as a SAN IP address.
    server_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let server_cert = server_params
        .signed_by(&server_key, &ca_issuer)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let ca_cert_path = dir.join("ca.crt");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");

    std::fs::write(&ca_cert_path, ca_cert.pem())?;
    std::fs::write(&cert_path, server_cert.pem())?;
    std::fs::write(&key_path, server_key.serialize_pem())?;

    Ok(TlsClusterConfig {
        ca_cert_file: ca_cert_path,
        cert_file: cert_path,
        key_file: key_path,
    })
}

/// Configuration for the cluster as a whole.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub base_port: u16,
    pub masters: u16,
    pub replicas_per_master: u16,
    pub bind: String,
    pub work_dir: PathBuf,
    pub redis_server_bin: String,
    pub redis_cli_bin: String,
    /// Extra `key value` lines appended to every node's `redis.conf`.
    ///
    /// If `requirepass` is present, it is also passed as `-a <password>` to
    /// `redis-cli --cluster create` and to the shutdown calls so the harness
    /// can still talk to the nodes.
    pub extra_config: HashMap<String, String>,
    /// Enable TLS on the cluster. When `true`, `start()` generates
    /// self-signed certificates in `work_dir/tls/` and configures every
    /// node with `tls-port` (plaintext port disabled).
    pub tls: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            base_port: 7000,
            masters: 3,
            replicas_per_master: 1,
            bind: "127.0.0.1".into(),
            work_dir: PathBuf::from("/tmp/redis-cluster"),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
            extra_config: HashMap::new(),
            tls: false,
        }
    }
}

impl ClusterConfig {
    pub fn total_nodes(&self) -> u16 {
        self.masters * (1 + self.replicas_per_master)
    }

    pub fn ports(&self) -> impl Iterator<Item = u16> {
        let base = self.base_port;
        let total = self.total_nodes();
        (0..total).map(move |i| base + i)
    }
}

/// Status returned by `poke()`.
#[derive(Debug)]
pub struct ClusterStatus {
    pub cluster_state: String,
    pub cluster_slots_assigned: u64,
    pub cluster_slots_ok: u64,
    pub cluster_known_nodes: u64,
    pub cluster_size: u64,
    pub raw: String,
}

/// Represents a running (or stopped) Redis Cluster.
pub struct RedisCluster {
    config: ClusterConfig,
    started: bool,
    /// Populated by `start()` when `config.tls` is `true`.
    tls_certs: Option<TlsClusterConfig>,
}

impl RedisCluster {
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            started: false,
            tls_certs: None,
        }
    }

    /// Returns the TLS certificate paths if TLS is enabled.
    ///
    /// Only populated after `start()` is called with `config.tls = true`.
    pub fn tls_certs(&self) -> Option<&TlsClusterConfig> {
        self.tls_certs.as_ref()
    }

    pub fn with_defaults() -> Self {
        Self::new(ClusterConfig::default())
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Start all cluster nodes and form the cluster (sync).
    pub fn start(&mut self) -> io::Result<()> {
        let dir = &self.config.work_dir;
        if dir.exists() {
            let _ = std::fs::remove_dir_all(dir);
        }
        std::fs::create_dir_all(dir)?;

        // Generate TLS certs if TLS is enabled.
        if self.config.tls {
            let tls = generate_tls_certs(&dir.join("tls"))?;
            self.tls_certs = Some(tls);
        }

        // Start each node.
        for port in self.config.ports() {
            let node_dir = dir.join(format!("node-{port}"));
            std::fs::create_dir_all(&node_dir)?;

            let conf_path = node_dir.join("redis.conf");
            let port_line = if self.config.tls {
                format!("port 0\ntls-port {port}\n")
            } else {
                format!("port {port}\n")
            };
            let mut conf = format!(
                "{port_line}bind {bind}\ndaemonize yes\n\
                 pidfile {ndir}/redis.pid\nlogfile {ndir}/redis.log\n\
                 dir {ndir}\nsave \"\"\nprotected-mode no\n\
                 cluster-enabled yes\ncluster-config-file {ndir}/nodes.conf\n\
                 cluster-node-timeout 5000\n",
                bind = self.config.bind,
                ndir = node_dir.display(),
            );
            if let Some(ref tls) = self.tls_certs {
                conf.push_str(&format!(
                    "tls-cert-file {cert}\ntls-key-file {key}\ntls-ca-cert-file {ca}\n\
                     tls-auth-clients no\ntls-replication yes\ntls-cluster yes\n",
                    cert = tls.cert_file.display(),
                    key = tls.key_file.display(),
                    ca = tls.ca_cert_file.display(),
                ));
            }
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
                return Err(io::Error::other(format!(
                    "redis-server failed to start on port {port}"
                )));
            }
        }

        // Wait for PING on every node.
        for port in self.config.ports() {
            self.wait_for_ping(port, Duration::from_secs(10))?;
        }

        // Form the cluster with redis-cli --cluster create.
        let mut endpoints: Vec<String> = self
            .config
            .ports()
            .map(|p| format!("{}:{}", self.config.bind, p))
            .collect();

        let mut args = vec!["--cluster".to_string(), "create".to_string()];
        args.append(&mut endpoints);
        args.push("--cluster-replicas".into());
        args.push(self.config.replicas_per_master.to_string());
        args.push("--cluster-yes".into());
        if let Some(pw) = self.config.extra_config.get("requirepass") {
            args.push("-a".into());
            args.push(pw.clone());
        }
        if let Some(ref tls) = self.tls_certs {
            append_tls_cli_args(&mut args, tls);
        }

        let output = Command::new(&self.config.redis_cli_bin)
            .args(&args)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(io::Error::other(format!(
                "redis-cli --cluster create failed: {stderr}{stdout}"
            )));
        }

        self.started = true;
        Ok(())
    }

    pub fn stop(&self) -> io::Result<()> {
        // Always try to stop nodes -- even if `started` is false there may
        // be leftovers from a previous run.
        let pw = self.config.extra_config.get("requirepass").cloned();
        for port in self.config.ports() {
            // Try graceful shutdown via redis-cli first.
            let mut args: Vec<String> = vec![
                "-h".into(),
                self.config.bind.clone(),
                "-p".into(),
                port.to_string(),
            ];
            if let Some(ref p) = pw {
                args.push("-a".into());
                args.push(p.clone());
            }
            if let Some(ref tls) = self.tls_certs {
                append_tls_cli_args(&mut args, tls);
            }
            args.push("SHUTDOWN".into());
            args.push("NOSAVE".into());
            let cli_ok = Command::new(&self.config.redis_cli_bin)
                .args(&args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());

            // If redis-cli couldn't connect (e.g. TLS nodes with no certs
            // available yet), fall back to killing via pid file.
            if !cli_ok {
                let pid_path = self
                    .config
                    .work_dir
                    .join(format!("node-{port}"))
                    .join("redis.pid");
                if let Ok(pid_str) = std::fs::read_to_string(&pid_path) {
                    let pid = pid_str.trim();
                    let _ = Command::new("kill")
                        .arg(pid)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }
        Ok(())
    }

    pub fn poke(&self) -> io::Result<ClusterStatus> {
        let pw = self.config.extra_config.get("requirepass");
        for port in self.config.ports() {
            let mut args: Vec<String> = vec![
                "-h".into(),
                self.config.bind.clone(),
                "-p".into(),
                port.to_string(),
            ];
            if let Some(p) = pw {
                args.push("-a".into());
                args.push(p.clone());
                args.push("--no-auth-warning".into());
            }
            if let Some(ref tls) = self.tls_certs {
                append_tls_cli_args(&mut args, tls);
            }
            args.push("CLUSTER".into());
            args.push("INFO".into());
            let output = Command::new(&self.config.redis_cli_bin)
                .args(&args)
                .output();
            if let Ok(out) = output
                && out.status.success()
            {
                let raw = String::from_utf8_lossy(&out.stdout).to_string();
                return Ok(parse_cluster_info(&raw));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no reachable cluster nodes",
        ))
    }

    pub fn wait_for_healthy(&self, timeout: Duration) -> io::Result<()> {
        let start = Instant::now();
        loop {
            if let Ok(status) = self.poke()
                && status.cluster_state == "ok"
                && status.cluster_slots_ok == 16384
            {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cluster did not become healthy in time",
                ));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn wait_for_ping(&self, port: u16, timeout: Duration) -> io::Result<()> {
        let pw = self.config.extra_config.get("requirepass");
        let start = Instant::now();
        loop {
            let mut args: Vec<String> = vec![
                "-h".into(),
                self.config.bind.clone(),
                "-p".into(),
                port.to_string(),
            ];
            if let Some(p) = pw {
                args.push("-a".into());
                args.push(p.clone());
                args.push("--no-auth-warning".into());
            }
            if let Some(ref tls) = self.tls_certs {
                append_tls_cli_args(&mut args, tls);
            }
            args.push("PING".into());
            let output = Command::new(&self.config.redis_cli_bin)
                .args(&args)
                .output();
            if let Ok(out) = output
                && out.status.success()
                && String::from_utf8_lossy(&out.stdout).trim() == "PONG"
            {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("redis-server on port {port} did not respond in time"),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for RedisCluster {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Append TLS flags to a redis-cli argument list.
///
/// Uses `--insecure` because the harness generates self-signed certificates
/// and `--cacert` may not work reliably with all redis-cli builds (depends
/// on the linked OpenSSL/LibreSSL version). Since `tls-auth-clients no` is
/// set on the server side, no client cert is needed.
fn append_tls_cli_args(args: &mut Vec<String>, _tls: &TlsClusterConfig) {
    args.push("--tls".into());
    args.push("--insecure".into());
}

fn parse_cluster_info(raw: &str) -> ClusterStatus {
    let mut map: HashMap<String, String> = HashMap::new();
    for line in raw.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once(':') {
            map.insert(k.to_string(), v.trim().to_string());
        }
    }

    let get_u64 = |key: &str| -> u64 { map.get(key).and_then(|v| v.parse().ok()).unwrap_or(0) };

    ClusterStatus {
        cluster_state: map
            .get("cluster_state")
            .cloned()
            .unwrap_or_else(|| "unknown".into()),
        cluster_slots_assigned: get_u64("cluster_slots_assigned"),
        cluster_slots_ok: get_u64("cluster_slots_ok"),
        cluster_known_nodes: get_u64("cluster_known_nodes"),
        cluster_size: get_u64("cluster_size"),
        raw: raw.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_defaults() {
        let cfg = ClusterConfig::default();
        assert_eq!(cfg.total_nodes(), 6);
        let ports: Vec<u16> = cfg.ports().collect();
        assert_eq!(ports, vec![7000, 7001, 7002, 7003, 7004, 7005]);
    }

    #[test]
    fn test_cluster_config_no_replicas() {
        let cfg = ClusterConfig {
            masters: 3,
            replicas_per_master: 0,
            ..Default::default()
        };
        assert_eq!(cfg.total_nodes(), 3);
    }

    #[test]
    fn test_parse_cluster_info() {
        let raw = "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:6\r\ncluster_size:3\r\n";
        let status = parse_cluster_info(raw);
        assert_eq!(status.cluster_state, "ok");
        assert_eq!(status.cluster_slots_assigned, 16384);
        assert_eq!(status.cluster_slots_ok, 16384);
        assert_eq!(status.cluster_known_nodes, 6);
        assert_eq!(status.cluster_size, 3);
    }
}
