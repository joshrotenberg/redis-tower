//! Sentinel configuration types

use crate::tls::TlsConfig;
use std::time::Duration;

/// Configuration for connecting to Redis via Sentinel
#[derive(Clone)]
pub struct SentinelConfig {
    /// List of Sentinel nodes (host, port)
    pub(crate) sentinels: Vec<(String, u16)>,

    /// Name of the master service to monitor
    pub(crate) master_name: String,

    /// Password for authenticating with Sentinel nodes (optional)
    pub(crate) sentinel_password: Option<String>,

    /// Password for authenticating with Redis nodes (optional)
    pub(crate) redis_password: Option<String>,

    /// Username for Redis ACL authentication (optional)
    pub(crate) redis_username: Option<String>,

    /// Timeout for Sentinel queries
    pub(crate) sentinel_timeout: Duration,

    /// Connection timeout for Redis nodes
    pub(crate) connection_timeout: Duration,

    /// Whether to enable read-from-replica support
    pub(crate) read_from_replicas: bool,

    /// TLS configuration for Redis connections
    pub(crate) tls: TlsConfig,
}

impl std::fmt::Debug for SentinelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentinelConfig")
            .field("sentinels", &self.sentinels)
            .field("master_name", &self.master_name)
            .field(
                "sentinel_password",
                &self.sentinel_password.as_ref().map(|_| "***"),
            )
            .field(
                "redis_password",
                &self.redis_password.as_ref().map(|_| "***"),
            )
            .field("redis_username", &self.redis_username)
            .field("sentinel_timeout", &self.sentinel_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .field("read_from_replicas", &self.read_from_replicas)
            .field("tls", &self.tls)
            .finish()
    }
}

impl SentinelConfig {
    /// Create a new builder for SentinelConfig
    pub fn builder() -> SentinelConfigBuilder {
        SentinelConfigBuilder::default()
    }
}

/// Builder for SentinelConfig
#[derive(Debug, Default)]
pub struct SentinelConfigBuilder {
    sentinels: Vec<(String, u16)>,
    master_name: Option<String>,
    sentinel_password: Option<String>,
    redis_password: Option<String>,
    redis_username: Option<String>,
    sentinel_timeout: Option<Duration>,
    connection_timeout: Option<Duration>,
    read_from_replicas: bool,
    tls: Option<TlsConfig>,
}

impl SentinelConfigBuilder {
    /// Add a Sentinel node to the configuration
    pub fn sentinel_node(mut self, host: impl Into<String>, port: u16) -> Self {
        self.sentinels.push((host.into(), port));
        self
    }

    /// Add multiple Sentinel nodes from an iterator
    pub fn sentinel_nodes<I, H>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = (H, u16)>,
        H: Into<String>,
    {
        self.sentinels
            .extend(nodes.into_iter().map(|(h, p)| (h.into(), p)));
        self
    }

    /// Set the master name to monitor
    pub fn master_name(mut self, name: impl Into<String>) -> Self {
        self.master_name = Some(name.into());
        self
    }

    /// Set password for Sentinel authentication
    pub fn sentinel_password(mut self, password: impl Into<String>) -> Self {
        self.sentinel_password = Some(password.into());
        self
    }

    /// Set password for Redis node authentication
    pub fn redis_password(mut self, password: impl Into<String>) -> Self {
        self.redis_password = Some(password.into());
        self
    }

    /// Set username for Redis ACL authentication
    pub fn redis_username(mut self, username: impl Into<String>) -> Self {
        self.redis_username = Some(username.into());
        self
    }

    /// Set timeout for Sentinel queries (default: 5 seconds)
    pub fn sentinel_timeout(mut self, timeout: Duration) -> Self {
        self.sentinel_timeout = Some(timeout);
        self
    }

    /// Set connection timeout for Redis nodes (default: 5 seconds)
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Enable read-from-replica support
    pub fn read_from_replicas(mut self, enabled: bool) -> Self {
        self.read_from_replicas = enabled;
        self
    }

    /// Set TLS configuration
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Build the SentinelConfig
    pub fn build(self) -> Result<SentinelConfig, SentinelConfigError> {
        if self.sentinels.is_empty() {
            return Err(SentinelConfigError::NoSentinels);
        }

        let master_name = self.master_name.ok_or(SentinelConfigError::NoMasterName)?;

        Ok(SentinelConfig {
            sentinels: self.sentinels,
            master_name,
            sentinel_password: self.sentinel_password,
            redis_password: self.redis_password,
            redis_username: self.redis_username,
            sentinel_timeout: self.sentinel_timeout.unwrap_or(Duration::from_secs(5)),
            connection_timeout: self.connection_timeout.unwrap_or(Duration::from_secs(5)),
            read_from_replicas: self.read_from_replicas,
            tls: self.tls.unwrap_or(TlsConfig::None),
        })
    }
}

/// Errors that can occur during Sentinel configuration
#[derive(Debug, thiserror::Error)]
pub enum SentinelConfigError {
    /// No Sentinel nodes provided
    #[error("At least one Sentinel node must be configured")]
    NoSentinels,

    /// No master name provided
    #[error("Master name must be specified")]
    NoMasterName,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .build()
            .unwrap();

        assert_eq!(config.sentinels.len(), 1);
        assert_eq!(config.master_name, "mymaster");
        assert_eq!(config.sentinel_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_builder_multiple_sentinels() {
        let config = SentinelConfig::builder()
            .sentinel_nodes([
                ("sentinel1", 26379),
                ("sentinel2", 26379),
                ("sentinel3", 26379),
            ])
            .master_name("mymaster")
            .build()
            .unwrap();

        assert_eq!(config.sentinels.len(), 3);
    }

    #[test]
    fn test_builder_with_auth() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .sentinel_password("sentinel-pass")
            .redis_password("redis-pass")
            .redis_username("default")
            .build()
            .unwrap();

        assert_eq!(config.sentinel_password, Some("sentinel-pass".to_string()));
        assert_eq!(config.redis_password, Some("redis-pass".to_string()));
        assert_eq!(config.redis_username, Some("default".to_string()));
    }

    #[test]
    fn test_builder_no_sentinels_error() {
        let result = SentinelConfig::builder().master_name("mymaster").build();

        assert!(matches!(result, Err(SentinelConfigError::NoSentinels)));
    }

    #[test]
    fn test_builder_no_master_name_error() {
        let result = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .build();

        assert!(matches!(result, Err(SentinelConfigError::NoMasterName)));
    }

    #[test]
    fn test_builder_custom_timeouts() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .sentinel_timeout(Duration::from_secs(10))
            .connection_timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        assert_eq!(config.sentinel_timeout, Duration::from_secs(10));
        assert_eq!(config.connection_timeout, Duration::from_secs(3));
    }
}
