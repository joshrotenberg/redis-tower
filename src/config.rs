//! Configuration for Redis client

use std::time::Duration;
use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};

use crate::tls::TlsConfig;

/// Configuration for Redis client connections
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// TLS configuration
    pub tls: TlsConfig,

    /// Reconnection configuration
    pub reconnect: ReconnectConfig,
}

impl ClientConfig {
    /// Create a new builder for client configuration
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            tls: TlsConfig::None,
            reconnect: ReconnectConfig::builder()
                .policy(ReconnectPolicy::exponential(
                    Duration::from_millis(100),
                    Duration::from_secs(5),
                ))
                .unlimited_attempts()
                .retry_on_reconnect(true)
                .build(),
        }
    }
}

/// Builder for client configuration
#[derive(Debug)]
pub struct ClientConfigBuilder {
    tls: TlsConfig,
    reconnect: Option<ReconnectConfig>,
}

impl ClientConfigBuilder {
    /// Create a new builder with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set TLS configuration
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::config::ClientConfig;
    /// use redis_tower::tls::TlsConfig;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let tls = TlsConfig::rustls()
    ///     .with_native_roots()
    ///     .build()?;
    ///
    /// let config = ClientConfig::builder()
    ///     .tls(tls)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Set reconnection configuration
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    /// use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};
    /// use std::time::Duration;
    ///
    /// let reconnect = ReconnectConfig::builder()
    ///     .policy(ReconnectPolicy::exponential(
    ///         Duration::from_millis(200),
    ///         Duration::from_secs(10),
    ///     ))
    ///     .max_attempts(5)
    ///     .build();
    ///
    /// let config = ClientConfig::builder()
    ///     .reconnect(reconnect)
    ///     .build();
    /// ```
    pub fn reconnect(mut self, reconnect: ReconnectConfig) -> Self {
        self.reconnect = Some(reconnect);
        self
    }

    /// Disable automatic reconnection
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .no_reconnect()
    ///     .build();
    /// ```
    pub fn no_reconnect(mut self) -> Self {
        use tower_resilience::reconnect::ReconnectPolicy;

        self.reconnect = Some(
            ReconnectConfig::builder()
                .policy(ReconnectPolicy::None)
                .build(),
        );
        self
    }

    /// Build the client configuration
    pub fn build(self) -> ClientConfig {
        ClientConfig {
            tls: self.tls,
            reconnect: self.reconnect.unwrap_or_else(|| {
                ReconnectConfig::builder()
                    .policy(ReconnectPolicy::exponential(
                        Duration::from_millis(100),
                        Duration::from_secs(5),
                    ))
                    .unlimited_attempts()
                    .retry_on_reconnect(true)
                    .build()
            }),
        }
    }
}

impl Default for ClientConfigBuilder {
    fn default() -> Self {
        Self {
            tls: TlsConfig::None,
            reconnect: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClientConfig::default();
        assert!(matches!(config.tls, TlsConfig::None));
    }

    #[test]
    fn test_builder_default() {
        let config = ClientConfig::builder().build();
        assert!(matches!(config.tls, TlsConfig::None));
    }

    #[test]
    fn test_builder_no_reconnect() {
        let config = ClientConfig::builder().no_reconnect().build();

        match config.reconnect.policy() {
            ReconnectPolicy::None => {}
            _ => panic!("Expected None policy"),
        }
    }

    #[test]
    fn test_builder_custom_reconnect() {
        let reconnect = ReconnectConfig::builder()
            .policy(ReconnectPolicy::exponential(
                Duration::from_millis(200),
                Duration::from_secs(10),
            ))
            .max_attempts(3)
            .build();

        let config = ClientConfig::builder().reconnect(reconnect).build();

        assert_eq!(config.reconnect.max_attempts(), Some(3));
    }
}
