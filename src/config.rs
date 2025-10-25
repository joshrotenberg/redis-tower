//! Configuration for Redis client

use std::time::Duration;
use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};

use crate::metrics::{MetricsCollector, MetricsConfig};
use crate::tls::TlsConfig;
use crate::tracing::TracingConfig;

/// Configuration for Redis client connections
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// TLS configuration
    pub tls: TlsConfig,

    /// Reconnection configuration
    pub reconnect: ReconnectConfig,

    /// Tracing configuration
    pub tracing: TracingConfig,

    /// Metrics collector
    pub metrics: MetricsCollector,
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
            tracing: TracingConfig::default(),
            metrics: MetricsCollector::new(),
        }
    }
}

/// Builder for client configuration
#[derive(Debug)]
pub struct ClientConfigBuilder {
    tls: TlsConfig,
    reconnect: Option<ReconnectConfig>,
    tracing: Option<TracingConfig>,
    metrics: Option<MetricsCollector>,
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

    /// Set tracing configuration
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    /// use redis_tower::tracing::TracingConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .tracing(TracingConfig::all())
    ///     .build();
    /// ```
    pub fn tracing(mut self, tracing: TracingConfig) -> Self {
        self.tracing = Some(tracing);
        self
    }

    /// Disable all tracing
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .no_tracing()
    ///     .build();
    /// ```
    pub fn no_tracing(mut self) -> Self {
        self.tracing = Some(TracingConfig::none());
        self
    }

    /// Set metrics collector
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    /// use redis_tower::metrics::{MetricsCollector, MetricsConfig};
    ///
    /// let metrics = MetricsCollector::with_config(MetricsConfig::all());
    ///
    /// let config = ClientConfig::builder()
    ///     .metrics(metrics)
    ///     .build();
    /// ```
    pub fn metrics(mut self, metrics: MetricsCollector) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Disable all metrics collection
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .no_metrics()
    ///     .build();
    /// ```
    pub fn no_metrics(mut self) -> Self {
        self.metrics = Some(MetricsCollector::with_config(MetricsConfig::none()));
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
            tracing: self.tracing.unwrap_or_default(),
            metrics: self.metrics.unwrap_or_default(),
        }
    }
}

impl Default for ClientConfigBuilder {
    fn default() -> Self {
        Self {
            tls: TlsConfig::None,
            reconnect: None,
            tracing: None,
            metrics: None,
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
