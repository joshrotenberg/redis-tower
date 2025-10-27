//! Configuration for Redis client

use std::time::Duration;
use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};

use crate::health::HealthCheckConfig;
use crate::hooks::Hooks;
use crate::metrics::{MetricsCollector, MetricsConfig};
use crate::tcp::TcpConfig;
use crate::tls::TlsConfig;
use crate::tracing::TracingConfig;

/// Configuration for Redis client connections
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// TLS configuration
    pub tls: TlsConfig,

    /// TCP socket configuration
    pub tcp: TcpConfig,

    /// Reconnection configuration
    pub reconnect: ReconnectConfig,

    /// Health check configuration
    pub health_check: HealthCheckConfig,

    /// Tracing configuration
    pub tracing: TracingConfig,

    /// Metrics collector
    pub metrics: MetricsCollector,

    /// Error and reconnection hooks
    pub hooks: Hooks,
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
            tcp: TcpConfig::default(),
            reconnect: ReconnectConfig::builder()
                .policy(ReconnectPolicy::exponential(
                    Duration::from_millis(100),
                    Duration::from_secs(5),
                ))
                .unlimited_attempts()
                .retry_on_reconnect(true)
                .build(),
            health_check: HealthCheckConfig::default(),
            tracing: TracingConfig::default(),
            metrics: MetricsCollector::new(),
            hooks: Hooks::new(),
        }
    }
}

/// Builder for client configuration
#[derive(Debug)]
pub struct ClientConfigBuilder {
    tls: TlsConfig,
    tcp: Option<TcpConfig>,
    reconnect: Option<ReconnectConfig>,
    health_check: Option<HealthCheckConfig>,
    tracing: Option<TracingConfig>,
    metrics: Option<MetricsCollector>,
    hooks: Option<Hooks>,
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

    /// Set TCP socket configuration
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::config::ClientConfig;
    /// use redis_tower::tcp::TcpConfig;
    ///
    /// let tcp = TcpConfig::new()
    ///     .with_nodelay(true)
    ///     .with_ttl(64);
    ///
    /// let config = ClientConfig::builder()
    ///     .tcp(tcp)
    ///     .build();
    /// ```
    pub fn tcp(mut self, tcp: TcpConfig) -> Self {
        self.tcp = Some(tcp);
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

    /// Set health check configuration
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    /// use redis_tower::health::HealthCheckConfig;
    /// use std::time::Duration;
    ///
    /// let health_check = HealthCheckConfig::builder()
    ///     .interval(Duration::from_secs(60))
    ///     .timeout(Duration::from_secs(10))
    ///     .failure_threshold(5)
    ///     .build();
    ///
    /// let config = ClientConfig::builder()
    ///     .health_check(health_check)
    ///     .build();
    /// ```
    pub fn health_check(mut self, health_check: HealthCheckConfig) -> Self {
        self.health_check = Some(health_check);
        self
    }

    /// Disable health checks
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .no_health_check()
    ///     .build();
    /// ```
    pub fn no_health_check(mut self) -> Self {
        self.health_check = Some(HealthCheckConfig::disabled());
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

    /// Set error and reconnection hooks
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    /// use redis_tower::hooks::Hooks;
    ///
    /// let hooks = Hooks::new()
    ///     .with_error_callback(|error| async move {
    ///         eprintln!("Redis error: {:?}", error);
    ///     })
    ///     .with_connect_callback(|attempt| async move {
    ///         println!("Connected on attempt {}", attempt);
    ///     });
    ///
    /// let config = ClientConfig::builder()
    ///     .hooks(hooks)
    ///     .build();
    /// ```
    pub fn hooks(mut self, hooks: Hooks) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// Set error callback
    ///
    /// Convenience method to set just the error callback without creating a Hooks object.
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .on_error(|error| async move {
    ///         eprintln!("Redis error: {:?}", error);
    ///     })
    ///     .build();
    /// ```
    pub fn on_error<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(crate::types::RedisError) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let hooks = self.hooks.take().unwrap_or_default();
        self.hooks = Some(hooks.with_error_callback(callback));
        self
    }

    /// Set connect callback
    ///
    /// Convenience method to set just the connect callback without creating a Hooks object.
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .on_connect(|attempt| async move {
    ///         println!("Connected on attempt {}", attempt);
    ///     })
    ///     .build();
    /// ```
    pub fn on_connect<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let hooks = self.hooks.take().unwrap_or_default();
        self.hooks = Some(hooks.with_connect_callback(callback));
        self
    }

    /// Set reconnect attempt callback
    ///
    /// Convenience method to set just the reconnect attempt callback without creating a Hooks object.
    ///
    /// # Example
    /// ```
    /// use redis_tower::config::ClientConfig;
    ///
    /// let config = ClientConfig::builder()
    ///     .on_reconnect_attempt(|attempt| async move {
    ///         println!("Attempting reconnect #{}", attempt);
    ///     })
    ///     .build();
    /// ```
    pub fn on_reconnect_attempt<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let hooks = self.hooks.take().unwrap_or_default();
        self.hooks = Some(hooks.with_reconnect_attempt_callback(callback));
        self
    }

    /// Build the client configuration
    pub fn build(self) -> ClientConfig {
        ClientConfig {
            tls: self.tls,
            tcp: self.tcp.unwrap_or_default(),
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
            health_check: self.health_check.unwrap_or_default(),
            tracing: self.tracing.unwrap_or_default(),
            metrics: self.metrics.unwrap_or_default(),
            hooks: self.hooks.unwrap_or_default(),
        }
    }
}

impl Default for ClientConfigBuilder {
    fn default() -> Self {
        Self {
            tls: TlsConfig::None,
            tcp: None,
            reconnect: None,
            health_check: None,
            tracing: None,
            metrics: None,
            hooks: None,
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
