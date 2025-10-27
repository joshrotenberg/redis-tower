//! Self-healing connection wrapper with automatic reconnection

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tower_resilience::reconnect::ReconnectConfig;

use crate::client::RedisConnection;
use crate::commands::Command;
use crate::health::{HealthCheckConfig, HealthChecker};
use crate::hooks::Hooks;
use crate::metrics::MetricsCollector;
use crate::tls::TlsConfig;
use crate::types::RedisError;

/// A self-healing Redis connection that automatically reconnects on failure
///
/// This wraps a RedisConnection and uses tower-resilience's ReconnectLayer
/// to automatically handle connection failures with configurable retry logic.
/// It also includes optional health checking to proactively detect connection issues.
#[derive(Clone)]
pub struct ResilientConnection {
    addr: Arc<String>,
    tls: TlsConfig,
    inner: Arc<Mutex<Option<RedisConnection>>>,
    reconnect_config: Arc<ReconnectConfig>,
    health_checker: HealthChecker,
    metrics: MetricsCollector,
    hooks: Hooks,
    attempt_counter: Arc<Mutex<usize>>,
}

impl ResilientConnection {
    /// Create a new resilient connection
    pub async fn new(
        addr: String,
        tls: TlsConfig,
        reconnect_config: ReconnectConfig,
        health_check_config: HealthCheckConfig,
        metrics: MetricsCollector,
        hooks: Hooks,
    ) -> Result<Self, RedisError> {
        let connection = RedisConnection::connect_with_config(&addr, tls.clone()).await?;
        metrics.record_connection_created();

        // Notify initial connection
        hooks.notify_connect(1).await;

        Ok(Self {
            addr: Arc::new(addr),
            tls,
            inner: Arc::new(Mutex::new(Some(connection))),
            reconnect_config: Arc::new(reconnect_config),
            health_checker: HealthChecker::new(health_check_config),
            metrics,
            hooks,
            attempt_counter: Arc::new(Mutex::new(1)),
        })
    }

    /// Get or create a connection, with optional health checking
    async fn get_or_reconnect(&self) -> Result<RedisConnection, RedisError> {
        let mut inner = self.inner.lock().await;

        // Check if we have a connection
        if let Some(conn) = inner.as_ref() {
            // Perform health check if needed
            if self.health_checker.should_check().await {
                let is_healthy = self.health_checker.check(conn).await;
                if !is_healthy {
                    // Health check failed, mark connection as failed
                    *inner = None;
                } else {
                    return Ok(conn.clone());
                }
            } else {
                return Ok(conn.clone());
            }
        }

        // Connection is dead or unhealthy, create a new one
        self.metrics.record_reconnection();

        // Increment attempt counter
        let mut attempt = self.attempt_counter.lock().await;
        *attempt += 1;
        let current_attempt = *attempt;
        drop(attempt);

        // Notify reconnection attempt
        self.hooks
            .notify_reconnect_attempt(current_attempt - 1)
            .await;

        let new_conn = RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await?;
        self.metrics.record_connection_created();

        // Notify successful connection
        self.hooks.notify_connect(current_attempt).await;

        *inner = Some(new_conn.clone());
        Ok(new_conn)
    }

    /// Mark connection as failed
    async fn mark_failed(&self) {
        let mut inner = self.inner.lock().await;
        *inner = None;
    }

    /// Execute a command with automatic reconnection (deprecated)
    ///
    /// # Deprecated
    /// Use [`call`](Self::call) instead for consistency with Tower's Service trait.
    #[deprecated(since = "0.2.0", note = "Use `call()` instead")]
    pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command + Clone,
    {
        self.call(command).await
    }

    /// Call a command with automatic reconnection
    ///
    /// This is the preferred method for executing commands on a connection pool.
    /// Automatically retries failed commands based on the reconnection policy.
    pub async fn call<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command + Clone,
    {
        let start = Instant::now();
        let max_attempts = self.reconnect_config.max_attempts().unwrap_or(3);
        let mut attempt = 0;

        let result = loop {
            attempt += 1;

            let conn = match self.get_or_reconnect().await {
                Ok(c) => c,
                Err(e) if attempt >= max_attempts => {
                    self.metrics.record_error("connection");
                    self.hooks.notify_error(e.clone()).await;
                    break Err(e);
                }
                Err(e) => {
                    self.hooks.notify_error(e).await;
                    // Wait before retry
                    if let Some(delay) = self
                        .reconnect_config
                        .policy()
                        .delay_for_attempt(attempt as usize)
                    {
                        tokio::time::sleep(delay).await;
                    }
                    continue;
                }
            };

            match conn.call(command.clone()).await {
                Ok(response) => break Ok(response),
                Err(e) if Self::is_connection_error(&e) => {
                    self.mark_failed().await;

                    if attempt >= max_attempts {
                        self.metrics.record_error("connection");
                        self.hooks.notify_error(e.clone()).await;
                        break Err(e);
                    }

                    self.hooks.notify_error(e).await;
                    // Wait before retry
                    if let Some(delay) = self
                        .reconnect_config
                        .policy()
                        .delay_for_attempt(attempt as usize)
                    {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(e) => {
                    // Record error in health checker for passive health tracking
                    self.health_checker.record_error(&e).await;

                    // Determine error type for metrics
                    match &e {
                        RedisError::Connection(_) => self.metrics.record_error("connection"),
                        RedisError::Protocol(_) => self.metrics.record_error("parse"),
                        RedisError::Redis(_) => self.metrics.record_error("redis"),
                        _ => self.metrics.record_error("other"),
                    }

                    // Notify error hook
                    self.hooks.notify_error(e.clone()).await;
                    break Err(e);
                }
            }
        };

        // Record command execution time
        self.metrics.record_command(start.elapsed());
        result
    }

    /// Check if an error indicates a connection failure
    fn is_connection_error(error: &RedisError) -> bool {
        matches!(error, RedisError::Connection(_))
    }

    /// Get the health checker for this connection
    ///
    /// This allows inspecting health status and statistics.
    pub fn health_checker(&self) -> &HealthChecker {
        &self.health_checker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tower_resilience::reconnect::ReconnectPolicy;

    #[test]
    fn test_is_connection_error() {
        assert!(ResilientConnection::is_connection_error(
            &RedisError::Connection("test".to_string())
        ));

        assert!(!ResilientConnection::is_connection_error(
            &RedisError::Protocol("test".to_string())
        ));
    }

    #[test]
    fn test_reconnect_config() {
        let config = ReconnectConfig::builder()
            .policy(ReconnectPolicy::exponential(
                Duration::from_millis(100),
                Duration::from_secs(5),
            ))
            .max_attempts(5)
            .build();

        assert_eq!(config.max_attempts(), Some(5));
    }
}
