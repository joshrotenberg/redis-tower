//! Self-healing connection wrapper with automatic reconnection

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tower_resilience::reconnect::ReconnectConfig;

use crate::client::RedisConnection;
use crate::commands::Command;
use crate::metrics::MetricsCollector;
use crate::tls::TlsConfig;
use crate::types::RedisError;

/// A self-healing Redis connection that automatically reconnects on failure
///
/// This wraps a RedisConnection and uses tower-resilience's ReconnectLayer
/// to automatically handle connection failures with configurable retry logic.
#[derive(Clone)]
pub struct ResilientConnection {
    addr: Arc<String>,
    tls: TlsConfig,
    inner: Arc<Mutex<Option<RedisConnection>>>,
    reconnect_config: Arc<ReconnectConfig>,
    metrics: MetricsCollector,
}

impl ResilientConnection {
    /// Create a new resilient connection
    pub async fn new(
        addr: String,
        tls: TlsConfig,
        reconnect_config: ReconnectConfig,
        metrics: MetricsCollector,
    ) -> Result<Self, RedisError> {
        let connection = RedisConnection::connect_with_config(&addr, tls.clone()).await?;
        metrics.record_connection_created();

        Ok(Self {
            addr: Arc::new(addr),
            tls,
            inner: Arc::new(Mutex::new(Some(connection))),
            reconnect_config: Arc::new(reconnect_config),
            metrics,
        })
    }

    /// Get or create a connection
    async fn get_or_reconnect(&self) -> Result<RedisConnection, RedisError> {
        let mut inner = self.inner.lock().await;

        if let Some(conn) = inner.as_ref() {
            return Ok(conn.clone());
        }

        // Connection is dead, create a new one
        self.metrics.record_reconnection();
        let new_conn = RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await?;
        self.metrics.record_connection_created();
        *inner = Some(new_conn.clone());
        Ok(new_conn)
    }

    /// Mark connection as failed
    async fn mark_failed(&self) {
        let mut inner = self.inner.lock().await;
        *inner = None;
    }

    /// Execute a command with automatic reconnection
    pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
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
                    break Err(e);
                }
                Err(_) => {
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

            match conn.execute(command.clone()).await {
                Ok(response) => break Ok(response),
                Err(e) if Self::is_connection_error(&e) => {
                    self.mark_failed().await;

                    if attempt >= max_attempts {
                        self.metrics.record_error("connection");
                        break Err(e);
                    }

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
                    // Determine error type for metrics
                    match &e {
                        RedisError::Connection(_) => self.metrics.record_error("connection"),
                        RedisError::Protocol(_) => self.metrics.record_error("parse"),
                        RedisError::Redis(_) => self.metrics.record_error("redis"),
                        _ => self.metrics.record_error("other"),
                    }
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
