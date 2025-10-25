//! Self-healing connection wrapper with automatic reconnection

use std::sync::Arc;
use tokio::sync::Mutex;
use tower_resilience::reconnect::ReconnectConfig;

use crate::client::RedisConnection;
use crate::commands::Command;
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
}

impl ResilientConnection {
    /// Create a new resilient connection
    pub async fn new(
        addr: String,
        tls: TlsConfig,
        reconnect_config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        let connection = RedisConnection::connect_with_config(&addr, tls.clone()).await?;

        Ok(Self {
            addr: Arc::new(addr),
            tls,
            inner: Arc::new(Mutex::new(Some(connection))),
            reconnect_config: Arc::new(reconnect_config),
        })
    }

    /// Get or create a connection
    async fn get_or_reconnect(&self) -> Result<RedisConnection, RedisError> {
        let mut inner = self.inner.lock().await;

        if let Some(conn) = inner.as_ref() {
            return Ok(conn.clone());
        }

        // Connection is dead, create a new one
        let new_conn = RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await?;
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
        let max_attempts = self.reconnect_config.max_attempts().unwrap_or(3);
        let mut attempt = 0;

        loop {
            attempt += 1;

            let conn = match self.get_or_reconnect().await {
                Ok(c) => c,
                Err(e) if attempt >= max_attempts => return Err(e),
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
                Ok(response) => return Ok(response),
                Err(e) if Self::is_connection_error(&e) => {
                    self.mark_failed().await;

                    if attempt >= max_attempts {
                        return Err(e);
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
                Err(e) => return Err(e),
            }
        }
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
