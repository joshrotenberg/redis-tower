//! Tower Service implementation for Sentinel

use super::config::SentinelConfig;
use super::discovery::discover_master;
use crate::client::RedisConnection;
use crate::types::RedisError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;
use tracing::debug;

/// A Tower Service that discovers and connects to the Redis master via Sentinel
///
/// This service integrates with Tower's `Reconnect` middleware to provide
/// automatic failover when the master changes.
#[derive(Clone)]
pub struct SentinelMakeService {
    config: Arc<SentinelConfig>,
}

impl SentinelMakeService {
    /// Create a new SentinelMakeService with the given configuration
    pub fn new(config: SentinelConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl Service<()> for SentinelMakeService {
    type Response = RedisConnection;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready to create a new service
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _target: ()) -> Self::Future {
        let config = Arc::clone(&self.config);

        Box::pin(async move {
            debug!("Discovering master via Sentinel");

            // Step 1: Query Sentinels for master address
            let (host, port) = discover_master(&config).await?;

            debug!("Connecting to master at {}:{}", host, port);

            // Step 2: Connect to the discovered master
            let addr = format!("{}:{}", host, port);
            let conn = RedisConnection::connect(&addr).await?;

            // Step 3: Authenticate if credentials provided
            if let Some(username) = &config.redis_username {
                if let Some(password) = &config.redis_password {
                    use crate::commands::AuthAcl;
                    conn.execute(AuthAcl::new(username, password)).await?;
                }
            } else if let Some(password) = &config.redis_password {
                use crate::commands::Auth;
                conn.execute(Auth::new(password)).await?;
            }

            debug!("Successfully connected to master at {}:{}", host, port);

            Ok(conn)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker_ref;

    #[tokio::test]
    #[ignore] // Requires running Sentinel
    async fn test_make_service_discovers_master() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .build()
            .unwrap();

        let mut make_service = SentinelMakeService::new(config);

        // Poll ready
        let mut cx = Context::from_waker(noop_waker_ref());
        let poll = make_service.poll_ready(&mut cx);
        assert!(matches!(poll, Poll::Ready(Ok(()))));

        // Call service should discover and connect to master
        let service = make_service.call(()).await;
        assert!(service.is_ok());
    }
}
