//! Shared sentinel-managed Redis client.

use std::sync::Arc;

use redis_tower::credentials::CredentialProvider;
use redis_tower_commands::Ping;
use redis_tower_core::{Command, RedisError};
use tokio::sync::Mutex;

use crate::connection::{SentinelConnection, SentinelConnectionBuilder};

/// Builder for [`SentinelClient`].
///
/// Obtain one via [`SentinelClient::builder`].
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// let client = SentinelClient::builder(
///     &["127.0.0.1:26379", "127.0.0.1:26380"],
///     "mymaster",
/// )
/// .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
/// .node_credentials(StaticCredentials::password("redis_pass"))
/// .connect()
/// .await?;
/// ```
pub struct SentinelClientBuilder {
    inner: SentinelConnectionBuilder,
}

impl SentinelClientBuilder {
    /// Authenticate sentinel connections with the given credential provider.
    ///
    /// Called once per sentinel query. Supports dynamic credentials (token
    /// rotation) via a custom [`CredentialProvider`] implementation.
    pub fn sentinel_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.inner = self.inner.sentinel_credentials(provider);
        self
    }

    /// Authenticate master (node) connections with the given credential provider.
    ///
    /// Sentinels and the data node commonly use different passwords in production.
    pub fn node_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.inner = self.inner.node_credentials(provider);
        self
    }

    /// Set the TLS configuration for sentinel connections.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn sentinel_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.inner = self.inner.sentinel_tls(tls);
        self
    }

    /// Set the TLS configuration for node (master) connections.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn node_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.inner = self.inner.node_tls(tls);
        self
    }

    /// Set the same TLS configuration for both sentinel and node connections.
    ///
    /// Convenience method when both hops share the same TLS settings.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.inner = self.inner.tls(tls);
        self
    }

    /// Connect to the master discovered via sentinel.
    pub async fn connect(self) -> Result<SentinelClient, RedisError> {
        let conn = self.inner.connect().await?;
        Ok(SentinelClient {
            inner: Arc::new(Mutex::new(conn)),
        })
    }
}

/// A shared, sentinel-managed Redis client.
///
/// Wraps a [`SentinelConnection`] in `Arc<Mutex<>>` for cross-task sharing.
/// Automatically rediscovers the master on connection failure.
///
/// # Auth and TLS
///
/// For auth or TLS, use [`SentinelClient::builder`] to configure sentinel
/// credentials, node credentials, and TLS independently for each hop:
///
/// ```ignore
/// use redis_tower_sentinel::SentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// let client = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
///     .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
///     .node_credentials(StaticCredentials::password("redis_pass"))
///     .connect()
///     .await?;
/// ```
///
/// # Concurrency
///
/// `SentinelClient` is `Clone + Send + Sync`. All clones share the same
/// `Arc<Mutex<SentinelConnection>>`, serializing all commands through one lock.
/// For higher concurrency, use
/// [`MultiplexedSentinelClient`](crate::MultiplexedSentinelClient).
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelClient;
///
/// let client = SentinelClient::connect(
///     &["127.0.0.1:26379", "127.0.0.1:26380"],
///     "mymaster",
/// ).await?;
///
/// let c = client.clone();
/// tokio::spawn(async move {
///     c.execute(Set::new("key", "value")).await.unwrap();
/// });
/// ```
#[derive(Clone)]
pub struct SentinelClient {
    inner: Arc<Mutex<SentinelConnection>>,
}

impl SentinelClient {
    /// Create a builder for configuring the client.
    ///
    /// Use the builder to set sentinel credentials, node credentials, and TLS
    /// independently for each hop.
    pub fn builder(sentinel_addrs: &[impl AsRef<str>], master_name: &str) -> SentinelClientBuilder {
        SentinelClientBuilder {
            inner: SentinelConnection::builder(sentinel_addrs, master_name),
        }
    }

    /// Connect to the master discovered via Sentinel.
    ///
    /// Uses plain TCP connections to both sentinel and master without
    /// authentication. For auth or TLS, use [`Self::builder`].
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let conn = SentinelConnection::connect(sentinel_addrs, master_name).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Execute a command against the current master.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut conn = self.inner.lock().await;
        conn.execute(cmd).await
    }

    /// Force rediscovery and reconnection to the master.
    pub async fn rediscover(&self) -> Result<(), RedisError> {
        let mut conn = self.inner.lock().await;
        conn.rediscover().await
    }

    /// Send a PING to the current master.
    ///
    /// Returns `Ok(())` on success. Useful for Kubernetes readiness probes
    /// and `/health` endpoints.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        let mut conn = self.inner.lock().await;
        conn.execute(Ping::new()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use redis_tower::credentials::StaticCredentials;

    use super::*;

    #[test]
    fn client_builder_sets_sentinel_credentials() {
        let builder = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sp"));
        // Access inner config fields through the connection builder.
        assert!(builder.inner.config.sentinel_credentials.is_some());
        assert!(builder.inner.config.node_credentials.is_none());
    }

    #[test]
    fn client_builder_sets_node_credentials() {
        let builder = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .node_credentials(StaticCredentials::new("user", "np"));
        assert!(builder.inner.config.node_credentials.is_some());
        assert!(builder.inner.config.sentinel_credentials.is_none());
    }

    #[test]
    fn client_builder_sets_both_independently() {
        let builder = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sp"))
            .node_credentials(StaticCredentials::password("np"));
        assert!(builder.inner.config.sentinel_credentials.is_some());
        assert!(builder.inner.config.node_credentials.is_some());
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn client_builder_tls_sets_both_hops() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster").tls(tls);
        assert!(builder.inner.config.sentinel_tls.is_some());
        assert!(builder.inner.config.node_tls.is_some());
    }
}
