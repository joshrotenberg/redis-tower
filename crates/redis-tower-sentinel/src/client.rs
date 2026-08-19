//! Shared sentinel-managed Redis client.

use std::sync::Arc;

use redis_tower::credentials::{
    CredentialProvider, CredentialReauthenticationHandle, Credentials, StreamingCredentialProvider,
    spawn_credential_reauthentication as spawn_reauthentication_task,
};
use redis_tower::{ReadPreference, ReadRoutingStrategy};
use redis_tower_commands::Ping;
use redis_tower_core::{Command, RedisError};
use redis_tower_protocol::RespLimits;
use tokio::sync::Mutex;

use crate::connection::{SentinelConnection, SentinelConnectionBuilder};

/// Builder for [`SentinelClient`].
///
/// Obtain one via [`SentinelClient::builder`].
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
/// # let _ = client;
/// # Ok(())
/// # }
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

    /// Set RESP decode limits for every sentinel and Redis data-node connection.
    ///
    /// The limits are retained across master rediscovery and failover. Defaults
    /// to [`RespLimits::default`].
    pub fn resp_limits(mut self, limits: RespLimits) -> Self {
        self.inner = self.inner.resp_limits(limits);
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

    /// Set the read preference for routing read-only commands.
    ///
    /// See [`SentinelConnectionBuilder::read_preference`] for the full
    /// semantics, including the no-usable-replica fallback.
    pub fn read_preference(mut self, pref: ReadPreference) -> Self {
        self.inner = self.inner.read_preference(pref);
        self
    }

    /// Set a custom read routing strategy for replica selection.
    ///
    /// See [`SentinelConnectionBuilder::read_routing`].
    pub fn read_routing(mut self, strategy: impl ReadRoutingStrategy) -> Self {
        self.inner = self.inner.read_routing(strategy);
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
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_sentinel::SentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// let client = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
///     .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
///     .node_credentials(StaticCredentials::password("redis_pass"))
///     .connect()
///     .await?;
/// # let _ = client;
/// # Ok(())
/// # }
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
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_sentinel::SentinelClient;
/// use redis_tower_commands::Set;
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
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SentinelClient {
    inner: Arc<Mutex<SentinelConnection>>,
}

impl SentinelClient {
    /// Re-authenticate the currently connected master and replicas.
    pub async fn reauthenticate_all(&self, credentials: &Credentials) -> Result<(), RedisError> {
        self.inner
            .lock()
            .await
            .reauthenticate_all(credentials)
            .await
    }

    /// Apply every credential emitted by `provider` to current data sockets.
    ///
    /// Sentinel discovery connections are short-lived and fetch from their
    /// configured provider each time; the stream is therefore applied only to
    /// the established master and replica sockets. Dropping the returned
    /// handle stops future updates.
    pub fn spawn_credential_reauthentication(
        &self,
        provider: Arc<dyn StreamingCredentialProvider>,
    ) -> CredentialReauthenticationHandle {
        let client = self.clone();
        spawn_reauthentication_task(provider, move |credentials| {
            let client = client.clone();
            async move { client.reauthenticate_all(&credentials).await }
        })
    }

    /// Create a builder for configuring the client.
    ///
    /// Use the builder to set sentinel credentials, node credentials, TLS, and
    /// RESP decode limits for each hop.
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

    /// Get the configured read preference.
    pub async fn read_preference(&self) -> ReadPreference {
        self.inner.lock().await.read_preference()
    }

    /// Address of the replica that served the most recent read-only command,
    /// or `None` if the most recent command went to the master.
    pub async fn last_replica_read(&self) -> Option<String> {
        self.inner
            .lock()
            .await
            .last_replica_read()
            .map(str::to_string)
    }
}

#[cfg(test)]
mod tests {
    use redis_tower::credentials::StaticCredentials;

    use super::*;

    #[test]
    fn client_builder_defaults_to_standard_resp_limits() {
        let builder = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster");
        assert_eq!(builder.inner.config.resp_limits, RespLimits::default());
    }

    #[test]
    fn client_builder_passes_through_resp_limits() {
        let limits = RespLimits {
            max_frame_size: 2048,
            max_depth: 12,
        };
        let builder = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster").resp_limits(limits);
        assert_eq!(builder.inner.config.resp_limits, limits);
    }

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
