//! Sentinel-managed Redis connection with automatic failover.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use redis_tower::credentials::CredentialProvider;
use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::discovery::{self, SentinelConfig};

/// Builder for [`SentinelConnection`].
///
/// Obtain one via [`SentinelConnection::builder`].
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelConnection;
/// use redis_tower::credentials::StaticCredentials;
///
/// let conn = SentinelConnection::builder(
///     &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
///     "mymaster",
/// )
/// .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
/// .node_credentials(StaticCredentials::password("redis_pass"))
/// .connect()
/// .await?;
/// ```
pub struct SentinelConnectionBuilder {
    sentinel_addrs: Vec<String>,
    master_name: String,
    pub(crate) config: SentinelConfig,
}

impl SentinelConnectionBuilder {
    /// Authenticate sentinel connections with the given credential provider.
    ///
    /// Called once per sentinel query. Supports dynamic credentials (token
    /// rotation) via a custom [`CredentialProvider`] implementation.
    pub fn sentinel_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.config.sentinel_credentials = Some(Arc::new(provider));
        self
    }

    /// Authenticate master (node) connections with the given credential provider.
    ///
    /// Used when connecting to the discovered Redis master. Sentinels and the
    /// data node commonly use different passwords in production.
    pub fn node_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.config.node_credentials = Some(Arc::new(provider));
        self
    }

    /// Set the TLS configuration for sentinel connections.
    ///
    /// When set, all connections to sentinel nodes use TLS. The hostname for
    /// SNI verification is derived from each sentinel's address.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn sentinel_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.sentinel_tls = Some(Arc::new(tls));
        self
    }

    /// Set the TLS configuration for node (master) connections.
    ///
    /// When set, connections to the discovered Redis master use TLS.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn node_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.node_tls = Some(Arc::new(tls));
        self
    }

    /// Set the same TLS configuration for both sentinel and node connections.
    ///
    /// Convenience method when both hops share the same TLS settings. Equivalent
    /// to calling `.sentinel_tls(tls.clone())` and `.node_tls(tls)` (the config
    /// is cloned internally and stored in an `Arc` for each hop).
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        let shared = Arc::new(tls);
        self.config.sentinel_tls = Some(shared.clone());
        self.config.node_tls = Some(shared);
        self
    }

    /// Connect to the Redis master discovered via sentinel.
    pub async fn connect(self) -> Result<SentinelConnection, RedisError> {
        SentinelConnection::connect_with_config(self.sentinel_addrs, self.master_name, self.config)
            .await
    }
}

/// A Redis connection managed by Sentinel.
///
/// Discovers the current master via Sentinel and connects to it.
/// When a command fails with a connection error, the connection
/// immediately rediscovers the master (which may have changed due to
/// failover). The caller should retry the command.
///
/// # Concurrency
///
/// `SentinelConnection` requires exclusive (`&mut self`) access for all
/// operations. It is NOT `Clone`. Share it via
/// [`SentinelClient`](crate::client::SentinelClient)
/// (`Arc<Mutex<SentinelConnection>>`) or use it directly in a single task.
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelConnection;
///
/// let mut conn = SentinelConnection::connect(
///     &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
///     "mymaster",
/// ).await?;
///
/// conn.execute(Set::new("key", "value")).await?;
/// ```
pub struct SentinelConnection {
    /// Current connection to the master.
    conn: RedisConnection,
    /// Sentinel addresses for rediscovery.
    sentinel_addrs: Vec<String>,
    /// Monitored master name.
    master_name: String,
    /// Address of the master this connection is currently bound to.
    ///
    /// Tracked so that rediscovery can log the old -> new master transition
    /// after a failover.
    current_addr: String,
    /// Whether the connection needs rediscovery.
    needs_rediscovery: bool,
    /// Sentinel and node configuration (credentials, TLS).
    config: SentinelConfig,
}

impl SentinelConnection {
    /// Create a builder for configuring the connection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use redis_tower_sentinel::SentinelConnection;
    /// use redis_tower::credentials::StaticCredentials;
    ///
    /// let conn = SentinelConnection::builder(
    ///     &["127.0.0.1:26379"],
    ///     "mymaster",
    /// )
    /// .node_credentials(StaticCredentials::password("secret"))
    /// .connect()
    /// .await?;
    /// ```
    pub fn builder(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> SentinelConnectionBuilder {
        SentinelConnectionBuilder {
            sentinel_addrs: sentinel_addrs
                .iter()
                .map(|a| a.as_ref().to_string())
                .collect(),
            master_name: master_name.to_string(),
            config: SentinelConfig::default(),
        }
    }

    /// Connect to the Redis master discovered via Sentinel.
    ///
    /// Uses plain TCP connections to both sentinel and master without
    /// authentication. For auth or TLS, use [`Self::builder`].
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        Self::connect_with_config(addrs, master_name.to_string(), SentinelConfig::default()).await
    }

    /// Internal: connect using explicit config.
    async fn connect_with_config(
        addrs: Vec<String>,
        master_name: String,
        config: SentinelConfig,
    ) -> Result<Self, RedisError> {
        let master_addr =
            discovery::discover_master_with_config(&addrs, &master_name, &config).await?;
        let conn = discovery::connect_hop(
            &master_addr,
            config.node_credentials.as_ref(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            config.node_tls.as_ref(),
        )
        .await?;

        Ok(Self {
            conn,
            sentinel_addrs: addrs,
            master_name,
            current_addr: master_addr,
            needs_rediscovery: false,
            config,
        })
    }

    /// Execute a command against the current master.
    ///
    /// If the connection was marked as needing rediscovery (after a
    /// previous connection error), rediscovers the master first.
    ///
    /// On a connection error during execution (which may indicate a
    /// failover), the master is rediscovered eagerly so that the next
    /// `execute()` call connects to the new master without an additional
    /// rediscovery round-trip. The current command cannot be retried here
    /// because it has already been consumed; the caller should retry if
    /// appropriate.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        if self.needs_rediscovery {
            self.rediscover().await?;
        }

        let result = self.conn.execute(cmd).await;
        if let Err(ref e) = result
            && (e.is_connection_error() || e.is_readonly())
        {
            // Two failover modes trigger rediscovery:
            //   - connection error: the master became unreachable.
            //   - READONLY: the master was demoted to a replica (REPLICAOF)
            //     with TCP intact, so writes now fail with READONLY. Without
            //     this the client wedges on the demoted node forever.
            tracing::warn!(
                error = %e,
                master_name = %self.master_name,
                "sentinel: master unreachable or demoted, rediscovering"
            );
            // Eagerly rediscover the new master so the next execute() call
            // connects immediately. The current command cannot be retried here
            // because it has been consumed; the caller should retry if
            // appropriate. If rediscovery fails, fall back to the deferred path
            // so the next call tries again.
            self.needs_rediscovery = self.rediscover().await.is_err();
        }
        result
    }

    /// Force rediscovery of the master and reconnect.
    ///
    /// Sentinel's view of the master can lag a failover, so each candidate is
    /// verified with `ROLE` before it is trusted: if the node still reports
    /// `slave` (the failover has not fully propagated, or we got the demoted
    /// old master back), the attempt is retried with exponential backoff until
    /// a real master is found or the attempt budget is exhausted.
    ///
    /// The reconnected master connection respects the node credentials and TLS
    /// settings configured via [`SentinelConnection::builder`].
    pub async fn rediscover(&mut self) -> Result<(), RedisError> {
        match discovery::connect_verified_master_with_config(
            &self.sentinel_addrs,
            &self.master_name,
            &self.config,
        )
        .await
        {
            Ok((conn, master_addr)) => {
                tracing::info!(
                    old_addr = %self.current_addr,
                    new_addr = %master_addr,
                    master_name = %self.master_name,
                    "sentinel: master rediscovered"
                );
                self.conn = conn;
                self.current_addr = master_addr;
                self.needs_rediscovery = false;
                Ok(())
            }
            Err(e) => {
                self.needs_rediscovery = true;
                Err(e)
            }
        }
    }

    /// Get the sentinel addresses.
    pub fn sentinel_addrs(&self) -> &[String] {
        &self.sentinel_addrs
    }

    /// Get the monitored master name.
    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// Discover current replica addresses from sentinel.
    pub async fn discover_replicas(&self) -> Result<Vec<String>, RedisError> {
        discovery::discover_replicas_with_config(
            &self.sentinel_addrs,
            &self.master_name,
            &self.config,
        )
        .await
    }
}

impl redis_tower::RedisExecutor for SentinelConnection {
    fn execute<Cmd: redis_tower_core::Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl std::future::Future<Output = Result<Cmd::Response, redis_tower_core::RedisError>> + Send
    {
        SentinelConnection::execute(self, cmd)
    }
}

impl<Cmd: Command + 'static> tower_service::Service<Cmd> for SentinelConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <RedisConnection as tower_service::Service<Cmd>>::poll_ready(&mut self.conn, cx)
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        <RedisConnection as tower_service::Service<Cmd>>::call(&mut self.conn, cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower::credentials::StaticCredentials;

    #[test]
    fn builder_sets_sentinel_credentials() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sentinel_pass"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_none());
    }

    #[test]
    fn builder_sets_node_credentials() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .node_credentials(StaticCredentials::new("alice", "redis_pass"));
        assert!(builder.config.node_credentials.is_some());
        assert!(builder.config.sentinel_credentials.is_none());
    }

    #[test]
    fn builder_sets_independent_credentials() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("s"))
            .node_credentials(StaticCredentials::password("n"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_some());
    }

    #[test]
    fn builder_stores_sentinel_addrs_and_name() {
        let builder =
            SentinelConnection::builder(&["127.0.0.1:26379", "127.0.0.1:26380"], "mymaster");
        assert_eq!(builder.sentinel_addrs.len(), 2);
        assert_eq!(builder.master_name, "mymaster");
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn builder_tls_sets_both_hops() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster").tls(tls);
        assert!(builder.config.sentinel_tls.is_some());
        assert!(builder.config.node_tls.is_some());
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn builder_sentinel_tls_only() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder =
            SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster").sentinel_tls(tls);
        assert!(builder.config.sentinel_tls.is_some());
        assert!(builder.config.node_tls.is_none());
    }
}
