//! Multiplexed sentinel-managed Redis client.
//!
//! [`MultiplexedSentinelClient`] batches concurrent requests from multiple
//! tasks into Redis pipelines automatically. It uses a single TCP connection
//! to the sentinel-discovered master, with a background worker shared across
//! all clones.
//!
//! # When to use
//!
//! - Many tasks issuing independent commands concurrently against a
//!   sentinel-managed Redis deployment
//! - Read-heavy workloads where connection pool overhead is undesirable
//! - High-concurrency scenarios where [`crate::SentinelClient`]'s mutex
//!   becomes a bottleneck
//!
//! For transactions (MULTI/EXEC) or commands requiring exclusive connection
//! access, use [`crate::SentinelConnection`] directly.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower_sentinel::MultiplexedSentinelClient;
//! use redis_tower::commands::*;
//!
//! let client = MultiplexedSentinelClient::connect(
//!     &["127.0.0.1:26379"],
//!     "mymaster",
//! ).await?;
//!
//! // Clone and share across tasks -- all share the same connection.
//! let c = client.clone();
//! tokio::spawn(async move {
//!     c.execute(Set::new("key", "value")).await.unwrap();
//! });
//!
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! ```

use std::sync::Arc;

use redis_tower::auto_pipeline::{
    AutoPipelineConfig, AutoPipelineReconnectConfig, AutoPipelineService,
};
use redis_tower::command_adapter::CommandAdapter;
use redis_tower::credentials::CredentialProvider;
use redis_tower_core::{Command, Frame, RedisError};
use tower_service::Service;

use crate::discovery::{self, SentinelConfig};

/// Builder for [`MultiplexedSentinelClient`].
///
/// Obtain one via [`MultiplexedSentinelClient::builder`].
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::MultiplexedSentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// let client = MultiplexedSentinelClient::builder(
///     &["127.0.0.1:26379"],
///     "mymaster",
/// )
/// .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
/// .node_credentials(StaticCredentials::password("redis_pass"))
/// .connect_with_reconnect()
/// .await?;
/// ```
pub struct MultiplexedSentinelClientBuilder {
    sentinel_addrs: Vec<String>,
    master_name: String,
    config: SentinelConfig,
}

impl MultiplexedSentinelClientBuilder {
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
    /// Sentinels and the data node commonly use different passwords in production.
    /// This credential is also used on every reconnect, so failover is
    /// re-authenticated automatically.
    pub fn node_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.config.node_credentials = Some(Arc::new(provider));
        self
    }

    /// Set the TLS configuration for sentinel connections.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn sentinel_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.sentinel_tls = Some(Arc::new(tls));
        self
    }

    /// Set the TLS configuration for node (master) connections.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn node_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.node_tls = Some(Arc::new(tls));
        self
    }

    /// Set the same TLS configuration for both sentinel and node connections.
    ///
    /// Convenience method when both hops share the same TLS settings.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        let shared = Arc::new(tls);
        self.config.sentinel_tls = Some(shared.clone());
        self.config.node_tls = Some(shared);
        self
    }

    /// Connect to the sentinel-discovered master (no automatic reconnection).
    ///
    /// For production use with failover support, prefer
    /// [`connect_with_reconnect`](Self::connect_with_reconnect).
    pub async fn connect(
        self,
    ) -> Result<MultiplexedSentinelClient<AutoPipelineService>, RedisError> {
        let master_addr = discovery::discover_master_with_config(
            &self.sentinel_addrs,
            &self.master_name,
            &self.config,
        )
        .await?;
        let conn = discovery::connect_hop(
            &master_addr,
            self.config.node_credentials.as_ref(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            self.config.node_tls.as_ref(),
        )
        .await?;
        Ok(MultiplexedSentinelClient {
            inner: CommandAdapter::new(AutoPipelineService::new(
                conn,
                AutoPipelineConfig::default(),
            )),
        })
    }

    /// Connect with automatic reconnection via sentinel discovery.
    ///
    /// On connection failure (or READONLY from a demoted master), the factory
    /// re-queries sentinel to find the current master. The reconnected master
    /// connection respects the configured node credentials and TLS.
    pub async fn connect_with_reconnect(
        self,
    ) -> Result<MultiplexedSentinelClient<AutoPipelineService>, RedisError> {
        let addrs = self.sentinel_addrs;
        let name = self.master_name;
        let config = self.config;

        let factory = move || {
            let addrs = addrs.clone();
            let name = name.clone();
            let config = config.clone();
            async move {
                // Verify ROLE so a reconnect lands on a real master, not a
                // demoted replica that sentinel still reports during a failover.
                let (conn, _addr) =
                    discovery::connect_verified_master_with_config(&addrs, &name, &config).await?;
                Ok(conn)
            }
        };
        // Enable READONLY-triggered reconnect: if the master is demoted to a
        // replica with TCP intact, writes return READONLY (not a connection
        // error), and the worker must rebuild via the factory to find the new
        // master instead of wedging on the demoted node.
        let pipeline_config = AutoPipelineConfig {
            reconnect_on_readonly: true,
            ..AutoPipelineConfig::default()
        };
        let svc = AutoPipelineService::with_factory(
            factory,
            pipeline_config,
            AutoPipelineReconnectConfig::default(),
        )
        .await?;
        Ok(MultiplexedSentinelClient {
            inner: CommandAdapter::new(svc),
        })
    }
}

/// A multiplexed sentinel-managed Redis client for high-concurrency workloads.
///
/// Wraps [`AutoPipelineService`] + [`CommandAdapter`] with sentinel discovery
/// for automatic master resolution. Clone-friendly: all clones share the same
/// background worker and TCP connection.
///
/// Concurrent requests from multiple tasks are batched into Redis pipelines
/// automatically. Single requests flush immediately with no batching delay.
///
/// # Auth and TLS
///
/// For auth or TLS, use [`MultiplexedSentinelClient::builder`] to configure
/// sentinel credentials, node credentials, and TLS independently:
///
/// ```ignore
/// use redis_tower_sentinel::MultiplexedSentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// let client = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
///     .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
///     .node_credentials(StaticCredentials::password("redis_pass"))
///     .connect_with_reconnect()
///     .await?;
/// ```
///
/// # Middleware
///
/// The type parameter `S` is the inner Frame-level [`Service`] and defaults to
/// [`AutoPipelineService`]. Use [`from_layered`](Self::from_layered) to wrap the
/// sentinel-managed client in a Tower middleware stack (circuit breaker,
/// timeout, retry).
#[derive(Clone)]
pub struct MultiplexedSentinelClient<S = AutoPipelineService> {
    inner: CommandAdapter<S>,
}

impl MultiplexedSentinelClient<AutoPipelineService> {
    /// Create a builder for configuring the client.
    ///
    /// Use the builder to set sentinel credentials, node credentials, and TLS
    /// independently for each hop. Credentials set via the builder are also
    /// used on every reconnect, so failover and re-auth are handled
    /// automatically.
    pub fn builder(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> MultiplexedSentinelClientBuilder {
        MultiplexedSentinelClientBuilder {
            sentinel_addrs: sentinel_addrs
                .iter()
                .map(|a| a.as_ref().to_string())
                .collect(),
            master_name: master_name.to_string(),
            config: SentinelConfig::default(),
        }
    }

    /// Connect to the sentinel-discovered master.
    ///
    /// Does not reconnect automatically on connection failure. For
    /// production use with failover support, use [`Self::connect_with_reconnect`].
    ///
    /// Uses plain TCP without auth. For auth or TLS, use [`Self::builder`].
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let master_addr =
            discovery::discover_master_with_config(&addrs, master_name, &SentinelConfig::default())
                .await?;
        let conn = discovery::connect_hop(
            &master_addr,
            None,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
        .await?;
        Ok(Self {
            inner: CommandAdapter::new(AutoPipelineService::new(
                conn,
                AutoPipelineConfig::default(),
            )),
        })
    }

    /// Connect with automatic reconnection via sentinel discovery.
    ///
    /// On connection failure, the factory re-queries sentinel to find
    /// the current master (which may have changed due to failover).
    ///
    /// Uses plain TCP without auth. For auth or TLS, use [`Self::builder`].
    pub async fn connect_with_reconnect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let name = master_name.to_string();
        let factory = move || {
            let addrs = addrs.clone();
            let name = name.clone();
            async move {
                // Verify ROLE so a reconnect lands on a real master, not a
                // demoted replica that sentinel still reports during a failover.
                let (conn, _addr) = discovery::connect_verified_master(&addrs, &name).await?;
                Ok(conn)
            }
        };
        // Enable READONLY-triggered reconnect: if the master is demoted to a
        // replica with TCP intact, writes return READONLY (not a connection
        // error), and the worker must rebuild via the factory to find the new
        // master instead of wedging on the demoted node.
        let config = AutoPipelineConfig {
            reconnect_on_readonly: true,
            ..AutoPipelineConfig::default()
        };
        let svc = AutoPipelineService::with_factory(
            factory,
            config,
            AutoPipelineReconnectConfig::default(),
        )
        .await?;
        Ok(Self {
            inner: CommandAdapter::new(svc),
        })
    }

    /// Gracefully shut down the multiplexed sentinel client.
    ///
    /// Signals the background worker to stop accepting new requests, then
    /// waits for all in-flight requests to complete and joins the background
    /// task. If other clones of this client are still alive, this returns
    /// immediately -- the worker continues running until the last clone shuts
    /// down or is dropped.
    ///
    /// For clean application shutdown, prefer calling `shutdown()` over
    /// simply dropping the client.
    pub async fn shutdown(self) {
        self.inner.into_inner().shutdown().await;
    }
}

impl<S> MultiplexedSentinelClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone,
    S::Future: Send + 'static,
{
    /// Build a sentinel client from a layered Frame-level [`Service`].
    ///
    /// The middleware injection point: wrap [`AutoPipelineService`] (or any
    /// `Service<Frame, Response = Frame, Error = RedisError>`) in a Tower stack
    /// and hand it here. Every [`execute`](Self::execute) then flows through the
    /// middleware. The caller is responsible for sentinel discovery when
    /// building the inner service; for the built-in discovery use
    /// [`connect`](Self::connect) or
    /// [`connect_with_reconnect`](Self::connect_with_reconnect).
    pub fn from_layered(service: S) -> Self {
        Self {
            inner: CommandAdapter::new(service),
        }
    }

    /// Execute a command against the sentinel-managed master.
    ///
    /// If other tasks are calling execute concurrently, their commands
    /// will be batched into a single Redis pipeline for efficiency.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut svc = self.inner.clone();
        std::future::poll_fn(|cx| <CommandAdapter<S> as Service<Cmd>>::poll_ready(&mut svc, cx))
            .await?;
        Service::call(&mut svc, cmd).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower::credentials::StaticCredentials;
    use redis_tower_commands::Get;
    use std::task::{Context, Poll};

    #[derive(Clone)]
    struct MockFrameService {
        reply: Frame,
    }

    impl Service<Frame> for MockFrameService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            std::future::ready(Ok(self.reply.clone()))
        }
    }

    #[tokio::test]
    async fn from_layered_routes_execute_through_injected_service() {
        let inner = MockFrameService {
            reply: Frame::BulkString(Some(Bytes::from("layered"))),
        };
        let client = MultiplexedSentinelClient::from_layered(inner);

        let client2 = client.clone();
        let val: Option<Bytes> = client2.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("layered")));
    }

    #[test]
    fn builder_sets_sentinel_credentials() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sp"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_none());
    }

    #[test]
    fn builder_sets_node_credentials() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .node_credentials(StaticCredentials::new("user", "np"));
        assert!(builder.config.node_credentials.is_some());
        assert!(builder.config.sentinel_credentials.is_none());
    }

    #[test]
    fn builder_sets_independent_credentials() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sp"))
            .node_credentials(StaticCredentials::password("np"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_some());
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn builder_tls_sets_both_hops() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster").tls(tls);
        assert!(builder.config.sentinel_tls.is_some());
        assert!(builder.config.node_tls.is_some());
    }
}
