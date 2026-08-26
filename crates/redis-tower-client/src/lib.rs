//! A unified Redis client over the three redis-tower deployment topologies.
//!
//! [`UniversalClient`] wraps the standalone, cluster, and sentinel multiplexed
//! clients behind one type, so application code can be written once and pointed
//! at any topology -- the fred-style "one client" ergonomics. It is the only
//! place in the workspace that can see all three client crates at once.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::commands::{Get, Set};
//! use redis_tower_client::UniversalClient;
//!
//! // Pick the topology from the URL scheme:
//! //   redis://host:port            -> standalone
//! //   redis+cluster://host:port    -> cluster (seed node)
//! //   redis+sentinel://h1,h2/name  -> sentinel (sentinels + master name)
//! // Each scheme accepts `user:pass@` credentials, and the `rediss` variants
//! // (rediss://, rediss+cluster://, rediss+sentinel://) enable TLS.
//! let client = UniversalClient::connect_url("redis://127.0.0.1:6379").await?;
//!
//! client.execute(Set::new("key", "value")).await?;
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! # let _ = val;
//! # Ok(())
//! # }
//! ```
//!
//! `UniversalClient` is [`Clone`] (every variant is a cheap handle) and
//! implements [`RedisExecutor`], so it drops into any generic code that accepts
//! `impl RedisExecutor`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;

use redis_tower::{MultiplexedClient, RedisExecutor};
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_core::{Command, RedisError};
use redis_tower_sentinel::MultiplexedSentinelClient;

/// A Redis client that abstracts over standalone, cluster, and sentinel
/// topologies.
///
/// Construct it with [`connect_url`](Self::connect_url) for URL-driven variant
/// selection, or with the explicit [`standalone`](Self::standalone),
/// [`cluster`](Self::cluster), and [`sentinel`](Self::sentinel) constructors.
/// All variants share the same [`execute`](Self::execute) surface.
#[derive(Clone)]
pub enum UniversalClient {
    /// A single-node [`MultiplexedClient`].
    Standalone(MultiplexedClient),
    /// A cluster-aware [`MultiplexedClusterClient`].
    Cluster(MultiplexedClusterClient),
    /// A sentinel-managed [`MultiplexedSentinelClient`].
    Sentinel(MultiplexedSentinelClient),
}

impl UniversalClient {
    /// Connect to a standalone server from a `redis://`, `rediss://`, or
    /// `unix://` URL.
    pub async fn standalone(url: &str) -> Result<Self, RedisError> {
        Ok(Self::Standalone(MultiplexedClient::connect_url(url).await?))
    }

    /// Connect to a cluster from a seed node address (`host:port`).
    ///
    /// The full topology is discovered from the seed.
    pub async fn cluster(seed_addr: &str) -> Result<Self, RedisError> {
        Ok(Self::Cluster(
            MultiplexedClusterClient::connect(seed_addr).await?,
        ))
    }

    /// Connect to a sentinel-managed master.
    ///
    /// `sentinel_addrs` are the `host:port` addresses of the sentinels;
    /// `master_name` is the monitored master's name.
    pub async fn sentinel<S: AsRef<str>>(
        sentinel_addrs: &[S],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        Ok(Self::Sentinel(
            MultiplexedSentinelClient::connect(sentinel_addrs, master_name).await?,
        ))
    }

    /// Connect, selecting the topology from the URL scheme:
    ///
    /// - `redis://`, `rediss://`, `unix://` -> [`Standalone`](Self::Standalone)
    /// - `redis+cluster://[user:pass@]host:port` / `rediss+cluster://...` ->
    ///   [`Cluster`](Self::Cluster) (the host is the seed node)
    /// - `redis+sentinel://[user:pass@]h1:p1,h2:p2/master-name` /
    ///   `rediss+sentinel://...` -> [`Sentinel`](Self::Sentinel)
    ///   (comma-separated sentinels, master name after the `/`)
    ///
    /// Every scheme carries authentication and TLS:
    ///
    /// - `user:pass@` userinfo authenticates the Redis data nodes on all
    ///   three topologies (`user:pass` is an ACL login, `:pass` a legacy
    ///   `requirepass` password); components are percent-decoded.
    /// - The `rediss` variants enable TLS -- for cluster, on every node
    ///   connection; for sentinel, on both the sentinel and node hops. A
    ///   `rediss` URL **errors** unless a TLS backend feature (`tls-rustls`
    ///   or `tls-native-tls`) is enabled; it never silently connects in
    ///   plaintext.
    /// - Sentinel URLs additionally accept `?sentinel_username=U&`
    ///   `sentinel_password=P` for sentinels that require their own
    ///   credentials, and default sentinel hosts without a port to 26379.
    ///   See [`MultiplexedSentinelClient::connect_url`].
    ///
    /// The cluster and sentinel variants connect with automatic
    /// reconnection, re-applying the URL's credentials and TLS after node
    /// failures or failover.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidUrl`] if a `+cluster` / `+sentinel` URL is
    /// missing its seed, sentinels, or master name, or if a `rediss` URL is
    /// used without a TLS feature; otherwise propagates the underlying
    /// connection error.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        // The cluster client's connect_url speaks plain redis:// / rediss://,
        // so translate the +cluster schemes onto it -- it wires the URL's
        // credentials and TLS (or rejects rediss:// without a TLS feature).
        if let Some(rest) = url.strip_prefix("redis+cluster://") {
            if rest.is_empty() {
                return Err(RedisError::InvalidUrl(
                    "redis+cluster URL requires a seed host:port".into(),
                ));
            }
            return Ok(Self::Cluster(
                MultiplexedClusterClient::connect_url(&format!("redis://{rest}")).await?,
            ));
        }
        if let Some(rest) = url.strip_prefix("rediss+cluster://") {
            if rest.is_empty() {
                return Err(RedisError::InvalidUrl(
                    "rediss+cluster URL requires a seed host:port".into(),
                ));
            }
            return Ok(Self::Cluster(
                MultiplexedClusterClient::connect_url(&format!("rediss://{rest}")).await?,
            ));
        }

        if url.starts_with("redis+sentinel://") || url.starts_with("rediss+sentinel://") {
            return Ok(Self::Sentinel(
                MultiplexedSentinelClient::connect_url(url).await?,
            ));
        }

        // Default: standalone (redis://, rediss://, unix://).
        Self::standalone(url).await
    }

    /// Execute a command against the underlying client, regardless of topology.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        match self {
            Self::Standalone(c) => c.execute(cmd).await,
            Self::Cluster(c) => c.execute(cmd).await,
            Self::Sentinel(c) => c.execute(cmd).await,
        }
    }

    /// The topology variant name (`"standalone"`, `"cluster"`, `"sentinel"`).
    pub fn topology(&self) -> &'static str {
        match self {
            Self::Standalone(_) => "standalone",
            Self::Cluster(_) => "cluster",
            Self::Sentinel(_) => "sentinel",
        }
    }
}

impl RedisExecutor for UniversalClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        UniversalClient::execute(self, cmd)
    }
}

impl std::fmt::Debug for UniversalClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniversalClient")
            .field("topology", &self.topology())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_redis_executor<T: RedisExecutor>() {}

    #[test]
    fn universal_client_implements_redis_executor() {
        assert_redis_executor::<UniversalClient>();
    }

    #[tokio::test]
    async fn connect_url_rejects_sentinel_without_master() {
        let err = UniversalClient::connect_url("redis+sentinel://127.0.0.1:26379")
            .await
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_url_rejects_sentinel_without_hosts() {
        let err = UniversalClient::connect_url("redis+sentinel:///mymaster")
            .await
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_url_rejects_empty_cluster_seed() {
        let err = UniversalClient::connect_url("redis+cluster://")
            .await
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_url_rejects_empty_tls_cluster_seed() {
        let err = UniversalClient::connect_url("rediss+cluster://")
            .await
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_url_rejects_tls_sentinel_without_master() {
        let err = UniversalClient::connect_url("rediss+sentinel://127.0.0.1:26379")
            .await
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_url_rejects_unknown_sentinel_query_param() {
        // A typo'd credential key must error, not silently drop auth.
        let err = UniversalClient::connect_url("redis+sentinel://127.0.0.1:26379/mymaster?bogus=1")
            .await
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }
}
