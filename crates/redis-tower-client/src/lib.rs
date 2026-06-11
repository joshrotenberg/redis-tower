//! A unified Redis client over the three redis-tower deployment topologies.
//!
//! [`UniversalClient`] wraps the standalone, cluster, and sentinel multiplexed
//! clients behind one type, so application code can be written once and pointed
//! at any topology -- the fred-style "one client" ergonomics. It is the only
//! place in the workspace that can see all three client crates at once.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower_client::UniversalClient;
//! use redis_tower::commands::*;
//!
//! // Pick the topology from the URL scheme:
//! //   redis://host:port            -> standalone
//! //   redis+cluster://host:port    -> cluster (seed node)
//! //   redis+sentinel://h1,h2/name  -> sentinel (sentinels + master name)
//! let client = UniversalClient::connect_url("redis://127.0.0.1:6379").await?;
//!
//! client.execute(Set::new("key", "value")).await?;
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! ```
//!
//! `UniversalClient` is [`Clone`] (every variant is a cheap handle) and
//! implements [`RedisExecutor`], so it drops into any generic code that accepts
//! `impl RedisExecutor`.

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
    /// - `redis+cluster://host:port` / `rediss+cluster://...` ->
    ///   [`Cluster`](Self::Cluster) (the host is the seed node)
    /// - `redis+sentinel://h1:p1,h2:p2/master-name` ->
    ///   [`Sentinel`](Self::Sentinel) (comma-separated sentinels, master name
    ///   after the `/`)
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidUrl`] if a `+cluster` / `+sentinel` URL is
    /// missing its seed, sentinels, or master name; otherwise propagates the
    /// underlying connection error.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        if let Some(rest) = url
            .strip_prefix("redis+cluster://")
            .or_else(|| url.strip_prefix("rediss+cluster://"))
        {
            let seed = rest.split('/').next().unwrap_or(rest);
            if seed.is_empty() {
                return Err(RedisError::InvalidUrl(
                    "redis+cluster URL requires a seed host:port".into(),
                ));
            }
            return Self::cluster(seed).await;
        }

        if let Some(rest) = url.strip_prefix("redis+sentinel://") {
            let (hosts, master) = rest.split_once('/').ok_or_else(|| {
                RedisError::InvalidUrl(
                    "redis+sentinel URL requires a master name: redis+sentinel://h1,h2/master"
                        .into(),
                )
            })?;
            let addrs: Vec<&str> = hosts.split(',').filter(|s| !s.is_empty()).collect();
            if addrs.is_empty() {
                return Err(RedisError::InvalidUrl(
                    "redis+sentinel URL requires at least one sentinel host".into(),
                ));
            }
            if master.is_empty() {
                return Err(RedisError::InvalidUrl(
                    "redis+sentinel URL requires a master name after the '/'".into(),
                ));
            }
            return Self::sentinel(&addrs, master).await;
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
}
