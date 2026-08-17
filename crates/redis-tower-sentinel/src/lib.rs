//! Redis Sentinel support for redis-tower.
//!
//! Provides automatic master discovery and failover handling via Redis
//! Sentinel.
//!
//! # Master Discovery
//!
//! [`SentinelConnection`] accepts a list of Sentinel addresses and a
//! monitored master name. On connect, it queries the Sentinels with
//! `SENTINEL GET-MASTER-ADDR-BY-NAME` to resolve the current master's
//! address, then opens a standard [`redis_tower_core::RedisConnection`] to
//! that node. See [`discovery`] for the lower-level discovery utilities.
//!
//! # Automatic Failover
//!
//! When a command fails with a connection error, the next call triggers
//! rediscovery -- the Sentinels are queried again to find the new master
//! (which may have changed due to a failover event). The connection is
//! then transparently re-established to the promoted master.
//!
//! # Replica Reads
//!
//! The builders for [`SentinelConnection`], [`SentinelClient`], and
//! [`MultiplexedSentinelClient`] accept a [`ReadPreference`] and optional
//! [`ReadRoutingStrategy`]. The default remains [`ReadPreference::Master`].
//! Opting into replica reads affects only commands classified as read-only;
//! writes continue to use the Sentinel-discovered master.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower_sentinel::{ReadPreference, SentinelConnection};
//!
//! let mut connection = SentinelConnection::builder(
//!     &["127.0.0.1:26379", "127.0.0.1:26380"],
//!     "mymaster",
//! )
//! .read_preference(ReadPreference::PreferReplica)
//! .connect()
//! .await?;
//! # let _ = &mut connection;
//! # Ok(())
//! # }
//! ```
//!
//! # Auth and TLS
//!
//! Use the builder APIs to configure credentials and TLS independently for the
//! sentinel hop (connecting to sentinel nodes) and the node hop (connecting to
//! the discovered master). Sentinels and the master commonly use different
//! passwords in production.
//! The node provider is consulted again after failover and reconnect. Shared
//! and multiplexed clients can retain an owned streaming-credential handle to
//! apply proactive updates to established master and replica data sockets;
//! Sentinel discovery sockets are short-lived and fetch on each query.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower_sentinel::SentinelClient;
//! use redis_tower::credentials::StaticCredentials;
//!
//! let client = SentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
//!     .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
//!     .node_credentials(StaticCredentials::password("redis_pass"))
//!     .connect()
//!     .await?;
//! # let _ = client;
//! # Ok(())
//! # }
//! ```
//!
//! # Usage
//!
//! [`SentinelClient`] provides a higher-level API on top of
//! [`SentinelConnection`] for users who prefer the `execute`-style interface
//! rather than working with `tower::Service` directly.
//!
//! For high-concurrency workloads, [`MultiplexedSentinelClient`] batches
//! concurrent requests into pipelines automatically using a single shared
//! connection.

#![forbid(unsafe_code)]

mod client;
mod connection;
pub mod discovery;
mod multiplexed;

pub use client::{SentinelClient, SentinelClientBuilder};
pub use connection::{SentinelConnection, SentinelConnectionBuilder};
pub use discovery::SentinelConfig;
pub use multiplexed::{MultiplexedSentinelClient, MultiplexedSentinelClientBuilder};
pub use redis_tower::{
    FirstReplicaRouting, NodeAddr, RandomRouting, ReadPreference, ReadRoutingStrategy,
    RoundRobinRouting,
};
