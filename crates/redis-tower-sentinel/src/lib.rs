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
//! # Usage
//!
//! [`SentinelClient`] provides a higher-level API on top of
//! [`SentinelConnection`] for users who prefer the `execute`-style interface
//! rather than working with `tower::Service` directly.
//!
//! For high-concurrency workloads, [`MultiplexedSentinelClient`] batches
//! concurrent requests into pipelines automatically using a single shared
//! connection.

mod client;
mod connection;
pub mod discovery;
mod multiplexed;

pub use client::SentinelClient;
pub use connection::SentinelConnection;
pub use multiplexed::MultiplexedSentinelClient;
