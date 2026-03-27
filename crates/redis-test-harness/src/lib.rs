//! Simple Redis topology management for integration testing.
//!
//! Provides [`cluster::RedisCluster`] and [`sentinel::RedisSentinel`] — both
//! generate configs and shell out to `redis-server` / `redis-cli`. No client
//! library dependencies.
//!
//! # Example
//!
//! ```no_run
//! use redis_test_harness::cluster::RedisCluster;
//! use redis_test_harness::sentinel::RedisSentinel;
//!
//! // Cluster: 3 masters + 3 replicas on ports 7000-7005
//! let cluster = RedisCluster::with_defaults();
//! cluster.start().unwrap();
//! let status = cluster.poke().unwrap();
//! assert_eq!(status.cluster_state, "ok");
//! cluster.stop().unwrap();
//!
//! // Sentinel: 1 master + 2 replicas + 3 sentinels
//! let sentinel = RedisSentinel::with_defaults();
//! sentinel.start().unwrap();
//! let status = sentinel.poke().unwrap();
//! assert_eq!(status.flags, "master");
//! sentinel.stop().unwrap();
//! ```

pub mod cluster;
pub mod sentinel;
pub(crate) mod util;

// Re-export the main types at crate root for convenience.
pub use cluster::{ClusterConfig, ClusterStatus, RedisCluster};
pub use sentinel::{RedisSentinel, SentinelConfig, SentinelMasterStatus};
