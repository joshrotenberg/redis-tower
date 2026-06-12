//! Redis Cluster support for redis-tower.
//!
//! This crate provides cluster-aware routing that directs commands to the
//! correct node based on the key's hash slot. It ships two client types
//! with different concurrency models; pick one based on your workload.
//!
//! # Which client to use
//!
//! | You need... | Use |
//! |---|---|
//! | Simple one-task-at-a-time usage, lowest moving parts | [`ClusterClient`] |
//! | High-concurrency sharing across many tokio tasks | [`MultiplexedClusterClient`] |
//! | Automatic per-node reconnect on failover | [`MultiplexedClusterClient`] |
//! | Credential rotation across reconnects | [`MultiplexedClusterClient`] |
//! | Per-node background auto-pipelining of concurrent requests | [`MultiplexedClusterClient`] |
//!
//! # Transactions
//!
//! MULTI/EXEC is **not** supported on the cluster clients: commands route to
//! their key's node while MULTI/EXEC route to the default node, so a
//! transaction scatters and is not atomic. Atomic cluster transactions need
//! all keys in one hash slot plus a slot-pinned executor (not yet implemented).
//! For a transaction, target a single-node client for the owning slot.
//!
//! ## [`ClusterClient`]
//!
//! A thin `Arc<Mutex<ClusterConnection>>`. Commands serialize through a
//! single cluster-wide lock, so throughput does not scale with
//! concurrency beyond the latency of one in-flight request. Use when you
//! want the simplest possible surface or when ordering across commands
//! must be total.
//!
//! ## [`MultiplexedClusterClient`]
//!
//! Owns one [`AutoPipelineService`](redis_tower::AutoPipelineService) per
//! master (and optionally per replica). Each per-node worker runs a
//! background task that automatically pipelines concurrent requests from
//! all sharing tasks into a single Redis pipeline, and transparently
//! reconnects via a [`ConnectionFactory`](redis_tower::reconnect::ConnectionFactory)
//! with exponential backoff. Cheap to `Clone`. No cluster-wide lock: slot
//! routing is a short read-lock lookup.
//!
//! Benchmark at concurrency 128 on a 3-master cluster (local laptop):
//! `ClusterClient` caps at ~14k ops/s (mutex-bound), `MultiplexedClusterClient`
//! reaches ~500k ops/s, beating redis-rs `cluster_async` by ~12% with
//! ~2x better p99 latency.
//!
//! # Slot Routing
//!
//! Redis Cluster partitions the keyspace into 16384 hash slots. Both
//! clients compute the slot for each command's key (respecting
//! `{hash_tag}` notation) and route the command to the node that owns
//! that slot. See [`slot`] for the hashing utilities.
//!
//! # Topology Discovery
//!
//! Both clients discover the cluster layout by issuing `CLUSTER SLOTS` to
//! a seed node, then maintain connections to each master (and optionally
//! replica) node. Topology is refreshed automatically on MOVED redirects.
//! See [`topology`] for the discovery types.
//!
//! # Redirect Handling
//!
//! MOVED and ASK redirects are handled transparently. MOVED triggers a
//! topology patch and retries against the new owner. ASK sends `ASKING`
//! followed by the command on the same connection -- for
//! [`MultiplexedClusterClient`], that happens via
//! [`AutoPipelineService::call_pipeline`](redis_tower::AutoPipelineService::call_pipeline),
//! which guarantees the two frames land contiguously on the wire with
//! no interleaving from other concurrent callers.
//!
//! # Read Preference
//!
//! [`ReadPreference`] controls whether read-only commands are routed to
//! masters, replicas, or replicas with a master fallback. Both clients
//! honor it.
//!
//! # Read Routing Strategy
//!
//! When reads are directed to replicas, the [`ReadRoutingStrategy`] trait
//! determines which replica is selected. Built-in strategies include
//! [`RoundRobinRouting`] (default), [`RandomRouting`], and
//! [`FirstReplicaRouting`]. Custom strategies can be provided via either
//! builder's `read_routing` method.
//!
//! # Authentication
//!
//! [`MultiplexedClusterClient`] accepts a
//! [`CredentialProvider`](redis_tower::credentials::CredentialProvider) via
//! `.credentials(provider)` on its builder. The provider is consulted on
//! initial connect and on every reconnect, so credential rotation flows
//! through automatically.
//!
//! # TLS
//!
//! [`MultiplexedClusterClient`] supports TLS behind the `tls-rustls` or
//! `tls-native-tls` feature. Pass a `TlsConfig` (from
//! `redis_tower_core::tls`) via `.tls(config)` on the builder -- the
//! seed connection used for topology discovery as well as every per-node
//! factory will speak TLS on each (re)connect. The SNI hostname is taken
//! from the host portion of each node's address; combine with
//! `.host_override(host)` if your nodes report IPs that don't match your
//! certificate.

mod client;
mod connection;
pub mod key_extractor;
mod multiplexed;
pub mod slot;
pub mod topology;

pub use client::ClusterClient;
pub use connection::{
    ClusterConnection, ClusterConnectionBuilder, FirstReplicaRouting, RandomRouting,
    ReadPreference, ReadRoutingStrategy, RoundRobinRouting,
};
pub use multiplexed::{MultiplexedClusterClient, MultiplexedClusterClientBuilder};
pub use slot::{SLOT_COUNT, extract_hash_tag, slot_for_key};
pub use topology::{ClusterTopology, NodeAddr, SlotRange};
